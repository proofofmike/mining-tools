#!/usr/bin/env python3

"""
Stratum Prevhash Race Timer
Credit: @proofofmike / ProofOfMike.com

Async Stratum prevhash race timer for comparing solo mining pool notify timing
from one client vantage point.

Measures which solo mining pool announces a new prevhash first.

Timing model:
  - Single asyncio event loop.
  - Timestamp is taken immediately after readline() returns.
  - Race signal is mining.notify where clean_jobs is true and prevhash changed.
  - First notify after connect/reconnect is a baseline only, even when clean=true.

Important accounting model:
  - Reconnects are counted in one centralized path.
  - Read timeouts and remote closes are reconnect events.
  - Connect failures/timeouts are connection failures, not established-session closes.
  - A pool can match a race after reconnect, but cannot start a new race until it has
    re-synced by matching a real confirmed race.

Methodology note:
  - A "win" means this client observed that pool first from this network/location.
  - It is not proof that the pool globally won block-template propagation.
"""

import argparse
import asyncio
import csv
import json
import platform
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


DEFAULT_POOLS: List[Tuple[str, str, int, str]] = [
    ("ckpool",       "solo.ckpool.org",            3333,  "US"),
    ("atlaspool",    "solo.atlaspool.io",          3333,  "US"),
    ("2miners",      "solo-btc.2miners.com",      2323,  "US"),
    ("public_pool",  "public-pool.io",             3333,  "US"),
#   ("parasite",     "parasite.wtf",               42069, "US"),
    ("solofury",     "btc.solofury.com",           6060,  "US"),
    ("solo_cat",     "solo.cat",                   3333,  "US"),
    ("helios",       "btc.heliospool.com",         3333,  "CA"),
    ("solopool_com", "stratum.solopool.com",       3333,  "US"),
    ("us_solohash",  "solo-ca.solohash.co.uk",     3333,  "US"),
    ("braiins_solo", "solo.stratum.braiins.com",   3333,  "US"),
]

CONFIRM_WINDOW = 15.0
RECONNECT_DELAY = 10.0
WARMUP_AFTER_CONSENSUS = 10.0
CONNECT_TIMEOUT = 15.0
READ_TIMEOUT = 60.0
MIN_DIRECTIONAL_RACES = 20

CLIENT_VERSION = "stratum-race-test/0.4-proofofmike"


class ReconnectSession(Exception):
    """Raised internally when an established connection should reconnect."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def wall_clock() -> str:
    return time.strftime("%H:%M:%S")


def local_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def loop_time() -> float:
    return asyncio.get_running_loop().time()


def ms(seconds: float) -> float:
    return round(seconds * 1000.0, 3)


def _print(pool_name: str, msg: str) -> None:
    print(f"[{wall_clock()}] {pool_name:<12} {msg}", flush=True)


def pct(values: Sequence[float], percentile: float) -> Optional[float]:
    """Nearest-rank percentile for small sample sizes."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = round((percentile / 100.0) * (len(ordered) - 1))
    return ordered[int(rank)]


def fnum(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


@dataclass
class PoolConfig:
    name: str
    host: str
    port: int
    country: str = "??"


@dataclass
class PoolState:
    name: str
    host: str
    port: int
    user: str
    country: str = "??"

    connected: bool = False
    current_prevhash: Optional[str] = None
    eligible: bool = False

    # Race results. These are confirmed-race arrivals only.
    wins: int = 0
    losses: int = 0
    seen: int = 0
    missed: int = 0
    winner_delays: List[float] = field(default_factory=list)  # always 0.0 for wins; useful for export/ranking
    delays: List[float] = field(default_factory=list)         # non-winner delays only
    all_arrival_offsets: List[float] = field(default_factory=list)  # includes winner 0.0 and losses

    # Race/state anomalies.
    unmatched: int = 0          # provisional starts that never got a second pool
    stale_repeats: int = 0      # clean=true prevhash already seen/closed
    unstable: int = 0           # clean=true prevhash change while not eligible to start

    # Connection health.
    connect_attempts: int = 0
    connections: int = 0
    reconnects: int = 0         # established session ended and will reconnect
    read_timeouts: int = 0
    remote_closes: int = 0
    connect_timeouts: int = 0
    connect_errors: int = 0
    other_disconnects: int = 0

    # Session timing.
    connected_at_wall: Optional[str] = None
    first_notify_at_wall: Optional[str] = None
    last_notify_at_wall: Optional[str] = None

    # Notify accounting.
    notify_total: int = 0
    clean_true: int = 0
    clean_false: int = 0
    noise_repeats: int = 0      # clean=false same-prevhash refreshes
    noise_prevhash_changes: int = 0
    parse_errors: int = 0
    bad_notify: int = 0

    def record_reconnect(self, reason: str) -> None:
        """Count an established-session reconnect in one place."""
        self.reconnects += 1

        if reason == "read_timeout":
            self.read_timeouts += 1
        elif reason == "remote_closed":
            self.remote_closes += 1
        else:
            self.other_disconnects += 1

    def reset_connection_state(self) -> None:
        self.connected = False
        self.current_prevhash = None
        self.eligible = False
        self.connected_at_wall = None
        self.first_notify_at_wall = None


@dataclass
class Race:
    index: int
    prevhash: str
    first_pool: str
    first_ts: float
    first_wall: str
    eligible_at_start: Set[str]
    arrivals: Dict[str, float] = field(default_factory=dict)
    arrival_wall: Dict[str, str] = field(default_factory=dict)
    confirmed: bool = False
    closed: bool = False
    counted: Set[str] = field(default_factory=set)
    missed_counted: bool = False

    def arrival_offsets_ms(self) -> Dict[str, float]:
        return {
            pool_name: ms(arrival_ts - self.first_ts)
            for pool_name, arrival_ts in self.arrivals.items()
        }

    def missed_pools(self) -> List[str]:
        return sorted(self.eligible_at_start - set(self.arrivals))


class RaceTracker:
    def __init__(self) -> None:
        self.active: Dict[str, Race] = {}
        self.all_races: List[Race] = []
        self.seen_prevhashes: Set[str] = set()

        self.consensus_prevhash: Optional[str] = None
        self.consensus_ts: Optional[float] = None
        self.tracking_enabled: bool = False

        self.last_wait_print: float = 0.0

    def _count_arrival(self, race: Race, pool_name: str, pools: Dict[str, PoolState]) -> None:
        if pool_name in race.counted:
            return

        p = pools[pool_name]
        p.seen += 1
        offset = ms(race.arrivals[pool_name] - race.first_ts)
        p.all_arrival_offsets.append(offset)

        if pool_name == race.first_pool:
            p.wins += 1
            p.winner_delays.append(0.0)
        else:
            p.losses += 1
            p.delays.append(offset)

        race.counted.add(pool_name)

    def _count_misses(self, race: Race, pools: Dict[str, PoolState]) -> None:
        if race.missed_counted:
            return

        if not race.confirmed:
            return

        for pool_name in race.missed_pools():
            pools[pool_name].missed += 1

        race.missed_counted = True

    def cleanup_races(self, pools: Dict[str, PoolState], force: bool = False) -> None:
        now = loop_time()

        for ph, race in list(self.active.items()):
            if not force and now - race.first_ts <= CONFIRM_WINDOW:
                continue

            if not race.confirmed:
                pools[race.first_pool].unmatched += 1
            else:
                self._count_misses(race, pools)

            race.closed = True
            del self.active[ph]

    def finalize(self, pools: Dict[str, PoolState]) -> None:
        self.cleanup_races(pools, force=True)

        # Also count misses for confirmed races that were already removed before the
        # final report. This is idempotent because each Race has missed_counted.
        for race in self.all_races:
            self._count_misses(race, pools)

    def _all_have_same_prevhash(self, pools: Dict[str, PoolState]) -> Tuple[bool, Optional[str]]:
        if any(p.current_prevhash is None for p in pools.values()):
            return False, None

        vals = {p.current_prevhash for p in pools.values()}

        if len(vals) != 1:
            return False, None

        return True, next(iter(vals))

    def _consensus_values(self, pools: Dict[str, PoolState]) -> List[str]:
        return sorted({p.current_prevhash[:10] for p in pools.values() if p.current_prevhash})

    def _eligible_starters(self, pools: Dict[str, PoolState]) -> Set[str]:
        return {
            name
            for name, p in pools.items()
            if p.eligible and p.current_prevhash == self.consensus_prevhash
        }

    def handle_notify(
        self,
        pool_name: str,
        recv_ts: float,
        prevhash: str,
        clean: bool,
        pools: Dict[str, PoolState],
    ) -> None:
        pool = pools[pool_name]
        pool.notify_total += 1
        pool.last_notify_at_wall = local_iso()
        if pool.first_notify_at_wall is None:
            pool.first_notify_at_wall = pool.last_notify_at_wall

        if clean:
            pool.clean_true += 1
        else:
            pool.clean_false += 1

        old_ph = pool.current_prevhash

        # First notify after connect/reconnect. This establishes only a baseline.
        # Some pools send clean=false for this; that is fine for sync, not a race.
        if old_ph is None:
            pool.current_prevhash = prevhash
            pool.eligible = False
            _print(pool_name, f"baseline {prevhash[:10]} clean={clean}")
            return

        # Same prevhash. clean=false is expected template-refresh noise.
        if prevhash == old_ph:
            if not clean:
                pool.noise_repeats += 1
            return

        # Prevhash changed.
        pool.current_prevhash = prevhash

        # clean=false prevhash changes are tracked, but ignored as race signals.
        if not clean:
            pool.noise_prevhash_changes += 1
            _print(pool_name, f"prevhash changed clean=false ignored {prevhash[:10]}")
            return

        # From here: clean=true + prevhash changed only.
        if not self.tracking_enabled:
            _print(pool_name, f"baseline {prevhash[:10]} clean=true")
            return

        race = self.active.get(prevhash)

        if race is not None:
            if pool_name not in race.arrivals:
                race.arrivals[pool_name] = recv_ts
                race.arrival_wall[pool_name] = local_iso()
                delay = ms(recv_ts - race.first_ts)
                _print(pool_name, f"match {prevhash[:10]} delay={delay} ms")

                # A reconnecting pool becomes eligible again only after it proves it
                # is synced to a real race.
                pool.eligible = True

                if not race.confirmed and len(race.arrivals) >= 2:
                    race.confirmed = True
                    self.consensus_prevhash = prevhash

                    for arrived_pool in race.arrivals:
                        pools[arrived_pool].eligible = True
                        self._count_arrival(race, arrived_pool, pools)

                elif race.confirmed:
                    self._count_arrival(race, pool_name, pools)

            return

        if prevhash in self.seen_prevhashes:
            pool.stale_repeats += 1
            return

        can_start = (
            pool.eligible
            and self.consensus_prevhash is not None
            and old_ph == self.consensus_prevhash
        )

        if not can_start:
            pool.unstable += 1
            return

        self.seen_prevhashes.add(prevhash)

        eligible_at_start = self._eligible_starters(pools)
        eligible_at_start.add(pool_name)

        race = Race(
            index=len(self.all_races) + 1,
            prevhash=prevhash,
            first_pool=pool_name,
            first_ts=recv_ts,
            first_wall=local_iso(),
            eligible_at_start=eligible_at_start,
        )
        race.arrivals[pool_name] = recv_ts
        race.arrival_wall[pool_name] = race.first_wall

        self.active[prevhash] = race
        self.all_races.append(race)

        _print(pool_name, f"PROVISIONAL start {prevhash[:10]}")

    def check_consensus(self, pools: Dict[str, PoolState]) -> None:
        if self.tracking_enabled:
            return

        ok, ph = self._all_have_same_prevhash(pools)

        if not ok:
            now = loop_time()

            if now - self.last_wait_print > 10:
                vals = self._consensus_values(pools)
                missing = sorted(name for name, p in pools.items() if p.current_prevhash is None)
                print(f"\n--- WAITING FOR BASELINE CONSENSUS: {vals} ---", flush=True)
                if missing:
                    print(f"--- MISSING BASELINE: {missing} ---", flush=True)
                print("", flush=True)
                self.last_wait_print = now

            return

        if self.consensus_prevhash != ph:
            self.consensus_prevhash = ph
            self.consensus_ts = loop_time()
            print(f"\n--- ALL POOLS BASELINED ON SAME PREVHASH {ph[:10]} ---\n", flush=True)

        if self.consensus_ts and loop_time() - self.consensus_ts >= WARMUP_AFTER_CONSENSUS:
            self.tracking_enabled = True
            self.seen_prevhashes.add(self.consensus_prevhash)

            for p in pools.values():
                p.eligible = True

            print("\n--- TRACKING STARTED ---\n", flush=True)


def delay_stats(values: Sequence[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {
            "avg": None,
            "median": None,
            "p95": None,
            "stddev": None,
            "best": None,
            "worst": None,
        }

    return {
        "avg": statistics.mean(values),
        "median": statistics.median(values),
        "p95": pct(values, 95),
        "stddev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "best": min(values),
        "worst": max(values),
    }


def print_pool_table(pools: Dict[str, PoolState]) -> None:
    columns = [
        ("Pool",        12),
        ("CC",           3),
        ("Wins",         5),
        ("Loss",         5),
        ("Seen",         5),
        ("Miss",         5),
        ("Avg",          8),
        ("Med",          8),
        ("P95",          8),
        ("Std",          8),
        ("Best",         8),
        ("Worst",        8),
        ("Unmatch",      7),
        ("Stale",        6),
        ("Unstable",     8),
        ("Reconn",       7),
        ("Timeout",      7),
        ("Closed",       6),
        ("ConnTO",       6),
        ("ConnErr",      7),
        ("Notify",       7),
        ("CleanT",       7),
        ("CleanF",       7),
        ("Noise",        7),
    ]

    header = " ".join(title.ljust(width) for title, width in columns)
    print(header)
    print("-" * len(header))

    for p in pools.values():
        stats = delay_stats(p.all_arrival_offsets)

        row = [
            p.name.ljust(12),
            p.country.ljust(3),
            str(p.wins).rjust(5),
            str(p.losses).rjust(5),
            str(p.seen).rjust(5),
            str(p.missed).rjust(5),
            fnum(stats["avg"]).rjust(8),
            fnum(stats["median"]).rjust(8),
            fnum(stats["p95"]).rjust(8),
            fnum(stats["stddev"]).rjust(8),
            fnum(stats["best"]).rjust(8),
            fnum(stats["worst"]).rjust(8),
            str(p.unmatched).rjust(7),
            str(p.stale_repeats).rjust(6),
            str(p.unstable).rjust(8),
            str(p.reconnects).rjust(7),
            str(p.read_timeouts).rjust(7),
            str(p.remote_closes).rjust(6),
            str(p.connect_timeouts).rjust(6),
            str(p.connect_errors).rjust(7),
            str(p.notify_total).rjust(7),
            str(p.clean_true).rjust(7),
            str(p.clean_false).rjust(7),
            str(p.noise_repeats).rjust(7),
        ]

        print(" ".join(row))


def print_race_detail(races: List[Race], limit: Optional[int] = None) -> None:
    selected = races[-limit:] if limit and limit > 0 else races
    if not selected:
        print("\nPER-RACE DETAIL: none")
        return

    print("\nPER-RACE DETAIL:")
    for race in selected:
        status = "confirmed" if race.confirmed else "unconfirmed"
        arrivals = sorted(race.arrival_offsets_ms().items(), key=lambda kv: kv[1])
        arrival_text = ", ".join(f"{name} +{delay:.1f}ms" for name, delay in arrivals)
        missed = race.missed_pools() if race.confirmed else []
        missed_text = f" | missed: {', '.join(missed)}" if missed else ""
        print(
            f"{race.index:>3}. {race.prevhash[:10]} {status:<11} "
            f"winner={race.first_pool:<12} arrivals: {arrival_text}{missed_text}"
        )


def print_rankings(pools: Dict[str, PoolState], confirmed_count: int) -> None:
    print("\nSPEED RANK, MEDIAN ARRIVAL OFFSET INCLUDING WINS:")
    speed_rank = [
        (statistics.median(p.all_arrival_offsets), p.name, p)
        for p in pools.values()
        if p.all_arrival_offsets
    ]
    for rank, (median_delay, _name, p) in enumerate(sorted(speed_rank), 1):
        print(
            f"{rank:>2}. {p.name:<12} median={median_delay:.1f}ms "
            f"avg={statistics.mean(p.all_arrival_offsets):.1f}ms seen={p.seen}/{confirmed_count} wins={p.wins}"
        )

    print("\nRELIABILITY RANK:")
    reliability_rank = sorted(
        pools.values(),
        key=lambda p: (-p.seen, p.missed, p.reconnects, p.read_timeouts + p.remote_closes, p.name),
    )
    for rank, p in enumerate(reliability_rank, 1):
        print(
            f"{rank:>2}. {p.name:<12} seen={p.seen}/{confirmed_count} missed={p.missed} "
            f"reconnects={p.reconnects} timeouts={p.read_timeouts} closed={p.remote_closes}"
        )


def runtime_info(args: argparse.Namespace, pool_configs: List[PoolConfig], start_local: str, start_utc: str) -> Dict[str, Any]:
    return {
        "credit": "@proofofmike / ProofOfMike.com",
        "client_version": CLIENT_VERSION,
        "started_local": start_local,
        "started_utc": start_utc,
        "python": sys.version.replace("\n", " "),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "duration_seconds": args.duration,
        "confirm_window_seconds": CONFIRM_WINDOW,
        "reconnect_delay_seconds": RECONNECT_DELAY,
        "warmup_after_consensus_seconds": WARMUP_AFTER_CONSENSUS,
        "connect_timeout_seconds": CONNECT_TIMEOUT,
        "read_timeout_seconds": READ_TIMEOUT,
        "pool_count": len(pool_configs),
        "pools": [asdict(pc) for pc in pool_configs],
    }


def pool_summary_dict(p: PoolState) -> Dict[str, Any]:
    stats = delay_stats(p.all_arrival_offsets)
    nonwin_stats = delay_stats(p.delays)
    return {
        "name": p.name,
        "country": p.country,
        "host": p.host,
        "port": p.port,
        "wins": p.wins,
        "losses": p.losses,
        "seen": p.seen,
        "missed": p.missed,
        "arrival_offset_ms": stats,
        "non_winner_delay_ms": nonwin_stats,
        "unmatched": p.unmatched,
        "stale_repeats": p.stale_repeats,
        "unstable": p.unstable,
        "connect_attempts": p.connect_attempts,
        "connections": p.connections,
        "reconnects": p.reconnects,
        "read_timeouts": p.read_timeouts,
        "remote_closes": p.remote_closes,
        "connect_timeouts": p.connect_timeouts,
        "connect_errors": p.connect_errors,
        "other_disconnects": p.other_disconnects,
        "notify_total": p.notify_total,
        "clean_true": p.clean_true,
        "clean_false": p.clean_false,
        "noise_repeats": p.noise_repeats,
        "noise_prevhash_changes": p.noise_prevhash_changes,
        "parse_errors": p.parse_errors,
        "bad_notify": p.bad_notify,
        "first_notify_at_wall": p.first_notify_at_wall,
        "last_notify_at_wall": p.last_notify_at_wall,
    }


def race_dict(r: Race) -> Dict[str, Any]:
    return {
        "index": r.index,
        "prevhash": r.prevhash,
        "prevhash_short": r.prevhash[:10],
        "winner": r.first_pool,
        "first_wall": r.first_wall,
        "confirmed": r.confirmed,
        "closed": r.closed,
        "eligible_at_start": sorted(r.eligible_at_start),
        "arrivals_offset_ms": r.arrival_offsets_ms(),
        "arrival_wall": dict(r.arrival_wall),
        "missed_pools": r.missed_pools() if r.confirmed else [],
    }


def write_json(path: str, pools: Dict[str, PoolState], races: List[Race], meta: Dict[str, Any]) -> None:
    confirmed = [r for r in races if r.confirmed]
    payload = {
        "meta": meta,
        "methodology_note": "Wins are first observed by this client/vantage point, not global proof of pool propagation victory.",
        "confirmed_races": len(confirmed),
        "unconfirmed_provisional_races": len([r for r in races if not r.confirmed]),
        "pools": [pool_summary_dict(p) for p in pools.values()],
        "races": [race_dict(r) for r in races],
    }
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_csv(prefix_or_path: str, pools: Dict[str, PoolState], races: List[Race]) -> Tuple[str, str]:
    path = Path(prefix_or_path)
    if path.suffix.lower() == ".csv":
        pool_path = path
        race_path = path.with_name(path.stem + "_races.csv")
    else:
        pool_path = Path(str(path) + "_pools.csv")
        race_path = Path(str(path) + "_races.csv")

    with pool_path.open("w", newline="") as f:
        fieldnames = [
            "pool", "country", "host", "port", "wins", "losses", "seen", "missed",
            "avg_ms", "median_ms", "p95_ms", "stddev_ms", "best_ms", "worst_ms",
            "unmatched", "stale", "unstable", "reconnects", "read_timeouts",
            "remote_closes", "connect_timeouts", "connect_errors", "notify_total",
            "clean_true", "clean_false", "noise_repeats", "noise_prevhash_changes",
            "parse_errors", "bad_notify",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for p in pools.values():
            stats = delay_stats(p.all_arrival_offsets)
            writer.writerow({
                "pool": p.name,
                "country": p.country,
                "host": p.host,
                "port": p.port,
                "wins": p.wins,
                "losses": p.losses,
                "seen": p.seen,
                "missed": p.missed,
                "avg_ms": stats["avg"],
                "median_ms": stats["median"],
                "p95_ms": stats["p95"],
                "stddev_ms": stats["stddev"],
                "best_ms": stats["best"],
                "worst_ms": stats["worst"],
                "unmatched": p.unmatched,
                "stale": p.stale_repeats,
                "unstable": p.unstable,
                "reconnects": p.reconnects,
                "read_timeouts": p.read_timeouts,
                "remote_closes": p.remote_closes,
                "connect_timeouts": p.connect_timeouts,
                "connect_errors": p.connect_errors,
                "notify_total": p.notify_total,
                "clean_true": p.clean_true,
                "clean_false": p.clean_false,
                "noise_repeats": p.noise_repeats,
                "noise_prevhash_changes": p.noise_prevhash_changes,
                "parse_errors": p.parse_errors,
                "bad_notify": p.bad_notify,
            })

    with race_path.open("w", newline="") as f:
        fieldnames = [
            "index", "prevhash", "confirmed", "winner", "first_wall",
            "pool", "offset_ms", "eligible_at_start", "missed_pools",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in races:
            offsets = r.arrival_offsets_ms()
            for pool_name, offset in sorted(offsets.items(), key=lambda kv: kv[1]):
                writer.writerow({
                    "index": r.index,
                    "prevhash": r.prevhash,
                    "confirmed": r.confirmed,
                    "winner": r.first_pool,
                    "first_wall": r.first_wall,
                    "pool": pool_name,
                    "offset_ms": offset,
                    "eligible_at_start": ";".join(sorted(r.eligible_at_start)),
                    "missed_pools": ";".join(r.missed_pools() if r.confirmed else []),
                })

    return str(pool_path), str(race_path)


def print_final_report(
    pools: Dict[str, PoolState],
    races: List[Race],
    duration: int,
    meta: Dict[str, Any],
    race_limit: Optional[int] = None,
) -> None:
    confirmed = [r for r in races if r.confirmed]
    unconfirmed = [r for r in races if not r.confirmed]

    print("\n================ FINAL REPORT ================\n")
    print("Credit: @proofofmike / ProofOfMike.com")
    print(f"Run duration: {duration}s")
    print(f"Confirmed races: {len(confirmed)}")
    print(f"Unconfirmed provisional races: {len(unconfirmed)}")
    if len(confirmed) < MIN_DIRECTIONAL_RACES:
        print(
            f"WARNING: only {len(confirmed)} confirmed races. Treat this as directional, "
            f"not statistically final. Suggested minimum: {MIN_DIRECTIONAL_RACES}+ races."
        )
    print()

    print_pool_table(pools)
    print_rankings(pools, len(confirmed))
    print_race_detail(races, limit=race_limit)

    print("\nCONNECTION DETAIL:")
    for p in pools.values():
        print(
            f"{p.name:<12} attempts={p.connect_attempts} "
            f"connected={p.connections} reconnects={p.reconnects} "
            f"timeouts={p.read_timeouts} remote_closed={p.remote_closes} "
            f"connect_timeouts={p.connect_timeouts} connect_errors={p.connect_errors} "
            f"other_disconnects={p.other_disconnects} "
            f"first_notify={p.first_notify_at_wall or 'N/A'} last_notify={p.last_notify_at_wall or 'N/A'}"
        )

    print("\nDEBUG ARRIVAL OFFSETS INCLUDING WINS:")
    for pool_name, p in pools.items():
        print(pool_name, p.all_arrival_offsets)

    print("\nRUNTIME:")
    print(f"  started_local = {meta['started_local']}")
    print(f"  started_utc   = {meta['started_utc']}")
    print(f"  python        = {meta['python']}")
    print(f"  platform      = {meta['platform']}")
    print(f"  client        = {CLIENT_VERSION}")
    print(f"  confirm_window={CONFIRM_WINDOW}s read_timeout={READ_TIMEOUT}s reconnect_delay={RECONNECT_DELAY}s")

    print("\nNotes:")
    print("  win        = first pool observed by this client/vantage point, not global propagation proof")
    print("  timestamp  = asyncio event-loop time taken immediately after readline() returns")
    print("  baseline   = first notify after connect/reconnect, clean=true OR clean=false")
    print("  race       = only clean=true + prevhash changed")
    print("  seen       = confirmed races where this pool's notify arrived inside the window")
    print("  miss       = confirmed races this pool was eligible for but did not match inside the window")
    print("  avg/med/p95/std/best/worst are arrival offsets in ms including wins as 0.0ms")
    print("  unmatch    = provisional starts by this pool that never got a second pool confirmation")
    print("  stale      = clean=true prevhash already seen/closed")
    print("  unstable   = clean=true prevhash change while the pool was not eligible to start")
    print("  reconnects = established session ended and reconnected: read timeout, remote close, or other disconnect")
    print("  noise      = clean=false same-prevhash notify/template refresh")


def send_json(writer: asyncio.StreamWriter, obj: object) -> None:
    writer.write((json.dumps(obj, separators=(",", ":")) + "\n").encode())


async def close_writer(writer: Optional[asyncio.StreamWriter]) -> None:
    if writer is None:
        return

    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass


async def pool_worker(
    name: str,
    host: str,
    port: int,
    user: str,
    tracker: RaceTracker,
    pools: Dict[str, PoolState],
    stop_event: asyncio.Event,
) -> None:
    pool = pools[name]

    while not stop_event.is_set():
        reader: Optional[asyncio.StreamReader] = None
        writer: Optional[asyncio.StreamWriter] = None
        established = False

        try:
            pool.connect_attempts += 1

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=CONNECT_TIMEOUT,
            )

            established = True
            pool.connected = True
            pool.connections += 1
            pool.connected_at_wall = local_iso()
            pool.current_prevhash = None
            pool.eligible = False
            _print(name, "connected")

            send_json(writer, {"id": 1, "method": "mining.subscribe", "params": []})
            send_json(writer, {"id": 2, "method": "mining.authorize", "params": [user, "x"]})
            await writer.drain()

            while not stop_event.is_set():
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=READ_TIMEOUT)
                except asyncio.TimeoutError:
                    raise ReconnectSession("read_timeout")

                recv_ts = loop_time()

                if not line:
                    raise ReconnectSession("remote_closed")

                line = line.strip()
                if not line:
                    continue

                try:
                    msg = json.loads(line)
                except Exception:
                    pool.parse_errors += 1
                    continue

                method = msg.get("method")

                if method == "client.get_version":
                    send_json(
                        writer,
                        {"id": msg.get("id"), "result": CLIENT_VERSION, "error": None},
                    )
                    await writer.drain()
                    continue

                if method != "mining.notify":
                    continue

                params = msg.get("params", [])
                if len(params) < 2 or not isinstance(params[1], str):
                    pool.bad_notify += 1
                    continue

                prevhash = params[1]
                clean = bool(params[8]) if len(params) > 8 else False

                tracker.handle_notify(
                    pool_name=name,
                    recv_ts=recv_ts,
                    prevhash=prevhash,
                    clean=clean,
                    pools=pools,
                )

        except ReconnectSession as e:
            if not stop_event.is_set():
                pool.record_reconnect(e.reason)

                if e.reason == "read_timeout":
                    _print(name, f"read timeout after {READ_TIMEOUT:.1f}s, reconnecting")
                elif e.reason == "remote_closed":
                    _print(name, "remote closed connection, reconnecting")
                else:
                    _print(name, f"disconnect ({e.reason}), reconnecting")

        except asyncio.TimeoutError:
            if not stop_event.is_set():
                pool.connect_timeouts += 1
                _print(name, f"connect timeout, reconnect in {int(RECONNECT_DELAY)}s")

        except Exception as e:
            if not stop_event.is_set():
                if established:
                    pool.record_reconnect("other")
                    _print(name, f"disconnect ({e}), reconnect in {int(RECONNECT_DELAY)}s")
                else:
                    pool.connect_errors += 1
                    _print(name, f"connect error ({e}), reconnect in {int(RECONNECT_DELAY)}s")

        finally:
            pool.reset_connection_state()
            await close_writer(writer)

        if not stop_event.is_set():
            await asyncio.sleep(RECONNECT_DELAY)


async def housekeeping(
    tracker: RaceTracker,
    pools: Dict[str, PoolState],
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(1.0)
        tracker.check_consensus(pools)
        tracker.cleanup_races(pools)


def load_pool_configs(path: Optional[str]) -> List[PoolConfig]:
    if not path:
        return [PoolConfig(name, host, port, country) for name, host, port, country in DEFAULT_POOLS]

    raw = json.loads(Path(path).read_text())
    if not isinstance(raw, list):
        raise ValueError("pools file must be a JSON list")

    configs: List[PoolConfig] = []
    for i, item in enumerate(raw, 1):
        if not isinstance(item, dict):
            raise ValueError(f"pool entry {i} must be an object")
        try:
            configs.append(
                PoolConfig(
                    name=str(item["name"]),
                    host=str(item["host"]),
                    port=int(item["port"]),
                    country=str(item.get("country", "??")),
                )
            )
        except KeyError as e:
            raise ValueError(f"pool entry {i} missing required field: {e}") from e

    names = [pc.name for pc in configs]
    if len(names) != len(set(names)):
        raise ValueError("pool names must be unique")

    return configs


async def run(args: argparse.Namespace) -> None:
    pool_configs = load_pool_configs(args.pools)
    pools = {
        pc.name: PoolState(name=pc.name, host=pc.host, port=pc.port, user=args.user, country=pc.country)
        for pc in pool_configs
    }

    tracker = RaceTracker()
    stop_event = asyncio.Event()
    start_local = local_iso()
    start_utc = utc_iso()
    meta = runtime_info(args, pool_configs, start_local, start_utc)

    print("\n================ START ================\n")
    print("Credit: @proofofmike / ProofOfMike.com")
    print(f"Duration: {args.duration}s")
    print(f"Pools: {', '.join(pools.keys())}")
    print("Timing: asyncio event-loop clock, single thread")
    print("Baseline: any mining.notify")
    print("Race signal: clean=true + prevhash change")
    print("Win meaning: first observed by this client/vantage point")
    print()

    tasks = [
        asyncio.create_task(pool_worker(pc.name, pc.host, pc.port, args.user, tracker, pools, stop_event))
        for pc in pool_configs
    ]
    tasks.append(asyncio.create_task(housekeeping(tracker, pools, stop_event)))

    try:
        await asyncio.sleep(args.duration)
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        tracker.finalize(pools)

    print_final_report(
        pools=pools,
        races=tracker.all_races,
        duration=args.duration,
        meta=meta,
        race_limit=args.race_limit,
    )

    if args.json_out:
        write_json(args.json_out, pools, tracker.all_races, meta)
        print(f"\nWrote JSON: {args.json_out}")

    if args.csv_out:
        pool_csv, race_csv = write_csv(args.csv_out, pools, tracker.all_races)
        print(f"Wrote CSV: {pool_csv}")
        print(f"Wrote CSV: {race_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Asyncio Stratum prevhash race timer by @proofofmike.")
    parser.add_argument("--user", required=True, help="Stratum username/address.worker")
    parser.add_argument("--duration", type=int, default=7200, help="Run duration in seconds")
    parser.add_argument("--pools", help="Optional pools.json file. List of {name, host, port, country}")
    parser.add_argument("--json-out", help="Write structured JSON results to this path")
    parser.add_argument("--csv-out", help="Write pool/race CSV results. If path ends .csv, race CSV appends _races.csv")
    parser.add_argument("--race-limit", type=int, default=0, help="Limit per-race detail printed; 0 prints all races")
    parser.add_argument("--probe-interval", type=int, default=0, help="Accepted for compatibility, ignored")
    parser.add_argument("--no-ping", action="store_true", help="Accepted for compatibility, ignored")
    args = parser.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
