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
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


DEFAULT_POOLS: List[Tuple[str, str, int]] = [
    ("ckpool",       "solo.ckpool.org",            3333),
    ("atlaspool",    "solo.atlaspool.io",          3333),
    ("parasite",     "parasite.wtf",               42069),
    ("public_pool",  "public-pool.io",             3333),
    ("solofury",     "btc.solofury.com",           6060),
    ("solo_cat",     "solo.cat",                   3333),
    ("helios",       "btc.heliospool.com",         3333),
    ("solopool_com", "stratum.solopool.com",       3333),
    ("us_solohash",  "solo-ca.solohash.co.uk",     3333),
    ("braiins_solo", "solo.stratum.braiins.com",   3333),
]

CONFIRM_WINDOW = 15.0
RECONNECT_DELAY = 10.0
WARMUP_AFTER_CONSENSUS = 10.0
CONNECT_TIMEOUT = 15.0
READ_TIMEOUT = 90.0
MIN_DIRECTIONAL_RACES = 20
SHUTDOWN_GRACE = 2.0
DEFAULT_BASELINE_TIMEOUT = 120.0
BLOCK_MINER_LOOKUP_TIMEOUT = 8.0
MEMPOOL_API_BASE = "https://mempool.space/api"

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


def stratum_prevhash_to_blockhash(stratum_hex: str) -> str:
    """Convert a stratum mining.notify prevhash into the canonical display block
    hash used by explorers / mempool.space.

    The stratum prevhash is byte-mangled: each 4-byte word is reversed, and the
    whole 32-byte value is in internal (little-endian) order. To get the display
    hash we reverse each 4-byte word, then reverse the full 32 bytes.

    Verified against the genesis block in the test suite. All compliant pools use
    the same encoding, which is why cross-pool matching works on the raw value;
    that raw value is NOT the explorer hash, so it must be transformed before any
    block lookup.
    """
    h = stratum_hex.strip().lower()
    if len(h) != 64:
        raise ValueError(f"prevhash must be 64 hex chars, got {len(h)}")
    raw = bytes.fromhex(h)
    word_swapped = b"".join(raw[i:i + 4][::-1] for i in range(0, 32, 4))
    return word_swapped[::-1].hex()


def short_hash(h: Optional[str]) -> str:
    """Distinguishing short form for logs. Real block hashes lead with ~18 zero
    chars, so the leading slice is useless; the tail is what differs."""
    if not h:
        return "?"
    return "\u2026" + h[-12:]


def format_stratum_error(err: Any) -> str:
    """Stratum errors are usually [code, message, traceback] or a string."""
    if isinstance(err, list) and len(err) >= 2:
        return f"{err[1]} (code {err[0]})"
    return str(err)


@dataclass
class PoolConfig:
    name: str
    host: str
    port: int


@dataclass
class PoolState:
    name: str
    host: str
    port: int
    user: str

    connected: bool = False
    current_prevhash: Optional[str] = None
    eligible: bool = False

    # Baseline gating.
    excluded_at_baseline: bool = False
    exclude_reason: Optional[str] = None

    # Auth/subscribe health.
    auth_failed: bool = False
    auth_error: Optional[str] = None
    subscribe_failed: bool = False
    subscribe_error: Optional[str] = None

    # Race results. These are confirmed-race arrivals only.
    wins: int = 0
    losses: int = 0
    seen: int = 0
    missed: int = 0
    delays: List[float] = field(default_factory=list)         # non-winner (chase) delays only
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

    # Optional post-run enrichment only. These fields are never populated from
    # the live timing path.
    block_height: Optional[int] = None
    block_miner: Optional[str] = None
    block_miner_source: Optional[str] = None

    def arrival_offsets_ms(self) -> Dict[str, float]:
        return {
            pool_name: ms(arrival_ts - self.first_ts)
            for pool_name, arrival_ts in self.arrivals.items()
        }

    def missed_pools(self) -> List[str]:
        return sorted(self.eligible_at_start - set(self.arrivals))


class RaceTracker:
    def __init__(self, baseline_timeout: float = DEFAULT_BASELINE_TIMEOUT) -> None:
        self.active: Dict[str, Race] = {}
        self.all_races: List[Race] = []
        self.seen_prevhashes: Set[str] = set()

        self.consensus_prevhash: Optional[str] = None
        self.consensus_ts: Optional[float] = None
        self.tracking_enabled: bool = False

        self.baseline_timeout: float = baseline_timeout
        self.baseline_started_ts: Optional[float] = None
        self.baseline_via_quorum: bool = False

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
        return sorted({short_hash(p.current_prevhash) for p in pools.values() if p.current_prevhash})

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
            _print(pool_name, f"baseline {short_hash(prevhash)} clean={clean}")
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
            _print(pool_name, f"prevhash changed clean=false ignored {short_hash(prevhash)}")
            return

        # From here: clean=true + prevhash changed only.
        if not self.tracking_enabled:
            _print(pool_name, f"baseline {short_hash(prevhash)} clean=true")
            return

        race = self.active.get(prevhash)

        if race is not None:
            if pool_name not in race.arrivals:
                race.arrivals[pool_name] = recv_ts
                race.arrival_wall[pool_name] = local_iso()
                delay = ms(recv_ts - race.first_ts)
                _print(pool_name, f"match {short_hash(prevhash)} delay={fnum(delay)} ms")

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

        _print(pool_name, f"PROVISIONAL start block {prevhash} (height resolved post-run)")

    def _quorum_baseline(
        self, pools: Dict[str, PoolState]
    ) -> Tuple[Optional[str], int, int, List[Tuple[str, str]]]:
        """After the deadline, baseline on the prevhash held by a strict majority
        of responding pools. Returns (modal_hash, modal_count, responding_count,
        excluded[(name, reason)]). modal_hash is None if no quorum yet."""
        responding = [p for p in pools.values() if p.current_prevhash]
        if len(responding) < 2:
            return None, 0, len(responding), []

        counts = Counter(p.current_prevhash for p in responding)
        modal, modal_count = counts.most_common(1)[0]

        if modal_count < 2 or modal_count * 2 <= len(responding):
            return None, modal_count, len(responding), []

        excluded: List[Tuple[str, str]] = []
        for name, p in pools.items():
            if p.current_prevhash != modal:
                reason = (
                    "no baseline before deadline"
                    if p.current_prevhash is None
                    else "diverged from quorum prevhash at baseline"
                )
                excluded.append((name, reason))

        return modal, modal_count, len(responding), excluded

    def _apply_baseline(
        self,
        pools: Dict[str, PoolState],
        candidate: str,
        now: float,
        excluded: List[Tuple[str, str]],
        via_quorum: bool,
        modal_count: int = 0,
        responding: int = 0,
    ) -> None:
        self.consensus_prevhash = candidate
        self.consensus_ts = now
        self.baseline_via_quorum = via_quorum

        for p in pools.values():
            p.excluded_at_baseline = False
            p.exclude_reason = None
        for name, reason in excluded:
            pools[name].excluded_at_baseline = True
            pools[name].exclude_reason = reason

        if via_quorum:
            ex_names = ", ".join(name for name, _ in excluded) or "none"
            print(
                f"\n--- QUORUM BASELINE ON {candidate} "
                f"({modal_count}/{responding} responding pools agreed) ---",
                flush=True,
            )
            print(f"--- EXCLUDED AT BASELINE: {ex_names} ---\n", flush=True)
        else:
            print(
                f"\n--- ALL POOLS BASELINED ON SAME PREVHASH {candidate} ---\n",
                flush=True,
            )

    def check_consensus(self, pools: Dict[str, PoolState]) -> None:
        if self.tracking_enabled:
            return

        now = loop_time()
        if self.baseline_started_ts is None:
            self.baseline_started_ts = now

        # Establish the baseline once. Full consensus wins immediately; otherwise
        # fall back to a majority quorum once the deadline passes.
        if self.consensus_prevhash is None:
            ok, ph = self._all_have_same_prevhash(pools)
            deadline_passed = (now - self.baseline_started_ts) >= self.baseline_timeout

            if ok:
                self._apply_baseline(pools, ph, now, excluded=[], via_quorum=False)
            elif deadline_passed:
                modal, modal_count, responding, excluded = self._quorum_baseline(pools)
                if modal is not None:
                    self._apply_baseline(
                        pools, modal, now, excluded=excluded, via_quorum=True,
                        modal_count=modal_count, responding=responding,
                    )
                else:
                    self._print_wait(pools, now, deadline_passed=True)
            else:
                self._print_wait(pools, now, deadline_passed=False)

        if self.consensus_prevhash is not None and self.consensus_ts is not None and (
            now - self.consensus_ts >= WARMUP_AFTER_CONSENSUS
        ):
            self.tracking_enabled = True
            self.seen_prevhashes.add(self.consensus_prevhash)

            for p in pools.values():
                if not p.excluded_at_baseline:
                    p.eligible = True

            print("\n--- TRACKING STARTED ---", flush=True)
            excluded_now = sorted(name for name, p in pools.items() if p.excluded_at_baseline)
            if excluded_now:
                print(f"--- NOT IN RACE (excluded at baseline): {excluded_now} ---", flush=True)
            print("", flush=True)

    def _print_wait(self, pools: Dict[str, PoolState], now: float, deadline_passed: bool) -> None:
        if now - self.last_wait_print <= 10:
            return
        vals = self._consensus_values(pools)
        missing = sorted(name for name, p in pools.items() if p.current_prevhash is None)
        if deadline_passed:
            print(
                "\n--- BASELINE DEADLINE PASSED, STILL NO QUORUM "
                "(need >=2 responding pools agreeing on one prevhash) ---",
                flush=True,
            )
        else:
            print(f"\n--- WAITING FOR BASELINE CONSENSUS: {vals} ---", flush=True)
        if missing:
            print(f"--- MISSING BASELINE: {missing} ---", flush=True)
        print("", flush=True)
        self.last_wait_print = now



def _nested_get(obj: Dict[str, Any], path: Iterable[str]) -> Any:
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _extract_block_height(data: Dict[str, Any]) -> Optional[int]:
    for path in (("height",), ("block", "height"), ("extras", "height")):
        value = _nested_get(data, path)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _extract_miner_tag(data: Dict[str, Any]) -> Optional[str]:
    """Best-effort parser for mempool.space enriched block responses."""
    candidates = [
        ("extras", "pool", "name"),
        ("extras", "pool", "slug"),
        ("extras", "pool", "id"),
        ("pool", "name"),
        ("pool", "slug"),
        ("pool", "id"),
        ("miner", "name"),
        ("miner",),
        ("mined_by",),
        ("poolName",),
        ("pool_name",),
    ]

    for path in candidates:
        value = _nested_get(data, path)
        if isinstance(value, str) and value.strip():
            return value.strip()

    for path in (("extras", "pool"), ("pool",)):
        value = _nested_get(data, path)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


def _fetch_json_blocking(url: str, timeout: float) -> Dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": CLIENT_VERSION,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        payload = response.read().decode(charset)
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("expected JSON object")
    return data


async def lookup_block_metadata(block_hash: str) -> Dict[str, Any]:
    """Lookup block height/miner tag after timing has stopped."""
    endpoints = [
        f"{MEMPOOL_API_BASE}/v1/block/{block_hash}",
        f"{MEMPOOL_API_BASE}/block/{block_hash}",
    ]

    last_error: Optional[str] = None

    for url in endpoints:
        try:
            data = await asyncio.to_thread(_fetch_json_blocking, url, BLOCK_MINER_LOOKUP_TIMEOUT)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
            last_error = str(e)
            continue

        return {
            "height": _extract_block_height(data),
            "miner": _extract_miner_tag(data) or "Unknown",
            "source": "mempool.space",
        }

    return {
        "height": None,
        "miner": "lookup_failed",
        "source": f"mempool.space error: {last_error or 'no usable response'}",
    }


async def enrich_races_with_block_miners(races: List[Race]) -> None:
    """Post-run only: attach block height and miner tag to confirmed races."""
    confirmed = [r for r in races if r.confirmed]
    unique_hashes = sorted({r.prevhash for r in confirmed})

    if not unique_hashes:
        return

    print("\n--- POST-RUN BLOCK MINER LOOKUP STARTED ---", flush=True)
    print(f"Looking up {len(unique_hashes)} unique confirmed block hash(es). Timing is already stopped.", flush=True)

    metadata: Dict[str, Dict[str, Any]] = {}
    for i, block_hash in enumerate(unique_hashes, 1):
        meta = await lookup_block_metadata(block_hash)
        metadata[block_hash] = meta
        height = meta["height"] if meta["height"] is not None else "N/A"
        print(f"  {i:>3}/{len(unique_hashes)} {short_hash(block_hash)} height={height} mined_by={meta['miner']}", flush=True)

    for race in confirmed:
        meta = metadata.get(race.prevhash, {})
        race.block_height = meta.get("height")
        race.block_miner = meta.get("miner") or "Unknown"
        race.block_miner_source = meta.get("source")

    print("--- POST-RUN BLOCK MINER LOOKUP FINISHED ---\n", flush=True)


def print_block_miner_summary(races: List[Race]) -> None:
    confirmed = [r for r in races if r.confirmed]
    enriched = [r for r in confirmed if r.block_miner]

    if not enriched:
        return

    print("\nBLOCK MINER TAG SUMMARY:")
    print("  Miner tags are post-run enrichment only. They are not used during timing.")

    columns = [
        ("Mined by", 18),
        ("Races", 5),
        ("Top winner", 14),
        ("Wins", 5),
        ("Avg spread", 10),
        ("Med spread", 10),
    ]
    header = " ".join(title.ljust(width) for title, width in columns)
    print(header)
    print("-" * len(header))

    grouped: Dict[str, List[Race]] = defaultdict(list)
    for race in enriched:
        grouped[race.block_miner or "Unknown"].append(race)

    for miner, miner_races in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        winner_counts = Counter(r.first_pool for r in miner_races)
        top_winner, top_wins = winner_counts.most_common(1)[0]
        spreads = []
        for race in miner_races:
            offsets = list(race.arrival_offsets_ms().values())
            if len(offsets) >= 2:
                spreads.append(max(offsets) - min(offsets))

        avg_spread = statistics.mean(spreads) if spreads else None
        med_spread = statistics.median(spreads) if spreads else None

        row = [
            miner[:18].ljust(18),
            str(len(miner_races)).rjust(5),
            top_winner.ljust(14),
            str(top_wins).rjust(5),
            fnum(avg_spread).rjust(10),
            fnum(med_spread).rjust(10),
        ]
        print(" ".join(row))

    print("\nBLOCK MINER TAG DETAIL:")
    for miner, miner_races in sorted(grouped.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        winner_text = ", ".join(f"{name}={count}" for name, count in Counter(r.first_pool for r in miner_races).most_common())
        print(f"  {miner}: races={len(miner_races)} winners: {winner_text}")

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
            f"{race.index:>3}. {short_hash(race.prevhash)} {status:<11} "
            f"winner={race.first_pool:<12} arrivals: {arrival_text}{missed_text}"
        )


def print_rankings(pools: Dict[str, PoolState], confirmed_count: int) -> None:
    def msfmt(v: Optional[float]) -> str:
        return "-" if v is None else f"{v:.1f}ms"

    print("\nRANKING  (Median counts wins as 0ms; ChaseMed = median delay on races NOT won, blank if always first)")
    print(f" {'Rk':>2}  {'Pool':<12} {'Median':>9}  {'ChaseMed':>9}  {'Seen':>4}  {'Wins':>4}")

    ranked = [
        (statistics.median(p.all_arrival_offsets), p)
        for p in pools.values()
        if p.all_arrival_offsets
    ]
    for rank, (median_delay, p) in enumerate(sorted(ranked, key=lambda x: x[0]), 1):
        chase_med = statistics.median(p.delays) if p.delays else None
        print(
            f" {rank:>2}  {p.name:<12} {msfmt(median_delay):>9}  {msfmt(chase_med):>9}  "
            f"{f'{p.seen}/{confirmed_count}':>4}  {p.wins:>4}"
        )

    print(
        "\n  high wins + low chase = dominant and fast. High wins + high chase = bimodal "
        "(wins when it wins, far behind otherwise), the signature of a geography/peering effect."
    )



def print_full_timing_table(pools: Dict[str, PoolState], confirmed_count: int) -> None:
    print("\nFULL POOL TIMING:")
    columns = [
        ("Pool", 12),
        ("Wins", 5),
        ("Seen", 7),
        ("Miss", 5),
        ("Avg", 8),
        ("Med", 8),
        ("P95", 8),
        ("Best", 8),
        ("Worst", 8),
        ("Reconn", 7),
        ("Timeout", 7),
        ("Closed", 6),
    ]

    header = " ".join(title.ljust(width) for title, width in columns)
    print(header)
    print("-" * len(header))

    ranked = sorted(
        pools.values(),
        key=lambda p: (
            statistics.median(p.all_arrival_offsets) if p.all_arrival_offsets else float("inf"),
            p.name,
        ),
    )

    for p in ranked:
        stats = delay_stats(p.all_arrival_offsets)

        row = [
            p.name.ljust(12),
            str(p.wins).rjust(5),
            f"{p.seen}/{confirmed_count}".rjust(7),
            str(p.missed).rjust(5),
            fnum(stats["avg"]).rjust(8),
            fnum(stats["median"]).rjust(8),
            fnum(stats["p95"]).rjust(8),
            fnum(stats["best"]).rjust(8),
            fnum(stats["worst"]).rjust(8),
            str(p.reconnects).rjust(7),
            str(p.read_timeouts).rjust(7),
            str(p.remote_closes).rjust(6),
        ]

        print(" ".join(row))


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
        "baseline_timeout_seconds": getattr(args, "baseline_timeout", DEFAULT_BASELINE_TIMEOUT),
        "shutdown_grace_seconds": SHUTDOWN_GRACE,
        "tag_block_miners": bool(getattr(args, "tag_block_miners", False)),
        "block_miner_lookup_timeout_seconds": BLOCK_MINER_LOOKUP_TIMEOUT,
        "block_miner_lookup_source": MEMPOOL_API_BASE,
        "pool_count": len(pool_configs),
        "pools": [asdict(pc) for pc in pool_configs],
    }


def pool_summary_dict(p: PoolState) -> Dict[str, Any]:
    stats = delay_stats(p.all_arrival_offsets)
    nonwin_stats = delay_stats(p.delays)
    return {
        "name": p.name,
        "host": p.host,
        "port": p.port,
        "excluded_at_baseline": p.excluded_at_baseline,
        "exclude_reason": p.exclude_reason,
        "auth_failed": p.auth_failed,
        "auth_error": p.auth_error,
        "subscribe_failed": p.subscribe_failed,
        "subscribe_error": p.subscribe_error,
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
        "prevhash_short": short_hash(r.prevhash),
        "block_height": r.block_height,
        "block_miner": r.block_miner,
        "block_miner_source": r.block_miner_source,
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
            "pool", "host", "port", "excluded_at_baseline", "exclude_reason",
            "auth_failed", "auth_error", "wins", "losses", "seen", "missed",
            "avg_ms", "median_ms", "p95_ms", "stddev_ms", "best_ms", "worst_ms",
            "chase_median_ms", "chase_avg_ms", "chase_p95_ms",
            "unmatched", "stale", "unstable", "reconnects", "read_timeouts",
            "remote_closes", "connect_timeouts", "connect_errors", "notify_total",
            "clean_true", "clean_false", "noise_repeats", "noise_prevhash_changes",
            "parse_errors", "bad_notify",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for p in pools.values():
            stats = delay_stats(p.all_arrival_offsets)
            chase = delay_stats(p.delays)
            writer.writerow({
                "pool": p.name,
                "host": p.host,
                "port": p.port,
                "excluded_at_baseline": p.excluded_at_baseline,
                "exclude_reason": p.exclude_reason,
                "auth_failed": p.auth_failed,
                "auth_error": p.auth_error,
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
                "chase_median_ms": chase["median"],
                "chase_avg_ms": chase["avg"],
                "chase_p95_ms": chase["p95"],
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
            "index", "prevhash", "block_height", "block_miner", "confirmed", "winner", "first_wall",
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
                    "block_height": r.block_height,
                    "block_miner": r.block_miner,
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
    verbose: bool = False,
    debug: bool = False,
    full_timing: bool = False,
) -> None:
    confirmed = [r for r in races if r.confirmed]
    unconfirmed = [r for r in races if not r.confirmed]

    print("\n================ SUMMARY ================\n")
    print("Credit: @proofofmike / ProofOfMike.com")
    print(f"Run duration: {duration}s")
    print(f"Confirmed races: {len(confirmed)}")
    print(f"Unconfirmed provisional races: {len(unconfirmed)}")

    if len(confirmed) < MIN_DIRECTIONAL_RACES:
        print(
            f"WARNING: only {len(confirmed)} confirmed races. Treat this as directional, "
            f"not statistically final. Suggested minimum: {MIN_DIRECTIONAL_RACES}+ races."
        )

    excluded = [p for p in pools.values() if p.excluded_at_baseline]
    print("\nEXCLUDED AT BASELINE (not part of the race):")
    if not excluded:
        print("  none")
    else:
        for p in sorted(excluded, key=lambda x: x.name):
            print(f"  {p.name:<12} {p.exclude_reason}")

    auth_problems = [p for p in pools.values() if p.auth_failed or p.subscribe_failed]
    print("\nAUTH / SUBSCRIBE ISSUES:")
    if not auth_problems:
        print("  none")
    else:
        for p in sorted(auth_problems, key=lambda x: x.name):
            if p.auth_failed:
                print(f"  {p.name:<12} authorize rejected: {p.auth_error}")
            if p.subscribe_failed:
                print(f"  {p.name:<12} subscribe rejected: {p.subscribe_error}")

    print_rankings(pools, len(confirmed))

    if full_timing:
        print_full_timing_table(pools, len(confirmed))

    problem_pools = [
        p for p in pools.values()
        if p.reconnects or p.read_timeouts or p.remote_closes or p.connect_timeouts or p.connect_errors
    ]

    print("\nCONNECTION ISSUES:")
    if not problem_pools:
        print("  none")
    else:
        for p in sorted(problem_pools, key=lambda x: (-x.reconnects, -x.read_timeouts, x.name)):
            parts = []
            if p.reconnects:
                parts.append(f"reconnects={p.reconnects}")
            if p.read_timeouts:
                parts.append(f"timeouts={p.read_timeouts}")
            if p.remote_closes:
                parts.append(f"closed={p.remote_closes}")
            if p.connect_timeouts:
                parts.append(f"connect_timeouts={p.connect_timeouts}")
            if p.connect_errors:
                parts.append(f"connect_errors={p.connect_errors}")
            print(f"  {p.name:<12} " + " ".join(parts))

    selected = confirmed[-race_limit:] if race_limit and race_limit > 0 else confirmed

    print("\nPER-RACE TIMING:")
    if not selected:
        print("  none")
    else:
        for race in selected:
            arrivals = sorted(race.arrival_offsets_ms().items(), key=lambda kv: kv[1])
            top = ", ".join(f"{name} +{delay:.1f}ms" for name, delay in arrivals[:3])
            more = f" (+{len(arrivals) - 3} more)" if len(arrivals) > 3 else ""
            miner = race.block_miner or ""
            miner_text = f" mined_by={miner:<18} " if miner else " "
            height_text = f"height={race.block_height} " if race.block_height is not None else ""
            print(
                f"  {race.index:>3}. {height_text}{short_hash(race.prevhash)}{miner_text}winner={race.first_pool:<12} "
                f"{top}{more}"
            )

    print_block_miner_summary(confirmed)

    print("\nNote: winner means first observed by this client/vantage point, not global propagation proof.")
    print(
        "Note: timing resolution is bounded by event-loop scheduling and TCP read buffering. "
        "Full-precision values are kept in the CSV/JSON for analysis, but treat sub-millisecond "
        "differences in any single race as noise, not signal."
    )

    if verbose:
        print("\nFULL POOL TABLE:")
        print_pool_table(pools)
        print_race_detail(races, limit=race_limit)

    if debug:
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

        print("\nDEBUG ARRIVAL OFFSETS INCLUDING WINS (raw ms; sub-ms is noise):")
        for pool_name, p in pools.items():
            print(pool_name, p.all_arrival_offsets)

        print("\nRUNTIME:")
        print(f"  started_local = {meta['started_local']}")
        print(f"  started_utc   = {meta['started_utc']}")
        print(f"  python        = {meta['python']}")
        print(f"  platform      = {meta['platform']}")
        print(f"  client        = {CLIENT_VERSION}")
        print(f"  confirm_window={CONFIRM_WINDOW}s read_timeout={READ_TIMEOUT}s reconnect_delay={RECONNECT_DELAY}s")

        print("\nDEBUG NOTES:")
        print("  timestamp  = asyncio event-loop time taken immediately after readline() returns")
        print("  baseline   = first notify after connect/reconnect, clean=true OR clean=false")
        print("  race       = only clean=true + prevhash changed")
        print("  seen       = confirmed races where this pool's notify arrived inside the window")
        print("  miss       = confirmed races this pool was eligible for but did not match inside the window")
        print("  chase      = median/avg/p95 arrival delay on races this pool did NOT win (wins excluded)")
        print("  baseline   = full consensus if all pools agree by --baseline-timeout, else majority quorum + exclusions")
        print("  blockhash  = stratum prevhash transformed to canonical explorer hash; height/miner are post-run lookups")
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
            pool.auth_failed = False
            pool.auth_error = None
            pool.subscribe_failed = False
            pool.subscribe_error = None
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

                # Responses to our subscribe (id 1) / authorize (id 2) have no method.
                if method is None:
                    mid = msg.get("id")
                    err = msg.get("error")
                    res = msg.get("result")
                    if mid == 2 and (err is not None or res is False):
                        if not pool.auth_failed:
                            pool.auth_failed = True
                            pool.auth_error = (
                                format_stratum_error(err) if err is not None else "authorize result=false"
                            )
                            _print(name, f"authorize rejected by pool: {pool.auth_error}")
                    elif mid == 1 and (err is not None or res is False):
                        if not pool.subscribe_failed:
                            pool.subscribe_failed = True
                            pool.subscribe_error = (
                                format_stratum_error(err) if err is not None else "subscribe result=false"
                            )
                            _print(name, f"subscribe rejected by pool: {pool.subscribe_error}")
                    continue

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

                # The stratum prevhash is byte-mangled and is NOT the explorer hash.
                # Transform once at ingest so prevhash is the canonical block hash
                # everywhere: matching, logging, and the post-run mempool lookup.
                try:
                    prevhash = stratum_prevhash_to_blockhash(params[1])
                except ValueError:
                    pool.bad_notify += 1
                    continue

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
        return [PoolConfig(name, host, port) for name, host, port in DEFAULT_POOLS]

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
        pc.name: PoolState(name=pc.name, host=pc.host, port=pc.port, user=args.user)
        for pc in pool_configs
    }

    tracker = RaceTracker(baseline_timeout=args.baseline_timeout)
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
        # Workers may be parked in readline() up to READ_TIMEOUT; give them a short
        # grace to exit cleanly, then cancel the stragglers so shutdown is bounded.
        _, pending = await asyncio.wait(tasks, timeout=SHUTDOWN_GRACE)
        for t in pending:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        tracker.finalize(pools)

    if args.tag_block_miners:
        await enrich_races_with_block_miners(tracker.all_races)

    print_final_report(
        pools=pools,
        races=tracker.all_races,
        duration=args.duration,
        meta=meta,
        race_limit=args.race_limit,
        verbose=args.verbose,
        debug=args.debug,
        full_timing=args.full_timing,
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
    parser.add_argument(
        "--baseline-timeout", type=float, default=DEFAULT_BASELINE_TIMEOUT,
        help="Seconds to wait for all pools to baseline before falling back to a majority quorum and excluding laggards",
    )
    parser.add_argument("--pools", help="Optional pools.json file. List of {name, host, port}")
    parser.add_argument("--json-out", help="Write structured JSON results to this path")
    parser.add_argument("--csv-out", help="Write pool/race CSV results. If path ends .csv, race CSV appends _races.csv")
    parser.add_argument("--race-limit", type=int, default=0, help="Limit per-race detail printed; 0 prints all races")
    parser.add_argument("--verbose", action="store_true", help="Print full pool table and full per-race detail")
    parser.add_argument("--full-timing", action="store_true", help="Print compact timing table for all pools")
    parser.add_argument("--tag-block-miners", action="store_true", help="After timing stops, look up block height/miner tags from mempool.space and include them in the report/export")
    parser.add_argument("--debug", action="store_true", help="Print connection detail, runtime info, and raw timing arrays")
    parser.add_argument("--probe-interval", type=int, default=0, help="Accepted for compatibility, ignored")
    parser.add_argument("--no-ping", action="store_true", help="Accepted for compatibility, ignored")
    args = parser.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
