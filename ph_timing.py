#!/usr/bin/env python3
"""
ph_timing.py

Single-pool Stratum initial work timing test.

Usage:
    python3 ph_timing.py pool_address port --worker worker.name

Example:
    python3 ph_timing.py solo.ckpool.org 3333 --worker bc1qexampleaddress.ph_timing

Purpose:
    Measures how long it takes a Stratum pool to deliver the first usable
    mining.notify job after connection.

Created by @proofofmike
https://github.com/proofofmike/mining-tools
"""

import argparse
import json
import socket
import sys
import time
from datetime import datetime, timezone


VERSION = "0.2.0"
DEFAULT_PASSWORD = "x"
DEFAULT_TIMEOUT = 15


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


def now_ns() -> int:
    return time.monotonic_ns()


def ms_between(start_ns: int, end_ns: int) -> float:
    return (end_ns - start_ns) / 1_000_000


def short_hash(value: str) -> str:
    if not isinstance(value, str):
        return str(value)

    if len(value) <= 20:
        return value

    return f"{value[:8]}…{value[-8:]}"


def send_json(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj, separators=(",", ":")) + "\n"
    sock.sendall(payload.encode("utf-8"))


def recv_json_lines(sock: socket.socket):
    buffer = b""

    while True:
        chunk = sock.recv(4096)

        if not chunk:
            return

        buffer += chunk

        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            line = line.strip()

            if not line:
                continue

            try:
                yield json.loads(line.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                yield {
                    "_invalid_json": line.decode("utf-8", errors="replace")
                }


def parse_notify(msg: dict):
    params = msg.get("params")

    if not isinstance(params, list) or len(params) < 9:
        return None

    return {
        "job_id": params[0],
        "prevhash": params[1],
        "coinbase1": params[2],
        "coinbase2": params[3],
        "merkle_branch": params[4],
        "version": params[5],
        "nbits": params[6],
        "ntime": params[7],
        "clean_jobs": params[8],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Single-pool Stratum initial work timing test by @proofofmike"
    )

    parser.add_argument("pool_address", help="Stratum pool hostname or IP")
    parser.add_argument("port", type=int, help="Stratum pool port")

    parser.add_argument(
        "--worker",
        required=True,
        help="Worker name / payout address. Many pools require a valid BTC address.",
    )

    parser.add_argument(
        "--password",
        default=DEFAULT_PASSWORD,
        help=f"Stratum password. Default: {DEFAULT_PASSWORD}",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Socket timeout in seconds. Default: {DEFAULT_TIMEOUT}",
    )

    args = parser.parse_args()

    pool = args.pool_address
    port = args.port

    start_ns = now_ns()
    connect_done_ns = None
    first_notify_ns = None

    subscribe_result = None
    authorize_result = None
    notify_count = 0

    print("================ START ================")
    print(f"Tool: ph_timing.py v{VERSION}")
    print("Author: @proofofmike")
    print(f"Pool: {pool}:{port}")
    print(f"Worker: {args.worker}")
    print("Test: time to first usable mining.notify")
    print("=======================================")
    print()

    try:
        print(f"[{utc_ts()}] connecting to {pool}:{port}")

        with socket.create_connection((pool, port), timeout=args.timeout) as sock:
            connect_done_ns = now_ns()
            sock.settimeout(args.timeout)

            print(f"[{utc_ts()}] TCP connected")

            send_json(
                sock,
                {
                    "id": 1,
                    "method": "mining.subscribe",
                    "params": ["ph_timing.py"],
                },
            )

            send_json(
                sock,
                {
                    "id": 2,
                    "method": "mining.authorize",
                    "params": [args.worker, args.password],
                },
            )

            print(f"[{utc_ts()}] subscribe and authorize sent")
            print(f"[{utc_ts()}] waiting for first mining.notify")
            print()

            for msg in recv_json_lines(sock):
                if "_invalid_json" in msg:
                    print(f"[{utc_ts()}] invalid JSON: {msg['_invalid_json'][:200]}")
                    continue

                msg_id = msg.get("id")
                method = msg.get("method")

                if msg_id == 1:
                    subscribe_result = msg.get("result")
                    if msg.get("error"):
                        print(f"[{utc_ts()}] subscribe error: {msg.get('error')}")
                    else:
                        print(f"[{utc_ts()}] subscribe response received")
                    continue

                if msg_id == 2:
                    authorize_result = msg.get("result")
                    if msg.get("error"):
                        print(f"[{utc_ts()}] authorize error: {msg.get('error')}")
                    else:
                        print(f"[{utc_ts()}] authorize result: {authorize_result}")
                    continue

                if method != "mining.notify":
                    continue

                notify = parse_notify(msg)

                if notify is None:
                    print(f"[{utc_ts()}] malformed mining.notify")
                    continue

                first_notify_ns = now_ns()
                notify_count += 1

                tcp_ms = ms_between(start_ns, connect_done_ns)
                work_after_tcp_ms = ms_between(connect_done_ns, first_notify_ns)
                total_ms = ms_between(start_ns, first_notify_ns)

                print("================ RESULT ================")
                print(f"Pool: {pool}:{port}")
                print(f"TCP connect time: {tcp_ms:.2f} ms")
                print(f"Initial work after TCP connect: {work_after_tcp_ms:.2f} ms")
                print(f"Total time to initial work: {total_ms:.2f} ms")
                print()
                print(f"Job ID: {notify['job_id']}")
                print(f"Prevhash: {short_hash(notify['prevhash'])}")
                print(f"Clean jobs: {notify['clean_jobs']}")
                print(f"nTime: {notify['ntime']}")
                print(f"nBits: {notify['nbits']}")
                print()
                print(f"Subscribe result received: {subscribe_result is not None}")
                print(f"Authorize result before first work: {authorize_result}")
                print("========================================")

                return 0

            print("Connection closed before receiving mining.notify", file=sys.stderr)
            return 6

    except KeyboardInterrupt:
        print()
        print("Interrupted by user.")
        return 130

    except socket.gaierror as e:
        print(f"DNS/host error for {pool}: {e}", file=sys.stderr)
        return 2

    except socket.timeout:
        print(f"Timed out waiting for initial work from {pool}:{port}", file=sys.stderr)
        return 3

    except ConnectionRefusedError:
        print(f"Connection refused by {pool}:{port}", file=sys.stderr)
        return 4

    except OSError as e:
        print(f"Socket error: {e}", file=sys.stderr)
        return 5

    finally:
        if first_notify_ns is None:
            print()
            print("================ SUMMARY ================")
            print(f"Pool: {pool}:{port}")

            if connect_done_ns is not None:
                print(f"TCP connect time: {ms_between(start_ns, connect_done_ns):.2f} ms")
            else:
                print("TCP connect time: failed")

            print(f"Subscribe result received: {subscribe_result is not None}")
            print(f"Authorize result before first work: {authorize_result}")
            print(f"mining.notify messages observed: {notify_count}")
            print("Initial work received: no")
            print("=========================================")


if __name__ == "__main__":
    raise SystemExit(main())
