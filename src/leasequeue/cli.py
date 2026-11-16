"""JSON commands for local worker scripts; never executes job payloads."""

import argparse
import json
import sqlite3
import sys

from leasequeue import __version__
from leasequeue.queue import KeyConflict, Queue


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--db", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    enqueue = commands.add_parser("enqueue")
    enqueue.add_argument("--key", required=True)
    enqueue.add_argument(
        "--payload", required=True, help="JSON object; use synthetic/nonsecret data"
    )
    enqueue.add_argument("--max-attempts", type=int, default=3)
    claim = commands.add_parser("claim")
    claim.add_argument("--lease-seconds", type=float, default=60)
    for command in ("ack", "fail"):
        action = commands.add_parser(command)
        action.add_argument("--id", type=int, required=True)
        action.add_argument("--receipt", required=True)
        if command == "fail":
            action.add_argument("--delay", type=float, default=0)
    show = commands.add_parser("show")
    show.add_argument("--id", type=int, required=True)
    commands.add_parser("stats")
    args = parser.parse_args(argv)
    try:
        queue = Queue(args.db)
        if args.command == "enqueue":
            result = {
                "id": queue.enqueue(
                    args.key, json.loads(args.payload), max_attempts=args.max_attempts
                )
            }
        elif args.command == "claim":
            result = queue.claim(lease_seconds=args.lease_seconds)
        elif args.command in ("ack", "fail"):
            changed = queue.finish(
                args.id,
                args.receipt,
                fail=args.command == "fail",
                delay=getattr(args, "delay", 0),
            )
            print(json.dumps({"updated": changed}))
            return 0 if changed else 1
        elif args.command == "show":
            result = queue.get(args.id)
        else:
            result = queue.stats()
        print(json.dumps(result, allow_nan=False))
        return 0
    except KeyConflict:
        print("Idempotency key already names different work.", file=sys.stderr)
        return 1
    except (ValueError, OSError, sqlite3.Error):
        print(
            "Cannot process command: check database, payload and options.",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        return 130
