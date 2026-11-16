"""Transactional single-host work leasing backed by a dedicated SQLite file."""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class KeyConflict(ValueError):
    """An existing idempotency key names different work."""


def seconds(value: float, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("duration must be numeric")
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError("duration must be finite and in range")
    return float(value)


class Queue:
    def __init__(self, database: str | Path, *, clock: Callable[[], float] = time.time):
        self.database = str(database)
        if self.database == ":memory:":
            raise ValueError("a persistent database path is required")
        self.clock = clock
        with self.transaction() as db:
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if tables and tables != {"leasequeue_jobs", "leasequeue_meta"}:
                raise ValueError("database must be dedicated to leasequeue")
            db.execute("CREATE TABLE IF NOT EXISTS leasequeue_meta(version INTEGER)")
            versions = list(db.execute("SELECT version FROM leasequeue_meta"))
            if not versions:
                db.execute("INSERT INTO leasequeue_meta VALUES (1)")
            elif [row[0] for row in versions] != [1]:
                raise ValueError("unsupported queue schema")
            db.execute("""CREATE TABLE IF NOT EXISTS leasequeue_jobs (
                id INTEGER PRIMARY KEY,
                key TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('ready','leased','done','dead')),
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL,
                available_at REAL NOT NULL,
                lease_until REAL,
                receipt TEXT,
                created_at REAL NOT NULL
            )""")
            db.execute("""CREATE INDEX IF NOT EXISTS leasequeue_ready
                ON leasequeue_jobs(state, available_at, id)""")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.database, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def now(self) -> float:
        value = self.clock()
        if not math.isfinite(value):
            raise ValueError("clock must be finite")
        return value

    def enqueue(
        self, key: str, payload: dict[str, Any], *, max_attempts: int = 3
    ) -> int:
        if not isinstance(key, str) or not key.strip() or len(key) > 200:
            raise ValueError("key must have 1 to 200 characters")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 1000:
            raise ValueError("max_attempts must be an integer from 1 to 1000")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        try:
            encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as error:
            raise ValueError("payload must be JSON serializable") from error
        if len(encoded.encode()) > 1024 * 1024:
            raise ValueError("payload exceeds 1 MiB")
        with self.transaction() as db:
            existing = db.execute(
                "SELECT * FROM leasequeue_jobs WHERE key=?", (key,)
            ).fetchone()
            if existing:
                if (
                    existing["payload"] != encoded
                    or existing["max_attempts"] != max_attempts
                ):
                    raise KeyConflict("idempotency key conflict")
                return existing["id"]
            now = self.now()
            row = db.execute(
                """INSERT INTO leasequeue_jobs
                (key,payload,state,max_attempts,available_at,created_at)
                VALUES (?,?,'ready',?,?,?)""",
                (key, encoded, max_attempts, now, now),
            )
            return row.lastrowid

    def claim(self, *, lease_seconds: float = 60) -> dict[str, Any] | None:
        duration = seconds(lease_seconds, positive=True)
        with self.transaction() as db:
            now = self.now()
            if not math.isfinite(now + duration):
                raise ValueError("lease expiry overflow")
            db.execute(
                """UPDATE leasequeue_jobs SET
                state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
                receipt=NULL,lease_until=NULL,available_at=?
                WHERE state='leased' AND lease_until<=?""",
                (now, now),
            )
            job = db.execute(
                """SELECT id FROM leasequeue_jobs
                WHERE state='ready' AND available_at<=? ORDER BY id LIMIT 1""",
                (now,),
            ).fetchone()
            if job is None:
                return None
            db.execute(
                """UPDATE leasequeue_jobs SET state='leased',
                attempts=attempts+1,receipt=?,lease_until=? WHERE id=?""",
                (uuid.uuid4().hex, now + duration, job["id"]),
            )
            return self.read(db, job["id"], receipt=True)

    def finish(
        self, job_id: int, receipt: str, *, fail: bool = False, delay: float = 0
    ) -> bool:
        if (
            type(job_id) is not int
            or job_id < 1
            or not isinstance(receipt, str)
            or not receipt
        ):
            raise ValueError("valid job id and receipt required")
        delay = seconds(delay)
        with self.transaction() as db:
            now = self.now()
            if not math.isfinite(now + delay):
                raise ValueError("retry time overflow")
            state = (
                "CASE WHEN attempts>=max_attempts THEN 'dead' ELSE 'ready' END"
                if fail
                else "'done'"
            )
            changed = db.execute(
                f"""UPDATE leasequeue_jobs SET state={state},
                receipt=NULL,lease_until=NULL,available_at=?
                WHERE id=? AND state='leased' AND receipt=? AND lease_until>?""",
                (now + delay, job_id, receipt, now),
            )
            return changed.rowcount == 1

    @staticmethod
    def read(db, job_id: int, *, receipt: bool = False) -> dict[str, Any] | None:
        row = db.execute(
            "SELECT * FROM leasequeue_jobs WHERE id=?", (job_id,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        if not receipt:
            result.pop("receipt")
        return result

    def get(self, job_id: int) -> dict[str, Any] | None:
        with self.transaction() as db:
            return self.read(db, job_id)

    def stats(self) -> dict[str, int]:
        with self.transaction() as db:
            counts = {state: 0 for state in ("ready", "leased", "done", "dead")}
            counts.update(
                {
                    row[0]: row[1]
                    for row in db.execute(
                        "SELECT state,COUNT(*) FROM leasequeue_jobs GROUP BY state"
                    )
                }
            )
            return counts
