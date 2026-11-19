import contextlib
import io
import json
import math
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from leasequeue.cli import main
from leasequeue.queue import KeyConflict, Queue


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "queue.db"
        self.clock = 1000.0
        self.q = Queue(self.path, clock=lambda: self.clock)

    def test_persistence_across_instances_and_fifo(self):
        first = self.q.enqueue("a", {"task": "evaluate"})
        second = self.q.enqueue("b", {})
        other = Queue(self.path, clock=lambda: self.clock)
        self.assertEqual(other.claim()["id"], first)
        self.assertEqual(other.claim()["id"], second)
        self.assertIsNone(other.claim())
        self.assertEqual(other.stats(), {"ready": 0, "leased": 2, "done": 0, "dead": 0})

    def test_same_key_is_idempotent_with_canonical_object_order(self):
        job = self.q.enqueue("same", {"a": 1, "b": 2})
        self.assertEqual(self.q.enqueue("same", {"b": 2, "a": 1}), job)
        self.assertEqual(self.q.stats()["ready"], 1)

    def test_key_conflict_cannot_overwrite_payload_or_attempt_policy(self):
        job = self.q.enqueue("same", {"a": 1})
        for payload, attempts in (({"a": 2}, 3), ({"a": 1}, 4)):
            with self.assertRaises(KeyConflict):
                self.q.enqueue("same", payload, max_attempts=attempts)
        self.assertEqual(self.q.get(job)["payload"], {"a": 1})

    def test_acknowledgement_requires_current_unexpired_receipt(self):
        job = self.q.enqueue("a", {})
        lease = self.q.claim(lease_seconds=10)
        self.assertFalse(self.q.finish(job, "wrong"))
        self.clock += 10
        self.assertFalse(self.q.finish(job, lease["receipt"]))
        replacement = self.q.claim()
        self.assertNotEqual(replacement["receipt"], lease["receipt"])
        self.assertEqual(replacement["attempts"], 2)
        self.assertFalse(self.q.finish(job, lease["receipt"]))
        self.assertTrue(self.q.finish(job, replacement["receipt"]))
        self.assertFalse(self.q.finish(job, replacement["receipt"]))
        self.assertEqual(self.q.get(job)["state"], "done")

    def test_expired_worker_cannot_fail_replacement(self):
        job = self.q.enqueue("a", {})
        old = self.q.claim(lease_seconds=1)
        self.clock += 2
        new = self.q.claim()
        self.assertFalse(self.q.finish(job, old["receipt"], fail=True))
        self.assertTrue(self.q.finish(job, new["receipt"]))

    def test_expiration_exhausts_attempt_budget(self):
        job = self.q.enqueue("a", {}, max_attempts=1)
        self.q.claim(lease_seconds=1)
        self.clock += 1
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.get(job)["state"], "dead")
        self.assertEqual(self.q.stats()["dead"], 1)

    def test_failure_delay_and_terminal_failure(self):
        job = self.q.enqueue("a", {}, max_attempts=2)
        first = self.q.claim()
        self.assertTrue(self.q.finish(job, first["receipt"], fail=True, delay=20))
        self.assertIsNone(self.q.claim())
        self.clock += 20
        second = self.q.claim()
        self.assertEqual(second["attempts"], 2)
        self.assertTrue(self.q.finish(job, second["receipt"], fail=True))
        self.assertEqual(self.q.get(job)["state"], "dead")
        self.assertIsNone(self.q.claim())

    def test_completed_key_is_not_resurrected_by_enqueue(self):
        job = self.q.enqueue("a", {})
        lease = self.q.claim()
        self.q.finish(job, lease["receipt"])
        self.assertEqual(self.q.enqueue("a", {}), job)
        self.assertIsNone(self.q.claim())

    def test_delayed_old_job_does_not_block_other_ready_work(self):
        first = self.q.enqueue("a", {})
        lease = self.q.claim()
        self.q.finish(first, lease["receipt"], fail=True, delay=100)
        second = self.q.enqueue("b", {})
        self.assertEqual(self.q.claim()["id"], second)

    def test_inspection_does_not_return_receipt_or_reap(self):
        job = self.q.enqueue("a", {})
        self.q.claim(lease_seconds=1)
        self.clock += 2
        self.assertNotIn("receipt", self.q.get(job))
        self.assertEqual(self.q.stats()["leased"], 1)
        self.assertIsNone(self.q.get(999))

    def test_invalid_payload_key_and_attempt_limits(self):
        for key, payload, attempts in (
            ("", {}, 3),
            (" ", {}, 3),
            ("x" * 201, {}, 3),
            ("ok", [], 3),
            ("ok", {"bad": math.nan}, 3),
            ("ok", {"bad": object()}, 3),
            ("ok", {}, True),
            ("ok", {}, 0),
            ("ok", {}, 1001),
            ("ok", {"huge": "x" * (1024 * 1024)}, 3),
        ):
            with self.subTest(key=key[:5], attempts=attempts):
                with self.assertRaises(ValueError):
                    self.q.enqueue(key, payload, max_attempts=attempts)
        self.assertEqual(sum(self.q.stats().values()), 0)

    def test_invalid_durations_cannot_mutate_work(self):
        self.q.enqueue("a", {})
        for duration in (0, -1, math.inf, math.nan, True, "1"):
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                self.q.claim(lease_seconds=duration)
        lease = self.q.claim()
        for delay in (-1, math.inf, math.nan, True):
            with self.assertRaises(ValueError):
                self.q.finish(lease["id"], lease["receipt"], fail=True, delay=delay)
        self.assertEqual(self.q.get(lease["id"])["state"], "leased")

    def test_oversized_numeric_arguments_are_rejected_without_mutation(self):
        job = self.q.enqueue("a", {})
        with self.assertRaises(ValueError):
            self.q.claim(lease_seconds=10**400)
        lease = self.q.claim()
        with self.assertRaises(ValueError):
            self.q.finish(job, lease["receipt"], fail=True, delay=10**400)
        with self.assertRaises(ValueError):
            self.q.finish(2**63, lease["receipt"])
        with self.assertRaises(ValueError):
            self.q.get(2**63)
        self.assertEqual(self.q.get(job)["state"], "leased")
        self.assertTrue(self.q.finish(job, lease["receipt"]))

    def test_nonfinite_clock_and_expiry_overflow_roll_back(self):
        self.q.enqueue("a", {})
        self.clock = math.nan
        with self.assertRaises(ValueError):
            self.q.claim()
        self.clock = 1e308
        with self.assertRaises(ValueError):
            self.q.claim(lease_seconds=1e308)
        self.assertEqual(self.q.stats()["ready"], 1)

    def test_unrelated_database_is_preserved(self):
        other = Path(self.temp.name) / "other.db"
        with sqlite3.connect(other) as db:
            db.execute("CREATE TABLE important(value)")
            db.execute("INSERT INTO important VALUES ('kept')")
        with self.assertRaises(ValueError):
            Queue(other)
        with sqlite3.connect(other) as db:
            self.assertEqual(
                db.execute("SELECT * FROM important").fetchall(), [("kept",)]
            )

    def test_future_schema_rejected(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE leasequeue_meta SET version=2")
        with self.assertRaises(ValueError):
            Queue(self.path)

    def test_transaction_failure_rolls_back(self):
        with self.assertRaises(RuntimeError):
            with self.q.transaction() as db:
                db.execute("DELETE FROM leasequeue_meta")
                raise RuntimeError("failure")
        Queue(self.path)

    def test_thread_contenders_do_not_double_claim(self):
        for i in range(20):
            self.q.enqueue(str(i), {})
        with ThreadPoolExecutor(max_workers=8) as pool:
            leases = list(pool.map(lambda _: Queue(self.path).claim(), range(30)))
        ids = [lease["id"] for lease in leases if lease is not None]
        self.assertEqual(len(ids), 20)
        self.assertEqual(len(set(ids)), 20)

    def test_process_contenders_do_not_double_claim(self):
        for i in range(6):
            self.q.enqueue(str(i), {})
        script = (
            "import sys,json; from leasequeue.queue import Queue; "
            "print(json.dumps(Queue(sys.argv[1]).claim()))"
        )
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(self.path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=os.environ.copy(),
            )
            for _ in range(8)
        ]
        leases = []
        try:
            for process in processes:
                out, err = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, err)
                leases.append(json.loads(out))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        ids = [lease["id"] for lease in leases if lease is not None]
        self.assertEqual(len(ids), 6)
        self.assertEqual(len(set(ids)), 6)

    def test_concurrent_enqueue_keeps_one_row(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(
                pool.map(lambda _: Queue(self.path).enqueue("shared", {}), range(12))
            )
        self.assertEqual(len(set(ids)), 1)
        self.assertEqual(self.q.stats()["ready"], 1)


class CliTests(unittest.TestCase):
    def test_json_cli_lifecycle_and_stale_receipt_status(self):
        with tempfile.TemporaryDirectory() as folder:
            base = ["--db", str(Path(folder) / "jobs.db")]

            def run(args):
                stream = io.StringIO()
                with contextlib.redirect_stdout(stream):
                    code = main(base + args)
                return code, json.loads(stream.getvalue())

            code, job = run(
                ["enqueue", "--key", "example", "--payload", '{"task":"test"}']
            )
            self.assertEqual(code, 0)
            code, lease = run(["claim"])
            self.assertEqual(lease["id"], job["id"])
            self.assertEqual(
                run(["ack", "--id", str(job["id"]), "--receipt", "stale"]),
                (1, {"updated": False}),
            )
            self.assertEqual(
                run(["ack", "--id", str(job["id"]), "--receipt", lease["receipt"]]),
                (0, {"updated": True}),
            )
            self.assertEqual(run(["stats"])[1]["done"], 1)
            self.assertEqual(run(["claim"]), (0, None))

    def test_invalid_json_and_conflicting_key_have_distinct_exit_codes(self):
        with tempfile.TemporaryDirectory() as folder:
            base = [
                "--db",
                str(Path(folder) / "jobs.db"),
                "enqueue",
                "--key",
                "test",
                "--payload",
            ]
            with (
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(main(base + ["bad json"]), 2)
                self.assertEqual(main(base + ['{"a":1}']), 0)
                self.assertEqual(main(base + ['{"a":2}']), 1)

    def test_oversized_job_ids_report_option_error_without_traceback(self):
        with tempfile.TemporaryDirectory() as folder:
            base = ["--db", str(Path(folder) / "jobs.db")]
            for args in (
                ["show", "--id", str(2**63)],
                ["ack", "--id", str(2**63), "--receipt", "stale"],
            ):
                output, errors = io.StringIO(), io.StringIO()
                with (
                    contextlib.redirect_stdout(output),
                    contextlib.redirect_stderr(errors),
                ):
                    self.assertEqual(main(base + args), 2)
                self.assertEqual(output.getvalue(), "")
                self.assertIn("check database, payload and options", errors.getvalue())

    def test_memory_database_rejected(self):
        with self.assertRaises(ValueError):
            Queue(":memory:")
