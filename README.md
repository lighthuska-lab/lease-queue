# lease-queue

A small, single-host SQLite queue for local evaluation jobs, data preparation,
and backend worker scripts. Enqueue JSON work once per idempotency key, claim
it under an expiring lease, then acknowledge or retry it. A new receipt on
every claim prevents an expired worker from completing a replacement lease.

Python 3.11 or newer; no runtime dependencies or cloud services. This is an
original project maintained by Light Huska. It is not published on PyPI.

## Install and try

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .
leasequeue --db jobs.db enqueue --key sample-1 --payload '{"task":"evaluate","dataset":"synthetic"}'
leasequeue --db jobs.db claim --lease-seconds 60
```

The claim prints a JSON object with its `id`, payload, attempt number,
`lease_until` timestamp and `receipt`. Run your own handler, then use the
returned values (the receipt below is a placeholder):

```bash
leasequeue --db jobs.db ack --id 1 --receipt RECEIPT_FROM_CLAIM
# On a recoverable handler failure instead:
leasequeue --db jobs.db fail --id 1 --receipt RECEIPT_FROM_CLAIM --delay 10
leasequeue --db jobs.db stats
leasequeue --db jobs.db show --id 1
```

Acknowledgement and failure are alternatives for the current claim. The queue
does not execute Python, shell commands, HTTP calls or anything in the payload.
Do not copy the placeholder receipt literally. Repeating an enqueue key with
the same JSON serialization and attempt limit returns the same job ID, including
after completion. Different work under that key fails instead of overwriting it.

## Python worker integration

```python
from leasequeue.queue import Queue

queue = Queue("jobs.db")
queue.enqueue("evaluate:synthetic:v1", {"dataset": "synthetic"})
job = queue.claim(lease_seconds=60)
if job is not None:
    # Your handler runs here, outside the database transaction.
    # Make its side effects independently idempotent.
    committed = queue.finish(job["id"], job["receipt"])
    if not committed:
        print("Lease expired or replaced; completion was rejected.")
```

`finish(..., fail=True, delay=10)` schedules another attempt after a failure.
The default budget is three claims, configurable at enqueue from 1 to 1,000.
Claims, including ones abandoned by a crashed process, count toward the budget.

## Contract and boundaries

- SQLite `BEGIN IMMEDIATE` serializes claim/update transactions. Separate
  processes on the same host cannot receive the same *current* lease. Handlers
  run outside the lock. Busy operations wait up to five seconds, then fail.
- Ready jobs are claimed in insertion order among jobs whose delay has elapsed.
  A delayed job does not block later ready work. Claims lazily reclaim expired
  leases, or mark them dead once their attempt budget is exhausted.
- A lease is valid strictly before `lease_until`. At the exact expiry boundary,
  neither acknowledgement nor failure can update the job. Every re-claim gets
  a fresh receipt. Terminal jobs are retained; there is no automatic purge.
- **Delivery can repeat. This is not exactly-once execution.** A slow worker
  may still be running after its lease expires. The receipt fences queue state,
  not external side effects. Use your own idempotency keys or fencing at the
  downstream system. No renewal/heartbeat API is included in this version.
- Expiry uses wall-clock Unix seconds; system clock changes affect leases.
  Select leases longer than expected handler duration. There is no priority,
  scheduling daemon, broker protocol, distributed consensus, or network service.
- Use a dedicated database file on a local filesystem, with its parent directory
  already present. In-memory databases and unrelated existing tables are
  rejected. SQLite's default rollback journal is used. Network-mounted storage,
  machine power-loss guarantees and hardware fault recovery are not validated.
- Keys are 1–200 characters. Payloads must be JSON objects; encoded payloads
  above 1 MiB and non-finite numbers are rejected. Object keys are sorted for
  identity comparison; this is serialization identity, not semantic equivalence.
- The database and CLI output contain job payloads and keys. CLI payload
  arguments can appear in shell history/process listings. Use synthetic or
  nonsecret data for demos; receipts are ownership checks, not authentication.
- `show` and `stats` report stored state without reaping expired leases. An
  expired job can still appear leased until the next `claim` operation.

CLI output is JSON. Exit `0` means success (including `null` when there is no
claim or matching job), `1` means a stale receipt or conflicting key, `2` means
invalid input/database failure, and `130` means interruption. Error messages do
not echo payloads. The tool does not create parent directories or run retries
around failed database operations.

## Development and review

```bash
python -m pip install -e . -r requirements-dev.txt
ruff check .
ruff format --check .
python -m unittest discover -s tests -v
python -m build
```

Tests cover concurrent processes and threads, idempotent enqueue/conflicts,
restart persistence, stale acknowledgements/failures, exact expiry boundaries,
retry delays and budgets, schema rejection, rollback, and the CLI lifecycle.
They use synthetic payloads, temporary local files, and no inference/API service.
These are correctness tests, not a performance or durability certification.

Contributors should claim work before implementation, preserve real authorship,
and provide reproducible tests. Independent reviews must be genuine. Main must
retain required CI and fresh approval gates; automation must not approve itself.

## License

MIT. See [LICENSE](LICENSE).
