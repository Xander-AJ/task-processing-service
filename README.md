# Task Processing Service

[![CI](https://github.com/Xander-AJ/task-processing-service/actions/workflows/ci.yml/badge.svg)](https://github.com/Xander-AJ/task-processing-service/actions/workflows/ci.yml)

A multi-tenant REST API where companies create background tasks and workers process
them without duplicate processing. Built with FastAPI and PostgreSQL.

## Stack

Python 3.12, FastAPI, SQLAlchemy 2.x, PostgreSQL 16, Alembic, pytest.

## Running it

`docker compose up` starts Postgres, runs the migrations, and starts the API on
:8000 plus a polling worker. API docs: http://localhost:8000/docs

The worker polls every second and processes a batch each pass. You can also trigger a
pass on demand with `POST /workers/process`.

## Local dev (without docker for the app)

    pip install -e ".[dev]"       # runtime + dev deps (pytest, ruff, mypy)
    docker compose up db          # just the database
    alembic upgrade head
    uvicorn app.main:app --reload

Dependencies and tooling config live in `pyproject.toml`. Lint and type-check with
`ruff check .` and `mypy`.

## Tests

With the db container running:

    pytest -v

Tests run against a real Postgres `tasks_test` db (created automatically) because
`FOR UPDATE SKIP LOCKED` has no SQLite equivalent.

## API examples

Create a task (with an idempotency key):

    curl -X POST http://localhost:8000/companies/$COMPANY_ID/tasks \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: order-123" \
      -d '{"type": "send_email", "payload": {"to": "a@b.com", "subject": "Hi"}}'

Get one task (taskId comes from the create response):

    curl http://localhost:8000/companies/$COMPANY_ID/tasks/$TASK_ID

List tasks, paginated and filtered:

    curl "http://localhost:8000/companies/$COMPANY_ID/tasks?status=pending&limit=2&offset=0"

Trigger a processing pass:

    curl -X POST http://localhost:8000/workers/process

## Design deep-dives

The bullets above are the quick version. This section is the depth behind a few decisions that were not obvious, for the reviewer who wants the reasoning and the evidence.

### At-least-once semantics

The system guarantees at-least-once processing, not exactly-once. Exactly-once is impossible here because a task's side effect and the status commit to Postgres are not one atomic operation. A handler sends an email through a provider, then commits `completed` to the database; a crash in the gap means the email went out but the row still reads `processing`. Stale-lock recovery later reclaims that row and runs the handler again. Closing the gap would need two-phase commit spanning Postgres and every downstream provider, and email and SMS providers do not enlist in an XA transaction, so the guarantee is not available to buy.

The design contract that follows is one sentence: task handlers must be idempotent. Concretely, a handler that sends an email should tolerate being invoked twice for the same task ID — either by passing the task ID to the provider as an idempotency key, or by recording completion somewhere it can check before re-sending.

I chose at-least-once because the alternative is complexity out of proportion to the value in this workload. The `tasks_released_total` counter, described under graceful shutdown, keeps the duplicate-processing risk visible during the deploys where it is most likely to occur.

### Fair claim: LATERAL top-K per company

The naive claim — `ORDER BY created_at ASC LIMIT N` — starves quiet tenants. One company with a large backlog fills every batch, and a company with two old tasks waits behind ten thousand newer ones. The system exists to serve multiple companies concurrently, so fairness is part of correctness, not a nicety.

The first working version ranked eligible tasks with a global window, `ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at)` in a CTE, and ordered the batch by `(rn, created_at)` so every company's oldest task is claimed before any company's second. That is fair and it passes the fairness tests. The cost is the window: Postgres sorts every eligible row to compute `rn`, then discards all but the first `per_company_cap` of each company. At 100k rows with ~82% eligible, that is an ~80,000-row sort on every claim; at 1M it is ~830,000 rows, spilling roughly 32MB to disk.

I rejected that in favour of a LATERAL top-K. Instead of ranking every eligible row, `claim_tasks()` in `app/services/worker_service.py` walks the distinct eligible companies — bounded by the tenant count, not the row count — and for each pulls only its oldest `per_company_cap` rows:

```sql
CROSS JOIN LATERAL (
    SELECT id, created_at FROM tasks
    WHERE company_id = ec.company_id AND (<eligibility>)
    ORDER BY created_at LIMIT :per_company_cap
) top
```

The reduced set — at most `companies × per_company_cap` rows — is what gets ranked, ordered, and locked. At 50 companies with cap 10, that is at most 500 rows to sort per claim, regardless of backlog size.

| Query                               | 100k tasks | 1M tasks | Sort spilled to disk?                   |
| ----------------------------------- | ---------- | -------- | --------------------------------------- |
| Global ROW_NUMBER window (baseline) | ~85 ms     | ~1097 ms | Yes (3.2MB @ 100k, 32MB @ 1M)           |
| LATERAL top-K per company           | ~50 ms     | ~562 ms  | No (in-memory quicksort at both scales) |

Numbers are medians of three steady-state runs, `EXPLAIN (ANALYZE, BUFFERS, VERBOSE)` with `track_io_timing=on`. The full plans are in `scripts/perf_explain.sql` (current) and `scripts/perf_explain_old.sql` (baseline); the workload is `scripts/perf_seed.py` with the default seed=42 (80/20 flooder skew, 20% backoff-delayed).

The honest limit: `rn` is computed inside an unlocked snapshot, so under concurrent workers two workers can assign a different `rn` to the same row. In the serial case fairness is exact; under N concurrent workers with M eligible companies, the divergence between snapshot `rn` and post-lock reality is bounded by how many rows another worker committed between snapshot read and lock — typically small at portfolio scale, and only ever an ordering effect, never a correctness one. Whether the ranking is exact is separate from whether the same task can be claimed twice — that second property is airtight, and it is the subject of the next subsection.

### The rank-then-lock race and EvalPlanQual

I built the first LATERAL version with the eligibility predicate only inside the candidate CTE — the outer query just joined and took `FOR UPDATE OF t SKIP LOCKED`. Every test passed except one: the high-contention test that runs the claim from ten threads and asserts the union of claimed IDs has no duplicates. It failed roughly one run in four.

Under READ COMMITTED, worker A and worker B build their candidate sets from separate MVCC snapshots and can both include the same row. Worker A locks it, commits the flip to `processing`, and releases the lock. Worker B's snapshot still shows the row as `pending`, and the row is no longer locked, so `SKIP LOCKED` has nothing to skip. Worker B locks it and claims it a second time. `SKIP LOCKED` only helps while a row is still locked; it cannot correct for a stale snapshot after the lock is gone.

The fix repeats the full eligibility predicate on the outer `t`:

```sql
JOIN per_company_top c ON c.id = t.id
WHERE (t.status = 'pending'    AND t.run_after <= :now)
   OR (t.status = 'processing' AND t.locked_at < :stale_before)
FOR UPDATE OF t SKIP LOCKED
```

Repeating it there triggers Postgres's EvalPlanQual: when `FOR UPDATE` reaches a row, Postgres re-fetches the latest committed version and re-evaluates the outer `WHERE` against it. The row that was `pending` in the stale snapshot is now `processing`, fails both branches, and drops out of the lock set. The high-contention test went from failing roughly one run in four to passing 30 consecutive iterations. The general pattern: any rank-then-lock queue on READ COMMITTED needs its eligibility predicate on the outer lock, because `SKIP LOCKED` handles contention but not staleness.

### Migrations as the schema of record

Test suites often build the schema with `Base.metadata.create_all()` and deploy with `alembic upgrade head`. Those are two independent definitions of the schema, and they drift — a column added in a model but not in a migration passes the tests and breaks the deploy. I rejected `create_all` in the tests so that the suite exercises the schema I actually ship.

`tests/conftest.py` runs `DROP SCHEMA public CASCADE; CREATE SCHEMA public;` and then `command.upgrade(cfg, "head")` once per session, with `sqlalchemy.url` overridden to `settings.test_database_url`. The Alembic `env.py` was changed to respect a URL already set on the config rather than overwriting it, so the production CLI still resolves `settings.database_url` while the tests point at their own database.

A provenance test keeps the fixture honest. `tests/test_schema.py::test_schema_built_by_migrations_not_create_all` reads `alembic_version.version_num` and asserts it equals the head revision resolved from Alembic's `ScriptDirectory`. Only a migration run stamps `alembic_version`; `create_all` never does. If someone reverts the fixture to `create_all` for speed, this test fails on the next run.

A drift test keeps the models honest. CI runs `alembic check` on every push and fails when the models and the migration files disagree. It earned its place the first time it ran, catching a partial index that existed in migration 0002 but had never been declared on the model.

Two limits are worth stating. `DROP SCHEMA public CASCADE` assumes a superuser-owned test database — correct for local dev and CI, wrong for managed Postgres where the app role is not superuser. And `alembic check` compares structure only; a data migration is outside its reach.

### Graceful shutdown with task release

I chose to release un-started tasks on shutdown rather than let them drain. A worker that finishes its current batch and exits leaves every already-claimed task locked in `processing` until the stale-recovery window elapses — five minutes — which strands a handful of tasks on every single deploy.

On SIGTERM, `app/worker.py` sets a `threading.Event`, and `process_available_tasks` checks it before starting each task in the batch. If the event is set, the current and remaining un-started tasks are handed back through `release_tasks()`, which resets their status to `pending` and clears `locked_at` and `locked_by` while leaving `retry_count` and `run_after` untouched — these tasks never failed, so they carry no retry penalty. A task already running is never interrupted, because interrupting it would produce exactly the committed-side-effect-without-committed-status case the at-least-once contract is built around.

Every released task increments `tasks_released_total`, so a burst of releases during a deploy is legible rather than silent.

### Observability

`GET /health` and `GET /ready` are split on purpose. Health is liveness: the process is up, it touches no database, it always returns 200. Ready runs `SELECT 1` and returns 503 when the database is unreachable. Collapsing the two means a load balancer pulls traffic on a transient database blip and an orchestrator restarts a pod that would have recovered on its own.

Metrics live on two endpoints because the API and the worker are separate processes. The API's `/metrics` exposes HTTP request metrics and queue gauges derived from the database. The worker runs its own HTTP server on port 9100 exposing claim, processing, and release counters. Each worker replica is its own scrape target; the counters are per-process and Prometheus sums them at query time.

Two choices are worth naming. The queue-depth gauges — `tasks_by_status` and `oldest_eligible_pending_age_seconds` — are computed at scrape time by a custom collector that queries Postgres. The age gauge filters `run_after <= now()`, because a backoff-delayed retry is an intentional wait, not a backlog. The collector catches database errors and yields nothing rather than failing the scrape; a `/metrics` that 500s during a Postgres blip is worse than a gap in the graph. Separately, HTTP request labels use the matched route template, `/companies/{companyId}/tasks`, not the raw path — raw paths carry UUIDs, and UUIDs in a label are how a metrics endpoint acquires unbounded cardinality and takes down the Prometheus scraping it.

I rejected OpenTelemetry for this system. One API, one database, and one polling worker have no cross-service hop worth tracing, and a trace of a single process is decoration. If the system grew a downstream service or a broker, the spans would start in `process_task()` for per-task timing and around the claim query for per-batch timing.

### Known limitations

- A composite `(company_id, created_at)` index would remove the remaining LATERAL cost — the per-company bitmap heap scan and top-N sort. Left as follow-up so the win from the query rewrite and the win from the index stay separately measurable.
- Fairness is verified in serial tests only. Under concurrent workers the `rn` computed in each snapshot can diverge from post-lock reality, so fairness is an approximation. Correctness — no duplicate claims — is tested and holds.
- `alembic check` verifies structural parity of models against migrations but not data-migration correctness; a silently-wrong backfill such as `run_after = created_at` in migration 0002 would pass CI.
- The test fixture's `DROP SCHEMA public CASCADE` requires a superuser-owned test database — fine for local and CI, incompatible with managed Postgres where the app role is not superuser.
- `scripts/` is committed but sits outside CI's mypy scope, so the audit harness could break without CI noticing.
- At 1M rows the plan triggers JIT compilation, roughly 120ms on top of ~440ms of execution. I would tune `jit_above_cost` for this workload before running it at that scale.
