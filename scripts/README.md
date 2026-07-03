# Performance-audit harness

Reproducible tooling for measuring the fair-claim query (`claim_tasks()` in
`app/services/worker_service.py`) under a realistic, skewed multi-tenant backlog.
It captures a **baseline** we can point EXPLAIN plans at before considering any
index or query rewrite. It measures only — it changes no schema and recommends
nothing.

## What it measures and why it exists

`claim_tasks()` uses a `ROW_NUMBER() OVER (PARTITION BY company_id ...)` window plus
`FOR UPDATE SKIP LOCKED` to claim tasks round-robin across companies, so a single
tenant with a huge backlog can't starve the others. That fairness has a cost, and
the cost only shows up at scale with real skew. This harness reproduces that skew
deterministically so the query's plan — index usage, the window sort, the outer
EvalPlanQual re-check, buffer/I/O behaviour — can be inspected and compared across
changes instead of guessed at.

## Workload shape (and the reasoning)

Defaults: **100k** tasks / **50** companies / **80%** flooder / **20%** future
`run_after` / **seed 42**.

- **100,000 pending tasks** — large enough that a sequential scan vs. an index scan
  is a real planner decision, not noise.
- **50 companies** — enough partitions that the window function's per-company
  ranking does meaningful work.
- **80% to one "flooder" company** — the exact starvation scenario the fair-claim
  query defends against; without heavy skew the round-robin ordering is untested.
- **20% future `run_after`** — models backoff-delayed retries that are pending but
  not yet eligible, exercising the `run_after <= now()` gate (and the partial index
  that backs it).
- **`created_at` jittered across the past 24h** — gives `ORDER BY created_at` real
  work rather than a trivially-ordered input.
- **seed 42** — the whole point: same seed ⇒ same company assignment, same task
  UUIDs, same future rows. The seed is printed in the seeder's summary as
  first-class reproducibility evidence. (Absolute timestamps track `now()` at insert
  time; the *distribution* is what's reproducible.)

## Reproducibility — exact command sequence

```bash
# 0. Postgres up (compose db service on localhost:5432, user/pass postgres/postgres)
docker compose up -d db

# 1. Fresh throwaway DB (NOT tasks / tasks_test)
docker compose exec -T db psql -U postgres -c "DROP DATABASE IF EXISTS tasks_perf;"
docker compose exec -T db psql -U postgres -c "CREATE DATABASE tasks_perf;"

# 2. Same schema as production, incl. the partial index
export DATABASE_URL="postgresql+psycopg2://postgres:postgres@localhost:5432/tasks_perf"
alembic upgrade head

# 3. Seed the deterministic workload (100k / 50 / 0.8 / 0.2 / seed 42)
python scripts/perf_seed.py --database-url "$DATABASE_URL" --yes

# 4. Refresh planner stats so the plan reflects production (autovacuum-fresh stats)
docker compose exec -T db psql -U postgres -d tasks_perf -c "ANALYZE tasks;"

# 5. Warmup + 3 EXPLAIN runs. Canonical form (with a local psql client):
#      psql "$DATABASE_URL" -f scripts/perf_explain.sql
#    Without a host psql client (file not in the container), pipe it in:
docker compose exec -T db psql -U postgres -d tasks_perf -f - < scripts/perf_explain.sql

# 6. Tear down
docker compose exec -T db psql -U postgres -c "DROP DATABASE tasks_perf;"
```

Step 4 (`ANALYZE`) is deliberate: without fresh statistics the planner uses stale
defaults and produces an unrepresentative plan. It updates statistics only — no
schema or index change.

## Maintenance discipline (not enforced by CI)

`scripts/perf_explain.sql` is a hand-copied snapshot of the SQL in `claim_tasks()`.
**It MUST be updated in the same commit as any change to that query** — otherwise the
audit measures a stale plan. **CI does not enforce this**: there is no automated
check that the SQL file matches the source. This is a maintenance convention, not a
guard — and naming that limitation here is the point.
