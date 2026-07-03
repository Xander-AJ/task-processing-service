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

## Design decisions

- `company_id` lives on the task row; there is no companies table because the spec
  has no endpoints for managing companies.
- Claiming and working are two separate transactions. Row locks are held only for the
  claim flip, never during the 0.5s of simulated work.
- Claims use `FOR UPDATE SKIP LOCKED`, so concurrent workers skip each other's rows
  instead of blocking or claiming the same task twice.
- Idempotency is unique per `(company_id, idempotency_key)`. The DB constraint is the
  source of truth; the app catches the conflict and returns the existing task.
- Stale locks (a task stuck in `processing` longer than 5 minutes) are reclaimable, so
  a dead worker can't strand a task forever.
- A task is retried up to `max_retries` (default 3), then marked `failed` with its
  last error recorded.
- Failed tasks retry with full-jitter exponential backoff (base 2s, ×2 per attempt,
  capped at 5min) via a `run_after` gate, so a poison task can't burn its retries in
  seconds. Stale-lock recovery is separate and unconditional.
- Workers claim tasks round-robin across companies (ROW_NUMBER partitioned by
  company_id), so a tenant with a large backlog can't starve others. A single
  CTE ranks and locks in one statement via FOR UPDATE SKIP LOCKED.

## Bonus features

Docker Compose, Idempotency-Key on create, structured JSON logging, and pagination
(limit/offset/total).

## Tradeoffs / next steps

- The worker is a simple poller. That's fine at this scale; production might use
  `LISTEN/NOTIFY` or a real queue to avoid the polling delay.
- Task processing is simulated (sleep + random failure) as described in the spec.
- No auth layer — `companyId` is taken from the path and trusted (out of scope here).
