"""Reproducible workload seeder for the fair-claim performance audit.

Purpose: generate the exact workload used to measure claim_tasks() for the
portfolio performance audit — it exists to make the EXPLAIN numbers reproducible,
NOT to seed a production or dev database. (It TRUNCATEs the tasks table.)

Workload shape (defaults): 100k pending tasks across 50 companies, with one
"flooder" company holding 80% of the backlog and the remaining 20% spread across
the other 49 tenants; 20% of tasks are backoff-delayed into the future
(run_after > now, so not yet eligible); created_at is jittered across the past 24h
so ORDER BY created_at has real work to do. This is the skew the round-robin
fair-claim query was designed to survive, which is why the audit uses it.

Determinism: everything derives from --seed (default 42) — company UUIDs, per-row
company assignment, task UUIDs, created_at jitter, and which rows are future. Same
flags => same logical rows. (Absolute timestamps are relative to now() at insert
time; the *distribution* is what's reproducible.) Bulk-inserts via psycopg2
execute_values so 100k rows land in well under a minute.

Never wipes a DB without confirmation: TRUNCATE is gated behind a prompt unless
--yes is passed.
"""

import argparse
import logging
import random
import uuid

# psycopg2-binary ships no type stubs; execute_values is the fast bulk-insert path.
from psycopg2.extras import execute_values  # type: ignore[import-untyped]
from sqlalchemy import Engine, create_engine, make_url, text

from app.config import settings

log = logging.getLogger("perf_seed")

_BATCH = 5000

# Literal columns are baked into the template; per-row params are:
#   id, company_id, created_offset_s, is_future, future_delay_s, created_offset_s
# created_at = now() - created_offset; run_after = now()+delay if future else created_at.
_TEMPLATE = (
    "(%s, %s, 'send_email', '{}'::jsonb, 'pending', 0, 3, "
    "now() - (%s * interval '1 second'), "
    "CASE WHEN %s THEN now() + (%s * interval '1 second') "
    "ELSE now() - (%s * interval '1 second') END, "
    "now())"
)
_INSERT = (
    "INSERT INTO tasks "
    "(id, company_id, task_type, payload, status, retry_count, max_retries, "
    "created_at, run_after, updated_at) VALUES %s"
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed a perf workload into the tasks table.")
    p.add_argument("--database-url", default=settings.database_url)
    p.add_argument("--total", type=int, default=100000)
    p.add_argument("--companies", type=int, default=50)
    p.add_argument("--flooder-share", type=float, default=0.8)
    p.add_argument("--future-share", type=float, default=0.2)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument(
        "--yes", action="store_true", help="skip the TRUNCATE confirmation prompt"
    )
    return p.parse_args()


def _redact(url: str) -> str:
    return make_url(url).render_as_string(hide_password=True)


def _build_rows(args: argparse.Namespace) -> tuple[list[tuple], list[uuid.UUID]]:
    """Build the per-row value tuples deterministically from the seed."""
    rng = random.Random(args.seed)
    companies = [uuid.UUID(int=rng.getrandbits(128)) for _ in range(args.companies)]
    flooder = companies[0]
    others = companies[1:]

    rows: list[tuple] = []
    for _ in range(args.total):
        task_id = str(uuid.UUID(int=rng.getrandbits(128)))
        if rng.random() < args.flooder_share or not others:
            company_id = flooder
        else:
            company_id = rng.choice(others)
        created_offset_s = rng.random() * 24 * 3600  # spread over the past 24h
        is_future = rng.random() < args.future_share
        future_delay_s = rng.uniform(1, 300) if is_future else 0.0
        rows.append(
            (
                task_id,
                str(company_id),
                created_offset_s,
                is_future,
                future_delay_s,
                created_offset_s,
            )
        )
    return rows, companies


def _summary(engine: Engine, args: argparse.Namespace) -> None:
    with engine.connect() as conn:
        total = conn.execute(text("SELECT count(*) FROM tasks")).scalar()
        top = conn.execute(
            text(
                "SELECT company_id, count(*) AS n FROM tasks "
                "GROUP BY company_id ORDER BY n DESC LIMIT 5"
            )
        ).all()
        bottom = conn.execute(
            text(
                "SELECT company_id, count(*) AS n FROM tasks "
                "GROUP BY company_id ORDER BY n ASC LIMIT 5"
            )
        ).all()
        future_pct = conn.execute(
            text(
                "SELECT 100.0 * avg((run_after > now())::int) FROM tasks"
            )
        ).scalar()
        span = conn.execute(
            text("SELECT min(created_at), max(created_at) FROM tasks")
        ).one()

    log.info("=" * 60)
    log.info("SEED (reproducibility key): %d", args.seed)
    log.info("=" * 60)
    log.info("total inserted: %d", total)
    log.info("top 5 companies by task count:")
    for company_id, n in top:
        log.info("  %s  %d", company_id, n)
    log.info("bottom 5 companies by task count:")
    for company_id, n in bottom:
        log.info("  %s  %d", company_id, n)
    log.info("run_after in the future: %.2f%%", float(future_pct or 0.0))
    log.info("created_at min: %s", span[0])
    log.info("created_at max: %s", span[1])
    log.info("=" * 60)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    log.info(
        "perf_seed config: url=%s total=%d companies=%d flooder_share=%.2f "
        "future_share=%.2f seed=%d",
        _redact(args.database_url),
        args.total,
        args.companies,
        args.flooder_share,
        args.future_share,
        args.seed,
    )

    engine = create_engine(args.database_url)

    if not args.yes:
        answer = input(
            f"About to TRUNCATE tasks on {_redact(args.database_url)}. "
            "Type 'yes' to continue: "
        )
        if answer.strip() != "yes":
            log.info("aborted — no rows changed")
            return

    rows, _ = _build_rows(args)

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        cur.execute("TRUNCATE tasks")
        execute_values(cur, _INSERT, rows, template=_TEMPLATE, page_size=_BATCH)
        raw.commit()
    finally:
        raw.close()

    _summary(engine, args)


if __name__ == "__main__":
    main()
