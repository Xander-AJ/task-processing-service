import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.config import settings
from app.metrics import (
    claim_batch_size,
    claim_duration_seconds,
    task_processing_duration_seconds,
    tasks_claimed_total,
    tasks_processed_total,
    tasks_released_total,
)
from app.models import Task, TaskStatus

log = logging.getLogger("tasks.worker")

STALE = timedelta(seconds=settings.lock_timeout_seconds)


def compute_backoff_seconds(
    retry_count: int, *, rng: Callable[[], float] = random.random
) -> float:
    """Full-jitter exponential backoff: random(0, min(cap, base * factor**retry_count)).

    The uncapped delay grows exponentially per attempt; the cap bounds it and the
    jitter (a uniform draw in [0, delay)) spreads retries out to avoid thundering
    herds. ``rng`` returns a value in [0, 1) and is injectable for tests.
    """
    ceiling = min(
        settings.retry_backoff_cap_seconds,
        settings.retry_backoff_base_seconds * settings.retry_backoff_factor**retry_count,
    )
    return rng() * ceiling


def claim_tasks(db: Session, batch_size: int = 10) -> list[Task]:
    """Atomically claim a batch of runnable tasks and mark them processing.

    Locks are held only for this short transaction, never during the actual
    work. SKIP LOCKED lets concurrent workers walk past rows another worker has
    already locked, so the same task is never claimed twice.
    """
    now = datetime.now(timezone.utc)
    stale_before = now - STALE

    # Round-robin fairness across companies. A naive `ORDER BY created_at` claims
    # the globally-oldest rows, so one tenant with a big backlog starves everyone
    # else. Instead we rank each company's eligible tasks with ROW_NUMBER (rn=1 is
    # that company's oldest) and order the batch by (rn, created_at): every
    # company's oldest task is claimed before any company's second. `rn <=
    # batch_size` caps how many rows a single flooder can contribute to the
    # candidate pool.
    #
    # This is raw SQL because a window function combined with FOR UPDATE SKIP
    # LOCKED can't be expressed cleanly through the ORM, and the locking
    # semantics (lock only the tasks rows `t`, skip already-locked ones) read far
    # more clearly written out. `from_statement` still returns session-attached
    # ORM Task objects, so the mutation/commit/logging block below is unchanged.
    #
    # Eligibility is identical to before: pending tasks past their run_after gate,
    # plus stale processing tasks whose worker died. Stale tasks are ranked
    # alongside pending ones rather than given a separate path.
    #
    # The eligibility predicate is repeated on the outer `t` (not just in the
    # `ranked` CTE) on purpose. `ranked` reads tasks without a lock, so two
    # workers can build overlapping candidate sets from the same snapshot. Under
    # READ COMMITTED, when `FOR UPDATE` meets a row a concurrent worker already
    # claimed and committed, Postgres re-fetches the latest version and re-checks
    # the outer WHERE (EvalPlanQual); the now-`processing`/fresh-locked row fails
    # both branches and is dropped. Without this re-check, SKIP LOCKED alone would
    # hand the freed row to a second worker and claim it twice.
    sql = text(
        """
        WITH ranked AS (
            SELECT id,
                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at) AS rn,
                   created_at
            FROM tasks
            WHERE (status = 'pending'    AND run_after <= :now)
               OR (status = 'processing' AND locked_at < :stale_before)
        ),
        candidates AS (
            SELECT id, rn, created_at FROM ranked WHERE rn <= :batch_size
        )
        SELECT t.*
        FROM tasks t
        JOIN candidates c ON c.id = t.id
        WHERE (t.status = 'pending'    AND t.run_after <= :now)
           OR (t.status = 'processing' AND t.locked_at < :stale_before)
        ORDER BY c.rn, c.created_at
        FOR UPDATE OF t SKIP LOCKED
        LIMIT :batch_size
        """
    )
    with claim_duration_seconds.time():
        rows = db.scalars(
            select(Task).from_statement(sql),
            {"now": now, "stale_before": stale_before, "batch_size": batch_size},
        ).all()

    for task in rows:
        task.status = TaskStatus.processing
        task.locked_at = now
        task.locked_by = settings.worker_id
        task.updated_at = now

    db.commit()

    claim_batch_size.observe(len(rows))
    tasks_claimed_total.inc(len(rows))

    for task in rows:
        log.info(
            "task_locked",
            extra={
                "event": "task_locked",
                "task_id": str(task.id),
                "company_id": str(task.company_id),
                "worker_id": settings.worker_id,
            },
        )
    return list(rows)


def process_task(
    db: Session,
    task: Task,
    rng: Callable[[], float] = random.random,
    backoff_rng: Callable[[], float] = random.random,
) -> str:
    """Run one task's work, then record the outcome in its own short transaction.
    Called after claim_tasks committed, so no row lock is held during the sleep.

    ``rng`` drives the simulated-failure coin flip; ``backoff_rng`` is a separate
    source used only for retry backoff jitter so the two are independently
    controllable in tests.
    """
    now = datetime.now(timezone.utc)
    with task_processing_duration_seconds.time():
        try:
            if task.task_type == "send_email":
                time.sleep(0.5)
                if rng() < 0.2:
                    raise RuntimeError("simulated send_email failure")

            task.status = TaskStatus.completed
            task.last_error = None
            task.locked_at = None
            task.locked_by = None
            task.updated_at = now
            db.commit()
            log.info(
                "task_completed",
                extra={"event": "task_completed", "task_id": str(task.id)},
            )
            tasks_processed_total.labels(outcome="completed").inc()
            return "completed"

        except Exception as err:
            db.rollback()
            now = datetime.now(timezone.utc)
            if task.retry_count < task.max_retries:
                task.status = TaskStatus.pending
                task.retry_count += 1
                task.last_error = str(err)
                task.locked_at = None
                task.locked_by = None
                # Gate re-claim behind exponential backoff; use the post-increment
                # retry_count so the delay grows with each attempt.
                task.run_after = now + timedelta(
                    seconds=compute_backoff_seconds(task.retry_count, rng=backoff_rng)
                )
                task.updated_at = now
                db.commit()
                log.info(
                    "task_retry_scheduled",
                    extra={
                        "event": "task_retry_scheduled",
                        "task_id": str(task.id),
                        "retry_count": task.retry_count,
                    },
                )
                tasks_processed_total.labels(outcome="retried").inc()
                return "pending"

            task.status = TaskStatus.failed
            task.last_error = str(err)
            task.locked_at = None
            task.locked_by = None
            task.updated_at = now
            db.commit()
            log.info(
                "task_failed",
                extra={"event": "task_failed", "task_id": str(task.id)},
            )
            tasks_processed_total.labels(outcome="failed").inc()
            return "failed"


def release_tasks(db: Session, tasks: list[Task]) -> None:
    """Hand claimed tasks back to the queue as healthy pending work.

    Used on graceful shutdown to un-claim tasks a stopping worker won't run.
    Resets the lock (status -> pending, locked_at/locked_by cleared) but leaves
    retry_count and run_after untouched: these tasks never failed, so they must
    not be penalised with a retry or a backoff delay. Committed in one
    transaction. No-op for an empty list.
    """
    if not tasks:
        return
    now = datetime.now(timezone.utc)
    for task in tasks:
        task.status = TaskStatus.pending
        task.locked_at = None
        task.locked_by = None
        task.updated_at = now
    db.commit()
    tasks_released_total.inc(len(tasks))
    for task in tasks:
        log.info(
            "task_released",
            extra={
                "event": "task_released",
                "task_id": str(task.id),
                "worker_id": settings.worker_id,
            },
        )


def process_available_tasks(
    db: Session,
    batch_size: int = 10,
    rng: Callable[[], float] = random.random,
    backoff_rng: Callable[[], float] = random.random,
    stop: threading.Event | None = None,
) -> dict:
    """Claim a batch and process it, honouring a graceful-shutdown signal.

    When ``stop`` is set, no new task is started: all remaining un-started tasks
    in the batch (including the one about to run) are released back to pending
    and excluded from the results. A task already in progress is never
    interrupted. With ``stop is None`` this behaves exactly as before.
    """
    claimed = claim_tasks(db, batch_size)
    results = []
    for i, task in enumerate(claimed):
        if stop is not None and stop.is_set():
            release_tasks(db, claimed[i:])
            break
        status = process_task(db, task, rng, backoff_rng)
        results.append(
            {"taskId": str(task.id), "status": status, "error": task.last_error}
        )
    return {"processed": len(results), "results": results}
