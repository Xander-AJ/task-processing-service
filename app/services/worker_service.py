import logging
import random
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Task, TaskStatus

log = logging.getLogger("tasks.worker")

STALE = timedelta(seconds=settings.lock_timeout_seconds)


def claim_tasks(db: Session, batch_size: int = 10) -> list[Task]:
    """Atomically claim a batch of runnable tasks and mark them processing.

    Locks are held only for this short transaction, never during the actual
    work. SKIP LOCKED lets concurrent workers walk past rows another worker has
    already locked, so the same task is never claimed twice.
    """
    now = datetime.now(timezone.utc)

    rows = db.scalars(
        select(Task)
        .where(
            or_(
                Task.status == TaskStatus.pending,
                # stale recovery: a task stuck in processing past the lock window
                # belonged to a worker that died; take it back.
                (Task.status == TaskStatus.processing) & (Task.locked_at < now - STALE),
            )
        )
        .order_by(Task.created_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    ).all()

    for task in rows:
        task.status = TaskStatus.processing
        task.locked_at = now
        task.locked_by = settings.worker_id
        task.updated_at = now

    db.commit()

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
    return rows


def process_task(db: Session, task: Task, rng=random.random) -> str:
    """Run one task's work, then record the outcome in its own short transaction.
    Called after claim_tasks committed, so no row lock is held during the sleep.
    """
    now = datetime.now(timezone.utc)
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
        return "failed"


def process_available_tasks(db: Session, batch_size: int = 10, rng=random.random) -> dict:
    claimed = claim_tasks(db, batch_size)
    results = []
    for task in claimed:
        status = process_task(db, task, rng)
        results.append(
            {"taskId": str(task.id), "status": status, "error": task.last_error}
        )
    return {"processed": len(claimed), "results": results}
