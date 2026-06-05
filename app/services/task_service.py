import logging
import uuid

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Task, TaskStatus

log = logging.getLogger("tasks.service")


def _find_by_key(db: Session, company_id: uuid.UUID, key: str) -> Task | None:
    return db.scalar(
        select(Task).where(
            Task.company_id == company_id, Task.idempotency_key == key
        )
    )


def create_task(
    db: Session,
    company_id: uuid.UUID,
    task_type: str,
    payload: dict,
    idempotency_key: str | None,
) -> tuple[Task, bool]:
    """Create a task, or return the existing one for a repeated Idempotency-Key.
    Returns (task, created) where created is False on an idempotent hit."""

    if idempotency_key:
        existing = _find_by_key(db, company_id, idempotency_key)
        if existing:
            log.info(
                "idempotent_hit",
                extra={"company_id": str(company_id), "task_id": str(existing.id)},
            )
            return existing, False

    task = Task(
        company_id=company_id,
        task_type=task_type,
        payload=payload,
        status=TaskStatus.pending,
        retry_count=0,
        max_retries=settings.max_retries,
        idempotency_key=idempotency_key,
    )
    db.add(task)
    try:
        db.commit()
    except IntegrityError:
        # A concurrent request inserted the same (company_id, key) first. Roll
        # back and return the winner instead of failing the caller.
        db.rollback()
        if idempotency_key is None:
            # No key means this wasn't an idempotency collision; surface the error.
            raise
        existing = _find_by_key(db, company_id, idempotency_key)
        if existing is None:
            raise
        return existing, False

    db.refresh(task)
    log.info(
        "task_created",
        extra={
            "company_id": str(company_id),
            "task_id": str(task.id),
            "type": task_type,
        },
    )
    return task, True


def get_task(db: Session, company_id: uuid.UUID, task_id: uuid.UUID) -> Task | None:
    return db.scalar(
        select(Task).where(Task.id == task_id, Task.company_id == company_id)
    )


def list_tasks(
    db: Session,
    company_id: uuid.UUID,
    status: TaskStatus | None,
    limit: int,
    offset: int,
) -> tuple[list[Task], int]:
    conditions = [Task.company_id == company_id]
    if status is not None:
        conditions.append(Task.status == status)

    total = db.scalar(select(func.count()).select_from(Task).where(*conditions))
    tasks = list(
        db.scalars(
            select(Task)
            .where(*conditions)
            .order_by(Task.created_at.desc(), Task.id.desc())
            .limit(limit)
            .offset(offset)
        )
    )
    return tasks, total
