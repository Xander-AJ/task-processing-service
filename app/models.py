import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Enum,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class TaskStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class Task(Base):
    __tablename__ = "tasks"
    # Idempotency keys are scoped per company: two companies can reuse the same
    # key independently, but a company can't create two tasks with one key.
    __table_args__ = (
        UniqueConstraint(
            "company_id", "idempotency_key", name="uq_tasks_company_idempotency_key"
        ),
        # Partial index backing the worker's claim query (pending + eligible). It
        # also lives in migration 0002; declared here so the model metadata matches
        # the migrated schema and `alembic check` reports no drift.
        Index(
            "ix_tasks_pending_run_after",
            "run_after",
            postgresql_where=text("status = 'pending'"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # Tasks belong to a company. companyId comes from the URL path; there is no
    # companies table because the spec has no endpoint to manage companies.
    company_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )

    # Idempotency-Key from the create request. Uniqueness is enforced per company
    # by the table-level constraint above, so a retried POST returns the original
    # task instead of creating a duplicate.
    idempotency_key: Mapped[str | None] = mapped_column(String, default=None)

    task_type: Mapped[str] = mapped_column(String, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status"),
        nullable=False,
        default=TaskStatus.pending,
        index=True,
    )

    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    last_error: Mapped[str | None] = mapped_column(String, default=None)

    # Worker locking. locked_at is when a worker claimed the task; a claim older
    # than the stale window (5 min) is treated as a dead worker and reclaimable.
    # locked_by records which worker holds it, for logs and debugging.
    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    locked_by: Mapped[str | None] = mapped_column(String, default=None)

    # When a pending task becomes eligible to claim. Failed tasks are pushed
    # into the future by backoff; new tasks default to now().
    run_after: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
