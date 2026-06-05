"""create tasks table

Revision ID: 0001
Revises:
Create Date: 2026-06-05

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

task_status = postgresql.ENUM(
    "pending", "processing", "completed", "failed", name="task_status"
)
# create_type=False so the column below references the enum we create
# explicitly, instead of create_table trying to CREATE TYPE a second time.
task_status_col = postgresql.ENUM(
    "pending", "processing", "completed", "failed",
    name="task_status", create_type=False,
)


def upgrade() -> None:
    task_status.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("company_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=True),
        sa.Column("task_type", sa.String(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", task_status_col, nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False),
        sa.Column("max_retries", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "company_id", "idempotency_key", name="uq_tasks_company_idempotency_key"
        ),
    )
    op.create_index("ix_tasks_company_id", "tasks", ["company_id"])
    op.create_index("ix_tasks_status", "tasks", ["status"])


def downgrade() -> None:
    op.drop_index("ix_tasks_status", table_name="tasks")
    op.drop_index("ix_tasks_company_id", table_name="tasks")
    op.drop_table("tasks")
    task_status.drop(op.get_bind(), checkfirst=True)
