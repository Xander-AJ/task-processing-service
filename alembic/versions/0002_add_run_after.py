"""add run_after

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-03

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tasks",
        sa.Column(
            "run_after",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    # Existing rows should stay eligible immediately: seed run_after from when
    # the task was created.
    op.execute("UPDATE tasks SET run_after = created_at")
    op.create_index(
        "ix_tasks_pending_run_after",
        "tasks",
        ["run_after"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_tasks_pending_run_after", table_name="tasks")
    op.drop_column("tasks", "run_after")
