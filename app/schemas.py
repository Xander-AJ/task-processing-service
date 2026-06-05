import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

from app.models import Task, TaskStatus


class CamelModel(BaseModel):
    # API speaks camelCase; our DB/Python code is snake_case. populate_by_name
    # lets us build these by Python field name and still emit camelCase aliases.
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


# type/payload are already the same in camelCase, so no alias generator needed.
class TaskCreate(BaseModel):
    type: str = Field(min_length=1)
    payload: dict

    @field_validator("type")
    @classmethod
    def type_not_blank(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("type must not be blank")
        return v


class TaskResponse(CamelModel):
    id: uuid.UUID
    company_id: uuid.UUID
    type: str
    payload: dict
    status: TaskStatus
    retry_count: int
    max_retries: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    locked_at: datetime | None

    @classmethod
    def from_task(cls, task: Task) -> "TaskResponse":
        # task_type -> type is the one mapping that isn't a plain rename.
        return cls(
            id=task.id,
            company_id=task.company_id,
            type=task.task_type,
            payload=task.payload,
            status=task.status,
            retry_count=task.retry_count,
            max_retries=task.max_retries,
            last_error=task.last_error,
            created_at=task.created_at,
            updated_at=task.updated_at,
            locked_at=task.locked_at,
        )


class TaskList(BaseModel):
    tasks: list[TaskResponse]
    total: int
    limit: int
    offset: int
