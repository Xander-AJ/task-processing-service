import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import TaskStatus
from app.schemas import TaskCreate, TaskList, TaskResponse
from app.services import task_service

router = APIRouter(prefix="/companies/{company_id}/tasks", tags=["tasks"])


@router.post("", status_code=201, response_model=TaskResponse)
def create_task(
    company_id: uuid.UUID,
    body: TaskCreate,
    response: Response,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key", max_length=128
    ),
    db: Session = Depends(get_db),
) -> TaskResponse:
    task, created = task_service.create_task(
        db, company_id, body.type, body.payload, idempotency_key
    )
    if not created:
        response.status_code = 200
    return TaskResponse.from_task(task)


@router.get("/{task_id}", response_model=TaskResponse)
def get_task(
    company_id: uuid.UUID,
    task_id: uuid.UUID,
    db: Session = Depends(get_db),
) -> TaskResponse:
    task = task_service.get_task(db, company_id, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return TaskResponse.from_task(task)


@router.get("", response_model=TaskList)
def list_tasks(
    company_id: uuid.UUID,
    status: TaskStatus | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> TaskList:
    tasks, total = task_service.list_tasks(db, company_id, status, limit, offset)
    return TaskList(
        tasks=[TaskResponse.from_task(t) for t in tasks],
        total=total,
        limit=limit,
        offset=offset,
    )
