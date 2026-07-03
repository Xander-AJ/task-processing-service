from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db import get_db
from app.services import worker_service

router = APIRouter(prefix="/workers", tags=["workers"])


@router.post("/process")
def process(db: Session = Depends(get_db)) -> dict:
    return worker_service.process_available_tasks(db)
