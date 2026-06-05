import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from app.models import Task, TaskStatus
from app.services import task_service, worker_service


def _create(db):
    task, _ = task_service.create_task(
        db, uuid.uuid4(), "send_email", {"to": "a@b.com"}, None
    )
    return task.id


def test_worker_success(db):
    task_id = _create(db)
    result = worker_service.process_available_tasks(db, rng=lambda: 0.99)
    assert result["processed"] == 1

    db.expire_all()
    task = db.get(Task, task_id)
    assert task.status == TaskStatus.completed
    assert task.locked_at is None


def test_worker_retry_behavior(db):
    task_id = _create(db)

    for expected in (1, 2, 3):
        worker_service.process_available_tasks(db, rng=lambda: 0.0)
        db.expire_all()
        task = db.get(Task, task_id)
        assert task.retry_count == expected
        assert task.status == TaskStatus.pending

    # fourth failure exhausts retries
    worker_service.process_available_tasks(db, rng=lambda: 0.0)
    db.expire_all()
    task = db.get(Task, task_id)
    assert task.status == TaskStatus.failed
    assert task.last_error


def test_stale_lock_recovery(db):
    task_id = _create(db)
    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    task = db.get(Task, task_id)
    task.status = TaskStatus.processing
    task.locked_by = "dead-worker"
    task.locked_at = stale
    db.commit()

    result = worker_service.process_available_tasks(db, rng=lambda: 0.99)
    assert result["processed"] == 1

    db.expire_all()
    task = db.get(Task, task_id)
    assert task.status == TaskStatus.completed


def test_no_duplicate_concurrent_processing(TestSession):
    # Create and COMMIT one pending task so both connections can see it.
    setup = TestSession()
    task_id = _create(setup)
    setup.close()

    def run_pass():
        session = TestSession()
        try:
            return worker_service.process_available_tasks(session, rng=lambda: 0.99)
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(run_pass)
        f2 = pool.submit(run_pass)
        counts = sorted([f1.result()["processed"], f2.result()["processed"]])

    # exactly one worker claimed it, the other got nothing
    assert counts == [0, 1]

    check = TestSession()
    try:
        task = check.get(Task, task_id)
        assert task.status == TaskStatus.completed
    finally:
        check.close()
