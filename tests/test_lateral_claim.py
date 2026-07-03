import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete

from app.config import settings
from app.models import Task, TaskStatus
from app.services import worker_service


def _insert(db, company_id, created_at, *, status=TaskStatus.pending,
            run_after=None, locked_at=None, locked_by=None):
    """Insert a task with explicit fields; run_after defaults to just-past."""
    if run_after is None:
        run_after = created_at - timedelta(minutes=1)
    task = Task(
        id=uuid.uuid4(),
        company_id=company_id,
        task_type="send_email",
        payload={"to": "a@b.com"},
        status=status,
        run_after=run_after,
        locked_at=locked_at,
        locked_by=locked_by,
        created_at=created_at,
        updated_at=created_at,
    )
    db.add(task)
    db.commit()
    return task.id


def test_lateral_respects_per_company_cap(db):
    base = datetime.now(UTC) - timedelta(hours=1)
    company = uuid.uuid4()
    for i in range(20):
        _insert(db, company, base + timedelta(seconds=i))

    claimed = worker_service.claim_tasks(db, batch_size=100)

    # The LATERAL's LIMIT caps this one company's contribution to the batch.
    from_company = [t for t in claimed if t.company_id == company]
    assert len(from_company) == settings.per_company_claim_cap == 10


def test_lateral_stale_recovery_bypasses_run_after(db):
    company = uuid.uuid4()
    stale = datetime.now(UTC) - timedelta(minutes=10)
    far_future = datetime.now(UTC) + timedelta(days=30)
    task_id = _insert(
        db,
        company,
        created_at=datetime.now(UTC) - timedelta(hours=1),
        status=TaskStatus.processing,
        run_after=far_future,  # would hide a pending task; must NOT hide a stale one
        locked_at=stale,
        locked_by="dead-worker",
    )

    claimed = worker_service.claim_tasks(db, batch_size=10)

    assert task_id in {t.id for t in claimed}


def test_lateral_high_contention_no_duplicates(TestSession):
    # Run the whole race many times: the tier-2 duplicate-claim race was ~1-in-4,
    # so a single pass isn't proof.
    for _ in range(20):
        setup = TestSession()
        setup.execute(delete(Task))  # clear between iterations
        setup.commit()
        base = datetime.now(UTC) - timedelta(hours=1)
        companies = [uuid.uuid4() for _ in range(4)]
        for c in companies:
            for i in range(5):  # 4 companies x 5 = 20 eligible tasks
                _insert(setup, c, base + timedelta(seconds=i))
        setup.close()

        def run_pass():
            session = TestSession()
            try:
                return [
                    t.id for t in worker_service.claim_tasks(session, batch_size=2)
                ]
            finally:
                session.close()

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(run_pass) for _ in range(10)]
            all_ids = [task_id for f in futures for task_id in f.result()]

        assert len(all_ids) == len(set(all_ids)), "duplicate claim under contention"
        assert len(all_ids) <= 20
