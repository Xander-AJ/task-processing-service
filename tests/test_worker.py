import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from prometheus_client import REGISTRY

from app.config import settings
from app.models import Task, TaskStatus
from app.services import task_service, worker_service


def _create(db):
    task, _ = task_service.create_task(
        db, uuid.uuid4(), "send_email", {"to": "a@b.com"}, None
    )
    return task.id


def _insert_pending(db, company_id, created_at):
    """Insert a pending, immediately-eligible task with an explicit created_at.

    Builds the Task directly (like test_stale_lock_recovery) so tests control
    company_id and ordering; run_after is set in the past so the backoff gate
    never hides the row.
    """
    task = Task(
        id=uuid.uuid4(),
        company_id=company_id,
        task_type="send_email",
        payload={"to": "a@b.com"},
        status=TaskStatus.pending,
        run_after=created_at - timedelta(minutes=1),
        created_at=created_at,
        updated_at=created_at,
    )
    db.add(task)
    db.commit()
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
        # backoff_rng=0.0 => zero delay, so the task stays immediately eligible.
        worker_service.process_available_tasks(
            db, rng=lambda: 0.0, backoff_rng=lambda: 0.0
        )
        db.expire_all()
        task = db.get(Task, task_id)
        assert task.retry_count == expected
        assert task.status == TaskStatus.pending

    # fourth failure exhausts retries
    worker_service.process_available_tasks(db, rng=lambda: 0.0, backoff_rng=lambda: 0.0)
    db.expire_all()
    task = db.get(Task, task_id)
    assert task.status == TaskStatus.failed
    assert task.last_error


def test_failed_task_waits_before_reclaim(db):
    task_id = _create(db)

    # Force a failure with max backoff delay.
    worker_service.process_available_tasks(db, rng=lambda: 0.0, backoff_rng=lambda: 1.0)
    db.expire_all()
    task = db.get(Task, task_id)
    assert task.status == TaskStatus.pending
    assert task.retry_count == 1
    assert task.run_after > datetime.now(UTC)

    # A second pass shouldn't be able to claim it yet.
    result = worker_service.process_available_tasks(db, rng=lambda: 0.0)
    assert result["processed"] == 0


def test_compute_backoff_bounds():
    base = settings.retry_backoff_base_seconds
    factor = settings.retry_backoff_factor
    cap = settings.retry_backoff_cap_seconds

    for n in (0, 1, 2, 10):
        assert worker_service.compute_backoff_seconds(n, rng=lambda: 1.0) == min(
            cap, base * factor**n
        )
    assert worker_service.compute_backoff_seconds(3, rng=lambda: 0.0) == 0.0


def test_stale_lock_recovery(db):
    task_id = _create(db)
    stale = datetime.now(UTC) - timedelta(minutes=10)
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


def test_fair_claim_across_companies(db):
    base = datetime.now(UTC) - timedelta(hours=1)
    company_a, company_b, company_c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    # Company A floods with 5 of the oldest tasks; B and C have 2 each, newer.
    for i in range(5):
        _insert_pending(db, company_a, base + timedelta(seconds=i))
    for i in range(2):
        _insert_pending(db, company_b, base + timedelta(minutes=1, seconds=i))
    for i in range(2):
        _insert_pending(db, company_c, base + timedelta(minutes=2, seconds=i))

    claimed = worker_service.claim_tasks(db, batch_size=3)

    assert len(claimed) == 3
    companies = {task.company_id for task in claimed}
    assert companies == {company_a, company_b, company_c}


def test_fair_claim_does_not_starve_quiet_tenant(db):
    base = datetime.now(UTC) - timedelta(hours=1)
    company_a, company_b = uuid.uuid4(), uuid.uuid4()

    # A has 10 older tasks; B has a single newer one. Pure FIFO would rank B 11th.
    for i in range(10):
        _insert_pending(db, company_a, base + timedelta(seconds=i))
    b_task = _insert_pending(db, company_b, base + timedelta(minutes=5))

    claimed = worker_service.claim_tasks(db, batch_size=3)

    assert len(claimed) == 3
    assert b_task in {task.id for task in claimed}


def test_concurrent_claim_no_duplicates(TestSession):
    # 8 pending tasks across 4 companies, committed so every connection sees them.
    setup = TestSession()
    base = datetime.now(UTC) - timedelta(hours=1)
    companies = [uuid.uuid4() for _ in range(4)]
    for c in companies:
        for i in range(2):
            _insert_pending(setup, c, base + timedelta(seconds=i))
    setup.close()

    def run_pass():
        session = TestSession()
        try:
            return [task.id for task in worker_service.claim_tasks(session, batch_size=2)]
        finally:
            session.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(run_pass) for _ in range(8)]
        all_ids = [task_id for f in futures for task_id in f.result()]

    # No task is ever claimed twice, and we never claim more than exist.
    assert len(all_ids) == len(set(all_ids))
    assert len(all_ids) <= 8


def test_release_tasks_returns_them_to_pending(db):
    base = datetime.now(UTC) - timedelta(hours=1)
    company = uuid.uuid4()
    for i in range(2):
        _insert_pending(db, company, base + timedelta(seconds=i))

    claimed = worker_service.claim_tasks(db, batch_size=2)
    assert len(claimed) == 2
    # Snapshot the fields release_tasks must leave untouched.
    before = {t.id: (t.retry_count, t.run_after) for t in claimed}
    for task in claimed:
        assert task.status == TaskStatus.processing
        assert task.locked_at is not None

    worker_service.release_tasks(db, claimed)

    db.expire_all()
    for task_id, (retry_count, run_after) in before.items():
        task = db.get(Task, task_id)
        assert task.status == TaskStatus.pending
        assert task.locked_at is None
        assert task.locked_by is None
        assert task.retry_count == retry_count
        assert task.run_after == run_after


def test_release_increments_released_counter(db):
    base = datetime.now(UTC) - timedelta(hours=1)
    company = uuid.uuid4()
    for i in range(2):
        _insert_pending(db, company, base + timedelta(seconds=i))

    # Read deltas, not absolutes: the default REGISTRY is process-global and
    # shared with every other test.
    before = REGISTRY.get_sample_value("tasks_released_total") or 0.0
    claimed = worker_service.claim_tasks(db, batch_size=2)
    assert len(claimed) == 2
    worker_service.release_tasks(db, claimed)
    after = REGISTRY.get_sample_value("tasks_released_total") or 0.0

    assert after - before == len(claimed)


def test_shutdown_releases_unstarted_tasks(db):
    base = datetime.now(UTC) - timedelta(hours=1)
    company = uuid.uuid4()
    task_ids = [
        _insert_pending(db, company, base + timedelta(seconds=i)) for i in range(3)
    ]

    stop = threading.Event()
    stop.set()  # already stopping before the first task runs

    result = worker_service.process_available_tasks(
        db, batch_size=3, stop=stop, rng=lambda: 0.99
    )

    assert result["processed"] == 0
    db.expire_all()
    for task_id in task_ids:
        task = db.get(Task, task_id)
        assert task.status == TaskStatus.pending
        assert task.locked_at is None
        assert task.locked_by is None
