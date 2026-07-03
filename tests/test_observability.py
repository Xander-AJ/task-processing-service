import uuid

from prometheus_client import REGISTRY

from app import main, metrics
from app.services import task_service, worker_service


def _create(db):
    task, _ = task_service.create_task(
        db, uuid.uuid4(), "send_email", {"to": "a@b.com"}, None
    )
    return task.id


def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_ok(client):
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ready"}


def test_ready_returns_503_when_db_down(client, monkeypatch):
    def boom(db):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(main, "_readiness_check", boom)
    resp = client.get("/ready")
    assert resp.status_code == 503
    assert resp.json() == {"status": "not_ready"}


def test_metrics_endpoint_exposes_queue_depth(client, TestSession, monkeypatch):
    # Point the collector at the test database so it sees tasks created here.
    monkeypatch.setattr(metrics, "SessionLocal", TestSession)

    company_id = str(uuid.uuid4())
    for _ in range(2):
        resp = client.post(
            f"/companies/{company_id}/tasks",
            json={"type": "send_email", "payload": {"to": "a@b.com"}},
        )
        assert resp.status_code == 201

    body = client.get("/metrics").text
    assert 'tasks_by_status{status="pending"} 2.0' in body
    assert "oldest_eligible_pending_age_seconds" in body


def test_metrics_scrape_survives_db_error(client, monkeypatch):
    def boom():
        raise RuntimeError("db unavailable")

    # Make the collector's session factory blow up; the scrape must still 200.
    monkeypatch.setattr(metrics, "SessionLocal", boom)
    resp = client.get("/metrics")
    assert resp.status_code == 200


def _processed(outcome: str) -> float:
    return (
        REGISTRY.get_sample_value("tasks_processed_total", {"outcome": outcome}) or 0.0
    )


def test_processed_counter_increments_by_outcome(db):
    # completed
    _create(db)
    before = _processed("completed")
    worker_service.process_available_tasks(db, rng=lambda: 0.99)
    assert _processed("completed") - before == 1

    # retried (force failure, retries still remain -> back to pending)
    _create(db)
    before = _processed("retried")
    worker_service.process_available_tasks(db, rng=lambda: 0.0, backoff_rng=lambda: 0.0)
    assert _processed("retried") - before == 1

    # failed (drive one task through retry exhaustion; final pass is the failure)
    _create(db)
    for _ in range(3):  # exhaust retries, keeping it immediately re-eligible
        worker_service.process_available_tasks(
            db, rng=lambda: 0.0, backoff_rng=lambda: 0.0
        )
    before = _processed("failed")
    worker_service.process_available_tasks(db, rng=lambda: 0.0, backoff_rng=lambda: 0.0)
    assert _processed("failed") - before == 1


def test_processing_latency_recorded_for_all_outcomes(db):
    name = "task_processing_duration_seconds_count"

    # success path
    _create(db)
    before = REGISTRY.get_sample_value(name) or 0.0
    worker_service.process_available_tasks(db, rng=lambda: 0.99)
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 1

    # failure path: the histogram must record latency here too, proving the
    # timer wraps the whole attempt regardless of outcome.
    _create(db)
    before = REGISTRY.get_sample_value(name) or 0.0
    worker_service.process_available_tasks(db, rng=lambda: 0.0, backoff_rng=lambda: 0.0)
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 1


def test_claim_metrics_recorded(db):
    for _ in range(3):
        _create(db)

    batch_count_before = REGISTRY.get_sample_value("claim_batch_size_count") or 0.0
    batch_sum_before = REGISTRY.get_sample_value("claim_batch_size_sum") or 0.0
    dur_count_before = REGISTRY.get_sample_value("claim_duration_seconds_count") or 0.0

    claimed = worker_service.claim_tasks(db, batch_size=10)
    assert len(claimed) == 3

    batch_count_after = REGISTRY.get_sample_value("claim_batch_size_count") or 0.0
    batch_sum_after = REGISTRY.get_sample_value("claim_batch_size_sum") or 0.0
    dur_count_after = REGISTRY.get_sample_value("claim_duration_seconds_count") or 0.0

    assert batch_count_after - batch_count_before == 1
    assert dur_count_after - dur_count_before == 1
    assert batch_sum_after - batch_sum_before == 3
