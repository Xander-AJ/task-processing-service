import uuid

from app import main, metrics


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
