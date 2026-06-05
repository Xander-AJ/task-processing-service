import uuid


def _is_uuid(value):
    uuid.UUID(value)
    return True


def test_create_task(client, company_id):
    resp = client.post(
        f"/companies/{company_id}/tasks",
        json={"type": "send_email", "payload": {"to": "a@b.com"}},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "pending"
    assert body["retryCount"] == 0
    assert body["maxRetries"] == 3
    assert _is_uuid(body["id"])
    assert body["companyId"] == company_id
    # camelCase keys present, snake_case absent
    assert "retryCount" in body and "createdAt" in body
    assert "retry_count" not in body and "locked_by" not in body


def test_get_task(client, company_id):
    created = client.post(
        f"/companies/{company_id}/tasks",
        json={"type": "send_email", "payload": {"to": "a@b.com"}},
    ).json()
    task_id = created["id"]

    resp = client.get(f"/companies/{company_id}/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json() == created

    other_company = str(uuid.uuid4())
    missing = client.get(f"/companies/{other_company}/tasks/{task_id}")
    assert missing.status_code == 404


def test_list_tasks_by_status(client, company_id):
    for _ in range(2):
        client.post(
            f"/companies/{company_id}/tasks",
            json={"type": "send_email", "payload": {}},
        )

    resp = client.get(f"/companies/{company_id}/tasks?status=pending")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["tasks"]) == 2


def test_list_tasks_pagination(client, company_id):
    for i in range(5):
        client.post(
            f"/companies/{company_id}/tasks",
            json={"type": "send_email", "payload": {"i": i}},
        )

    page1 = client.get(f"/companies/{company_id}/tasks?limit=2&offset=0").json()
    page2 = client.get(f"/companies/{company_id}/tasks?limit=2&offset=2").json()

    assert page1["total"] == 5 and page2["total"] == 5
    assert len(page1["tasks"]) == 2 and len(page2["tasks"]) == 2

    ids1 = {t["id"] for t in page1["tasks"]}
    ids2 = {t["id"] for t in page2["tasks"]}
    assert ids1.isdisjoint(ids2)


def test_idempotency_same_key(client, company_id):
    headers = {"Idempotency-Key": "key-123"}
    body = {"type": "send_email", "payload": {"to": "a@b.com"}}

    first = client.post(f"/companies/{company_id}/tasks", json=body, headers=headers)
    second = client.post(f"/companies/{company_id}/tasks", json=body, headers=headers)
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]

    other_company = str(uuid.uuid4())
    third = client.post(
        f"/companies/{other_company}/tasks", json=body, headers=headers
    )
    assert third.status_code == 201
    assert third.json()["id"] != first.json()["id"]
