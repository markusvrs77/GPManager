import pytest

from db import sqlite_cursor


@pytest.fixture(autouse=True)
def _clean():
    yield
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM schedule_runs")
        cur.execute("DELETE FROM schedules WHERE name LIKE 'apitest%'")


def test_schedules_page_renders(client):
    res = client.get("/schedules")
    assert res.status_code == 200
    assert "Schedules" in res.get_data(as_text=True)


def test_schedule_crud_roundtrip(client):
    res = client.post("/api/schedules", json={
        "name": "apitest-nightly",
        "job_type": "gpcopy",
        "cron_expr": "0 2 * * *",
        "config": {"connection_id": 1, "tables": [{"schema": "s", "table": "t"}]},
    })
    data = res.get_json()
    assert data["ok"], data
    sid = data["id"]

    names = [s["name"] for s in client.get("/api/schedules").get_json()["schedules"]]
    assert "apitest-nightly" in names

    assert client.post(f"/api/schedules/{sid}/toggle").get_json()["enabled"] == 0

    res = client.put(f"/api/schedules/{sid}", json={"cron_expr": "not a cron"})
    assert res.status_code == 400

    assert client.delete(f"/api/schedules/{sid}").get_json()["ok"]


def test_preview_cron_and_window(client):
    data = client.post("/api/schedules/preview", json={
        "cron_expr": "0 2 * * *",
        "date_window": {"from": {"preset": "yesterday"}, "to": {"preset": "yesterday"}},
    }).get_json()

    assert data["ok"]
    assert len(data["next_runs"]) == 5
    assert data["date_from"] < data["date_to"]

    res = client.post("/api/schedules/preview", json={"cron_expr": "bad"})
    assert res.status_code == 400


def test_invalid_cron_rejected_on_create(client):
    res = client.post("/api/schedules", json={
        "name": "apitest-bad",
        "job_type": "gpcopy",
        "cron_expr": "13 37",
    })
    assert res.status_code == 400
