import pytest

from db import sqlite_cursor
from job_manager import create_job


@pytest.fixture(autouse=True)
def _clean():
    yield
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM job_items WHERE job_id IN "
                    "(SELECT id FROM jobs WHERE config_json LIKE '%recent_api_test%')")
        cur.execute("DELETE FROM jobs WHERE config_json LIKE '%recent_api_test%'")


def _mk(job_type):
    return create_job(job_type, 1, {"marker": "recent_api_test"})


def test_recent_jobs_filtered_by_types_newest_first(client):
    a = _mk("gpcopy")
    b = _mk("vacuum")
    c = _mk("gpcopy_increment")

    data = client.get(
        "/api/jobs/recent?types=gpcopy,gpcopy_increment&limit=10"
    ).get_json()

    assert data["ok"]
    ids = [j["id"] for j in data["jobs"]]
    assert c in ids and a in ids
    assert b not in ids
    # newest first
    assert ids.index(c) < ids.index(a)


def test_recent_jobs_limit(client):
    for _ in range(3):
        _mk("gpcopy")

    data = client.get("/api/jobs/recent?types=gpcopy&limit=2").get_json()
    assert data["ok"]
    assert len(data["jobs"]) == 2
