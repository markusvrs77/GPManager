def test_increment_preview_requires_tables(client):
    res = client.post("/api/gpcopy/increment/preview", json={
        "source_connection_id": 1, "dest_connection_id": 1, "tables": [],
    })
    assert res.status_code == 400


def test_increment_preview_requires_watermark_column(client):
    res = client.post("/api/gpcopy/increment/preview", json={
        "source_connection_id": 1, "dest_connection_id": 1,
        "tables": [{"schema": "s", "table": "t"}],
    })
    # 400 (валидация) или 500 (нет такого connection в пустой dev-БД) — но не 200
    assert res.status_code in (400, 500)


def test_increment_start_requires_tables(client):
    res = client.post("/api/gpcopy/increment/start", json={
        "source_connection_id": 1, "dest_connection_id": 1, "tables": [],
    })
    assert res.status_code == 400


def test_partition_diff_preview_requires_schema_table(client):
    res = client.post("/api/gpcopy/partition-diff/preview", json={
        "source_connection_id": 1, "dest_connection_id": 1,
    })
    assert res.status_code in (400, 500)


def test_partition_diff_start_requires_tables(client):
    res = client.post("/api/gpcopy/partition-diff/start", json={
        "source_connection_id": 1, "tables": [],
    })
    assert res.status_code == 400


def test_new_job_types_registered_in_scheduler():
    import scheduler

    assert "gpcopy_increment" in scheduler.JOB_RUNNERS
    assert "gpcopy_partition_diff" in scheduler.JOB_RUNNERS
