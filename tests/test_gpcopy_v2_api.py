def test_date_include_json_uses_ready_table_configs(monkeypatch):
    """Пер-табличные SQL-срезы из table_configs идут в json как есть."""
    import modules.gpcopy as g

    monkeypatch.setattr(
        g, "get_connection_by_id",
        lambda cid: {"id": cid, "database": "adb", "host": "h"},
    )

    items = g.build_gpcopy_date_include_json_preview({
        "source_connection_id": 1,
        "dest_connection_id": 2,
        "table_configs": [{
            "schema": "s", "table": "t",
            "source": "s.t", "dest": "s.t",
            "sql": "SELECT * FROM s.t WHERE d >= '2026-01-01' AND d < '2026-02-01'",
        }],
    })

    assert items == [{
        "source": "adb.s.t",
        "dest": "adb.s.t",
        "sql": "SELECT * FROM s.t WHERE d >= '2026-01-01' AND d < '2026-02-01'",
    }]


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
