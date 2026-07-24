def test_date_include_json_uses_ready_table_configs(monkeypatch):
    """Пер-табличные SQL-срезы из table_configs идут в json как есть."""
    import modules.gpcopy as g

    monkeypatch.setattr(
        g, "get_connection_by_id",
        lambda cid: {"id": cid, "database": "adb", "host": "h"},
    )
    # непартиционированная таблица: leaf-запрос вернул бы её саму
    monkeypatch.setattr(
        g, "fetch_leaves_by_key",
        lambda conn, entries, date_from="", date_to="": {("s", "t"): [("s", "t")]},
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


def test_date_include_json_expands_partitioned_table(monkeypatch):
    """
    gpcopy не принимает SQL-срез для родительской партиционированной таблицы
    ("Don't support partition table ... with SQL statement"), поэтому в json
    должны уйти leaf-партиции — каждая со своим срезом.
    """
    import modules.gpcopy as g

    monkeypatch.setattr(
        g, "get_connection_by_id",
        lambda cid: {"id": cid, "database": "adb", "host": "h"},
    )
    monkeypatch.setattr(
        g, "fetch_leaves_by_key",
        lambda conn, entries, date_from="", date_to="": {
            ("dwh_stage", "s01_t_operjrn"): [
                ("dwh_stage", "s01_t_operjrn_1_prt_1"),
                ("dwh_stage", "s01_t_operjrn_1_prt_2"),
            ],
        },
    )

    items = g.build_gpcopy_date_include_json_preview({
        "source_connection_id": 1,
        "dest_connection_id": 2,
        "date_from": "2026-07-01",
        "date_to": "2026-07-02",
        "table_configs": [{
            "schema": "dwh_stage", "table": "s01_t_operjrn",
            "source": "dwh_stage.s01_t_operjrn",
            "dest": "dwh_stage.s01_t_operjrn",
            "date_column": "oper_date",
            "sql": "SELECT * FROM dwh_stage.s01_t_operjrn WHERE oper_date >= '2026-07-01'",
        }],
    })

    assert [i["source"] for i in items] == [
        "adb.dwh_stage.s01_t_operjrn_1_prt_1",
        "adb.dwh_stage.s01_t_operjrn_1_prt_2",
    ]
    assert [i["dest"] for i in items] == [
        "adb.dwh_stage.s01_t_operjrn_1_prt_1",
        "adb.dwh_stage.s01_t_operjrn_1_prt_2",
    ]
    # родителя в json быть не должно
    assert all("s01_t_operjrn\"" not in i["sql"] for i in items)
    assert items[0]["sql"] == (
        'SELECT * FROM "dwh_stage"."s01_t_operjrn_1_prt_1" '
        "WHERE \"oper_date\" >= '2026-07-01' AND \"oper_date\" < '2026-07-02'"
    )


def test_expand_date_entries_requires_date_column_for_partitioned():
    """Без колонки даты пересобрать срез по партициям нельзя — понятная ошибка."""
    import pytest

    import modules.gpcopy as g

    entries = [{
        "schema": "s", "table": "parent",
        "dest_schema": "s", "dest_table": "parent",
        "date_column": None,
        "sql": "SELECT * FROM s.parent WHERE d >= '2026-01-01'",
    }]

    with pytest.raises(ValueError) as e:
        g.expand_date_entries_to_leaves(
            entries,
            {("s", "parent"): [("s", "parent_1_prt_1")]},
            "2026-01-01", "2026-01-02",
        )

    assert "партиционирована" in str(e.value)


def test_parse_range_bound():
    """Границы RANGE-партиций из каталога (формат GP7 / PG12)."""
    import modules.gpcopy as g

    assert g.parse_range_bound(
        "FOR VALUES FROM ('2025-01-02') TO ('2025-01-03')"
    ) == ("2025-01-02", "2025-01-03")

    # DEFAULT и незнакомые форматы -> None (партицию оставляем)
    assert g.parse_range_bound("DEFAULT") is None
    assert g.parse_range_bound("FOR VALUES IN ('a', 'b')") is None
    assert g.parse_range_bound("") is None

    # составной ключ не отсекаем
    assert g.parse_range_bound(
        "FOR VALUES FROM ('2025-01-02', 1) TO ('2025-01-03', 5)"
    ) is None

    assert g.parse_range_bound(
        "FOR VALUES FROM ('2025-01-02') TO (MAXVALUE)"
    ) == ("2025-01-02", None)


def test_range_overlaps():
    import modules.gpcopy as g

    # партиция за день до диапазона / после него
    assert not g.range_overlaps("2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03")
    assert not g.range_overlaps("2025-01-05", "2025-01-06", "2025-01-02", "2025-01-03")
    # ровно нужный день и MAXVALUE-хвост
    assert g.range_overlaps("2025-01-02", "2025-01-03", "2025-01-02", "2025-01-03")
    assert g.range_overlaps("2025-01-01", None, "2026-07-01", "2026-07-02")


def test_select_partitions_by_bounds_keeps_default():
    """Из 2000 партиций остаются попавшие в день + DEFAULT (в неё могло попасть)."""
    import modules.gpcopy as g

    children = [
        ("t_1_prt_998", "FOR VALUES FROM ('2025-01-02') TO ('2025-01-03')"),
        ("t_1_prt_999", "FOR VALUES FROM ('2025-01-03') TO ('2025-01-04')"),
        ("t_1_prt_other", "DEFAULT"),
    ]

    assert g.select_partitions_by_bounds(children, "2025-01-03", "2025-01-04") == [
        "t_1_prt_999", "t_1_prt_other",
    ]


def test_expand_date_entries_keeps_plain_tables():
    """Обычная таблица проходит без изменений (в т.ч. если структура неизвестна)."""
    import modules.gpcopy as g

    entry = {
        "schema": "s", "table": "t",
        "dest_schema": "s", "dest_table": "t",
        "date_column": "d",
        "sql": "SELECT 1",
    }

    assert g.expand_date_entries_to_leaves(
        [entry], {("s", "t"): [("s", "t")]}, "2026-01-01", "2026-01-02"
    ) == [entry]

    # каталог не отдал структуру — ведём себя как раньше
    assert g.expand_date_entries_to_leaves(
        [entry], {}, "2026-01-01", "2026-01-02"
    ) == [entry]


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
