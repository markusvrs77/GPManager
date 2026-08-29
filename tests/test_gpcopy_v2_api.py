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


def test_failure_report_keeps_stderr_when_stdout_is_huge():
    """
    Лог многочасового gpcopy вытеснял STDERR из отчёта, и причина падения
    не доходила до интерфейса. Теперь STDERR идёт первым, от STDOUT — хвост.
    """
    import modules.gpcopy as g

    stdout = "\n".join("INFO: copying table t{}".format(i) for i in range(20000))
    stderr = "Error: ERROR: child process exited with exit code 1"

    report = g.build_failure_report("gpcopy --truncate", 1, stdout, stderr)

    assert "child process exited with exit code 1" in report
    # STDERR раньше хвоста STDOUT
    assert report.index("STDERR:") < report.index("STDOUT (последние строки)")
    # хвост, а не начало лога
    assert "copying table t19999" in report
    assert "copying table t0\n" not in report


def test_parse_gpcopy_summary_and_failed_leaves():
    """Из падения 2/2131 партиций видно, что почти всё скопировалось."""
    import modules.gpcopy as g

    log = (
        '20260725:16:39:12 gpcopy:-[ERROR]:-[Worker 1] Failed to copy table '
        '"adb"."dwh_stage"."s01_t_bal_1_prt_995" => '
        '"adb"."dwh_stage"."s01_t_bal_1_prt_995"\n'
        '20260725:16:26:11 gpcopy:-[ERROR]:-[Worker 2] Failed to copy table '
        '"adb"."dwh_stage"."s01_t_bal_1_prt_1191" => '
        '"adb"."dwh_stage"."s01_t_bal_1_prt_1191"\n'
        '20260725:16:41:18 gpcopy:-[INFO]:-\tDatabase adb: successfully copied '
        '2129 tables, skipped 0 tables, failed 2 tables\n'
    )

    assert g.parse_gpcopy_summary(log) == {"copied": 2129, "skipped": 0, "failed": 2}

    failed = g.parse_failed_leaf_tables(log)
    assert set(failed) == {
        ("dwh_stage", "s01_t_bal_1_prt_995"),
        ("dwh_stage", "s01_t_bal_1_prt_1191"),
    }

    assert g.parse_gpcopy_summary("no summary here") is None


def test_is_gpcopy_success():
    """После рестарта rc неизвестен — успех определяем по сводке gpcopy."""
    import modules.gpcopy as g

    assert g.is_gpcopy_success(0, None) is True
    assert g.is_gpcopy_success(1, None) is False
    # переподхват: rc=None, сводка без упавших -> успех
    assert g.is_gpcopy_success(None, {"copied": 10, "skipped": 0, "failed": 0}) is True
    assert g.is_gpcopy_success(None, {"copied": 9, "skipped": 0, "failed": 1}) is False
    # rc=None и сводки нет (лог оборван) -> не считаем успехом
    assert g.is_gpcopy_success(None, None) is False


def test_pid_alive():
    import os

    import modules.gpcopy as g

    assert g.pid_alive(os.getpid()) is True
    assert g.pid_alive(None) is False
    assert g.pid_alive(999999999) is False


def test_find_owner_item():
    """Партиция из лога относится к своей таблице; точное имя важнее префикса."""
    import modules.gpcopy as g

    keys = [
        (1, "dwh_stage", "s01_t_bal"),
        (2, "dwh_stage", "s01_t_operjrn"),
        (3, "public", "plain_table"),
    ]

    assert g.find_owner_item("dwh_stage", "s01_t_bal_1_prt_995", keys) == 1
    assert g.find_owner_item("dwh_stage", "s01_t_operjrn_1_def_pr_x", keys) == 2
    # непартиционированная таблица приходит своим именем
    assert g.find_owner_item("public", "plain_table", keys) == 3
    # чужая схема/таблица — никому
    assert g.find_owner_item("other", "s01_t_bal_1_prt_1", keys) is None
    assert g.find_owner_item("dwh_stage", "unknown_1_prt_1", keys) is None


def test_build_retry_config():
    """Ретрай перезаливает только упавшие партиции, целиком (--truncate)."""
    import pytest

    import modules.gpcopy as g

    config = {
        "source_connection_id": 1,
        "dest_connection_id": 2,
        "selected_tables": [{"schema": "dwh_stage", "table": "s01_t_bal"}],
        "gpcopy_path": "/usr/local/bin/gpcopy",
        "jobs": 4,
        "no_ownership": True,
        "append": True,
        "truncate": False,
        "failed_leaves": [
            ["dwh_stage", "s01_t_bal_1_prt_995"],
            ["dwh_stage", "s01_t_bal_1_prt_1191"],
        ],
    }

    retry = g.build_retry_config(
        config,
        [("dwh_stage", "s01_t_bal_1_prt_995"),
         ("dwh_stage", "s01_t_bal_1_prt_1191")],
    )

    assert retry["selected_tables"] == retry["expanded_tables"] == [
        {"schema": "dwh_stage", "table": "s01_t_bal_1_prt_995"},
        {"schema": "dwh_stage", "table": "s01_t_bal_1_prt_1191"},
    ]
    # партиции перезаливаются целиком, настройки подключения сохраняются
    assert retry["truncate"] is True
    assert retry["append"] is False
    assert retry["no_ownership"] is True
    assert retry["jobs"] == 4
    assert "failed_leaves" not in retry
    # исходный конфиг не изменён
    assert config["append"] is True

    # выбранный режим существующих таблиц действует и на дозагрузку
    retry_drop = g.build_retry_config(
        config, [("dwh_stage", "s01_t_bal_1_prt_995")],
        existing_mode="drop",
    )
    assert retry_drop["drop"] is True
    assert retry_drop["truncate"] is False

    with pytest.raises(ValueError):
        g.build_retry_config(
            config, [("s", "t")], existing_mode="explode",
        )

    with pytest.raises(ValueError):
        g.build_retry_config(config, [])

    with pytest.raises(ValueError):
        g.build_retry_config(
            {"mode": "date_filter"}, [("s", "t_1_prt_1")]
        )


def test_leaf_belongs_to_item():
    """Партиции относим к родителю; чужие таблицы не цепляем."""
    import modules.gpcopy as g

    assert g.leaf_belongs_to_item("s", "t_1_prt_5", "s", "t")
    assert g.leaf_belongs_to_item("s", "t_1_def_pr_x", "s", "t")
    assert g.leaf_belongs_to_item("s", "t", "s", "t")           # непартиц.
    # другая схема
    assert not g.leaf_belongs_to_item("other", "t_1_prt_5", "s", "t")
    # другая таблица с похожим префиксом
    assert not g.leaf_belongs_to_item("s", "t_hist_1_prt_5", "s", "t")


def test_parse_progress_counter():
    """Детальный live-процент берём из счётчика '(done/total) tables done'."""
    import modules.gpcopy as g

    assert g.parse_progress_counter(
        "20260724:15:42:14 gpcopy:-[INFO]:-[Worker 2] "
        "[Progress: (0/1) DBs, (5/5216) tables done] Finished copying table"
    ) == (5, 5216)

    assert g.parse_progress_counter(
        "[Progress: (1/1) DBs, (5216/5216) tables done]"
    ) == (5216, 5216)

    assert g.parse_progress_counter("just an info line") is None
    assert g.parse_progress_counter("") is None


def test_parse_finished_tables():
    """Скопированные до падения таблицы не должны попадать в failed."""
    import modules.gpcopy as g

    stdout = (
        '20260724:15:42:14 gpcopy:-[INFO]:-[Worker 2] Start copying table '
        '"adb"."dwh_stage"."s01_t_trnatr" => "adb"."dwh_stage"."s01_t_trnatr"\n'
        '20260724:15:42:14 gpcopy:-[INFO]:-[Worker 2] [Progress: (0/1) DBs, '
        '(1/5216) tables done] Finished copying table '
        '"adb"."dwh_stage"."s01_t_trnatr" => "adb"."dwh_stage"."s01_t_trnatr"\n'
        '20260724:15:43:39 gpcopy:-[INFO]:-[Worker 1] [Progress: (0/1) DBs, '
        '(3/5216) tables done] Finished copying table '
        '"adb"."dwh_stage"."s01_n_crdinekv" => "adb"."dwh_stage"."s01_n_crdinekv"\n'
    )

    assert g.parse_finished_tables(stdout) == {
        ("dwh_stage", "s01_t_trnatr"),
        ("dwh_stage", "s01_n_crdinekv"),
    }

    # "Start copying" без "Finished" не считается
    assert g.parse_finished_tables(
        '[INFO]:-Start copying table "adb"."s"."t" => "adb"."s"."t"'
    ) == set()

    assert g.parse_finished_tables("") == set()


def test_extract_error_lines():
    """По таблицам показываем строки с ошибками, а не срез начала лога."""
    import modules.gpcopy as g

    stdout = (
        "20260724:15:42:14 gpcopy:-[INFO]:-Start copying table\n"
        "20260724:17:50:01 gpcopy:-[ERROR]:-[Worker 2] Finished task 178221_P_DA_V_ "
        "with error: ERROR: child process exited with exit code 1\n"
        "20260724:17:50:01 gpcopy:-[INFO]:-done\n"
    )

    lines = g.extract_error_lines(stdout, "")

    assert len(lines) == 1
    assert lines[0].endswith("exit code 1")

    # дубли не копим, порядок — последние ошибки
    assert g.extract_error_lines("ERROR: a\nERROR: a\nERROR: b", "") == [
        "ERROR: a", "ERROR: b",
    ]
    assert g.extract_error_lines("all fine", "") == []


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


def test_retry_config_from_partition_job():
    """Дозагрузка упавших работает и для синхронизации партиций."""
    import modules.gpcopy as g

    config = {
        "source_connection_id": 1,
        "dest_connection_id": 2,
        "tables": [{"schema": "dwh_stage", "table": "s01_t_bal"}],
        "count_mode": "stats",
        "recompute": True,
        "partitions": [{"schema": "dwh_stage", "table": "s01_t_bal_1_prt_1"}],
        "failed_leaves": [["dwh_stage", "s01_t_bal_1_prt_1538"]],
    }

    retry = g.build_retry_config(
        config, [("dwh_stage", "s01_t_bal_1_prt_1538")])

    # льём ровно упавшую партицию
    assert retry["selected_tables"] == [
        {"schema": "dwh_stage", "table": "s01_t_bal_1_prt_1538"}]
    assert retry["expanded_tables"] == retry["selected_tables"]
    assert retry["truncate"] is True

    # партиционная специфика в обычную gpcopy-задачу не тащится
    for key in ("partitions", "recompute", "count_mode", "failed_leaves"):
        assert key not in retry

    # подключения сохраняются
    assert retry["source_connection_id"] == 1
    assert retry["dest_connection_id"] == 2
