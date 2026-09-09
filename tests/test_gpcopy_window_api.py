"""
HTTP-слой окна с очисткой: приём флага в задании и превью удаления.

Отдельно от tests/test_gpcopy_window_cleanup.py — там проверяется сама
функция, здесь то, что маршруты доносят до неё правильные аргументы и не
дают собрать заведомо опасное задание.
"""

import app as app_module


TABLES = [{"schema": "dwh_bi", "table": "fact_ops",
           "source": "dwh_bi.fact_ops", "dest": "dwh_bi.fact_ops",
           "date_column": "created_at"}]

FROM = "2026-09-01"
TO = "2026-09-02"


def _capture_job(monkeypatch):
    """Перехватывает конфигурацию задания вместо создания настоящего."""
    box = {}

    def fake_create_job(job_type, connection_id, config, **kw):
        box["job_type"] = job_type
        box["config"] = config
        return 777

    monkeypatch.setattr(app_module, "create_job", fake_create_job)
    monkeypatch.setattr(app_module, "start_job_thread", lambda *a, **kw: None,
                        raising=False)
    return box


def _start(client, **extra):
    body = {
        "source_connection_id": 1, "dest_connection_id": 2,
        "date_from": FROM, "date_to": TO,
        "table_configs": TABLES,
    }
    body.update(extra)
    return client.post("/api/gpcopy/start-date", json=body)


def test_cleanup_flag_reaches_job_config(client, monkeypatch):
    """Флаг доезжает до задания, вставка при этом остаётся append."""
    box = _capture_job(monkeypatch)

    r = _start(client, window_cleanup=True)
    assert r.status_code == 200

    cfg = box["config"]
    assert cfg["window_cleanup"] is True
    assert cfg["append"] is True
    assert cfg["date_from"] == FROM and cfg["date_to"] == TO
    assert cfg["mode"] == "date_filter"


def test_without_flag_nothing_is_deleted(client, monkeypatch):
    """Прежнее поведение не меняется: без флага очистки нет."""
    box = _capture_job(monkeypatch)

    assert _start(client).status_code == 200
    assert box["config"]["window_cleanup"] is False


def test_cleanup_rejects_truncate(client, monkeypatch):
    """
    truncate стёр бы таблицу целиком, а не окно, и очистка теряет смысл.
    Такое задание не должно создаваться вовсе.
    """
    box = _capture_job(monkeypatch)

    r = _start(client, window_cleanup=True, truncate=True)

    assert r.status_code == 400
    assert "window_cleanup" in r.get_json()["message"]
    assert box == {}


def test_preview_counts_without_deleting(client, monkeypatch):
    """Превью обязано звать очистку только в режиме подсчёта."""
    seen = {}

    def fake_clear(dest_id, configs, date_from, date_to, dry_run=False):
        seen.update(dest_id=dest_id, dry_run=dry_run,
                    date_from=date_from, date_to=date_to)
        return [("dwh_bi", "fact_ops", 1200), ("dwh_bi", "fact_pay", 300)]

    monkeypatch.setattr(app_module, "clear_window_in_dest", fake_clear)

    r = client.post("/api/gpcopy/window-preview", json={
        "dest_connection_id": 2, "date_from": FROM, "date_to": TO,
        "table_configs": TABLES,
    })

    body = r.get_json()
    assert body["ok"] is True
    assert body["total_rows"] == 1500
    assert body["tables"][0] == {"schema": "dwh_bi", "table": "fact_ops",
                                "rows": 1200}
    assert seen["dry_run"] is True          # главное в этом тесте
    assert seen["dest_id"] == 2


def test_preview_rejects_empty_selection(client):
    r = client.post("/api/gpcopy/window-preview", json={
        "dest_connection_id": 2, "date_from": FROM, "date_to": TO,
        "table_configs": [],
    })

    assert r.status_code == 400
    assert r.get_json()["ok"] is False


def test_preview_reports_bad_window_as_client_error(client, monkeypatch):
    """Битая граница — ошибка запроса, а не пятисотка."""
    r = client.post("/api/gpcopy/window-preview", json={
        "dest_connection_id": 2, "date_from": "вчера", "date_to": TO,
        "table_configs": TABLES,
    })

    assert r.status_code == 400
    assert r.get_json()["ok"] is False
