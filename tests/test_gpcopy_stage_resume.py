"""
Частичный сбой gpcopy при загрузке через промежуточные таблицы.

Случай с живого кластера: 260 из 261 партиции доехали, одна упала на
сетевой ошибке — и всё скопированное (9.4 ТБ) выбрасывалось. Теперь
скопированное сохраняется, «Дозагрузить упавшие» догружает только
недоехавшее и переливает всё в цель, «Удалить промежуточные» — отказ.
"""

import json

import pytest

import app as app_module
import modules.gpcopy as gp
from modules import gpcopy_stage as st
from job_manager import create_job, get_job, get_job_items


CONN = {"host": "gp-host", "port": 5432, "username": "gpadmin",
        "database_name": "adb"}


class _FakePg:
    autocommit = False

    def set_session(self, **kw):
        pass

    def close(self):
        pass


def _entry(i):
    return {"source": "adb.s.a_prt_{}".format(i),
            "dest": "adb.opsentri_gpcopy_stage.j1_{:05d}".format(i),
            "leaf": ["s", "a_prt_{}".format(i)]}


def _merge(n=3):
    return {"item": ["s", "a"], "target": ["arch", "a_copy"], "truncate": True,
            "stages": [["opsentri_gpcopy_stage", "j1_{:05d}".format(i)]
                       for i in range(1, n + 1)],
            "entries": [_entry(i) for i in range(1, n + 1)]}


# ------------------------------------------------------------ чистые функции

def test_partial_failure_keeps_stages_with_pending():
    kept, dropped = st.kept_after_failure(
        [_merge()], {("s", "a_prt_1"), ("s", "a_prt_3")})

    assert dropped == []
    assert [e["leaf"] for e in kept[0]["pending"]] == [["s", "a_prt_2"]]
    assert [e["done"] for e in kept[0]["entries"]] == [True, False, True]


def test_nothing_copied_means_nothing_to_keep():
    kept, dropped = st.kept_after_failure([_merge()], set())

    assert kept == [] and len(dropped) == 1


def test_done_from_previous_attempt_is_not_repeated():
    """Во второй попытке лог содержит только дозагруженное."""
    first, _ = st.kept_after_failure([_merge()], {("s", "a_prt_1")})
    merges, _ = st.resume_plan(first)

    second, _ = st.kept_after_failure(merges, {("s", "a_prt_2")})

    assert [e["leaf"] for e in second[0]["pending"]] == [["s", "a_prt_3"]]


def test_resume_plan_sends_only_pending_to_gpcopy():
    kept, _ = st.kept_after_failure([_merge()], {("s", "a_prt_1")})

    merges, entries = st.resume_plan(kept)

    assert entries == [
        {"source": "adb.s.a_prt_2",
         "dest": "adb.opsentri_gpcopy_stage.j1_00002"},
        {"source": "adb.s.a_prt_3",
         "dest": "adb.opsentri_gpcopy_stage.j1_00003"},
    ]
    assert "pending" not in merges[0]
    assert len(merges[0]["stages"]) == 3        # переливаются все


def test_split_full_name_handles_quotes():
    assert st.split_full_name('adb."Sales"."a""b"') == ("Sales", 'a"b')
    assert st.split_full_name("adb.s.t") == ("s", "t")
    assert st.split_full_name("bad") == (None, None)


# ------------------------------------------------------------ завершение задачи

LOG_ONE_FAILED = (
    'Finished copying table "adb"."s"."a_prt_1" => '
    '"adb"."opsentri_gpcopy_stage"."j1_00001"\n'
    'Failed to copy table "adb"."s"."a_prt_2" => '
    '"adb"."opsentri_gpcopy_stage"."j1_00002"\n')


def test_partial_failure_keeps_stage_tables(monkeypatch):
    dropped = []
    monkeypatch.setattr(gp, "finish_stage_merges",
                        lambda config, apply, merges=None:
                        dropped.append(list(merges or [])) or {})

    cfg = {"source_connection_id": 1, "dest_connection_id": 2,
           "tables": [{"schema": "s", "table": "a"}],
           "targets": {"s.a": "arch.a_copy"},
           "stage_merges": [_merge(2)]}
    job_id = create_job("gpcopy", 1, cfg)

    gp.finalize_gpcopy_job(job_id, get_job_items(job_id), 1, LOG_ONE_FAILED,
                           "", "cmd", 1, cfg)

    saved = json.loads(get_job(job_id)["config_json"])
    assert "stage_merges" not in saved
    assert [e["leaf"] for e in saved["stage_kept"][0]["pending"]] == \
        [["s", "a_prt_2"]]
    assert dropped == [[]]                       # ничего не удалено
    item = get_job_items(job_id)[0]
    assert item["status"] == "failed"
    assert "скопировано 1 из 2" in item["error_message"]
    assert "Дозагрузить упавшие" in item["error_message"]


def test_kept_stages_survive_sweep():
    job_id = create_job("gpcopy", 1, {"stage_kept": [_merge()]})

    # задача не идёт, но держит промежуточные таблицы для дозагрузки
    assert gp._job_is_active(job_id) is True


# ------------------------------------------------------------ дозагрузка

def test_retry_resumes_through_kept_stages(client, monkeypatch):
    started = []

    class _NowThread:
        def __init__(self, target, args, daemon):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(app_module, "run_gpcopy_job",
                        lambda job_id: started.append(job_id))
    monkeypatch.setattr(app_module.threading, "Thread", _NowThread)

    kept = [dict(_merge(2), pending=[_entry(2)])]
    parent = create_job("gpcopy", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "targets": {"s.a": "arch.a_copy"},
        "failed_leaves": [["s", "a_prt_2"]],
        "stage_kept": kept, "truncate": True})

    r = client.post("/api/gpcopy/retry-failed", json={"job_id": parent})

    body = r.get_json()
    assert body["ok"] is True, body
    resume = json.loads(get_job(body["job_id"])["config_json"])
    assert resume["stage_resume"] == kept
    assert resume["targets"] == {"s.a": "arch.a_copy"}
    assert resume["truncate"] is True
    # упавшая партиция таблицы с картой не ушла обычной дозагрузкой
    assert body["job_ids"] == [body["job_id"]]
    # промежуточные теперь у дозагрузки
    assert "stage_kept" not in json.loads(get_job(parent)["config_json"])
    assert started == [body["job_id"]]


def test_prepare_resume_sends_only_pending(monkeypatch):
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: _FakePg())
    monkeypatch.setattr(st, "missing_stage_tables", lambda conn, merges: [])

    kept = [_merge(2)]
    kept[0]["entries"][0]["done"] = True
    cfg = {"source_connection_id": 1, "dest_connection_id": 2,
           "tables": [{"schema": "s", "table": "a"}], "stage_resume": kept}
    job_id = create_job("gpcopy", 1, cfg)

    path = gp.prepare_stage_resume(job_id, cfg, CONN)

    with open(path, encoding="utf-8") as f:
        assert json.load(f) == [{
            "source": "adb.s.a_prt_2",
            "dest": "adb.opsentri_gpcopy_stage.j1_00002"}]
    saved = json.loads(get_job(job_id)["config_json"])
    assert len(saved["stage_merges"][0]["stages"]) == 2   # перелив всех
    assert "stage_resume" not in saved


def test_prepare_resume_without_pending_skips_gpcopy(monkeypatch):
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: _FakePg())
    monkeypatch.setattr(st, "missing_stage_tables", lambda conn, merges: [])

    kept = [_merge(1)]
    kept[0]["entries"][0]["done"] = True
    cfg = {"stage_resume": kept}

    assert gp.prepare_stage_resume(
        create_job("gpcopy", 1, dict(cfg)), cfg, CONN) is None


def test_prepare_resume_fails_if_stages_are_gone(monkeypatch):
    monkeypatch.setattr(gp, "open_psycopg2_connection_by_cfg",
                        lambda cfg: _FakePg())
    monkeypatch.setattr(st, "missing_stage_tables",
                        lambda conn, merges: [("opsentri_gpcopy_stage",
                                               "j1_00001")])
    cfg = {"stage_resume": [_merge()]}

    with pytest.raises(Exception) as err:
        gp.prepare_stage_resume(create_job("gpcopy", 1, dict(cfg)), cfg, CONN)

    assert "перезапусти копирование целиком" in str(err.value)


# ------------------------------------------------------------ отказ

def test_drop_stages_button(client, monkeypatch):
    dropped = []
    monkeypatch.setattr(gp, "finish_stage_merges",
                        lambda config, apply, merges=None:
                        dropped.append(list(merges)) or {})

    job_id = create_job("gpcopy", 1, {
        "source_connection_id": 1, "dest_connection_id": 2,
        "tables": [{"schema": "s", "table": "a"}],
        "stage_kept": [_merge()]})

    r = client.post("/api/gpcopy/jobs/{}/drop-stages".format(job_id))

    assert r.get_json()["ok"] is True
    assert dropped == [[_merge()]]
    assert "stage_kept" not in json.loads(get_job(job_id)["config_json"])


def test_drop_stages_without_kept_is_rejected(client):
    job_id = create_job("gpcopy", 1, {"source_connection_id": 1,
                                      "dest_connection_id": 2})

    r = client.post("/api/gpcopy/jobs/{}/drop-stages".format(job_id))

    assert r.status_code == 400
