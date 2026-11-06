# -*- coding: utf-8 -*-
"""Письма о статусе бэкапов и индикатор хода gpbackup / gprestore."""

import pytest

import modules.backup_notify as bn
import modules.gpbackup as gb
from db import sqlite_cursor
from job_manager import create_job, mark_job_failed
from modules.connections import create_connection


class FakeSMTP:
    sent = []

    def __init__(self, host, port, timeout=None, context=None):
        self.host, self.port = host, port
        self.calls = []

    def ehlo(self):
        pass

    def starttls(self, context=None):
        self.calls.append("starttls")

    def login(self, user, password):
        self.calls.append(("login", user, password))

    def send_message(self, msg, from_addr=None, to_addrs=None):
        FakeSMTP.sent.append({"msg": msg, "to": to_addrs, "calls": self.calls,
                              "host": self.host})

    def quit(self):
        pass


@pytest.fixture
def smtp(monkeypatch):
    FakeSMTP.sent = []
    monkeypatch.setattr(bn.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(bn.smtplib, "SMTP_SSL", FakeSMTP)
    with sqlite_cursor(commit=True) as cur:
        cur.execute("DELETE FROM mail_settings")
    bn.save_mail_settings({"host": "smtp.corp.kz", "port": 587,
                           "security": "starttls", "from_addr": "ops@corp.kz",
                           "username": "ops", "password": "synthetic-pass"})
    return FakeSMTP


def _cluster(name):
    return create_connection({"name": name, "host": "mdw", "port": 5432,
                              "database_name": "adb", "username": "gpadmin"})


# ------------------------------------------------------------ настройки

def test_mail_settings_never_return_password(client, smtp):
    d = client.get("/api/mail-settings").get_json()

    assert d["mail"]["has_password"] is True
    assert "password" not in d["mail"]
    assert "synthetic-pass" not in str(d)

    # пустой пароль при сохранении — прежний остаётся
    bn.save_mail_settings({"host": "smtp.corp.kz", "port": 25,
                           "security": "none", "from_addr": "ops@corp.kz",
                           "username": "ops"})
    assert bn.get_mail_settings(with_password=True)["password"] == "synthetic-pass"


@pytest.mark.parametrize("data", [
    {"host": "", "from_addr": "a@b.kz"},
    {"host": "smtp;rm", "from_addr": "a@b.kz"},
    {"host": "smtp", "from_addr": "not-an-email"},
    {"host": "smtp", "from_addr": "a@b.kz", "port": 70000},
    {"host": "smtp", "from_addr": "a@b.kz", "security": "tls13"},
    {"host": "smtp", "from_addr": "a@b.kz", "public_url": "javascript:alert(1)"},
])
def test_mail_settings_validation(data):
    with pytest.raises(ValueError):
        bn.save_mail_settings(data)


def test_recipients_parsed_and_validated():
    assert bn.parse_recipients("a@corp.kz, b@corp.kz\nA@corp.kz;c@corp.kz") == \
        ["a@corp.kz", "b@corp.kz", "c@corp.kz"]

    with pytest.raises(ValueError):
        bn.parse_recipients("a@corp.kz, evil\r\nBcc: x@y.z")


# ------------------------------------------------------------ письмо

def test_message_escapes_and_has_status():
    subject, text, body = bn.build_message({
        "op": "restore", "status": "failed", "cluster": "<b>TEST</b>",
        "dbname": "adb", "reason": "Не выполнено 1: ERROR: <x> already exists",
        "failed": [{"error": "ERROR: <x>", "statement": "CREATE TABLE <x>"}],
        "hint": "Объект уже есть", "log_tail": "line <1>",
    })

    assert "ОШИБКА" in subject and "Восстановление" in subject
    assert "<b>TEST</b>" not in body and "&lt;b&gt;TEST&lt;/b&gt;" in body
    assert "CREATE TABLE &lt;x&gt;" in body
    assert "Объект уже есть" in text


def test_partial_backup_mail_lists_skipped(client, smtp):
    cid = _cluster("gp-mail-partial")
    bn.save_backup_notify(cid, {"recipients": "dba@corp.kz", "on_done": False})
    cfg = {"connection_id": cid, "dbname": "adb", "backup_type": "full"}
    job_id = create_job("gpbackup", cid, cfg)
    log = (
        "20261106:02:00:01 gpbackup:gpadmin:mdw:1-[INFO]:-Backup Timestamp = 20261106020001\n"
        "20261106:02:10:00 gpbackup:gpadmin:mdw:1-[WARNING]:-Worker 2: skipping "
        "table s.bad1 due to error: boom\n"
        "20261106:02:20:01 gpbackup:gpadmin:mdw:1-[WARNING]:-Backup completed "
        "with status Partial: 1 table(s) were skipped and are NOT in this backup: s.bad1\n"
    )
    gb._finalize_backup_job(job_id, "gpbackup", cfg, [], log, 3, "gpbackup")

    assert bn.notify_job(job_id) is True
    sent = smtp.sent[-1]
    assert sent["to"] == ["dba@corp.kz"]
    assert sent["calls"][0] == "starttls"
    assert "частично" in sent["msg"]["Subject"]
    assert "s.bad1" in sent["msg"].get_body(("plain",)).get_content()
    assert bn.get_backup_notify(cid)["last_sent_at"]


def test_events_filter_and_restore_switch(client, smtp):
    cid = _cluster("gp-mail-filter")
    bn.save_backup_notify(cid, {"recipients": "dba@corp.kz", "on_done": False,
                                "on_restore": False})
    job_id = create_job("gprestore", cid, {"connection_id": cid,
                                           "backup_timestamp": "20261106020001"})
    mark_job_failed(job_id, "boom")

    assert bn.notify_job(job_id) is False          # восстановления выключены
    assert smtp.sent == []

    bn.save_backup_notify(cid, {"recipients": "dba@corp.kz", "on_restore": True})
    assert bn.notify_job(job_id) is True
    assert "ОШИБКА" in smtp.sent[-1]["msg"]["Subject"]


def test_smtp_failure_is_recorded_not_raised(client, monkeypatch, smtp):
    cid = _cluster("gp-mail-fail")
    bn.save_backup_notify(cid, {"recipients": "dba@corp.kz"})
    job_id = create_job("gprestore", cid, {"connection_id": cid})
    mark_job_failed(job_id, "boom")

    def refuse(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(bn.smtplib, "SMTP", refuse)

    assert bn.notify_job(job_id) is False
    assert "connection refused" in bn.get_backup_notify(cid)["last_error"]


def test_notify_api_roundtrip(client, smtp):
    cid = _cluster("gp-mail-api")

    r = client.post("/api/backup/notify", json={"connection_id": cid,
                                                "recipients": "x"})
    assert r.status_code == 400

    d = client.post("/api/backup/notify", json={
        "connection_id": cid, "recipients": "dba@corp.kz", "on_done": False,
    }).get_json()
    assert d["notify"]["recipients"] == ["dba@corp.kz"]
    assert d["notify"]["on_done"] is False
    assert d["mail"]["configured"] is True

    t = client.post("/api/backup/notify/test", json={"connection_id": cid}).get_json()
    assert t["ok"] is True and t["sent_to"] == ["dba@corp.kz"]
    assert "Тест" in smtp.sent[-1]["msg"]["Subject"]


# ------------------------------------------------------------ ход задачи

def test_progress_backup_counter():
    log = ("x [INFO]:-Gathering table state information\n"
           "x [INFO]:-Writing data to file\n"
           "Tables backed up:  12 / 48 [=====>     ]  25.00%\r"
           "Tables backed up:  24 / 48 [==========>]  50.00%\r")
    p = gb.parse_progress(log, "gpbackup")

    assert p["phase"] == "Запись данных"
    assert (p["done"], p["total"]) == (24, 48)
    assert 55 < p["percent"] < 62          # 20 + 77 * 0.5


def test_progress_restore_phases():
    log = ("x [INFO]:-Restoring pre-data metadata\n"
           "Pre-data objects restored:  2 / 2  100.00% 0s\n"
           "x [INFO]:-Pre-data metadata restore completed with failures\n"
           "x [INFO]:-Restored data to table public.t from file (table 3 of 4)\n")
    p = gb.parse_progress(log, "gprestore")

    assert p["phase"] == "Загрузка данных" and p["done"] == 3
    assert gb.parse_progress("", "gpbackup")["phase"] == "Запуск"


def test_progress_api_lists_running(client, tmp_path):
    cid = _cluster("gp-progress")
    job_id = create_job("gpbackup", cid, {"connection_id": cid, "backup_type": "full"})
    log = tmp_path / "job.log"
    log.write_text("x [INFO]:-Writing data to file\nTables backed up: 1 / 4\n",
                   encoding="utf-8")
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE jobs SET status='running', started_at=?, log_file=? "
                    "WHERE id=?", ("2026-11-06 02:00:00", str(log), job_id))

    d = client.get("/api/backup/progress?connection_id={}".format(cid)).get_json()

    assert d["ok"] is True
    run = d["running"][0]
    assert run["job_id"] == job_id and run["total"] == 4
    assert run["phase"] == "Запись данных"
