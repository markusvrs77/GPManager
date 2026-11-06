# -*- coding: utf-8 -*-
"""
Письма о статусе бэкапов и восстановлений Greenplum.

Почтовый сервер — один на приложение (mail_settings, правит администратор),
получатели и события — у каждого кластера свои (backup_notify). Письмо
уходит после завершения любой задачи gpbackup / gprestore — ручной и по
расписанию — в фоне: сбой почты задачу не роняет, а записывается в
backup_notify.last_error и виден на странице.
"""

import html
import json
import re
import smtplib
import ssl
import threading
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate, make_msgid

from db import sqlite_cursor

SECURITY_MODES = ("none", "starttls", "ssl")

SMTP_TIMEOUT_SECONDS = 20

MAX_RECIPIENTS = 50

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_HOST_RE = re.compile(r"^[A-Za-z0-9.-]+$")

STATUS_TEXT = {
    "done": "успешно",
    "partial": "частично",
    "failed": "ошибка",
    "interrupted": "прервана",
}

STATUS_COLOR = {
    "done": "#0e8a5f",
    "partial": "#b7791f",
    "failed": "#c53030",
    "interrupted": "#b7791f",
}

TYPE_TEXT = {
    "full": "Полный", "incremental": "Инкремент",
    "metadata_only": "Метаданные", "data_only": "Данные",
}


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------
# почтовый сервер
# ------------------------------------------------------------------

def get_mail_settings(with_password=False):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM mail_settings WHERE id = 1")
        row = cur.fetchone()

    data = dict(row) if row else {}
    out = {
        "host": data.get("host") or "",
        "port": data.get("port") or 25,
        "security": data.get("security") or "starttls",
        "username": data.get("username") or "",
        "from_addr": data.get("from_addr") or "",
        "public_url": data.get("public_url") or "",
        "has_password": bool(data.get("password")),
        "updated_at": data.get("updated_at"),
    }
    out["configured"] = bool(out["host"] and out["from_addr"])

    if with_password:
        out["password"] = data.get("password") or ""

    return out


def save_mail_settings(data):
    """
    Проверка и сохранение SMTP. Пароль: непустой — заменить,
    clear_password — стереть, иначе прежний. ValueError — что не так.
    """
    data = data or {}
    host = str(data.get("host") or "").strip()
    from_addr = str(data.get("from_addr") or "").strip()
    security = str(data.get("security") or "starttls").strip()
    username = str(data.get("username") or "").strip()
    public_url = str(data.get("public_url") or "").strip().rstrip("/")

    if not host or not _HOST_RE.match(host):
        raise ValueError("Укажите SMTP-сервер (имя или IP)")

    try:
        port = int(data.get("port") or 25)
    except (TypeError, ValueError):
        raise ValueError("Порт — число")

    if not 1 <= port <= 65535:
        raise ValueError("Порт — от 1 до 65535")

    if security not in SECURITY_MODES:
        raise ValueError("Защита: none, starttls или ssl")

    if not _EMAIL_RE.match(from_addr):
        raise ValueError("Адрес отправителя некорректен")

    if public_url and not re.match(r"^https?://[^\s<>\"']+$", public_url):
        raise ValueError("Адрес Opsentri — вида https://host:port")

    current = get_mail_settings(with_password=True)
    password = current["password"]

    if data.get("clear_password"):
        password = ""
    elif data.get("password"):
        password = str(data["password"])

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO mail_settings (id, host, port, security, username,
                                       password, from_addr, public_url,
                                       updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                host = excluded.host, port = excluded.port,
                security = excluded.security, username = excluded.username,
                password = excluded.password, from_addr = excluded.from_addr,
                public_url = excluded.public_url,
                updated_at = excluded.updated_at
            """,
            (host, port, security, username, password or None, from_addr,
             public_url, _now()),
        )

    return get_mail_settings()


# ------------------------------------------------------------------
# получатели кластера
# ------------------------------------------------------------------

def parse_recipients(raw):
    """Адреса через запятую, точку с запятой или с новой строки."""
    items = raw if isinstance(raw, list) else re.split(r"[\s,;]+", str(raw or ""))
    out = []

    for item in items:
        addr = str(item).strip()

        if not addr:
            continue

        if not _EMAIL_RE.match(addr):
            raise ValueError("Некорректный адрес: {}".format(addr))

        if addr.lower() not in [a.lower() for a in out]:
            out.append(addr)

    if len(out) > MAX_RECIPIENTS:
        raise ValueError("Не больше {} получателей".format(MAX_RECIPIENTS))

    return out


def get_backup_notify(connection_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM backup_notify WHERE connection_id = ?",
                    (int(connection_id),))
        row = cur.fetchone()

    data = dict(row) if row else {}

    return {
        "connection_id": int(connection_id),
        "configured": bool(row),
        "enabled": bool(data.get("enabled", 1)),
        "recipients": [r for r in (data.get("recipients") or "").splitlines() if r],
        "on_done": bool(data.get("on_done", 1)),
        "on_partial": bool(data.get("on_partial", 1)),
        "on_failed": bool(data.get("on_failed", 1)),
        "on_restore": bool(data.get("on_restore", 1)),
        "last_sent_at": data.get("last_sent_at"),
        "last_error": data.get("last_error"),
    }


def save_backup_notify(connection_id, data):
    data = data or {}
    recipients = parse_recipients(data.get("recipients"))

    def flag(name):
        return 1 if data.get(name, True) else 0

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO backup_notify (connection_id, enabled, recipients,
                                       on_done, on_partial, on_failed,
                                       on_restore)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
                enabled = excluded.enabled, recipients = excluded.recipients,
                on_done = excluded.on_done, on_partial = excluded.on_partial,
                on_failed = excluded.on_failed,
                on_restore = excluded.on_restore
            """,
            (int(connection_id), flag("enabled"), "\n".join(recipients),
             flag("on_done"), flag("on_partial"), flag("on_failed"),
             flag("on_restore")),
        )

    return get_backup_notify(connection_id)


def _record_result(connection_id, error):
    with sqlite_cursor(commit=True) as cur:
        if error:
            cur.execute("UPDATE backup_notify SET last_error = ? "
                        "WHERE connection_id = ?",
                        (str(error)[:1000], int(connection_id)))
        else:
            cur.execute("UPDATE backup_notify SET last_sent_at = ?, "
                        "last_error = NULL WHERE connection_id = ?",
                        (_now(), int(connection_id)))


# ------------------------------------------------------------------
# отправка
# ------------------------------------------------------------------

def _header(value):
    """Без переводов строк: имя кластера не должно дописать заголовок."""
    return re.sub(r"[\r\n]+", " ", str(value or "")).strip()


def send_mail(recipients, subject, text, html_body, settings=None):
    """SMTP по настройкам mail_settings. Исключение — причина сбоя."""
    settings = settings or get_mail_settings(with_password=True)

    if not settings.get("configured"):
        raise RuntimeError("Почтовый сервер не настроен")

    if not recipients:
        raise RuntimeError("Нет получателей")

    msg = EmailMessage()
    msg["Subject"] = _header(subject)
    msg["From"] = settings["from_addr"]
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=settings["from_addr"].split("@")[-1])
    msg.set_content(text)
    msg.add_alternative(html_body, subtype="html")

    host, port = settings["host"], int(settings["port"] or 25)
    security = settings.get("security") or "starttls"

    if security == "ssl":
        smtp = smtplib.SMTP_SSL(host, port, timeout=SMTP_TIMEOUT_SECONDS,
                                context=ssl.create_default_context())
    else:
        smtp = smtplib.SMTP(host, port, timeout=SMTP_TIMEOUT_SECONDS)

    try:
        smtp.ehlo()

        if security == "starttls":
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()

        if settings.get("username"):
            smtp.login(settings["username"], settings.get("password") or "")

        smtp.send_message(msg, from_addr=settings["from_addr"],
                          to_addrs=recipients)
    finally:
        try:
            smtp.quit()
        except Exception:
            pass


# ------------------------------------------------------------------
# содержимое письма
# ------------------------------------------------------------------

def _fmt_ts(ts):
    ts = str(ts or "")

    if len(ts) != 14:
        return ts or "—"

    return "{}.{}.{} {}:{}:{}".format(ts[6:8], ts[4:6], ts[:4],
                                       ts[8:10], ts[10:12], ts[12:14])


def _fmt_dur(sec):
    if sec is None:
        return "—"

    return "{:02d}:{:02d}:{:02d}".format(sec // 3600, sec % 3600 // 60, sec % 60)


def build_message(event):
    """
    event -> (subject, text, html). Поля event: op (backup|restore),
    status, cluster, host, dbname, backup_timestamp, backup_type,
    backup_dir, started_at, finished_at, duration_sec, job_id, summary,
    skipped, reason, failed, hint, log_tail, url, scope.
    """
    op = "Бэкап" if event.get("op") == "backup" else "Восстановление"
    status = event.get("status") or "failed"
    status_text = STATUS_TEXT.get(status, status)
    cluster = event.get("cluster") or "кластер"
    skipped = event.get("skipped") or []

    if status == "partial" and skipped:
        status_text += " — пропущено {} табл.".format(len(skipped))

    subject = "[Opsentri] {} {}: {} / {}".format(
        op, status_text.upper() if status == "failed" else status_text,
        cluster, event.get("dbname") or "")

    rows = [
        ("Кластер", "{} ({})".format(cluster, event.get("host") or "—")),
        ("База", event.get("dbname") or "—"),
        ("Операция", op + (" · задача #{}".format(event["job_id"])
                           if event.get("job_id") else "")),
        ("Статус", status_text),
        ("Копия", _fmt_ts(event.get("backup_timestamp"))),
    ]

    if event.get("backup_type"):
        rows.append(("Тип", TYPE_TEXT.get(event["backup_type"], event["backup_type"])))

    if event.get("scope"):
        rows.append(("Что", event["scope"]))

    rows += [
        ("Каталог", event.get("backup_dir") or "каталоги данных (по умолчанию)"),
        ("Начало", event.get("started_at") or "—"),
        ("Окончание", event.get("finished_at") or "—"),
        ("Длительность", _fmt_dur(event.get("duration_sec"))),
    ]

    summary = event.get("summary") or {}

    if summary.get("tables") is not None:
        line = "{} табл.".format(summary["tables"])

        if summary.get("schemas") is not None:
            line += ", схем: {}".format(summary["schemas"])

        if summary.get("db_size"):
            line += ", размер БД: {}".format(summary["db_size"])

        rows.append(("В копии", line))

    if event.get("reason"):
        rows.append(("Причина", event["reason"]))

    if event.get("hint"):
        rows.append(("Что делать", event["hint"]))

    failed = event.get("failed") or []
    tail = event.get("log_tail") or ""

    # ---- текст
    text = ["{} {} — {}".format(op, status_text, cluster), ""]
    text += ["{}: {}".format(k, v) for k, v in rows]

    if skipped:
        text += ["", "Не попали в копию (восстановятся пустыми):"]
        text += ["  " + t for t in skipped[:50]]
        if len(skipped) > 50:
            text.append("  … и ещё {}".format(len(skipped) - 50))

    if failed:
        text += ["", "Не выполнено ({}):".format(len(failed))]
        for f in failed[:10]:
            text += ["- " + f["error"], "  " + (f.get("statement") or "")[:300]]

    if tail:
        text += ["", "Хвост лога:", tail]

    if event.get("url"):
        text += ["", "Открыть: " + event["url"]]

    # ---- html
    e = html.escape
    color = STATUS_COLOR.get(status, "#c53030")

    body = [
        '<div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px;'
        'color:#1f2330;max-width:720px">',
        '<div style="border-left:4px solid {c};padding:10px 14px;'
        'background:#f6f6fb;margin-bottom:16px">'
        '<div style="font-size:12px;color:#6b6f80">Opsentri · Greenplum</div>'
        '<div style="font-size:18px;font-weight:700">{op}: '
        '<span style="color:{c}">{st}</span></div>'
        '<div style="color:#4a4e5e">{cl} / {db}</div></div>'.format(
            c=color, op=e(op), st=e(status_text), cl=e(cluster),
            db=e(event.get("dbname") or "")),
        '<table cellpadding="6" cellspacing="0" style="border-collapse:collapse;'
        'width:100%;font-size:13.5px">',
    ]

    for k, v in rows:
        body.append(
            '<tr><td style="color:#6b6f80;width:150px;vertical-align:top;'
            'border-bottom:1px solid #ececf3">{}</td>'
            '<td style="border-bottom:1px solid #ececf3">{}</td></tr>'.format(
                e(k), e(str(v))))

    body.append("</table>")

    if skipped:
        body.append('<h3 style="font-size:14px;color:#b7791f;margin:18px 0 6px">'
                    'Не попали в копию — восстановятся пустыми ({})</h3>'
                    '<div style="font-family:Consolas,monospace;font-size:12.5px">'
                    '{}</div>'.format(len(skipped), "<br>".join(
                        e(t) for t in skipped[:50]) + (
                        "<br>… и ещё {}".format(len(skipped) - 50)
                        if len(skipped) > 50 else "")))

    if failed:
        body.append('<h3 style="font-size:14px;color:#c53030;margin:18px 0 6px">'
                    'Не выполнено ({})</h3>'.format(len(failed)))
        for f in failed[:10]:
            body.append(
                '<div style="border-left:3px solid #c53030;background:#fdf1f1;'
                'padding:6px 10px;margin-bottom:6px"><b style="color:#c53030">{}'
                '</b><div style="font-family:Consolas,monospace;font-size:12px;'
                'white-space:pre-wrap">{}</div></div>'.format(
                    e(f["error"]), e((f.get("statement") or "")[:600])))

    if tail:
        body.append('<h3 style="font-size:14px;margin:18px 0 6px">Хвост лога</h3>'
                    '<pre style="font-family:Consolas,monospace;font-size:12px;'
                    'background:#f6f6fb;padding:10px;white-space:pre-wrap;'
                    'word-break:break-word">{}</pre>'.format(e(tail)))

    if event.get("url"):
        body.append('<p style="margin-top:18px"><a href="{0}" style="color:#5b3fd6;'
                    'font-weight:600">Открыть в Opsentri</a></p>'.format(
                        e(event["url"], quote=True)))

    body.append("</div>")

    return subject, "\n".join(text), "".join(body)


# ------------------------------------------------------------------
# событие из задачи
# ------------------------------------------------------------------

def _wants(rule, op, status):
    if not rule["enabled"] or not rule["recipients"]:
        return False

    if op == "restore" and not rule["on_restore"]:
        return False

    if status == "done":
        return rule["on_done"]
    if status == "partial":
        return rule["on_partial"]

    return rule["on_failed"]


def job_event(job_id):
    """Событие для письма по завершённой задаче gpbackup / gprestore."""
    from job_manager import get_job
    from modules.connections import get_connection_by_id
    from modules.gpbackup import (duration_seconds, error_hint, error_reason,
                                  failed_statements)

    job = get_job(job_id)

    if not job or job.get("job_type") not in ("gpbackup", "gprestore"):
        return None

    try:
        config = json.loads(job.get("config_json") or "{}")
    except ValueError:
        config = {}

    cid = int(config.get("connection_id") or job.get("connection_id") or 0)
    conn = get_connection_by_id(cid) or {}
    op = "backup" if job["job_type"] == "gpbackup" else "restore"
    status = job.get("status") or "failed"

    record = None

    if op == "backup":
        with sqlite_cursor() as cur:
            cur.execute("SELECT * FROM backups WHERE job_id = ? ORDER BY id DESC "
                        "LIMIT 1", (int(job_id),))
            row = cur.fetchone()
            record = dict(row) if row else None

        if record and record.get("status") in ("done", "partial", "failed"):
            status = record["status"]

    if status not in ("done", "partial", "interrupted"):
        status = "failed"

    error = job.get("error_message") or ""
    event = {
        "op": op,
        "status": status,
        "connection_id": cid,
        "cluster": conn.get("name"),
        "host": conn.get("host"),
        "dbname": (config.get("redirect_db") if op == "restore" else None)
        or config.get("dbname") or conn.get("database_name"),
        "backup_timestamp": (record or {}).get("backup_timestamp")
        or config.get("backup_timestamp"),
        "backup_type": config.get("backup_type") if op == "backup" else None,
        "backup_dir": config.get("backup_dir") or "",
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "duration_sec": duration_seconds(job.get("started_at"),
                                         job.get("finished_at")),
        "job_id": int(job_id),
        "summary": None,
        "skipped": [],
        "reason": None,
        "failed": [],
        "hint": None,
        "log_tail": "",
        "scope": None,
    }

    if op == "restore":
        scope = config.get("restore_scope")
        if scope == "schemas":
            event["scope"] = "схемы: " + ", ".join(
                s for s in str(config.get("include_schemas") or "").split() if s)
        elif scope == "tables":
            names = [s for s in str(config.get("include_tables") or "").split() if s]
            event["scope"] = "таблиц: {}".format(len(names))
        else:
            event["scope"] = "вся БД"

    if record:
        try:
            event["skipped"] = json.loads(record.get("skipped_tables") or "[]") or []
        except ValueError:
            event["skipped"] = []

        if status in ("done", "partial"):
            try:
                from modules.gpbackup_files import backup_contents

                full = dict(record, job_config=job.get("config_json"))
                event["summary"] = backup_contents(full, use_catalog=False)["summary"]
            except Exception:
                event["summary"] = None

    if status != "done" and error:
        event["failed"] = failed_statements(error, limit=10)
        event["reason"] = (
            "Не выполнено {}: {}".format(len(event["failed"]), event["failed"][0]["error"])
            if event["failed"] else error_reason(error))
        event["hint"] = error_hint(event["reason"] or error)

        if status != "partial":
            marker = error.rfind("Хвост лога:")
            tail = error[marker + len("Хвост лога:"):] if marker >= 0 else error
            event["log_tail"] = "\n".join(tail.strip().splitlines()[-40:])

    settings = get_mail_settings()

    if settings.get("public_url"):
        event["url"] = settings["public_url"] + "/backups"

    return event


def notify_job(job_id):
    """Синхронно: письмо по задаче, если кластер на это подписан. -> bool."""
    event = job_event(job_id)

    if not event or not event["connection_id"]:
        return False

    rule = get_backup_notify(event["connection_id"])

    if not rule["configured"] or not _wants(rule, event["op"], event["status"]):
        return False

    subject, text, body = build_message(event)

    try:
        send_mail(rule["recipients"], subject, text, body)
    except Exception as e:
        _record_result(event["connection_id"], "{}: {}".format(
            type(e).__name__, str(e)[:900]))
        return False

    _record_result(event["connection_id"], None)
    return True


def notify_finished(job_id, connection_id):
    """
    После finalize: дешёвая проверка подписки здесь, отправка — в фоне,
    чтобы SMTP-таймаут не держал задачу.
    """
    try:
        rule = get_backup_notify(int(connection_id or 0))
    except Exception:
        return

    if not rule["configured"] or not rule["enabled"] or not rule["recipients"]:
        return

    def run():
        try:
            notify_job(job_id)
        except Exception:
            pass

    threading.Thread(target=run, daemon=True).start()


def send_test(connection_id, recipients=None):
    """Тестовое письмо получателям кластера. Исключение — причина сбоя."""
    from modules.connections import get_connection_by_id

    rule = get_backup_notify(connection_id)
    to = parse_recipients(recipients) if recipients else rule["recipients"]
    conn = get_connection_by_id(int(connection_id)) or {}

    event = {
        "op": "backup", "status": "done", "cluster": conn.get("name"),
        "host": conn.get("host"), "dbname": conn.get("database_name"),
        "backup_timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
        "backup_type": "full", "started_at": _now(), "finished_at": _now(),
        "duration_sec": 0,
    }
    subject, text, body = build_message(event)
    subject = subject.replace("[Opsentri]", "[Opsentri] Тест:", 1)
    note = ("Это тестовое письмо: так будут выглядеть уведомления о бэкапах "
            "кластера.")
    send_mail(to, subject, note + "\n\n" + text,
              "<p>{}</p>".format(html.escape(note)) + body)
    return to
