import json

import pytest

import notifiers
import scheduler


def _channel(type_, cfg):
    return {"type": type_, "config_json": json.dumps(cfg), "enabled": 1}


EVENT = {
    "schedule": "nightly",
    "job_type": "gpcopy",
    "status": "failed",
    "fired_at": "2026-07-21 02:00:00",
    "error": "boom",
    "job_id": 7,
}


def test_webhook_posts_event_json(monkeypatch):
    calls = []
    monkeypatch.setattr(
        notifiers, "_http_post_json",
        lambda url, payload, **kw: calls.append((url, payload)) or 200,
    )

    ok, err = notifiers.send(_channel("webhook", {"url": "http://x/hook"}), EVENT)

    assert ok and err is None
    assert calls[0][0] == "http://x/hook"
    assert calls[0][1]["status"] == "failed"


def test_teams_wraps_messagecard(monkeypatch):
    calls = []
    monkeypatch.setattr(
        notifiers, "_http_post_json",
        lambda url, payload, **kw: calls.append(payload) or 200,
    )

    ok, _ = notifiers.send(_channel("teams", {"url": "http://teams/hook"}), EVENT)

    assert ok
    assert calls[0]["@type"] == "MessageCard"
    assert "nightly" in calls[0]["text"]


def test_telegram_url_and_payload(monkeypatch):
    calls = []
    monkeypatch.setattr(
        notifiers, "_http_post_json",
        lambda url, payload, **kw: calls.append((url, payload)) or 200,
    )

    ok, _ = notifiers.send(
        _channel("telegram", {"bot_token": "TOK", "chat_id": "42"}), EVENT
    )

    assert ok
    assert "botTOK/sendMessage" in calls[0][0]
    assert calls[0][1]["chat_id"] == "42"
    assert "nightly" in calls[0][1]["text"]


def test_email_sends_via_smtp(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=None):
            sent["host"] = host
            sent["port"] = port

        def starttls(self):
            sent["tls"] = True

        def login(self, user, password):
            sent["login"] = user

        def sendmail(self, from_addr, to_addrs, msg):
            sent["from"] = from_addr
            sent["to"] = to_addrs
            sent["msg"] = msg

        def quit(self):
            sent["quit"] = True

    monkeypatch.setattr(notifiers.smtplib, "SMTP", FakeSMTP)

    ok, err = notifiers.send(
        _channel("email", {
            "host": "smtp.local", "port": 25,
            "from": "gpm@local", "to": "dba@local",
        }),
        EVENT,
    )

    assert ok, err
    assert sent["host"] == "smtp.local"
    assert sent["to"] == ["dba@local"]
    assert sent["quit"]


def test_unknown_type_fails_cleanly():
    ok, err = notifiers.send(_channel("pigeon", {}), EVENT)
    assert not ok
    assert "pigeon" in err


def test_adapter_exception_returns_error(monkeypatch):
    def _raise(url, payload, **kw):
        raise RuntimeError("net down")

    monkeypatch.setattr(notifiers, "_http_post_json", _raise)

    ok, err = notifiers.send(_channel("webhook", {"url": "http://x"}), EVENT)
    assert not ok
    assert "net down" in err


def test_scheduler_notify_on_filter(monkeypatch):
    captured = []
    monkeypatch.setattr(
        notifiers, "notify_channels",
        lambda ids, event: captured.append((ids, event["status"])),
    )

    schedule = {
        "id": 1, "name": "s", "job_type": "gpcopy",
        "notify_on": "failure", "notify_channel_ids": "[3]",
    }

    scheduler._notify(schedule, "done", None, 1)
    assert captured == []

    scheduler._notify(schedule, "failed", "err", 1)
    assert captured == [([3], "failed")]

    schedule["notify_on"] = "never"
    scheduler._notify(schedule, "failed", "err", 1)
    assert len(captured) == 1
