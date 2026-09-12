"""
Каналы уведомлений планировщика (spec §6).

Единый интерфейс: send(channel, event) -> (ok, error).
channel — строка notification_channels (dict), event — словарь:
schedule, job_type, status, fired_at, error, job_id.
Все внешние вызовы best-effort: ошибка канала не роняет планировщик.
"""

import json
import smtplib
import urllib.request
from email.mime.text import MIMEText

import scheduler_store as store


HTTP_TIMEOUT_SECONDS = 15


def _http_post_json(url, payload, headers=None, proxy=None, timeout=HTTP_TIMEOUT_SECONDS):
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request = urllib.request.Request(url, data=data, method="POST")
    request.add_header("Content-Type", "application/json")

    for key, value in (headers or {}).items():
        request.add_header(key, value)

    if proxy:
        request.set_proxy(proxy, "http")
        request.set_proxy(proxy, "https")

    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.status


def _format_text(event):
    lines = [
        "GPManager schedule: {}".format(event.get("schedule")),
        "Job type: {}".format(event.get("job_type")),
        "Status: {}".format(event.get("status")),
        "Fired at: {}".format(event.get("fired_at")),
    ]

    if event.get("job_id"):
        lines.append("Job id: #{}".format(event.get("job_id")))

    if event.get("error"):
        lines.append("Error: {}".format(str(event.get("error"))[:500]))

    return "\n".join(lines)


# ------------------------------------------------------------
# Adapters
# ------------------------------------------------------------

def send_webhook(cfg, event):
    _http_post_json(cfg["url"], event, proxy=cfg.get("proxy"))


def send_teams(cfg, event):
    failed = event.get("status") == "failed"
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "E0466B" if failed else "0EA371",
        "summary": "GPManager: {} {}".format(
            event.get("schedule"), event.get("status")
        ),
        "title": "GPManager · {} · {}".format(
            event.get("schedule"), event.get("status")
        ),
        "text": _format_text(event).replace("\n", "<br>"),
    }
    _http_post_json(cfg["url"], payload, proxy=cfg.get("proxy"))


def send_telegram(cfg, event):
    url = "https://api.telegram.org/bot{}/sendMessage".format(cfg["bot_token"])
    payload = {
        "chat_id": cfg["chat_id"],
        "text": _format_text(event),
    }
    _http_post_json(url, payload, proxy=cfg.get("proxy"))


def send_email(cfg, event):
    to_addrs = cfg["to"] if isinstance(cfg["to"], list) else [cfg["to"]]

    message = MIMEText(_format_text(event), "plain", "utf-8")
    message["Subject"] = "[GPManager] {}: {}".format(
        event.get("status"), event.get("schedule")
    )
    message["From"] = cfg["from"]
    message["To"] = ", ".join(to_addrs)

    smtp = smtplib.SMTP(cfg["host"], int(cfg.get("port", 25)), timeout=15)

    try:
        if cfg.get("tls"):
            smtp.starttls()

        if cfg.get("username"):
            smtp.login(cfg["username"], cfg.get("password") or "")

        smtp.sendmail(cfg["from"], to_addrs, message.as_string())
    finally:
        smtp.quit()


def send_whatsapp(cfg, event):
    """
    Meta Cloud API (best-effort, spec §2): требует WhatsApp Business
    phone_number_id + access token. Без них канал вернёт ошибку конфигурации.
    """
    url = "https://graph.facebook.com/v19.0/{}/messages".format(
        cfg["phone_number_id"]
    )
    payload = {
        "messaging_product": "whatsapp",
        "to": cfg["to"],
        "type": "text",
        "text": {"body": _format_text(event)},
    }
    _http_post_json(
        url,
        payload,
        headers={"Authorization": "Bearer {}".format(cfg["access_token"])},
        proxy=cfg.get("proxy"),
    )


ADAPTERS = {
    "webhook": send_webhook,
    "teams": send_teams,
    "telegram": send_telegram,
    "email": send_email,
    "whatsapp": send_whatsapp,
}


# ------------------------------------------------------------
# Entry points
# ------------------------------------------------------------

def send(channel, event):
    """Возвращает (ok, error). Никогда не бросает."""
    try:
        channel_type = str(channel.get("type") or "")
        adapter = ADAPTERS.get(channel_type)

        if adapter is None:
            return False, "Unknown channel type: {}".format(channel_type)

        cfg = json.loads(channel.get("config_json") or "{}")
        adapter(cfg, event)
        return True, None

    except KeyError as e:
        return False, "Channel config missing key: {}".format(e)
    except Exception as e:
        return False, str(e)


def notify_channels(channel_ids, event):
    """Шлёт событие во все включённые каналы; возвращает [(id, ok, error)]."""
    results = []

    for channel_id in channel_ids or []:
        channel = store.get_channel(channel_id)

        if not channel or not channel.get("enabled"):
            continue

        ok, error = send(channel, event)
        results.append((channel_id, ok, error))

    return results
