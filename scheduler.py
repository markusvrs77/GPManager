"""
Движок планировщика (spec §5): daemon-поток, тик раз в 60с, только лидер
материализует задачи. Расписание -> обычный job через create_job + раннер
из реестра JOB_RUNNERS (реальные раннеры регистрирует app.py).
"""

import json
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta

import scheduler_store as store
from job_manager import create_job, get_job
from modules.date_window import resolve_date_window


TICK_SECONDS = 60
MISFIRE_GRACE_SECONDS = 3600

HOLDER_ID = "worker-{}".format(uuid.uuid4().hex[:12])

# job_type -> callable(job_id). Реальные раннеры регистрирует app.py.
JOB_RUNNERS = {}

# Тесты выставляют True, чтобы раннер выполнялся синхронно в tick().
RUN_SYNC = False

_thread = None


def register_runner(job_type, func):
    JOB_RUNNERS[str(job_type)] = func


def materialize_config(config, run_date):
    """
    Резолвит относительное date_window в конкретные date_from/date_to
    на момент запуска (spec §4). Остальной конфиг не трогаем.
    """
    config = dict(config or {})
    date_window = config.get("date_window")

    if date_window:
        date_from, date_to = resolve_date_window(date_window, run_date)
        config["date_from"] = date_from
        config["date_to"] = date_to

        if date_window.get("column"):
            config["date_filter_column"] = date_window["column"]

        config.setdefault("mode", "date_filter")

    return config


def _notify(schedule, status, error, job_id):
    """Шлёт событие в каналы расписания по политике notify_on (spec §6)."""
    notify_on = schedule.get("notify_on") or "failure"

    if notify_on == "never":
        return

    if notify_on == "failure" and status != "failed":
        return

    try:
        channel_ids = json.loads(schedule.get("notify_channel_ids") or "[]")
    except (ValueError, TypeError):
        channel_ids = []

    if not channel_ids:
        return

    event = {
        "schedule": schedule.get("name"),
        "job_type": schedule.get("job_type"),
        "status": status,
        "fired_at": store.fmt(datetime.now()),
        "error": error,
        "job_id": job_id,
    }

    try:
        import notifiers

        notifiers.notify_channels(channel_ids, event)
    except Exception:
        # Уведомления best-effort: канал не должен ронять планировщик.
        traceback.print_exc()


def _maybe_queue_retry(schedule, failed_run, now):
    max_retries = int(schedule.get("max_retries") or 0)

    if failed_run["attempt_no"] >= max_retries:
        return

    delay = int(schedule.get("retry_delay_seconds") or 0)
    fire_at = store.fmt(now + timedelta(seconds=delay))

    store.record_run(
        schedule["id"],
        fired_at=fire_at,
        run_date=failed_run["run_date"],
        status="queued",
        attempt_no=failed_run["attempt_no"] + 1,
    )


def _execute(runner, job_id, run_id, schedule, now):
    error = None

    try:
        runner(job_id)
        job = get_job(job_id)
        ok = bool(job) and job.get("status") == "done"

        if not ok:
            error = (job or {}).get("error_message") or "job status: {}".format(
                (job or {}).get("status")
            )
    except Exception as e:
        ok = False
        error = "{}\n{}".format(e, traceback.format_exc())

    status = "done" if ok else "failed"
    store.update_run(run_id, status=status, error=error)
    store.update_schedule_last(
        schedule["id"],
        status=status,
        job_id=job_id,
        error=error,
        run_at=store.fmt(now),
    )

    if not ok:
        run = store.get_run(run_id)
        if run:
            _maybe_queue_retry(schedule, run, now)

    _notify(schedule, status, error, job_id)


def _launch(schedule, run_date_str, attempt_no, now, existing_run_id=None):
    schedule_id = schedule["id"]
    now_str = store.fmt(now)
    runner = JOB_RUNNERS.get(schedule["job_type"])

    if runner is None:
        message = "No runner registered for job_type: {}".format(schedule["job_type"])

        if existing_run_id:
            store.update_run(existing_run_id, status="failed", error=message)
        else:
            store.record_run(
                schedule_id,
                fired_at=now_str,
                run_date=run_date_str,
                status="failed",
                attempt_no=attempt_no,
                error=message,
            )

        store.update_schedule_last(
            schedule_id, status="failed", error=message, run_at=now_str
        )
        _notify(schedule, "failed", message, None)
        return

    run_date = store.parse_ts(run_date_str)
    config = materialize_config(
        json.loads(schedule.get("config_json") or "{}"),
        run_date,
    )

    connection_id = (
        config.get("connection_id")
        or config.get("source_connection_id")
    )

    job_id = create_job(schedule["job_type"], connection_id, config)

    if existing_run_id:
        store.update_run(existing_run_id, status="running", job_id=job_id)
        run_id = existing_run_id
    else:
        run_id = store.record_run(
            schedule_id,
            fired_at=now_str,
            run_date=run_date_str,
            status="running",
            attempt_no=attempt_no,
            job_id=job_id,
        )

    if RUN_SYNC:
        _execute(runner, job_id, run_id, schedule, now)
    else:
        thread = threading.Thread(
            target=_execute,
            args=(runner, job_id, run_id, schedule, now),
        )
        thread.daemon = True
        thread.start()


def _process_due(schedule, now):
    schedule_id = schedule["id"]
    next_at = store.parse_ts(schedule["next_run_at"])

    # Сразу двигаем next_run_at, чтобы падение ниже не зациклило расписание.
    store.set_next_run(
        schedule_id, store.compute_next_run(schedule["cron_expr"], now)
    )

    # Misfire старше grace: только сдвиг, без catch-up (spec §5).
    if (now - next_at).total_seconds() > MISFIRE_GRACE_SECONDS:
        return

    policy = schedule.get("overlap_policy") or "skip"

    if policy != "parallel" and store.get_active_run(schedule_id):
        if policy == "skip":
            store.record_run(
                schedule_id,
                fired_at=store.fmt(now),
                run_date=store.fmt(next_at),
                status="skipped",
                error="overlap: previous run still active",
            )
        else:
            # queue: возвращаем срок, следующий тик попробует снова.
            store.set_next_run(schedule_id, schedule["next_run_at"])
        return

    _launch(schedule, store.fmt(next_at), 0, now)


def tick(now=None):
    now = now or datetime.now()

    if not store.acquire_leader_lock(HOLDER_ID, now):
        return "not-leader"

    for schedule in store.list_due_schedules(store.fmt(now)):
        try:
            _process_due(schedule, now)
        except Exception as e:
            store.update_schedule_last(
                schedule["id"],
                status="failed",
                error="{}\n{}".format(e, traceback.format_exc())[:2000],
                run_at=store.fmt(now),
            )

    for run in store.list_due_retry_runs(store.fmt(now)):
        try:
            schedule = store.get_schedule(run["schedule_id"])

            if schedule:
                _launch(
                    schedule,
                    run["run_date"],
                    run["attempt_no"],
                    now,
                    existing_run_id=run["id"],
                )
        except Exception as e:
            store.update_run(run["id"], status="failed", error=str(e))

    return "leader"


def run_now(schedule_id, now=None):
    """Ручной запуск расписания (кнопка Run now). Уважает overlap-политику."""
    now = now or datetime.now()
    schedule = store.get_schedule(schedule_id)

    if not schedule:
        raise ValueError("Schedule not found: {}".format(schedule_id))

    policy = schedule.get("overlap_policy") or "skip"

    if policy != "parallel" and store.get_active_run(schedule_id):
        return {"started": False, "reason": "previous run still active"}

    _launch(schedule, store.fmt(now), 0, now)
    return {"started": True}


def _loop():
    while True:
        try:
            tick()
        except Exception:
            traceback.print_exc()

        time.sleep(TICK_SECONDS)


def start_scheduler():
    """Запускает daemon-поток планировщика (идемпотентно)."""
    global _thread

    if _thread is not None and _thread.is_alive():
        return _thread

    _thread = threading.Thread(target=_loop, name="gpm-scheduler")
    _thread.daemon = True
    _thread.start()

    return _thread
