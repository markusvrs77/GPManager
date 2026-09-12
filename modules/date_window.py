"""
Резолв относительных окон дат для запланированных gpcopy-задач.

Spec (docs/superpowers/specs/2026-07-21-scheduler-cron-design.md §4):
окно задаётся пресетами или сдвигами от run_date и резолвится в момент
запуска. Семантика границ: >= from AND < to, поэтому дневные `to`-границы
резолвятся в следующий день (окно «to: yesterday» включает вчера целиком).
Часовые границы — точные таймстемпы без сдвига.
"""

import re
from datetime import datetime, timedelta


DATE_FMT = "%Y-%m-%d"
TS_FMT = "%Y-%m-%d %H:%M:%S"

_SHIFT_RE = re.compile(r"^run_date([+-]\d+)([dh])$")


def _month_start(dt):
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _resolve_endpoint(endpoint, run_date, role):
    """
    Возвращает (datetime, granularity), granularity — 'd' или 'h'.
    role — 'from' или 'to': нужен пресетам-диапазонам (last_month и т.п.),
    у которых начало и конец окна различаются.
    """

    if not isinstance(endpoint, dict):
        raise ValueError("date_window endpoint must be a dict: {!r}".format(endpoint))

    expr = endpoint.get("expr")

    if expr is not None:
        match = _SHIFT_RE.match(str(expr).strip())

        if not match:
            raise ValueError("Bad shift expression: {!r}".format(expr))

        amount = int(match.group(1))
        unit = match.group(2)

        if unit == "d":
            day = run_date.replace(hour=0, minute=0, second=0, microsecond=0)
            return day + timedelta(days=amount), "d"

        return run_date + timedelta(hours=amount), "h"

    preset = endpoint.get("preset")

    if preset is None:
        raise ValueError("date_window endpoint needs 'preset' or 'expr'")

    preset = str(preset).strip().lower()
    day = run_date.replace(hour=0, minute=0, second=0, microsecond=0)

    if preset == "today":
        return day, "d"

    if preset == "yesterday":
        return day - timedelta(days=1), "d"

    if preset == "n_days_ago":
        n = int(endpoint.get("n", 0))
        return day - timedelta(days=n), "d"

    if preset == "this_month_to_date":
        if role == "from":
            return _month_start(day), "d"
        return day, "d"

    if preset == "last_month":
        this_month_start = _month_start(day)
        prev_month_end = this_month_start - timedelta(days=1)

        if role == "from":
            return _month_start(prev_month_end), "d"
        return prev_month_end, "d"

    if preset == "n_hours_ago":
        n = int(endpoint.get("n", 0))
        return run_date - timedelta(hours=n), "h"

    raise ValueError("Unknown date_window preset: {!r}".format(preset))


def _format(dt, granularity, role):
    if granularity == "h":
        return dt.strftime(TS_FMT)

    # Дневная to-граница эксклюзивна: сдвигаем на следующий день,
    # чтобы указанный день попал в окно целиком (>= from AND < to).
    if role == "to":
        dt = dt + timedelta(days=1)

    return dt.strftime(DATE_FMT)


def resolve_date_window(spec, run_date):
    """
    spec: {"from": {...}, "to": {...}} (+ опц. "column" — здесь не используется).
    run_date: datetime логического запуска (в timezone расписания).
    Возвращает (date_from, date_to) строками для существующего пути
    build_gpcopy_date_include_json.
    """

    if not isinstance(spec, dict):
        raise ValueError("date_window spec must be a dict")

    from_dt, from_gran = _resolve_endpoint(spec.get("from"), run_date, "from")
    to_dt, to_gran = _resolve_endpoint(spec.get("to"), run_date, "to")

    date_from = _format(from_dt, from_gran, "from")
    date_to = _format(to_dt, to_gran, "to")

    # ISO-строки сравниваются лексикографически корректно.
    if date_from >= date_to:
        raise ValueError(
            "Empty date window: from={} >= to={}".format(date_from, date_to)
        )

    return date_from, date_to
