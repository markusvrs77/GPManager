from datetime import datetime

import pytest

from modules.date_window import resolve_date_window


RUN = datetime(2026, 7, 21, 2, 0, 0)


def test_yesterday_to_yesterday_fully_includes_yesterday():
    spec = {"from": {"preset": "yesterday"}, "to": {"preset": "yesterday"}}
    assert resolve_date_window(spec, RUN) == ("2026-07-20", "2026-07-21")


def test_n_days_ago_window():
    spec = {"from": {"preset": "n_days_ago", "n": 7}, "to": {"preset": "yesterday"}}
    assert resolve_date_window(spec, RUN) == ("2026-07-14", "2026-07-21")


def test_last_month_month_boundary():
    spec = {"from": {"preset": "last_month"}, "to": {"preset": "last_month"}}
    assert resolve_date_window(spec, RUN) == ("2026-06-01", "2026-07-01")


def test_last_month_over_year_boundary():
    run = datetime(2026, 1, 15, 3, 30, 0)
    spec = {"from": {"preset": "last_month"}, "to": {"preset": "last_month"}}
    assert resolve_date_window(spec, run) == ("2025-12-01", "2026-01-01")


def test_this_month_to_date_includes_today():
    spec = {"from": {"preset": "this_month_to_date"}, "to": {"preset": "today"}}
    assert resolve_date_window(spec, RUN) == ("2026-07-01", "2026-07-22")


def test_shift_expression_days():
    spec = {"from": {"expr": "run_date-7d"}, "to": {"expr": "run_date-1d"}}
    assert resolve_date_window(spec, RUN) == ("2026-07-14", "2026-07-21")


def test_hours_are_exact_timestamps():
    spec = {"from": {"preset": "n_hours_ago", "n": 6}, "to": {"expr": "run_date-0h"}}
    assert resolve_date_window(spec, RUN) == (
        "2026-07-20 20:00:00",
        "2026-07-21 02:00:00",
    )


def test_unknown_preset_raises():
    spec = {"from": {"preset": "nope"}, "to": {"preset": "today"}}
    with pytest.raises(ValueError):
        resolve_date_window(spec, RUN)


def test_bad_shift_expression_raises():
    spec = {"from": {"expr": "run_date+1x"}, "to": {"preset": "today"}}
    with pytest.raises(ValueError):
        resolve_date_window(spec, RUN)


def test_empty_window_raises():
    spec = {"from": {"preset": "today"}, "to": {"preset": "n_days_ago", "n": 3}}
    with pytest.raises(ValueError):
        resolve_date_window(spec, RUN)
