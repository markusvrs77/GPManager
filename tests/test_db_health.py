# -*- coding: utf-8 -*-
"""Чистые хелперы «Здоровья БД» — без подключения к БД."""

from modules.db_health import (
    classify_staleness_days,
    dead_ratio,
    verdict_activity,
    verdict_segments,
)


def test_classify_staleness_days():
    assert classify_staleness_days(None) == "crit"   # никогда не analyze
    assert classify_staleness_days(45) == "crit"
    assert classify_staleness_days(10) == "warn"
    assert classify_staleness_days(0) == "ok"
    assert classify_staleness_days(3) == "ok"


def test_verdict_segments():
    assert verdict_segments(total=10, down=1, unbalanced=0) == "crit"
    assert verdict_segments(total=10, down=0, unbalanced=2) == "warn"
    assert verdict_segments(total=10, down=0, unbalanced=0) == "ok"
    assert verdict_segments(total=0, down=0, unbalanced=0) == "warn"


def test_verdict_activity():
    assert verdict_activity(long_queries=[1], idle_txn=[]) == "warn"
    assert verdict_activity(long_queries=[], idle_txn=[1]) == "warn"
    assert verdict_activity(long_queries=[], idle_txn=[]) == "ok"


def test_dead_ratio():
    assert dead_ratio(1000, 200) == 0.2
    assert dead_ratio(0, 50) == 50.0    # пустая таблица с dead-строками
    assert dead_ratio(None, None) == 0.0
