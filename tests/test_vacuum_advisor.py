# -*- coding: utf-8 -*-
"""Движок рекомендаций Vacuum/Analyze — чистые правила, без БД."""

import pytest

from modules.vacuum_advisor import build_command, build_recommendations


def _row(**kw):
    base = {
        "schemaname": "dwh", "relname": "t",
        "n_live_tup": 0, "n_dead_tup": 0, "n_mod": None,
        "size_bytes": 0, "never_vacuumed": False, "never_analyzed": False,
        "analyze_age_days": 1, "vacuum_age_days": 1, "frozen_age": None,
    }
    base.update(kw)
    return base


def test_never_analyzed_is_crit_analyze():
    recs = build_recommendations([_row(relname="a", n_live_tup=500,
                                       never_analyzed=True,
                                       analyze_age_days=None)])
    assert len(recs) == 1
    assert recs[0]["action"] == "ANALYZE"
    assert recs[0]["severity"] == "crit"
    assert recs[0]["command"] == 'ANALYZE "dwh"."a";'


def test_dead_rows_vacuum_and_combined():
    # 30% мёртвых -> VACUUM (warn)
    recs = build_recommendations([_row(relname="b", n_live_tup=7000,
                                       n_dead_tup=3000)])
    assert recs[0]["action"] == "VACUUM"
    assert recs[0]["severity"] == "warn"

    # 60% мёртвых + не было ANALYZE -> VACUUM_ANALYZE (crit)
    recs = build_recommendations([_row(relname="c", n_live_tup=4000,
                                       n_dead_tup=6000, never_analyzed=True)])
    assert recs[0]["action"] == "VACUUM_ANALYZE"
    assert recs[0]["severity"] == "crit"

    # мало мёртвых строк — не трогаем
    assert build_recommendations([_row(n_live_tup=100, n_dead_tup=50)]) == []


def test_stale_statistics_analyze():
    # изменено 40% строк с последнего ANALYZE
    recs = build_recommendations([_row(relname="d", n_live_tup=1000,
                                       n_mod=400)])
    assert recs[0]["action"] == "ANALYZE"

    # ANALYZE был 45 дней назад
    recs = build_recommendations([_row(relname="e", n_live_tup=1000,
                                       analyze_age_days=45)])
    assert recs[0]["action"] == "ANALYZE"


def test_bloat_rules():
    # x5 раздутость -> VACUUM FULL crit
    recs = build_recommendations(
        [_row(relname="f", n_live_tup=1000)],
        bloat=[{"schemaname": "dwh", "relname": "f",
                "pages": 5000, "expected_pages": 1000, "diag": ""}],
    )
    assert recs[0]["action"] == "VACUUM_FULL"
    assert recs[0]["severity"] == "crit"
    assert recs[0]["command"] == 'VACUUM FULL "dwh"."f";'

    # умеренная раздутость -> обычный VACUUM
    recs = build_recommendations(
        [_row(relname="g", n_live_tup=1000)],
        bloat=[{"schemaname": "dwh", "relname": "g",
                "pages": 2000, "expected_pages": 1000,
                "diag": "moderate amount of bloat suspected"}],
    )
    assert recs[0]["action"] == "VACUUM"


def test_freeze_and_sorting():
    recs = build_recommendations([
        _row(relname="warn_t", n_live_tup=8000, n_dead_tup=2000,
             size_bytes=10),
        _row(relname="frozen", n_live_tup=100, frozen_age=1500000000,
             size_bytes=1),
    ])
    # crit раньше warn
    assert recs[0]["table"] == "frozen"
    assert recs[0]["action"] == "VACUUM_FREEZE"
    assert recs[0]["severity"] == "crit"


def test_build_command_validation():
    with pytest.raises(ValueError):
        build_command("DROP", "dwh", "t")
