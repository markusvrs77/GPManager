# -*- coding: utf-8 -*-
"""Причина падения из лога gpcopy: пустые обёртки не в счёт."""

from modules.gpcopy import extract_failure_cause

LOG = """20260827:12:30:24 gpcopy:gpadmin:ffin-test:2762536-[INFO]:-Initializing
20260827:12:30:24 gpcopy:gpadmin:ffin-test:2762536-[INFO]:-Source 7.3.4
20260827:12:30:26 gpcopy:gpadmin:ffin-test:2762536-[ERROR]:-permission denied for schema dwh_fin_pbi
20260827:12:30:28 gpcopy:gpadmin:ffin-test:2762536-[INFO]:-Copied 0 databases
Error: Error Detail: command error message:
: ERROR: child process exited with exit code 1  (seg9 172.17.56.197:6101) (SQLSTATE 38000)"""


def test_cause_skips_wrappers_and_strips_prefix():
    cause = extract_failure_cause(LOG)

    # пустая обёртка gpcopy отброшена
    assert not any("command error message" in c.lower() for c in cause)
    # префикс лога снят, сообщение сегмента на месте
    assert "permission denied for schema dwh_fin_pbi" in cause
    assert any(c.startswith("ERROR: child process exited") for c in cause)
    # INFO-строки не попадают
    assert not any("Copied 0 databases" in c for c in cause)


def test_cause_dedupes_and_limits():
    noisy = "\n".join([": ERROR: out of memory"] * 20 +
                      [": FATAL: connection refused"])
    cause = extract_failure_cause(noisy, limit=3)

    assert cause == ["ERROR: out of memory", "FATAL: connection refused"]


def test_cause_on_empty_log():
    assert extract_failure_cause("") == []
    assert extract_failure_cause(None) == []
