"""
RPC-порт gpcopy: параллельные запуски на одном хосте.

Второй gpcopy падал сразу: «listen tcp 0.0.0.0:7667: bind: address
already in use». Каждый запуск получает свой порт (--rpc-port).
"""

import pytest

from modules import gpcopy_ports as ports
from modules.gpcopy import build_gpcopy_command


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv("GPCOPY_RPC_PORTS", raising=False)
    ports._reserved.clear()
    yield
    ports._reserved.clear()


def test_single_run_keeps_default_port():
    assert ports.pick_rpc_port(now=0, is_free=lambda p: True) == 7667


def test_parallel_runs_get_different_ports():
    """Порт ещё не занят gpcopy, но уже выдан — второму не достаётся."""
    first = ports.pick_rpc_port(now=0, is_free=lambda p: True)
    second = ports.pick_rpc_port(now=1, is_free=lambda p: True)

    assert (first, second) == (7667, 7668)


def test_busy_port_is_skipped():
    """7667 держит чужой gpcopy (как в логе) — берём следующий."""
    assert ports.pick_rpc_port(now=0, is_free=lambda p: p != 7667) == 7668


def test_reservation_expires():
    ports.pick_rpc_port(now=0, is_free=lambda p: True)

    later = ports.RESERVE_SECONDS + 1
    assert ports.pick_rpc_port(now=later, is_free=lambda p: True) == 7667


def test_range_from_environment(monkeypatch):
    monkeypatch.setenv("GPCOPY_RPC_PORTS", "9100-9101")

    assert ports.pick_rpc_port(now=0, is_free=lambda p: True) == 9100
    assert ports.pick_rpc_port(now=0, is_free=lambda p: True) == 9101
    assert ports.pick_rpc_port(now=0, is_free=lambda p: True) is None


def test_bad_environment_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("GPCOPY_RPC_PORTS", "abc")

    assert ports.port_range() == ports.DEFAULT_RANGE


def _cmd(**kw):
    return build_gpcopy_command(gpcopy_path="gpcopy", source_host="src",
                                dest_host="dst", include_tables="db.s.t", **kw)


def test_command_carries_rpc_port():
    cmd = _cmd(rpc_port=7668)

    assert cmd[cmd.index("--rpc-port") + 1] == "7668"


def test_no_port_means_gpcopy_default():
    assert "--rpc-port" not in _cmd()


def test_manual_rpc_port_wins():
    cmd = _cmd(rpc_port=7668, extra_args="--rpc-port 9000")

    assert cmd.count("--rpc-port") == 1
    assert "9000" in cmd and "7668" not in cmd
