"""
RPC-порт gpcopy для параллельных запусков.

gpcopy поднимает на хосте, где запущен, RPC-слушатель — по умолчанию на
7667 (--rpc-port). Второй gpcopy на том же хосте падал сразу, ничего не
скопировав: «listen tcp 0.0.0.0:7667: bind: address already in use».

Каждый запуск получает свой свободный порт из небольшого диапазона,
начиная с 7667: одиночный запуск идёт как раньше, параллельные — на
7668, 7669, … Диапазон узкий, чтобы его можно было открыть в firewall;
задаётся переменной окружения GPCOPY_RPC_PORTS («7667-7699»).

Проверка «порт свободен» и запуск gpcopy разнесены во времени, поэтому
выданный порт резервируется на RESERVE_SECONDS: две задачи, стартующие
одновременно в одном процессе приложения, не получат один и тот же.
"""

import os
import socket
import threading
import time


DEFAULT_RANGE = (7667, 7699)
RESERVE_SECONDS = 180

_lock = threading.Lock()
_reserved = {}


def port_range():
    """(первый, последний) из GPCOPY_RPC_PORTS или по умолчанию."""
    raw = (os.environ.get("GPCOPY_RPC_PORTS") or "").strip()

    try:
        first, last = [int(p) for p in raw.split("-", 1)]
        if 1024 <= first <= last <= 65535:
            return first, last
    except ValueError:
        pass

    return DEFAULT_RANGE


def port_is_free(port):
    """Можно ли сейчас занять порт на всех интерфейсах этого хоста."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def pick_rpc_port(now=None, is_free=port_is_free):
    """
    Первый свободный и не выданный недавно порт диапазона, либо None —
    тогда флаг не передаётся и gpcopy берёт свой 7667.
    """
    now = time.time() if now is None else now
    first, last = port_range()

    with _lock:
        for port, until in list(_reserved.items()):
            if until <= now:
                _reserved.pop(port, None)

        for port in range(first, last + 1):
            if port in _reserved:
                continue

            if is_free(port):
                _reserved[port] = now + RESERVE_SECONDS
                return port

    return None
