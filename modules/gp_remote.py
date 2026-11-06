# -*- coding: utf-8 -*-
"""
gpbackup / gprestore на координаторе кластера по SSH.

gpbackup работает только с координатора того кластера, который бэкапит:
оттуда он по SSH создаёт каталоги на сегментах, там пишет файлы копии и
историю. Запущенный на сервере Opsentri против чужого кластера, он падает
с «Unable to create backup directories on N segments». Поэтому для
кластера на другом хосте команда уходит по SSH на его координатор
(ключ без пароля: Opsentri -> gpadmin@координатор).

Пароль БД не попадает в командную строку: удалённый скрипт читает его
первой строкой stdin. Перед exec скрипт печатает свой PID — по нему
останавливается удалённый процесс.

Файлы копии там же читаются через RemoteFS — только чтение.
"""

import glob as _glob
import os
import re
import shlex
import shutil
import socket
import subprocess
import tempfile
from datetime import datetime

from db import sqlite_cursor

MODES = ("auto", "local", "ssh")

DEFAULT_GPHOME = "/usr/local/gpdb/greenplum-db"
DEFAULT_SSH_USER = "gpadmin"

REMOTE_PID_MARK = "OPSENTRI_REMOTE_PID="
_REMOTE_PID_RE = re.compile(re.escape(REMOTE_PID_MARK) + r"(\d+)")

_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]*$")
_USER_RE = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
_PATH_RE = re.compile(r"^/[A-Za-z0-9_./-]*$")
# сегмент шаблона для удалённого glob: только наши шаблоны
_GLOB_PART_RE = re.compile(r"^[A-Za-z0-9_.*-]+$")

SSH_TIMEOUT_SECONDS = 60


# ------------------------------------------------------------------
# настройки кластера
# ------------------------------------------------------------------

def get_host_settings(connection_id):
    with sqlite_cursor() as cur:
        cur.execute("SELECT * FROM backup_hosts WHERE connection_id = ?",
                    (int(connection_id),))
        row = cur.fetchone()

    data = dict(row) if row else {}

    return {
        "connection_id": int(connection_id),
        "mode": data.get("mode") or "auto",
        "ssh_host": data.get("ssh_host") or "",
        "ssh_user": data.get("ssh_user") or DEFAULT_SSH_USER,
        "ssh_port": int(data.get("ssh_port") or 22),
        "gphome": data.get("gphome") or DEFAULT_GPHOME,
        "updated_at": data.get("updated_at"),
    }


def _clean_path(value, what):
    value = str(value or "").strip().rstrip("/") or ""

    if value and (not _PATH_RE.match(value) or ".." in value.split("/")):
        raise ValueError("{}: абсолютный путь без пробелов и ..".format(what))

    return value


def save_host_settings(connection_id, data):
    data = data or {}
    mode = str(data.get("mode") or "auto")
    host = str(data.get("ssh_host") or "").strip()
    user = str(data.get("ssh_user") or DEFAULT_SSH_USER).strip()
    gphome = _clean_path(data.get("gphome") or DEFAULT_GPHOME, "GPHOME")

    if mode not in MODES:
        raise ValueError("Режим: auto, local или ssh")

    if host and not _HOST_RE.match(host):
        raise ValueError("Хост — имя или IP без пробелов")

    if not _USER_RE.match(user):
        raise ValueError("Некорректный пользователь SSH")

    raw_port = data.get("ssh_port")

    try:
        port = 22 if raw_port in (None, "") else int(raw_port)
    except (TypeError, ValueError):
        raise ValueError("Порт SSH — число")

    if not 1 <= port <= 65535:
        raise ValueError("Порт SSH — от 1 до 65535")

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO backup_hosts (connection_id, mode, ssh_host, ssh_user,
                                      ssh_port, gphome, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
                mode = excluded.mode, ssh_host = excluded.ssh_host,
                ssh_user = excluded.ssh_user, ssh_port = excluded.ssh_port,
                gphome = excluded.gphome, updated_at = excluded.updated_at
            """,
            (int(connection_id), mode, host, user, port, gphome,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )

    return get_host_settings(connection_id)


def _local_addresses():
    names = {"localhost", socket.gethostname().lower(), socket.getfqdn().lower()}
    addrs = {"127.0.0.1", "::1"}

    try:
        addrs.update(socket.gethostbyname_ex(socket.gethostname())[2])
    except OSError:
        pass

    return names, addrs


def is_local_host(host):
    """Этот ли хост: по имени или по IP (с резолвом имени)."""
    host = str(host or "").strip().lower()

    if not host:
        return True

    names, addrs = _local_addresses()

    if host in names or host in addrs or host.startswith("127."):
        return True

    try:
        return socket.gethostbyname(host) in addrs
    except OSError:
        return False


def resolve_target(connection_id):
    """
    None — запускать здесь; иначе {host, user, port, gphome} координатора.
    auto: здесь, если координатор кластера — этот же хост.
    """
    if not connection_id:
        return None

    try:
        from modules.connections import get_connection_by_id

        conn = get_connection_by_id(int(connection_id))
    except Exception:
        conn = None

    if not conn:
        return None

    s = get_host_settings(connection_id)
    host = s["ssh_host"] or str(conn.get("host") or "").strip()

    if s["mode"] == "local":
        return None

    if s["mode"] == "auto" and is_local_host(host):
        return None

    if not host or not _HOST_RE.match(host):
        return None

    return {"host": host, "user": s["ssh_user"], "port": s["ssh_port"],
            "gphome": s["gphome"]}


def describe(target):
    if not target:
        return "на этом сервере Opsentri"

    return "по SSH на {}@{}".format(target["user"], target["host"])


# ------------------------------------------------------------------
# SSH
# ------------------------------------------------------------------

def ssh_base(target):
    cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=15",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=6",
    ]

    if os.name == "posix":
        # одно соединение на серию чтений файлов (состав копии, отчёт)
        cmd += ["-o", "ControlMaster=auto",
                "-o", "ControlPath=" + os.path.join(
                    tempfile.gettempdir(), "opsentri-ssh-%r@%h-%p"),
                "-o", "ControlPersist=60"]

    return cmd + ["-p", str(int(target["port"])),
                  "{}@{}".format(target["user"], target["host"])]


def _remote_binary(target, path):
    """Бинарь по умолчанию — из GPHOME координатора."""
    name = os.path.basename(path)

    if path.startswith(DEFAULT_GPHOME + "/") or path == name:
        return "{}/bin/{}".format(target["gphome"], name)

    return path


def wrap_command(target, cmd, env):
    """
    Команда для запуска на координаторе -> (argv ssh, stdin).
    PG*-переменные, кроме пароля, — в export скрипта; пароль — stdin.
    """
    argv = [_remote_binary(target, cmd[0])] + list(cmd[1:])
    exports = " ".join(
        "{}={}".format(k, shlex.quote(str(env[k])))
        for k in ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE")
        if env.get(k)
    )
    gp_path = shlex.quote(target["gphome"] + "/greenplum_path.sh")

    script = (
        "IFS= read -r PGPASSWORD || true; "
        "if [ -n \"$PGPASSWORD\" ]; then export PGPASSWORD; else unset PGPASSWORD; fi; "
        "if [ -f {gp} ]; then . {gp} >/dev/null 2>&1; fi; "
        "{exports}"
        "echo {mark}$$; "
        "exec {argv}"
    ).format(
        gp=gp_path,
        exports="export {}; ".format(exports) if exports else "",
        mark=REMOTE_PID_MARK,
        argv=" ".join(shlex.quote(a) for a in argv),
    )

    password = str(env.get("PGPASSWORD") or "")

    return ssh_base(target) + [script], password.replace("\n", "") + "\n"


def run(target, script, timeout=SSH_TIMEOUT_SECONDS, stdin_text=None,
        binary=False):
    """Короткая команда на координаторе -> (rc, вывод)."""
    kwargs = {"stdout": subprocess.PIPE, "stderr": subprocess.PIPE,
              "timeout": timeout}

    if stdin_text is not None:
        kwargs["input"] = stdin_text.encode("utf-8")
    else:
        kwargs["stdin"] = subprocess.DEVNULL

    proc = subprocess.run(ssh_base(target) + [script], **kwargs)
    out = proc.stdout if binary else proc.stdout.decode("utf-8", "replace")

    if proc.returncode != 0 and not binary:
        out = out + proc.stderr.decode("utf-8", "replace")

    return proc.returncode, out


def remote_pid(log_text):
    found = None

    for found in _REMOTE_PID_RE.finditer(log_text or ""):
        pass

    return int(found.group(1)) if found else None


def stop_remote(target, log_text):
    """SIGINT удалённому gpbackup/gprestore: он сам сделает cleanup."""
    pid = remote_pid(log_text)

    if not target or not pid:
        return False

    try:
        rc, _out = run(target, "kill -INT {} 2>/dev/null || true".format(pid),
                       timeout=30)
        return rc == 0
    except Exception:
        return False


def check(target):
    """Проверка связи: хост, версии gpbackup/gprestore. -> (ok, текст)."""
    gp = shlex.quote(target["gphome"])
    script = (
        "hostname; "
        "if [ -f {gp}/greenplum_path.sh ]; then . {gp}/greenplum_path.sh >/dev/null 2>&1; "
        "else echo 'нет {gphome}/greenplum_path.sh'; fi; "
        "{gp}/bin/gpbackup --version 2>&1 | head -1; "
        "{gp}/bin/gprestore --version 2>&1 | head -1"
    ).format(gp=gp, gphome=target["gphome"])

    try:
        rc, out = run(target, script, timeout=40)
    except subprocess.TimeoutExpired:
        return False, "Нет ответа по SSH за 40 с"
    except FileNotFoundError:
        return False, "На сервере Opsentri нет клиента ssh"

    return rc == 0 and "version" in out.lower(), out.strip()[:2000]


# ------------------------------------------------------------------
# файлы: здесь или на координаторе
# ------------------------------------------------------------------

class LocalFS(object):
    remote = False

    def isdir(self, path):
        return os.path.isdir(path)

    def isfile(self, path):
        return os.path.isfile(path)

    def read(self, path, tail=None):
        try:
            with open(path, "rb") as f:
                if tail:
                    f.seek(0, os.SEEK_END)
                    f.seek(max(0, f.tell() - tail))
                return f.read().decode("utf-8", errors="replace")
        except OSError:
            return None

    def read_bytes(self, path):
        try:
            with open(path, "rb") as f:
                return f.read()
        except OSError:
            return None

    def glob(self, base, *parts):
        return sorted(_glob.glob(os.path.join(_glob.escape(base), *parts)))

    def disk_usage(self, path):
        try:
            u = shutil.disk_usage(path)
        except OSError:
            return None
        return {"total": u.total, "used": u.used, "free": u.free}


# хост, до которого SSH не достучался, минуту не трогаем: иначе каждое
# чтение файла ждало бы ConnectTimeout
_ssh_down = {}
SSH_DOWN_SECONDS = 60


def _target_key(target):
    return (target["host"], target["user"], int(target["port"]))


def ssh_is_down(target):
    import time

    return _ssh_down.get(_target_key(target), 0) > time.time()


class RemoteFS(object):
    remote = True

    def __init__(self, target):
        self.target = target

    def _run(self, script, binary=False, timeout=SSH_TIMEOUT_SECONDS):
        import time

        empty = b"" if binary else ""

        if ssh_is_down(self.target):
            return 255, empty

        try:
            rc, out = run(self.target, script, timeout=timeout, binary=binary)
        except (subprocess.TimeoutExpired, OSError):
            rc, out = 255, empty

        # 255 — ошибка самого ssh (нет связи, ключ), а не команды
        if rc == 255:
            _ssh_down[_target_key(self.target)] = time.time() + SSH_DOWN_SECONDS
        else:
            _ssh_down.pop(_target_key(self.target), None)

        return rc, out

    def isdir(self, path):
        return self._run("test -d {}".format(shlex.quote(path)))[0] == 0

    def isfile(self, path):
        return self._run("test -f {}".format(shlex.quote(path)))[0] == 0

    def read(self, path, tail=None):
        script = ("tail -c {} {}".format(int(tail), shlex.quote(path)) if tail
                  else "cat {}".format(shlex.quote(path)))
        rc, out = self._run(script, binary=True, timeout=120)
        return out.decode("utf-8", "replace") if rc == 0 else None

    def read_bytes(self, path):
        rc, out = self._run("cat {}".format(shlex.quote(path)), binary=True,
                            timeout=120)
        return out if rc == 0 else None

    def glob(self, base, *parts):
        for part in parts:
            if not _GLOB_PART_RE.match(part):
                raise ValueError("Недопустимый шаблон: {}".format(part))

        pattern = shlex.quote(base.rstrip("/")) + "/" + "/".join(parts)
        rc, out = self._run('for f in {}; do [ -e "$f" ] && echo "$f"; done; true'
                            .format(pattern))
        return sorted(line for line in out.splitlines() if line.startswith("/"))

    def disk_usage(self, path):
        rc, out = self._run("df -P -B1 {} | tail -1".format(shlex.quote(path)))
        parts = out.split()

        if rc != 0 or len(parts) < 4 or not parts[1].isdigit():
            return None

        return {"total": int(parts[1]), "used": int(parts[2]),
                "free": int(parts[3])}


def fs_for(connection_id):
    target = resolve_target(connection_id)
    return RemoteFS(target) if target else LocalFS()


# заполненность диска каталога на координаторе: обзор страницы
# опрашивается раз в 20 с и не должен ждать SSH — считаем в фоне
_disk_cache = {}
DISK_CACHE_SECONDS = 300


def remote_disk_usage(connection_id, target, path):
    """Кэш {total, used, free, path} или None; обновление — в фоне."""
    import threading
    import time

    key = (int(connection_id), path)
    hit = _disk_cache.get(key)
    now = time.time()

    if not hit or now - hit[0] > DISK_CACHE_SECONDS:
        _disk_cache[key] = (now, hit[1] if hit else None)

        def refresh():
            usage = RemoteFS(target).disk_usage(path)
            _disk_cache[key] = (time.time(), dict(usage, path=path) if usage else None)

        threading.Thread(target=refresh, daemon=True).start()

    return _disk_cache[key][1]
