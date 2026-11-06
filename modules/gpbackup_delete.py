# -*- coding: utf-8 -*-
"""
Удаление копии gpbackup с диска — без gpbackup_manager.

Каталог копии …/backups/<YYYYMMDD>/<ts> есть на координаторе и на каждом
сегменте: в каталоге данных сегмента или, с --backup-dir, в
<backup_dir>/<префикс><content> (префикс — от каталога координатора:
seg-1 -> seg, gpseg-1 -> gpseg). Удаление идёт с координатора: по SSH на
хост каждого сегмента (у gpadmin доступ между хостами кластера есть —
на нём работает сам gpbackup).

Безопасность: пути строятся только из gp_segment_configuration и метки
копии, каждый проверяется шаблоном …/backups/<8 цифр>/<14 цифр>; удаляется
только каталог этой метки. Запись в gpbackup_history.db помечается
date_deleted — инкремент на удалённую копию не опрётся.

Удаление долгое (сотни тысяч файлов на NFS) — идёт в фоне, запись реестра
на это время в статусе deleting.
"""

import posixpath
import re
import shlex
import subprocess
import threading
from datetime import datetime

from db import sqlite_cursor

_TS_RE = re.compile(r"^\d{14}$")
_PATH_RE = re.compile(r"^/[A-Za-z0-9_./-]+/backups/\d{8}/\d{14}$")
_HOST_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

DELETE_TIMEOUT_SECONDS = 6 * 3600

_lock = threading.Lock()
_running = set()

_HISTORY_PY = r'''
import sqlite3, sys
path, ts, now = sys.argv[1:4]
try:
    conn = sqlite3.connect(path, timeout=30)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(backups)")]
    if "date_deleted" not in cols:
        print("HISTORY\tno-column")
    else:
        n = conn.execute("UPDATE backups SET date_deleted = ? WHERE timestamp = ? "
                         "AND COALESCE(date_deleted, '') = ''", (now, ts)).rowcount
        conn.commit()
        print("HISTORY\t%d" % n)
except Exception as e:
    print("HISTORY\terror: %s" % e)
'''


# ------------------------------------------------------------------ пути

def segment_prefix(coordinator_datadir):
    """/data/master/seg-1 -> seg; /data/master/gpseg-1 -> gpseg."""
    base = posixpath.basename(str(coordinator_datadir or "").rstrip("/"))
    return base[:-2] if base.endswith("-1") and len(base) > 2 else "gpseg"


def backup_locations(segments, backup_dir, ts):
    """
    segments: [(content, role, hostname, datadir)] -> [(host, path)] —
    где лежит каталог копии. С --backup-dir — по одному на content (пишет
    primary); без него — в каталоге данных, и у зеркала тоже (после
    переключения файлы остались на бывшем primary).
    """
    ts = str(ts or "")

    if not _TS_RE.match(ts):
        raise ValueError("Метка копии — 14 цифр")

    coordinator = next((d for c, r, h, d in segments if int(c) == -1 and r == "p"), None)
    prefix = segment_prefix(coordinator)
    backup_dir = str(backup_dir or "").strip().rstrip("/")
    out = []

    for content, role, host, datadir in segments:
        if backup_dir:
            if role != "p":
                continue
            base = "{}/{}{}".format(backup_dir, prefix, int(content))
        else:
            base = str(datadir or "").rstrip("/")

        path = posixpath.join(base, "backups", ts[:8], ts)

        if not _HOST_RE.match(str(host or "")) or not _PATH_RE.match(path) \
                or ".." in path.split("/"):
            raise ValueError("Небезопасный путь или хост: {} {}".format(host, path))

        if (host, path) not in out:
            out.append((host, path))

    return out


def delete_script(locations):
    """Скрипт для координатора: параллельно по хостам, строка результата на путь."""
    lines = [
        "set -u",
        "del_one() {",
        '  r=$(ssh -n -o BatchMode=yes -o ConnectTimeout=20 '
        '-o StrictHostKeyChecking=accept-new "$1" "$2" 2>&1 | tail -n 1)',
        '  printf "%s\\t%s\\t%s\\n" "$1" "$3" "$r"',
        "}",
    ]

    for host, path in locations:
        p = shlex.quote(path)
        remote = ("if [ -d {p} ]; then rm -rf -- {p}; rmdir -- {d} 2>/dev/null; "
                  "if [ -d {p} ]; then echo FAIL; else echo DELETED; fi; "
                  "else echo ABSENT; fi").format(p=p, d=shlex.quote(posixpath.dirname(path)))
        lines.append("del_one {} {} {} &".format(shlex.quote(host), shlex.quote(remote), p))

    lines.append("wait")
    return "\n".join(lines) + "\n"


def parse_result(output, locations):
    """Вывод скрипта -> {deleted, absent, failed: [(host, path, why)]}."""
    seen = {}

    for line in (output or "").splitlines():
        parts = line.split("\t", 2)
        if len(parts) == 3:
            seen[(parts[0], parts[1])] = parts[2].strip()

    result = {"deleted": 0, "absent": 0, "failed": []}

    for host, path in locations:
        r = seen.get((host, path))

        if r == "DELETED":
            result["deleted"] += 1
        elif r == "ABSENT":
            result["absent"] += 1
        else:
            result["failed"].append((host, path, r or "нет ответа"))

    return result


# ------------------------------------------------------------- выполнение

def fetch_segments(connection_id):
    """gp_segment_configuration -> [(content, role, hostname, datadir)] (чтение)."""
    from modules.connections import open_gp_connection

    conn = open_gp_connection(int(connection_id))

    try:
        cur = conn.cursor()
        cur.execute("SELECT content, role, hostname, datadir "
                    "FROM gp_segment_configuration ORDER BY content, role")
        return [tuple(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _exec(connection_id, script, stdin_text=None, timeout=DELETE_TIMEOUT_SECONDS):
    """На координаторе: по SSH (если gpbackup запускается там) или здесь."""
    from modules import gp_remote

    target = gp_remote.resolve_target(connection_id)

    if target:
        return gp_remote.run(target, script, timeout=timeout, stdin_text=stdin_text)

    proc = subprocess.run(["bash", "-c", script], input=(stdin_text or "").encode("utf-8"),
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    return proc.returncode, proc.stdout.decode("utf-8", "replace")


def _mark_history(connection_id, coordinator_datadir, ts):
    if not coordinator_datadir:
        return "нет каталога координатора"

    path = posixpath.join(str(coordinator_datadir).rstrip("/"), "gpbackup_history.db")
    script = "python3 - {} {} {}".format(
        shlex.quote(path), shlex.quote(ts),
        shlex.quote(datetime.now().strftime("%Y%m%d%H%M%S")))

    try:
        _rc, out = _exec(connection_id, script, stdin_text=_HISTORY_PY, timeout=120)
    except Exception as e:
        return str(e)[:200]

    line = next((l for l in out.splitlines() if l.startswith("HISTORY\t")), "")
    return line.split("\t", 1)[1] if line else (out.strip()[-200:] or "нет ответа")


def delete_backup_files(record, segments):
    """Удалить файлы одной копии. -> (ok, текст для журнала)."""
    ts = str(record.get("backup_timestamp") or "")
    locations = backup_locations(segments, record.get("backup_dir"), ts)
    _rc, out = _exec(record["connection_id"], delete_script(locations))
    result = parse_result(out, locations)

    coordinator = next((d for c, r, h, d in segments if int(c) == -1 and r == "p"), None)
    history = _mark_history(record["connection_id"], coordinator, ts)

    text = "удалено каталогов: {}, не было: {}, история: {}".format(
        result["deleted"], result["absent"], history)

    if result["failed"]:
        text += "; не удалось: " + "; ".join(
            "{} {} — {}".format(h, p, why[:120]) for h, p, why in result["failed"][:5])
        return False, text

    return True, text


def _set_status(connection_id, ts, status, error=None):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE backups SET status = ?, delete_error = ? "
                    "WHERE connection_id = ? AND backup_timestamp = ?",
                    (status, error, int(connection_id), str(ts)))


def start_delete(records):
    """
    Удалить копии с диска в фоне (основная + её доборы). records — записи
    реестра. ValueError — нечего или уже удаляется.
    """
    records = [r for r in records
               if _TS_RE.match(str(r.get("backup_timestamp") or ""))
               and r.get("status") not in ("deleted", "deleting")]

    if not records:
        raise ValueError("У записи нет файлов на диске — уберите её из списка")

    keys = {(int(r["connection_id"]), str(r["backup_timestamp"])) for r in records}

    with _lock:
        if keys & _running:
            raise ValueError("Копия уже удаляется")
        _running.update(keys)

    before = {(int(r["connection_id"]), str(r["backup_timestamp"])): r.get("status")
              for r in records}

    for cid, ts in keys:
        _set_status(cid, ts, "deleting")

    _spawn(_delete_worker, records, before)
    return len(keys)


def _spawn(target, *args):
    threading.Thread(target=target, args=args, daemon=True).start()


def _delete_worker(records, before):
    segments = {}

    for r in records:
        cid, ts = int(r["connection_id"]), str(r["backup_timestamp"])

        try:
            if cid not in segments:
                segments[cid] = fetch_segments(cid)
            ok, text = delete_backup_files(r, segments[cid])
        except Exception as e:
            ok, text = False, str(e)[:500]

        if ok:
            _set_status(cid, ts, "deleted", None)
        else:
            _set_status(cid, ts, before.get((cid, ts)) or "failed",
                        "Удалить с диска не удалось: " + text)

        with _lock:
            _running.discard((cid, ts))


def reset_stale_deleting():
    """После рестарта: прерванное удаление — копия неполная, повторить."""
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE backups SET status = 'failed', delete_error = ? "
                    "WHERE status = 'deleting'",
                    ("Удаление прервано рестартом Opsentri — файлы удалены не все, "
                     "повторите «Удалить с диска».",))
        return cur.rowcount
