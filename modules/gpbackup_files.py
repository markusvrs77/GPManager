# -*- coding: utf-8 -*-
"""
Копии gpbackup прямо по файлам на координаторе — замена gpbackup_manager.

gpbackup_manager в неинтерактивном запуске ждёт терминал и падает, а всё,
что от него нужно странице, gpbackup и так пишет на диск:

  <каталог>/backups/<YYYYMMDD>/<ts>/
      gpbackup_<ts>_config.yaml   база, тип, статус, каталог
      gpbackup_<ts>_report        отчёт: размер БД, счётчики объектов
      gpbackup_<ts>_toc.yaml      оглавление: точный список таблиц
  <данные координатора>/gpbackup_history.db   история (SQLite)

<каталог> — <backup_dir>/gpseg-1 при --backup-dir, иначе каталог данных
координатора. GPManager запускает gpbackup на координаторе, поэтому всё
читается локально. Модуль ничего на диске не меняет: только чтение.
"""

import glob
import json
import os
import re
import sqlite3

from db import sqlite_cursor

_TS_RE = re.compile(r"^\d{14}$")
_DATE_RE = re.compile(r"^\d{8}$")

# имя, которое можно передать в --include-schema / --include-table
_NAME_RE = re.compile(r"^[A-Za-z0-9_$]+$")

# сколько таблиц отдавать странице для выбора
MAX_TABLES_LISTED = 20000

# каталог данных координатора по подключению (на живом кластере не меняется)
_coordinator_dirs = {}


# ------------------------------------------------------------------
# разбор файлов
# ------------------------------------------------------------------

_REPORT_FIELD_RE = re.compile(r"^([a-z][a-z ]*?):\s+(.*?)\s*$")
_REPORT_COUNT_RE = re.compile(r"^([a-z][a-z0-9 _/-]*?)\s+(\d+)\s*$")


def parse_backup_report(text):
    """
    Отчёт gpbackup -> {fields: {...}, objects: {...}}.

    fields — строки «ключ: значение» (database size, duration, backup
    status …); objects — блок «count of database objects in backup»,
    ключи в нижнем регистре, как в отчёте (tables, schemas …).
    """
    fields = {}
    objects = {}
    in_counts = False

    for raw in (text or "").splitlines():
        line = raw.strip()

        if not line:
            continue

        if line.lower().startswith("count of database objects"):
            in_counts = True
            continue

        if in_counts:
            match = _REPORT_COUNT_RE.match(line)

            if match:
                objects[match.group(1).strip()] = int(match.group(2))
                continue

            in_counts = False

        match = _REPORT_FIELD_RE.match(line)

        if match and match.group(2):
            fields.setdefault(match.group(1).strip(), match.group(2))

    return {"fields": fields, "objects": objects}


def _yaml_scalar(value):
    value = value.strip()

    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value[1:-1].replace("''", "'")

    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1].replace('\\"', '"').replace("\\\\", "\\")

    return value


def _unquote_ident(name):
    """quote_ident() из gpbackup -> имя как в каталоге: "Sales" -> Sales."""
    if len(name) >= 2 and name[0] == name[-1] == '"':
        return name[1:-1].replace('""', '"')

    return name


def parse_backup_config(text):
    """
    gpbackup_<ts>_config.yaml -> {ключ: строка | [строки]}.
    Только верхний уровень: скаляры и списки «- x».
    """
    out = {}
    current = None

    for raw in (text or "").splitlines():
        if not raw.strip():
            continue

        if raw[0] not in " \t-":
            key, _, value = raw.partition(":")
            current = key.strip().lower()
            value = value.strip()

            if value in ("", "[]"):
                out[current] = []
            else:
                out[current] = _yaml_scalar(value)
            continue

        stripped = raw.strip()

        if stripped.startswith("- ") and isinstance(out.get(current), list):
            out[current].append(_yaml_scalar(stripped[2:]))

    return out


def config_backup_type(cfg):
    """Флаги config.yaml -> наш тип бэкапа."""
    def flag(name):
        return str(cfg.get(name) or "").lower() == "true"

    if flag("incremental"):
        return "incremental"
    if flag("metadataonly"):
        return "metadata_only"
    if flag("dataonly"):
        return "data_only"
    return "full"


def config_status(raw):
    """Статус gpbackup -> статус реестра (None — копия ещё пишется)."""
    value = str(raw or "").strip().lower()

    if value in ("success", ""):
        return "done"
    if value.startswith("partial"):
        return "partial"
    if value.startswith("in progress"):
        return None
    return "failed"


def parse_toc(text):
    """
    gpbackup_<ts>_toc.yaml -> {tables: [{schema, name, rows, data}], has_rows}.

    Таблица — запись predataentries с objecttype TABLE или запись
    dataentries. Листовые партиции (dataentries с partitionroot)
    сворачиваются в корневую таблицу, их строки суммируются. Разбор
    построчный: файл бывает на десятки мегабайт, полный YAML не нужен.
    """
    state = {"section": None, "entry": None, "has_rows": False}
    order = []
    tables = {}
    leaves = set()

    def flush():
        entry, section = state["entry"], state["section"]

        if not entry or section not in ("predataentries", "dataentries"):
            return

        schema = _unquote_ident(entry.get("schema", ""))
        name = _unquote_ident(entry.get("name", ""))

        if not schema or not name:
            return

        rows = None

        if section == "predataentries":
            if entry.get("objecttype", "").upper() != "TABLE":
                return
            key = (schema, name)
        else:
            root = entry.get("partitionroot", "")
            raw_rows = entry.get("rowscopied")

            if raw_rows is not None and raw_rows.lstrip("-").isdigit():
                rows = int(raw_rows)
                state["has_rows"] = True

            if root:
                leaves.add((schema, name))
                key = (schema, _unquote_ident(root))
            else:
                key = (schema, name)

        if key not in tables:
            tables[key] = {"schema": key[0], "name": key[1], "rows": None,
                           "data": False}
            order.append(key)

        if section == "dataentries":
            tables[key]["data"] = True

            if rows is not None:
                tables[key]["rows"] = (tables[key]["rows"] or 0) + rows

    list_indent = None

    for raw in (text or "").splitlines():
        if not raw.strip():
            continue

        if raw[0] not in " \t-":
            flush()
            state["entry"] = None
            state["section"] = raw.split(":", 1)[0].strip().lower()
            list_indent = None
            continue

        stripped = raw.strip()
        indent = len(raw) - len(raw.lstrip())

        # запись списка секции: «- schema: x» (yaml.v2 не отступает список,
        # другие сериализаторы — отступают). Вложенные списки записи
        # (tier: - 0) глубже — это не новая запись.
        if stripped.startswith("- ") or stripped == "-":
            if list_indent is None:
                list_indent = indent

            if indent != list_indent:
                continue

            flush()
            state["entry"] = {}
            stripped = stripped[1:].strip()
        elif list_indent is None or indent != list_indent + 2:
            continue

        if state["entry"] is None or ":" not in stripped:
            continue

        key, _, value = stripped.partition(":")
        key = key.strip().lower()

        if key in ("schema", "name", "objecttype", "partitionroot",
                   "rowscopied") and key not in state["entry"]:
            state["entry"][key] = _yaml_scalar(value)

    flush()

    # в GP7 листовые партиции бывают отдельными TABLE в predata — их не
    # показываем: восстанавливается корневая таблица
    result = [tables[k] for k in order if k not in leaves]

    return {"tables": result, "has_rows": state["has_rows"]}


# ------------------------------------------------------------------
# где лежат файлы
# ------------------------------------------------------------------

def coordinator_data_dir(connection_id):
    """Каталог данных координатора: переменная окружения или каталог GP."""
    for var in ("COORDINATOR_DATA_DIRECTORY", "MASTER_DATA_DIRECTORY"):
        value = (os.environ.get(var) or "").strip()

        if value:
            return value

    key = int(connection_id or 0)

    if not key:
        return None

    if key in _coordinator_dirs:
        return _coordinator_dirs[key]

    datadir = None

    try:
        from modules.connections import open_gp_connection

        conn = open_gp_connection(key)

        try:
            cur = conn.cursor()
            cur.execute("SET statement_timeout = 10000")
            cur.execute(
                "SELECT datadir FROM gp_segment_configuration "
                "WHERE content = -1 AND role = 'p' LIMIT 1"
            )
            row = cur.fetchone()
            datadir = row[0] if row else None
        finally:
            conn.close()
    except Exception:
        datadir = None

    if datadir:
        _coordinator_dirs[key] = datadir

    return datadir


def _clean_dir(path):
    path = str(path or "").strip()

    if not path or ".." in path.replace("\\", "/").split("/"):
        return ""

    return path.rstrip("/\\") or path


def backup_roots(connection_id, backup_dirs=()):
    """Каталоги …/backups, где gpbackup координатора держит копии."""
    roots = []

    datadir = coordinator_data_dir(connection_id)

    if datadir:
        roots.append(os.path.join(datadir, "backups"))

    for d in backup_dirs:
        d = _clean_dir(d)

        if d:
            roots.append(os.path.join(d, "gpseg-1", "backups"))

    seen = []

    for r in roots:
        if r not in seen and os.path.isdir(r):
            seen.append(r)

    return seen


def backup_files_dir(record):
    """
    Каталог файлов копии на координаторе (…/backups/<YYYYMMDD>/<ts>)
    или None, если его не видно отсюда.
    """
    record = record or {}
    ts = str(record.get("backup_timestamp") or "")

    if not _TS_RE.match(ts):
        return None

    candidates = []

    skipped_file = _clean_dir(record.get("skipped_file"))

    if skipped_file:
        candidates.append(os.path.dirname(skipped_file))

    backup_dir = _clean_dir(record.get("backup_dir"))

    if backup_dir:
        candidates.append(os.path.join(backup_dir, "gpseg-1", "backups",
                                       ts[:8], ts))
    else:
        datadir = coordinator_data_dir(record.get("connection_id"))

        if datadir:
            candidates.append(os.path.join(datadir, "backups", ts[:8], ts))

    for path in candidates:
        if os.path.isdir(path):
            return path

    return None


def _read(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return None


# ------------------------------------------------------------------
# список копий (вместо gpbackup_manager list-backups)
# ------------------------------------------------------------------

def _truthy(value):
    return str(value if value is not None else "").strip().lower() in ("1", "true", "t")


def read_history_db(datadir):
    """
    gpbackup_history.db -> [{timestamp, dbname, backup_type, status,
    backup_dir}], без удалённых. Только чтение; колонки берутся по
    факту (схема менялась между версиями gpbackup).
    """
    path = os.path.join(datadir or "", "gpbackup_history.db")

    if not datadir or not os.path.isfile(path):
        return []

    try:
        conn = sqlite3.connect("file:{}?mode=ro".format(path), uri=True,
                               timeout=5)
    except sqlite3.Error:
        return []

    try:
        conn.row_factory = sqlite3.Row
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(backups)")}

        if "timestamp" not in cols:
            return []

        rows = [dict(r) for r in conn.execute("SELECT * FROM backups")]
    except sqlite3.Error:
        return []
    finally:
        conn.close()

    out = []

    for r in rows:
        ts = str(r.get("timestamp") or "")

        if not _TS_RE.match(ts) or str(r.get("date_deleted") or "").strip():
            continue

        status = config_status(r.get("status"))

        if status is None:
            continue

        out.append({
            "timestamp": ts,
            "dbname": r.get("database_name") or "",
            "backup_type": config_backup_type({
                "incremental": "true" if _truthy(r.get("incremental")) else "",
                "metadataonly": "true" if _truthy(r.get("metadata_only")) else "",
                "dataonly": "true" if _truthy(r.get("data_only")) else "",
            }),
            "status": status,
            "backup_dir": r.get("backup_dir") or "",
        })

    return out


def scan_backup_dirs(roots):
    """
    Обход …/backups/<YYYYMMDD>/<ts>/gpbackup_<ts>_config.yaml ->
    [{timestamp, dbname, backup_type, status, backup_dir, files_dir,
    skipped_file}].
    """
    found = {}

    for root in roots:
        pattern = os.path.join(root, "*", "*", "gpbackup_*_config.yaml")

        for path in glob.glob(pattern):
            ts_dir = os.path.dirname(path)
            ts = os.path.basename(ts_dir)
            day = os.path.basename(os.path.dirname(ts_dir))

            if not _TS_RE.match(ts) or not _DATE_RE.match(day) or ts in found:
                continue

            if os.path.basename(path) != "gpbackup_{}_config.yaml".format(ts):
                continue

            cfg = parse_backup_config(_read(path))
            status = config_status(cfg.get("status"))

            if status is None:
                continue

            skipped = os.path.join(ts_dir,
                                   "gpbackup_{}_skipped_tables".format(ts))
            has_skipped = os.path.isfile(skipped)

            if status == "done" and has_skipped:
                status = "partial"

            backup_dir = cfg.get("backupdir")

            found[ts] = {
                "timestamp": ts,
                "dbname": cfg.get("databasename") or "",
                "backup_type": config_backup_type(cfg),
                "status": status,
                "backup_dir": backup_dir if isinstance(backup_dir, str) else "",
                "files_dir": ts_dir,
                "skipped_file": skipped if has_skipped else None,
            }

    return [found[k] for k in sorted(found)]


def list_disk_backups(connection_id, backup_dirs=()):
    """
    Копии кластера на диске: история gpbackup плюс обход каталогов
    (находятся и копии, которых нет в истории).
    """
    datadir = coordinator_data_dir(connection_id)
    history = read_history_db(datadir)

    dirs = list(backup_dirs) + [h["backup_dir"] for h in history
                                if h.get("backup_dir")]
    roots = backup_roots(connection_id, dirs)
    result = {b["timestamp"]: b for b in scan_backup_dirs(roots)}

    for h in history:
        if h["timestamp"] not in result:
            result[h["timestamp"]] = dict(h, files_dir=None, skipped_file=None)

    return {
        "backups": [result[k] for k in sorted(result)],
        "datadir": datadir,
        "roots": roots,
        "history": bool(history),
    }


def read_report_text(record):
    """Текст отчёта копии с диска или None."""
    files_dir = backup_files_dir(record)

    if not files_dir:
        return None

    return _read(os.path.join(
        files_dir, "gpbackup_{}_report".format(record["backup_timestamp"])))


# ------------------------------------------------------------------
# каталог текущей БД — подсказка для выбора при восстановлении
# ------------------------------------------------------------------

def catalog_tables(connection_id, schemas=None):
    """
    Пользовательские таблицы текущей БД (без листовых партиций)
    -> [{schema, name}]. Только чтение каталога.
    """
    from modules.connections import open_gp_connection

    conn = open_gp_connection(int(connection_id))

    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 30000")
        cur.execute("SHOW server_version_num")
        version = int(cur.fetchone()[0])

        if version >= 100000:
            leaf_filter = "AND NOT c.relispartition"
        else:
            leaf_filter = ("AND NOT EXISTS (SELECT 1 FROM pg_partition_rule pr "
                           "WHERE pr.parchildrelid = c.oid)")

        sql = """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema',
                                    'gp_toolkit', 'pg_aoseg', 'pg_bitmapindex')
              AND n.nspname NOT LIKE 'pg\\_temp%%'
              AND n.nspname NOT LIKE 'pg\\_toast%%'
              {leaf}
        """.format(leaf=leaf_filter)
        params = []

        if schemas:
            sql += " AND n.nspname = ANY(%s)"
            params.append(list(schemas))

        sql += " ORDER BY 1, 2 LIMIT %s"
        params.append(MAX_TABLES_LISTED + 1)

        cur.execute(sql, params)
        return [{"schema": r[0], "name": r[1], "rows": None}
                for r in cur.fetchall()]
    finally:
        conn.close()


# ------------------------------------------------------------------
# состав копии
# ------------------------------------------------------------------

def _cached_summary(record):
    try:
        return json.loads(record.get("details_json") or "null")
    except ValueError:
        return None


def _store_summary(backup_id, summary):
    with sqlite_cursor(commit=True) as cur:
        cur.execute("UPDATE backups SET details_json = ? WHERE id = ?",
                    (json.dumps(summary, ensure_ascii=False), int(backup_id)))


def _skipped_names(record):
    try:
        return json.loads(record.get("skipped_tables") or "[]") or []
    except ValueError:
        return []


def _scope_schemas(record):
    try:
        cfg = json.loads(record.get("job_config") or "null") or {}
    except ValueError:
        return None

    return [s.strip() for s in str(cfg.get("include_schemas") or "").splitlines()
            if s.strip()] or None


def backup_contents(record, report_loader=None, use_catalog=True,
                    refresh=False):
    """
    Состав копии: сводка (кэшируется в backups.details_json) и список
    таблиц для выбора при восстановлении.

    report_loader(record) -> текст отчёта или None — запасной источник,
    когда файлов копии отсюда не видно.
    """
    ts = record.get("backup_timestamp")
    files_dir = backup_files_dir(record)

    toc = None
    report = None

    if files_dir:
        toc_text = _read(os.path.join(files_dir,
                                      "gpbackup_{}_toc.yaml".format(ts)))
        toc = parse_toc(toc_text) if toc_text else None

        report_text = _read(os.path.join(files_dir,
                                         "gpbackup_{}_report".format(ts)))
        report = parse_backup_report(report_text) if report_text else None

    summary = None if refresh else _cached_summary(record)

    if report is None and toc is None and summary is None and report_loader:
        try:
            text = report_loader(record)
            report = parse_backup_report(text) if text else None
        except Exception:
            report = None

    skipped = _skipped_names(record)

    if summary is None and (toc or report):
        objects = (report or {}).get("objects") or {}
        fields = (report or {}).get("fields") or {}
        toc_tables = toc["tables"] if toc else None

        summary = {
            "tables": len(toc_tables) if toc_tables is not None
                      else objects.get("tables"),
            "data_tables": sum(1 for t in toc_tables if t["data"])
                           if toc_tables is not None else None,
            "schemas": len({t["schema"] for t in toc_tables})
                       if toc_tables is not None else objects.get("schemas"),
            "skipped": len(skipped),
            "rows": sum(t["rows"] or 0 for t in toc_tables)
                    if toc and toc["has_rows"] else None,
            "db_size": fields.get("database size"),
            "duration": fields.get("duration"),
            "filtering": fields.get("object filtering"),
            "compression": fields.get("compression"),
            "gpbackup_version": fields.get("gpbackup version"),
            "objects": {k: v for k, v in objects.items() if v},
            "source": "toc" if toc else "report",
        }
        _store_summary(record["id"], summary)

    tables = None
    source = None
    note = None

    if toc and toc["tables"]:
        tables = toc["tables"]
        source = "toc"
    elif use_catalog and record.get("connection_id"):
        try:
            tables = catalog_tables(record["connection_id"],
                                    _scope_schemas(record))
            source = "catalog"
            note = ("Оглавление копии не видно с этого хоста — список взят "
                    "из текущей БД, в копии состав может отличаться.")
        except Exception as e:
            msg = str(e).strip().splitlines()
            note = "Список таблиц недоступен: {}".format(
                msg[0][:200] if msg else type(e).__name__)

    truncated = bool(tables) and len(tables) > MAX_TABLES_LISTED

    if truncated:
        tables = tables[:MAX_TABLES_LISTED]

    listed = None
    schemas = None

    if tables is not None:
        skipped_set = set(skipped)
        listed = []
        counts = {}

        for t in tables:
            fq = "{}.{}".format(t["schema"], t["name"])
            counts[t["schema"]] = counts.get(t["schema"], 0) + 1
            listed.append({
                "schema": t["schema"],
                "name": t["name"],
                "fq": fq,
                "rows": t.get("rows"),
                "selectable": bool(_NAME_RE.match(t["schema"])
                                   and _NAME_RE.match(t["name"])),
                "skipped": fq in skipped_set,
            })

        schemas = [{"name": k, "tables": counts[k],
                    "selectable": bool(_NAME_RE.match(k))}
                   for k in sorted(counts)]

    return {
        "summary": summary,
        "tables": listed,
        "schemas": schemas,
        "tables_source": source,
        "truncated": truncated,
        "files_dir": files_dir,
        "note": note,
    }
