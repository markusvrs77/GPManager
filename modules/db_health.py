# -*- coding: utf-8 -*-
"""
«Здоровье БД» — аналитика кластера БЕЗ нагрузки.

Все запросы читают только системные каталоги и статистические вьюхи
(pg_stat_*, pg_locks, gp_segment_configuration, gp_toolkit.*) — никаких
сканов пользовательских таблиц. Каждый запрос ограничен statement_timeout,
результат кэшируется на TTL, чтобы обновление страницы не дёргало кластер.
"""

import time

try:
    from connections import get_connection_by_id
except ImportError:
    from modules.connections import get_connection_by_id

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg


CACHE_TTL_SECONDS = 60
STATEMENT_TIMEOUT_MS = 15000

LONG_QUERY_SEC = 300          # запрос дольше 5 минут — warning
IDLE_TXN_SEC = 60             # idle in transaction дольше минуты — warning
STALE_ANALYZE_WARN_DAYS = 7
STALE_ANALYZE_CRIT_DAYS = 30
DEAD_RATIO_WARN = 0.2         # мёртвых строк больше 20% живых

_SYSTEM_SCHEMAS = (
    "pg_catalog", "information_schema", "gp_toolkit",
    "pg_toast", "pg_aoseg", "pg_bitmapindex", "gpmanager_sync_stage",
)

_cache = {}


# ------------------------------------------------------------------
# чистые хелперы (юнит-тестируются без БД)
# ------------------------------------------------------------------

def classify_staleness_days(days):
    """ok | warn | crit по давности ANALYZE (None = никогда)."""
    if days is None:
        return "crit"
    if days >= STALE_ANALYZE_CRIT_DAYS:
        return "crit"
    if days >= STALE_ANALYZE_WARN_DAYS:
        return "warn"
    return "ok"


def verdict_segments(total, down, unbalanced):
    if down:
        return "crit"
    if unbalanced:
        return "warn"
    return "ok" if total else "warn"


def verdict_activity(long_queries, idle_txn):
    if long_queries:
        return "warn"
    if idle_txn:
        return "warn"
    return "ok"


def dead_ratio(n_live, n_dead):
    live = max(int(n_live or 0), 1)
    return round(float(n_dead or 0) / live, 3)


# ------------------------------------------------------------------
# сбор
# ------------------------------------------------------------------

def _rows(cur, sql, params=None):
    # без params не передаём второй аргумент, иначе psycopg2
    # интерполирует любые '%' в SQL (LIKE и т.п.)
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _section(fn, cur):
    """Одна упавшая секция не валит весь отчёт."""
    try:
        return fn(cur)
    except Exception as e:
        try:
            cur.connection.rollback()
        except Exception:
            pass
        return {"error": str(e).strip().split("\n")[0][:300]}


def _collect_segments(cur):
    rows = _rows(cur, """
        SELECT content, role, preferred_role, status, hostname, port
        FROM gp_segment_configuration
    """)
    down = [r for r in rows if r["status"] != "u"]
    unbalanced = [r for r in rows if r["role"] != r["preferred_role"]]

    # content = -1 — координатор (master) и его standby, не сегменты данных
    coord = [r for r in rows if int(r["content"]) < 0]
    segs = [r for r in rows if int(r["content"]) >= 0]

    return {
        "total": len(rows),
        "masters": sum(1 for r in coord if r["role"] == "p"),
        "standbys": sum(1 for r in coord if r["role"] == "m"),
        "primaries": sum(1 for r in segs if r["role"] == "p"),
        "mirrors": sum(1 for r in segs if r["role"] == "m"),
        "down": len(down),
        "unbalanced": len(unbalanced),
        "down_list": [
            {"content": r["content"], "hostname": r["hostname"], "port": r["port"]}
            for r in down[:20]
        ],
        "verdict": verdict_segments(len(rows), len(down), len(unbalanced)),
    }


def _collect_database(cur):
    rows = _rows(cur, """
        SELECT numbackends, xact_commit, xact_rollback,
               blks_read, blks_hit, temp_files, temp_bytes, deadlocks
        FROM pg_stat_database
        WHERE datname = current_database()
    """)
    if not rows:
        return {}
    r = rows[0]
    hit = int(r.get("blks_hit") or 0)
    read = int(r.get("blks_read") or 0)
    r["cache_hit_pct"] = round(hit * 100.0 / max(hit + read, 1), 2)
    return r


def _collect_activity(cur):
    states = _rows(cur, """
        SELECT COALESCE(state, 'unknown') AS state, count(*) AS cnt
        FROM pg_stat_activity
        GROUP BY 1
    """)

    active = _rows(cur, """
        SELECT pid, usename, COALESCE(state, '') AS state,
               EXTRACT(EPOCH FROM (now() - query_start))::bigint AS query_sec,
               EXTRACT(EPOCH FROM (now() - xact_start))::bigint AS xact_sec,
               LEFT(COALESCE(query, ''), 300) AS query
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND COALESCE(state, '') NOT IN ('', 'idle')
          AND COALESCE(query, '') NOT LIKE 'START_REPLICATION%'
        ORDER BY query_start ASC NULLS LAST
        LIMIT 50
    """)

    long_queries = [
        a for a in active
        if a["state"] == "active" and (a["query_sec"] or 0) >= LONG_QUERY_SEC
    ]
    idle_txn = [
        a for a in active
        if a["state"].startswith("idle in transaction")
        and (a["xact_sec"] or 0) >= IDLE_TXN_SEC
    ]

    return {
        "states": {s["state"]: int(s["cnt"]) for s in states},
        "long_queries": long_queries[:15],
        "idle_in_transaction": idle_txn[:15],
        "verdict": verdict_activity(long_queries, idle_txn),
    }


def _collect_locks(cur):
    rows = _rows(cur, """
        SELECT
            w.pid AS waiting_pid,
            w.usename AS waiting_user,
            EXTRACT(EPOCH FROM (now() - w.query_start))::bigint AS waiting_sec,
            LEFT(COALESCE(w.query, ''), 200) AS waiting_query,
            b.pid AS blocking_pid,
            b.usename AS blocking_user,
            LEFT(COALESCE(b.query, ''), 200) AS blocking_query
        FROM pg_locks blocked
        JOIN pg_stat_activity w ON w.pid = blocked.pid
        JOIN pg_locks blocking
          ON blocking.granted
         AND blocking.pid <> blocked.pid
         AND blocking.locktype = blocked.locktype
         AND blocking.database IS NOT DISTINCT FROM blocked.database
         AND blocking.relation IS NOT DISTINCT FROM blocked.relation
         AND blocking.page IS NOT DISTINCT FROM blocked.page
         AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
         AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
         AND blocking.classid IS NOT DISTINCT FROM blocked.classid
         AND blocking.objid IS NOT DISTINCT FROM blocked.objid
         AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
        JOIN pg_stat_activity b ON b.pid = blocking.pid
        WHERE NOT blocked.granted
        LIMIT 20
    """)
    return {
        "chains": rows,
        "verdict": "crit" if rows else "ok",
    }


def _collect_vacuum_analyze(cur):
    not_in = ", ".join("'%s'" % s for s in _SYSTEM_SCHEMAS)

    summary = _rows(cur, """
        SELECT
            count(*) AS total,
            SUM(CASE WHEN last_analyze IS NULL AND last_autoanalyze IS NULL
                     THEN 1 ELSE 0 END) AS never_analyzed,
            SUM(CASE WHEN last_vacuum IS NULL AND last_autovacuum IS NULL
                     THEN 1 ELSE 0 END) AS never_vacuumed
        FROM pg_stat_all_tables
        WHERE schemaname NOT IN (%s) AND n_live_tup > 0
    """ % not_in)[0]

    stale = _rows(cur, """
        SELECT schemaname, relname, n_live_tup, n_dead_tup,
            EXTRACT(EPOCH FROM (now() - GREATEST(
                COALESCE(last_analyze, 'epoch'::timestamptz),
                COALESCE(last_autoanalyze, 'epoch'::timestamptz)
            )))::bigint / 86400 AS analyze_age_days,
            (last_analyze IS NULL AND last_autoanalyze IS NULL) AS never_analyzed
        FROM pg_stat_all_tables
        WHERE schemaname NOT IN (%s) AND n_live_tup > 0
        ORDER BY GREATEST(
            COALESCE(last_analyze, 'epoch'::timestamptz),
            COALESCE(last_autoanalyze, 'epoch'::timestamptz)
        ) ASC
        LIMIT 15
    """ % not_in)

    dead = _rows(cur, """
        SELECT schemaname, relname, n_live_tup, n_dead_tup,
            EXTRACT(EPOCH FROM (now() - GREATEST(
                COALESCE(last_vacuum, 'epoch'::timestamptz),
                COALESCE(last_autovacuum, 'epoch'::timestamptz)
            )))::bigint / 86400 AS vacuum_age_days,
            (last_vacuum IS NULL AND last_autovacuum IS NULL) AS never_vacuumed
        FROM pg_stat_all_tables
        WHERE schemaname NOT IN (%s) AND n_dead_tup > 0
        ORDER BY n_dead_tup DESC
        LIMIT 15
    """ % not_in)

    for r in stale:
        days = None if r.pop("never_analyzed") else int(r["analyze_age_days"] or 0)
        r["analyze_age_days"] = days
        r["verdict"] = classify_staleness_days(days)
    for r in dead:
        r["ratio"] = dead_ratio(r["n_live_tup"], r["n_dead_tup"])
        r["verdict"] = "crit" if r["ratio"] >= DEAD_RATIO_WARN else "warn"
        if r.pop("never_vacuumed"):
            r["vacuum_age_days"] = None
        else:
            r["vacuum_age_days"] = int(r["vacuum_age_days"] or 0)

    never = int(summary.get("never_analyzed") or 0)
    verdict = "ok"
    if never or any(r["verdict"] == "crit" for r in stale + dead):
        verdict = "crit"
    elif any(r["verdict"] == "warn" for r in stale + dead):
        verdict = "warn"

    return {
        "total_tables": int(summary.get("total") or 0),
        "never_analyzed": never,
        "never_vacuumed": int(summary.get("never_vacuumed") or 0),
        "stale_analyze": stale,
        "top_dead": dead,
        "verdict": verdict,
    }


def _collect_spill(cur):
    rows = _rows(cur, """
        SELECT sess_id, usename, LEFT(COALESCE(query, ''), 200) AS query,
               SUM(size)::bigint AS total_bytes,
               SUM(numfiles)::bigint AS files
        FROM gp_toolkit.gp_workfile_usage_per_query
        GROUP BY sess_id, usename, query
        ORDER BY total_bytes DESC
        LIMIT 10
    """)
    return {
        "queries": rows,
        "verdict": "warn" if rows else "ok",
    }


_SECTIONS = [
    ("segments", _collect_segments),
    ("database", _collect_database),
    ("activity", _collect_activity),
    ("locks", _collect_locks),
    ("vacuum_analyze", _collect_vacuum_analyze),
    ("spill", _collect_spill),
]


def collect_health(connection_id, force=False):
    """Отчёт о здоровье кластера. Кэш TTL, чтобы не дёргать БД при F5."""
    key = int(connection_id)
    now = time.time()

    if not force:
        hit = _cache.get(key)
        if hit and now - hit[0] < CACHE_TTL_SECONDS:
            data = dict(hit[1])
            data["cached"] = True
            return data

    cfg = get_connection_by_id(key)
    if not cfg:
        raise Exception("Connection %s not found" % key)

    conn = open_psycopg2_connection_by_cfg(cfg)
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = %s" % int(STATEMENT_TIMEOUT_MS))

        sections = {}
        for name, fn in _SECTIONS:
            sections[name] = _section(fn, cur)

        data = {
            "ok": True,
            "cached": False,
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ttl_seconds": CACHE_TTL_SECONDS,
            "sections": sections,
        }
        _cache[key] = (now, data)
        return data
    finally:
        try:
            conn.close()
        except Exception:
            pass
