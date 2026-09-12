# -*- coding: utf-8 -*-
"""
Ассистент Vacuum / Analyze: смотрит на статистику таблиц (как вкладка
«Здоровье БД») и советует, что именно запустить — VACUUM, ANALYZE,
VACUUM ANALYZE, VACUUM FULL или VACUUM FREEZE — с готовой командой
и объяснением причины.

Правила чистые (build_recommendations), сбор статистики отделён —
это позволяет тестировать логику без БД.
"""

import time

try:
    from modules.gpcopy import open_psycopg2_connection_by_cfg, quote_ident
except ImportError:
    from gpcopy import open_psycopg2_connection_by_cfg, quote_ident

try:
    from modules.connections import get_connection_by_id
except ImportError:
    from connections import get_connection_by_id

try:
    from modules.db_health import _SYSTEM_SCHEMAS
except Exception:
    _SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "gp_toolkit",
                       "pg_toolkit", "pg_aoseg", "pg_bitmapindex")


# пороги (доли и штуки)
DEAD_RATIO_WARN = 0.2          # мёртвых строк >= 20% -> VACUUM
DEAD_RATIO_CRIT = 0.5          # >= 50% -> критично
MIN_DEAD_TUPLES = 1000         # меньше — не трогаем, не стоит того
MOD_RATIO_WARN = 0.2           # изменено >= 20% строк с последнего ANALYZE
ANALYZE_AGE_WARN_DAYS = 30     # статистика старше месяца
BLOAT_FULL_FACTOR = 4          # страниц в N раз больше ожидаемого -> VACUUM FULL
FREEZE_AGE_CRIT = 1000000000   # возраст relfrozenxid -> VACUUM FREEZE

MAX_RECOMMENDATIONS = 200

_SEVERITY_ORDER = {"crit": 0, "warn": 1}


def full_name(schema, table):
    return "{}.{}".format(quote_ident(schema), quote_ident(table))


def build_command(action, schema, table):
    """Готовая SQL-команда для рекомендации."""
    verbs = {
        "VACUUM": "VACUUM",
        "VACUUM_FULL": "VACUUM FULL",
        "ANALYZE": "ANALYZE",
        "VACUUM_ANALYZE": "VACUUM ANALYZE",
        "VACUUM_FREEZE": "VACUUM FREEZE",
    }

    if action not in verbs:
        raise ValueError("Неизвестное действие: {}".format(action))

    return "{} {};".format(verbs[action], full_name(schema, table))


def build_recommendations(stats, bloat=None, max_rows=MAX_RECOMMENDATIONS):
    """
    Чистый движок правил.

    stats: [{schemaname, relname, n_live_tup, n_dead_tup, n_mod (или None),
             size_bytes, never_vacuumed, never_analyzed,
             analyze_age_days, vacuum_age_days, frozen_age (или None)}]
    bloat: [{schemaname, relname, pages, expected_pages, diag}]
    -> список рекомендаций, отсортированный crit -> warn, внутри по размеру.
    """
    bloat_map = {}

    for row in (bloat or []):
        key = (row.get("schemaname"), row.get("relname"))
        bloat_map[key] = row

    out = []

    for row in stats:
        schema = row.get("schemaname")
        table = row.get("relname")

        if not schema or not table:
            continue

        live = int(row.get("n_live_tup") or 0)
        dead = int(row.get("n_dead_tup") or 0)
        n_mod = row.get("n_mod")
        frozen_age = row.get("frozen_age")

        need_vacuum = False
        need_analyze = False
        need_full = False
        need_freeze = False
        severity = "warn"
        reasons = []

        # --- раздутость (gp_bloat_diag) ---
        b = bloat_map.get((schema, table))

        if b:
            pages = int(b.get("pages") or 0)
            expected = int(b.get("expected_pages") or 0)
            factor = (pages / float(expected)) if expected > 0 else 0

            if factor >= BLOAT_FULL_FACTOR or "significant" in str(
                    b.get("diag") or "").lower():
                need_full = True
                severity = "crit"
                reasons.append(
                    "Сильная раздутость: {} страниц вместо ~{} (x{:.1f}). "
                    "VACUUM FULL перезапишет таблицу, но блокирует её "
                    "(ACCESS EXCLUSIVE) — запускать в окно обслуживания.".format(
                        pages, expected, factor if factor else 0,
                    )
                )
            else:
                need_vacuum = True
                reasons.append(
                    "Умеренная раздутость: {} страниц вместо ~{} — "
                    "обычный VACUUM вернёт место в оборот.".format(
                        pages, expected,
                    )
                )

        # --- мёртвые строки ---
        total = live + dead
        ratio = (dead / float(total)) if total > 0 else 0

        if dead >= MIN_DEAD_TUPLES and ratio >= DEAD_RATIO_WARN and not need_full:
            need_vacuum = True

            if ratio >= DEAD_RATIO_CRIT:
                severity = "crit"

            reasons.append(
                "Мёртвых строк {} из {} ({:.0f}%){}".format(
                    dead, total, ratio * 100,
                    " — таблицу ни разу не вакуумировали" if row.get(
                        "never_vacuumed") else "",
                )
            )

        # --- статистика для планировщика ---
        if row.get("never_analyzed") and live > 0:
            need_analyze = True
            severity = "crit"
            reasons.append(
                "ANALYZE не выполнялся ни разу — планировщик строит планы "
                "вслепую."
            )
        else:
            mod_ratio = (int(n_mod) / float(live)) if (
                n_mod is not None and live > 0) else 0
            age = row.get("analyze_age_days")

            if mod_ratio >= MOD_RATIO_WARN:
                need_analyze = True
                reasons.append(
                    "С последнего ANALYZE изменено ~{:.0f}% строк — "
                    "статистика устарела.".format(mod_ratio * 100)
                )
            elif age is not None and int(age) >= ANALYZE_AGE_WARN_DAYS:
                need_analyze = True
                reasons.append(
                    "ANALYZE был {} дней назад.".format(int(age))
                )

        # --- заморозка xid ---
        if frozen_age is not None and int(frozen_age) >= FREEZE_AGE_CRIT:
            need_freeze = True
            severity = "crit"
            reasons.append(
                "Возраст relfrozenxid {} — близко к пределу отката xid, "
                "нужен VACUUM FREEZE.".format(int(frozen_age))
            )

        # --- итоговое действие ---
        if need_full:
            action = "VACUUM_FULL"
        elif need_freeze:
            action = "VACUUM_FREEZE"
        elif need_vacuum and need_analyze:
            action = "VACUUM_ANALYZE"
        elif need_vacuum:
            action = "VACUUM"
        elif need_analyze:
            action = "ANALYZE"
        else:
            continue

        out.append({
            "schema": schema,
            "table": table,
            "action": action,
            "severity": severity,
            "reasons": reasons,
            "command": build_command(action, schema, table),
            "size_bytes": int(row.get("size_bytes") or 0),
            "n_live_tup": live,
            "n_dead_tup": dead,
            "dead_ratio": round(ratio, 3),
            "analyze_age_days": row.get("analyze_age_days"),
            "vacuum_age_days": row.get("vacuum_age_days"),
        })

    out.sort(key=lambda r: (
        _SEVERITY_ORDER.get(r["severity"], 9),
        -r["size_bytes"],
        -r["n_dead_tup"],
    ))

    return out[:max_rows]


# ------------------------------------------------------------------
# сбор статистики из целевой БД
# ------------------------------------------------------------------

def _dict_rows(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


_STATS_SQL = """
    SELECT s.schemaname, s.relname,
        s.n_live_tup::bigint AS n_live_tup,
        s.n_dead_tup::bigint AS n_dead_tup,
        {mod_col} AS n_mod,
        pg_total_relation_size(s.relid)::bigint AS size_bytes,
        (s.last_vacuum IS NULL AND s.last_autovacuum IS NULL) AS never_vacuumed,
        (s.last_analyze IS NULL AND s.last_autoanalyze IS NULL) AS never_analyzed,
        CASE WHEN s.last_analyze IS NULL AND s.last_autoanalyze IS NULL
             THEN NULL
             ELSE EXTRACT(EPOCH FROM (now() - GREATEST(
                 COALESCE(s.last_analyze, 'epoch'::timestamptz),
                 COALESCE(s.last_autoanalyze, 'epoch'::timestamptz)
             )))::bigint / 86400 END AS analyze_age_days,
        CASE WHEN s.last_vacuum IS NULL AND s.last_autovacuum IS NULL
             THEN NULL
             ELSE EXTRACT(EPOCH FROM (now() - GREATEST(
                 COALESCE(s.last_vacuum, 'epoch'::timestamptz),
                 COALESCE(s.last_autovacuum, 'epoch'::timestamptz)
             )))::bigint / 86400 END AS vacuum_age_days,
        CASE WHEN c.relfrozenxid <> '0'::xid
             THEN age(c.relfrozenxid)::bigint END AS frozen_age
    FROM pg_stat_all_tables s
    JOIN pg_class c ON c.oid = s.relid
    WHERE s.schemaname NOT IN ({schemas})
      AND (s.n_live_tup > 0 OR s.n_dead_tup > 0)
    ORDER BY s.n_dead_tup DESC, s.n_live_tup DESC
    LIMIT 1000
"""


def collect_stats(cur):
    schemas = ", ".join("'%s'" % s for s in _SYSTEM_SCHEMAS)

    try:
        cur.execute(_STATS_SQL.format(
            mod_col="s.n_mod_since_analyze::bigint",
            schemas=schemas,
        ))
        return _dict_rows(cur)
    except Exception:
        # старые версии без n_mod_since_analyze
        cur.connection.rollback()
        cur.execute(_STATS_SQL.format(mod_col="NULL::bigint", schemas=schemas))
        return _dict_rows(cur)


def collect_bloat(cur):
    """gp_toolkit.gp_bloat_diag; на не-Greenplum её нет — пустой список."""
    schemas = ", ".join("'%s'" % s for s in _SYSTEM_SCHEMAS)

    try:
        cur.execute("""
            SELECT bdinspname AS schemaname, bdirelname AS relname,
                   bdirelpages::bigint AS pages,
                   bdiexppages::bigint AS expected_pages,
                   bdidiag AS diag
            FROM gp_toolkit.gp_bloat_diag
            WHERE bdinspname NOT IN (%s)
            LIMIT 500
        """ % schemas)
        return _dict_rows(cur)
    except Exception:
        cur.connection.rollback()
        return []


def advise(connection_id):
    """Полный проход: подключение -> статистика -> рекомендации."""
    conn_cfg = get_connection_by_id(int(connection_id))

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    started = time.time()
    conn = open_psycopg2_connection_by_cfg(conn_cfg)

    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '20s'")
        stats = collect_stats(cur)
        bloat = collect_bloat(cur)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    recommendations = build_recommendations(stats, bloat)

    return {
        "ok": True,
        "duration_seconds": round(time.time() - started, 1),
        "tables_scanned": len(stats),
        "bloat_rows": len(bloat),
        "recommendations": recommendations,
        "counts": {
            "crit": sum(1 for r in recommendations if r["severity"] == "crit"),
            "warn": sum(1 for r in recommendations if r["severity"] == "warn"),
        },
    }
