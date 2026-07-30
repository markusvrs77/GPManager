# -*- coding: utf-8 -*-
"""
Пользователи и гранты: кто и что может делать с таблицами.

Только чтение системных каталогов (pg_roles, pg_auth_members, relacl,
attacl) — никакой нагрузки на данные. Права, полученные через роли,
разворачиваются до пользователя с пометкой источника. SQL для
GRANT/REVOKE только генерируется текстом — модуль ничего не меняет.
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
                       "pg_toast", "pg_aoseg", "pg_bitmapindex")


# порядок букв в чипах: S I U D T R G
PRIV_ORDER = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
              "REFERENCES", "TRIGGER")

WRITE_PRIVS = ("INSERT", "UPDATE", "DELETE")
DANGEROUS_PRIVS = ("TRUNCATE", "DELETE")

MAX_TABLES_PER_USER = 400
# в графе держим самые «populярные» таблицы — иначе полотно превращается
# в кашу из связей
MAX_GRAPH_TABLES = 140

_CACHE = {}
_CACHE_TTL = 90


# ------------------------------------------------------------------
# чистые функции
# ------------------------------------------------------------------

def sort_privileges(privs):
    """Привилегии в каноническом порядке, без дублей."""
    seen = set()
    out = []

    for p in PRIV_ORDER:
        if p in privs and p not in seen:
            seen.add(p)
            out.append(p)

    # незнакомые (на будущее) — в конец
    for p in sorted(set(privs) - seen):
        out.append(p)

    return out


def expand_effective_grants(direct_by_grantee, membership):
    """
    Права пользователя = свои + права всех ролей, в которых он состоит
    (рекурсивно). Чистая функция.

    direct_by_grantee: {grantee: [{schema, table, privileges, grantable}]}
    membership: {role: [входит в роли]}
    -> {user: {(schema, table): {"privileges": set, "grantable": set,
                                 "sources": [str]}}}
    """
    def roles_of(name, seen=None):
        seen = seen if seen is not None else set()
        out = []

        for parent in membership.get(name, []):
            if parent in seen:
                continue
            seen.add(parent)
            out.append(parent)
            out.extend(roles_of(parent, seen))

        return out

    effective = {}
    names = set(direct_by_grantee) | set(membership)

    for name in names:
        acc = {}

        chain = [(name, "direct")] + [(r, r) for r in roles_of(name)]

        for holder, source in chain:
            for row in direct_by_grantee.get(holder, []):
                key = (row["schema"], row["table"])
                entry = acc.setdefault(
                    key, {"privileges": set(), "grantable": set(),
                          "sources": []}
                )
                entry["privileges"].update(row.get("privileges") or [])
                entry["grantable"].update(row.get("grantable") or [])

                if source not in entry["sources"]:
                    entry["sources"].append(source)

        if acc:
            effective[name] = acc

    return effective


def classify_risk(is_superuser, privileges_by_table):
    """
    Уровень риска пользователя: superuser или TRUNCATE/DELETE — высокий,
    прочая запись — средний, только чтение — низкий. Чистая функция.
    """
    reasons = []
    write_tables = 0
    danger_tables = 0

    for privs in privileges_by_table:
        if any(p in privs for p in DANGEROUS_PRIVS):
            danger_tables += 1
        if any(p in privs for p in WRITE_PRIVS):
            write_tables += 1

    if is_superuser:
        reasons.append("superuser")

    if danger_tables:
        reasons.append("truncate/delete на {} табл.".format(danger_tables))
    elif write_tables:
        reasons.append("запись в {} табл.".format(write_tables))

    if is_superuser or danger_tables:
        level = "high"
    elif write_tables:
        level = "medium"
    else:
        level = "low"

    return {"level": level, "reasons": reasons,
            "write_tables": write_tables, "danger_tables": danger_tables}


def build_grant_sql(schema, table, privileges, grantee, with_grant_option=False):
    """GRANT-скрипт для таблицы. Чистая функция."""
    privs = sort_privileges(privileges)

    if not privs:
        raise ValueError("Список привилегий пуст")

    sql = "GRANT {}\n    ON {}.{}\n    TO {}".format(
        ", ".join(privs),
        quote_ident(schema), quote_ident(table), quote_ident(grantee),
    )

    if with_grant_option:
        sql += "\n    WITH GRANT OPTION"

    return sql + ";"


def build_revoke_sql(schema, table, privileges, grantee):
    """REVOKE-скрипт для таблицы. Чистая функция."""
    privs = sort_privileges(privileges) or ["ALL PRIVILEGES"]

    return "REVOKE {}\n    ON {}.{}\n    FROM {};".format(
        ", ".join(privs),
        quote_ident(schema), quote_ident(table), quote_ident(grantee),
    )


def build_graph(users, max_tables=MAX_GRAPH_TABLES):
    """
    Payload для графа связей: узлы (пользователи, таблицы, схемы) и связи
    пользователь -> таблица. Таблицы обрезаются по числу связей, чтобы
    граф оставался читаемым. Чистая функция.
    """
    table_weight = {}
    schema_weight = {}

    for user in users:
        for t in user.get("tables") or []:
            key = (t["schema"], t["table"])
            table_weight[key] = table_weight.get(key, 0) + 1
            schema_weight[t["schema"]] = schema_weight.get(t["schema"], 0) + 1

    top = sorted(table_weight.items(), key=lambda kv: -kv[1])[:max_tables]
    kept = set(k for k, _ in top)

    nodes = []
    links = []

    for schema in sorted(set(s for s, _ in kept)):
        nodes.append({
            "id": "schema:" + schema, "type": "schema", "label": schema,
            "weight": schema_weight.get(schema, 0),
        })

    for (schema, table), weight in top:
        nodes.append({
            "id": "table:{}.{}".format(schema, table), "type": "table",
            "label": table, "schema": schema, "weight": weight,
        })

    for user in users:
        nodes.append({
            "id": "user:" + user["name"], "type": "user",
            "label": user["name"], "weight": len(user.get("tables") or []),
            "superuser": bool(user.get("is_superuser")),
            "kind": user.get("kind"),
        })

        for t in user.get("tables") or []:
            key = (t["schema"], t["table"])

            if key not in kept:
                continue

            links.append({
                "source": "user:" + user["name"],
                "target": "table:{}.{}".format(t["schema"], t["table"]),
                "write": bool(any(p in (t.get("privileges") or [])
                                  for p in WRITE_PRIVS)),
                "schema": t["schema"],
            })

    return {
        "nodes": nodes,
        "links": links,
        "tables_total": len(table_weight),
        "tables_shown": len(kept),
    }


# ------------------------------------------------------------------
# сбор из каталогов
# ------------------------------------------------------------------

def _rows(cur, sql, params=None):
    if params:
        cur.execute(sql, params)
    else:
        cur.execute(sql)

    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def collect_raw(cur):
    """Роли, членство, гранты на таблицы и колонки, активность."""
    schemas_not_in = ", ".join("'%s'" % s for s in _SYSTEM_SCHEMAS)

    roles = _rows(cur, """
        SELECT rolname, rolsuper, rolcanlogin, rolcreaterole
        FROM pg_roles
        WHERE rolname NOT LIKE 'pg\\_%'
        ORDER BY rolname
    """)

    members = _rows(cur, """
        SELECT m.rolname AS member, r.rolname AS role_name
        FROM pg_auth_members am
        JOIN pg_roles m ON m.oid = am.member
        JOIN pg_roles r ON r.oid = am.roleid
    """)

    grants = _rows(cur, """
        SELECT n.nspname AS schema_name,
               c.relname AS table_name,
               pg_get_userbyid(c.relowner) AS owner,
               CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                    ELSE pg_get_userbyid(a.grantee) END AS grantee,
               a.privilege_type,
               a.is_grantable,
               c.reltuples::bigint AS rows_estimate,
               c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND c.relacl IS NOT NULL
          AND n.nspname NOT IN ({schemas})
          AND NOT EXISTS (
              SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid
          )
        LIMIT 20000
    """.format(schemas=schemas_not_in))

    try:
        col_grants = _rows(cur, """
            SELECT n.nspname AS schema_name,
                   c.relname AS table_name,
                   at.attname AS column_name,
                   CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                        ELSE pg_get_userbyid(a.grantee) END AS grantee,
                   a.privilege_type
            FROM pg_attribute at
            JOIN pg_class c ON c.oid = at.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL aclexplode(at.attacl) a
            WHERE at.attacl IS NOT NULL
              AND n.nspname NOT IN ({schemas})
            LIMIT 5000
        """.format(schemas=schemas_not_in))
    except Exception:
        cur.connection.rollback()
        col_grants = []

    try:
        activity = _rows(cur, """
            SELECT usename AS role_name,
                   max(backend_start)::text AS last_seen
            FROM pg_stat_activity
            WHERE usename IS NOT NULL
            GROUP BY usename
        """)
    except Exception:
        cur.connection.rollback()
        activity = []

    return {
        "roles": roles,
        "members": members,
        "grants": grants,
        "col_grants": col_grants,
        "activity": activity,
    }


def build_overview(raw):
    """Сырые каталожные строки -> структура для UI. Чистая функция."""
    role_info = {r["rolname"]: r for r in raw.get("roles") or []}

    membership = {}

    for m in raw.get("members") or []:
        membership.setdefault(m["member"], []).append(m["role_name"])

    # прямые гранты по грантополучателю
    direct = {}
    table_meta = {}

    for g in raw.get("grants") or []:
        key = (g["schema_name"], g["table_name"])
        table_meta.setdefault(key, {
            "owner": g.get("owner"),
            # reltuples -1 (или отрицательное) в GP/PG12+ = «неизвестно»
            "rows": max(0, int(g.get("rows_estimate") or 0)),
            "relkind": g.get("relkind"),
        })

        rows = direct.setdefault(g["grantee"], {})
        entry = rows.setdefault(key, {
            "schema": g["schema_name"], "table": g["table_name"],
            "privileges": set(), "grantable": set(),
        })
        entry["privileges"].add(g["privilege_type"])

        if g.get("is_grantable"):
            entry["grantable"].add(g["privilege_type"])

    direct_lists = {
        grantee: list(rows.values()) for grantee, rows in direct.items()
    }

    effective = expand_effective_grants(direct_lists, membership)

    # колоночные права
    col_by_table = {}

    for c in raw.get("col_grants") or []:
        key = (c["grantee"], c["schema_name"], c["table_name"])
        cols = col_by_table.setdefault(key, {})
        cols.setdefault(c["column_name"], set()).add(c["privilege_type"])

    last_seen = {a["role_name"]: a["last_seen"]
                 for a in raw.get("activity") or []}

    users = []

    for name in sorted(set(list(effective) + list(role_info))):
        info = role_info.get(name) or {}
        tables_map = effective.get(name) or {}

        if not tables_map and name not in role_info:
            continue

        tables = []

        for (schema, table), acc in tables_map.items():
            privileges = sort_privileges(acc["privileges"])
            sources = acc["sources"]
            meta = table_meta.get((schema, table)) or {}
            cols = col_by_table.get((name, schema, table)) or {}

            tables.append({
                "schema": schema,
                "table": table,
                "owner": meta.get("owner"),
                "rows": meta.get("rows") or 0,
                "privileges": privileges,
                "grantable": sort_privileges(acc["grantable"]),
                "via_role": None if "direct" in sources else sources[0],
                "sources": sources,
                "columns": [
                    {"name": col, "privileges": sort_privileges(privs)}
                    for col, privs in sorted(cols.items())
                ],
                "grant_sql": build_grant_sql(
                    schema, table, privileges, name,
                    with_grant_option=bool(acc["grantable"]),
                ) if privileges else "",
                "revoke_sql": build_revoke_sql(
                    schema, table, privileges, name),
            })

        tables.sort(key=lambda t: (-t["rows"], t["schema"], t["table"]))
        # реальное число объектов до обрезки — чтобы в UI не было
        # одинакового «400» у всех
        tables_total = len(tables)
        tables = tables[:MAX_TABLES_PER_USER]

        risk = classify_risk(
            bool(info.get("rolsuper")),
            [t["privileges"] for t in tables],
        )

        aggregate = set()

        for t in tables:
            aggregate.update(t["privileges"])

        users.append({
            "name": name,
            "is_superuser": bool(info.get("rolsuper")),
            "can_login": bool(info.get("rolcanlogin")),
            "kind": "user" if info.get("rolcanlogin") else "group",
            "member_of": sorted(membership.get(name, [])),
            "schemas": sorted(set(t["schema"] for t in tables)),
            "tables_count": tables_total,
            "tables_shown": len(tables),
            "privileges": sort_privileges(aggregate),
            "risk": risk,
            "last_seen": last_seen.get(name),
            "tables": tables,
        })

    # пользователи с правами — первыми, внутри по риску
    risk_rank = {"high": 0, "medium": 1, "low": 2}
    users.sort(key=lambda u: (
        0 if u["tables_count"] else 1,
        risk_rank.get(u["risk"]["level"], 3),
        -u["tables_count"],
        u["name"],
    ))

    all_tables = set()
    all_schemas = set()
    write_privileges = 0

    for g in raw.get("grants") or []:
        all_tables.add((g["schema_name"], g["table_name"]))
        all_schemas.add(g["schema_name"])

        if g["privilege_type"] in WRITE_PRIVS:
            write_privileges += 1

    summary = {
        "users": sum(1 for u in users if u["kind"] == "user"),
        "groups": sum(1 for u in users if u["kind"] == "group"),
        "superusers": sum(1 for u in users if u["is_superuser"]),
        "tables": len(all_tables),
        "schemas": len(all_schemas),
        "write_privileges": write_privileges,
        "review": sum(1 for u in users if u["risk"]["level"] == "high"),
    }

    return {
        "summary": summary,
        "users": users,
        "graph": build_graph([u for u in users if u["tables_count"]]),
    }


def collect_grants(connection_id, force=False):
    """Срез прав по подключению (кэш 90 с)."""
    key = int(connection_id)
    now = time.time()
    cached = _CACHE.get(key)

    if cached and not force and now - cached["ts"] < _CACHE_TTL:
        return cached["data"]

    conn_cfg = get_connection_by_id(key)

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    started = now
    conn = open_psycopg2_connection_by_cfg(conn_cfg)

    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '25s'")
        raw = collect_raw(cur)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    data = build_overview(raw)
    data["ok"] = True
    data["connection_id"] = key
    data["connection_name"] = conn_cfg.get("name")
    data["generated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    data["duration_seconds"] = round(time.time() - started, 1)

    _CACHE[key] = {"ts": time.time(), "data": data}

    return data
