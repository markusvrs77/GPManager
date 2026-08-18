# -*- coding: utf-8 -*-
"""
Пользователи и гранты: кто и что может делать с таблицами.

Только чтение системных каталогов (pg_roles, pg_auth_members, relacl,
attacl) — никакой нагрузки на данные. Права, полученные через роли,
разворачиваются до пользователя с пометкой источника. SQL для
GRANT/REVOKE только генерируется текстом — модуль ничего не меняет.
"""

import json
import time
import zlib

from db import sqlite_cursor

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

# срез живёт в SQLite: страница читает сохранённое и не трогает источник,
# пока пользователь не нажмёт «обновить срез»


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


def seriate_matrix(row_keys, col_keys, cells):
    """
    Сериация бинарной матрицы: столбцы по «популярности», строки по
    битовому коду присутствия. Похожие строки встают рядом, и блоки
    одинакового доступа видны глазом. Чистая функция.

    cells: {(row, col): вес}
    -> (row_order, col_order)
    """
    col_weight = {}

    for (row, col), weight in cells.items():
        col_weight[col] = col_weight.get(col, 0) + (weight or 1)

    col_order = sorted(col_keys,
                       key=lambda c: (-col_weight.get(c, 0), str(c)))
    index = {c: i for i, c in enumerate(col_order)}
    n = len(col_order)

    def row_code(row):
        bits = 0

        for col in col_order:
            if (row, col) in cells:
                bits |= 1 << (n - index[col])

        return bits

    row_order = sorted(row_keys, key=lambda r: (-row_code(r), str(r)))

    return row_order, col_order


def group_by_access_profile(agg):
    """
    Пользователи с одинаковым профилем доступа — это де-факто роль.
    Склеиваем их в группы, чтобы вместо 71 строки читать 10.
    Чистая функция.

    agg: {(user, schema): {"tables": n, "write_tables": m}}
    -> [{"users": [...], "size": n, "schemas": [...], "tables": n,
         "write_tables": m}]
    """
    by_user = {}

    for (user, schema), v in (agg or {}).items():
        by_user.setdefault(user, {})[schema] = (v["tables"], v["write_tables"])

    groups = {}

    for user, profile in by_user.items():
        signature = tuple(sorted(
            (schema, counts[0], counts[1])
            for schema, counts in profile.items()
        ))
        groups.setdefault(signature, []).append(user)

    out = []

    for signature, users in groups.items():
        out.append({
            "users": sorted(users),
            "size": len(users),
            "schemas": [s for s, _t, _w in signature],
            "tables": sum(t for _s, t, _w in signature),
            "write_tables": sum(w for _s, _t, w in signature),
        })

    out.sort(key=lambda g: (-g["size"], -g["tables"], g["users"][0]))

    return out


def build_sankey(user_src, src_schema, top_users=None, top_schemas=None,
                 user_src_write=None, src_schema_write=None,
                 superusers=None):
    """
    Поток прав: пользователь -> роль (или «напрямую») -> схема.
    Толщина = число таблиц, отдельно считается доля прав записи.
    По умолчанию в диаграмму попадают ВСЕ пользователи и схемы; если
    передать top_users / top_schemas, мелкие узлы свернутся в «прочие».
    Чистая функция.

    user_src:         {(user, source): tables}
    src_schema:       {(source, schema): tables}
    user_src_write:   то же, но только таблицы с правом записи
    src_schema_write: то же, но только таблицы с правом записи
    superusers:       множество имён superuser-ролей (пометка узлов)
    """
    user_src_write = user_src_write or {}
    src_schema_write = src_schema_write or {}
    supers = set(superusers or ())

    def top_keys(totals, limit):
        ranked = sorted(totals.items(), key=lambda kv: (-kv[1], kv[0]))
        return set(k for k, _v in ranked[:limit])

    user_totals = {}
    schema_totals = {}

    for (user, _src), n in user_src.items():
        user_totals[user] = user_totals.get(user, 0) + n

    for (_src, schema), n in src_schema.items():
        schema_totals[schema] = schema_totals.get(schema, 0) + n

    keep_users = top_keys(user_totals, top_users) if top_users else None
    keep_schemas = (top_keys(schema_totals, top_schemas)
                    if top_schemas else None)

    OTHER_U = "прочие пользователи"
    OTHER_S = "прочие схемы"

    left = {}
    left_w = {}
    right = {}
    right_w = {}

    for (user, src), n in user_src.items():
        name = user if keep_users is None or user in keep_users else OTHER_U
        key = (name, src)
        left[key] = left.get(key, 0) + n
        left_w[key] = left_w.get(key, 0) + user_src_write.get((user, src), 0)

    for (src, schema), n in src_schema.items():
        sch = (schema if keep_schemas is None or schema in keep_schemas
               else OTHER_S)
        key = (src, sch)
        right[key] = right.get(key, 0) + n
        right_w[key] = right_w.get(key, 0) + src_schema_write.get(
            (src, schema), 0)

    sources = sorted(set([s for _u, s in left] + [s for s, _c in right]))

    def total(pairs, idx, key):
        return sum(n for k, n in pairs.items() if k[idx] == key)

    nodes = (
        [{"id": "u:" + u, "label": u, "column": 0,
          "value": total(left, 0, u), "write": total(left_w, 0, u),
          "superuser": u in supers,
          "sources": sorted(set(s for uu, s in left if uu == u))}
         for u in sorted(set(u for u, _s in left))] +
        [{"id": "r:" + s, "label": s, "column": 1,
          "value": total(right, 0, s), "write": total(right_w, 0, s),
          "users": len(set(u for u, ss in left if ss == s)),
          "direct": s == "напрямую"}
         for s in sources] +
        [{"id": "s:" + c, "label": c, "column": 2,
          "value": total(right, 1, c), "write": total(right_w, 1, c)}
         for c in sorted(set(c for _s, c in right))]
    )

    links = (
        [{"source": "u:" + u, "target": "r:" + s, "value": n,
          "write": left_w.get((u, s), 0)}
         for (u, s), n in sorted(left.items(), key=lambda kv: -kv[1])] +
        [{"source": "r:" + s, "target": "s:" + c, "value": n,
          "write": right_w.get((s, c), 0)}
         for (s, c), n in sorted(right.items(), key=lambda kv: -kv[1])]
    )

    return {
        "nodes": nodes,
        "links": links,
        "users_total": len(user_totals),
        "schemas_total": len(schema_totals),
        "tables_total": sum(user_totals.values()),
    }


def build_graph(users, max_tables=MAX_GRAPH_TABLES, agg=None,
                schema_tables=None):
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

    # агрегация «пользователь -> схема»: основной режим графа, где вместо
    # тысяч связей — десятки понятных. Если агрегацию посчитали снаружи
    # (по полному списку таблиц, до обрезки) — берём её.
    if agg is None or schema_tables is None:
        agg = {}
        schema_tables = {}

        for user in users:
            for t in user.get("tables") or []:
                k = (user["name"], t["schema"])
                e = agg.setdefault(k, {"tables": 0, "write_tables": 0})
                e["tables"] += 1

                if any(p in WRITE_PRIVS for p in (t.get("privileges") or [])):
                    e["write_tables"] += 1

                schema_tables.setdefault(t["schema"], set()).add(t["table"])

    user_schema_links = [
        {
            "source": "user:" + name, "target": "schema:" + schema,
            "tables": v["tables"], "write_tables": v["write_tables"],
            "write": v["write_tables"] > 0,
        }
        for (name, schema), v in sorted(agg.items())
    ]

    schema_nodes = [
        {
            "id": "schema:" + schema, "type": "schema", "label": schema,
            "tables": len(tabs),
        }
        for schema, tabs in sorted(schema_tables.items())
    ]

    return {
        "nodes": nodes,
        "links": links,
        "schema_nodes": schema_nodes,
        "user_schema_links": user_schema_links,
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
    # агрегация для графа считается по полному составу прав (до обрезки
    # MAX_TABLES_PER_USER), иначе часть схем пропадала бы из графа
    agg_full = {}
    schema_tables_full = {}
    # потоки для sankey: пользователь -> источник (роль/напрямую) -> схема
    flow_user_src = {}
    flow_src_schema = {}
    # те же потоки, но только права записи — чтобы в диаграмме была видна
    # доля «опасного» трафика, а не только объём
    flow_user_src_w = {}
    flow_src_schema_w = {}

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

        for t in tables:
            key = (name, t["schema"])
            e = agg_full.setdefault(key, {"tables": 0, "write_tables": 0})
            e["tables"] += 1
            is_write = any(p in WRITE_PRIVS for p in t["privileges"])

            if is_write:
                e["write_tables"] += 1

            schema_tables_full.setdefault(t["schema"], set()).add(t["table"])

            # поток «через что пришло право» — только для login-ролей,
            # иначе группы дублировали бы объёмы своих участников
            if info.get("rolcanlogin"):
                src = t["via_role"] or "напрямую"
                fk = (name, src)
                flow_user_src[fk] = flow_user_src.get(fk, 0) + 1
                sk = (src, t["schema"])
                flow_src_schema[sk] = flow_src_schema.get(sk, 0) + 1

                if is_write:
                    flow_user_src_w[fk] = flow_user_src_w.get(fk, 0) + 1
                    flow_src_schema_w[sk] = flow_src_schema_w.get(sk, 0) + 1

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

    # порядок строк/столбцов матрицы: похожие профили доступа рядом
    matrix_cells = {
        (name, schema): v["tables"] for (name, schema), v in agg_full.items()
    }
    row_order, col_order = seriate_matrix(
        sorted(set(n for n, _s in agg_full)),
        sorted(set(s for _n, s in agg_full)),
        matrix_cells,
    )

    return {
        "summary": summary,
        "users": users,
        "matrix_order": {"rows": row_order, "cols": col_order},
        "role_groups": group_by_access_profile(agg_full),
        # в поток идут ВСЕ пользователи (без «прочих») — иначе половина
        # связей прячется именно там, где её надо разбирать
        "sankey": build_sankey(
            flow_user_src, flow_src_schema,
            user_src_write=flow_user_src_w,
            src_schema_write=flow_src_schema_w,
            superusers=set(
                n for n, i in role_info.items() if i.get("rolsuper")
            ),
        ),
        "graph": build_graph(
            [u for u in users if u["tables_count"]],
            agg=agg_full, schema_tables=schema_tables_full,
        ),
    }


def load_schema_snapshot(connection_id, schema):
    """Сохранённая матрица схемы (None, если её ещё не читали)."""
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT generated_at, payload_json
            FROM grants_schema_snapshots
            WHERE connection_id = ? AND schema_name = ?
            """,
            (int(connection_id), str(schema)),
        )
        row = cur.fetchone()

    if not row:
        return None

    data = _unpack(row["payload_json"])

    if data is None:
        return None

    data["generated_at"] = row["generated_at"]
    data["from_snapshot"] = True

    return data


def save_schema_snapshot(connection_id, schema, data):
    payload = {k: v for k, v in data.items()
               if k not in ("generated_at", "from_snapshot")}

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO grants_schema_snapshots (
                connection_id, schema_name, generated_at, payload_json
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(connection_id, schema_name) DO UPDATE SET
                generated_at = excluded.generated_at,
                payload_json = excluded.payload_json
            """,
            (int(connection_id), str(schema),
             data.get("generated_at"), _pack(payload)),
        )


def collect_schema_matrix(connection_id, schema, max_tables=400, force=False):
    """
    Матрица «пользователи × таблицы» внутри одной схемы (drill из общей
    матрицы). Тоже живёт в SQLite: в источник идём только когда матрицы
    ещё нет или её просят обновить.
    """
    schema = str(schema or "").strip()

    if not schema:
        raise ValueError("Схема обязательна")

    if not force:
        stored = load_schema_snapshot(connection_id, schema)

        if stored is not None:
            return stored

    conn_cfg = get_connection_by_id(int(connection_id))

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    conn = open_psycopg2_connection_by_cfg(conn_cfg)

    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '20s'")

        members = _rows(cur, """
            SELECT m.rolname AS member, r.rolname AS role_name
            FROM pg_auth_members am
            JOIN pg_roles m ON m.oid = am.member
            JOIN pg_roles r ON r.oid = am.roleid
        """)

        grants = _rows(cur, """
            SELECT c.relname AS table_name,
                   CASE WHEN a.grantee = 0 THEN 'PUBLIC'
                        ELSE pg_get_userbyid(a.grantee) END AS grantee,
                   a.privilege_type
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL aclexplode(c.relacl) a
            WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND c.relacl IS NOT NULL
              AND n.nspname = %s
              AND NOT EXISTS (
                  SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid
              )
        """, (schema,))
    finally:
        try:
            conn.close()
        except Exception:
            pass

    membership = {}

    for m in members:
        membership.setdefault(m["member"], []).append(m["role_name"])

    direct = {}

    for g in grants:
        rows = direct.setdefault(g["grantee"], {})
        entry = rows.setdefault(("", g["table_name"]), {
            "schema": "", "table": g["table_name"],
            "privileges": set(), "grantable": set(),
        })
        entry["privileges"].add(g["privilege_type"])

    effective = expand_effective_grants(
        {k: list(v.values()) for k, v in direct.items()}, membership
    )

    cells = {}
    table_weight = {}

    for user, tables_map in effective.items():
        for (_s, table), acc in tables_map.items():
            privs = sort_privileges(acc["privileges"])
            cells[user + "|" + table] = {
                "privileges": privs,
                "write": any(p in WRITE_PRIVS for p in privs),
                "danger": any(p in DANGEROUS_PRIVS for p in privs),
                "via_role": None if "direct" in acc["sources"]
                else acc["sources"][0],
            }
            table_weight[table] = table_weight.get(table, 0) + 1

    tables = [t for t, _w in sorted(table_weight.items(),
                                    key=lambda kv: -kv[1])[:max_tables]]
    users = sorted(effective)

    cell_keys = {}

    for key in cells:
        user, table = key.split("|", 1)

        if table in table_weight:
            cell_keys[(user, table)] = 1

    row_order, col_order = seriate_matrix(users, tables, cell_keys)

    data = {
        "ok": True,
        "schema": schema,
        "rows": row_order,
        "cols": col_order,
        "cells": cells,
        "tables_total": len(table_weight),
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    save_schema_snapshot(connection_id, schema, data)
    data["from_snapshot"] = False

    return data


EMPTY_SUMMARY = {
    "users": 0, "groups": 0, "superusers": 0, "tables": 0, "schemas": 0,
    "write_privileges": 0, "review": 0,
}


def _pack(payload):
    """Срез — это мегабайты JSON, поэтому в БД кладём его сжатым."""
    return zlib.compress(
        json.dumps(payload, ensure_ascii=False).encode("utf-8"), 6)


def _unpack(blob):
    """Читаем и сжатый, и старый несжатый формат."""
    if blob is None:
        return None

    if isinstance(blob, (bytes, bytearray)):
        try:
            blob = zlib.decompress(bytes(blob)).decode("utf-8")
        except zlib.error:
            blob = bytes(blob).decode("utf-8", "replace")

    try:
        return json.loads(blob)
    except Exception:
        return None


def load_snapshot(connection_id):
    """Сохранённый срез прав из SQLite (None, если его ещё нет)."""
    with sqlite_cursor() as cur:
        cur.execute(
            """
            SELECT generated_at, duration_seconds, payload_json
            FROM grants_snapshots
            WHERE connection_id = ?
            """,
            (int(connection_id),),
        )
        row = cur.fetchone()

    if not row:
        return None

    data = _unpack(row["payload_json"])

    if data is None:
        return None

    data["generated_at"] = row["generated_at"]
    data["duration_seconds"] = row["duration_seconds"]
    data["from_snapshot"] = True

    return data


def save_snapshot(connection_id, data):
    """Кладём срез в SQLite — страница будет читать его, а не источник."""
    payload = {k: v for k, v in data.items()
               if k not in ("generated_at", "duration_seconds",
                            "from_snapshot")}

    with sqlite_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO grants_snapshots (
                connection_id, generated_at, duration_seconds, payload_json
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(connection_id) DO UPDATE SET
                generated_at = excluded.generated_at,
                duration_seconds = excluded.duration_seconds,
                payload_json = excluded.payload_json
            """,
            (int(connection_id), data.get("generated_at"),
             data.get("duration_seconds"), _pack(payload)),
        )
        # матрицы схем пересобираются под новый срез
        cur.execute(
            "DELETE FROM grants_schema_snapshots WHERE connection_id = ?",
            (int(connection_id),),
        )


def empty_overview(connection_id, conn_name=None):
    """Пустой ответ, пока срез ни разу не собирали."""
    return {
        "ok": True,
        "empty": True,
        "connection_id": int(connection_id),
        "connection_name": conn_name,
        "generated_at": None,
        "summary": dict(EMPTY_SUMMARY),
        "users": [],
        "matrix_order": {"rows": [], "cols": []},
        "role_groups": [],
        "sankey": {"nodes": [], "links": [], "users_total": 0,
                   "schemas_total": 0, "tables_total": 0},
        "graph": {"nodes": [], "links": [], "schema_nodes": [],
                  "user_schema_links": [], "tables_shown": 0,
                  "tables_total": 0},
    }


def collect_grants(connection_id, force=False):
    """
    Срез прав по подключению. Без force читаем сохранённый в SQLite —
    источник не трогаем вообще. С force идём в каталоги и перезаписываем
    срез (кнопка «обновить срез»).
    """
    key = int(connection_id)

    if not force:
        stored = load_snapshot(key)

        if stored is not None:
            return stored

        conn_cfg = get_connection_by_id(key)

        return empty_overview(
            key, conn_cfg.get("name") if conn_cfg else None)

    conn_cfg = get_connection_by_id(key)

    if not conn_cfg:
        raise ValueError("Подключение не найдено")

    started = time.time()
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

    save_snapshot(key, data)
    data["from_snapshot"] = False

    return data
