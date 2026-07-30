# -*- coding: utf-8 -*-
"""Пользователи и гранты: чистые функции — без БД."""

import pytest

from modules.grants import (
    build_graph,
    build_grant_sql,
    build_overview,
    build_revoke_sql,
    classify_risk,
    expand_effective_grants,
    sort_privileges,
)


def test_sort_privileges():
    assert sort_privileges({"UPDATE", "SELECT", "TRUNCATE"}) == [
        "SELECT", "UPDATE", "TRUNCATE"]
    # дубли и незнакомые не теряются
    assert sort_privileges(["SELECT", "SELECT", "MAINTAIN"]) == [
        "SELECT", "MAINTAIN"]


def test_expand_effective_grants_through_roles():
    direct = {
        "analyst_ro": [{"schema": "dwh", "table": "orders",
                        "privileges": {"SELECT"}, "grantable": set()}],
        "dwh_writer": [{"schema": "dwh", "table": "orders",
                        "privileges": {"INSERT", "UPDATE"},
                        "grantable": set()}],
        "s.ivanov": [{"schema": "stg", "table": "raw",
                      "privileges": {"SELECT"}, "grantable": set()}],
    }
    # s.ivanov -> dwh_writer -> analyst_ro (вложенная роль)
    membership = {"s.ivanov": ["dwh_writer"], "dwh_writer": ["analyst_ro"]}

    eff = expand_effective_grants(direct, membership)
    ivanov = eff["s.ivanov"]

    assert ivanov[("stg", "raw")]["sources"] == ["direct"]
    orders = ivanov[("dwh", "orders")]
    assert orders["privileges"] == {"SELECT", "INSERT", "UPDATE"}
    # право пришло только через роли, прямого нет
    assert "direct" not in orders["sources"]
    assert set(orders["sources"]) == {"dwh_writer", "analyst_ro"}


def test_expand_handles_role_cycles():
    membership = {"a": ["b"], "b": ["a"]}
    direct = {"b": [{"schema": "s", "table": "t",
                     "privileges": {"SELECT"}, "grantable": set()}]}

    eff = expand_effective_grants(direct, membership)

    assert eff["a"][("s", "t")]["privileges"] == {"SELECT"}


def test_classify_risk_levels():
    assert classify_risk(True, [["SELECT"]])["level"] == "high"
    assert classify_risk(False, [["SELECT", "TRUNCATE"]])["level"] == "high"
    assert classify_risk(False, [["SELECT", "INSERT"]])["level"] == "medium"
    assert classify_risk(False, [["SELECT"], ["SELECT"]])["level"] == "low"

    risk = classify_risk(False, [["DELETE"], ["INSERT"], ["SELECT"]])
    assert risk["danger_tables"] == 1
    assert risk["write_tables"] == 2


def test_grant_and_revoke_sql():
    sql = build_grant_sql("dwh_bi", "pnm_rop", ["INSERT", "SELECT"],
                          "gp_admin", with_grant_option=True)

    assert 'GRANT SELECT, INSERT' in sql
    assert 'ON "dwh_bi"."pnm_rop"' in sql
    assert 'TO "gp_admin"' in sql
    assert sql.rstrip().endswith("WITH GRANT OPTION;")

    rev = build_revoke_sql("dwh_bi", "pnm_rop", ["SELECT"], "s.ivanov")
    assert rev == ('REVOKE SELECT\n    ON "dwh_bi"."pnm_rop"\n'
                   '    FROM "s.ivanov";')

    with pytest.raises(ValueError):
        build_grant_sql("s", "t", [], "u")


def test_build_graph_nodes_and_links():
    users = [
        {"name": "gp_admin", "is_superuser": True, "kind": "user", "tables": [
            {"schema": "dwh", "table": "orders", "privileges": ["SELECT",
                                                                "INSERT"]},
            {"schema": "stg", "table": "raw", "privileges": ["SELECT"]},
        ]},
        {"name": "s.ivanov", "is_superuser": False, "kind": "user", "tables": [
            {"schema": "dwh", "table": "orders", "privileges": ["SELECT"]},
        ]},
    ]

    graph = build_graph(users)
    types = {}

    for n in graph["nodes"]:
        types.setdefault(n["type"], []).append(n["label"])

    assert set(types["schema"]) == {"dwh", "stg"}
    assert set(types["user"]) == {"gp_admin", "s.ivanov"}
    assert set(types["table"]) == {"orders", "raw"}
    assert len(graph["links"]) == 3
    # запись помечена только там, где есть INSERT/UPDATE/DELETE
    writes = [l for l in graph["links"] if l["write"]]
    assert len(writes) == 1
    assert writes[0]["source"] == "user:gp_admin"

    # обрезка по числу таблиц
    small = build_graph(users, max_tables=1)
    assert small["tables_shown"] == 1
    assert small["tables_total"] == 2


def test_build_overview_end_to_end():
    raw = {
        "roles": [
            {"rolname": "gp_admin", "rolsuper": True, "rolcanlogin": True,
             "rolcreaterole": True},
            {"rolname": "analyst_ro", "rolsuper": False, "rolcanlogin": False,
             "rolcreaterole": False},
            {"rolname": "s.ivanov", "rolsuper": False, "rolcanlogin": True,
             "rolcreaterole": False},
        ],
        "members": [{"member": "s.ivanov", "role_name": "analyst_ro"}],
        "grants": [
            {"schema_name": "dwh", "table_name": "orders", "owner": "etl",
             "grantee": "gp_admin", "privilege_type": "SELECT",
             "is_grantable": True, "rows_estimate": 100, "relkind": "r"},
            {"schema_name": "dwh", "table_name": "orders", "owner": "etl",
             "grantee": "gp_admin", "privilege_type": "TRUNCATE",
             "is_grantable": False, "rows_estimate": 100, "relkind": "r"},
            {"schema_name": "dwh", "table_name": "orders", "owner": "etl",
             "grantee": "analyst_ro", "privilege_type": "SELECT",
             "is_grantable": False, "rows_estimate": 100, "relkind": "r"},
        ],
        "col_grants": [
            {"schema_name": "dwh", "table_name": "orders",
             "column_name": "amount", "grantee": "analyst_ro",
             "privilege_type": "SELECT"},
        ],
        "activity": [{"role_name": "gp_admin",
                      "last_seen": "2026-07-30 12:53:00"}],
    }

    data = build_overview(raw)
    by_name = {u["name"]: u for u in data["users"]}

    # superuser с truncate — высокий риск, первым в списке
    assert data["users"][0]["name"] == "gp_admin"
    assert by_name["gp_admin"]["risk"]["level"] == "high"
    assert by_name["gp_admin"]["last_seen"] == "2026-07-30 12:53:00"

    # s.ivanov получил SELECT через роль analyst_ro
    ivanov = by_name["s.ivanov"]
    assert ivanov["tables_count"] == 1
    assert ivanov["tables"][0]["via_role"] == "analyst_ro"
    assert ivanov["risk"]["level"] == "low"
    assert ivanov["kind"] == "user"

    # группа без login помечена как группа
    assert by_name["analyst_ro"]["kind"] == "group"
    # колоночные права попали в свою роль
    assert by_name["analyst_ro"]["tables"][0]["columns"] == [
        {"name": "amount", "privileges": ["SELECT"]}]

    assert data["summary"]["tables"] == 1
    assert data["summary"]["schemas"] == 1
    assert data["summary"]["superusers"] == 1
    assert data["summary"]["review"] == 1
    assert data["graph"]["links"]
