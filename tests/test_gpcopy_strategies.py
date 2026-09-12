"""
/api/gpcopy/strategies — применимость стратегий переноса.

Маршрут отвечает на вопрос оператора «чем гарантируется отсутствие дублей
при повторном запуске», а не «какой флаг передать gpcopy». Ответ дают три
факта об исходной таблице: разрешается ли ключ, партиционирована ли она,
есть ли колонка даты. Здесь эти факты подставлены синтетически — проверяется
разбор фактов в матрицу стратегий, а не чтение каталогов.
"""

import app as app_module


TABLES = [
    # ключ есть, партиционирована, дата есть — годится всё
    ("dwh_bi", "fact_ops"),
    # ни ключа, ни партиций, но дата есть — окно и EXCEPT ALL
    ("stage", "raw_events"),
    # ничего: только полная замена
    ("mart", "agg_channel"),
]


def _patch_catalog(monkeypatch):
    tc = app_module.table_catalog

    monkeypatch.setattr(
        tc, "fetch_unique_indexes",
        lambda cid, tables: ({("dwh_bi", "fact_ops"): ["ops_id"]}, {}),
    )
    monkeypatch.setattr(
        tc, "resolve_keys_hierarchy",
        lambda tables, pk_map, unique_map: (
            {("dwh_bi", "fact_ops"): {"columns": ["ops_id"], "source": "pk"}},
            [("stage", "raw_events"), ("mart", "agg_channel")],
        ),
    )
    monkeypatch.setattr(tc, "load_sync_keys", lambda cid, tables: {})
    monkeypatch.setattr(tc, "fetch_partition_pairs", lambda cid: {})
    monkeypatch.setattr(
        tc, "classify_partition_roles",
        lambda tables, child_parent: {("dwh_bi", "fact_ops"): {"kind": "parent"}},
    )
    monkeypatch.setattr(
        tc, "fetch_date_columns_bulk",
        lambda cid, tables: {
            ("dwh_bi", "fact_ops"): ["created_at"],
            ("stage", "raw_events"): ["loaded_at"],
        },
    )


def _post(client):
    return client.post("/api/gpcopy/strategies", json={
        "connection_id": 1,
        "tables": [{"schema": s, "table": t} for s, t in TABLES],
    })


def test_strategies_matrix(client, monkeypatch):
    """Три факта о таблице превращаются в пять вердиктов с причиной отказа."""
    _patch_catalog(monkeypatch)

    body = _post(client).get_json()
    assert body["ok"] is True

    full = body["tables"]["dwh_bi.fact_ops"]
    assert full["key_columns"] == ["ops_id"]
    assert full["partitioned"] is True
    assert full["date_columns"] == ["created_at"]
    assert all(v["ok"] for v in full["strategies"].values())

    # без ключа и партиций окно всё равно работает: дату для нарезки видно
    keyless = body["tables"]["stage.raw_events"]
    assert keyless["strategies"]["window"]["ok"] is True
    assert keyless["strategies"]["except_all"]["ok"] is True
    assert keyless["strategies"]["key"]["ok"] is False
    assert keyless["strategies"]["key"]["reason"] == "ключ не разрешён"
    assert keyless["strategies"]["partitions"]["ok"] is False

    # голая таблица: гарантию даёт только полная замена
    bare = body["tables"]["mart.agg_channel"]
    assert bare["strategies"]["full"]["ok"] is True
    assert bare["strategies"]["window"]["ok"] is False
    assert bare["strategies"]["window"]["reason"] == "нет колонки даты"
    assert bare["strategies"]["except_all"]["ok"] is False


def test_dates_read_in_one_batch(client, monkeypatch):
    """
    Даты берутся одним запросом на всю выборку.

    Поштучный get_date_columns_for_table открывает отдельное соединение на
    каждую таблицу — на выборе схемы целиком это сотни коннектов подряд,
    поэтому маршрут обязан ходить в каталог батчем.
    """
    _patch_catalog(monkeypatch)

    calls = []

    def spy(connection_id, tables):
        calls.append(list(tables))
        return {}

    monkeypatch.setattr(app_module.table_catalog, "fetch_date_columns_bulk", spy)

    def boom(*a, **kw):
        raise AssertionError("поштучное чтение дат вернулось в маршрут")

    monkeypatch.setattr(app_module, "get_date_columns_for_table", boom)

    assert _post(client).get_json()["ok"] is True
    assert len(calls) == 1
    assert calls[0] == [(s, t) for s, t in TABLES]


def test_saved_key_unlocks_key_strategy(client, monkeypatch):
    """Ключ, назначенный оператором вручную, разблокирует стратегию по ключу."""
    _patch_catalog(monkeypatch)
    monkeypatch.setattr(
        app_module.table_catalog, "load_sync_keys",
        lambda cid, tables: {
            ("stage", "raw_events"): {"columns": ["event_id"], "source": "saved"},
        },
    )

    body = _post(client).get_json()
    keyless = body["tables"]["stage.raw_events"]

    assert keyless["key_columns"] == ["event_id"]
    assert keyless["key_source"] == "saved"
    assert keyless["strategies"]["key"]["ok"] is True


def test_empty_selection_is_rejected(client):
    """Считать применимость не по чему — это ошибка запроса, а не пустой ответ."""
    r = client.post("/api/gpcopy/strategies", json={
        "connection_id": 1, "tables": [],
    })

    assert r.status_code == 400
    assert r.get_json()["ok"] is False
