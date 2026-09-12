"""
Окно с очисткой: DELETE диапазона в приёмнике перед загрузкой того же
диапазона.

Это первый шаг стратегий, который трогает данные, поэтому проверяется не
только текст SQL, но и поведение вокруг него: границы уходят параметрами,
все таблицы чистятся одной транзакцией, ошибка откатывает всё целиком.
Соединение с кластером подменено заглушкой — тесты не ходят в базу.
"""

import pytest

import modules.gpcopy as g


CFG = {"schema": "dwh_bi", "table": "fact_ops",
       "source": "dwh_bi.fact_ops", "dest": "dwh_bi.fact_ops",
       "date_column": "created_at"}

FROM = "2026-09-01"
TO = "2026-09-02"


# ---------------------------------------------------------------- SQL

def test_delete_keeps_bounds_as_parameters():
    """Границы не склеиваются в текст — иначе это дыра прямо в DELETE."""
    sql = g.build_window_delete_sql("dwh_bi", "fact_ops", "created_at")

    assert sql == (
        'DELETE FROM "dwh_bi"."fact_ops" '
        'WHERE "created_at" >= %s AND "created_at" < %s'
    )
    assert "2026" not in sql


def test_window_is_half_open():
    """
    Полуинтервал [from, to) — тот же, что у среза источника
    build_date_slice_sql. Разойдись они, строка на границе либо потерялась
    бы при очистке, либо задвоилась при вставке.
    """
    where = g.build_window_where("created_at")
    slice_sql = g.build_date_slice_sql("dwh_bi", "fact_ops", "created_at",
                                       FROM, TO)

    assert ">= %s" in where and "< %s" in where
    assert ">= '2026-09-01'" in slice_sql and "< '2026-09-02'" in slice_sql


def test_identifiers_are_quoted():
    """Колонка с $ — обычное дело в DWH, имя должно пережить экранирование."""
    sql = g.build_window_delete_sql("dwh stage", 'odd"name', "date_change$")

    assert '"dwh stage"' in sql
    assert '"odd""name"' in sql
    assert '"date_change$"' in sql


def test_bad_date_column_is_rejected():
    with pytest.raises(ValueError):
        g.build_window_delete_sql("s", "t", "created_at; DROP TABLE users--")


@pytest.mark.parametrize("value", [
    "2026-09-01", "2026-09-01 03:00", "2026-09-01 03:00:00",
    "2026-09-01T03:00:00",
])
def test_valid_window_bounds(value):
    assert g.validate_window_bound(value) == value


@pytest.mark.parametrize("value", [
    None, "", "01.09.2026", "2026-9-1", "yesterday",
    "2026-09-01' OR '1'='1",
])
def test_invalid_window_bounds(value):
    with pytest.raises(ValueError):
        g.validate_window_bound(value)


def test_slice_sql_rejects_injected_bound():
    """
    Раньше границы попадали в срез склейкой без проверки формата, а
    приходят они из тела запроса.
    """
    with pytest.raises(ValueError):
        g.build_date_slice_sql("s", "t", "d", "2026-09-01' OR '1'='1", TO)


# --------------------------------------------------------- цель очистки

def test_target_is_destination_not_source():
    """Чистим приёмник: перенос может идти в другую схему."""
    targets = g.window_targets([{
        "source": "src_schema.fact_ops", "dest": "dst_schema.fact_ops",
        "date_column": "created_at",
    }])

    assert targets == [("dst_schema", "fact_ops", "created_at")]


def test_missing_date_column_is_rejected():
    with pytest.raises(ValueError):
        g.window_targets([{"source": "s.t", "dest": "s.t"}])


# ------------------------------------------------------- выполнение

class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 7

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.conn.calls.append((sql, params))
        if self.conn.boom and self.conn.boom in sql:
            raise RuntimeError("нет такой таблицы")

    def fetchone(self):
        return [42]


class FakeConn:
    def __init__(self, boom=None):
        self.calls = []
        self.committed = 0
        self.rolled_back = 0
        self.closed = False
        self.autocommit = True
        self.boom = boom

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True


def _patch_conn(monkeypatch, conn):
    monkeypatch.setattr(g, "get_connection_by_id", lambda cid: {"id": cid})
    monkeypatch.setattr(g, "open_psycopg2_connection_by_cfg", lambda cfg: conn)


def test_all_tables_cleared_in_one_transaction(monkeypatch):
    """
    Один commit на всю выборку. Иначе падение на пятой таблице из десяти
    оставило бы четыре с дырой в данных и шесть нетронутыми.
    """
    conn = FakeConn()
    _patch_conn(monkeypatch, conn)

    tables = [
        dict(CFG, dest="dwh_bi.fact_ops"),
        dict(CFG, dest="dwh_bi.fact_pay"),
        dict(CFG, dest="dwh_bi.fact_log"),
    ]
    report = g.clear_window_in_dest(2, tables, FROM, TO)

    assert conn.autocommit is False
    assert conn.committed == 1
    assert conn.rolled_back == 0
    assert len(conn.calls) == 3
    assert all(params == (FROM, TO) for _sql, params in conn.calls)
    assert report == [("dwh_bi", "fact_ops", 7), ("dwh_bi", "fact_pay", 7),
                      ("dwh_bi", "fact_log", 7)]
    assert conn.closed is True


def test_failure_rolls_everything_back(monkeypatch):
    conn = FakeConn(boom="fact_pay")
    _patch_conn(monkeypatch, conn)

    tables = [dict(CFG, dest="dwh_bi.fact_ops"),
              dict(CFG, dest="dwh_bi.fact_pay")]

    with pytest.raises(RuntimeError):
        g.clear_window_in_dest(2, tables, FROM, TO)

    assert conn.committed == 0
    assert conn.rolled_back == 1
    assert conn.closed is True


def test_dry_run_counts_and_rolls_back(monkeypatch):
    """Превью показывает, сколько строк снесёт, и ничего не удаляет."""
    conn = FakeConn()
    _patch_conn(monkeypatch, conn)

    report = g.clear_window_in_dest(2, [CFG], FROM, TO, dry_run=True)

    assert report == [("dwh_bi", "fact_ops", 42)]
    assert conn.committed == 0
    assert conn.rolled_back == 1
    assert "count(*)" in conn.calls[0][0]
    assert "DELETE" not in conn.calls[0][0]


def test_empty_window_is_rejected_before_any_sql(monkeypatch):
    conn = FakeConn()
    _patch_conn(monkeypatch, conn)

    with pytest.raises(ValueError):
        g.clear_window_in_dest(2, [CFG], TO, FROM)

    assert conn.calls == []


def test_bad_bound_never_reaches_the_database(monkeypatch):
    conn = FakeConn()
    _patch_conn(monkeypatch, conn)

    with pytest.raises(ValueError):
        g.clear_window_in_dest(2, [CFG], "2026-09-01'; DELETE FROM x--", TO)

    assert conn.calls == []
