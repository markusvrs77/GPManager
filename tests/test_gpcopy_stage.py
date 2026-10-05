"""
Промежуточные таблицы gpcopy для секционированной таблицы в другую цель.

gpcopy 2.7 не пишет несколько таблиц источника в одну таблицу приёмника,
поэтому партиции идут в свои промежуточные таблицы и переливаются в
цель одной транзакцией.
"""

from modules import gpcopy_stage as st


class _Cur:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.log.append(sql)
        if self.conn.fail_on and self.conn.fail_on in sql:
            raise RuntimeError("no partition of relation found for row")

    def fetchall(self):
        return [("id",), ("d",)]


class _Conn:
    def __init__(self, fail_on=None):
        self.log = []
        self.fail_on = fail_on
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return _Cur(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


ROOTS = {"adb.arch.a_copy": {"item": ["s", "a"], "target": ["arch", "a_copy"],
                             "truncate": True}}


def _full(schema, table):
    return "adb.{}.{}".format(schema, table)


def test_each_leaf_gets_its_own_stage():
    entries = [
        {"source": "adb.s.a_prt_1", "dest": "adb.arch.a_copy", "sql": "q1"},
        {"source": "adb.s.a_prt_2", "dest": "adb.arch.a_copy", "sql": "q2"},
        {"source": "adb.s.c", "dest": "adb.s.c"},
    ]

    out, merges = st.plan_stages(entries, ROOTS, 42, _full)

    assert [e["dest"] for e in out] == [
        "adb.opsentri_gpcopy_stage.j42_00001",
        "adb.opsentri_gpcopy_stage.j42_00002",
        "adb.s.c",
    ]
    assert out[0]["sql"] == "q1"
    assert merges == [{"item": ["s", "a"], "target": ["arch", "a_copy"],
                       "truncate": True,
                       "stages": [["opsentri_gpcopy_stage", "j42_00001"],
                                  ["opsentri_gpcopy_stage", "j42_00002"]]}]


def test_merge_is_one_transaction_truncate_then_inserts():
    conn = _Conn()
    merges = [{"item": ["s", "a"], "target": ["arch", "a_copy"],
               "truncate": True,
               "stages": [["stg", "j1_00001"], ["stg", "j1_00002"]]}]

    assert st.apply_merges(conn, merges) == {}

    dml = [s for s in conn.log if not s.lstrip().startswith("SELECT")]
    assert dml == [
        'TRUNCATE TABLE "arch"."a_copy"',
        'INSERT INTO "arch"."a_copy" ("id", "d") SELECT "id", "d" '
        'FROM "stg"."j1_00001"',
        'INSERT INTO "arch"."a_copy" ("id", "d") SELECT "id", "d" '
        'FROM "stg"."j1_00002"',
    ]
    assert conn.commits == 1 and conn.rollbacks == 0


def test_append_merge_does_not_truncate():
    conn = _Conn()
    st.apply_merges(conn, [{"item": ["s", "a"], "target": ["arch", "a_copy"],
                            "truncate": False, "stages": [["stg", "j1_1"]]}])

    assert not any("TRUNCATE" in s for s in conn.log)


def test_failed_merge_rolls_back_and_reports():
    """Строка без партиции в цели — откат: цель не изменена."""
    conn = _Conn(fail_on="j1_00002")
    merges = [{"item": ["s", "a"], "target": ["arch", "a_copy"],
               "truncate": True,
               "stages": [["stg", "j1_00001"], ["stg", "j1_00002"]]}]

    errors = st.apply_merges(conn, merges)

    assert "no partition" in errors[("s", "a")]
    assert "цель не изменена" in errors[("s", "a")]
    assert conn.rollbacks == 1 and conn.commits == 0


def test_drop_stages_drops_only_stage_tables():
    conn = _Conn()
    st.drop_stages(conn, [{"stages": [["stg", "j1_00001"],
                                      ["stg", "j1_00002"]]}])

    assert conn.log == ['DROP TABLE IF EXISTS "stg"."j1_00001"',
                        'DROP TABLE IF EXISTS "stg"."j1_00002"']
