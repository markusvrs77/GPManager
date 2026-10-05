"""
Карта «хост сегмента -> основной IP» для gpcopy в кластер-приёмник.

Сегменты источника подключаются к сегментам приёмника по адресам из его
gp_segment_configuration. Если это адреса внутренней сети, копирование
падает по таймауту; карта уходит в gpcopy через --dest-mapping-file.
"""

import pytest

import app as app_module
from modules import gpcopy_ip_map as ipmap
from modules.connections import create_connection
from modules.gpcopy import build_gpcopy_command


def _gp(name):
    return create_connection({"name": name, "host": "mdw", "port": 5432,
                              "database_name": "adb", "username": "gpadmin"})


# ------------------------------------------------------------ проверка записей

def test_entries_without_ip_are_skipped():
    """Хост без IP в редакторе — ещё не заполнен, а не ошибка."""
    got = ipmap.normalize_entries([
        {"host": "sdw1", "addresses": ["sdw1-ic"], "ip": "10.0.0.11"},
        {"host": "sdw2", "ip": ""},
    ])

    assert got == [{"host": "sdw1", "addresses": ["sdw1-ic"],
                    "ip": "10.0.0.11"}]


@pytest.mark.parametrize("entry", [
    {"host": "sdw1", "ip": "10.0.0.300"},
    {"host": "sdw1", "ip": "10.0.0.1,10.0.0.2"},
    {"host": "sdw1,evil", "ip": "10.0.0.1"},
    {"host": "sdw1\n10.0.0.9", "ip": "10.0.0.1"},
    {"host": "", "ip": "10.0.0.1"},
])
def test_bad_entry_is_rejected(entry):
    """Битая строка сломала бы файл карты — её не сохраняем молча."""
    with pytest.raises(ValueError):
        ipmap.normalize_entries([entry])


def test_duplicate_host_is_rejected():
    with pytest.raises(ValueError):
        ipmap.normalize_entries([{"host": "sdw1", "ip": "10.0.0.1"},
                                 {"host": "sdw1", "ip": "10.0.0.2"}])


def test_file_lists_hostname_and_every_address():
    """
    gpcopy может искать сегмент и по hostname, и по address — карта
    покрывает оба, каждый ключ ровно один раз.
    """
    lines = ipmap.mapping_lines([
        {"host": "sdw1", "addresses": ["192.168.50.11", "sdw1-ic"],
         "ip": "10.0.0.11"},
        {"host": "sdw2", "ip": "10.0.0.12"},
    ])

    assert lines == [
        "sdw1,10.0.0.11",
        "192.168.50.11,10.0.0.11",
        "sdw1-ic,10.0.0.11",
        "sdw2,10.0.0.12",
    ]


# ------------------------------------------------------------ хранение и файл

def test_saved_map_becomes_mapping_file():
    cid = _gp("prod-ipmap")
    ipmap.save_ip_map(cid, [{"host": "sdw1", "ip": "10.0.0.11"},
                            {"host": "sdw2", "ip": "10.0.0.12"}])

    path = ipmap.write_dest_mapping_file({"id": cid})

    with open(path, encoding="utf-8") as fh:
        assert fh.read() == "sdw1,10.0.0.11\nsdw2,10.0.0.12\n"


def test_no_map_means_no_file():
    """Кластер без карты — команда gpcopy остаётся прежней."""
    cid = _gp("test-no-map")

    assert ipmap.write_dest_mapping_file({"id": cid}) is None
    assert ipmap.write_dest_mapping_file({"host": "x"}) is None


def test_empty_list_clears_map():
    cid = _gp("prod-clear")
    ipmap.save_ip_map(cid, [{"host": "sdw1", "ip": "10.0.0.11"}])
    ipmap.save_ip_map(cid, [])

    assert ipmap.get_ip_map(cid) == []


def test_unknown_connection_is_reported():
    with pytest.raises(LookupError):
        ipmap.save_ip_map(987654, [{"host": "sdw1", "ip": "10.0.0.1"}])


# ------------------------------------------------------------ команда gpcopy

def _cmd(**kw):
    return build_gpcopy_command(gpcopy_path="gpcopy", source_host="src",
                                dest_host="dst", include_tables="db.s.t", **kw)


def test_command_carries_mapping_file():
    cmd = _cmd(dest_mapping_file="/tmp/map.txt")

    i = cmd.index("--dest-mapping-file")
    assert cmd[i + 1] == "/tmp/map.txt"


def test_command_without_map_is_unchanged():
    assert "--dest-mapping-file" not in _cmd()


def test_manual_flag_in_extra_args_wins():
    """Флаг, заданный руками в доп. аргументах, второй раз не добавляется."""
    cmd = _cmd(dest_mapping_file="/tmp/auto.txt",
               extra_args="--dest-mapping-file /tmp/manual.txt")

    assert cmd.count("--dest-mapping-file") == 1
    assert "/tmp/manual.txt" in cmd and "/tmp/auto.txt" not in cmd


# ------------------------------------------------------------ API

def test_api_saves_and_returns_map(client):
    cid = _gp("prod-api")

    r = client.post("/api/connections/{}/segment-ip-map".format(cid), json={
        "entries": [{"host": "sdw1", "addresses": ["sdw1-ic"],
                     "ip": "10.0.0.11"}],
    })
    assert r.status_code == 200 and r.get_json()["ok"] is True

    got = client.get("/api/connections/{}/segment-ip-map".format(cid))
    assert got.get_json()["entries"] == [
        {"host": "sdw1", "addresses": ["sdw1-ic"], "ip": "10.0.0.11"}]


def test_api_rejects_bad_ip(client):
    cid = _gp("prod-api-bad")

    r = client.post("/api/connections/{}/segment-ip-map".format(cid), json={
        "entries": [{"host": "sdw1", "ip": "not-an-ip"}],
    })

    assert r.status_code == 400
    assert ipmap.get_ip_map(cid) == []


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql = sql

    def fetchall(self):
        return self.rows


class _FakeConn:
    def __init__(self, rows):
        self.cur = _FakeCursor(rows)
        self.readonly = None
        self.closed = False

    def set_session(self, readonly=None, autocommit=None):
        self.readonly = readonly

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True


def test_api_lists_segment_hosts_read_only(client, monkeypatch):
    """Хосты группируются по имени; соединение только на чтение и закрывается."""
    cid = _gp("prod-hosts")
    fake = _FakeConn([
        ("sdw1", "sdw1-ic1", 2, 0),
        ("sdw1", "sdw1-ic2", 0, 2),
        ("sdw2", "sdw2", 2, 2),
    ])
    monkeypatch.setattr(app_module, "open_gp_connection", lambda _id: fake)

    body = client.get(
        "/api/connections/{}/segment-hosts".format(cid)).get_json()

    assert body["hosts"] == [
        {"host": "sdw1", "addresses": ["sdw1-ic1", "sdw1-ic2"],
         "primaries": 2, "mirrors": 2},
        {"host": "sdw2", "addresses": [], "primaries": 2, "mirrors": 2},
    ]
    assert fake.readonly is True and fake.closed is True
    assert "gp_segment_configuration" in fake.cur.sql
