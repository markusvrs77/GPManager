import pytest

import config
from app import app as flask_app
from db import init_db


@pytest.fixture(scope="session", autouse=True)
def _isolated_db(tmp_path_factory):
    """Изолирует тесты от реальной dev-БД.

    Все обращения к БД идут через config.SQLITE_DB_PATH (db.get_sqlite_connection
    и делегирующие к ней хелперы), поэтому достаточно подменить один атрибут на
    временный файл и один раз инициализировать схему на нём. Реальная
    instance/gp_reorganize_center.sqlite3 при этом не открывается и не меняется.
    """
    db_path = tmp_path_factory.mktemp("gpm_db") / "test.sqlite3"

    mp = pytest.MonkeyPatch()
    mp.setattr(config, "SQLITE_DB_PATH", str(db_path))

    # init_db() runs only under __main__ in app.py, so tests must init explicitly.
    init_db()

    yield

    mp.undo()


@pytest.fixture
def client():
    flask_app.config.update(TESTING=True)
    with flask_app.test_client() as c:
        yield c
