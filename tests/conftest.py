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
def admin_user():
    """Администратор для тестов.

    Функциональная область видимости намеренно: tests/test_security.py
    чистит таблицу users перед каждым своим тестом, и сессионный
    администратор пережил бы не всякий порядок запуска.
    """
    import modules.security as sec

    user = sec.get_user_by_name("pytest-admin")

    if user is None:
        user_id = sec.create_user("pytest-admin", "pytest-admin-pass", "admin",
                                  must_change_password=False)
        user = sec.get_user(user_id)

    return user


@pytest.fixture
def client(admin_user):
    """Вошедший администратор: остальным тестам права неинтересны."""
    import modules.security as sec
    from modules.web_auth import SESSION_COOKIE

    flask_app.config.update(TESTING=True)
    token = sec.create_session(admin_user["id"])

    with flask_app.test_client() as c:
        c.set_cookie(SESSION_COOKIE, token)
        yield c


@pytest.fixture
def anon_client(admin_user):
    """Тот, кто не вошёл.

    Администратор всё равно создаётся: без него приложение считает себя
    ненастроенным и уводит на первичную настройку, а не на форму входа.
    """
    flask_app.config.update(TESTING=True)

    with flask_app.test_client() as c:
        yield c


@pytest.fixture
def as_user():
    """Клиент под учёткой с заданной ролью и правами.

    as_user("operator") — оператор со стандартным набором;
    as_user("viewer", connection_ids=[2]) — наблюдатель с одним кластером.
    """
    import modules.security as sec
    from modules.web_auth import SESSION_COOKIE

    created = []

    def _make(role, overrides=None, connection_ids=None, username=None):
        name = username or "pytest-{}-{}".format(role, len(created))
        existing = sec.get_user_by_name(name)

        if existing:
            sec.delete_user(existing["id"])

        user_id = sec.create_user(name, "pytest-password", role,
                                  must_change_password=False)
        created.append(user_id)

        for capability, allowed in (overrides or {}).items():
            sec.set_override(user_id, capability, allowed)

        if connection_ids is not None:
            sec.set_user_connections(user_id, connection_ids)

        flask_app.config.update(TESTING=True)
        token = sec.create_session(user_id)

        c = flask_app.test_client()
        c.set_cookie(SESSION_COOKIE, token)
        return c

    yield _make

    for user_id in created:
        sec.delete_user(user_id)
