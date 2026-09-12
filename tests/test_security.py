"""
Пользователи, роли и права.

Проверяется не столько «право выдаётся», сколько «право отбирается»: в
приложении, которое умеет DELETE и TRUNCATE на PROD, дороже ошибиться в
сторону лишнего доступа.
"""

import pytest

import modules.security as sec


@pytest.fixture(autouse=True)
def _clean_users():
    """Каждый тест начинает с пустого списка пользователей."""
    from db import sqlite_cursor

    with sqlite_cursor(commit=True) as cur:
        for table in ("user_sessions", "user_permissions",
                      "user_connections", "users"):
            cur.execute("DELETE FROM {}".format(table))
    yield


# ------------------------------------------------------------ пароли

def test_password_roundtrip():
    stored = sec.hash_password("правильный-конь")

    assert sec.verify_password("правильный-конь", stored) is True
    assert sec.verify_password("неправильный", stored) is False


def test_password_never_stored_in_clear():
    stored = sec.hash_password("секрет")

    assert "секрет" not in stored
    assert stored.startswith("scrypt$")


def test_same_password_gives_different_hashes():
    """Своя соль у каждого: одинаковые пароли не должны быть видны."""
    assert sec.hash_password("одинаковый") != sec.hash_password("одинаковый")


@pytest.mark.parametrize("stored", ["", None, "мусор", "bcrypt$1$2$3$4$5"])
def test_broken_hash_never_verifies(stored):
    assert sec.verify_password("любой", stored) is False


# ------------------------------------------------------------ роли

def test_viewer_cannot_run_anything():
    uid = sec.create_user("viewer1", "пароль", "viewer")
    caps = sec.effective_capabilities(sec.get_user(uid))

    assert "sync.view" in caps
    assert "sync.run" not in caps
    assert "users.manage" not in caps


def test_operator_runs_but_does_not_edit_connections():
    """Доступы к кластерам — вотчина администратора."""
    uid = sec.create_user("operator1", "пароль", "operator")
    caps = sec.effective_capabilities(sec.get_user(uid))

    assert "sync.run" in caps
    assert "connections.edit" not in caps
    assert "grants.edit" not in caps
    assert "users.manage" not in caps


def test_admin_gets_everything():
    uid = sec.create_user("admin1", "пароль", "admin")

    assert sec.effective_capabilities(sec.get_user(uid)) == sec.CAPABILITY_CODES


def test_disabled_user_has_no_capabilities():
    uid = sec.create_user("admin2", "пароль", "admin")
    sec.set_user_active(uid, False)

    assert sec.effective_capabilities(sec.get_user(uid)) == frozenset()


# ------------------------------------------------------ точечные права

def test_override_adds_capability():
    uid = sec.create_user("ivanov", "пароль", "operator")
    sec.set_override(uid, "grants.edit", True)

    assert sec.has_capability(sec.get_user(uid), "grants.edit") is True


def test_override_removes_capability_given_by_role():
    """Оператору можно запретить запуск переноса, оставив остальное."""
    uid = sec.create_user("ivanov", "пароль", "operator")
    sec.set_override(uid, "sync.run", False)

    user = sec.get_user(uid)
    assert sec.has_capability(user, "sync.run") is False
    assert sec.has_capability(user, "vacuum.run") is True


def test_override_cleared_returns_to_role():
    uid = sec.create_user("ivanov", "пароль", "operator")
    sec.set_override(uid, "sync.run", False)
    sec.set_override(uid, "sync.run", None)

    assert sec.has_capability(sec.get_user(uid), "sync.run") is True


def test_admin_cannot_be_stripped_of_user_management():
    """
    Иначе можно отобрать право у последнего администратора и запереть
    систему без единого входа.
    """
    uid = sec.create_user("admin3", "пароль", "admin")
    sec.set_override(uid, "users.manage", False)

    assert sec.has_capability(sec.get_user(uid), "users.manage") is True


def test_unknown_capability_is_rejected():
    uid = sec.create_user("ivanov", "пароль", "operator")

    with pytest.raises(ValueError):
        sec.set_override(uid, "выдуманное.право", True)


# ------------------------------------------------------------ кластеры

def test_user_without_clusters_reaches_none():
    """Пусто означает пусто, а не «все»."""
    uid = sec.create_user("ivanov", "пароль", "operator")
    user = sec.get_user(uid)

    assert sec.allowed_connection_ids(user) == frozenset()
    assert sec.can_use_connection(user, 1) is False


def test_cluster_scope_is_enforced():
    uid = sec.create_user("ivanov", "пароль", "operator")
    sec.set_user_connections(uid, [2])          # только TEST
    user = sec.get_user(uid)

    assert sec.can_use_connection(user, 2) is True
    assert sec.can_use_connection(user, 1) is False     # PROD закрыт


def test_admin_reaches_every_cluster():
    uid = sec.create_user("admin4", "пароль", "admin")
    user = sec.get_user(uid)

    assert sec.allowed_connection_ids(user) is None
    assert sec.can_use_connection(user, 999) is True


# ------------------------------------------------------------ вход

def test_login_succeeds():
    sec.create_user("ivanov", "пароль", "operator")

    assert sec.authenticate("ivanov", "пароль")["username"] == "ivanov"


def test_username_is_case_insensitive():
    sec.create_user("Ivanov", "пароль", "operator")

    assert sec.authenticate("IVANOV", "пароль")["username"] == "ivanov"


def test_wrong_password_rejected():
    sec.create_user("ivanov", "пароль", "operator")

    with pytest.raises(sec.AuthError):
        sec.authenticate("ivanov", "не тот")


def test_unknown_user_gives_same_message_as_wrong_password():
    """Иначе форма входа становится справочником существующих учёток."""
    sec.create_user("ivanov", "пароль", "operator")

    with pytest.raises(sec.AuthError) as unknown:
        sec.authenticate("нет-такого", "пароль")

    with pytest.raises(sec.AuthError) as wrong:
        sec.authenticate("ivanov", "не тот")

    assert str(unknown.value) == str(wrong.value)


def test_disabled_user_cannot_log_in():
    uid = sec.create_user("ivanov", "пароль", "operator")
    sec.set_user_active(uid, False)

    with pytest.raises(sec.AuthError):
        sec.authenticate("ivanov", "пароль")


def test_lockout_after_repeated_failures():
    sec.create_user("ivanov", "пароль", "operator")

    for _ in range(sec.MAX_FAILED_LOGINS):
        with pytest.raises(sec.AuthError):
            sec.authenticate("ivanov", "не тот")

    # верный пароль тоже не пускает, пока держится блокировка
    with pytest.raises(sec.AuthError) as locked:
        sec.authenticate("ivanov", "пароль")

    assert "заблокирован" in str(locked.value)


def test_duplicate_username_rejected():
    sec.create_user("ivanov", "пароль", "operator")

    with pytest.raises(ValueError):
        sec.create_user("IVANOV", "другой", "viewer")


# ------------------------------------------------------------ сессии

def test_session_roundtrip():
    uid = sec.create_user("ivanov", "пароль", "operator")
    token = sec.create_session(uid)

    assert sec.resolve_session(token)["id"] == uid


def test_session_token_is_not_stored_in_clear():
    """Файл базы не должен давать возможности войти чужой сессией."""
    from db import sqlite_cursor

    uid = sec.create_user("ivanov", "пароль", "operator")
    token = sec.create_session(uid)

    with sqlite_cursor() as cur:
        cur.execute("SELECT token_hash FROM user_sessions")
        stored = [row["token_hash"] for row in cur.fetchall()]

    assert token not in stored


def test_logout_invalidates_session():
    uid = sec.create_user("ivanov", "пароль", "operator")
    token = sec.create_session(uid)
    sec.destroy_session(token)

    assert sec.resolve_session(token) is None


def test_role_change_invalidates_sessions():
    """Понижение в правах должно действовать сразу, а не со следующего входа."""
    uid = sec.create_user("ivanov", "пароль", "admin")
    token = sec.create_session(uid)
    sec.set_user_role(uid, "viewer")

    assert sec.resolve_session(token) is None


def test_permission_change_invalidates_sessions():
    uid = sec.create_user("ivanov", "пароль", "operator")
    token = sec.create_session(uid)
    sec.set_override(uid, "sync.run", False)

    assert sec.resolve_session(token) is None


def test_deactivation_invalidates_sessions():
    uid = sec.create_user("ivanov", "пароль", "operator")
    token = sec.create_session(uid)
    sec.set_user_active(uid, False)

    assert sec.resolve_session(token) is None


def test_garbage_token_resolves_to_nobody():
    assert sec.resolve_session("выдуманный-токен") is None
    assert sec.resolve_session("") is None
    assert sec.resolve_session(None) is None


def test_admin_exists_tracks_active_admins():
    assert sec.admin_exists() is False

    uid = sec.create_user("admin5", "пароль", "admin")
    assert sec.admin_exists() is True

    sec.set_user_active(uid, False)
    assert sec.admin_exists() is False
