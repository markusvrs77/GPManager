"""
Управление пользователями через API.

Проверяются главным образом запреты. Интерфейс прячет опасные кнопки,
но их обходит любой, кто умеет отправить POST руками, — значит, отказ
должен приходить с сервера.
"""

import pytest

import modules.security as sec


@pytest.fixture(autouse=True)
def _only_the_test_admin():
    """Убирает посторонние учётки, оставшиеся от соседних тестов."""
    from db import sqlite_cursor

    with sqlite_cursor(commit=True) as cur:
        for table in ("user_sessions", "user_permissions",
                      "user_connections", "users"):
            cur.execute("DELETE FROM {}".format(table))
    yield


def _create(client, name="petrov", role="operator"):
    response = client.post("/api/users", json={
        "username": name, "password": "достаточно-длинный", "role": role,
    })
    return response.get_json()["user"]["id"]


# ------------------------------------------------------------ создание

def test_admin_creates_a_user(client):
    response = client.post("/api/users", json={
        "username": "petrov", "password": "достаточно-длинный",
        "role": "operator",
    })

    assert response.status_code == 200
    assert response.get_json()["user"]["username"] == "petrov"


def test_new_user_must_change_the_password_given_to_them(client):
    response = client.post("/api/users", json={
        "username": "petrov", "password": "достаточно-длинный",
        "role": "operator",
    })

    assert response.get_json()["user"]["must_change_password"] is True


def test_short_password_is_refused(client):
    response = client.post("/api/users", json={
        "username": "petrov", "password": "коротк", "role": "viewer",
    })

    assert response.status_code == 400
    assert sec.get_user_by_name("petrov") is None


def test_duplicate_name_is_refused(client):
    _create(client, name="petrov", role="viewer")

    response = client.post("/api/users", json={
        "username": "PETROV", "password": "достаточно-длинный",
        "role": "admin",
    })

    assert response.status_code == 400


def test_unknown_role_is_refused(client):
    response = client.post("/api/users", json={
        "username": "petrov", "password": "достаточно-длинный",
        "role": "начальник",
    })

    assert response.status_code == 400


def test_new_user_starts_without_clusters(client):
    """Доступ к кластерам выдаётся отдельно и осознанно."""
    response = client.post("/api/users", json={
        "username": "petrov", "password": "достаточно-длинный",
        "role": "operator",
    })

    assert response.get_json()["user"]["connection_ids"] == []


# ------------------------------------------------------------ правка

def test_role_change_is_saved(client):
    user_id = _create(client)

    response = client.put("/api/users/{}".format(user_id),
                          json={"role": "viewer"})

    assert response.get_json()["user"]["role"] == "viewer"


def test_override_is_saved_and_visible(client):
    user_id = _create(client)

    response = client.put("/api/users/{}".format(user_id),
                          json={"overrides": {"grants.edit": True}})

    assert "grants.edit" in response.get_json()["user"]["capabilities"]


def test_missing_override_returns_the_capability_to_the_role(client):
    user_id = _create(client)
    client.put("/api/users/{}".format(user_id),
               json={"overrides": {"sync.run": False}})

    response = client.put("/api/users/{}".format(user_id),
                          json={"overrides": {}})

    assert "sync.run" in response.get_json()["user"]["capabilities"]


def test_clusters_are_saved(client):
    user_id = _create(client)

    response = client.put("/api/users/{}".format(user_id),
                          json={"connection_ids": [2, 5]})

    assert response.get_json()["user"]["connection_ids"] == [2, 5]


def test_deactivation_is_saved(client):
    user_id = _create(client)

    response = client.put("/api/users/{}".format(user_id),
                          json={"is_active": False})

    assert response.get_json()["user"]["is_active"] is False


def test_unknown_user_is_404(client):
    response = client.put("/api/users/99999", json={"role": "viewer"})

    assert response.status_code == 404


# ------------------------------------------------- последний администратор

def test_last_admin_cannot_be_demoted(client, admin_user):
    response = client.put("/api/users/{}".format(admin_user["id"]),
                          json={"role": "viewer"})

    assert response.status_code == 400
    assert sec.get_user(admin_user["id"])["role"] == "admin"


def test_last_admin_cannot_be_switched_off(client, admin_user):
    response = client.put("/api/users/{}".format(admin_user["id"]),
                          json={"is_active": False})

    assert response.status_code == 400
    assert sec.get_user(admin_user["id"])["is_active"] is True


def test_last_admin_cannot_be_deleted(client, admin_user):
    response = client.delete("/api/users/{}".format(admin_user["id"]))

    assert response.status_code == 400
    assert sec.get_user(admin_user["id"]) is not None


def test_admin_can_be_demoted_once_another_one_exists(client, admin_user):
    _create(client, name="second-admin", role="admin")

    response = client.put("/api/users/{}".format(admin_user["id"]),
                          json={"role": "viewer"})

    assert response.status_code == 200


def test_nobody_deletes_themselves(client, admin_user):
    """Даже когда администраторов двое — это всегда промах руки."""
    _create(client, name="second-admin", role="admin")

    response = client.delete("/api/users/{}".format(admin_user["id"]))

    assert response.status_code == 400
    assert "себя" in response.get_json()["message"]


# ------------------------------------------------------------ пароли

def test_admin_resets_a_password(client):
    user_id = _create(client)

    response = client.post("/api/users/{}/password".format(user_id),
                           json={"password": "новый-длинный-пароль"})

    assert response.status_code == 200
    assert sec.verify_password("новый-длинный-пароль",
                               sec.get_user(user_id)["password_hash"])


def test_reset_password_must_be_changed_by_its_owner(client):
    user_id = _create(client)

    client.post("/api/users/{}/password".format(user_id),
                json={"password": "новый-длинный-пароль"})

    assert sec.get_user(user_id)["must_change_password"] is True


def test_short_reset_password_is_refused(client):
    user_id = _create(client)
    before = sec.get_user(user_id)["password_hash"]

    response = client.post("/api/users/{}/password".format(user_id),
                           json={"password": "коротк"})

    assert response.status_code == 400
    assert sec.get_user(user_id)["password_hash"] == before


def test_reset_closes_open_sessions(client):
    """Смена пароля обязана выбивать того, кто уже сидит внутри."""
    user_id = _create(client)
    token = sec.create_session(user_id)

    client.post("/api/users/{}/password".format(user_id),
                json={"password": "новый-длинный-пароль"})

    assert sec.resolve_session(token) is None
