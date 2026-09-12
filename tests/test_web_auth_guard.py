"""
Охрана маршрутов в работе.

Проверяется поведение отказа: куда уводит неизвестного, что видит
наблюдатель, пытающийся запустить перенос, и что происходит, когда id
чужого кластера подставлен руками в тело запроса.
"""

import pytest

import modules.security as sec
from modules.web_auth import SESSION_COOKIE


# ------------------------------------------------------------ без входа

def test_page_sends_anonymous_to_login(anon_client):
    response = anon_client.get("/gpcopy")

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


def test_login_form_remembers_where_the_user_was_going(anon_client):
    response = anon_client.get("/vacuum")

    location = response.headers["Location"]

    assert location.startswith("/login")
    assert "vacuum" in location


def test_api_answers_anonymous_with_401_not_a_redirect(anon_client):
    """Фоновый запрос страницы не должен получать HTML формы входа."""
    response = anon_client.get("/api/connections")

    assert response.status_code == 401
    assert response.get_json()["ok"] is False


def test_login_page_itself_is_open(anon_client):
    assert anon_client.get("/login").status_code == 200


def test_static_stays_open(anon_client):
    """Иначе форма входа осталась бы без стилей."""
    assert anon_client.get("/static/css/auth.css").status_code == 200


# ------------------------------------------------------------ вход

def test_login_sets_session_cookie(anon_client):
    sec.create_user("guard-login", "пароль-достаточной-длины", "operator",
                    must_change_password=False)

    response = anon_client.post("/login", data={
        "username": "guard-login", "password": "пароль-достаточной-длины",
    })

    assert response.status_code == 302
    assert SESSION_COOKIE in response.headers.get("Set-Cookie", "")


def test_wrong_password_does_not_set_a_cookie(anon_client):
    sec.create_user("guard-bad", "пароль-достаточной-длины", "operator",
                    must_change_password=False)

    response = anon_client.post("/login", data={
        "username": "guard-bad", "password": "не тот",
    })

    assert response.status_code == 401
    assert SESSION_COOKIE not in response.headers.get("Set-Cookie", "")


@pytest.mark.parametrize("target", [
    "//evil.example/phish",
    "https://evil.example/phish",
    "javascript:alert(1)",
])
def test_login_never_redirects_off_site(anon_client, target):
    """Иначе форма входа становится заготовкой для фишинга."""
    name = "guard-next-{}".format(abs(hash(target)))
    sec.create_user(name, "пароль-достаточной-длины", "operator",
                    must_change_password=False)

    response = anon_client.post("/login", data={
        "username": name, "password": "пароль-достаточной-длины",
        "next": target,
    })

    assert response.headers["Location"] == "/"


def test_logout_clears_the_session(client):
    assert client.get("/gpcopy").status_code == 200

    client.post("/logout")

    assert client.get("/gpcopy").status_code == 302


def test_temporary_password_blocks_everything_else(as_user):
    """Пароль, который знает администратор, не должен работать как обычный."""
    import modules.security as s
    from app import app as flask_app

    user_id = s.create_user("guard-temp", "временный-пароль", "admin")
    token = s.create_session(user_id)

    c = flask_app.test_client()
    c.set_cookie(SESSION_COOKIE, token)

    response = c.get("/gpcopy")

    assert response.status_code == 302
    assert "/account/password" in response.headers["Location"]

    s.delete_user(user_id)


# ------------------------------------------------------------ права

def test_viewer_sees_the_sync_page(as_user):
    assert as_user("viewer").get("/gpcopy").status_code == 200


def test_viewer_cannot_start_a_transfer(as_user):
    response = as_user("viewer").post("/api/gpcopy/start", json={})

    assert response.status_code == 403
    assert "Недостаточно прав" in response.get_json()["message"]


def test_operator_gets_past_the_guard_on_start(as_user):
    """Дальше решает сам маршрут; охрана его уже пропустила."""
    response = as_user("operator").post("/api/gpcopy/start", json={})

    assert response.status_code != 403


def test_operator_cannot_reach_user_management(as_user):
    assert as_user("operator").get("/users").status_code == 403


def test_admin_reaches_user_management(client):
    assert client.get("/users").status_code == 200


def test_override_opens_a_single_route(as_user):
    without = as_user("viewer")
    with_right = as_user("viewer", overrides={"sync.run": True})

    assert without.post("/api/gpcopy/start", json={}).status_code == 403
    assert with_right.post("/api/gpcopy/start", json={}).status_code != 403


def test_override_closes_a_route_the_role_opened(as_user):
    c = as_user("operator", overrides={"vacuum.run": False})

    assert c.post("/api/vacuum/start", json={}).status_code == 403
    # остальное у роли осталось
    assert c.get("/vacuum").status_code == 200


def test_denied_page_explains_itself(as_user):
    response = as_user("viewer").get("/users")

    assert response.status_code == 403
    assert "Нет доступа" in response.get_data(as_text=True)


# ------------------------------------------------------------ кластеры

def test_cluster_id_in_the_query_string_is_checked(as_user):
    c = as_user("operator", connection_ids=[2])

    response = c.get("/api/objects/tree?connection_id=1")

    assert response.status_code == 403
    assert "кластеру" in response.get_json()["message"]


def test_allowed_cluster_passes_the_guard(as_user):
    c = as_user("operator", connection_ids=[1])

    assert c.get("/api/objects/tree?connection_id=1").status_code != 403


def test_cluster_id_in_the_body_is_checked(as_user):
    """Подстановка чужого id руками — основной способ обойти фильтр списка."""
    c = as_user("operator", connection_ids=[2])

    response = c.post("/api/gpcopy/start", json={
        "source_connection_id": 1, "dest_connection_id": 2,
    })

    assert response.status_code == 403


def test_user_without_clusters_reaches_none_of_them(as_user):
    c = as_user("operator")

    assert c.get("/api/objects/tree?connection_id=1").status_code == 403


def test_admin_is_not_limited_by_cluster_lists(client):
    assert client.get("/api/objects/tree?connection_id=1").status_code != 403


# ------------------------------------------------- некуда идти

def test_kafka_only_user_is_taken_to_kafka_not_to_a_refusal(as_user):
    """
    «/» открывают все и всегда.

    Пользователь с одной только Kafka упирался в отказ по Dashboard, а
    единственная кнопка на той странице вела обратно в тот же отказ.
    """
    c = as_user("viewer", overrides={
        code: False for code in _all_view_caps() if code != "kafka.view"
    })

    response = c.get("/")

    assert response.status_code == 302
    assert response.headers["Location"] == "/kafka"


def test_kafka_only_user_actually_reaches_kafka(as_user):
    c = as_user("viewer", overrides={
        code: False for code in _all_view_caps() if code != "kafka.view"
    })

    assert c.get("/", follow_redirects=True).status_code == 200


def test_refusal_page_offers_a_way_out(as_user):
    """Из отказа должен быть выход: в открытый раздел или наружу."""
    c = as_user("viewer", overrides={
        code: False for code in _all_view_caps() if code != "kafka.view"
    })

    body = c.get("/users").get_data(as_text=True)

    assert "/kafka" in body
    assert "/logout" in body


def test_user_without_anything_can_still_log_out(as_user):
    c = as_user("viewer", overrides={code: False for code in _all_view_caps()})

    response = c.get("/")

    assert response.status_code == 403
    assert "/logout" in response.get_data(as_text=True)


def test_sidebar_hides_a_toolkit_with_nothing_in_it(as_user):
    c = as_user("viewer", overrides={
        code: False for code in _all_view_caps() if code != "kafka.view"
    })

    body = c.get("/kafka").get_data(as_text=True)

    assert "Greenplum Toolkit" not in body
    assert "Kafka" in body


def _all_view_caps():
    return [code for code in sec.CAPABILITY_CODES if code.endswith(".view")]


def test_sidebar_hides_the_coming_soon_stubs(as_user):
    """
    Заглушки «скоро» открыть нельзя никому — в меню им не место.

    Проверяется на администраторе: если пометки исчезли даже у него,
    значит, скрыты они по существу, а не по нехватке прав.
    """
    body = as_user("admin").get("/", follow_redirects=True).get_data(as_text=True)

    assert "скоро" not in body
    assert "Oracle Toolkit" not in body
    assert "Pipelines" not in body


def test_empty_direction_takes_its_heading_with_it(as_user):
    """Заголовок над пустым списком — такая же ложь, как ссылка в отказ."""
    c = as_user("viewer", overrides={
        code: False for code in _all_view_caps() if code != "kafka.view"
    })

    body = c.get("/kafka").get_data(as_text=True)

    assert "DB Operations" not in body
    assert "Data Flow" in body


def test_viewer_without_jobs_does_not_poll_the_jobs_api(as_user):
    """Иначе наблюдатель ловил бы 403 каждые тридцать секунд."""
    c = as_user("viewer", overrides={
        code: False for code in _all_view_caps() if code != "kafka.view"
    })

    assert "/api/jobs/active" not in c.get("/kafka").get_data(as_text=True)
