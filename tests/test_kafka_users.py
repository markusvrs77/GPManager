# -*- coding: utf-8 -*-
"""
Пользователи Kafka: проверка заявки, разговор с брокером и журнал.

Главный тест здесь — test_password_never_reaches_the_audit_log. Всё
остальное чинится повторным нажатием кнопки, а пароль, утёкший в
SQLite, остаётся там навсегда.
"""

import pytest

from modules import kafka_client, kafka_users
from modules.kafka_audit import recent
from modules.kafka_client import KafkaUnavailable
from modules.kafka_clusters import create_cluster, delete_cluster, get_cluster

CLUSTER = {"bootstrap_servers": "kfk1:9092", "security_protocol": "SASL_SSL",
           "request_timeout_ms": 2000}

PASSWORD = "очень-секретный-пароль"


class FakeMechanism(object):
    def __init__(self, name):
        self.name = name


class FakeAdmin(object):
    """Запоминает, что ушло на брокер, и отвечает без ошибок."""

    def __init__(self, answer=None, described=None):
        self.answer = answer if answer is not None else {}
        self.described = described or {}
        self.alterations = None

    def describe_user_scram_credentials(self, users=None):
        return self.described

    def alter_user_scram_credentials(self, alterations):
        self.alterations = alterations
        return self.answer

    def close(self):
        pass


@pytest.fixture
def cluster_id():
    new_id = create_cluster(dict(CLUSTER, name="kafka-users-test"))
    yield new_id
    delete_cluster(new_id)


# ------------------------------------------------------------ заявка

def test_username_keeps_a_plain_name():
    assert kafka_users.validate_username(" svc_etl ") == "svc_etl"


def test_username_accepts_a_pasted_principal():
    """Имя чаще всего копируют прямо из таблицы правил."""
    assert kafka_users.validate_username("User:svc_etl") == "svc_etl"


@pytest.mark.parametrize("name", [
    "", "   ", "имя-кириллицей", "с пробелом", "-начинается-с-дефиса",
    "двое:точие", "звёздочка*",
])
def test_bad_username_is_refused(name):
    with pytest.raises(ValueError):
        kafka_users.validate_username(name)


def test_short_password_is_refused():
    with pytest.raises(ValueError):
        kafka_users.validate_password("коротко")


def test_password_with_edge_spaces_is_refused():
    """Обрезанный при копировании пробел — вечная загадка «не подходит»."""
    with pytest.raises(ValueError):
        kafka_users.validate_password("  пароль-достаточной-длины  ")


def test_mechanism_defaults_to_sha512():
    assert kafka_users.validate_mechanism("") == "SCRAM-SHA-512"


def test_unknown_mechanism_is_refused():
    with pytest.raises(ValueError):
        kafka_users.validate_mechanism("SCRAM-SHA-1")


def test_iterations_below_the_broker_minimum_are_refused():
    with pytest.raises(ValueError):
        kafka_users.validate_iterations(1024)


def test_audit_details_drop_the_password():
    spec = kafka_users.build_user_spec({
        "username": "svc_etl", "password": PASSWORD,
    })

    assert "password" not in kafka_users.audit_details(spec)


def test_mechanism_name_is_readable():
    assert kafka_users.mechanism_name(
        FakeMechanism("SCRAM_SHA_512")) == "SCRAM-SHA-512"


# ------------------------------------------------------------ транспорт

def test_upsert_sends_the_right_password(monkeypatch, cluster_id):
    """
    На брокер уходит не пароль, а PBKDF2 от него.

    Проверяем повторением того же расчёта: иначе легко отправить
    чужую строку и заметить это только при первом входе клиента.
    """
    import hashlib

    admin = FakeAdmin()
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: admin)

    kafka_client.upsert_scram_user(get_cluster(cluster_id), {
        "username": "svc_etl", "password": PASSWORD,
        "mechanism": "SCRAM-SHA-512", "iterations": 8192,
    })

    sent = admin.alterations[0]
    expected = hashlib.pbkdf2_hmac(
        "sha512", PASSWORD.encode("utf-8"), sent.salt, 8192)

    assert sent.user == "svc_etl"
    assert sent.iterations == 8192
    assert sent.salted_password == expected


def test_plain_password_does_not_survive_the_request(monkeypatch, cluster_id):
    """В объекте, уходящем на брокер, открытого пароля быть не должно."""
    admin = FakeAdmin()
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: admin)

    kafka_client.upsert_scram_user(get_cluster(cluster_id), {
        "username": "svc_etl", "password": PASSWORD,
        "mechanism": "SCRAM-SHA-512", "iterations": 8192,
    })

    sent = admin.alterations[0]
    body = " ".join(str(v) for v in vars(sent).values()) + repr(sent)

    assert PASSWORD not in body


def test_broker_refusal_in_the_answer_is_not_silent(monkeypatch, cluster_id):
    """
    alter_user_scram_credentials не бросает исключение при отказе.

    Он складывает ошибку в ответ, и без разбора ответа страница
    рапортовала бы об успехе на пустом месте.
    """
    admin = FakeAdmin(answer={"svc_etl": "NOT_CONTROLLER"})
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: admin)

    with pytest.raises(KafkaUnavailable):
        kafka_client.upsert_scram_user(get_cluster(cluster_id), {
            "username": "svc_etl", "password": PASSWORD,
            "mechanism": "SCRAM-SHA-512", "iterations": 8192,
        })


def test_describe_keeps_a_broken_user_from_hiding_the_rest(monkeypatch,
                                                           cluster_id):
    admin = FakeAdmin(described={
        "good": {"error": None, "credential_infos": [
            {"mechanism": FakeMechanism("SCRAM_SHA_512"), "iterations": 8192}]},
        "bad": {"error": "RESOURCE_NOT_FOUND", "credential_infos": []},
    })
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: admin)

    rows = kafka_client.fetch_scram_users(get_cluster(cluster_id))

    assert [r["username"] for r in rows] == ["bad", "good"]
    assert rows[0]["error"] == "RESOURCE_NOT_FOUND"


# ------------------------------------------------------------ маршруты

def test_create_user_answers_with_the_principal(monkeypatch, client,
                                                cluster_id):
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    response = client.post(
        "/api/kafka/clusters/{}/users".format(cluster_id),
        json={"username": "svc_etl", "password": PASSWORD},
    )

    assert response.status_code == 200
    assert response.get_json()["user"]["principal"] == "User:svc_etl"


def test_create_user_says_it_granted_nothing(monkeypatch, client, cluster_id):
    """Учётка без правил ничего не может — это должно быть сказано вслух."""
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    response = client.post(
        "/api/kafka/clusters/{}/users".format(cluster_id),
        json={"username": "svc_etl", "password": PASSWORD},
    )

    assert "прав" in response.get_json()["hint"].lower()


def test_password_never_reaches_the_audit_log(monkeypatch, client,
                                              cluster_id):
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    client.post(
        "/api/kafka/clusters/{}/users".format(cluster_id),
        json={"username": "svc_etl", "password": PASSWORD},
    )

    written = "".join(str(row) for row in recent(cluster_id))

    assert "svc_etl" in written
    assert PASSWORD not in written


def test_short_password_is_refused_by_the_route(monkeypatch, client,
                                                cluster_id):
    admin = FakeAdmin()
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: admin)

    response = client.post(
        "/api/kafka/clusters/{}/users".format(cluster_id),
        json={"username": "svc_etl", "password": "коротко"},
    )

    assert response.status_code == 400
    # до брокера заявка не дошла
    assert admin.alterations is None


def test_unreachable_broker_is_logged_as_a_failure(monkeypatch, client,
                                                   cluster_id):
    def boom(cluster):
        raise KafkaUnavailable("кластер недоступен")

    monkeypatch.setattr(kafka_client, "open_admin", boom)

    response = client.post(
        "/api/kafka/clusters/{}/users".format(cluster_id),
        json={"username": "svc_etl", "password": PASSWORD},
    )

    assert response.status_code == 502
    assert recent(cluster_id)[0]["result"] == "error"


def test_delete_warns_that_the_rules_outlive_the_user(monkeypatch, client,
                                                      cluster_id):
    """
    Правила переживают учётку и достанутся тёзке, если его заведут.

    Про это нужно сказать в ответе: иначе снятая учётная запись
    выглядит как отозванный доступ, а это не одно и то же.
    """
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    response = client.delete(
        "/api/kafka/clusters/{}/users/svc_etl".format(cluster_id))

    assert response.status_code == 200
    assert "Правила" in response.get_json()["hint"]


def test_unknown_cluster_is_404(client):
    assert client.get("/api/kafka/clusters/999999/users").status_code == 404


def test_operator_cannot_create_a_kafka_user(as_user, cluster_id):
    """Завести SCRAM-учётку значит выдать вход в кластер."""
    response = as_user("operator").post(
        "/api/kafka/clusters/{}/users".format(cluster_id),
        json={"username": "svc_etl", "password": PASSWORD},
    )

    assert response.status_code == 403


def test_operator_may_still_read_the_list(monkeypatch, as_user, cluster_id):
    monkeypatch.setattr(kafka_client, "open_admin", lambda c: FakeAdmin())

    response = as_user("operator").get(
        "/api/kafka/clusters/{}/users".format(cluster_id))

    assert response.status_code == 200
