"""
Карта прав должна покрывать все маршруты.

Смысл файла в одном тесте — test_every_route_has_a_policy. Маршрут,
забытый в POLICY, отдаёт 403 всем подряд, включая администратора; пусть
об этом узнаёт сборка, а не дежурный в понедельник.
"""

import pytest

from app import app as flask_app
from modules import web_auth
import modules.security as sec


def _endpoints():
    return {rule.endpoint for rule in flask_app.url_map.iter_rules()}


def test_every_route_has_a_policy():
    missing = sorted(e for e in _endpoints() if e not in web_auth.POLICY)

    assert missing == [], (
        "Маршруты без записи в POLICY (закрыты для всех): {}".format(missing)
    )


def test_policy_has_no_entries_for_dead_routes():
    """Оставшаяся запись маскирует опечатку в имени endpoint."""
    stale = sorted(set(web_auth.POLICY) - _endpoints())

    assert stale == []


def test_policy_names_only_real_capabilities():
    markers = {web_auth.PUBLIC, web_auth.AUTHENTICATED}
    unknown = []

    for endpoint, rule in web_auth.POLICY.items():
        values = rule.values() if isinstance(rule, dict) else [rule]

        for value in values:
            if value not in markers and value not in sec.CAPABILITY_CODES:
                unknown.append((endpoint, value))

    assert unknown == []


def test_every_method_of_a_split_route_is_described():
    """
    У маршрута с разными правами на GET и POST метод, не попавший в
    словарь, молча закрывается — проверяем, что таких нет.
    """
    gaps = []

    for rule in flask_app.url_map.iter_rules():
        policy = web_auth.POLICY.get(rule.endpoint)

        if not isinstance(policy, dict):
            continue

        for method in rule.methods - {"HEAD", "OPTIONS"}:
            if method not in policy:
                gaps.append((rule.endpoint, method))

    assert gaps == []


def test_public_routes_are_only_the_expected_ones():
    """Список открытых наружу адресов не должен расти незаметно."""
    public = sorted(
        e for e, v in web_auth.POLICY.items() if v == web_auth.PUBLIC
    )

    assert public == [
        "auth.login_page",
        "auth.login_submit",
        "auth.setup_page",
        "auth.setup_submit",
        "static",
    ]


@pytest.mark.parametrize("endpoint", ["", "нет-такого", "api_выдуманный"])
def test_unknown_endpoint_is_denied(endpoint):
    assert web_auth.required_capability(endpoint, "GET") is None


def test_method_outside_the_dict_is_denied():
    assert web_auth.required_capability("api_schedules", "DELETE") is None


def test_write_routes_never_settle_for_a_view_capability():
    """
    Маршрут, который что-то меняет, не должен требовать просмотра.

    Исключения перечислены поимённо: это чтения через POST — тело
    запроса длиннее, чем влезает в строку адреса.
    """
    reads_via_post = {
        "api_catalog_expand_mask", "api_catalog_resolve_list",
        "api_catalog_resolve_columns", "api_catalog_primary_keys",
        "api_catalog_resolve_keys", "api_catalog_compute_unique",
        "api_gpcopy_precheck", "api_gpcopy_preview_date_json",
        "api_gpcopy_strategies", "api_gpcopy_window_preview",
        "api_gpcopy_increment_preview", "api_gpcopy_partition_diff_preview",
        "api_gpcopy_partition_diff_preview_bulk", "api_gpcopy_sync_preview",
        "api_schedules_preview", "api_test_connection",
        # считает рекомендацию по pg_attribute и ничего не меняет
        "api_reorganize_recommendation",
        "kafka.api_kafka_ping", "kafka.api_kafka_overview_refresh",
        "kafka.api_kafka_groups_refresh", "kafka.api_kafka_messages_read",
        "kafka.api_kafka_acls_list",
        "auth.login_submit", "auth.setup_submit", "auth.logout",
        "auth.password_submit",
    }

    offenders = []

    for rule in flask_app.url_map.iter_rules():
        writes = rule.methods & {"POST", "PUT", "DELETE", "PATCH"}

        if not writes or rule.endpoint in reads_via_post:
            continue

        for method in writes:
            required = web_auth.required_capability(rule.endpoint, method)

            if required and required.endswith(".view"):
                offenders.append((rule.endpoint, method, required))

    assert offenders == []
