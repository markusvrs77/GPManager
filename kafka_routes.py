# -*- coding: utf-8 -*-
"""Роуты вкладки Kafka. Отдельный Blueprint: app.py и так на 3400 строк."""

from flask import Blueprint, jsonify, render_template, request

from modules.kafka_audit import recent as audit_recent
from modules.kafka_audit import write as audit_write
from modules.kafka_client import (
    KafkaUnavailable,
    add_partitions,
    alter_topic_configs,
    create_topic,
    delete_group,
    delete_topic,
    fetch_topic_configs,
    library_available,
    ping,
    reset_offsets,
)
from modules.kafka_clusters import (
    create_cluster,
    delete_cluster,
    get_cluster,
    list_clusters,
    update_cluster,
)
from modules.kafka_groups import (
    GroupBusy,
    assert_group_is_idle,
    build_reset_specs,
    collect_groups,
    find_group,
)
from modules.kafka_overview import collect_overview
from modules.kafka_topics import (
    assert_can_grow,
    build_config_changes,
    build_topic_spec,
    parse_configs,
    validate_topic_name,
)

kafka_bp = Blueprint("kafka", __name__)


def _fail(message, code=400):
    return jsonify({"ok": False, "message": str(message)}), code


def _cluster_or_404(cluster_id):
    cluster = get_cluster(cluster_id)

    if not cluster:
        raise LookupError("Кластер не найден: {}".format(cluster_id))

    return cluster


@kafka_bp.route("/kafka")
def kafka_page():
    return render_template(
        "kafka.html",
        clusters=list_clusters(),
        library_ready=library_available(),
    )


@kafka_bp.route("/kafka/connections")
def kafka_connections_page():
    return render_template(
        "kafka_connections.html",
        clusters=list_clusters(),
        library_ready=library_available(),
    )


@kafka_bp.route("/api/kafka/clusters", methods=["GET"])
def api_kafka_clusters():
    return jsonify({"ok": True, "clusters": list_clusters()})


@kafka_bp.route("/api/kafka/clusters", methods=["POST"])
def api_kafka_cluster_create():
    try:
        cluster_id = create_cluster(request.get_json(silent=True) or {})
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "id": cluster_id})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>", methods=["PUT"])
def api_kafka_cluster_update(cluster_id):
    try:
        _cluster_or_404(cluster_id)
        changed = update_cluster(
            cluster_id, request.get_json(silent=True) or {})
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "changed": changed})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>", methods=["DELETE"])
def api_kafka_cluster_delete(cluster_id):
    return jsonify({"ok": bool(delete_cluster(cluster_id))})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/ping", methods=["POST"])
def api_kafka_ping(cluster_id):
    try:
        cluster = _cluster_or_404(cluster_id)
    except LookupError as error:
        return _fail(error, 404)

    return jsonify(ping(cluster))


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/overview",
                methods=["GET"])
def api_kafka_overview(cluster_id):
    return _overview(cluster_id, force=False)


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/overview/refresh",
                methods=["POST"])
def api_kafka_overview_refresh(cluster_id):
    return _overview(cluster_id, force=True)


def _overview(cluster_id, force):
    try:
        _cluster_or_404(cluster_id)
        data = collect_overview(cluster_id, force=force)
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        # 502: приложение живо, недоступен внешний кластер
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "overview": data})


# ---------------- консьюмер-группы ----------------

@kafka_bp.route("/kafka/groups")
def kafka_groups_page():
    return render_template(
        "kafka_groups.html",
        clusters=list_clusters(),
        library_ready=library_available(),
    )


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/groups",
                methods=["GET"])
def api_kafka_groups(cluster_id):
    return _groups(cluster_id, force=False)


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/groups/refresh",
                methods=["POST"])
def api_kafka_groups_refresh(cluster_id):
    return _groups(cluster_id, force=True)


def _groups(cluster_id, force):
    try:
        _cluster_or_404(cluster_id)
        data = collect_groups(cluster_id, force=force)
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "groups": data})


def _idle_group_or_error(cluster_id, group_id):
    """Группа из среза, уже проверенная на активных участников."""
    group = find_group(collect_groups(cluster_id), group_id)

    if not group:
        raise LookupError(
            "Группа не найдена: {} — обновите срез".format(group_id))

    assert_group_is_idle(group)

    return group


def _group_partitions(group, topics):
    """Партиции группы, при желании суженные до выбранных топиков."""
    wanted = set(topics or [])
    pairs = []

    for topic in group.get("topics") or []:
        if wanted and topic.get("name") not in wanted:
            continue

        for part in topic.get("parts") or []:
            pairs.append((topic.get("name"), part.get("p")))

    return pairs


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/groups/<group_id>/reset",
    methods=["POST"])
def api_kafka_group_reset(cluster_id, group_id):
    body = request.get_json(silent=True) or {}

    try:
        cluster = _cluster_or_404(cluster_id)
        group = _idle_group_or_error(cluster_id, group_id)
        specs = build_reset_specs(
            body.get("mode"),
            body.get("timestamp"),
            _group_partitions(group, body.get("topics")),
        )
    except LookupError as error:
        return _fail(error, 404)
    except GroupBusy as error:
        return _fail(error, 409)
    except ValueError as error:
        return _fail(error)

    intent = {
        "mode": body.get("mode"),
        "timestamp": body.get("timestamp"),
        "partitions": len(specs),
    }

    try:
        answer = reset_offsets(cluster, group_id, specs)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "reset_offsets", group_id, intent, "error")
        return _fail(error, 502)

    failed = [
        {"topic": topic, "partition": part, "error": text}
        for (topic, part), text in sorted(answer.items()) if text
    ]
    done = len(answer) - len(failed)
    result = "ok" if not failed else ("partial" if done else "failed")

    intent["done"] = done
    intent["failed"] = failed
    audit_write(cluster_id, "reset_offsets", group_id, intent, result)

    return jsonify({"ok": True, "done": done, "failed": failed,
                    "result": result})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/groups/<group_id>",
                methods=["DELETE"])
def api_kafka_group_delete(cluster_id, group_id):
    try:
        cluster = _cluster_or_404(cluster_id)
        _idle_group_or_error(cluster_id, group_id)
    except LookupError as error:
        return _fail(error, 404)
    except GroupBusy as error:
        return _fail(error, 409)

    try:
        delete_group(cluster, group_id)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "delete_group", group_id, None, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "delete_group", group_id, None, "ok")

    return jsonify({"ok": True})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/audit",
                methods=["GET"])
def api_kafka_audit(cluster_id):
    return jsonify({"ok": True, "records": audit_recent(cluster_id)})


# ---------------- управление топиками ----------------

def _topic_in_overview(cluster_id, name):
    """Топик из среза обзора: оттуда берём текущее число партиций."""
    data = collect_overview(cluster_id)

    for topic in data.get("topics") or []:
        if topic.get("name") == name:
            return topic

    return None


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/topics",
                methods=["POST"])
def api_kafka_topic_create(cluster_id):
    try:
        cluster = _cluster_or_404(cluster_id)
        spec = build_topic_spec(request.get_json(silent=True) or {})
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    try:
        create_topic(cluster, spec)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "create_topic", spec["name"], spec, "error")
        code = 409 if "уже есть" in str(error) else 502
        return _fail(error, code)

    audit_write(cluster_id, "create_topic", spec["name"], spec, "ok")
    collect_overview(cluster_id, force=True)

    return jsonify({"ok": True, "name": spec["name"]})


@kafka_bp.route("/api/kafka/clusters/<int:cluster_id>/topics/<name>",
                methods=["DELETE"])
def api_kafka_topic_delete(cluster_id, name):
    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        return _fail(error)

    try:
        delete_topic(cluster, topic)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "delete_topic", topic, None, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "delete_topic", topic, None, "ok")
    collect_overview(cluster_id, force=True)

    return jsonify({"ok": True})


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/topics/<name>/partitions",
    methods=["POST"])
def api_kafka_topic_partitions(cluster_id, name):
    body = request.get_json(silent=True) or {}
    known = None

    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
        known = _topic_in_overview(cluster_id, topic)

        if not known:
            raise LookupError(
                "Топик не найден: {} — обновите срез".format(topic))

        total = assert_can_grow(known.get("partitions"), body.get("total"))
    except LookupError as error:
        return _fail(error, 404)
    except ValueError as error:
        # уменьшение партиций — конфликт состояния, не ошибка ввода
        code = 409 if "уменьшить" in str(error) else 400
        return _fail(error, code)

    intent = {"from": known.get("partitions"), "to": total}

    try:
        add_partitions(cluster, topic, total)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "add_partitions", topic, intent, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "add_partitions", topic, intent, "ok")
    collect_overview(cluster_id, force=True)

    return jsonify({"ok": True, "total": total})


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/topics/<name>/configs",
    methods=["GET"])
def api_kafka_topic_configs(cluster_id, name):
    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
        described = fetch_topic_configs(cluster, topic)
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    return jsonify({"ok": True, "configs": parse_configs(described, topic)})


@kafka_bp.route(
    "/api/kafka/clusters/<int:cluster_id>/topics/<name>/configs",
    methods=["PUT"])
def api_kafka_topic_configs_update(cluster_id, name):
    body = request.get_json(silent=True) or {}

    try:
        cluster = _cluster_or_404(cluster_id)
        topic = validate_topic_name(name)
        current = parse_configs(fetch_topic_configs(cluster, topic), topic)
        changes = build_config_changes(current, body.get("configs") or {})
    except LookupError as error:
        return _fail(error, 404)
    except KafkaUnavailable as error:
        return _fail(error, 502)
    except ValueError as error:
        return _fail(error)

    if not changes:
        return jsonify({"ok": True, "changed": 0,
                        "message": "Изменений нет"})

    try:
        alter_topic_configs(cluster, topic, changes)
    except KafkaUnavailable as error:
        audit_write(cluster_id, "alter_configs", topic, changes, "error")
        return _fail(error, 502)

    audit_write(cluster_id, "alter_configs", topic, changes, "ok")

    return jsonify({"ok": True, "changed": len(changes)})
