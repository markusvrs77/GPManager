# -*- coding: utf-8 -*-
"""Роуты вкладки Kafka. Отдельный Blueprint: app.py и так на 3400 строк."""

from flask import Blueprint, jsonify, render_template, request

from modules.kafka_audit import recent as audit_recent
from modules.kafka_audit import write as audit_write
from modules.kafka_client import (
    KafkaUnavailable,
    delete_group,
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
