# -*- coding: utf-8 -*-
"""Роуты вкладки Kafka. Отдельный Blueprint: app.py и так на 3400 строк."""

from flask import Blueprint, jsonify, render_template, request

from modules.kafka_client import KafkaUnavailable, library_available, ping
from modules.kafka_clusters import (
    create_cluster,
    delete_cluster,
    get_cluster,
    list_clusters,
    update_cluster,
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
