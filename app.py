from flask import Flask, render_template, request, redirect, url_for, jsonify
import threading
from config import APP_HOST, APP_PORT, APP_DEBUG
from db import init_db
from modules.connections import (
    list_connections,
    create_connection,
    delete_connection,
    test_gp_connection,
)
from modules.vacuum_analyze import run_vacuum_analyze_job
from job_manager import (
    create_job,
    create_job_items,
    get_job,
    get_job_items,
    get_latest_job,
    mark_interrupted_jobs_on_startup,
    set_stop_flag,
)
from modules.skew_analyzer import (
    analyze_tables_skew,
    get_last_skew_results,
    run_skew_job,
    get_skew_results_by_job,
    get_skew_summary_by_job,
    get_skew_result_segments,
    get_latest_problem_skew_results,
)
from modules.object_tree import get_object_tree

from modules.reorganize import (
    get_reorganize_targets,
    run_reorganize_job,
    get_distribution_recommendation,
    apply_distribution_and_reorganize,
)


from modules.gpcopy import (
    run_gpcopy_job,
    build_gpcopy_date_include_json_preview,
    get_date_columns_for_table,
)

from modules.dashboard import get_session_limits_stats


app = Flask(__name__)


@app.route("/")
@app.route("/dashboard")
def dashboard_page():
    connections = list_connections()
    last_results = get_last_skew_results(limit=1000)
    latest_skew_job = get_latest_job("skew")

    skew_summary = build_skew_dashboard_summary(last_results)

    return render_template(
        "dashboard.html",
        connections=connections,
        last_results=last_results,
        latest_skew_job=latest_skew_job,
        skew_summary=skew_summary,
    )


@app.route("/connections")
def connections_page():
    connections = list_connections()
    return render_template(
        "connections.html",
        connections=connections,
    )


@app.route("/connections/add", methods=["POST"])
def add_connection():
    try:
        create_connection(request.form.to_dict())
        return redirect(url_for("connections_page"))
    except Exception as e:
        connections = list_connections()
        return render_template(
            "connections.html",
            connections=connections,
            error=str(e),
        )


@app.route("/connections/delete/<int:connection_id>", methods=["POST"])
def remove_connection(connection_id):
    delete_connection(connection_id)
    return redirect(url_for("connections_page"))


@app.route("/objects")
def objects_page():
    connections = list_connections()
    return render_template(
        "objects.html",
        connections=connections,
    )


@app.route("/api/connections")
def api_connections():
    return jsonify(
        {
            "ok": True,
            "connections": list_connections(),
        }
    )


@app.route("/api/connections/<int:connection_id>/test", methods=["POST"])
def api_test_connection(connection_id):
    result = test_gp_connection(connection_id)
    return jsonify(result)


@app.route("/api/objects/tree")
def api_objects_tree():
    connection_id = request.args.get("connection_id", type=int)

    if not connection_id:
        return jsonify(
            {
                "ok": False,
                "message": "connection_id обязателен",
            }
        ), 400

    try:
        tree = get_object_tree(connection_id)
        return jsonify(
            {
                "ok": True,
                "tree": tree,
            }
        )
    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


@app.route("/skew")
def skew_page():
    return redirect(url_for("maintenance_page"))


@app.route("/api/skew/analyze", methods=["POST"])
def api_skew_analyze():
    data = request.get_json(silent=True) or {}

    connection_id = data.get("connection_id")
    tables = data.get("tables") or []

    if not connection_id:
        return jsonify(
            {
                "ok": False,
                "message": "connection_id обязателен",
            }
        ), 400

    if not tables:
        return jsonify(
            {
                "ok": False,
                "message": "Не выбраны таблицы",
            }
        ), 400

    try:
        results = analyze_tables_skew(
            connection_id=int(connection_id),
            tables=tables,
        )

        return jsonify(
            {
                "ok": True,
                "results": results,
            }
        )

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


@app.route("/api/skew/results")
def api_skew_results():
    limit = request.args.get("limit", 100, type=int)

    return jsonify(
        {
            "ok": True,
            "results": get_last_skew_results(limit),
        }
    )

@app.route("/api/skew/start", methods=["POST"])
def api_skew_start_job():
    data = request.get_json(silent=True) or {}

    connection_id = data.get("connection_id")
    tables = data.get("tables") or []

    if not connection_id:
        return jsonify(
            {
                "ok": False,
                "message": "connection_id обязателен",
            }
        ), 400

    if not tables:
        return jsonify(
            {
                "ok": False,
                "message": "Не выбраны таблицы",
            }
        ), 400

    try:
        config = {
            "source_connection_id": source_connection_id,
            "dest_connection_id": dest_connection_id,
            "tables": tables,

            "gpcopy_path": gpcopy_path,
            "jobs": jobs,
            "on_segment_threshold": on_segment_threshold,

            "target_schema": target_schema,
            "target_mode": target_mode,

            "truncate": truncate,
            "drop": drop,
            "append": append,
            "skip_existing": skip_existing,
            "analyze": analyze,
            "dry_run": dry_run,
            "validate_count": validate_count,
            "extra_args": extra_args,
        }

        job_id = create_job(
            job_type="skew",
            connection_id=int(connection_id),
            config=config,
        )

        #run_background_job(job_id, run_skew_job)
        threading.Thread(
            target=run_skew_job,
            args=(job_id,),
            daemon=True
        ).start()

        return jsonify(
            {
                "ok": True,
                "job_id": job_id,
            }
        )

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


@app.route("/api/jobs/<int:job_id>")
def api_get_job(job_id):
    job = get_job(job_id)

    if not job:
        return jsonify(
            {
                "ok": False,
                "message": "Job not found",
            }
        ), 404

    return jsonify(
        {
            "ok": True,
            "job": job,
        }
    )

@app.route("/api/jobs/<int:job_id>/status")
def api_job_status(job_id):
    job = get_job(job_id)

    if not job:
        return jsonify(
            {
                "ok": False,
                "message": "Job not found",
            }
        ), 404

    items = get_job_items(job_id)

    total = len(items)
    done = len([i for i in items if i.get("status") == "done"])
    failed = len([i for i in items if i.get("status") == "failed"])
    skipped = len([i for i in items if i.get("status") == "skipped"])
    running = len([i for i in items if i.get("status") == "running"])

    finished = done + failed + skipped

    percent = 0

    if total > 0:
        percent = round((finished * 100.0) / total, 2)

    return jsonify(
        {
            "ok": True,
            "job": job,
            "items": items,
            "summary": {
                "total": total,
                "done": done,
                "failed": failed,
                "skipped": skipped,
                "running": running,
                "finished": finished,
                "percent": percent,
            },
        }
    )

@app.route("/api/jobs/<int:job_id>/items")
def api_get_job_items(job_id):
    return jsonify(
        {
            "ok": True,
            "items": get_job_items(job_id),
        }
    )


@app.route("/api/jobs/<int:job_id>/stop", methods=["POST"])
def api_stop_job(job_id):
    job = get_job(job_id)

    if not job:
        return jsonify(
            {
                "ok": False,
                "message": "Job not found",
            }
        ), 404

    update_job_status(
        job_id=job_id,
        status="stopping",
        error_message="Stop requested by user",
    )

    return jsonify(
        {
            "ok": True,
            "message": "Stop requested",
            "job_id": job_id,
        }
    )


@app.route("/api/jobs/<int:job_id>/skew-results")
def api_get_job_skew_results(job_id):
    try:
        results = get_skew_results_by_job(job_id)
        summary = get_skew_summary_by_job(job_id)

        return jsonify(
            {
                "ok": True,
                "results": results,
                "summary": summary,
            }
        )
    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


@app.route("/api/jobs/latest/skew")
def api_get_latest_skew_job():
    job = get_latest_job("skew")

    if not job:
        return jsonify(
            {
                "ok": False,
                "message": "No skew job found",
            }
        ), 404

    return jsonify(
        {
            "ok": True,
            "job": job,
        }
    )


@app.route("/api/jobs/active")
def api_get_active_jobs():
    job_type = request.args.get("job_type")

    return jsonify(
        {
            "ok": True,
            "jobs": get_active_jobs(job_type),
        }
    )


@app.route("/api/skew-results/<int:result_id>/segments")
def api_get_skew_result_segments(result_id):
    try:
        data = get_skew_result_segments(result_id)

        if not data:
            return jsonify(
                {
                    "ok": False,
                    "message": "Skew result not found",
                }
            ), 404

        return jsonify(
            {
                "ok": True,
                "result": data["result"],
                "segments": data["segments"],
            }
        )

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


@app.route("/reorganize")
def reorganize_page():
    return redirect(url_for("maintenance_page"))


@app.route("/api/reorganize/start", methods=["POST"])
def api_start_reorganize():
    try:
        payload = request.get_json() or {}

        connection_id = payload.get("connection_id")
        selected_tables = payload.get("tables") or []

        if not connection_id:
            return jsonify(
                {
                    "ok": False,
                    "message": "connection_id is required",
                }
            ), 400

        if not selected_tables:
            return jsonify(
                {
                    "ok": False,
                    "message": "No tables selected",
                }
            ), 400

        expanded_tables = []

        for item in selected_tables:
            schema_name = item.get("schema")
            table_name = item.get("table")

            if not schema_name or not table_name:
                continue

            targets = get_reorganize_targets(
                connection_id=connection_id,
                schema_name=schema_name,
                table_name=table_name,
            )

            for target in targets:
                key = "{}.{}".format(
                    target["schema_name"],
                    target["table_name"],
                )

                expanded_tables.append(
                    {
                        "schema": target["schema_name"],
                        "table": target["table_name"],
                        "full_name": key,
                    }
                )

        unique_tables = []
        seen = set()

        for item in expanded_tables:
            key = "{}.{}".format(item["schema"], item["table"])

            if key in seen:
                continue

            seen.add(key)
            unique_tables.append(item)

        if not unique_tables:
            return jsonify(
                {
                    "ok": False,
                    "message": "No reorganize targets found",
                }
            ), 400  
      
        job_id = create_job(
            job_type="reorganize",
            connection_id=connection_id,
            config={
                "source": "web",
                "selected_tables": selected_tables,
                "expanded_tables": unique_tables,
            },
        )       

        create_job_items(
            job_id=job_id,
            items=[
                {
                    "schema_name": item["schema"],
                    "table_name": item["table"],
                    "action": "REORGANIZE",
                }
                for item in unique_tables
            ],
        )  

        #run_background_job(
        #    target=run_reorganize_job,
        #    args=(job_id,),
        #)

        threading.Thread(
            target=run_reorganize_job,
            args=(job_id,),
            daemon=True
        ).start()

        return jsonify(
            {
                "ok": True,
                "job_id": job_id,
                "total_items": len(unique_tables),
            }
        )

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500    


@app.route("/api/reorganize/recommendation", methods=["POST"])
def api_reorganize_recommendation():
    try:
        data = request.get_json(force=True)

        connection_id = data.get("connection_id")
        schema_name = data.get("schema_name")
        table_name = data.get("table_name")

        if not connection_id or not schema_name or not table_name:
            return jsonify(
                {
                    "ok": False,
                    "message": "connection_id, schema_name, table_name are required",
                }
            ), 400

        result = get_distribution_recommendation(
            connection_id=connection_id,
            schema_name=schema_name,
            table_name=table_name,
        )

        return jsonify(result)

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


@app.route("/api/reorganize/apply-distribution", methods=["POST"])
def api_reorganize_apply_distribution():
    try:
        data = request.get_json(force=True)

        connection_id = data.get("connection_id")
        schema_name = data.get("schema_name")
        table_name = data.get("table_name")
        distribution_type = data.get("distribution_type")
        columns = data.get("columns") or []

        if not connection_id or not schema_name or not table_name or not distribution_type:
            return jsonify(
                {
                    "ok": False,
                    "message": "connection_id, schema_name, table_name, distribution_type are required",
                }
            ), 400

        result = apply_distribution_and_reorganize(
            connection_id=connection_id,
            schema_name=schema_name,
            table_name=table_name,
            distribution_type=distribution_type,
            columns=columns,
        )

        return jsonify(result)

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500

@app.route("/maintenance")
def maintenance_page():
    connections = list_connections()

    last_results = get_last_skew_results(limit=1000)
    problem_skew_results = get_latest_problem_skew_results(limit=500)

    latest_skew_job = get_latest_job("skew")
    latest_reorganize_job = get_latest_job("reorganize")

    return render_template(
        "maintenance.html",
        connections=connections,
        last_results=last_results,
        problem_skew_results=problem_skew_results,
        latest_skew_job=latest_skew_job,
        latest_reorganize_job=latest_reorganize_job,
    )

def build_skew_dashboard_summary(results):
    summary = {
        "total": 0,
        "ok": 0,
        "warning": 0,
        "critical": 0,
        "empty": 0,
        "failed": 0,
        "interrupted": 0,
        "max_skew": 0,
        "avg_skew": 0,
    }

    if not results:
        return summary

    total_skew = 0

    for r in results:
        summary["total"] += 1

        status = str(r.get("status") or "").upper()

        if status == "OK":
            summary["ok"] += 1
        elif status == "WARNING":
            summary["warning"] += 1
        elif status == "CRITICAL":
            summary["critical"] += 1
        elif status == "EMPTY":
            summary["empty"] += 1
        elif status == "FAILED":
            summary["failed"] += 1
        elif status == "INTERRUPTED":
            summary["interrupted"] += 1

        skew = float(r.get("skew_ratio") or 0)
        total_skew += skew

        if skew > summary["max_skew"]:
            summary["max_skew"] = skew

    summary["avg_skew"] = round(total_skew / summary["total"], 4)

    return summary

@app.route("/vacuum")
def vacuum_page():
    connections = list_connections()

    latest_vacuum_job = get_latest_job("vacuum_analyze")

    return render_template(
        "vacuum.html",
        connections=connections,
        latest_vacuum_job=latest_vacuum_job,
    )

@app.route("/api/vacuum/start", methods=["POST"])
def api_vacuum_start():
    data = request.get_json(silent=True) or {}

    connection_id = data.get("connection_id")
    selected_tables = data.get("tables") or []
    action = data.get("action") or "VACUUM_ANALYZE"

    if not connection_id:
        return jsonify(
            {
                "ok": False,
                "message": "connection_id is required",
            }
        ), 400

    if not selected_tables:
        return jsonify(
            {
                "ok": False,
                "message": "Не выбраны таблицы",
            }
        ), 400

    allowed_actions = [
        "VACUUM",
        "VACUUM_FULL",
        "ANALYZE",
        "VACUUM_ANALYZE",
        "VACUUM_FREEZE",
    ]

    action = str(action).upper().strip()

    if action not in allowed_actions:
        return jsonify(
            {
                "ok": False,
                "message": "Unknown action: {}".format(action),
            }
        ), 400

    try:
        unique_tables = []
        seen = set()

        for item in selected_tables:
            schema_name = None
            table_name = None

            if isinstance(item, dict):
                schema_name = item.get("schema") or item.get("schema_name")
                table_name = item.get("table") or item.get("table_name")

            elif isinstance(item, str):
                value = item.strip()

                if "." in value:
                    parts = value.split(".", 1)
                    schema_name = parts[0].strip().strip('"')
                    table_name = parts[1].strip().strip('"')

            if not schema_name or not table_name:
                continue

            key = "{}.{}".format(schema_name, table_name)

            if key in seen:
                continue

            seen.add(key)

            unique_tables.append(
                {
                    "schema": schema_name,
                    "table": table_name,
                }
            )

        if not unique_tables:
            return jsonify(
                {
                    "ok": False,
                    "message": "Нет корректных таблиц для запуска",
                }
            ), 400

        job_id = create_job(
            job_type="vacuum_analyze",
            connection_id=int(connection_id),
            config={
                "source": "web",
                "action": action,
                "tables": unique_tables,
            },
        )

        threading.Thread(
            target=run_vacuum_analyze_job,
            args=(job_id,),
            daemon=True,
        ).start()

        return jsonify(
            {
                "ok": True,
                "job_id": job_id,
                "total_items": len(unique_tables),
                "action": action,
            }
        )

    except Exception as e:
        return jsonify(
            {
                "ok": False,
                "message": str(e),
            }
        ), 500


def update_job_status(job_id, status, error_message=None):
    with sqlite_cursor(commit=True) as cur:
        if error_message is not None:
            cur.execute(
                """
                UPDATE jobs
                SET status = ?,
                    error_message = ?
                WHERE id = ?
                """,
                (
                    status,
                    str(error_message),
                    job_id,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE jobs
                SET status = ?
                WHERE id = ?
                """,
                (
                    status,
                    job_id,
                ),
            )


@app.route("/gpcopy")
def gpcopy_page():
    connections = list_connections()

    return render_template(
        "gpcopy.html",
        connections=connections,
    )


@app.route("/api/gpcopy/start", methods=["POST"])
def api_gpcopy_start():
    data = request.get_json(silent=True) or {}

    source_connection_id = data.get("source_connection_id")
    dest_connection_id = data.get("dest_connection_id")
    selected_tables = data.get("tables") or []

    gpcopy_path = data.get("gpcopy_path") or "/usr/local/gpdb/greenplum-db/bin/gpcopy"
    jobs = data.get("jobs") or 4
    on_segment_threshold = data.get("on_segment_threshold")
    extra_args = data.get("extra_args") or ""

    target_schema = data.get("target_schema") or ""
    target_table_mode = data.get("target_table_mode") or "same"

    truncate = bool(data.get("truncate"))
    drop = bool(data.get("drop"))
    append = bool(data.get("append"))
    skip_existing = bool(data.get("skip_existing"))
    analyze = bool(data.get("analyze"))
    dry_run = bool(data.get("dry_run"))
    validate_count = bool(data.get("validate_count"))

    if not source_connection_id:
        return jsonify({
            "ok": False,
            "message": "source_connection_id is required",
        }), 400

    if not dest_connection_id:
        return jsonify({
            "ok": False,
            "message": "dest_connection_id is required",
        }), 400

    if not selected_tables:
        return jsonify({
            "ok": False,
            "message": "Не выбраны таблицы",
        }), 400

    try:
        source_connection_id = int(source_connection_id)
        dest_connection_id = int(dest_connection_id)
    except Exception:
        return jsonify({
            "ok": False,
            "message": "connection_id должен быть числом",
        }), 400

    # ---------------------------------------------------------
    # ВАЖНО:
    # Убираем дубли выбранных таблиц.
    # Иногда frontend отправляет одну и ту же таблицу 2 раза:
    # например при выборе schema checkbox + table checkbox.
    # ---------------------------------------------------------
    unique_tables = []
    seen = set()

    for item in selected_tables:
        # На случай если frontend отправил строку "schema.table"
        if isinstance(item, str):
            parts = item.split(".", 1)
            if len(parts) != 2:
                continue

            schema_name = parts[0].strip()
            table_name = parts[1].strip()
        else:
            schema_name = item.get("schema") or item.get("schema_name")
            table_name = item.get("table") or item.get("table_name")

        if not schema_name or not table_name:
            continue

        key = (schema_name, table_name)

        if key in seen:
            continue

        seen.add(key)

        unique_tables.append({
            "schema": schema_name,
            "table": table_name,
        })

    if not unique_tables:
        return jsonify({
            "ok": False,
            "message": "После очистки дублей не осталось таблиц для gpcopy",
        }), 400

    try:
        config = {
            "source_connection_id": source_connection_id,
            "dest_connection_id": dest_connection_id,
            "selected_tables": selected_tables,
            "expanded_tables": unique_tables,

            "gpcopy_path": gpcopy_path,
            "jobs": int(jobs),
            "on_segment_threshold": on_segment_threshold,
            "extra_args": extra_args,

            "target_schema": target_schema,
            "target_table_mode": target_table_mode,

            "truncate": truncate,
            "drop": drop,
            "append": append,
            "skip_existing": skip_existing,
            "analyze": analyze,
            "dry_run": dry_run,
            "validate_count": validate_count,
        }

        job_id = create_job(
            job_type="gpcopy",
            connection_id=source_connection_id,
            config=config,
        )

        create_job_items(
            job_id=job_id,
            items=[
                {
                    "schema_name": item["schema"],
                    "table_name": item["table"],
                    "action": "GPCOPY",
                }
                for item in unique_tables
            ],
        )

        threading.Thread(
            target=run_gpcopy_job,
            args=(job_id,),
            daemon=True,
        ).start()

        return jsonify({
            "ok": True,
            "job_id": job_id,
            "total_items": len(unique_tables),
            "message": "gpcopy job started",
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "message": str(e),
        }), 500

    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 500

@app.route("/api/dashboard/session-limits")
def api_dashboard_session_limits():
    connection_id = request.args.get("connection_id", type=int)

    if not connection_id:
        return jsonify({
            "ok": False,
            "message": "connection_id is required",
        }), 400

    try:
        data = get_session_limits_stats(connection_id)

        return jsonify({
            "ok": True,
            "summary": data["summary"],
            "rows": data["rows"],
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "message": str(e),
        }), 500

    except Exception as e:
        return jsonify({
            "ok": False,
            "message": str(e),
        }), 500

@app.route("/api/gpcopy/date-columns")
def api_gpcopy_date_columns():
    connection_id = request.args.get("connection_id", type=int)
    schema_name = request.args.get("schema")
    table_name = request.args.get("table")

    if not connection_id:
        return jsonify({
            "ok": False,
            "message": "connection_id обязателен",
        }), 400

    if not schema_name or not table_name:
        return jsonify({
            "ok": False,
            "message": "schema и table обязательны",
        }), 400

    try:
        columns = get_date_columns_for_table(
            connection_id=connection_id,
            schema_name=schema_name,
            table_name=table_name,
        )

        return jsonify({
            "ok": True,
            "columns": columns,
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "message": str(e),
        }), 500

@app.route("/api/gpcopy/preview-date-json", methods=["POST"])
def api_gpcopy_preview_date_json():
    data = request.get_json(silent=True) or {}

    try:
        include_json = build_gpcopy_date_include_json_preview(data)

        return jsonify({
            "ok": True,
            "include_json": include_json,
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "message": str(e),
        }), 400

@app.route("/api/gpcopy/start-date", methods=["POST"])
def api_gpcopy_start_date():
    payload = request.get_json(silent=True) or {}

    source_connection_id = payload.get("source_connection_id")
    dest_connection_id = payload.get("dest_connection_id")
    table_configs = payload.get("table_configs") or []

    if not source_connection_id:
        return jsonify({"ok": False, "message": "source_connection_id is required"}), 400

    if not dest_connection_id:
        return jsonify({"ok": False, "message": "dest_connection_id is required"}), 400

    if not table_configs:
        return jsonify({"ok": False, "message": "table_configs is empty"}), 400

    for item in table_configs:
        if not item.get("source_schema"):
            return jsonify({"ok": False, "message": "source_schema is required"}), 400

        if not item.get("source_table"):
            return jsonify({"ok": False, "message": "source_table is required"}), 400

        if not item.get("date_column"):
            return jsonify({"ok": False, "message": "date_column is required"}), 400

        if not item.get("date_from"):
            return jsonify({"ok": False, "message": "date_from is required"}), 400

        if not item.get("date_to"):
            return jsonify({"ok": False, "message": "date_to is required"}), 400

    job_id = create_job("gpcopy_date", "queued")

    create_job_items(
        job_id,
        [
            {
                "schema_name": item["source_schema"],
                "table_name": item["source_table"],
                "action": "GPCOPY_DATE",
                "status": "queued",
            }
            for item in table_configs
        ],
    )

    thread = threading.Thread(
        target=run_gpcopy_job,
        kwargs={
            "job_id": job_id,
            "payload": payload,
            "mode": "date",
        },
        daemon=True,
    )
    thread.start()

    return jsonify(
        {
            "ok": True,
            "job_id": job_id,
            "message": f"GPCOPY by date запущен. Job #{job_id}",
        }
    )


if __name__ == "__main__":
    init_db()
    
    interrupted_jobs = mark_interrupted_jobs_on_startup()

    if interrupted_jobs:
        print("Interrupted jobs after application startup:", interrupted_jobs)

    app.run(
        host=APP_HOST,
        port=APP_PORT,
        debug=APP_DEBUG,
        use_reloader=False,
    )
