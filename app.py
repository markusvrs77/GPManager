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
from job_manager import (
    create_job,
    create_job_items,
    get_job,
    get_job_items,
    get_latest_job,
    set_stop_flag,
    #run_background_job,
    mark_interrupted_jobs_on_startup,
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

app = Flask(__name__)


@app.route("/")
def index():
    connections = list_connections()
    return render_template(
        "index.html",
        connections=connections,
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
    connections = list_connections()
    last_results  = get_last_skew_results(limit=1000)

    return render_template(
        "skew.html",
        connections=connections,
        last_results =last_results ,
    )


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
            "tables": tables,
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
    set_stop_flag(job_id)

    return jsonify(
        {
            "ok": True,
            "message": "Stop requested",
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
    connections = list_connections()
    problem_skew_results = get_latest_problem_skew_results(limit=500)

    return render_template(
        "reorganize.html",
        connections=connections,
        problem_skew_results=problem_skew_results,
    )


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
