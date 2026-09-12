# -*- coding: utf-8 -*-
"""
Переподхват in-process задач после рестарта GPManager.

gpcopy/gpbackup/gprestore — внешние бинари, они переживают рестарт сами
(pid + лог, см. gpcopy.resume_unfinished_gpcopy_jobs). Задачи же, которые
работают внутри процесса (vacuum, reorganize, skew, sync, copy_pipe),
рестарт обрывает — но статус каждой строки лежит в SQLite, поэтому при
старте мы возвращаем прерванные строки в очередь и перезапускаем раннер:
готовые строки он пропускает, работа продолжается с места остановки.

Почему это безопасно по типам:
- vacuum_analyze / reorganize / skew — операции идемпотентны;
- gpcopy_sync — upsert через staging, повтор безопасен;
- gpcopy_increment — watermark пересчитывается от приёмника, дублей нет;
- gpcopy_partition_diff — diff пересчитывается, совпавшие партиции отпадут;
- copy_pipe — только truncate-режим (append задвоил бы строки).
"""

import json
import threading

from job_manager import list_unfinished_jobs, requeue_interrupted_items


RESUMABLE_JOB_TYPES = (
    "vacuum_analyze",
    "reorganize",
    "skew",
    "gpcopy_sync",
    "gpcopy_increment",
    "gpcopy_partition_diff",
    "copy_pipe",
)


def _load_runners():
    from modules.gpcopy_increment import run_gpcopy_increment_job
    from modules.gpcopy_partition import run_gpcopy_partition_diff_job
    from modules.gpcopy_sync import run_gpcopy_sync_job
    from modules.reorganize import run_reorganize_job
    from modules.skew_analyzer import run_skew_job
    from modules.sync_transport import run_copy_pipe_job
    from modules.vacuum_analyze import run_vacuum_analyze_job

    return {
        "vacuum_analyze": run_vacuum_analyze_job,
        "reorganize": run_reorganize_job,
        "skew": run_skew_job,
        "gpcopy_sync": run_gpcopy_sync_job,
        "gpcopy_increment": run_gpcopy_increment_job,
        "gpcopy_partition_diff": run_gpcopy_partition_diff_job,
        "copy_pipe": run_copy_pipe_job,
    }


def resume_inprocess_jobs(exclude_ids=None, runners=None):
    """
    Возвращает список job_id, которые переподхвачены (их не надо
    помечать interrupted). runners — для тестов.
    """
    handled = []
    exclude = set(int(i) for i in (exclude_ids or []))

    try:
        unfinished = list_unfinished_jobs()
    except Exception:
        return handled

    if runners is None:
        runners = _load_runners()

    for job in unfinished:
        job_id = int(job["id"])
        job_type = job.get("job_type")

        if job_id in exclude or job_type not in RESUMABLE_JOB_TYPES:
            continue

        runner = runners.get(job_type)

        if runner is None:
            continue

        if job_type == "copy_pipe":
            try:
                config = json.loads(job.get("config_json") or "{}")
            except Exception:
                config = {}

            # append без truncate при повторе задвоит строки — не подхватываем
            if config.get("append") and not config.get("truncate"):
                continue

        try:
            requeue_interrupted_items(job_id)
        except Exception:
            continue

        threading.Thread(
            target=runner, args=(job_id,), daemon=True,
        ).start()
        handled.append(job_id)

    return handled
