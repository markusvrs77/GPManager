window.STATE =
{
  "slug": "pg-diff-sync",
  "dir": "2026-10-02-pg-diff-sync--wip",
  "title": "Postgres: сравнение двух баз и перенос только разницы",
  "mode": "semi",
  "depth": "normal",
  "polish": null,
  "tier": "T1",
  "briefFile": "2026-10-02-brief.md",
  "memoryFile": "AGENTS.md",
  "skillDir": "C:/Users/Meirbekk/.claude/skills/autopilot",
  "startedAt": "2026-10-02T13:05:28+05:00",
  "updatedAt": "2026-10-02T13:53:12+05:00",
  "finishedAt": null,
  "stages": [
    {
      "id": "preflight",
      "status": "done",
      "startedAt": "2026-10-02T13:05:28+05:00",
      "finishedAt": "2026-10-02T13:06:30+05:00"
    },
    {
      "id": "manifest",
      "status": "done",
      "startedAt": "2026-10-02T13:06:30+05:00",
      "finishedAt": "2026-10-02T13:12:00+05:00"
    },
    {
      "id": "briefing",
      "status": "done",
      "startedAt": "2026-10-02T13:12:00+05:00",
      "finishedAt": "2026-10-02T13:20:40+05:00"
    },
    {
      "id": "spec",
      "status": "done",
      "startedAt": "2026-10-02T13:20:40+05:00",
      "finishedAt": "2026-10-02T13:25:00+05:00"
    },
    {
      "id": "plan",
      "status": "done",
      "startedAt": "2026-10-02T13:25:00+05:00",
      "note": "3 таска, ярус T1",
      "finishedAt": "2026-10-02T13:30:28+05:00"
    },
    {
      "id": "build",
      "status": "active",
      "startedAt": "2026-10-02T13:30:28+05:00"
    },
    {
      "id": "review",
      "status": "active",
      "startedAt": "2026-10-02T13:43:27+05:00"
    },
    {
      "id": "final",
      "status": "pending"
    }
  ],
  "requirements": {
    "total": 9,
    "done": 0,
    "inTicket": 9,
    "inSpec": 0,
    "placeholder": 0,
    "deferred": 0,
    "dropped": 0
  },
  "tickets": [
    {
      "id": "01",
      "title": "Сравнение баз: бэкенд и общие примитивы",
      "requirements": [
        "R02",
        "R03.1",
        "R05i",
        "R06i.1",
        "R07i"
      ],
      "blockedBy": [],
      "wave": 1,
      "zone": [
        "modules/pg_sync_common.py",
        "modules/pg_compare.py",
        "db.py",
        "app.py",
        "modules/web_auth.py"
      ],
      "status": "repair",
      "retries": 0,
      "repairs": 1,
      "handoffs": 0,
      "startedAt": "2026-10-02T13:30:38+05:00",
      "repairFindings": [
        "ключ из nullable/частичного уник. индекса; дубли ключа в приёмнике; типы колонок в structure_diff; стоп между запросами; stream_copy теряет ошибку приёмника; temp-таблица после неудачного rollback"
      ]
    },
    {
      "id": "02",
      "title": "Загрузка разницы, полная, создать и залить",
      "requirements": [
        "R03",
        "R06i",
        "R07i",
        "G01",
        "G02"
      ],
      "blockedBy": [
        "01"
      ],
      "wave": 2,
      "zone": [
        "modules/pg_diff_load.py",
        "app.py",
        "modules/web_auth.py"
      ],
      "status": "pending",
      "retries": 0,
      "repairs": 0,
      "handoffs": 0
    },
    {
      "id": "03",
      "title": "Режим «Сравнение и разница» во вкладке",
      "requirements": [
        "R01",
        "R02",
        "R04",
        "R05i",
        "G01",
        "G02"
      ],
      "blockedBy": [
        "01"
      ],
      "wave": 2,
      "zone": [
        "templates/gpcopy_pipeline.html",
        "static/js/pg_compare.js"
      ],
      "status": "pending",
      "retries": 0,
      "repairs": 0,
      "handoffs": 0
    }
  ],
  "singlePass": null,
  "tests": null,
  "debt": {
    "placeholders": [],
    "assumptions": [],
    "emptyEnv": []
  },
  "additions": [],
  "coverage": {
    "findings": 4,
    "fixed": 3,
    "deferred": 1,
    "note": "3 полупокрытия дописаны (галка «создать и залить», no_dest для отдельных таблиц, полная загрузка без сравнения); передача по сети только разницы — Вне рамок"
  },
  "concerns": [
    "pg_compare.py:62-111 — Reinvention: запрос pg_inherits/r-p отношений дублирует table_catalog",
    "pg_compare.py:520 — table_columns читается дважды на таблицу",
    "pg_compare.py:481 — latest_compare_job сканирует все задачи pg_compare",
    "app.py:1855 — /latest не фильтрует job_in_scope как /results (доступ к подключениям проверен)",
    "tests/test_pg_compare.py:124 — SQL подсчёта проверяется по подстрокам",
    "tests/test_pg_sync_common.py:258 — слабая проверка завершения потока",
    "pg_compare.py:446 — valid_unique_keys требует PG11+ (indnkeyatts); на PG10 таблица с кандидатом unique_index получит error",
    "pg_compare.py:483 — уникальный индекс с INCLUDE никогда не становится ключом (несовпадение наборов) — безопасный откат к sync_keys/без ключа",
    "pg_compare.py:738 — при неудачном переоткрытии соединения текущая таблица остаётся без строки результата",
    "pg_compare.py:396 — неудачный rollback в compare_table обнаруживается только на следующей таблице",
    "tests — ветка зависшего pump в stream_copy не покрыта"
  ],
  "reviewers": {
    "manifestSpec": "ae5a310f94189b2ce",
    "craft": "af0fb57f8090eb9c4"
  },
  "blind": null
}
