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
  "updatedAt": "2026-10-02T14:48:40+05:00",
  "finishedAt": "2026-10-02T14:48:40+05:00",
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
      "status": "done",
      "startedAt": "2026-10-02T13:30:28+05:00",
      "note": "5 из 5 тасков",
      "finishedAt": "2026-10-02T14:48:40+05:00"
    },
    {
      "id": "review",
      "status": "done",
      "startedAt": "2026-10-02T13:43:27+05:00",
      "note": "проверено 5 из 5",
      "finishedAt": "2026-10-02T14:48:40+05:00"
    },
    {
      "id": "final",
      "status": "done",
      "startedAt": "2026-10-02T14:18:12+05:00",
      "note": "слепая приёмка на живом PG 17; 1 расхождение исправлено",
      "finishedAt": "2026-10-02T14:48:40+05:00"
    }
  ],
  "requirements": {
    "total": 10,
    "done": 10,
    "inTicket": 0,
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
      "status": "done",
      "retries": 0,
      "repairs": 1,
      "handoffs": 0,
      "startedAt": "2026-10-02T13:30:38+05:00",
      "repairFindings": [
        "ключ из nullable/частичного уник. индекса; дубли ключа в приёмнике; типы колонок в structure_diff; стоп между запросами; stream_copy теряет ошибку приёмника; temp-таблица после неудачного rollback"
      ],
      "finishedAt": "2026-10-02T13:53:20+05:00",
      "commit": "e0ba298",
      "tests": {
        "passed": 599,
        "failed": 0
      },
      "files": [
        "modules/pg_sync_common.py",
        "modules/pg_compare.py",
        "db.py",
        "app.py",
        "modules/web_auth.py"
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
      "status": "done",
      "retries": 0,
      "repairs": 2,
      "handoffs": 0,
      "startedAt": "2026-10-02T13:53:20+05:00",
      "repairFindings": [
        "identity ALWAYS / generated-колонки ломают INSERT/UPDATE/COPY",
        "setval последовательностей; create на непустой таблице; NULL-ключ при delete_missing; порядок done/итог; ANALYZE staging; keyless INSERT без ctid-join; статус сравнения"
      ],
      "finishedAt": "2026-10-02T14:18:12+05:00",
      "commit": "a4dc7d6",
      "tests": {
        "passed": 669,
        "failed": 0
      },
      "files": [
        "modules/pg_diff_load.py",
        "app.py",
        "modules/web_auth.py"
      ]
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
      "status": "done",
      "retries": 0,
      "repairs": 1,
      "handoffs": 0,
      "startedAt": "2026-10-02T13:53:20+05:00",
      "repairFindings": [
        "гонка start/смена пары → запись не в подтверждённый приёмник; опрос загрузки задваивается и не стоп на 404; «сделано» без чисел; листья партиций в полной загрузке; мёртвые экспорты; политика gpConfirm"
      ],
      "finishedAt": "2026-10-02T14:09:39+05:00",
      "commit": "2599b42",
      "tests": {
        "passed": 655,
        "failed": 0
      },
      "files": [
        "templates/gpcopy_pipeline.html",
        "static/js/pg_compare.js",
        "static/js/gpcopy_pipeline.js"
      ]
    },
    {
      "id": "04",
      "title": "«Создать и залить» на настоящем PostgreSQL",
      "requirements": [
        "G02",
        "D01"
      ],
      "blockedBy": [
        "02"
      ],
      "wave": 3,
      "zone": [
        "modules/pg_diff_load.py"
      ],
      "status": "done",
      "startedAt": "2026-10-02T14:26:06+05:00",
      "retries": 0,
      "repairs": 1,
      "handoffs": 0,
      "repairFindings": [
        "уборка staging трогает stopping-задачи; search_path при чтении DDL; COLLATE; virtual generated"
      ],
      "finishedAt": "2026-10-02T14:40:18+05:00",
      "commit": "d7e2b29",
      "tests": {
        "passed": 682,
        "failed": 0
      },
      "files": [
        "modules/pg_diff_load.py"
      ]
    },
    {
      "id": "05",
      "title": "Доводка интерфейса по отложенным замечаниям",
      "requirements": [
        "R05i",
        "R07i",
        "R03.3"
      ],
      "blockedBy": [
        "03",
        "04"
      ],
      "wave": 4,
      "zone": [
        "static/js/pg_compare.js"
      ],
      "status": "done",
      "startedAt": "2026-10-02T14:40:18+05:00",
      "retries": 0,
      "repairs": 0,
      "handoffs": 0,
      "finishedAt": "2026-10-02T14:48:40+05:00",
      "commit": "464fa89",
      "tests": {
        "passed": 683,
        "failed": 0
      },
      "files": [
        "static/js/pg_compare.js",
        "static/js/gpcopy_pipeline.js"
      ]
    }
  ],
  "singlePass": null,
  "tests": {
    "passed": 683,
    "failed": 0
  },
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
    "tests — ветка зависшего pump в stream_copy не покрыта",
    "pg_compare.js:670 — перерисовка таблицы при опросе закрывает открытый select",
    "pg_compare.js:67 — pgcmpEsc/pgcmpApi повторяют скрытые хелперы gpcopy_pipeline.js",
    "«Ожидалось» хранится только на клиенте — после перезагрузки страницы не видно (в config pg_diff_load оно есть)",
    "лента «Запуски» подписывает новые типы сырыми именами (RUN_LABELS)",
    "pg_compare.js:175 — переключение режима во время POST загрузки отключает её опрос на экране (сама загрузка идёт)",
    "pg_compare.js:906 — лист партиции с корнем в другой схеме не отсеивается в полной загрузке без сравнения (двойная работа, данные не портятся)",
    "pg_diff_load.py:444 — staging stg_<job>_* остаётся после падения процесса, уборки нет",
    "pg_diff_load.py:476 — _rollback/_reopen_broken/_cancel_rest скопированы из pg_compare (дублирование)",
    "tests/test_pg_diff_load_runner.py:148 — стоп-тест проходит через check() после исключения",
    "create_missing_objects открывает источник не в read-only (только читает DDL)",
    "путь без ключа проверен только по структуре SQL — нет живого PostgreSQL",
    "identity ALWAYS вне ключа не обновляется → факт update может быть меньше ожидания",
    "pg_diff_load.py:209 — setval при is_called=false после RESTART может сдвинуть последовательность назад (дубля не даёт)",
    "pg_diff_load.py:209 — без прав на последовательность предупреждение показывает сырой текст ошибки",
    "pg_diff_load.py:426 — тип/выражение из источника (enum, своя функция), которого нет в приёмнике → item failed (данные не страдают)",
    "pg_diff_load create — не переносятся default-ы, CHECK/FK и не-PK индексы (только типы, NOT NULL, PK)"
  ],
  "reviewers": {
    "manifestSpec": "ae5a310f94189b2ce",
    "craft": "af0fb57f8090eb9c4"
  },
  "blind": {
    "agreed": 8,
    "drift": [
      "G02 — «создать и залить» падал на PostgreSQL 17 → исправлено таском 04, перепроверено вживую"
    ],
    "live": "PG 17.4, src_db/dst_db; источник не изменился (md5 совпали)"
  }
}
