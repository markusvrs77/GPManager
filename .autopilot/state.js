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
  "updatedAt": "2026-10-02T13:30:38+05:00",
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
      "status": "pending"
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
      "status": "in-progress",
      "retries": 0,
      "repairs": 0,
      "handoffs": 0,
      "startedAt": "2026-10-02T13:30:38+05:00"
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
  "concerns": [],
  "reviewers": {
    "manifestSpec": null,
    "craft": null
  },
  "blind": null
}
