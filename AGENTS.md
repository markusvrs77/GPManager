<!-- autopilot:start -->
# Opsentri

Flask-консоль администрирования Greenplum, PostgreSQL и Kafka для DBA: перенос и PG↔PG-синхронизация данных, обслуживание, резервные копии, гранты, расписания.

## Команды

| Команда | Что делает |
|---------|------------|
| `pip install -r requirements.txt -r requirements-dev.txt` | Установить зависимости |
| `python -m pytest -q` | Тесты (669 passed) |
| `python -m py_compile app.py db.py job_manager.py modules/*.py` | Синтаксис после правок Python (glob — в bash) |
| `python app.py` | Запуск на 0.0.0.0:8080 (не проверялось) |

## Структура

```
app.py                  — точка входа и большинство HTML/API-маршрутов (в т.ч. /api/pg/compare/*, /api/pg/diff-load/start)
auth_routes.py, users_routes.py, kafka_routes.py — блюпринты входа, пользователей, Kafka
config.py               — пути instance/ и logs/, хост/порт, SQLITE_DB_PATH
db.py                   — схема SQLite, init_db(), get_sqlite_connection()
job_manager.py          — задачи и job_items в SQLite: create_job, request_stop_job
scheduler.py, scheduler_store.py — планировщик расписаний
modules/                — логика: gpcopy*, pg_*, kafka_*, web_auth (POLICY), security
templates/              — Jinja-страницы; base.html — оболочка, gpcopy_pipeline.html — «Перенос» и PG-сравнение
static/js/, static/css/ — vanilla JS и стили по страницам; ui.js — общие хелперы (gpConfirm)
tests/                  — pytest; conftest.py (временная SQLite, client), pg_fakes.py
docs/superpowers/       — планы и спеки
instance/               — рабочая SQLite, не коммитить
```

## Подводные камни

- `init_db()`, переподхват задач и `start_scheduler()` вызываются только в `if __name__ == "__main__"` app.py: `flask run` их не выполнит.
- Путь к SQLite брать только через `db.get_sqlite_connection()` / `config.SQLITE_DB_PATH` в момент вызова: тесты подменяют атрибут на временный файл.
- `job_manager.create_job` сам заводит job_items по `config["tables"]` (action — из item или `config["item_action"]`); `create_job_items` для того же набора задвоит строки.
- Права проверяет один before_request по `POLICY` в modules/web_auth.py, ключ — имя endpoint, не URL; маршрута нет в карте → 403 и красный tests/test_web_auth_policy.py.
- Версии статики поднимаются вручную `?v=N` в шаблоне (pg_compare.js подключён в gpcopy_pipeline.html с `?v=1`).
- modules/pg_sync_common.py — общие примитивы PG↔PG: `open_pg`, `table_columns`, `row_hash_sql`, `stream_copy` (сам не коммитит и не откатывает), `StopWatch` (cancel() соединений при стопе).
- modules/pg_compare.py — сравнение по ключу (pk / valid unique / sync_keys) через `pg_temp.pgcmp_src`; результаты в SQLite, раннер `run_pg_compare_job`.
- modules/pg_diff_load.py — применение `diff` / `full` / `create`: `load_diff`, `load_full`, `run_pg_diff_load_job`; итог по таблице пишется в `error_message` item (`insert=N; update=M; delete=K`).
- Источник открывается `open_pg(id, readonly=True)` (`set_session(readonly=True)`); пишется только приёмник.
- DELETE в diff — только при `delete_missing is True` и для строк с ключом `IS NOT NULL`.
- Staging живёт в схеме `opsentri_sync_stage` приёмника (`STAGE_SCHEMA`), UNLOGGED-таблицы `stg_<job>_<n>`.
- Живого PostgreSQL в тестах нет: фейковые соединения из tests/pg_fakes.py (`render()` печатает Composable), образец раннер-теста — tests/test_copy_pipe_runner.py.
- JS нового режима — static/js/pg_compare.js (IIFE, в window ничего); id DOM `pgcmp*`, CSS-классы `pgcmp-*`.
- Путь к бинарю gpcopy — из запроса или переменной `GPCOPY_PATH`; Secure-cookie — `OPSENTRI_COOKIE_SECURE=1`.

## Как здесь работает Autopilot

Сборка ведётся навыком `/autopilot`. Требования, спецификация и таски — в `.autopilot/`.
Прогресс — `.autopilot/dashboard.html`. Правило: требование из `manifest.md`
может снять только пользователь.

Если работа продолжается — скажи «продолжи автопилот»: состояние поднимется
из `.autopilot/state.js`, переспрашивать ничего не нужно.
<!-- autopilot:end -->
