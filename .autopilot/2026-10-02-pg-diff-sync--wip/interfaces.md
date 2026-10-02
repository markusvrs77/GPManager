# Интерфейсы

## Правила проекта (не выводятся из кода)

- Python 3.11, Flask, psycopg2, SQLite (метаданные/задачи), vanilla JS, Bootstrap. Без новых зависимостей: недостающая зависимость → `BLOCKED`, не установка.
- Корень репозитория: `C:/project/GPManager/.claude/worktrees/pg-diff-sync` (git worktree, ветка `claude/pg-diff-sync`). Работать только в нём.
- Проверки после правок Python: `python -m py_compile app.py db.py job_manager.py modules/*.py` и `python -m pytest -q` (pytest в requirements-dev.txt). Весь набор тестов должен остаться зелёным — это и есть проверка R04.
- Не трогать и не коммитить: `instance/*.sqlite3`, креды, пароли, `.env`.
- Не выдумывать функции `job_manager.py`, `db.py` и др. — сверять сигнатуры перед вызовом. Не предполагать колонки SQLite — сверять с `db.py` / `PRAGMA table_info`.
- Глобальные JS-функции и id DOM нового режима — с префиксом `pgcmp`. Не дублировать глобальные имена.
- Не оставлять заглушки, выдающие себя за готовое. job item → `done` только после реально завершённой операции (после коммита).
- Стоп: обновить состояние задачи; отменить активный запрос/COPY psycopg2 (`conn.cancel()`); откатить незавершённую транзакцию приёмника.
- Никаких изменений в источнике: соединение источника read-only. Меняется только приёмник. DELETE — только при явном `delete_missing=true`.
- Перед INSERT/UPDATE/DELETE: валидировать ключевые колонки; проверять дубликаты ключа в staging; превью-числа (из сравнения); транзакция приёмника.
- Идентификаторы SQL — только `psycopg2.sql.Identifier`; пользовательский ввод не склеивать в идентификаторы, имена сверять с каталогом.
- Существующие маршруты, функции и UI режима «Перенос» не меняют поведения и контракта (R04). Разрешённые аддитивные правки — спецификация §13.
- Новые маршруты обязаны попасть в `POLICY` (modules/web_auth.py) — иначе 403 и красный `tests/test_web_auth_policy.py`.
- Тесты: фейки + monkeypatch + временная SQLite из `tests/conftest.py`; живого PostgreSQL нет. Образец раннер-теста: `tests/test_copy_pipe_runner.py`.
- Версии статики поднимаются вручную `?v=N` в шаблоне.
- Комментарии и тексты UI — по-русски, в стиле окружающего кода.

## Границы, решённые в спецификации

| Модуль | Владеет | Выставляет | Прячет |
|---|---|---|---|
| `modules/pg_sync_common.py` | общие примитивы PG↔PG | `open_pg(connection_id, readonly=False) -> conn` (проверяет `db_type=='postgres'`, нормализует сессию); `table_columns(conn, schema, table) -> [name,...]`; `row_hash_sql(alias, columns) -> sql.Composable`; `stream_copy(src_conn, dst_conn, select_sql, dst_table, dst_columns) -> rows`; `StopWatch(job_id, conns)` — контекст-менеджер, `.stopped` | SET нормализации, `os.pipe` + поток чтения, опрос стопа |
| `modules/pg_compare.py` | сравнение и его результаты | `expand_selection(src_conn, dst_conn, schemas, tables) -> [{schema, table, in_src, in_dst}]`; `compare_table(src_conn, dst_conn, schema, table, key_columns) -> {status, src_rows, dst_rows, to_insert, to_update, to_delete, message}`; `run_pg_compare_job(job_id)`; `save_result(job_id, row)`; `get_results(job_id) -> [dict]`; `latest_compare_job(src_id, dst_id) -> dict or None` | SQL сравнения, временную таблицу, резолв ключей |
| `modules/pg_diff_load.py` | применение | `load_diff(src_conn, dst_conn, schema, table, key_columns, delete_missing, stage_name) -> {insert, update, delete}`; `load_full(src_conn, dst_conn, schema, table, truncate) -> {rows}`; `run_pg_diff_load_job(job_id)` | staging, порядок DML, транзакции |
| `app.py` | HTTP-контракт | `POST /api/pg/compare/start` `{source_connection_id, dest_connection_id, schemas:[...], tables:[{schema,table}]}` → `{job_id}`; `GET /api/pg/compare/results?job_id=` → `{job, results}`; `GET /api/pg/compare/latest?source_connection_id=&dest_connection_id=` → `{job, results}` или `{job:null}`; `POST /api/pg/diff-load/start` `{source_connection_id, dest_connection_id, compare_job_id (необязателен; без него допустим только action='full'), delete_missing, tables:[{schema, table, action:'diff' or 'full' or 'create'}]}` → `{job_id}`. Дерево объектов — существующий маршрут каталога | валидацию, раскрытие выбора |
| `static/js/pg_compare.js` | UI режима | глобальные функции `pgcmp*` | состояние выбора, опрос |

Config задачи `pg_diff_load` содержит `tables` (с `action` и `key_columns` из результата сравнения), `expected` (`to_insert`/`to_update`/`to_delete` по таблице), `delete_missing` и `compare_job_id`.

**Швы для тестов:**
- SQL-построители `compare_table` и `load_diff` / `load_full` проверяются как чистые функции.
- Раннеры проверяются на фейковых соединениях и временной SQLite (по образцу `tests/test_copy_pipe_runner.py`).
- Маршруты проверяются через `client` из `tests/conftest.py`.

Живого PostgreSQL в тестах нет.

## Построено тасками

(дополняется по мере приёмки тасков)
