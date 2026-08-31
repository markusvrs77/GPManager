# PostgreSQL Metadata Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace runtime SQLite metadata with PostgreSQL, introduce versioned migrations and repositories, and remove import-time application side effects.

**Architecture:** A dedicated metadata package owns SQLAlchemy engine creation, Alembic migrations, transaction boundaries, and repositories. Greenplum/PostgreSQL administration connections remain psycopg2 connections and never share the metadata engine. Flask is created through an application factory; initialization, recovery, and scheduler startup become explicit commands or services.

**Tech Stack:** Python 3.11, Flask, SQLAlchemy 2, Alembic, PostgreSQL, psycopg2, unittest.

## Global Constraints

- `GPMANAGER_METADATA_DSN` is mandatory outside tests; no password appears in logs.
- SQLite is accepted only by the later `migrate-sqlite` command.
- Preserve current repository function behavior while routes are migrated.
- Never import a function that has not been verified in the current module.
- Every schema change is represented by an Alembic revision and tested from an empty database.

---

### Task 1: Dependency and configuration contract

**Files:**
- Create: `pyproject.toml`
- Create: `gpmanager/__init__.py`
- Create: `gpmanager/settings.py`
- Create: `tests/test_settings.py`
- Modify: `requirements.txt`

**Interfaces:**
- Produces: `Settings.from_env(environ: Mapping[str, str]) -> Settings`
- Produces: `Settings.metadata_dsn_redacted() -> str`

- [ ] **Step 1: Write failing configuration tests**

```python
class SettingsTest(unittest.TestCase):
    def test_production_requires_metadata_dsn_and_secret_key(self):
        with self.assertRaisesRegex(ValueError, "GPMANAGER_METADATA_DSN"):
            Settings.from_env({"GPMANAGER_ENV": "production"})

    def test_redaction_removes_password(self):
        settings = Settings.from_env({
            "GPMANAGER_ENV": "production",
            "GPMANAGER_METADATA_DSN": "postgresql://user:secret@db/gpmanager",
            "GPMANAGER_SECRET_KEY": "x" * 32,
        })
        self.assertNotIn("secret", settings.metadata_dsn_redacted())
```

- [ ] **Step 2: Run the tests and confirm the missing module failure**

Run: `python -m unittest tests.test_settings -v`

Expected: FAIL because `gpmanager.settings` does not exist.

- [ ] **Step 3: Implement immutable settings**

```python
@dataclass(frozen=True)
class Settings:
    environment: str
    metadata_dsn: str
    secret_key: str
    scheduler_enabled: bool

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> "Settings":
        environment = environ.get("GPMANAGER_ENV", "development").strip().lower()
        dsn = environ.get("GPMANAGER_METADATA_DSN", "").strip()
        secret = environ.get("GPMANAGER_SECRET_KEY", "").strip()
        if not dsn:
            raise ValueError("GPMANAGER_METADATA_DSN is required")
        if environment == "production" and len(secret) < 32:
            raise ValueError("GPMANAGER_SECRET_KEY must contain at least 32 characters")
        return cls(environment, dsn, secret, False)
```

Add SQLAlchemy, Alembic, Argon2, Flask-WTF, and production WSGI dependencies to `pyproject.toml`; keep `requirements.txt` as a generated compatibility export, not the source of truth.

- [ ] **Step 4: Run tests**

Run: `python -m unittest tests.test_settings -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml requirements.txt gpmanager/__init__.py gpmanager/settings.py tests/test_settings.py
git commit -m "build: define production configuration contract"
```

### Task 2: PostgreSQL engine and transaction boundary

**Files:**
- Create: `gpmanager/metadata/__init__.py`
- Create: `gpmanager/metadata/database.py`
- Create: `tests/test_metadata_database.py`

**Interfaces:**
- Produces: `create_metadata_engine(settings: Settings) -> Engine`
- Produces: `session_factory(engine: Engine) -> sessionmaker[Session]`
- Produces: `SqlAlchemyUnitOfWork`

- [ ] **Step 1: Write failing rollback and redaction tests**

```python
def test_unit_of_work_rolls_back_on_error(metadata_session_factory):
    with self.assertRaises(RuntimeError):
        with SqlAlchemyUnitOfWork(metadata_session_factory) as uow:
            uow.session.execute(text("INSERT INTO test_probe(value) VALUES ('x')"))
            raise RuntimeError("stop")
    with metadata_session_factory() as session:
        count = session.scalar(text("SELECT COUNT(*) FROM test_probe"))
    self.assertEqual(count, 0)
```

- [ ] **Step 2: Run the test and verify failure**

Run: `python -m unittest tests.test_metadata_database -v`

Expected: FAIL because the database module is missing.

- [ ] **Step 3: Implement engine and unit of work**

```python
def create_metadata_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.metadata_dsn,
        pool_pre_ping=True,
        pool_recycle=300,
        future=True,
    )

class SqlAlchemyUnitOfWork:
    def __init__(self, factory):
        self._factory = factory

    def __enter__(self):
        self.session = self._factory()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.session.rollback() if exc_type else self.session.commit()
        finally:
            self.session.close()
```

- [ ] **Step 4: Run database tests against disposable PostgreSQL**

Run: `python -m unittest tests.test_metadata_database -v`

Expected: PASS with `GPMANAGER_TEST_METADATA_DSN` configured.

- [ ] **Step 5: Commit**

```bash
git add gpmanager/metadata tests/test_metadata_database.py
git commit -m "feat: add PostgreSQL metadata unit of work"
```

### Task 3: Complete Alembic schema

**Files:**
- Create: `alembic.ini`
- Create: `migrations/env.py`
- Create: `migrations/versions/0001_initial_metadata.py`
- Create: `gpmanager/metadata/models.py`
- Create: `tests/test_metadata_migrations.py`
- Reference: `db.py:33`

**Interfaces:**
- Produces tables: `connections`, `jobs`, `job_items`, `skew_results`, `skew_result_segments`, `gpcopy_plans`, `gpcopy_plan_items`, `gpcopy_target_reservations`, `schedules`, `schedule_runs`
- Reserves tables for Phase 2: `users`, `roles`, `user_roles`, `connection_permissions`, `sessions`, `audit_events`

- [ ] **Step 1: Write failing empty-database migration test**

```python
def test_upgrade_head_creates_complete_schema(self):
    command.upgrade(self.alembic_config, "head")
    names = set(inspect(self.engine).get_table_names())
    self.assertTrue({"connections", "jobs", "job_items", "gpcopy_plans", "schedules"} <= names)
```

- [ ] **Step 2: Run and verify failure**

Run: `python -m unittest tests.test_metadata_migrations -v`

Expected: FAIL because Alembic is not configured.

- [ ] **Step 3: Define models and initial revision**

Use PostgreSQL-native types for timestamps and JSON, explicit foreign keys, status check constraints, and a partial unique index for active target reservations. Define job claim fields:

```python
worker_id: Mapped[str | None] = mapped_column(String(64))
heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
```

Define connection safety fields:

```python
environment: Mapped[str] = mapped_column(String(16), nullable=False)
credential_envelope: Mapped[dict | None] = mapped_column(JSONB)
endpoint_fingerprint: Mapped[str | None] = mapped_column(String(128))
```

- [ ] **Step 4: Test upgrade, downgrade, and re-upgrade**

Run: `python -m unittest tests.test_metadata_migrations -v`

Expected: PASS for `upgrade head`, `downgrade base`, and a second `upgrade head`.

- [ ] **Step 5: Commit**

```bash
git add alembic.ini migrations gpmanager/metadata/models.py tests/test_metadata_migrations.py
git commit -m "feat: add versioned PostgreSQL metadata schema"
```

### Task 4: Repository adapters

**Files:**
- Create: `gpmanager/metadata/repositories/connections.py`
- Create: `gpmanager/metadata/repositories/jobs.py`
- Create: `gpmanager/metadata/repositories/plans.py`
- Create: `gpmanager/metadata/repositories/schedules.py`
- Create: `gpmanager/metadata/repositories/__init__.py`
- Create: `tests/test_metadata_repositories.py`
- Modify: `modules/connections.py`
- Modify: `job_manager.py`
- Modify: `modules/gpcopy_plan.py`
- Modify: `modules/scheduler_repository.py`

**Interfaces:**
- Produces repository methods matching verified existing public function signatures.
- Consumes: `SqlAlchemyUnitOfWork`

- [ ] **Step 1: Add contract tests for existing functions**

Test `create_job`, `get_job`, `create_job_items`, `request_stop_job`, plan consumption, target reservations, schedule claims, and connection listing against PostgreSQL. Assert JSON fields returned to callers remain decoded exactly as current callers expect.

- [ ] **Step 2: Run contract tests and verify SQLite-coupling failures**

Run: `python -m unittest tests.test_metadata_repositories -v`

Expected: FAIL while existing modules still call `sqlite_cursor`.

- [ ] **Step 3: Implement repositories with state-checked updates**

Job claims use:

```sql
SELECT id
FROM jobs
WHERE status = 'queued'
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT :limit
```

Every terminal transition includes an allowed previous-state predicate and increments `version`. Plan consumption, job creation, item creation, and target reservation remain one transaction.

- [ ] **Step 4: Replace SQLite internals without changing module APIs**

Keep verified function signatures in `job_manager.py`, `modules/gpcopy_plan.py`, and `modules/scheduler_repository.py`; delegate their bodies to repositories. Remove duplicate `get_connection_by_id` implementations and use one connection repository.

- [ ] **Step 5: Run repository and existing tests**

Run: `python -m unittest discover -s tests -v`

Expected: all existing and new tests PASS against metadata PostgreSQL.

- [ ] **Step 6: Commit**

```bash
git add gpmanager/metadata/repositories modules/connections.py job_manager.py modules/gpcopy_plan.py modules/scheduler_repository.py tests/test_metadata_repositories.py
git commit -m "refactor: route metadata access through PostgreSQL repositories"
```

### Task 5: Application factory and explicit lifecycle

**Files:**
- Create: `gpmanager/app_factory.py`
- Create: `gpmanager/cli.py`
- Create: `tests/test_app_factory.py`
- Modify: `app.py`
- Modify: `scheduler.py`

**Interfaces:**
- Produces: `create_app(settings: Settings | None = None) -> Flask`
- Produces CLI commands: `gpmanager migrate`, `gpmanager check-config`

- [ ] **Step 1: Write a no-side-effect import test**

```python
@patch("scheduler.start_scheduler")
@patch("job_manager.mark_interrupted_jobs_on_startup")
def test_create_app_does_not_start_runtime(self, recover, start):
    app = create_app(self.settings)
    self.assertIsNotNone(app)
    start.assert_not_called()
    recover.assert_not_called()
```

- [ ] **Step 2: Run and verify current import side effects fail the test**

Run: `python -m unittest tests.test_app_factory -v`

Expected: FAIL because `app.py` currently initializes storage and scheduler at import.

- [ ] **Step 3: Move construction into `create_app`**

Register blueprints and extensions inside the factory. Keep `app.py` as a compatibility WSGI shim only:

```python
from gpmanager.app_factory import create_app

app = create_app()
```

Do not call migrations, recovery, or scheduler from the shim.

- [ ] **Step 4: Add explicit CLI migration and configuration checks**

`gpmanager migrate` runs Alembic to `head`; `check-config` validates metadata connectivity without printing DSN credentials.

- [ ] **Step 5: Run application and test suites**

Run: `python -m unittest discover -s tests -v`

Expected: PASS and process exits cleanly without a scheduler thread.

- [ ] **Step 6: Commit**

```bash
git add gpmanager/app_factory.py gpmanager/cli.py app.py scheduler.py tests/test_app_factory.py
git commit -m "refactor: add explicit Flask application lifecycle"
```

### Task 6: Foundation verification

**Files:**
- Modify: `.github/workflows/python-package.yml`
- Create: `tests/test_clean_import.py`

- [ ] **Step 1: Add PostgreSQL service to CI and install from `pyproject.toml`**

Configure CI to migrate a disposable PostgreSQL database before tests. Do not use repository `venv` files.

- [ ] **Step 2: Run syntax and test checks**

Run:

```bash
python -m py_compile app.py db.py job_manager.py modules/*.py gpmanager/*.py gpmanager/metadata/*.py gpmanager/metadata/repositories/*.py
python -m unittest discover -s tests -v
alembic upgrade head
alembic downgrade base
alembic upgrade head
```

Expected: every command exits `0`.

- [ ] **Step 3: Verify SQLite is absent from runtime imports**

Run: `rg -n "sqlite3|sqlite_cursor|SQLITE_DB_PATH" app.py job_manager.py scheduler.py modules gpmanager -g '*.py'`

Expected: matches exist only in the future migration utility or explicitly marked compatibility tests.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/python-package.yml tests/test_clean_import.py
git commit -m "ci: verify PostgreSQL metadata foundation"
```

