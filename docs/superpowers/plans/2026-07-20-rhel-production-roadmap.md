# GPManager RHEL Production Program Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the approved RHEL production architecture as five independently reviewable and deployable phases.

**Architecture:** PostgreSQL becomes the sole live metadata store; a Flask application factory serves authenticated versioned APIs; separate executor and scheduler services own asynchronous work; a Go agent provides licensing and credential cryptography; Nuitka and signed RPMs provide the RHEL distribution. Every phase preserves a runnable system and has an explicit rollback boundary.

**Tech Stack:** Python 3.11, Flask, SQLAlchemy 2, Alembic, PostgreSQL, psycopg2, Go, Nuitka, systemd, Nginx, SELinux, RPM.

## Global Constraints

- Never run destructive operations against the source/production database.
- Prod-to-Test synchronization may change only the destination database.
- DELETE synchronization requires explicit `delete_missing=true` and a consumed Preview plan.
- Validate identifiers, logical keys, duplicate keys, preview counts, and destination transactions before DML.
- A job item is `done` only after the real operation and post-validation complete.
- Stop updates metadata state, cancels SQL/COPY, terminates the external process tree, and rolls back incomplete destination work.
- Preserve existing API compatibility unless the approved versioned migration intentionally deprecates an unsafe endpoint.
- Do not store plaintext credentials, session secrets, license secrets, or preview tokens.
- Do not ship Python `.py` files in the RHEL package.

---

## Program sequence

### Phase 1: PostgreSQL foundation

Execute [PostgreSQL Foundation Plan](2026-07-20-postgresql-foundation.md).

Exit criteria:

- all metadata reads and writes use PostgreSQL repositories;
- importing the application has no startup side effects;
- Alembic owns the complete schema;
- existing unit tests and new repository tests pass;
- SQLite is not used at runtime.

### Phase 2: Authentication and web security

Execute [Authentication and Web Security Plan](2026-07-20-auth-web-security.md).

Exit criteria:

- every route has an explicit access policy;
- local users, Argon2id, RBAC, connection scopes, CSRF, safe sessions, rate limiting, and audit logging are enforced;
- client responses contain correlation IDs instead of tracebacks;
- XSS sinks and duplicate page globals are removed.

### Phase 3: Executor, scheduler, and database safety

Execute [Executor and Database Safety Plan](2026-07-20-executor-database-safety.md).

Exit criteria:

- web processes never execute long-running jobs;
- PostgreSQL job claims and heartbeats support multiple processes;
- source/destination endpoint fingerprints prevent alias bypass;
- all destructive workflows use Preview/Apply, reservations, target transactions, and post-validation;
- stop tests prove SQL/COPY cancellation, process-tree termination, and rollback.

### Phase 4: Go licensing and credential service

Execute [Go License Agent Plan](2026-07-20-go-license-agent.md).

Exit criteria:

- online lease, seven-day grace, signed offline license, read-only failure mode, and rehost workflow work;
- credential ciphertext is stored in PostgreSQL and decrypted only through the local agent;
- license failure never interrupts an in-flight target transaction;
- protocol and cryptographic test vectors pass in Python and Go.

### Phase 5: RHEL compiled distribution

Execute [RHEL Packaging and Release Plan](2026-07-20-rhel-packaging-release.md).

Exit criteria:

- clean RHEL installation contains no `.py` files;
- systemd, Nginx, SELinux, migrations, upgrade, rollback, backup, restore, and uninstall are tested;
- RPM signatures, integrity manifest, dependency scan, and SBOM pass;
- no Critical or High issue remains open.

## Cross-phase interface freeze

The following interfaces become compatibility boundaries after Phase 1:

```python
class MetadataUnitOfWork(Protocol):
    jobs: JobRepository
    plans: PlanRepository
    schedules: ScheduleRepository
    connections: ConnectionRepository
    audit: AuditRepository

    def __enter__(self) -> "MetadataUnitOfWork": ...
    def __exit__(self, exc_type, exc, tb) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
```

```python
class OperationAuthorizer(Protocol):
    def authorize(
        self,
        *,
        actor_id: int,
        action: str,
        connection_ids: tuple[int, ...],
        destructive: bool,
    ) -> None: ...
```

```python
class ExecutorControl(Protocol):
    def request_stop(self, job_id: int, actor_id: int) -> None: ...
    def active_runtime(self, job_id: int) -> dict | None: ...
```

```go
type LicenseService interface {
	Evaluate(ctx context.Context, req EvaluateRequest) (Decision, error)
	EncryptCredential(ctx context.Context, plaintext []byte) (Envelope, error)
	DecryptCredential(ctx context.Context, envelope Envelope) ([]byte, error)
}
```

## Program-level verification

- [ ] Run all Python unit and integration tests against a disposable metadata PostgreSQL database.
- [ ] Run `go test ./...`, `go vet ./...`, and race tests for the license agent.
- [ ] Run the Greenplum compatibility suite against every supported version.
- [ ] Install the signed RPM into a clean RHEL test environment and execute the full smoke suite.
- [ ] Verify the installed file list contains no `.py`, plaintext secret, private signing key, development database, or build cache.
- [ ] Perform an audited backup/restore and upgrade/rollback drill.
- [ ] Record release evidence and approve the release only when every phase exit criterion passes.

