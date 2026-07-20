# GPManager RHEL Production Hybrid Design

**Status:** Approved design

**Date:** 2026-07-20

**Primary target:** RHEL Linux

**Future targets:** Ubuntu/Debian, Windows Server, SaaS. These targets are outside the first implementation scope, but business interfaces must remain portable.

## 1. Goals

Deliver GPManager as an installable, supportable, and security-gated on-premise product for RHEL. The first production version keeps the existing Flask application as the orchestration and UI layer, compiles Python code with Nuitka, and introduces a signed Go license agent. The architecture must allow backend capabilities to move incrementally to Go or Rust without breaking public API contracts.

The product must protect production/source databases, execute destination changes transactionally, support reliable cancellation, avoid shipping Python source files, and support both online and offline enterprise licensing.

## 2. Non-goals

- A full backend rewrite in Go or Rust before the first production release.
- Ubuntu/Debian or Windows Server packages in the first release.
- An absolute guarantee that software installed on a customer-controlled root server cannot be reverse engineered.
- Active-active multi-region deployment in the first release.
- Self-service user registration or public API tokens in the first release.

## 3. Packaging and code protection

The RHEL distribution consists of signed RPM packages containing:

- `gpmanager-web`: Nuitka native executable for HTTP and UI;
- `gpmanager-executor`: Nuitka native executable for job execution;
- `gpmanager-scheduler`: Nuitka native executable for scheduled dispatch;
- `gpmanager`: Nuitka native administrative and migration CLI;
- `gpmanager-license-agent`: native Go executable;
- templates, JavaScript, CSS, database migrations, systemd units, Nginx configuration, SELinux policy, default configuration, and a signed integrity manifest.

Python `.py` files are not installed on customer servers. Static assets remain inspectable because the browser must receive them. Nuitka compilation, signed RPMs, integrity checks, and the Go agent raise the cost of copying and modification but do not claim to defeat a root user absolutely.

The external `gpcopy` executable remains an optional, separately installed dependency. The installer validates its supported version and executable path.

## 4. Runtime architecture

Traffic flows through Nginx with TLS to `gpmanager-web`. The web component handles authentication, RBAC, CSRF, validation, previews, and job creation. It never starts background threads or performs long-running database operations.

`gpmanager-executor` claims jobs from PostgreSQL metadata storage with transactional `FOR UPDATE SKIP LOCKED`. It owns active psycopg2 connections and external GPCOPY process groups. A control thread observes stop requests, calls `connection.cancel()` for SQL/COPY, terminates the complete GPCOPY process group, rolls back incomplete destination transactions, and only then confirms cancellation.

`gpmanager-scheduler` runs as a separate systemd service. PostgreSQL advisory locking elects one scheduler leader, preventing duplicate dispatch when multiple instances exist.

`gpmanager-license-agent` runs as a separate Go service behind a root-owned Unix socket. It validates licensing and build integrity and provides credential encryption/decryption only to authorized GPManager service processes. Unix peer credentials, filesystem permissions, and SELinux policy restrict socket access.

All services run as a dedicated unprivileged `gpmanager` user. Importing application modules has no database initialization, recovery, scheduler, or thread-start side effects. Schema changes are applied explicitly through `gpmanager migrate` before service startup.

## 5. Metadata storage

PostgreSQL is the only live metadata backend in development and production. SQLite remains supported solely as an input to the one-time migration utility.

Metadata PostgreSQL stores:

- users, roles, password hashes, sessions, and external identity mappings;
- connection definitions and encrypted credential envelopes;
- jobs, job items, progress, worker ownership, heartbeats, and errors;
- immutable GPCOPY preview plans, token hashes, staging metadata, and target reservations;
- schedules and run history;
- audit events;
- installation identity and non-secret license cache state.

Versioned migrations define the complete schema. Foreign keys, unique constraints, check constraints, indexes, and row locks enforce state invariants. Credentials and license secrets are never stored in plaintext.

## 6. Authentication and authorization

The first release uses built-in users and RBAC while keeping the identity model ready for OIDC or SAML.

The installer creates the first administrator with a one-time temporary password that must be changed. Passwords use Argon2id. There is no public registration. Login is rate-limited and temporarily locked after repeated failures.

Sessions use `Secure`, `HttpOnly`, and `SameSite=Strict` cookies, bounded lifetime, rotation after authentication, and revocation after password or role changes. Every state-changing browser request requires a CSRF token. Destructive Apply requires recent authentication and confirmation of a valid preview.

Roles are:

- `viewer`: dashboard, previews, history, and exports;
- `operator`: approved job creation and stop operations;
- `admin`: connections, users, schedules, licensing, and system configuration.

Operator permissions are additionally scoped to connections and environments. Each connection is labeled `production`, `test`, or `development`. Production is allowed as a source for synchronization but is never allowed as a Prod-to-Test destination. Other production maintenance requires a distinct permission and explicit confirmation.

The user model includes `identity_provider` and `external_subject` so future SSO does not require replacing RBAC records.

## 7. Source and destination safety

Connection IDs alone never establish database identity. Before Preview and Apply, GPManager computes and records a real endpoint fingerprint using database name, server address and port, and a stable cluster identifier where the supported database version provides one.

Apply is rejected when source and destination fingerprints refer to the same database, even when different connection IDs or DNS aliases are used. Apply is also rejected unless the destination environment is explicitly permitted for the operation.

Source psycopg2 sessions are read-only. Destination roles use least privilege and are separate from source roles. External GPCOPY is permitted only through a validated executable and supported arguments.

Every destination-changing workflow follows:

1. authorize user and license;
2. validate source/destination identity and environment;
3. create Preview with counts and invariants;
4. persist an immutable, expiring, single-use token hash;
5. revalidate the target and reserve affected objects;
6. execute in a destination transaction;
7. perform post-apply validation;
8. commit;
9. record audit and terminal job state.

Legacy destructive endpoints do not execute operations. They return a migration/deprecation response until clients move to the safe versioned workflow.

## 8. Licensing

Online licensing uses a signed 24-hour lease renewed every six hours. If the licensing service becomes unreachable, an existing valid installation receives a seven-day grace period with visible warnings.

Offline installations use an Ed25519-signed license containing customer ID, edition, enabled modules, limits, expiry, and installation fingerprint. Hardware binding may optionally use TPM. Rehosting requires a signed rehost workflow.

The private signing key remains only in vendor-controlled infrastructure. The Go agent contains public verification keys.

An expired license prevents creation of new jobs and schedule changes. It still permits:

- completion and safe cancellation of already running jobs;
- read-only dashboard and history access;
- export of existing data;
- installation of a replacement license.

Licensing is never checked between destination DML and `commit`. A missing or damaged agent places the application in read-only mode rather than allowing destructive operations.

## 9. Secrets and logging

Database credentials are encrypted through the Go agent. The PostgreSQL metadata database stores ciphertext and key metadata, not plaintext encryption keys. Decrypted credentials exist only in process memory for the duration required to open a database connection.

API clients receive safe error messages and correlation IDs. Complete exceptions are written to structured server logs without credentials, tokens, connection strings, row data, or generated secret material.

Audit records include actor, source IP, action, connection fingerprint, preview counts, job ID, timestamps, result, and correlation ID. They exclude passwords, session values, license tokens, and copied row values.

## 10. SQLite migration

`gpmanager migrate-sqlite` performs the one-time transition from an existing GPManager SQLite database.

The command inspects every source table through `PRAGMA table_info`, creates a backup, and first produces a read-only preview report. The apply phase uses one PostgreSQL transaction and verifies row counts and checksums before commit.

Connections are imported with an unconfirmed environment label and cannot run destructive operations until an administrator classifies them. Existing plaintext passwords are read only during migration, immediately encrypted through the Go agent, and never logged or stored plaintext in PostgreSQL.

Jobs, items, schedules, plans, and compatible history retain relationships through an explicit ID mapping. Non-terminal jobs and plans become `interrupted`; they are never resumed automatically. Preview tokens are invalidated and not migrated. Staging cleanup is a separate explicitly confirmed operation.

The original SQLite file is not deleted and becomes a read-only backup.

## 11. API compatibility

Existing read-only contracts remain compatible where doing so does not weaken security. New production APIs use `/api/v1`. The bundled frontend moves to versioned APIs in the same release.

Legacy endpoints that bypass authentication, preview, transaction, reservation, or cancellation rules return a documented deprecation error. Compatibility never overrides production safety.

## 12. Failure handling

Job state transitions are transactional and validate the previous state. A job item becomes `done` only after the real operation and required post-validation complete.

Executor ownership and heartbeat distinguish active jobs from stale jobs. Startup recovery affects only stale ownership records, never every running job globally. Target reservations persist until a confirmed terminal state or an audited recovery procedure releases them.

Loss of the web process does not interrupt executor transactions. Loss of the executor causes the database transaction to roll back; recovery marks the job interrupted after heartbeat expiry. Loss of metadata PostgreSQL stops new work and leaves active destination operations in a safe cancel-or-complete path. Loss of the licensing service follows the lease and grace rules.

## 13. Production release gates

The release requires:

- unit tests for domain modules, state machines, and migrations;
- integration tests with metadata PostgreSQL and separate source and destination PostgreSQL databases;
- a Greenplum compatibility suite for every supported version;
- alias-connection tests proving production can never become the destination;
- Preview/Apply tests for INSERT, UPDATE, DELETE, Full, Date, and Partition workflows;
- duplicate-key, null-key, stale-preview, concurrent-reservation, and target-change tests;
- stop tests during SQL, COPY, external GPCOPY, and commit boundaries, proving rollback and child-process termination;
- RBAC, CSRF, session fixation, brute-force, XSS, traceback, secret-redaction, path, and argument-injection tests;
- concurrency tests for job claims, scheduler leadership, and target reservations;
- restart and dependency-failure tests for every service;
- SQLite migration tests for every supported legacy schema;
- Nuitka tests proving the installed product runs without `.py` files;
- installation, upgrade, rollback, backup, restore, and uninstall tests in a clean supported RHEL image or VM;
- dependency and SBOM scanning, RPM signature validation, and integrity-manifest verification;
- installation, security, operations, upgrade, rollback, and disaster-recovery documentation.

Any Critical or High defect, source-safety violation, failed rollback, incomplete external-process termination, plaintext credential, or unsigned release artifact blocks production release.

## 14. Future migration to Go or Rust

The public API, job state model, preview-plan contract, metadata schema, and license-agent protocol are language-neutral boundaries. Backend modules move incrementally behind these contracts. The executor and security-critical operations are the first candidates; UI/API orchestration can move later without changing user-visible workflows.

