# Executor and Database Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move asynchronous work out of web processes and enforce source safety, transactional Preview/Apply, reliable cancellation, and scheduler leadership.

**Architecture:** Web requests create immutable plans and queued jobs in metadata PostgreSQL. Dedicated executors claim work, own runtime resources, heartbeat ownership, and finalize state. A control loop cancels psycopg2 operations and external process groups. All destructive modules consume one safety service for endpoint identity, target reservations, transaction boundaries, and post-validation.

**Tech Stack:** Python 3.11, PostgreSQL, psycopg2, systemd-compatible processes, unittest/integration tests.

## Global Constraints

- Source sessions are read-only and never execute DDL/DML.
- Different connection IDs do not imply different databases.
- No destructive operation bypasses Preview/Apply.
- Cancellation is terminal only after runtime termination and destination rollback are confirmed.
- Web processes never hold active job database connections or child processes.

---

### Task 1: Endpoint identity and environment guard

**Files:**
- Create: `gpmanager/database/identity.py`
- Create: `gpmanager/database/safety.py`
- Create: `tests/test_database_identity.py`
- Modify: `modules/connections.py`

**Interfaces:**
- Produces: `inspect_endpoint(connection_id: int, readonly: bool) -> EndpointIdentity`
- Produces: `assert_safe_destination(source, destination, operation) -> None`

- [ ] Write tests with two connection IDs returning the same database/address/port/cluster identity; assert every synchronization mode rejects Apply.
- [ ] Implement a parameterized identity query returning `current_database`, server address/port, server version, and supported stable cluster identifier. Hash canonical JSON with SHA-256.
- [ ] Persist the fingerprint and observed values; never treat DNS name or connection ID as identity.
- [ ] Enforce destination environment and dedicated production-maintenance permission in one server-side guard.
- [ ] Run `python -m unittest tests.test_database_identity -v`; expect PASS.
- [ ] Commit with `git commit -m "security: enforce database endpoint identity"`.

### Task 2: Transactional job claims and heartbeats

**Files:**
- Create: `gpmanager/executor/claims.py`
- Create: `gpmanager/executor/heartbeat.py`
- Create: `tests/test_executor_claims.py`
- Modify: `job_manager.py`

**Interfaces:**
- Produces: `claim_jobs(worker_id: str, limit: int) -> list[Job]`
- Produces: `heartbeat_job(job_id, worker_id, version) -> None`
- Produces: `recover_stale_jobs(cutoff, worker_id) -> list[int]`

- [ ] Write concurrent claim tests proving one job is returned to one worker only.
- [ ] Implement `FOR UPDATE SKIP LOCKED`, allowed-state predicates, version increments, ownership, and heartbeat timestamps.
- [ ] Recover only jobs whose heartbeat expired; never mark all running jobs interrupted at process startup.
- [ ] Remove `RUNNING_THREADS` as a source of truth; PostgreSQL state is authoritative.
- [ ] Run claim/recovery tests; expect PASS.
- [ ] Commit with `git commit -m "feat: add transactional executor ownership"`.

### Task 3: Executor process and operation registry

**Files:**
- Create: `gpmanager/executor/main.py`
- Create: `gpmanager/executor/registry.py`
- Create: `gpmanager/executor/runtime.py`
- Create: `tests/test_executor_runtime.py`
- Modify: `modules/scheduled_operations.py`
- Modify: `app.py`

**Interfaces:**
- Produces: `OperationRegistry.register(job_type, runner)`
- Produces CLI: `gpmanager executor`

- [ ] Write tests proving web job creation does not start a thread and executor dispatches exactly one registered runner.
- [ ] Register existing verified runners by current signatures; reject unknown job types without dynamic import from user input.
- [ ] Change web and scheduler starters to create jobs only. Executor owns execution and status transitions.
- [ ] Add graceful SIGTERM: stop claiming, request cancellation, wait bounded time, then leave heartbeat evidence for recovery.
- [ ] Run executor tests and existing job tests; expect PASS.
- [ ] Commit with `git commit -m "refactor: move jobs into executor service"`.

### Task 4: Runtime cancellation controller

**Files:**
- Create: `gpmanager/executor/cancellation.py`
- Create: `tests/test_cancellation.py`
- Modify: `job_manager.py`
- Modify: `modules/gpcopy.py`

**Interfaces:**
- Produces: `RuntimeRegistry.register_connection(job_id, conn, rollback_on_cancel)`
- Produces: `RuntimeRegistry.register_process_group(job_id, process)`
- Produces: `CancellationController.cancel(job_id) -> CancellationResult`

- [ ] Write tests proving `connection.cancel()` is called, rollback-required connections roll back, and job state stays `stopping` until confirmation.
- [ ] Start external GPCOPY in a new process group/session; terminate the group, wait, escalate to kill, and verify no descendant survives.
- [ ] Record cancellation errors and require operator intervention instead of falsely marking the job cancelled.
- [ ] Make stop idempotent and available even when the license is expired.
- [ ] Run cancellation tests on Linux; expect PASS for SQL, COPY mock, graceful process exit, and forced process kill.
- [ ] Commit with `git commit -m "feat: guarantee runtime cancellation and rollback"`.

### Task 5: Unified immutable Preview/Apply service

**Files:**
- Create: `gpmanager/operations/contracts.py`
- Create: `gpmanager/operations/plans.py`
- Create: `gpmanager/operations/apply.py`
- Create: `tests/test_operation_plans.py`
- Modify: `modules/gpcopy_sync.py`
- Modify: `modules/gpcopy_full.py`
- Modify: `modules/gpcopy_date.py`
- Modify: `modules/gpcopy_partition_transfer.py`

**Interfaces:**
- Produces: `PreviewService.create(request, actor) -> PreviewResult`
- Produces: `ApplyService.consume(token, actor) -> int` job ID

- [ ] Write contract tests for single-use hashed tokens, expiry, fingerprint changes, target count changes, duplicate/null keys, explicit delete confirmation, and reservations.
- [ ] Move common token/plan/reservation behavior behind the service while preserving mode-specific validators.
- [ ] Store the endpoint fingerprints, actor, exact target set, mapped columns, key policy, preview counts, and config fingerprint in every plan.
- [ ] Recheck environment and endpoint identity at Apply; abort if either differs from Preview.
- [ ] Run all GPCOPY plan tests; expect PASS.
- [ ] Commit with `git commit -m "refactor: unify immutable preview and apply plans"`.

### Task 6: Remove unsafe destructive paths

**Files:**
- Modify: `app.py`
- Modify: `modules/reorganize.py`
- Modify: `modules/gpcopy.py`
- Create: `tests/test_no_unsafe_write_routes.py`

- [ ] Add route tests proving legacy `/api/gpcopy/start` rejects `truncate`, `drop`, or any write and directs clients to `/api/v1`.
- [ ] Convert Apply Distribution into a plan-backed queued operation with preview SQL, reservation, executor runner, cancellation, and transaction.
- [ ] Reject any write runner without `plan_id`, destination fingerprint, and reservation.
- [ ] Ensure per-item `done` is set only after actual post-validation; external process exit code alone is insufficient.
- [ ] Run route and operation tests; expect PASS.
- [ ] Commit with `git commit -m "security: retire destructive legacy execution paths"`.

### Task 7: Scheduler singleton and queued dispatch

**Files:**
- Modify: `scheduler.py`
- Modify: `modules/scheduler_repository.py`
- Modify: `modules/scheduler_api.py`
- Create: `tests/test_scheduler_leadership.py`

- [ ] Write a two-instance test proving one advisory-lock leader claims due runs and neither instance starts worker threads.
- [ ] Implement a session-level advisory lock for leadership and release it on shutdown.
- [ ] Scheduler creates queued jobs only; executor starts them. Preserve overlap and misfire behavior transactionally.
- [ ] Run all scheduler tests plus leadership tests; expect PASS.
- [ ] Commit with `git commit -m "refactor: run scheduler as PostgreSQL-elected service"`.

### Task 8: End-to-end safety verification

- [ ] Run source/destination alias tests for every mode.
- [ ] Run Preview/Apply tests with concurrent target changes and reservations.
- [ ] Run stop during SELECT, COPY FROM/TO, DELETE, TRUNCATE, external GPCOPY, and immediately before commit.
- [ ] Verify source query logs contain no DDL/DML.
- [ ] Kill web, executor, and scheduler independently and verify recovery state.
- [ ] Run the full test suite and required `py_compile` command.
- [ ] Commit evidence harness with `git commit -m "test: gate executor and database safety"`.

