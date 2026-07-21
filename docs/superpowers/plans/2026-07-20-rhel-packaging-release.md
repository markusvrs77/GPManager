# RHEL Packaging and Production Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a signed, source-free, installable GPManager RPM and prove installation, operation, upgrade, rollback, and recovery on RHEL.

**Architecture:** A reproducible build compiles Python entry points with Nuitka and the license agent with Go, vendors reviewed browser assets, generates an integrity manifest and SBOM, and builds signed RPMs. Systemd runs web, executor, scheduler, and agent services under a dedicated account; Nginx terminates TLS; SELinux confines files, sockets, and network access.

**Tech Stack:** RHEL, Python 3.11, Nuitka, Go, RPM, systemd, Nginx, SELinux, CycloneDX/SPDX-compatible SBOM.

## Global Constraints

- Installed production paths contain no `.py`, private key, plaintext credential, development database, build cache, or test fixture secret.
- Package scripts never delete user data or automatically run destructive database downgrade.
- Configuration and data survive upgrade, rollback, and uninstall unless an administrator explicitly removes them.
- The release is blocked by any Critical/High finding or failed source-safety/rollback test.

---

### Task 1: Deterministic Python and Go build inputs

**Files:**
- Create: `requirements.lock`
- Create: `build/compile-python.ps1`
- Create: `build/compile-python.sh`
- Create: `build/compile-go.sh`
- Create: `build/verify-build-inputs.py`
- Modify: `pyproject.toml`

- [ ] Add a test that fails on an unhashed Python dependency, unexpected network fetch during build, dirty generated dependency file, or unsupported compiler version.
- [ ] Lock Python dependencies with hashes and commit Go module checksums.
- [ ] Define four Nuitka entry points: `web`, `executor`, `scheduler`, and CLI. Explicitly include templates, static assets, migrations, required Flask metadata, timezone data, and psycopg2 runtime libraries.
- [ ] Build Go with `-trimpath`, an explicit version, and reproducible flags; strip only after preserving controlled symbol/debug artifacts for vendor support.
- [ ] Run two clean builds and compare approved artifact hashes or explain signed non-deterministic sections.
- [ ] Commit with `git commit -m "build: define reproducible native compilation"`.

### Task 2: Compiled-runtime smoke tests

**Files:**
- Create: `tests/packaging/test_compiled_runtime.py`
- Create: `build/smoke-compiled.sh`

- [ ] Test every executable with `--version` and `check-config` from a directory containing no source tree.
- [ ] Migrate a disposable metadata PostgreSQL database, start all compiled services, log in, create a preview, queue a non-destructive test job, and stop services cleanly.
- [ ] Scan the install tree for `.py`, source maps containing source, secrets, SQLite files, and absolute build paths; fail on any match.
- [ ] Run the smoke script in a clean RHEL-compatible build container; expect exit `0`.
- [ ] Commit with `git commit -m "test: verify source-free compiled runtime"`.

### Task 3: systemd, filesystem, and configuration layout

**Files:**
- Create: `packaging/systemd/gpmanager-web.service`
- Create: `packaging/systemd/gpmanager-executor.service`
- Create: `packaging/systemd/gpmanager-scheduler.service`
- Create: `packaging/systemd/gpmanager-license-agent.service`
- Create: `packaging/sysusers/gpmanager.conf`
- Create: `packaging/tmpfiles/gpmanager.conf`
- Create: `packaging/config/gpmanager.env.example`
- Create: `tests/packaging/test_systemd_units.py`

- [ ] Define least-privilege services with `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=strict`, explicit writable paths, restart policy, timeouts, and dependency ordering.
- [ ] Put immutable binaries/assets under `/usr`, config under `/etc/gpmanager`, mutable state under `/var/lib/gpmanager`, logs in journald, and runtime sockets under `/run/gpmanager`.
- [ ] Run `systemd-analyze verify` and unit-policy tests; expect PASS.
- [ ] Commit with `git commit -m "packaging: add hardened systemd services"`.

### Task 4: Nginx and SELinux policy

**Files:**
- Create: `packaging/nginx/gpmanager.conf`
- Create: `packaging/selinux/gpmanager.te`
- Create: `packaging/selinux/gpmanager.fc`
- Create: `packaging/selinux/gpmanager.if`
- Create: `tests/packaging/test_nginx_selinux.py`

- [ ] Configure TLS-only proxying, bounded request sizes/timeouts, secure headers, WebSocket rules only if required, and no direct public web-service port.
- [ ] Define SELinux types for executables, config, state, and Unix socket; allow only required metadata DB, managed DB, licensing network, and local proxy access.
- [ ] Install in enforcing mode, exercise application workflows, and fail on unexpected AVC denials.
- [ ] Commit with `git commit -m "packaging: add Nginx and SELinux confinement"`.

### Task 5: RPM spec, signatures, integrity, and SBOM

**Files:**
- Create: `packaging/rpm/gpmanager.spec`
- Create: `build/build-rpm.sh`
- Create: `build/generate-sbom.sh`
- Create: `tests/packaging/test_rpm_contents.py`

- [ ] Define RPM dependencies, service user creation, migration preflight, install/upgrade scripts, and non-destructive uninstall behavior.
- [ ] Generate the signed integrity manifest after compilation and before RPM assembly.
- [ ] Generate SBOMs for Python, Go, OS-linked libraries, and browser assets.
- [ ] Sign RPMs only in protected release CI; packages contain public verification material only.
- [ ] Inspect RPM contents and signatures; expect no forbidden files and valid signatures.
- [ ] Commit with `git commit -m "packaging: build signed GPManager RPM"`.

### Task 6: RHEL installation, upgrade, rollback, and backup tests

**Files:**
- Create: `tests/packaging/rhel-install.sh`
- Create: `tests/packaging/rhel-upgrade.sh`
- Create: `tests/packaging/rhel-backup-restore.sh`
- Create: `docs/operations/installation-rhel.md`
- Create: `docs/operations/upgrade-rollback.md`
- Create: `docs/operations/backup-restore.md`

- [ ] Install on a clean supported RHEL image/VM, configure metadata PostgreSQL, bootstrap admin, install a development license, and run the smoke suite.
- [ ] Upgrade from the previous supported schema/package; verify data, permissions, jobs, schedules, and audit history.
- [ ] Exercise application rollback without database downgrade, then perform a documented restore when schema rollback is required.
- [ ] Backup metadata PostgreSQL and agent state, restore into a clean instance, and verify license/rehost rules.
- [ ] Uninstall packages and verify `/etc/gpmanager` and `/var/lib/gpmanager` remain unless explicit purge is requested.
- [ ] Commit with `git commit -m "docs: add RHEL lifecycle runbooks"`.

### Task 7: SQLite migration utility

**Files:**
- Create: `gpmanager/migration/sqlite_import.py`
- Create: `gpmanager/migration/report.py`
- Create: `tests/fixtures/sqlite/*.sql`
- Create: `tests/test_sqlite_import.py`
- Modify: `gpmanager/cli.py`

- [ ] Build legacy fixtures from every supported `PRAGMA table_info` shape; do not copy real credentials into fixtures.
- [ ] Implement preview/report mode, SQLite backup, schema validation, ID mapping, interrupted-state conversion, preview-token invalidation, credential encryption through the agent, and one PostgreSQL transaction.
- [ ] Verify counts/checksums, rollback on injected failure, environment confirmation, and preservation of the original SQLite file.
- [ ] Run migration tests; expect PASS for every legacy fixture.
- [ ] Commit with `git commit -m "feat: migrate legacy SQLite metadata safely"`.

### Task 8: Final release gate

**Files:**
- Create: `docs/releases/release-checklist.md`
- Create: `docs/security/threat-model.md`
- Create: `docs/operations/disaster-recovery.md`
- Modify: `.github/workflows/python-package.yml`

- [ ] Run Python unit/integration/security tests and required compilation checks.
- [ ] Run Go unit, race, vet, protocol, cryptographic, lease, and integrity tests.
- [ ] Run PostgreSQL and supported Greenplum compatibility suites.
- [ ] Run source-alias, concurrency, cancellation, restart, licensing outage, migration, RPM, SELinux, backup/restore, and upgrade/rollback tests.
- [ ] Scan dependencies and SBOM; resolve every Critical/High result or document an approved false positive with evidence.
- [ ] Verify signed RPM and manifest on a separate clean verifier host.
- [ ] Obtain release approval only when every checklist item is evidenced and no blocking defect remains.
- [ ] Commit with `git commit -m "release: add production readiness gates"`.

