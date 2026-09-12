# Authentication and Web Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Require authenticated, scoped authorization for every GPManager capability and remove the web vulnerabilities identified by the audit.

**Architecture:** Flask request hooks establish an actor from a server-side session. Policy functions combine role permissions, connection scopes, environment rules, recent authentication, and later license decisions. CSRF protects browser mutations; audit and correlation middleware record safe evidence without exposing traces or secrets.

**Tech Stack:** Flask, Flask-WTF, argon2-cffi, PostgreSQL, Bootstrap, Vanilla JavaScript, unittest.

## Global Constraints

- No route is implicitly public; only login and health endpoints have explicit anonymous access.
- No self-registration.
- Never return traceback, credential, token, DSN, or copied row data to clients or audit logs.
- Production destination denial is enforced server-side, independent of UI state.

---

### Task 1: Users, roles, sessions, and bootstrap admin

**Files:**
- Create: `migrations/versions/0002_identity_and_audit.py`
- Create: `gpmanager/security/passwords.py`
- Create: `gpmanager/security/sessions.py`
- Create: `gpmanager/security/identity.py`
- Create: `tests/test_identity.py`
- Modify: `gpmanager/cli.py`

**Interfaces:**
- Produces: `hash_password(str) -> str`, `verify_password(str, str) -> bool`
- Produces: `IdentityService.authenticate(username, password, ip) -> Actor`
- Produces CLI: `gpmanager create-admin --username NAME`

- [ ] Write failing tests for Argon2id hashes, disabled users, login lockout, one-time password expiry, session rotation, and revocation after role/password changes.
- [ ] Run `python -m unittest tests.test_identity -v`; expect failure because security modules do not exist.
- [ ] Add identity tables with unique normalized usernames, provider/subject identity, password version, failed-login counters, lock expiry, and server-side session hashes.
- [ ] Implement password and session services. Store only a hash of the random session token; use constant-time comparisons.
- [ ] Implement `create-admin` so the temporary password is printed once, never logged, and must be changed on first login.
- [ ] Run the identity tests; expect PASS.
- [ ] Commit with `git commit -m "feat: add local identity and secure sessions"`.

### Task 2: RBAC and connection-scoped policy

**Files:**
- Create: `gpmanager/security/policy.py`
- Create: `gpmanager/security/decorators.py`
- Create: `tests/test_policy.py`
- Modify: `modules/connections.py`

**Interfaces:**
- Produces: `authorize(actor, action, connection_ids=(), destructive=False) -> None`
- Produces: `require_permission(action, destructive=False)` decorator

- [ ] Write a permission matrix test for `viewer`, `operator`, and `admin`, including operator connection scopes and recent-auth requirement.
- [ ] Add `production`, `test`, and `development` validation to connection creation and updates.
- [ ] Implement policy evaluation with deny-by-default behavior. Production-as-destination must raise a dedicated `ProductionDestinationDenied` exception.
- [ ] Apply connection scopes after loading IDs from trusted metadata, never from UI labels.
- [ ] Run `python -m unittest tests.test_policy -v`; expect PASS.
- [ ] Commit with `git commit -m "feat: enforce scoped RBAC policies"`.

### Task 3: Authentication routes, CSRF, and safe cookies

**Files:**
- Create: `gpmanager/web/auth.py`
- Create: `templates/login.html`
- Create: `tests/test_web_security.py`
- Modify: `gpmanager/app_factory.py`
- Modify: `templates/base.html`

**Interfaces:**
- Produces routes: `GET/POST /login`, `POST /logout`, `POST /reauthenticate`

- [ ] Write failing tests for anonymous denial, login rotation, `Secure`/`HttpOnly`/`SameSite=Strict`, CSRF rejection, logout, forced password change, and reauthentication expiry.
- [ ] Configure server-side sessions, global CSRF validation, request body limits, trusted proxy handling, and login throttling.
- [ ] Add login/logout UI without embedding secrets in HTML or JavaScript storage.
- [ ] Run `python -m unittest tests.test_web_security -v`; expect PASS.
- [ ] Commit with `git commit -m "feat: secure browser authentication and CSRF"`.

### Task 4: Route-by-route authorization inventory

**Files:**
- Create: `docs/security/route-permissions.md`
- Create: `tests/test_route_permissions.py`
- Modify: `app.py`
- Modify: `modules/scheduler_api.py`

**Interfaces:**
- Consumes: `require_permission`

- [ ] Generate a test inventory from Flask `url_map`; fail when a route lacks `public` or `permission` metadata.
- [ ] Classify every current route. Reads use `viewer`; job creation/stop use `operator`; connection/user/license/configuration mutations use `admin`; destructive Apply also requires recent auth.
- [ ] Replace unsafe legacy mutation handlers with a `410 Gone` deprecation response containing the safe `/api/v1` replacement.
- [ ] Document the complete matrix and verify code and documentation contain identical endpoint sets.
- [ ] Run `python -m unittest tests.test_route_permissions -v`; expect PASS.
- [ ] Commit with `git commit -m "security: require explicit policy on every route"`.

### Task 5: Error, audit, and security-header middleware

**Files:**
- Create: `gpmanager/web/errors.py`
- Create: `gpmanager/web/audit.py`
- Create: `gpmanager/web/headers.py`
- Create: `tests/test_error_audit_headers.py`
- Modify: `app.py`

**Interfaces:**
- Produces: correlation ID on every response
- Produces: append-only `AuditEvent` records

- [ ] Write tests proving client errors omit traceback/paths/secrets, logs redact sensitive keys, and audit records include actor/action/result/correlation ID.
- [ ] Replace all `traceback.format_exc()` response fields with typed errors and correlation IDs.
- [ ] Add CSP, `X-Content-Type-Options: nosniff`, frame denial, strict referrer policy, and HSTS when TLS proxy headers are trusted.
- [ ] Ensure audit write failure blocks destructive operations before execution but never interrupts an in-flight destination transaction.
- [ ] Run `python -m unittest tests.test_error_audit_headers -v`; expect PASS.
- [ ] Commit with `git commit -m "security: add safe errors audit and response headers"`.

### Task 6: Remove DOM XSS and duplicate globals

**Files:**
- Modify: `templates/dashboard.html`
- Modify: `static/js/dashboard.js`
- Modify: `static/js/gpcopy.js`
- Modify: `static/js/reorganize.js`
- Modify: `static/js/skew.js`
- Create: `tests/test_frontend_security.py`

- [ ] Add static tests that reject duplicate global function declarations and unapproved interpolation of API values into `innerHTML`/`insertAdjacentHTML`.
- [ ] Move Dashboard JavaScript into the page-specific file and keep one `dashboardLoadSessionLimitsStats` entry point.
- [ ] Render untrusted values with `textContent`, DOM element creation, or a single reviewed escaping helper for attribute contexts.
- [ ] Store no session or license credential in `localStorage` or `sessionStorage`; preview tokens remain short-lived and are cleared after consumption.
- [ ] Run frontend static tests and browser smoke tests; expect PASS and no CSP violation.
- [ ] Commit with `git commit -m "security: remove DOM injection and global collisions"`.

### Task 7: Phase verification

- [ ] Run `python -m unittest discover -s tests -v` and the required `py_compile` command.
- [ ] Run an unauthenticated crawl and verify only login and health are reachable.
- [ ] Run RBAC tests for all three roles and connection scopes.
- [ ] Run CSRF, XSS, session fixation, brute-force, and secret-redaction tests.
- [ ] Confirm all CDN assets are local or carry approved integrity metadata compatible with CSP.
- [ ] Commit CI/security configuration with `git commit -m "ci: gate authentication and web security"`.

