# Go License Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a signed local Go service for online/offline licensing, integrity checks, and credential envelope encryption.

**Architecture:** The agent exposes a versioned length-delimited JSON protocol over a Unix socket. It validates peer identity, signed leases/licenses, installation binding, and integrity manifests. Python uses a small typed client and fails closed to read-only mode. Cryptographic messages have canonical encodings and shared test vectors.

**Tech Stack:** Go, Ed25519, AES-256-GCM, Unix domain sockets, Python 3.11, unittest.

## Global Constraints

- Vendor private signing keys never enter source control, builds, packages, logs, or customer systems.
- Encryption keys never enter PostgreSQL.
- License failure never interrupts an already executing destination transaction.
- Offline and online signatures use canonical versioned payloads and explicit expiry.
- The Unix socket is not an authorization substitute; peer credentials and application RBAC remain required.

---

### Task 1: Versioned protocol and shared fixtures

**Files:**
- Create: `license-agent/go.mod`
- Create: `license-agent/internal/protocol/messages.go`
- Create: `license-agent/internal/protocol/framing.go`
- Create: `gpmanager/license/protocol.py`
- Create: `testdata/license/v1/*.json`
- Create: `tests/test_license_protocol.py`

**Interfaces:**
- Produces operations: `evaluate`, `encrypt_credential`, `decrypt_credential`, `installation_request`, `install_license`, `status`

- [ ] Define request/response envelopes containing `version`, `request_id`, `operation`, `payload`, and typed error code.
- [ ] Write Go and Python tests that encode/decode identical fixture bytes and reject oversized, unknown-version, truncated, or duplicate-field frames.
- [ ] Implement a four-byte big-endian frame length with a one-megabyte maximum.
- [ ] Run `go test ./...` and `python -m unittest tests.test_license_protocol -v`; expect PASS.
- [ ] Commit with `git commit -m "feat: define license agent protocol"`.

### Task 2: Offline license verification

**Files:**
- Create: `license-agent/internal/license/offline.go`
- Create: `license-agent/internal/license/offline_test.go`
- Create: `tools/license-signer/main.go`
- Modify: `testdata/license/v1/*.json`

**Interfaces:**
- Produces: `VerifyOffline(document []byte, publicKeys KeySet, now time.Time, fingerprint string) (Entitlements, error)`

- [ ] Create test vectors for valid, expired, wrong-customer, wrong-fingerprint, altered-payload, unknown-key, and future-version licenses.
- [ ] Define canonical signed content: customer, edition, modules, limits, issued/expiry timestamps, installation fingerprint, rehost counter, key ID, and version.
- [ ] Implement Ed25519 verification using embedded public keys selected by key ID.
- [ ] Keep the signer tool outside the customer package and load its private key only from an explicit secure path/environment in vendor CI.
- [ ] Run Go tests; expect PASS.
- [ ] Commit with `git commit -m "feat: verify signed offline licenses"`.

### Task 3: Online lease and grace state machine

**Files:**
- Create: `license-agent/internal/license/lease.go`
- Create: `license-agent/internal/license/state.go`
- Create: `license-agent/internal/license/state_test.go`

**Interfaces:**
- Produces states: `valid`, `grace`, `expired`, `invalid`, `unavailable`

- [ ] Write clock-controlled tests for 24-hour lease, six-hour renewal scheduling, seven-day grace, clock rollback, invalid signature, and recovery after outage.
- [ ] Persist only signed lease material and monotonic validation evidence in a root-owned state directory.
- [ ] Return read-only decisions after grace expiry; allow `job.stop`, `job.read`, `history.read`, `export`, and `license.install`.
- [ ] Evaluate license only before job/preview/schedule creation; executor does not re-evaluate between DML and commit.
- [ ] Run Go race and unit tests; expect PASS.
- [ ] Commit with `git commit -m "feat: implement online lease grace policy"`.

### Task 4: Installation fingerprint and rehost

**Files:**
- Create: `license-agent/internal/installation/fingerprint.go`
- Create: `license-agent/internal/installation/rehost.go`
- Create: `license-agent/internal/installation/fingerprint_test.go`

- [ ] Write tests for stable RHEL machine identity, VM clone changes, missing TPM, optional TPM binding, and signed rehost request generation.
- [ ] Canonicalize approved signals without collecting unrelated hardware or personal data.
- [ ] Store installation ID with `0600` permissions; never silently regenerate it after activation.
- [ ] Generate signed/public rehost requests containing no secret key.
- [ ] Run Go tests; expect PASS.
- [ ] Commit with `git commit -m "feat: bind licenses to installation identity"`.

### Task 5: Credential envelope encryption

**Files:**
- Create: `license-agent/internal/secrets/envelope.go`
- Create: `license-agent/internal/secrets/keystore.go`
- Create: `license-agent/internal/secrets/envelope_test.go`
- Create: `gpmanager/license/client.py`
- Create: `tests/test_credential_envelope.py`

**Interfaces:**
- Produces envelope fields: version, key ID, nonce, ciphertext, authenticated context
- Produces Python: `LicenseAgentClient.encrypt_credential`, `decrypt_credential`, `evaluate`

- [ ] Write tests proving random nonces, tamper detection, connection-ID authenticated context, key rotation, peer denial, and zero-length rejection.
- [ ] Use AES-256-GCM with a locally generated key in a root-owned agent keystore; authenticate installation ID and connection ID as additional data.
- [ ] Never log plaintext or return it in typed error details; overwrite temporary Go buffers where practical.
- [ ] Replace metadata plaintext password writes with agent-produced envelopes and migrate connection opening through the client.
- [ ] Run Go and Python tests; expect PASS.
- [ ] Commit with `git commit -m "security: encrypt database credentials through agent"`.

### Task 6: Unix service and Python authorization integration

**Files:**
- Create: `license-agent/cmd/gpmanager-license-agent/main.go`
- Create: `license-agent/internal/server/server.go`
- Create: `license-agent/internal/server/peer_linux.go`
- Create: `tests/test_license_authorization.py`
- Modify: `gpmanager/security/policy.py`

- [ ] Write service tests for peer UID/GID enforcement, socket permissions, request timeout, malformed frames, concurrent clients, and graceful shutdown.
- [ ] Implement socket activation-compatible serving and structured redacted logging.
- [ ] Add Python license decisions after RBAC but before Preview/job creation. Map unavailable/invalid decisions to read-only mode.
- [ ] Confirm stop/read/history/export/license-install remain available after expiry.
- [ ] Run `go test -race ./...` and Python authorization tests; expect PASS.
- [ ] Commit with `git commit -m "feat: integrate local licensing service"`.

### Task 7: Integrity manifest and phase verification

**Files:**
- Create: `license-agent/internal/integrity/manifest.go`
- Create: `tools/integrity-manifest/main.go`
- Create: `license-agent/internal/integrity/manifest_test.go`

- [ ] Define a signed manifest with package version and SHA-256 hashes of executables, migrations, templates, and static assets.
- [ ] Verify manifest at agent startup and before destructive authorization; ignore mutable configuration, logs, and data directories.
- [ ] Test valid, modified, missing, extra-critical-file, and key-rotation cases.
- [ ] Run Go tests, Python client tests, protocol fixtures, offline activation, online outage/grace, and credential rotation tests.
- [ ] Commit with `git commit -m "security: verify signed installation integrity"`.

