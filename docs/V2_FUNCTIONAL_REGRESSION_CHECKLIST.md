# V2 Functional & Regression Checklist

**Purpose:** single working checklist for reviewing Nafazbot V2 functions before runtime cutover and before any LIVE canary.

**Status meanings**
- [x] Implemented in the current V2 foundation (not necessarily proven in a deployed runtime).
- [ ] Not implemented or not yet verified.
- [!] Mandatory blocker for LIVE until evidence is attached.
- N/A only when the feature is explicitly disabled by approved configuration.

**Safety rule:** a checked design/schema item is not evidence that the runtime function works. Record test output, CI run, logs, and operator sign-off for each runtime gate. Never place API keys, Telegram tokens, DSNs, or other secrets in this checklist.

## 1. Branch, build and release integrity
- [x] One canonical development branch: `architecture/v2-postgres-control-plane`.
- [x] One consolidated draft PR: #33; no merge authorization implied.
- [ ] Confirm PR head and base are the intended commits before every release.
- [ ] CI Python tests pass on the exact candidate SHA.
- [ ] CI Go unit tests, race tests and vet pass on the exact candidate SHA.
- [ ] PostgreSQL migrations apply cleanly to an empty database.
- [ ] Every migration replays idempotently.
- [ ] Scanner and executor Docker images build from the same candidate SHA.
- [ ] Dependency lock/pinning and vulnerability review completed.
- [ ] Rollback artifact and recovery procedure documented.

## 2. PostgreSQL durable configuration
- [x] Tables exist for immutable config versions, active pointer, config audit, operator state/actions and outbox.
- [x] Database starts with trading disabled / STOPPED.
- [x] RLS is enabled and client roles have no access to V2 control tables.
- [ ] Provision a least-privilege server-side DB role; prove it has only required grants.
- [ ] Verify DB TLS, connection limits, pool lifecycle, timeout and reconnect behavior.
- [x] Implement canonical JSON and SHA-256 verification (independent runtime/DB fixture evidence still required).
- [x] Implement fail-closed checks for unsupported schema, malformed JSON, non-finite numbers and hash mismatch (CI evidence pending).
- [x] Implement rejection of raw secret fields; secret references remain allowed (secret-manager integration pending).
- [x] Implement V2 root-key, duplicate-ID and dangling/mismatched venue/account/route/risk reference validation (full contract coverage pending).
- [x] Implement transactional create-draft, validate, activate (CAS) and rollback-as-new-draft operations; integration/concurrency tests pending.
- [x] Implement active-version compare-and-swap under a PostgreSQL advisory transaction lock; concurrent DB integration test pending.
- [ ] Verify concurrent activations cannot create two active pointers or lose audit records.
- [ ] Verify immutable snapshot fields cannot be updated or deleted by runtime role.
- [x] Implement config lifecycle audit and outbox writes in the same transaction as config mutations; DB integration/replay tests pending.
- [ ] Verify restart loads the active PostgreSQL version, never a local stale file.
- [ ] Verify DB outage prevents LIVE startup and new order admission.
- [ ] Verify recovery after DB outage does not auto-start trading.

## 3. Redis cache, notification and outbox
- [x] Redis is specified as transient cache/transport, not the configuration authority.
- [x] Snapshot loader fetches PostgreSQL first and verifies Redis read-back.
- [ ] Verify Redis payload includes version, schema version and content hash.
- [ ] Verify stale, malformed, wrong-version and wrong-hash Redis payloads are rejected.
- [ ] Verify Redis outage blocks LIVE readiness (no stale-cache fallback).
- [x] Implement Redis invalidation consumer that treats event payloads as hints and refetches the active snapshot from PostgreSQL.
- [x] Implement serialized, atomic in-process snapshot reference swap after PostgreSQL/hash validation and Redis mirror verification.
- [ ] Wire the consumer/store into executor startup and every order-admission path; runtime integration proof pending.
- [x] Implement outbox row claiming with PostgreSQL `FOR UPDATE SKIP LOCKED` and expiring worker leases; multi-worker DB integration test pending.
- [x] Implement bounded exponential retry/backoff and lease release for Redis publish failures; poison-event policy and outage replay integration tests pending.
- [x] Publish stable `event_key` in every event envelope for consumer deduplication; idempotent consumer implementation/tests pending.
- [x] Add PostgreSQL executor membership/lease table and per-instance version/hash acknowledgement API.
- [x] Implement a lease-scoped convergence report and fail-closed `require_converged` API.
- [ ] Run multi-process PostgreSQL integration tests for heartbeat, restart fencing, stale lease expiry and acknowledgement races.
- [x] Implement PostgreSQL entry gate checking durable RUNNING state, trading_enabled, operator config version, active config version/hash and all live executor acknowledgements.
- [x] Add fail-closed engine hook: LIVE entry candidates and final pre-submit path are denied when no V2 admission gate is injected; MOCK path remains unchanged.
- [ ] Wire PostgreSQL pool, verified snapshot loader, consumer heartbeat/reconciliation and gate into executor startup after V2-to-runtime config mapping is approved.
- [ ] Add a transactional cross-process STOP fencing barrier; current final pre-submit read is not an atomic STOP-vs-submit serialization primitive.
- [ ] Verify old cache keys and snapshots expire only after all consumers have advanced.

## 4. Runtime configuration cutover
- [ ] Map every legacy config.json/environment field to V2 or an explicit deprecation decision.
- [ ] Identify all readers of legacy JSON, environment variables, SQLite/JSONL and Supabase.
- [ ] Define a one-way cutover; no silent dual authority or conflicting dual writes.
- [ ] Add explicit compatibility mode and migration report.
- [ ] Compare legacy and V2 effective configuration without exposing secret values.
- [ ] Prove risk limits and venue settings are unchanged by migration.
- [ ] Prove missing V2 config never silently falls back to legacy config in LIVE.
- [ ] Wire verified PostgreSQL load into startup only after migration/cutover tests pass; current engine explicitly denies LIVE entries without the injected V2 gate.
- [ ] Verify graceful shutdown closes DB/Redis pools and releases leases.

## 5. Telegram operator control plane
- [x] Database schema supports durable operator state and command idempotency.
- [x] Specification requires Python ownership of trading control.
- [ ] Implement Telegram token from environment/secret manager only.
- [ ] Implement both user-ID and chat-ID allowlists; empty allowlists deny all.
- [ ] Implement /trade_status and verify it is read-only.
- [ ] Implement /trade_stop with mandatory reason, audit and durable entry barrier.
- [ ] Implement /trade_start with one-time confirmation bound to actor, chat and config version.
- [ ] Implement /trade_confirm with expiry, single use and replay protection.
- [ ] Reject unauthorized user/chat, malformed commands and duplicate update/command IDs.
- [ ] Verify duplicate Telegram delivery returns the original result and never repeats side effects.
- [ ] Verify command ledger captures actor, chat, update ID, command ID, reason, timestamp and outcome.
- [ ] Verify STOP is durable across process restart and blocks new entry on every executor instance.
- [ ] Verify STOP does not automatically close positions.
- [ ] Verify open-order policy: default keep_and_manage; cancel_unfilled_entries only when explicitly selected.
- [ ] Verify Telegram outage does not disable local durable STOP or permit accidental START.
- [ ] Go Telegram is read-only alerts only; it cannot start/stop or authorize orders.

## 6. START readiness gates
- [ ] Database reachable and required migrations/schema version verified.
- [ ] Active config version/hash validated and identical across executor instances.
- [ ] Redis version verified; outbox lag and consumer acknowledgements within policy.
- [ ] Secret references resolve; credentials are valid without logging their values.
- [ ] Venue capabilities, symbols, quote/settle assets and contract metadata are fresh.
- [ ] IDR/USDT LIVE FX source is real, fresh and within spread/fee bounds; no static synthetic fallback.
- [ ] Spot and futures balances are fresh, finite and sufficient.
- [ ] Account-wide futures positions reconciled, including unmanaged symbols.
- [ ] Open orders and pending orders reconciled.
- [ ] Position mode, margin mode and leverage verified; never changed automatically.
- [ ] Risk limits are positive, valid and loaded from the active version.
- [ ] Kill switch and venue halt state are clear.
- [ ] Durable order/risk journal and recovery worker are healthy.
- [ ] Python is the only process with trade-capable credentials and order submission authority.
- [ ] START confirmation is valid and bound to current config version.
- [ ] Trading state transitions STOPPED → START_PENDING → STARTING → RUNNING are audited.
- [ ] Process restart never infers RUNNING from process liveness; default recovery is STOPPED until readiness is re-established.

## 7. STOP and order-admission barrier
- [ ] STOP first commits durable STOPPING state before acknowledging success.
- [ ] All executor instances observe STOP barrier before admitting a new entry.
- [ ] In-flight entry race is covered by a fencing/version check immediately before submit.
- [ ] STOP returns incomplete if any instance has not acknowledged the barrier.
- [ ] New entry submissions are rejected while STOPPING, STOPPED or ERROR_HALTED.
- [ ] Existing positions remain managed; no automatic force-close.
- [ ] Existing open-order handling follows persisted policy.
- [ ] STOP is idempotent and safe under duplicate Telegram updates.
- [ ] STOP remains effective through Redis outage and process restart using PostgreSQL authority.
- [ ] Operator receives truthful state and per-instance acknowledgement summary.

## 8. Go public scanner functions
- [ ] Scanner uses public market-data endpoints only; no private account endpoints.
- [ ] Scanner never holds trade-capable credentials.
- [ ] `check_open_positions=false` is enforced for the Go scanner.
- [ ] Candidate/Redis signal is never accepted as order authorization.
- [ ] Signal publication remains disabled until Python sole-consumer proof is complete.
- [ ] Venue adapters validate market metadata, symbol type, quote asset and freshness.
- [ ] Funding, contract size and instrument metadata are authoritative; no guessed values.
- [ ] IDR/USDT conversion uses a live FX feed with freshness, spread and fee checks.
- [ ] Scanner handles rate limits, timeouts, malformed books and stale data safely.
- [ ] Scanner restart and Redis reconnect do not create duplicate or stale trade authorization.
- [ ] Scanner Telegram surface is read-only alerting.

## 9. Python executor and exchange functions
- [ ] Python is the only account-state authority and only order submit/cancel authority.
- [ ] Balance reads fail closed on missing, malformed, non-finite or stale values.
- [ ] Futures position reconciliation is account-wide and includes unmanaged positions.
- [ ] Open orders, partial fills, cancelled/rejected orders and ambiguous acknowledgements reconcile.
- [ ] Client order IDs are deterministic/idempotent where venue supports them.
- [ ] No blind retry after ambiguous order acknowledgement; reconcile by client order ID first.
- [ ] Contract size, precision, min amount/notional and tick size come from fresh venue metadata.
- [ ] Position mode and leverage are verified before entry; no automatic account setting changes.
- [ ] OKX Hedge Mode close side/posSide parameters are verified against actual adapter behavior.
- [ ] One-way venues use correct reduce-only close semantics.
- [ ] Entry sizing checks both legs, balances, exposure caps, fees, slippage and available capital.
- [ ] Daily loss, exposure, order count, TTL, refresh and failure breakers are enforced.
- [ ] Every order lifecycle transition is durably journaled before/after external side effects as designed.
- [ ] Recovery after restart reconciles exchange state before any new entry.
- [ ] No order is sent when DB journal, outbox/recovery, or reconciliation is unhealthy.
- [ ] No real exchange order is used in development/CI.

## 10. Multi-CEX and route functions
- [ ] Venue instance IDs are unique; exchange brand is not treated as a unique account.
- [ ] Multiple accounts for the same exchange are isolated.
- [ ] Route references resolve to the correct spot and futures venue/account pair.
- [ ] Spot quote assets and futures settle assets are compatible.
- [ ] Only explicitly enabled routes/symbols can be considered.
- [ ] Unsupported market types/capabilities fail closed.
- [ ] Venue halt is scoped correctly and cannot leak across unrelated accounts.
- [ ] Rate limits and concurrency controls are scoped per venue/account/API key.
- [ ] Duplicate route or symbol mapping is rejected.
- [ ] Route-level and account-level exposure limits are both enforced.

## 11. Functional regression scenarios
- [ ] Clean first boot with no active config: process may be healthy, trading remains STOPPED.
- [ ] Restart with valid active config: same version/hash restored, trading remains STOPPED unless explicitly restarted.
- [ ] Restart with corrupt snapshot/hash mismatch: fail closed.
- [ ] PostgreSQL unavailable at startup/runtime: no LIVE admission.
- [ ] Redis unavailable at startup/runtime: no LIVE admission; recover via PostgreSQL refetch.
- [ ] Stale cache/invalidation: reload DB version and atomically swap.
- [ ] Two concurrent config writers: one CAS succeeds, stale writer is rejected.
- [ ] Outbox publisher crashes after publish but before marking delivered: duplicate is harmless.
- [ ] One executor instance misses notification: reconciliation catches it and blocks convergence.
- [ ] Unauthorized Telegram user, authorized user in wrong chat, empty allowlist: all denied.
- [ ] Duplicate Telegram update and duplicate command ID: exactly one side effect.
- [ ] START without readiness or expired/wrong-version confirmation: denied.
- [ ] STOP during entry preparation and immediately before submit: no new order admitted after barrier; the final pre-submit DB check exists, but transactional STOP fencing is still pending.
- [ ] STOP while position/open orders exist: policy honored; no force-close.
- [ ] Venue API timeout/429/5xx/malformed response: no unsafe fallback.
- [ ] Partial fill and ambiguous acknowledgement: reconcile, never blind resubmit.
- [ ] Unknown external position/order: venue halted for reconciliation.
- [ ] Live FX stale/wide spread/missing: IDR↔USDT LIVE route blocked.
- [ ] Process crash at every state transition: durable state recovers safely.

## 12. Observability and operator evidence
- [ ] Structured logs include correlation/request/command IDs and config version, never secret values.
- [ ] Metrics cover DB/Redis health, active config version, outbox lag, consumer acknowledgements and readiness gates.
- [ ] Alerts cover ERROR_HALTED, stale FX, reconciliation mismatch, failed STOP barrier and config divergence.
- [ ] Dashboard/status endpoint distinguishes process liveness from trading-enabled state.
- [ ] Operator status shows why START is blocked and which checks failed.
- [ ] Audit history is queryable and append-only where required.
- [ ] Runbooks cover DB restore, Redis rebuild, config rollback, credential rotation and venue halt.

## 13. LIVE canary release gate
- [!] All CI jobs pass on the exact release SHA.
- [!] All mandatory runtime checklist items above have reproducible evidence.
- [!] Authenticated exchange smoke tests are read-only and approved separately.
- [!] Paper/sandbox soak and failure-injection/restart drills pass.
- [!] Python sole-consumer and credential isolation are proven.
- [!] Manual operator START/STOP drill passes, including multi-instance STOP barrier.
- [!] Risk limits are reviewed and approved by the operator; migration does not alter them.
- [!] LIVE FX, venue metadata, balances and reconciliation are fresh at canary time.
- [!] Explicit operator approval is recorded before any real order.
- [!] Initial canary has strict size/route/venue scope, monitoring and immediate STOP procedure.

## Evidence register

| Date (UTC) | Candidate SHA | CI run | Checklist section | Evidence / artifact | Result | Reviewer |
|---|---|---|---|---|---|---|
| 2026-11-13 | `850db4e92f57bb26cb78293f3ab7bbc8a423f0fd` | `43433436237` | Outbox publisher + regression tests | Python, Go, Docker images and PostgreSQL migration/idempotency/RLS jobs all succeeded | Pass | Pending |
| 2026-11-13 | `81e9196b54838ab105ac1e7166bc042f9459659d` | `43434420893` (earlier head only) | Config invalidation + consumer leases + convergence API | Added PostgreSQL consumer registry, process restart resets to STARTING, DB refetch/Redis mirror/atomic swap/ack path, periodic reconciliation, lease-scoped convergence API and SQL smoke assertions. Run `43434420893` was triggered at earlier head `058ee031`; latest SHA has no confirmed CI run yet | Pending | Pending |

**Current release decision:** NOT LIVE-READY. The PostgreSQL admission gate and fail-closed engine hooks are implemented, including a final pre-submit recheck. The gate is not yet injected by executor startup because the V2-to-legacy runtime config mapping and cutover are not complete; therefore LIVE entry is intentionally denied. Transactional STOP fencing and deployed multi-process proof remain outstanding.


## 14. Runtime entry admission implementation notes
- The PostgreSQL admission gate requires operator state RUNNING/trading_enabled=true, the local snapshot to match the active DB version and hash, signals_enabled=true, and every live executor lease to acknowledge the same version/hash.
- The engine checks the gate before entry preparation and again immediately before the spot order API call. Missing gate or any check error denies LIVE entry; MOCK behavior is unchanged.
- The current V2 snapshot validator still requires signals_enabled=false pending sole-consumer proof, so the admission gate correctly remains closed under the present contract.
- This is not yet a transactional fencing lock shared with STOP. Do not interpret the final DB read as proof that a concurrent STOP can never race an external submit.
