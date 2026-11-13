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
- [ ] Verify snapshot canonical JSON and SHA-256 against independent fixtures.
- [ ] Verify unsupported schema, malformed JSON, non-finite numbers and hash mismatch fail closed.
- [ ] Verify raw secret values are rejected; only secret references are stored.
- [ ] Verify unknown keys, duplicate IDs and dangling venue/account/route/risk references fail closed.
- [ ] Verify create-draft, validate, activate, and rollback are transactional.
- [ ] Verify optimistic concurrency rejects stale expected-version writes.
- [ ] Verify concurrent activations cannot create two active pointers or lose audit records.
- [ ] Verify immutable snapshot fields cannot be updated or deleted by runtime role.
- [ ] Verify every config mutation writes actor, timestamp, reason, diff, audit and outbox in one transaction.
- [ ] Verify restart loads the active PostgreSQL version, never a local stale file.
- [ ] Verify DB outage prevents LIVE startup and new order admission.
- [ ] Verify recovery after DB outage does not auto-start trading.

## 3. Redis cache, notification and outbox
- [x] Redis is specified as transient cache/transport, not the configuration authority.
- [x] Snapshot loader fetches PostgreSQL first and verifies Redis read-back.
- [ ] Verify Redis payload includes version, schema version and content hash.
- [ ] Verify stale, malformed, wrong-version and wrong-hash Redis payloads are rejected.
- [ ] Verify Redis outage blocks LIVE readiness (no stale-cache fallback).
- [ ] Verify invalidation causes a PostgreSQL refetch before in-memory swap.
- [ ] Verify in-memory snapshot swap is atomic; active requests never observe partial config.
- [ ] Verify outbox publisher claims rows safely across multiple workers.
- [ ] Verify publish retry/backoff, poison-event handling and replay after Redis outage.
- [ ] Verify duplicate outbox delivery is idempotent at consumers.
- [ ] Verify every executor instance acknowledges the activated version.
- [ ] Verify partial propagation is visible and blocks new LIVE entries until convergence.
- [ ] Verify old cache keys and snapshots expire only after all consumers have advanced.

## 4. Runtime configuration cutover
- [ ] Map every legacy config.json/environment field to V2 or an explicit deprecation decision.
- [ ] Identify all readers of legacy JSON, environment variables, SQLite/JSONL and Supabase.
- [ ] Define a one-way cutover; no silent dual authority or conflicting dual writes.
- [ ] Add explicit compatibility mode and migration report.
- [ ] Compare legacy and V2 effective configuration without exposing secret values.
- [ ] Prove risk limits and venue settings are unchanged by migration.
- [ ] Prove missing V2 config never silently falls back to legacy config in LIVE.
- [ ] Wire verified PostgreSQL load into startup only after migration/cutover tests pass.
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
- [ ] STOP during entry preparation and immediately before submit: no new order admitted after barrier.
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
| 2026-11-13 | `d6fbd474d15dc95db1ba04337053f178352740ef` | `43431989761` | Config snapshot reader | CI in progress at checklist update | Pending | Pending |

**Current release decision:** NOT LIVE-READY. The V2 database schema and snapshot-reader foundation do not yet constitute a deployed, verified trading control plane.
