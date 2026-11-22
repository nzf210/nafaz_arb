# NAFAZ Executor V3 — Durable Data Model

Status: PR-01 design artifact — review required before migration implementation.
Repository: nzf210/nafaz_arb
Scope: Executor only. The Go scanner and its opportunity contract are frozen.

## 1. Authority and compatibility rules

- PostgreSQL is the canonical authority for execution sessions, legs, policy snapshots, state transitions, intents, orders, fills, hedge obligations, positions, exits and risk decisions.
- Redis is a rebuildable cache/wakeup/notification layer. It must not be the only source from which an active position or pending order can be reconstructed.
- Supabase REST and the SQLite reporting outbox are projections/reporting paths, not competing writers for canonical execution state.
- Existing tables `execution_sessions`, `execution_legs` and `pair_pnl_snapshots` are reused. Do not create parallel session or leg tables.
- Existing legacy columns and Redis keys remain readable during a controlled migration. Do not destructively remove or reinterpret them in PR-01.
- A schema migration in Git does not prove it has been applied to any deployed database.

## 2. Identity model

A mutable pair label is display metadata, never the primary identity.

### Execution session

| Field | Meaning | Rule |
|---|---|---|
| session_id | Executor-generated immutable session ID | Primary key; stable across restart |
| source_opportunity_id | Scanner opportunity ID | Preserve exact source identifier |
| admission_key | Executor idempotency key for one accepted dispatch | Unique; retries of the same dispatch resolve to the same session |
| account_scope_id | Non-secret account/credential scope identifier | No credential material |
| pair_key | Human-readable pair label | Display/search only |
| state / state_version | Materialized session state and monotonic version | Updated with compare-and-swap |
| fencing_generation | Current ownership generation | Every state mutation checks it |
| policy_snapshot / policy_snapshot_hash | Immutable execution policy used at admission | Hash verifies canonical snapshot bytes |
| created_at / updated_at / completed_at | Lifecycle timestamps | UTC timestamptz |

The exact admission-key composition must be deterministic and documented by the admission service. It must include the source dispatch identity and account/route scope; it must not be based only on pair_key. The same dispatch retried after a timeout must not create a second session. A genuinely new scanner dispatch must have a distinct source dispatch identity.

### Leg / venue identity

Each session has one logical Spot leg and one logical Futures leg. Each leg preserves:

- leg_id, session_id, leg_type;
- account_scope_id, venue, market_type;
- native_symbol exactly as submitted to the venue;
- normalized_symbol for matching/reporting only;
- base_asset, quote_asset and settle_asset;
- contract_type, contract_size and position_side for derivatives;
- quantity_unit and explicit native-order quantity versus base-equivalent quantity;
- target, filled, remaining and verified-exposure quantities;
- leg state and monotonic state_version.

The local Spot identity (venue + native symbol + quote asset, e.g. IDR) and foreign Futures identity (venue + native contract symbol + settlement asset) are independent. Never infer one venue's native symbol from the other leg or from pair_key.

## 3. Existing schema gap

The V2 migration `supabase/migrations/20261114000100_add_executor_v2_core.sql` already creates:

- `execution_sessions(session_id, opportunity_id, pair_key, strategy_snapshot, status, owner_fencing, timestamps)`;
- `execution_legs(leg_id, session_id, leg_type, venue, market_type, symbol, target_qty, filled_qty, exposure_qty, status, timestamps)`;
- `pair_pnl_snapshots`;
- `command_journal`.

These are useful foundations, but the current schema does not itself establish:
- a unique dispatch/admission idempotency key;
- an account scope on session and leg identity;
- separate native and normalized market identity / quote and settlement assets;
- typed native order quantity versus base-equivalent exposure quantity;
- monotonic state versions and transition-event history;
- an immutable, hash-verifiable policy snapshot contract;
- a fencing generation typed as a monotonic numeric generation rather than an opaque owner string.

The current `target_qty`, `filled_qty` and `exposure_qty` columns are NUMERIC, but their unit semantics are not explicit. PR-01 must not silently reinterpret these columns.

## 4. Proposed additive model

The migration should extend the existing tables, not replace them. Proposed logical additions:

### execution_sessions
- `admission_key TEXT NULL` for legacy rows; create a partial unique index over non-null values after duplicate analysis; never fabricate keys for legacy sessions.
- `account_scope_id TEXT`;
- `state_version BIGINT NOT NULL DEFAULT 0`;
- `fencing_generation BIGINT NOT NULL DEFAULT 0`;
- `policy_snapshot JSONB NULL` during compatibility rollout; require a valid non-null snapshot only for newly admitted V3 sessions after the V3 admission writer is enabled; do not backfill legacy rows with `{}`;
- `policy_snapshot_hash TEXT`;
- `state_reason TEXT`;
- `source_dispatch_id TEXT` where the scanner contract supplies a distinct dispatch identity; otherwise preserve source_opportunity_id without changing the scanner.

Keep `opportunity_id`, `pair_key`, `strategy_snapshot`, `status`, and `owner_fencing` during compatibility. Define one canonical mapping before deprecating any field.

### execution_legs
- `account_scope_id TEXT`;
- `native_symbol TEXT`, `normalized_symbol TEXT`;
- `base_asset TEXT`, `quote_asset TEXT`, `settle_asset TEXT`;
- `contract_type TEXT`, `contract_size NUMERIC`, `position_side TEXT`;
- `quantity_unit TEXT`;
- `target_order_qty NUMERIC`, `filled_order_qty NUMERIC`, `remaining_order_qty NUMERIC`;
- `target_base_qty NUMERIC`, `filled_base_qty NUMERIC`, `verified_exposure_base_qty NUMERIC`;
- `state_version BIGINT NOT NULL DEFAULT 0`;
- `state_reason TEXT`.

Retain the legacy quantity columns until all readers/writers have an explicit migration mapping. Do not dual-write values with ambiguous units.

### execution_session_events
Append-only aggregate transition journal:
- event_id (primary key);
- session_id (foreign key with `ON DELETE RESTRICT`; execution history is not cascade-deleted);
- aggregate_version (monotonic and unique per session);
- entity_type (`SESSION` or `LEG`), entity_id, entity_version (the version of the changed session/leg), from_state, to_state, event_type;
- actor_id / worker_id and fencing_generation;
- correlation_id, idempotency_key;
- event_payload JSONB;
- occurred_at.

The changed session/leg row, aggregate version increment, event insert and transactional outbox insert must commit in one PostgreSQL transaction. Unique (session_id, aggregate_version), unique (session_id, idempotency_key), and unique event_id prevent replay duplication. A leg transition increments that leg's entity_version and the parent session's aggregate_version; a session transition increments both its entity_version and aggregate_version.

## 5. State and transition invariants

- Every state mutation is a compare-and-swap on expected state_version and current fencing_generation.
- Session and leg versions are monotonic; stale workers cannot overwrite newer state.
- Every accepted session has exactly one Spot leg and one Futures leg, unless a documented admission rejection occurs before leg creation.
- A session is not COMPLETED merely because both orders are terminal. Completion also requires verified zero managed exposure, no open/unknown orders, no unresolved hedge obligation/allocation, and final reconciliation.
- FAILED or ABORTED does not imply flat exposure.
- Unknown or missing exchange evidence is UNKNOWN, never zero.
- Session state is a materialized projection of independent leg/order/fill/obligation/exposure state; it is not an independent source that may contradict those domains.

## 6. Immutable policy snapshot

At admission, persist a canonical snapshot containing:
- strategy parameters and hedge ratio;
- execution mode and order policy;
- venue/account/market identities;
- fee schedule values, source, verification status and timestamp;
- exchange market metadata version, precision, lot/contract size and minimums;
- FX rate, source, timestamp and validity;
- risk limits and account scope;
- exit target, stop, trailing and time-decay policy;
- position mode and leverage assumptions.

Persist canonical JSON plus a stable cryptographic hash (canonicalization algorithm/version must be pinned, e.g. RFC 8785 JSON Canonicalization Scheme, with SHA-256). Hash input must include an explicit schema/version domain separator. The snapshot is immutable for the session. Policy changes apply only to new sessions unless a separately audited migration explicitly updates an existing session. Secrets and API credentials must never be included.

## 7. Migration and backfill rules

1. Inspect complete current DDL, constraints, indexes, RLS and applied migration evidence before writing SQL.
2. Backfill legacy session rows with provenance; do not invent account scope, dispatch identity, native quote/settlement, contract metadata, admission keys or policy snapshots. Keep unknown legacy fields NULL.
3. Rows with ambiguous identity/quantity units remain marked legacy/unknown and are not silently promoted to V3-ready sessions.
4. Add nullable columns first; validate/backfill only values with evidence; add constraints and uniqueness only after duplicate analysis. For admission idempotency, use a partial unique index on non-null admission_key so legacy NULL rows remain valid.
5. Keep the migration additive and reversible where practical. Do not drop old columns or Redis keys in PR-01. Do not add a default `{}` policy snapshot that makes legacy sessions appear V3-complete. Avoid `ON DELETE CASCADE` from sessions to legs/events/orders/fills; use RESTRICT and explicit audited retention/archive workflows.
6. Verify migration replay and rollback in disposable PostgreSQL, plus existing Supabase migration CI.
7. Do not connect to production or apply a migration to the user's VPS as part of this PR.

## 8. PR-01 acceptance tests

- Same admission_key submitted concurrently creates one session and returns the same session identity.
- Different source dispatch identities do not collide even when pair_key is identical.
- Two account scopes using the same pair_key remain isolated.
- Stale state_version update is rejected without changing the row or event journal.
- Stale fencing_generation update is rejected.
- Session/leg update, aggregate version and event journal commit atomically; injected failure leaves no partial state.
- Spot IDR symbol and Futures USDT contract identity survive serialization/restart unchanged.
- Contract quantity and base-equivalent exposure remain distinct for non-unit contract sizes.
- Policy snapshot hash is stable, immutable, versioned and contains no secrets; legacy sessions remain snapshot-unknown rather than receiving fabricated `{}` snapshots.
- Legacy records with ambiguous unit or venue identity are preserved as legacy/unknown, not fabricated into complete sessions.

## 9. Explicitly out of scope

- Scanner code, opportunity payload, ranking, Redis publication and scanner configuration.
- Order submission/cancellation behavior (PR-03).
- Canonical fill-ledger redesign (PR-04).
- Hedge pooling/allocation (PR-05).
- New profit/exit thresholds or strategy changes (PR-07/PR-08).
- Runtime LIVE cutover, production migration or exchange operations.
