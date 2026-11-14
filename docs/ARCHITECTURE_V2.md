# Nafazbot V2 — Target Architecture Specification

**Status:** Target architecture. PostgreSQL config/operator-control foundations are present on PR #33; the multi-venue schema, strict config model, and execution persistence boundary are added by this implementation branch. Runtime cutover remains disabled.

## 1. Goals

- Support any number of spot and futures venues, including multiple accounts on one exchange.
- Support explicit many-to-many spot/futures routes without a singleton global futures exchange.
- Add venues through adapters and declared capabilities, not strategy rewrites.
- Keep public market discovery separate from private account state and order execution.
- Use local PostgreSQL as durable source of truth; Redis is transient infrastructure.
- Recover safely from process, host, Redis, database, and network failures.
- Fail closed on missing metadata, ambiguous symbols, stale account state, unknown order outcomes, and incomplete reconciliation.

## 2. Component boundaries

### Go scanner — public data only

Reads public instruments, books, tickers, funding and public metadata for every configured scanner venue. Normalizes data, calculates candidate opportunities, and publishes candidate events. It must not read private balances, positions, or open orders; hold trade-capable credentials; or submit, amend, or cancel orders. Candidate messages are not order authorization. Private account checks do not belong in the scanner; legacy check_open_positions must be disabled/removed from its runtime contract.

### Python executor — sole account and execution authority

Only the executor receives trade-capable credentials. It reads balances, positions, open orders, leverage and position mode; reconciles exchange state with PostgreSQL; validates candidate freshness, route, risk, funds, exposure, metadata and capabilities; submits/manages orders; handles partial fills, ambiguous acknowledgements, rollback/hedge recovery and exits; and persists every state transition. It must reconcile before accepting new entries. Withdrawal permissions are prohibited.

### Redis — transient coordination

Used for candidate delivery, queues, short-lived caches and coordination. It is not authoritative for balances, orders, fills, positions, risk state or execution journal. Queue delivery is at-least-once; consumers deduplicate durably. Redis loss must not erase accepted order state.

### PostgreSQL — durable source of truth

Local PostgreSQL stores configuration versions/references, execution intents, idempotency keys, orders, fills, position/balance snapshots, reconciliation, risk state, halts, operator actions, append-only execution journal and transactional outbox. Use migrations, constraints, transactions, UTC timestamps, indexes and retention policies. Persist financial values as NUMERIC/decimal. Never store plaintext exchange secrets.

## 3. Domain model

- Exchange: vendor identity, such as OKX or Indodax.
- Venue: configured endpoint/product family, such as OKX linear perpetual or OKX spot.
- Account: one logical account at an exchange; multiple accounts may exist per exchange.
- Market: instrument on a venue, including native symbol and canonical base/quote.
- Route: exactly one permitted spot leg and one futures leg, with account references and risk profile.
- Strategy: trading policy applied to one or more routes.

Venue ID, account ID, market ID and route ID are distinct. Durable order/fill/position records must carry venue/account identity; route identity where applicable. Never key state only by exchange name or symbol. A unified exchange account must not be duplicated in a way that double-counts exposure.

## 4. Adapter contract

Each adapter declares supported capabilities explicitly. Required dimensions include market type; public market methods; executor-only private methods; order types; post-only/reduce-only/client-order-ID behavior; client ID uniqueness scope; base-vs-contract quantity; contract size; amount step/minimum; price tick/min notional; position mode; margin mode/leverage; rate limits; pagination; timestamp behavior; retry safety and lookup capabilities.

Adapters must never guess contract size, precision, minimums, funding intervals or position mode. Normalize to canonical base exposure while retaining native contract quantity. Reject missing, stale, contradictory or ambiguous metadata. Distinguish definitive rejection from ambiguous timeout/acknowledgement. Never blindly retry order creation after an ambiguous result; reconcile by client ID/order ID first. Require documented-payload fixtures and regression tests. A route may use only capabilities verified by tests.

## 5. Route eligibility

Routes are explicit; do not automatically enable every Cartesian pairing. A route is eligible only when both referenced venues/accounts exist and are enabled; leg market types are correct; symbols resolve unambiguously; base assets match; quote/FX conversion is explicitly supported and fresh; required adapter capabilities and contract metadata exist; account state is reconciled; risk limits pass; and neither account nor route is halted. Many spot venues may pair with many futures venues, and one venue may serve many routes.

## 6. Durable execution lifecycle

Minimum lifecycle: RECEIVED → VALIDATING → AUTHORIZED → SUBMITTING → ACKNOWLEDGED / PARTIALLY_FILLED / FILLED; cancellation flows through CANCEL_PENDING → CANCELED. Terminal or recovery states include REJECTED, UNKNOWN, RECOVERY_REQUIRED and CLOSED.

Persist intent and idempotency key before external submission. Record client order ID and exchange order ID. A timeout means UNKNOWN, not failed; reconcile before retry. Persist each fill idempotently. Commit state change and outbox event in one PostgreSQL transaction, then publish to Redis. Consumers deduplicate using durable unique constraints.

Startup order: validate config; connect to PostgreSQL and verify migrations; load venue/account/route registry; initialize executor clients; reconcile balances, positions and open orders; resolve unknown submissions and unfinished intents; verify leverage/position mode without silently changing account settings; then enable only routes that pass readiness.

A database outage blocks new entries. Existing exposure still requires best-effort monitoring and a documented recovery procedure; never silently fall back to Redis or JSON as the source of truth.

## 7. Suggested PostgreSQL entities

schema_migrations; exchanges; venues; accounts; venue_capabilities; markets; market_metadata_snapshots; strategies; trading_routes; route_risk_limits; opportunities; execution_intents; orders; order_fills; balance_snapshots; position_snapshots; reconciliation_runs; risk_events; venue_halts; operator_actions; execution_journal; outbox_events; consumer_offsets.

Use unique constraints for venue/account/exchange order identity and client-order idempotency within the exchange's documented scope. Keep exchange event time and ingestion time separately. Retain raw payloads only when useful and redacted.

## 8. Security and LIVE boundary

Go scanner receives no private account secrets. Only Python executor receives trade-capable credentials via environment/secret injection. No withdrawal/transfer permission. Secrets never appear in tracked files, logs, traces, fixtures or database rows. LIVE requires valid PostgreSQL, migrations, positive hard risk limits, adapter readiness and successful startup reconciliation. Global LIVE mode does not make every route ready. Keep executor signals disabled until Python is verified as the sole trading consumer. Any uncertain account state, ambiguous order outcome, missing metadata or database write failure blocks new entries.

## 9. Observability and operations

Metrics/logs are tagged by venue/account/route, never by secrets: market-data freshness, adapter errors, queue age, outbox backlog, order latency, unknown orders, reconciliation drift/age, exposure, daily PnL, halts, DB health and migration version. Back up PostgreSQL and test restore. Redis can be rebuilt from durable state and fresh market data.

## 10. Delivery phases and acceptance

1. Approve architecture and config contract; no runtime changes.
2. Add PostgreSQL migrations, repositories, journal and transactional outbox; prove restart/idempotency.
3. Add venue/account/market/route registry and strict validation; test multiple spot/futures and multiple accounts.
4. Normalize current Indodax, Tokocrypto and OKX adapters against capability contract.
5. Fan out scanner public discovery across eligible venues.
6. Fan out executor clients and reconciliation per account/route.
7. Run read-only LIVE smoke tests per account before operator-authorized live orders.

Acceptance tests include at least two spot and two futures venues concurrently; multiple accounts on one exchange; differing symbols/contract sizes; isolated failure of one venue; duplicate queue delivery; restart/Redis loss/DB reconnect; ambiguous acknowledgement; partial fill/cancel/hedge failure; reconciliation mismatch; invalid config and secret-leak checks.

## 11. Current-state gaps

The legacy runtime still starts from config.json through ExecutorConfig.load and constructs one configured local/global pair. Existing Supabase tables, SQLite outbox, Redis recovery keys and singleton exchange settings remain compatibility paths. The V2 tables and repository introduced by the implementation branch are additive and are not yet the runtime source of truth.


## 12. Operator control plane and durable configuration activation

The control plane is a separate Python-owned service/module alongside the executor. The Go scanner's Telegram integration is alert/read-only only: it must not own trading state, authorize START, or be the authority for STOP. The Python control plane owns Telegram polling, PostgreSQL writes, and publication of control events. The executor remains the only process that can submit/cancel exchange orders.

### Telegram command contract

Use explicit commands that do not collide with Telegram's built-in `/start`: `/trade_status`, `/trade_stop <reason>`, `/trade_start <reason>`, and `/trade_confirm <confirmation_id>`. Require both an allowlisted Telegram user ID and allowlisted chat ID for every mutating command. Empty allowlists mean deny all; never fall back to "authorized chat implies authorized user". Resolve bot token only from a secret reference/environment at runtime; never store its value in PostgreSQL, JSON, logs, callback data or audit rows.

Every update is deduplicated by Telegram `update_id` and persisted with command ID, actor user/chat IDs, command, reason, received/committed timestamps, previous/new state, config version, result and status. Replays return the persisted result and never repeat the transition. Expose pending/confirmed/rejected/completed status. START requires a short-lived, one-time confirmation bound to the actor, chat, requested config version and confirmation ID. STOP is always available to an authorized operator and is never delayed by START confirmation.

### Durable trading state

`process_alive` and `trading_enabled` are independent. The singleton PostgreSQL control row starts disabled and is authoritative across restarts. Executor startup always loads it; it must never infer enabled from process liveness, LIVE mode, Redis contents or a prior in-memory value. States are `STOPPED`, `START_PENDING`, `STARTING`, `RUNNING`, `STOPPING`, and `ERROR_HALTED`. Only a successful readiness transaction may move STARTING to RUNNING.

STOP first commits STOPPING (which immediately denies new entry authorization), broadcasts a versioned control event, and waits for all executor instances to close their entry-admission gates and drain/resolve already-submitting entry requests. It then commits STOPPED. If Redis or an instance is unreachable, the durable STOPPING state continues to deny entries on reconnect/restart; the command must report that the distributed stop barrier is incomplete rather than claim fully stopped. STOP does not close positions. Open-order policy is explicit and versioned: default `keep_and_manage` (retain and continue managing existing orders/positions); optional `cancel_unfilled_entries` requires separate operator policy and reconciled cancellation. No automatic force-close or position-closing orders.

START may move to RUNNING only if PostgreSQL/migrations, active config version/hash, Redis cache version, secret references, venue credentials, market metadata, live FX source freshness/spread/fees, account/order/position reconciliation, risk limits, all venue/route halts and kill-switch state pass. A failed or unknown check leaves trading disabled and records a reason. LIVE mode alone never enables trading.

### Configuration versioning and cache protocol

PostgreSQL is authoritative for all non-secret operational configuration, including venue/account/route/strategy/risk definitions, feature flags, FX source and thresholds, execution/order policies, and Telegram allowlists/policies. Store only secret reference names/identifiers. Credential and Telegram token values remain in an external secret manager or injected environment.

Each proposed snapshot is immutable and versioned. Validate schema, references, adapter capabilities, risk invariants, secret-reference names and LIVE FX constraints before activation. In one transaction: compare expected active version (optimistic concurrency), insert the immutable snapshot and content hash, append actor/time/reason/diff audit, advance active version, and insert an outbox event. After commit, publish the version/invalidation event to Redis. Redis contains a versioned cache copy only. Every service loads the active snapshot from PostgreSQL at startup/restart, validates it and its hash, writes/refreshes Redis, and atomically swaps its in-process immutable snapshot. On event, a service fetches PostgreSQL (not Redis) and only swaps after validation. If DB is unavailable, hash/version cannot be verified, or Redis cannot be refreshed/verified for a LIVE service, fail closed for new entries; never silently use stale config. Outbox replay repairs lost Redis notifications.

Rollback creates a new version whose snapshot is copied from a previously validated version; it never rewinds/deletes version history. Require expected active version and explicit actor/reason. Multi-instance reload acknowledgements and active-version metrics make partial propagation visible. Configuration changes never mutate an already loaded object in place.

### Deployment boundary

This is a target contract until runtime wiring, migration application and integration tests are complete. Existing JSON/SQLite/Redis/Supabase paths are legacy compatibility paths, not silently promoted to authoritative V2 config. Cutover must be explicit and fail closed; do not dual-write conflicting authorities.


## 13. V2 PostgreSQL foundation implementation boundary

The implementation branch adds an additive timestamped migration, `20261114000100_v2_multi_venue_execution_core.sql`, after the config/control migrations. It creates V2-prefixed exchange, venue, account, secret-reference, capability, market, risk-profile, route, execution-intent, execution-journal, balance-snapshot, position-snapshot, reconciliation-run and venue-halt tables. Monetary, price, quantity, fee, PnL, contract-size, step and notional fields use PostgreSQL `NUMERIC`. Foreign keys, unique idempotency keys, route market-type checks, disabled-by-default flags and append-only journal protection are included. Nullable V2 identity columns are added to the existing order recovery registry without guessing or backfilling legacy identities.

Python now has a typed V2 domain model and strict validator. It rejects unknown nested keys, duplicate and dangling references, inconsistent account/venue links, spot/perpetual market-type mismatches, invalid risk limits, malformed secret references, private scanner configuration and invalid LIVE FX policies. An enabled route also requires a code-supplied capability registry proving the required private reads, order/cancel/lookup semantics and multi-venue/multi-account support. The production registry is intentionally empty until adapter conformance tests and evidence are supplied; therefore an enabled route fails closed by default. Disabled routes remain valid configuration candidates.

`PostgresExecutionRepository` provides a persistence boundary for idempotent intent creation and compare-and-swap state transitions. Each successful write appends the journal record and transactional outbox event in the same PostgreSQL transaction. Execution events are published to a separate Redis channel; Redis remains delivery-only. The repository is not yet wired into the live order submission path, so this branch does not claim that current exchange orders are persisted in PostgreSQL.

### Explicitly not cut over

- `executor/main.py` still loads the legacy `config.json`; no V2 snapshot is activated or substituted for the user's current configuration.
- No legacy secrets are copied, printed, encrypted, or migrated by this change. V2 stores references only.
- The scanner remains public-market-data-only. It does not receive private account credentials or order authority.
- No exchange client, account setting, risk limit, execution mode, or LIVE deployment setting is changed.
- No live order is submitted, canceled, amended, or force-closed.

Remaining work includes the verified production adapter capability matrix, explicit v1-to-v2 candidate mapping for each account/route, secret-reference resolver, runtime injection and PostgreSQL startup integration, fill/order lifecycle migration, a transactional STOP-vs-submit fence, and deployed multi-process/restart/outbox-consumer tests. Until those are complete, V2 configuration is a validated foundation only and LIVE admission remains closed.

### V2 persistence invariants

- Perpetual market metadata must include a positive contract size; spot market rows must not carry a futures contract size.
- Before accepting a durable execution intent, PostgreSQL verifies that the venue/account is a route leg, the canonical symbol matches the venue-bound market and route allowlist, the spot quote or futures settle asset is allowed by that route, and both route legs still have verified capability evidence.
- Capability evidence revocation therefore blocks subsequent intent creation even if an earlier route row remains enabled.
- Position snapshots are append-only observations. An exchange position ID is indexed for lookup but is not unique across time, so successive reconciliation runs can retain history.
- CI exercises the repository against a real PostgreSQL instance inside a rollback-only transaction, in addition to migration replay and SQL schema smoke tests.

## 14. V2 control-plane runtime and restart semantics

The Python executor has an opt-in bootstrap (`NAFAZ_V2_CONTROL_ENABLED=true`). It reads the bootstrap PostgreSQL DSN from the environment name in `NAFAZ_V2_DATABASE_DSN_ENV` (default `NAFAZ_DATABASE_URL`), fetches and validates the active immutable V2 snapshot, resolves the Redis URL through the snapshot's `redis.url_env`, and verifies the Redis mirror by read-back. The Redis endpoint must match the legacy executor's configured Redis endpoint; a mismatch aborts V2 bootstrap. Secrets are never logged.

Every successful executor bootstrap takes the exclusive operator-entry advisory lock and writes durable `STOPPED / trading_enabled=false` before registering the new process lease. It also rejects pending START confirmations from the previous process. Process liveness is then reported separately through `config_consumers`; a live process does not imply trading is enabled. An operator must issue a fresh Telegram START after restart.

The runtime starts the PostgreSQL-authoritative config invalidation consumer, transactional outbox publisher and (when enabled and its token resolves) the Python Telegram long-polling service. The Telegram bot uses both user-ID and chat-ID allowlists. A `bot_token_env` reference is resolved directly from that environment variable; a `bot_token_ref` is currently injected through `NAFAZ_TELEGRAM_BOT_TOKEN` until a dedicated external secret-manager adapter is installed. The token is never logged.

The entry gate verifies the current Redis snapshot on each admission and again while holding the shared PostgreSQL submission fence. STOP and config activation take the matching exclusive fence. Thus Redis outage, stale cache, PostgreSQL outage, config-version mismatch, consumer divergence or STOP prevents a new spot entry. The exchange call is made while the shared fence is held; existing position management and exits do not take this entry fence. STOP currently supports only `keep_and_manage`: it neither cancels open orders nor closes positions.

### Current runtime boundary

This is a control-plane integration, not the complete V1-to-V2 config cutover. `executor/main.py` still loads the existing `config.json` for exchange adapters and legacy strategy/execution parameters. The V2 PostgreSQL snapshot is authoritative for V2 validation, operator policy, cache convergence and entry admission, but its venue/account/route/strategy/risk fields are not yet mapped into the live adapter factory. No automatic V1 config migration or credential copy occurs.

START remains intentionally unavailable in the current binary: the V2 validator continues to require `execution.signals_enabled=false`, the production adapter capability registry is empty, and no runtime readiness provider currently proves credentials, multi-venue market metadata, live FX freshness/spread/fee bounds, complete account reconciliation, risk and kill-switch checks, or Python-only order authority. The Telegram confirmation path reports these failed gates and leaves the durable state STOPPED. Do not set LIVE signals true or treat the process as LIVE-ready until those proofs and smoke tests are implemented and reviewed.

If V2 bootstrap is disabled or fails, the executor process may remain alive for monitoring/recovery, but the LIVE admission gate is absent and new LIVE entries fail closed. MOCK behavior remains unchanged. No user configuration is rewritten by bootstrap.
