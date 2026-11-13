# Nafazbot V2 — Target Architecture Specification

**Status:** Proposed design baseline; not yet implemented by the current runtime.

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

The baseline repository describes a Go read-only scanner and Python executor, but also describes JSON-file scanner history, a Supabase-oriented executor outbox, and executor settings centered on local_exchange_name/global_exchange_name. Those are not yet a PostgreSQL-backed multi-venue registry. This document is a target design, not an implementation claim.
