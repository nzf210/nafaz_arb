# NAFAZ Executor V3 — Durable Fill Lifecycle

Status: PR-04 implementation; isolated from the V2 runtime.
Scope: confirmed-fill persistence and durable private-event intake only.

## Ownership boundary

- PostgreSQL is canonical for V3 fill facts, inbox identity, leg fill totals, and session event order.
- This stage accepts confirmed ENTRY trade facts only: Spot BUY and configured SHORT Futures SELL. Exit-direction fills are rejected until the separate exit-fill accounting contract is implemented. The service cannot submit, cancel, replace, or close orders.
- V2 `execution_fill_ledger`, Redis payloads, and V2 engine behavior remain unchanged.
- A private stream is an acceleration source. REST reconciliation can feed the same durable inbox contract.
- A fill is not inferred from an order acknowledgement or cumulative order status.

## Fill identity and transaction

A fill is uniquely identified by account scope, venue, market type, exact native symbol, and venue trade ID. A missing trade ID is rejected; a synthetic timestamp/price/quantity identity is not used.

The transaction performs:
1. Lock the durable session and validate account scope and the current coordinator lease/generation.
2. Lock the exact session leg and validate venue, market type, native symbol, quote asset, and account scope.
3. Check the event idempotency key and venue trade identity.
4. Insert the immutable fill fact.
5. Increment confirmed base/contract quantity and quote quantity using PostgreSQL NUMERIC / Python Decimal.
6. Recompute the leg's weighted average fill price.
7. Increment the leg entity version and parent aggregate cursor.
8. Append the corresponding session event in the same transaction.

A duplicate trade with identical facts is a no-op. Reusing a trade identity or event idempotency key with different immutable facts is a conflict. No cumulative total is changed on replay. A new trade with a stale leg version, aggregate cursor, or fencing generation is rejected and rolled back before it can affect totals. Verified fees require both a fee asset and an authoritative fee source; string truthiness is never accepted as fee verification. The database also enforces a composite session/leg foreign key so a fill cannot reference a leg belonging to another session.

## Late fills and uncertainty

A late entry fill received after a canceled/closed/closing/unknown/rejected leg, a HEDGED/EXIT session, or a terminal session is not discarded. The fill fact is preserved and the affected leg/session is moved to RECONCILIATION_REQUIRED. This is an explicit audited correction to a previously terminal projection; it is not permission to resume entry.

A leg remains FILLED only when the existing lifecycle already established that state; this service does not infer order terminality from a trade. In-flight/cancel-pending state is not rewritten to a terminal order status.

## Durable inbox

The inbox identity is account + venue + event type + source event ID. Identical re-delivery is idempotent; a changed payload under the same identity is quarantined as a conflict. Processing transitions are constrained:
- RECEIVED -> PROCESSING or QUARANTINED
- PROCESSING -> PROCESSED, RETRY, or QUARANTINED
- RETRY -> PROCESSING or QUARANTINED

The inbox persists raw normalized payloads for replay/audit. The V3 event processor and exchange stream adapters are intentionally not wired to the live V2 engine in this PR.

## Explicitly not delivered here

- No order intent / mutation gateway integration.
- No hedge obligation calculation or Futures submission.
- No hedge aggregation/allocation.
- No liquidity scheduler, exit coordinator, or PnL engine.
- No runtime cutover, live exchange calls, production migration, or VPS deployment.

## Required validation

- Exact decimal validation and timezone-aware fill timestamps.
- PostgreSQL exactly-once fill replay and immutable identity conflict.
- Account/venue/market/symbol binding.
- Atomic leg totals + aggregate event journal.
- Durable inbox duplicate handling and processing state transitions.
- Late fill after completion forces reconciliation.
- Full repository CI remains mandatory; PostgreSQL integration tests must run in an environment with TEST_POSTGRES_DSN, otherwise they are explicitly reported as skipped.
