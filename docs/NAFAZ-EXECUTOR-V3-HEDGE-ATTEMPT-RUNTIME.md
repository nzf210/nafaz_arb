# Runtime hedge attempt ledger repository

`executor/hedge_attempt_ledger.py` is the PostgreSQL repository layer for the additive
attempt schema introduced by PR-05. A hedge claim remains the serialized
Spot-fill generation; this repository stores each distinct Futures venue order
within that generation.

## Operations

- `prepare_attempt`: takes a per-claim advisory transaction lock, locks the
  claim, refuses creation while any attempt is unresolved, calculates the
  remaining target from persisted verified fills and active reservations, and
  inserts the next attempt with a distinct client order ID.
- `mark_submitting`, `mark_unknown`, and `mark_acknowledged`: optimistic-version
  transitions. The caller must persist PREPARED/SUBMITTING state before the
  venue mutation and retain the existing Global STOP / mutation fence.
- `record_fill`: appends a verified fill once, rejects changed immutable
  facts on replay, and updates the attempt's cumulative confirmed quantity in
  the same transaction.
- `finalize_attempt`: refuses terminalization until both order state and
  fill history are reconciled. A terminal partial fill releases the outstanding
  reservation so a later attempt can cover the true residual.

## Required integration sequence

1. Prepare the attempt before calling the existing submit-intent and Global
   STOP mutation gateway.
2. Use the attempt's client order ID for every venue lookup and reconciliation.
3. Persist SUBMITTING before crossing the exchange create-order boundary.
4. Persist ACKNOWLEDGED / UNKNOWN only from authoritative mutation outcomes.
5. Record fills from reconciled exchange trade facts, not order cumulative
   quantity snapshots alone.
6. Finalize only after terminal order status and complete trade-history
   reconciliation; then compute and prepare a new residual attempt.
7. Keep claim-level confirmation and position-level exposure fenced until the
   sum of verified attempt fills covers the claim target.

## Current runtime wiring in PR-56

The runtime repository is now connected to the existing entry and reconciliation
paths:

- `ArbitrageEngine` owns one `HedgeAttemptLedger` backed by the existing
  `GlobalStopService` PostgreSQL pool.
- Entry hedge preparation reserves an attempt after exchange sizing and before
  the existing submit-intent / Global STOP mutation fence.
- The first attempt reuses the claim's original client order ID to preserve
  compatibility with the claim lifecycle. Subsequent attempts use a distinct
  deterministic client order ID.
- The attempt is moved to `SUBMITTING` inside the existing submit fence,
  then to `ACKNOWLEDGED` or `UNKNOWN` based on authoritative mutation
  outcomes.
- `TradeFillReconciliationWorker` mirrors individually verified venue trades
  into the exact attempt using the client order ID and venue trade ID.
- Attempt finalization requires terminal order state and complete individual
  trade reconciliation. Claim-level `CONFIRMED` is written only when the
  sum of verified attempt fills covers the claim target.
- Terminal partial attempts release their reservation only after reconciliation;
  the next entry monitor pass can then prepare a distinct residual attempt.
- Terminal Spot orders with a retryable partial Futures attempt remain in the
  durable pending-entry queue as residual-hedge work items. The monitor reuses
  the stored terminal Spot fill snapshot and never re-polls, cancels, or
  reprices the terminal Spot order.
- Startup recovery runs after Global STOP submit-intent reconciliation. It
  adopts PREPARED/SUBMITTING/UNKNOWN attempts when the exact venue order is
  found by client ID, and rejects an unresolved attempt only after that
  authoritative recovery pass proves no accepted order exists.
- A legacy `SUBMITTED` claim with no attempt-ledger row is deliberately
  blocked. It is not safe to assume its old exchange order is terminal.
- Below-minimum residuals and unresolved/unknown submissions remain explicit
  manual reconciliation obligations; they are not auto-retried.

No LIVE configuration or exchange order was changed.
