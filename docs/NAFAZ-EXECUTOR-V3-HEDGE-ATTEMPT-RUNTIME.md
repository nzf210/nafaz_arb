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

This repository is intentionally not wired into the legacy entry hedge path in
this PR. The integration must be done as a separate change with regression
coverage for mutation gateway fencing, restart recovery, duplicate fill replay,
and partial terminal fill residual retry. No LIVE order path should be enabled
until those tests pass.
