# NAFAZ Executor V3 — Durable Order Intent Contract

Status: PR-03D foundation. Not wired into the V2 runtime mutation paths.
Scope: Executor only; scanner remains unchanged.

## 1. Authority boundaries

- `executor_mutation_intents` is the canonical logical mutation intent.
- `executor_mutation_intent_events` is the append-only intent lifecycle journal.
- `executor_submit_attempts` remains the create-order external attempt ledger.
- `order_registry` remains the venue order lifecycle/recovery projection.
- These records have distinct responsibilities; none is a substitute for another.
- No exchange client or network call exists in `OrderIntentService`.

## 2. Intent identity

Every intent is scoped by `account_scope_id` and a caller-supplied idempotency key. A versioned SHA-256 identity derives the stable intent ID. CREATE intents also derive a stable venue-safe client order ID from the same identity.

The canonical request hash includes operation, venue, market, native instrument, session/leg identity, order parameters, reservation values and request metadata. Reusing an idempotency key with a different hash raises `OrderIntentConflict`. A retry with the same hash returns the existing row without adding a second event.

CANCEL is a distinct logical mutation intent with an exact target exchange order ID or client order ID. REPLACE is intentionally composed as a terminally verified CANCEL followed by a new CREATE intent; no blind cancel-and-replace shortcut is provided.

## 3. Reservations

CREATE may reserve base-equivalent quantity and quote notional before any private request. The service serializes reservation calculation per account/venue/market/instrument using a PostgreSQL transaction advisory lock.

- Base quantity is aggregated across the instrument independent of quote asset.
- Notional is aggregated only within the same reservation asset.
- Caller-supplied limits are checked against active PREPARED, SUBMITTING, UNKNOWN and ACKNOWLEDGED intents.
- UNKNOWN and ACKNOWLEDGED retain reservations.
- Definitive rejection, pre-submit abort, verified cancel terminal state, or completed terminal reconciliation release the reservation.
- No reservation is inferred to be released from an HTTP acknowledgement alone.

## 4. Lifecycle and fencing

- PREPARED is persisted before a mutation can cross the venue boundary.
- SUBMITTING requires a live account/venue/market/instrument lease and a non-empty immutable risk-control snapshot.
- The owner scope, owner ID and generation are stored on the intent and its lifecycle event.
- Every post-submit transition revalidates the exact lease row, owner, generation and expiry under row lock.
- An expired/superseded owner cannot acknowledge, reject or mark an intent unknown.
- Recovery uses `reclaim_for_reconciliation`: it transfers ownership to a newer generation but does not make SUBMITTING/UNKNOWN retryable. The new owner must reconcile exact venue state.
- A new CREATE attempt requires a new logical intent/idempotency key after the previous attempt is definitively resolved.

## 5. Terminal evidence

- `mark_cancel_confirmed` requires a fresh venue order fetch, a terminal status, and reconciled racing fills.
- `mark_completed` requires a terminal order status and fill reconciliation.
- `mark_rejected` requires an explicit definitive rejection.
- `mark_aborted` is allowed only from PREPARED, proving no private request was sent.
- UNKNOWN never becomes rejected merely because an eventually consistent lookup returns NOT_FOUND.

## 6. Validation and non-goals

Tests cover deterministic identity, request collision, concurrent reservation serialization, quote-asset separation, cancel terminal evidence, lease fencing, and recovery owner transfer.

This PR adds the durable intent service and schema only. It does not yet route MutationGateway or existing Spot/Futures/exit/cancel call sites through the service. That integration is the next stage and must not create dual writers or alter the current V2 live path prematurely.
