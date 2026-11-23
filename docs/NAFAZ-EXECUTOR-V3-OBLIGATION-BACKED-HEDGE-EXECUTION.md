# NAFAZ Executor V3 — Obligation-Backed Hedge Execution

Status: PR-07 implementation. This stage is the first bridge from persisted liquidity
eligibility to a real mutation intent; it remains gated by the existing Global STOP,
instrument ownership, and risk controls.

## Execution protocol

1. Persist and validate the liquidity decision under the session coordinator lease.
2. For SUBMIT/PARTIAL_SUBMIT only, reserve the exact base-equivalent quantity from
   compatible OPEN/PARTIALLY_HEDGED obligations.
3. Create a distinct account-scoped CREATE mutation intent with
   \`action_class=HEDGE\`, exact Futures venue/market/native symbol, contract quantity,
   limit price at the evaluated executable VWAP, session/leg identity, risk evidence,
   and the reservation UUID.
4. Bind the reservation to the intent's deterministic client order ID before calling
   the gateway.
5. Execute only through \`MutationGateway.execute_create_intent\`.
6. Keep the reservation SUBMITTED after ACK until confirmed Futures fills are
   persisted and allocated. Ambiguous outcomes become UNKNOWN and remain reserved.
7. Release a reservation after a definitive gateway rejection only; release requires
   terminal-order and fill-reconciliation proof.

## Gateway HEDGE fence

A session-bound HEDGE intent is rejected unless:
- The session is HEDGE_PENDING or PARTIALLY_HEDGED and the coordinator lease is active.
- The target leg is the exact paired Futures SHORT leg.
- The mutation intent's venue, market, native symbol, pair route, account, side,
  quantity, and base-per-unit conversion match the reservation and admitted leg.
- The reservation is SUBMITTED, bound to the exact client order ID, and its
  reservation items conserve the reserved base quantity.
- The Futures leg state can accept an initial hedge submit or an additional
  obligation-backed order.

The gateway transaction advances a newly used Futures leg through PREPARED to
SUBMITTING before the venue request. Later hedge orders append a fenced leg event
without resetting an already OPEN/PARTIALLY_FILLED leg lifecycle state.

## Retry behavior

The reservation and intent use stable idempotency keys derived from the caller's
hedge execution key. Retrying an ACKNOWLEDGED intent returns its durable result;
SUBMITTING/UNKNOWN never causes a blind second venue request. A terminal REJECTED
or ABORTED intent releases only its own reservation remainder.

## Scope

- The coordinator accepts a venue client from the caller; it does not create exchange
  clients or bypass the central gateway.
- It does not infer fills from order acknowledgements.
- Fill ingestion and reservation allocation remain separate evidence-driven stages.
- No V2 runtime call-site cutover, scanner/strategy/risk threshold change, production
  migration, LIVE config, or deployment is included.
