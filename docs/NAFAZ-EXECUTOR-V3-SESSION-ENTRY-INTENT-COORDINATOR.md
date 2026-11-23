# PR-03K — Spot-First Session Entry Coordination

## Purpose

Prepare only the initial Spot BUY intent. The paired Futures leg is not submitted
as a parallel entry order: its short quantity is derived from confirmed Spot fills,
durable hedge obligations, and reservations. This avoids creating an additional
short when the scheduler later hedges an already-filled Spot leg.

## Protocol

1. Lock and load the admitted session and its two venue-bound legs.
2. Require a current coordinator lease matching the persisted session scope,
   owner, and fencing generation.
3. Require exactly one SPOT and one FUTURES leg, but exactly one initial intent
   specification mapped to the SPOT leg.
4. Validate the Spot BUY side, BASE quantity unit, immutable admitted quantity,
   finite positive price, supported order type, stable idempotency key, and
   \`action_class=ENTRY\`.
5. Persist the Spot CREATE intent idempotently, bound to the exact account,
   session, Spot leg, venue, market, native symbol, and pair route.
6. Transition only the Spot leg to PREPARED.
7. Advance the session to ENTRY_IN_PROGRESS. The Futures leg remains CREATED
   until a confirmed Spot fill produces a durable hedge obligation.

## Futures hedge lifecycle

A later hedge execution coordinator must:
- Consume an OPEN/PARTIALLY_HEDGED obligation.
- Reserve the exact base-equivalent amount and contract route.
- Create a separate \`action_class=HEDGE\` mutation intent for the Futures leg.
- Bind reservation and mutation intent to the same deterministic client order ID.
- Submit only through the centralized Mutation Gateway and existing Global STOP,
  instrument ownership, and risk fences.
- Allocate only confirmed Futures fills to the reservation; release only
  proven-unfilled remainder after terminal order and fill reconciliation.

A session-bound \`action_class=ENTRY\` mutation is restricted to the Spot BUY leg.
Futures SELL orders must be obligation-backed HEDGE intents.

## Crash and retry behavior

The Spot intent uses a stable account-scoped idempotency key. If the process stops
after intent persistence or after the Spot leg transition, retrying the same
specification reuses the same intent and continues the aggregate transition.
The Futures leg remains dormant throughout this stage.

## Safety boundary

This service persists a logical Spot intent and session events only. It has no
exchange client and never submits an order. No Futures order is permitted before
a confirmed Spot fill, obligation, and reservation exist.

No V2 call-site cutover, scanner/strategy/risk-policy changes, production
migration, LIVE configuration, or deployment are included.
