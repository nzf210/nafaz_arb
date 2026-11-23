# PR-03I — Session Entry Intent Coordination

## Purpose

Connect the durable V3 session aggregate to the PR-03D/03E logical mutation-intent
and CREATE gateway foundation. This stage prepares both entry-leg intents before
the session can advance from `ADMITTED` to `ENTRY_IN_PROGRESS`.

## Protocol

1. Lock and load the session and both venue-bound legs from PostgreSQL.
2. Require a current coordinator lease matching the persisted session scope,
   owner, and fencing generation.
3. Require exactly one SPOT and one FUTURES leg and exactly one caller specification
   per leg.
4. Validate both specifications before writing either intent:
   - Spot entry side is BUY.
   - Futures entry side is SELL and the admitted position side is SHORT.
   - Each order quantity exactly equals the immutable admitted `order_quantity`.
   - Both order prices are finite and positive; order type is LIMIT or MARKET.
   - Each leg has a stable idempotency key and ENTRY action class.
5. Persist each logical CREATE intent through `OrderIntentService.prepare_create`.
   Intent identity remains account-scoped and immutable; each intent is bound to
   the exact session, leg, venue, market, native instrument, and pair route.
6. Transition each leg to PREPARED using the coordinator lease and aggregate
   compare-and-swap.
7. Only after both intents exist and both legs are PREPARED, transition the
   session to ENTRY_IN_PROGRESS.

## Crash and retry behavior

Intent preparation and aggregate transitions are separate PostgreSQL transactions.
This is deliberate: every step is idempotent and restartable, without holding a
long transaction across multiple repository operations.

- Crash before any intent: retry prepares both intents.
- Crash after the first intent: retry returns that exact intent by its idempotency
  key and prepares the other; no second reservation is created.
- Crash after both intents but before leg transitions: retry reuses both intents
  and advances the legs.
- Crash after one leg transition: retry observes the persisted PREPARED leg and
  advances only the remaining leg.
- Crash after both legs transition but before session transition: retry advances
  the session only.
- A stale coordinator generation fails the aggregate transition. Prepared intents
  remain non-submittable and reserved until a valid coordinator recovers the
  session and resumes with the same idempotency keys.

## Safety boundary

This service only persists logical intents and session events. It has no exchange
client and never calls `MutationGateway.execute_create_intent`. A later runtime
stage must explicitly route each prepared intent through the single mutation
gateway after the existing instrument ownership/risk/Global STOP fence.

No V2 call sites, scanner logic, strategy thresholds, sizing policy, exit policy,
production migration, LIVE configuration, or deployment are changed here.
