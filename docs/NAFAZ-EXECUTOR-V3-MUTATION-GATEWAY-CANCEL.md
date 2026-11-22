# NAFAZ Executor V3 — Protective CANCEL and De-risk Mutation Contract

Status: PR-03F. V2 call sites remain unchanged.
Scope: Executor only.

## 1. Protective cancel is a first-class durable intent

A CANCEL intent targets one exact venue order by exchange order ID or venue client order ID. The target, account, venue, market type, native instrument, pair route and session/leg identity are immutable. The gateway never falls back to another account or venue.

## 2. STOP-safe cancellation fence

Protective cancellation must remain available during global STOP. The cancellation fence:

- takes the global shared mutation lock so STOP transitions serialize with the cancel boundary;
- takes the exact account/venue/market/instrument mutation lock;
- acquires and validates the scoped ownership lease;
- records the global STOP epoch/state in the intent risk snapshot;
- atomically persists SUBMITTING and the event before any cancel HTTP request.

Unlike ENTRY, cancellation does not consult ENTRY permission or DERISK_ALLOWED. It reduces outstanding exposure and must remain available to unwind pending orders. The scoped ownership lease and durable intent are still mandatory.

## 3. Cancellation protocol

1. Resolve the exact venue client and verify its venue identity.
2. Require exact fetch and cancel capabilities before entering the fence.
3. Resolve a target client ID to an exact exchange order ID when needed.
4. Fetch the order before cancel; reject mismatched IDs and unclassifiable state.
5. Send one cancel request at most.
6. Fetch the exact order again.
7. If terminal, invoke the fill-reconciliation callback. Only then may the intent become CANCEL_CONFIRMED and a local Spot slot be released.
8. If the venue accepted cancel but the order remains open, persist ACKNOWLEDGED and leave the intent unresolved.
9. Timeout, disconnect, mismatched state, missing evidence, or unreconciled racing fills remain UNKNOWN. Reinvocation never sends a second cancel request.

Terminal status alone is insufficient: racing fills must be reconciled before cancellation is considered complete.

## 4. De-risk CREATE orders

The centralized create fence also accepts scoped DERISK intents. DERISK is permitted under global STOP but remains subject to hierarchical DERISK_ALLOWED controls and the exact instrument ownership lease. This is the path for new protective orders such as exits; it is not a general ENTRY bypass.

## 5. Non-goals

- No existing V2 entry, hedge, exit or cancel call site is switched in this PR.
- No dual writer or V2/V3 live execution.
- No scanner changes, production migration, VPS deployment or live exchange action.
