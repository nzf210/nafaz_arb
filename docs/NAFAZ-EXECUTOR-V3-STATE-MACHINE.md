# NAFAZ Executor V3 — Session and Leg State Machine

Status: PR-01 design artifact — review required before runtime wiring.
Repository: nzf210/nafaz_arb
Scope: Executor only. Scanner contract remains frozen.

## 1. Design rule

The session is the durable aggregate identity, but Spot and Futures are independent execution legs. Session state is a materialized coordination projection; it must never replace order, fill, hedge-obligation or exchange-position evidence.

A venue acknowledgement is not a fill. A cancel acknowledgement is not terminal proof. A terminal order is not proof that the pair is flat. Unknown evidence must remain UNKNOWN / RECONCILIATION_REQUIRED.

## 2. Session lifecycle

```text
CREATED
  -> VALIDATING
  -> ADMITTED
  -> ENTRY_IN_PROGRESS
  -> HEDGE_PENDING
  -> PARTIALLY_HEDGED
  -> HEDGED
  -> EXIT_IN_PROGRESS
  -> EXIT_PARTIAL
  -> RECONCILIATION_REQUIRED
  -> RECOVERY_IN_PROGRESS
  -> COMPLETED
```

Additional states:
- REJECTED: admission failed before any exchange exposure was created.
- ABORTED: coordinator stopped the workflow; this does not assert that exposure is zero.
- FAILED: unrecoverable workflow error; may still have live exposure and therefore requires reconciliation/de-risk.
- BLOCKED: canonical state, ownership, risk or venue evidence is insufficient to permit a new exposure mutation.

Do not use one generic FAILED state to represent both flat terminal failure and unresolved exposure.

## 3. Independent leg lifecycle

Spot and Futures legs each have their own state/version:

```text
NOT_STARTED
  -> PREPARED
  -> SUBMITTING
  -> SUBMISSION_UNKNOWN
  -> OPEN
  -> PARTIALLY_FILLED
  -> FILLED
  -> CANCEL_PENDING
  -> CANCELED
  -> CLOSING
  -> CLOSED
  -> UNKNOWN
```

The arrows are not a single linear sequence. Valid transitions depend on observed order/fill facts:
- SUBMITTING may become OPEN, PARTIALLY_FILLED, FILLED, REJECTED or SUBMISSION_UNKNOWN.
- OPEN may become PARTIALLY_FILLED, FILLED, CANCEL_PENDING or UNKNOWN.
- CANCEL_PENDING may become CANCELED, PARTIALLY_FILLED, FILLED or UNKNOWN after fresh venue reconciliation.
- FILLED is an order fact; a leg can remain exposed until a verified exit.
- CANCELED may still have a nonzero filled quantity and corresponding exposure.
- CLOSED requires verified zero exposure for that leg and no live/unknown order that can increase it.
- UNKNOWN is not a terminal/flat state.

## 4. Transition envelope

Every transition command must carry:
- session_id; leg_id when the transition targets a leg;
- entity_type and entity_id;
- expected current state and entity_version;
- expected parent session aggregate_version;
- worker/actor identity;
- current fencing_generation;
- event_idempotency_key;
- correlation_id;
- transition reason and evidence references.

The transaction must:
1. lock or compare-and-swap the relevant row;
2. verify expected state/version and fencing generation;
3. validate the transition against the transition table;
4. update the target session/leg projection and increment its entity_version;
5. increment the parent session aggregate_version for every accepted aggregate event;
6. append the event with both aggregate_version and entity_version;
7. enqueue any transactional outbox event;
7. commit atomically.

A stale or duplicate transition must not create a second state change or duplicate downstream command. The aggregate_version is the single ordered event-stream cursor per session; entity_version is scoped to the changed session or leg. A leg-only event must not reuse another event's aggregate_version.

## 5. Transition rules and evidence

| From | To | Required evidence / condition |
|---|---|---|
| CREATED | VALIDATING | Durable session exists and admission request is bound to an idempotency key |
| VALIDATING | ADMITTED | Payload, freshness, route identity, balances, risk and market minimums validated |
| VALIDATING | REJECTED | Stable rejection reason; no order intent or exchange exposure exists |
| ADMITTED | ENTRY_IN_PROGRESS | Both venue-bound leg identities and immutable policy snapshot persisted |
| ENTRY_IN_PROGRESS | HEDGE_PENDING | Confirmed Spot fill exists and a durable hedge obligation is created |
| HEDGE_PENDING | PARTIALLY_HEDGED | Some but not all obligation quantity is covered by confirmed Futures fills |
| HEDGE_PENDING / PARTIALLY_HEDGED | HEDGED | Required base-equivalent hedge coverage confirmed within policy tolerance |
| Any active state | BLOCKED | Ownership, risk, DB, venue or market evidence prevents safe new exposure |
| HEDGED | EXIT_IN_PROGRESS | Exit policy decides to exit and durable exit intent/quantity reservation exists |
| EXIT_IN_PROGRESS | EXIT_PARTIAL | One or both legs have confirmed partial close fills and residual exposure remains |
| EXIT_IN_PROGRESS / EXIT_PARTIAL | RECONCILIATION_REQUIRED | Timeout, inconsistent order/fill/position, unknown order or failed accounting |
| RECONCILIATION_REQUIRED | RECOVERY_IN_PROGRESS | Recovery owner and fencing generation acquired; reconciliation scope persisted |
| RECOVERY_IN_PROGRESS | prior safe active state | All order/fill/position/obligation facts reconcile and resume is permitted |
| EXIT_IN_PROGRESS / EXIT_PARTIAL | COMPLETED | Both legs flat, all orders terminal, obligations/allocations zero, final reconciliation complete |
| Any active state | ABORTED / FAILED | Audited operator/system reason; exposure status remains separately tracked |

A recovery transition must never infer that an unknown order is absent or that an unknown position is flat.

## 6. Derived session state

Session status is derived from:
- both leg states and verified base-equivalent exposure;
- current order inventory and unresolved submit/cancel intents;
- confirmed fill ledger watermark/completeness;
- outstanding hedge obligations and in-flight reservations;
- pooled hedge allocations, where applicable;
- risk/ownership/fencing state;
- reconciliation completeness.

Priority when facts conflict:
1. UNKNOWN / RECONCILIATION_REQUIRED if any critical source is incomplete or contradictory;
2. protective/exit progress if exposure exists and risk policy requires de-risk;
3. ENTRY/HEDGE progress while entry obligations remain;
4. HEDGED only when both legs' verified base-equivalent exposure satisfy hedge policy;
5. COMPLETED only after verified flatness and all finalization gates.

The projection must be recomputable from canonical records. Redis or in-memory manager state may accelerate the view but cannot decide it.

## 7. Partial fill and cancel/fill race

- Every newly confirmed Spot fill delta contributes exactly once to a durable hedge obligation.
- Futures hedge completion is measured from confirmed Futures fills, not submitted order quantity.
- Cancel/fill races are resolved from fresh venue order state plus trade history/fill reconciliation.
- A canceled order with nonzero fill retains its filled exposure and hedge obligation.
- A fill observed after a session advanced is not discarded; it is applied idempotently and may move the session back to reconciliation/hedge repair.
- Slot release and replacement decisions occur only after terminal order proof and fill reconciliation for the relevant order.

## 8. Completion gate

A session may enter COMPLETED only if all are true:
- Spot managed exposure is verified zero;
- Futures base-equivalent position is verified zero;
- all relevant exchange orders are terminal and no submission is unknown;
- confirmed fills are accounted for in the canonical ledger;
- hedge obligations, in-flight reservations and pooled allocations are zero/released;
- final fee/funding/PnL reconciliation has completed to the required confidence;
- no unresolved risk or operator incident blocks closure;
- the final state transition is committed under the current fencing generation.

FAILED, ABORTED, CANCELED and CLOSED order states do not independently satisfy this gate.

## 9. Existing V2 compatibility mapping

Existing `PositionState` values (`IDLE`, `ENTRY_PENDING`, `ENTRY_PARTIAL`, `HEDGED_ACTIVE`, `EXIT_PENDING`, `CLOSED`, `FAILED`, `RECONCILIATION_REQUIRED`) remain legacy runtime concepts during migration.

They must be mapped explicitly to V3 session/leg projections; do not rename enum values in place or change Redis payload semantics in this PR. Existing `LegStatus` in `executor/leg_managers.py` is an in-memory projection and is not canonical until PostgreSQL hydration/replay is proven. Legacy rows with unknown account scope, market identity, quantity units or policy provenance must remain explicitly legacy/unknown; a migration default is not evidence.

## 10. PR-01 transition test matrix

- Valid admission creates one session and two independently identified legs.
- Illegal transition is rejected with no row/version/event change.
- Duplicate event idempotency key has no additional effect.
- Out-of-order stale event cannot regress a leg/session state.
- A terminal canceled Spot order with partial fill leaves Spot exposure and hedge obligation intact.
- Filled Spot leg with unconfirmed Futures hedge cannot project to HEDGED.
- Futures contract count is converted to base-equivalent exposure using the pinned contract size.
- One leg closing does not close the other leg or mark the session COMPLETED.
- Unknown order/position/fill history forces RECONCILIATION_REQUIRED, never COMPLETED.
- Restart reconstructs session and both legs from PostgreSQL records without relying on Redis.
- Legacy sessions without an evidenced immutable policy snapshot remain LEGACY/UNKNOWN for policy provenance; they are not silently promoted by a default empty JSON object.
- Deleting a session cannot cascade-delete its legs or transition history; retention is an explicit audited archive/purge operation.
- A stale fencing generation cannot mutate state or publish a transition event.


## 11. PR-01A aggregate-version and legacy-ownership clarifications

- `execution_sessions.aggregate_version` is the durable per-session event cursor. It increments once for every accepted session or leg transition, atomically with the event insert.
- The session's own `state_version` and each leg's `state_version` remain entity-scoped versions. They are not substitutes for aggregate_version.
- A leg transition increments that leg's state_version and the parent aggregate_version; it does not increment the session state_version unless the session projection itself changes.
- Legacy rows with unknown ownership must not be assigned a trusted fencing generation by default. Keep generation NULL/unknown until a V3 ownership acquisition establishes a real generation. Preserve V2 owner_fencing during compatibility.
- The transition transaction must verify the expected aggregate_version in addition to entity version and fencing generation. A failed compare-and-swap changes no row and appends no event.
- Session deletion must not cascade to legs, events, orders, fills, obligations, positions or exit records. Use RESTRICT/non-cascading relationships and a separate audited retention process.
