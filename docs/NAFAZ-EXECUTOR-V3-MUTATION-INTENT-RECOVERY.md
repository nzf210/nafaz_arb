# NAFAZ Executor V3 — Mutation Intent Recovery Contract

Status: PR-03G read-only reconciliation worker. No automatic resubmission.
Scope: Executor only.

## 1. Recovery boundary

`MutationIntentRecoveryWorker` reconciles only SUBMITTING, UNKNOWN and ACKNOWLEDGED intents. PREPARED intents have not crossed the mutation boundary and are not auto-submitted by this worker. Terminal intents are ignored.

The worker has read-only venue capabilities: exact order lookup by exchange order ID or stable client order ID. It has no create or cancel method and cannot issue a second mutation.

## 2. Fencing

Before a venue read, the worker acquires:

- the global shared mutation lock, serializing with STOP transitions;
- the exact account/venue/market/instrument advisory lock;
- a current ownership lease.

The worker rebinds the intent to the new owner ID/generation and appends an owner-reclaimed event in the same PostgreSQL transaction. A stale owner cannot acknowledge, reject or complete the intent afterward. The lease is released after reconciliation.

Read-only reconciliation remains available under global STOP.

## 3. Evidence rules

- Exact order found: persist venue acknowledgement if the intent was SUBMITTING or UNKNOWN.
- Exact order remains open: retain ACKNOWLEDGED; do not resubmit.
- Exact order is terminal: require fill reconciliation before COMPLETED or CANCEL_CONFIRMED.
- Definitive CREATE rejection from exact venue state: mark REJECTED.
- NOT_FOUND, timeout, mismatched order ID, unavailable venue client, or missing fill evidence never implies rejection or flatness; retain UNKNOWN/ACKNOWLEDGED for later reconciliation.
- CANCEL intents are never re-cancelled by the recovery worker.

## 4. Scheduling

The worker exposes `reconcile_one` and `reconcile_batch` for a future supervised scheduler. This PR does not start a background loop or wire it into the live executor. A later cutover stage must add bounded cadence, backoff, observability, and shutdown semantics before runtime activation.
