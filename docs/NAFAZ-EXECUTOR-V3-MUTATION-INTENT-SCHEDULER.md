# PR-03H — Supervised Mutation Intent Recovery Scheduler

## Scope

Add a bounded, single-flight scheduler around the read-only
`MutationIntentRecoveryWorker` introduced in PR-03G.

## Guarantees

- Calls only `reconcile_batch(limit=...)`; the scheduler has no exchange client
  and cannot create, cancel, replace, or close orders.
- Enforces a configured batch ceiling (1–500) and sequential execution; cycles
  never overlap.
- Uses bounded exponential backoff when no intent is definitively resolved or a
  cycle fails. A resolved intent resets the cadence to the configured base.
- Propagates task cancellation and supports graceful stop after the active
  read-only batch finishes.
- Does not reinterpret NOT_FOUND, missing venue history, or missing fill evidence.
  Those remain unresolved according to the worker's evidence rules.

## Runtime integration boundary

This PR deliberately does not start the scheduler from the V2 engine. The
current V2 order paths do not produce V3 logical mutation intents, and the
recovery worker cannot mark terminal intents complete without an authoritative
fill-reconciliation callback. Starting it prematurely would create a polling
loop with incomplete evidence and misleading readiness semantics.

The scheduler is ready to be attached to the V3 runtime after:
1. V3 session/leg admission and order-intent creation are the authoritative
   producer for the relevant mutation path.
2. A venue/account-bound resolver is wired from the configured exchange registry.
3. A fill reconciler proves complete trade-history coverage and cumulative-fill
   agreement before terminal completion.
4. Startup recovery runs before the scheduler is started; shutdown awaits it.

## Validation

Unit tests cover bounded batch arguments, sequential execution, graceful stop,
task cancellation, and invalid configuration. PostgreSQL/venue behavior remains
covered by the PR-03G integration suite.
