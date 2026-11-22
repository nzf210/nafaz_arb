# NAFAZ Executor V3 — Session Coordinator Contract

Status: PR-03C implementation contract; no V2 runtime cutover.
Scope: Executor only. Scanner contracts and source remain frozen.

## 1. Purpose

The Session Coordinator is the single orchestration boundary for one durable Spot/Futures execution session. It coordinates admission, ownership, risk permission, durable aggregate creation, and recovery ownership. It does not implement exchange order submission, fill accounting, hedging, exits, or venue reconciliation.

The existing V2 order path remains authoritative until the coordinator and all downstream V3 lifecycle services have passed their own validation stages.

## 2. Authority and dependency direction

The coordinator depends on narrow interfaces:

- ExecutionSessionRepository: canonical PostgreSQL session/leg projection and append-only event journal.
- OwnershipLeaseService: PostgreSQL-backed session coordinator lease and monotonically increasing generation.
- RiskControlPlane: hierarchical ENTRY permission and a versioned risk snapshot.
- PolicySnapshotBuilder: deterministic immutable strategy, execution, risk, fee, market metadata, FX, and exit policy snapshot.
- AdmissionIdentity: stable source dispatch ID, account scope, pair key, session ID, and admission idempotency key.

PostgreSQL remains the only canonical writer. Redis may wake workers or cache read models but cannot grant admission, ownership, or risk permission. Supabase and reporting outboxes remain projections.

## 3. Admission protocol

The admission operation must execute these steps in order:

1. Validate the source dispatch identity and derive a deterministic admission key scoped by account and route. A retry of the same dispatch must resolve to the same aggregate; a new dispatch must not collide.
2. Resolve and validate exactly one venue-bound SPOT leg and one venue-bound FUTURES leg. Preserve each venue's native symbol, market type, quote/settlement asset, contract metadata, position side, and quantity units. Both legs must represent the same normalized base asset.
3. Build the complete immutable policy snapshot. Reject missing or stale critical metadata; never replace missing fee, FX, market, or risk evidence with guessed defaults.
4. Acquire the session-coordinator lease using a dedicated scope (venue=coordinator, market_type=session, instrument=session_id). This scope is distinct from each venue/instrument mutation lease. The session generation fences aggregate transitions; venue/instrument generations fence private exchange mutations.
5. In a PostgreSQL transaction, verify that the session lease is still owned by the caller, has the acquired generation, and has not expired. Read and evaluate the hierarchical ENTRY risk controls in the same transaction. Capture the exact risk-control versions and effective budget decision.
6. Create the session, both legs, policy hashes, initial event journal records, and an admission decision record atomically. Persist the coordinator lease scope/owner/generation with the session so every later transition can revalidate the same lease.
7. Return an admitted session reference only after commit. This return value is not permission to submit an exchange order by itself; a later execution stage must persist order intent and re-check current fencing/risk immediately before the mutation boundary.

If a retry finds the same admission key, compare the complete source, account, route, leg identity, and immutable policy hashes. Return the existing session only when all identity fields match. Otherwise raise AdmissionConflict.

## 4. Atomicity and stale-owner rules

A lease acquisition followed by an unrelated repository transaction is not sufficient fencing. The repository transaction that creates or transitions a session must lock/read the ownership lease row and verify:

- exact scope key;
- exact owner ID;
- exact generation;
- lease expiry later than PostgreSQL clock_timestamp().

The same validation is required for every aggregate transition, not only initial admission. A lease expiry or generation transfer must reject the stale coordinator even if the stale worker still has a cached session object.

Risk control reads must be transactionally consistent with the admission write. A STOP committed before admission's risk check must block admission. A STOP racing after the check must serialize through the same canonical control-plane locking/transaction contract; it must not be possible for an uncommitted admission to bypass a committed STOP.

The session coordinator lease is not a replacement for per-instrument ownership. Each private mutation must continue to carry the venue/instrument scope key and generation through the existing submit-attempt/mutation gateway boundary.

## 5. Session lifecycle responsibilities

The coordinator owns only aggregate-level transitions:

- CREATED -> VALIDATING -> ADMITTED;
- ADMITTED -> ENTRY_IN_PROGRESS only after the next stage has durably created the required order intents;
- active session -> BLOCKED / RECONCILIATION_REQUIRED when canonical evidence, ownership, or risk permission is insufficient;
- RECONCILIATION_REQUIRED -> RECOVERY_IN_PROGRESS after a new session lease generation is acquired and recovery scope is persisted.

The coordinator must not mark a session HEDGED, EXIT_PARTIAL, or COMPLETED based only on order acknowledgements or V2 in-memory state. Those transitions require downstream fill, position, obligation, exit, and reconciliation evidence.

FAILED and ABORTED do not assert flatness. Unknown orders, fills, positions, or obligations remain unresolved and prevent completion.

## 6. Recovery and crash behavior

- Crash before session transaction commit: no session admission exists; the lease expires or is safely released.
- Crash after commit but before response: retry resolves the same admission key to the existing session.
- Crash after admission but before order intent: session remains ADMITTED; recovery may resume only after acquiring a valid session lease and re-reading canonical state.
- Lease expires while worker is running: all aggregate writes fail closed until ownership is reacquired with a newer generation.
- Another worker reclaims an expired lease: old generation can never mutate the session again.
- PostgreSQL unavailable: admission and state transitions stop; Redis must not be used as an alternate authority.

## 7. Explicit non-goals for this stage

- No exchange API calls or order submission.
- No V2 order path cutover or dual execution.
- No scanner changes or scanner schema changes.
- No production migration, VPS deployment, live exchange action, or production database write.
- No claim that an admitted session is already hedged or risk-neutral.

## 8. Required validation before runtime integration

Unit and PostgreSQL integration tests must cover:

- deterministic admission identity and same-key idempotency;
- same key with changed account, route, leg identity, or policy hash rejected;
- exactly one Spot and one Futures leg with matching base asset;
- missing/stale market, fee, FX, or policy evidence rejected;
- concurrent same-dispatch admission creates exactly one session;
- STOP / ENTRY risk-control serialization under concurrent admission;
- expired lease, wrong owner, and stale generation rejected at both create and transition boundaries;
- lease reclaim increments generation and fences the old worker;
- crash/retry around commit returns the original session;
- no exchange mutation is reachable from the coordinator.

The coordinator is not eligible for runtime wiring until these tests pass and the durable event stream can reconstruct the initial aggregate and every subsequent coordinator transition.


## 9. Implemented coordinator input and recovery contract

The implementation exposes `SessionCoordinator.admit`, `SessionCoordinator.transition`,
`SessionCoordinator.transition_leg`, and `SessionCoordinator.recover`.

- Admission identity is SHA-256 over a versioned canonical payload containing account scope,
  source dispatch ID, and pair route. Opportunity identity remains a collision check, not a
  second idempotency namespace.
- The immutable policy snapshot must contain evidence records for `market_metadata`,
  `fee_schedule`, and `fx_conversion`. Each record carries `status`, `observed_at`, and
  `valid_until`; timestamps must be timezone-aware and current. FX may be explicitly
  `NOT_REQUIRED`; market metadata and fee evidence must be `VERIFIED`.
- The repository verifies the coordinator lease row under `FOR UPDATE` in the same
  transaction as session/leg/event writes. It locks the same risk-control advisory keys
  used by risk-control updates before evaluating ENTRY permission, preventing a STOP
  update from racing past admission.
- Recovery is a separate operation. It loads the persisted session and policy rather than
  rebuilding policy from current market data, acquires the session coordinator scope,
  increments only the aggregate cursor when the generation changes, and appends
  `COORDINATOR_LEASE_RECLAIMED`. The session state version is unchanged by a fence-only
  reclaim.
- Idempotent admission replay with a different coordinator generation is rejected with
  an instruction to use the recovery operation. A duplicate with the same generation
  returns the existing aggregate without creating new events.

These methods remain an isolated V3 service. No engine/runtime wiring or exchange
mutation is introduced by this stage.
