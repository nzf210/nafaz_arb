# NAFAZ Executor V3 — Scoped Ownership and Risk Control

Status: PR-02 implementation contract. The schema and mutation-fence integration are staged in the PR-02 branch; CI and code review remain required. No production migration has been applied.

## 1. Ownership identity

A mutation owner is scoped by the tuple:

- credential/account scope ID;
- venue;
- market type;
- native venue instrument symbol.

The account scope ID is either an explicit stable ID on the exchange credential configuration or a one-way SHA-256 fingerprint of the API key plus venue/testnet identity. Raw API keys and secrets are never persisted in ownership rows. An explicit ID is recommended when API credentials may rotate; it must continue to identify the same exchange account.

The scope key is a deterministic SHA-256 digest of the normalized tuple. Spot and perpetual instruments are separate scopes, and different instruments on the same account/venue do not share a lease.

## 2. Lease and fencing contract

- PostgreSQL is the only ownership authority; Redis and process-local caches do not grant ownership.
- Each lease has a monotonically increasing numeric generation and a database-clock expiry.
- A live lease can be renewed only by the same process owner ID.
- An expired lease can be reclaimed by another owner only by incrementing generation.
- A worker must validate owner ID, scope identity, generation and unexpired lease at the mutation boundary.
- Every durable submit attempt records account_scope_id, owner_scope_key and owner_generation separately from the global STOP fence_epoch.
- Legacy submit attempts retain NULL ownership provenance. They are not assigned a trusted generation by backfill.
- A graceful shutdown releases only leases owned by that process; crash recovery relies on lease expiry and generation increment.

## 3. Mutation serialization and STOP

Order submission now uses two advisory-lock layers:

1. A shared global lock is held across the venue request. Independent submits can share it.
2. A per-account/venue/market/instrument exclusive lock is held across the request. A second mutation for the same native instrument cannot overlap.

Manual global STOP and resume use the exclusive global advisory lock. They wait for already-admitted shared submit scopes, then update the canonical STOP row. New entry submits cannot pass the lock while a STOP transition is pending and will observe the committed STOP state afterward. The existing de-risk lock remains separate.

The global STOP epoch and scoped ownership generation are independent fencing dimensions; neither may be substituted for the other.

## 4. Independent risk permissions

The durable risk-control projection is hierarchical: global, account, venue, instrument, pair and session.

- ENTRY_BLOCKED controls whether a new entry may create exposure.
- DE_RISK_ALLOWED controls whether a protective hedge/de-risk action may proceed.
- These permissions are independent. A global STOP or entry halt can block new entries while still allowing a hedge when DE_RISK_ALLOWED is true.
- A control projection update and its append-only event are committed in one transaction with a monotonic version and optional expected-version CAS.
- Submit admission takes the same transaction-scoped advisory locks as control updates, in deterministic key order. This establishes a linearization point: an entry is admitted under the last committed policy, or it observes the committed block; it cannot read a stale control while an update is concurrently committing.
- Every scoped submit attempt stores the effective control flags and versions it observed in risk_control_snapshot, alongside the ownership generation.
- Idempotency keys are scoped to the control record. Reusing a key with different event content is rejected.

The current PR wires durable control checks into spot entry, futures hedge, and de-risk mutation fences used by exit/rollback/force-close paths. ENTRY and HEDGE admission evaluate the existing RiskBudgetEvaluator using current projected metrics and configured defaults; durable per-scope limits override those defaults. Protective HEDGE/DERISK actions are not rejected by entry-only exposure budgets, but still require the durable DE_RISK_ALLOWED permission. Existing risk persistence and exit policy remain authoritative; no strategy or exit thresholds are changed.

## 5. Current integration boundary

Included:
- additive ownership/risk schema and RLS/grant protections;
- append-only ownership and risk-control event journals;
- account-scoped submit intent and owner-generation fields;
- spot entry and futures hedge account-scope derivation;
- parallel submit behavior for independent native instruments;
- fail-closed same-scope ownership and stale-generation checks;
- ENTRY_BLOCKED versus DE_RISK_ALLOWED integration at the submit fence.

Not claimed complete:
- V3 session repository wiring into the existing V2 coordinator;
- exit, rollback and force-close migration to session-owned mutation intents;
- a complete dynamic risk-budget evaluator for every configured numeric limit;
- full process heartbeat/lease renewal independent of mutation traffic;
- production deployment or database migration.

## 6. Validation contract

CI must cover:
- migration apply and idempotent replay;
- RLS and service-role grants;
- lease renewal, expiry reclaim, generation increment and stale-owner rejection;
- same-instrument mutual exclusion;
- independent-instrument parallel submits;
- global STOP interaction with shared submit locks;
- ENTRY_BLOCKED rejecting entries while DE_RISK_ALLOWED preserves approved hedges;
- Python, Go, race/vet and Docker quality gates.

No scanner source or scanner behavior is part of this work.
