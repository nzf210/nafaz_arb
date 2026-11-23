# NAFAZ Executor V3 — Session-Bound Hedge Submit Fencing

Status: PR-03 follow-up implementation.
Scope: enforce the coordinator admission boundary for V3 HEDGE CREATE intents at the shared venue mutation fence.

## Submit contract

A V3 HEDGE mutation must be backed by an immutable PREPARED CREATE intent with both `session_id` and `leg_id`. The referenced leg must be a FUTURES leg on the exact account, venue, market type, native instrument, and pair route pinned by the session.

At the external submit boundary, in the same PostgreSQL transaction that owns the global STOP shared fence and the venue/instrument ownership lease, the gateway locks and verifies:
- the logical mutation intent is still PREPARED and is a CREATE;
- the intent's session ID matches the gateway's scoped session ID;
- the session is HEDGE_PENDING or PARTIALLY_HEDGED;
- the leg is FUTURES and PREPARED, SUBMITTING, or PARTIALLY_FILLED;
- the session coordinator ownership lease is active and its owner/generation exactly matches the session's persisted fencing generation.

A failed check rejects the mutation before any exchange client call. Legacy V2 HEDGE calls without a V3 mutation intent remain on their existing compatibility path; this PR does not change or activate V2 runtime behavior.

## Other action classes

Session-bound ENTRY retains its existing ENTRY_IN_PROGRESS + PREPARED-leg gate. Session-bound DERISK receives a coordinator-lease and exact-route check, with protective session/leg states permitted. Sessionless DERISK remains available for existing account-scoped protective operations.

## Safety and recovery

The venue call remains behind the existing Global STOP fence, scoped ownership lease, durable submit attempt, and OrderIntentService SUBMITTING transition. Ambiguous outcomes remain UNKNOWN and are never blindly retried.

This PR does not create a venue client, run a live order, modify scanner strategy/risk policy, run production migrations, or deploy to the VPS.
