# NAFAZ Executor V3 — Intent-Bound CREATE Mutation Path

Status: PR-03E create-path integration. CANCEL integration is a separate follow-up.
Scope: Executor only. No engine call-site cutover.

## 1. Call boundary

The V3 caller creates an immutable CREATE intent through `OrderIntentService.prepare_create`, then calls `MutationGateway.execute_create_intent(client, intent_id)`.

The gateway loads the canonical intent and uses only its persisted venue, market type, native instrument, side, order type, quantity, price, pair route, account scope, session/leg identity and stable client order ID. It verifies the supplied venue client matches the intent venue.

## 2. Before the venue request

1. The gateway rejects any intent not in PREPARED. SUBMITTING and UNKNOWN return SUBMISSION_UNKNOWN without another HTTP call.
2. The external submit-attempt row is prepared with a foreign-key link to the logical mutation intent. Its complete order parameters must exactly match the immutable intent.
3. The existing global STOP fence acquires the global shared lock and exact instrument mutation scope.
4. It acquires ownership, evaluates current risk controls, checks unresolved attempts and global STOP permission.
5. In the same PostgreSQL transaction that changes `executor_submit_attempts` to SUBMITTING, `OrderIntentService.mark_submitting_in_transaction` changes the logical intent to SUBMITTING, persists owner/scope/generation and the exact risk-control snapshot, and appends the lifecycle event.
6. Only after that transaction commits does the gateway enter the venue HTTP call.

A failure in either durable transition rolls back both projections and the venue call is not entered.

## 3. After the venue request

- Definitive venue rejection is persisted to both attempt and logical intent.
- Timeout, disconnect, malformed acknowledgement, or persistence uncertainty leaves both records unresolved/UNKNOWN; it never retries.
- A valid venue ACK is persisted first to the submit attempt and then to the logical intent. If the second write fails, the gateway returns SUBMISSION_UNKNOWN; it never reports durable success from only one projection.
- The exact lease owner/generation captured by the fence is required for all logical intent outcome transitions.

## 4. Compatibility and safety

- Existing V2 submit attempts keep `mutation_intent_id=NULL` and retain their current path.
- A V3 attempt cannot be submitted without a matching PREPARED logical intent.
- A V3 attempt cannot be prepared with different venue, market, instrument, route, side, type, quantity, price, account scope or client order ID.
- The existing V2 gateway entrypoint remains available. No existing engine call site is switched in this PR.
- CANCEL intent execution is not included yet; its own fence must allow protective cancellation under STOP and require terminal/fill evidence.
- No scanner changes, production DB migration, VPS deployment, or live exchange action.
