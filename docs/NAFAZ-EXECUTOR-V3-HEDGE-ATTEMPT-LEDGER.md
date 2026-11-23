# PR-05 follow-up — Durable hedge attempt ledger

## Why this follow-up exists

A Spot-fill generation is a durable hedge obligation, not a single exchange order.
The existing `executor_hedge_intent_claims` row serializes the generation, but it
cannot safely represent multiple Futures order attempts after a terminal partial
fill. Reusing the same venue client order ID after an exchange has consumed it is
unsafe; treating a partial terminal order as fully hedged is also unsafe.

## Additive schema

Migration `20261123000100_add_executor_hedge_attempt_ledger.sql` adds:

- `executor_hedge_attempts`: one row per unique venue order attempt, with
  monotonically increasing attempt number, account/venue/instrument identity,
  a fresh unique client order ID, lifecycle status and optimistic version.
- `executor_hedge_attempt_fills`: append-only verified base-equivalent Futures
  fill facts, deduplicated by attempt/trade and account/venue/instrument/trade.
- `executor_hedge_attempt_events`: append-only attempt audit timeline with
  idempotent event keys.

The existing claim row remains the per-generation concurrency gate. The attempt
ledger does not replace the global STOP/mutation fence or the existing durable
submit-attempt ledger.

## Required runtime transaction contract

1. Lock the parent Spot order and its claim generation.
2. Sum only verified fill facts from all attempts for that claim.
3. Compute residual as target minus confirmed base-equivalent fills minus
   outstanding reservations from unresolved attempts.
4. Create the next attempt with a never-before-used client order ID before any
   venue mutation; bind it to the normal mutation gateway and fencing generation.
5. Never retry PREPARED/SUBMITTING/UNKNOWN/OPEN attempts by creating another
   order. Reconcile the exact attempt first.
6. A terminal partial fill closes that attempt only after order and fill
   reconciliation. A later residual may then receive a new attempt number and
   new client order ID.
7. Fill insert, attempt cumulative projection, claim aggregate projection and
   audit event must commit atomically. Duplicate venue trade replay is a no-op;
   changed immutable facts for the same trade identity are rejected.
8. The claim is CONFIRMED only when aggregate verified fills cover its target.
   Otherwise it remains unresolved/deferred and the pair stays fenced according
   to the existing risk policy.

## Safety boundary

This schema-only stage does not enable retries, submit/cancel orders, alter the
existing hedge path, run migrations against production, or change LIVE config.
Runtime wiring and PostgreSQL concurrency/recovery tests are mandatory before
the follow-up can be considered complete.
