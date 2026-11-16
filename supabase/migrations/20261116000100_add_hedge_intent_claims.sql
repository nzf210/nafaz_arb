-- Durable hedge-intent claims: cross-instance idempotency for the spot-to-
-- futures hedge pipeline.
--
-- Two executor instances processing the same spot entry must never both claim
-- the same hedge obligation (same parent spot order + same cumulative spot
-- fill generation). The claim row is the atomic gate: INSERT ... ON CONFLICT
-- DO NOTHING returning the row is a database-level CAS, so only one worker
-- can hold a generation. The stable client_order_id is derived from the
-- claim identity, so retries and restarts reuse the same venue order id and
-- the existing executor_submit_attempts fail-closed ledger keeps covering the
-- venue side-effect boundary (this table does NOT replace the STOP fence).
create table if not exists public.executor_hedge_intent_claims (
    -- "<parent_spot_exchange>:<parent_spot_order_id>:<fill_version>"
    claim_key text primary key,
    parent_spot_exchange text not null,
    parent_spot_order_id text not null,
    parent_spot_client_order_id text,
    -- Monotonic cumulative spot fill (base units) this generation hedges.
    cumulative_spot_filled numeric(38, 18) not null check (cumulative_spot_filled >= 0),
    -- Target hedge quantity in futures base-equivalent units.
    target_hedge_qty numeric(38, 18) not null check (target_hedge_qty >= 0),
    -- Residual this claim is authorized to hedge.
    residual_qty numeric(38, 18) not null check (residual_qty > 0),
    hedge_ratio numeric(18, 8) not null default 1.0 check (hedge_ratio > 0),
    futures_exchange text not null,
    futures_symbol text not null,
    -- Stable deterministic client order id for the venue submission.
    hedge_client_order_id text not null unique,
    -- Optimistic concurrency version for claim updates.
    version integer not null default 0,
    status text not null default 'CLAIMED' check (
        status in ('CLAIMED', 'SUBMITTED', 'CONFIRMED', 'ABANDONED')
    ),
    confirmed_qty numeric(38, 18) not null default 0 check (confirmed_qty >= 0),
    claimed_by text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists hedge_intent_claims_parent_idx
    on public.executor_hedge_intent_claims
    (parent_spot_exchange, parent_spot_order_id, created_at desc);

alter table public.executor_hedge_intent_claims enable row level security;
revoke all on table public.executor_hedge_intent_claims from anon, authenticated;
grant select, insert, update, delete on table public.executor_hedge_intent_claims to service_role;

comment on table public.executor_hedge_intent_claims is
'Atomic per-generation hedge claims. A claim is the only authorization to submit a hedge for a given (parent spot order, cumulative fill) generation; duplicate claims are impossible by primary-key conflict.';
