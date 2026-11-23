-- Durable, per-attempt hedge submission ledger.
--
-- A hedge claim represents one cumulative Spot-fill generation. It is NOT a
-- venue order attempt: a generation may require multiple distinct venue orders
-- after terminal partial fills. Every attempt therefore owns a fresh client
-- order ID, while verified Futures fills are appended once and aggregated.
--
-- This migration is additive. Existing claim rows remain valid and are not
-- rewritten. Runtime adoption is intentionally a separate, tested change.

create table if not exists public.executor_hedge_attempts (
    attempt_id uuid primary key default gen_random_uuid(),
    claim_key text not null
        references public.executor_hedge_intent_claims(claim_key)
        on delete restrict,
    attempt_no integer not null check (attempt_no > 0),
    futures_account_scope_id text not null
        check (length(trim(futures_account_scope_id)) > 0),
    futures_exchange text not null
        check (length(trim(futures_exchange)) > 0),
    futures_symbol text not null
        check (length(trim(futures_symbol)) > 0),
    hedge_client_order_id text not null unique
        check (length(trim(hedge_client_order_id)) > 0),
    venue_order_id text,
    requested_qty_base numeric(38, 18) not null
        check (requested_qty_base > 0),
    status text not null default 'PREPARED' check (
        status in (
            'PREPARED', 'SUBMITTING', 'UNKNOWN', 'ACKNOWLEDGED',
            'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED',
            'REJECTED', 'RECONCILIATION_REQUIRED'
        )
    ),
    confirmed_qty_base numeric(38, 18) not null default 0
        check (confirmed_qty_base >= 0),
    version bigint not null default 0 check (version >= 0),
    submitted_at timestamptz,
    terminal_at timestamptz,
    last_reconciled_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint executor_hedge_attempt_claim_number_key unique (claim_key, attempt_no),
    check (confirmed_qty_base <= requested_qty_base)
);

create index if not exists executor_hedge_attempts_claim_idx
    on public.executor_hedge_attempts (claim_key, attempt_no);

create index if not exists executor_hedge_attempts_recovery_idx
    on public.executor_hedge_attempts (status, updated_at)
    where status in (
        'SUBMITTING', 'UNKNOWN', 'ACKNOWLEDGED', 'OPEN',
        'PARTIALLY_FILLED', 'RECONCILIATION_REQUIRED'
    );

create table if not exists public.executor_hedge_attempt_fills (
    fill_id uuid primary key default gen_random_uuid(),
    attempt_id uuid not null
        references public.executor_hedge_attempts(attempt_id)
        on delete restrict,
    futures_account_scope_id text not null
        check (length(trim(futures_account_scope_id)) > 0),
    futures_exchange text not null
        check (length(trim(futures_exchange)) > 0),
    futures_symbol text not null
        check (length(trim(futures_symbol)) > 0),
    venue_trade_id text not null
        check (length(trim(venue_trade_id)) > 0),
    source_event_id text,
    fill_qty_base numeric(38, 18) not null check (fill_qty_base > 0),
    fill_price numeric(38, 18) not null check (fill_price > 0),
    fee_amount numeric(38, 18) check (fee_amount is null or fee_amount >= 0),
    fee_currency text,
    venue_filled_at timestamptz,
    observed_at timestamptz not null default now(),
    constraint executor_hedge_fill_attempt_trade_key unique (attempt_id, venue_trade_id),
    constraint executor_hedge_fill_trade_scope_key unique (futures_account_scope_id, futures_exchange, futures_symbol, venue_trade_id),
    constraint executor_hedge_fill_source_event_key unique (attempt_id, source_event_id)
);

create index if not exists executor_hedge_attempt_fills_attempt_idx
    on public.executor_hedge_attempt_fills (attempt_id, observed_at);

create table if not exists public.executor_hedge_attempt_events (
    event_id uuid primary key default gen_random_uuid(),
    attempt_id uuid not null
        references public.executor_hedge_attempts(attempt_id)
        on delete restrict,
    event_key text not null check (length(trim(event_key)) > 0),
    event_type text not null check (length(trim(event_type)) > 0),
    payload jsonb not null default '{}'::jsonb,
    occurred_at timestamptz not null default now(),
    unique (attempt_id, event_key)
);

create index if not exists executor_hedge_attempt_events_timeline_idx
    on public.executor_hedge_attempt_events (attempt_id, occurred_at, event_id);

alter table public.executor_hedge_attempts enable row level security;
alter table public.executor_hedge_attempt_fills enable row level security;
alter table public.executor_hedge_attempt_events enable row level security;

revoke all on table public.executor_hedge_attempts from anon, authenticated;
revoke all on table public.executor_hedge_attempt_fills from anon, authenticated;
revoke all on table public.executor_hedge_attempt_events from anon, authenticated;

grant select, insert, update on table public.executor_hedge_attempts to service_role;
-- Fill facts and event facts are append-only by privilege, including for the
-- runtime service role. Corrections must be represented by separate facts.
grant select, insert on table public.executor_hedge_attempt_fills to service_role;
grant select, insert on table public.executor_hedge_attempt_events to service_role;

comment on table public.executor_hedge_attempts is
'One durable row per distinct venue hedge order attempt within a Spot-fill generation. Each retry after a terminal partial fill requires a new attempt_no and client order ID.';
comment on table public.executor_hedge_attempt_fills is
'Append-only, idempotent verified Futures fill facts. Base-equivalent quantity is authoritative for hedge coverage.';
comment on table public.executor_hedge_attempt_events is
'Append-only per-attempt audit events with idempotent event keys.';
