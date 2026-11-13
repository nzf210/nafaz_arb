-- Durable ledger for every PostgreSQL-fenced create-order side effect.
-- A SUBMITTING/UNKNOWN row survives process or DB-connection loss and blocks
-- subsequent new entries until an exact venue lookup/reconciliation resolves it.
create table if not exists public.executor_submit_attempts (
    client_order_id text primary key,
    exchange text not null,
    market_type text not null,
    symbol text not null,
    side text not null,
    order_type text not null,
    quantity double precision not null check (quantity > 0),
    price double precision not null check (price >= 0),
    pair_key text not null,
    amount_spec jsonb not null default '{}'::jsonb,
    recovery_context jsonb not null default '{}'::jsonb,
    status text not null check (
        status in ('PREPARED', 'SUBMITTING', 'UNKNOWN', 'ACKNOWLEDGED', 'REJECTED')
    ),
    submit_count integer not null default 0 check (submit_count >= 0),
    fence_epoch bigint,
    exchange_order_id text,
    venue_response jsonb,
    last_error text,
    last_lookup_at timestamptz,
    last_lookup_result text,
    created_at timestamptz not null default now(),
    submit_started_at timestamptz,
    resolved_at timestamptz,
    updated_at timestamptz not null default now()
);

create index if not exists executor_submit_attempts_unresolved_idx
    on public.executor_submit_attempts (submit_started_at, created_at)
    where status in ('SUBMITTING', 'UNKNOWN');

create index if not exists executor_submit_attempts_exchange_client_idx
    on public.executor_submit_attempts (exchange, client_order_id);

alter table public.executor_submit_attempts enable row level security;
revoke all on table public.executor_submit_attempts from anon, authenticated;
grant select, insert, update, delete on table public.executor_submit_attempts to service_role;

comment on table public.executor_submit_attempts is
'Durable intent/side-effect ledger. SUBMITTING and UNKNOWN are fail-closed unresolved states and must never be auto-retried.';
