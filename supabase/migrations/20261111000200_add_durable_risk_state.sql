-- Durable risk state snapshots. Unlike Redis, this table survives cache loss and
-- is queried synchronously during executor startup before entry listeners start.
create table if not exists public.risk_state (
    state_id text primary key,
    state_type text not null,
    scope text not null check (scope in ('global','exchange','pair','position')),
    exchange text,
    pair_key text,
    reason text not null default '',
    value jsonb not null default 'null'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    resolved_at timestamptz,
    is_mock boolean not null default true,
    constraint risk_state_scope_fields check (
        (scope = 'global' and exchange is null and pair_key is null)
        or (scope = 'exchange' and exchange is not null and pair_key is null)
        or (scope in ('pair','position') and pair_key is not null)
    )
);
create index if not exists idx_risk_state_active_scope
    on public.risk_state (is_mock, scope, state_type, updated_at desc)
    where resolved_at is null;
create index if not exists idx_risk_state_active_exchange
    on public.risk_state (exchange, updated_at desc)
    where resolved_at is null;
create index if not exists idx_risk_state_active_pair
    on public.risk_state (pair_key, updated_at desc)
    where resolved_at is null;

alter table public.risk_state enable row level security;
revoke all on table public.risk_state from anon, authenticated;
grant select, insert, update, delete on table public.risk_state to service_role;

drop trigger if exists trg_risk_state_updated_at on public.risk_state;
create trigger trg_risk_state_updated_at before update on public.risk_state
    for each row execute function public.set_updated_at();
