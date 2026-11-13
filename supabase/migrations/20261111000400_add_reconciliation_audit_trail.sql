-- Immutable, queryable audit history for order recovery and reconciliation.
-- Unlike mutable order_registry snapshots, each distinct event is retained once.
create table if not exists public.reconciliation_audit (
    audit_id bigint generated always as identity primary key,
    event_id text not null unique,
    event_type text not null,
    pair_key text,
    exchange_name text,
    market_type text check (market_type is null or market_type in ('spot','perpetual')),
    entity_type text not null default 'order',
    entity_id text,
    from_state text,
    to_state text,
    reason text,
    actor text not null default 'executor',
    is_mock boolean not null default true,
    occurred_at timestamptz not null default now(),
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_reconciliation_audit_pair_time
    on public.reconciliation_audit (pair_key, occurred_at desc);
create index if not exists idx_reconciliation_audit_entity_time
    on public.reconciliation_audit (entity_type, entity_id, occurred_at desc);
create index if not exists idx_reconciliation_audit_type_time
    on public.reconciliation_audit (event_type, occurred_at desc);
create index if not exists idx_reconciliation_audit_live_time
    on public.reconciliation_audit (occurred_at desc)
    where is_mock = false;

alter table public.reconciliation_audit enable row level security;
revoke all on table public.reconciliation_audit from anon, authenticated;
grant select, insert on table public.reconciliation_audit to service_role;
revoke update, delete, truncate on table public.reconciliation_audit from service_role;

comment on table public.reconciliation_audit is
    'Append-only, idempotent audit trail for order lifecycle, orphan detection, recovery, and reconciliation decisions. Mutable current state remains in order_registry.';
comment on column public.reconciliation_audit.event_id is
    'Stable source event identity; unique to make replay and outbox retries idempotent.';
