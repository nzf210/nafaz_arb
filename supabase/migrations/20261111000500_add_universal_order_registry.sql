-- Universal order registry: durable identity + lifecycle for every order the
-- executor submits, so a restart, container restart or VPS reboot can re-attach
-- exchange orders to their pair, position and lifecycle.
--
-- Why a separate table instead of extending public.orders:
--   * public.orders is pair-scoped reporting keyed by a generated uuid and its
--     columns are shaped for per-leg analytics.
--   * Recovery needs a bot-scoped identity that survives even when no position
--     row exists yet (an entry order placed one millisecond before the crash).
--   * order_role cannot be derived from leg: a spot SELL is an exit, a rollback
--     or a force close, and only the bot knows which.
--
-- Coexistence is intentional: public.orders keeps the per-leg analytics and
-- order_fills keeps the fill ledger. This table is the recovery authority.

create table if not exists public.order_registry (
    local_order_id uuid primary key,
    client_order_id text,
    exchange_order_id text,
    -- Bot-scoped position identity ("position:<pair_key>"). Not a foreign key:
    -- an entry order must be recoverable before any position row exists.
    position_key text,
    pair_key text,
    exchange_name text not null,
    market_type text not null check (market_type in ('spot','perpetual')),
    -- Legacy-compatible leg vocabulary so existing reporting keeps working.
    leg text check (leg in ('spot_entry','futures_entry','spot_exit','futures_exit','rollback','force_close')),
    order_role text not null check (order_role in (
        'ENTRY','HEDGE','EXIT_SPOT','EXIT_FUTURES','EMERGENCY','FORCE_CLOSE','ROLLBACK'
    )),
    symbol text not null,
    side text not null check (side in ('buy','sell')),
    order_type text not null,
    status text not null,
    requested_qty numeric(38,18) not null default 0 check (requested_qty >= 0),
    filled_qty numeric(38,18) not null default 0 check (filled_qty >= 0),
    remaining_qty numeric(38,18) not null default 0 check (remaining_qty >= 0),
    limit_price numeric(38,18),
    average_fill_price numeric(38,18),
    reduce_only boolean not null default false,
    is_mock boolean not null default true,
    recovery_status text not null default 'MANAGED' check (recovery_status in (
        'MANAGED','ADOPTED','UNVERIFIED','ORPHAN_ORDER','PHANTOM'
    )),
    orphan_reason text,
    submitted_at timestamptz,
    last_reconciled_at timestamptz,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

-- Idempotency: a retried submit or a re-adopted order must update its own row.
-- Partial indexes keep multiple orphans (which have no client id) insertable.
create unique index if not exists uq_order_registry_exchange_order_id
    on public.order_registry (lower(exchange_name), exchange_order_id)
    where exchange_order_id is not null;
create unique index if not exists uq_order_registry_client_order_id
    on public.order_registry (lower(exchange_name), client_order_id)
    where client_order_id is not null;

create index if not exists idx_order_registry_open
    on public.order_registry (exchange_name, status)
    where status not in ('closed','filled','canceled','cancelled','rejected','expired');
create index if not exists idx_order_registry_pair
    on public.order_registry (pair_key, created_at desc);
create index if not exists idx_order_registry_recovery
    on public.order_registry (recovery_status, updated_at desc);
create index if not exists idx_order_registry_unlinked
    on public.order_registry (exchange_name, symbol)
    where pair_key is null;

alter table public.order_registry enable row level security;
revoke all on table public.order_registry from anon, authenticated;
grant select, insert, update, delete on table public.order_registry to service_role;

drop trigger if exists trg_order_registry_updated_at on public.order_registry;
create trigger trg_order_registry_updated_at
    before update on public.order_registry
    for each row execute function public.set_updated_at();

comment on table public.order_registry is
    'Durable universal order registry. One row per submitted order, keyed by a deterministic local id derived from the client order id. Recovery authority after a restart; public.orders remains the per-leg analytics view.';
comment on column public.order_registry.position_key is
    'Bot-scoped position identity (position:<pair_key>), nullable until an order is linked to a position.';
comment on column public.order_registry.recovery_status is
    'MANAGED/ADOPTED: linked and trusted. UNVERIFIED/PHANTOM: venue state unknown. ORPHAN_ORDER: active on exchange with no bot position; requires manual reconciliation.';
