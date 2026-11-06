-- NAFAZ core operational schema for Supabase Postgres.
-- All financial quantities use NUMERIC; do not use float/double precision for persisted money.
-- Backend access is service-role only. Never expose the service-role key to a browser/client.

create extension if not exists pgcrypto;

create table if not exists public.exchange_accounts (
    id uuid primary key default gen_random_uuid(),
    exchange_name text not null,
    market_type text not null check (market_type in ('spot','perpetual')),
    account_label text not null default 'default',
    is_testnet boolean not null default false,
    is_enabled boolean not null default true,
    api_key_ciphertext text,
    api_secret_ciphertext text,
    passphrase_ciphertext text,
    encryption_key_version integer,
    permissions jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (exchange_name, market_type, account_label)
);

create table if not exists public.strategy_configs (
    id uuid primary key default gen_random_uuid(),
    name text not null unique,
    is_enabled boolean not null default false,
    execution_mode text not null default 'MAKER_TAKER',
    config jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.risk_limits (
    id uuid primary key default gen_random_uuid(),
    name text not null unique,
    is_enabled boolean not null default true,
    max_daily_loss_usdt numeric(38,18),
    max_exposure_per_exchange_usdt numeric(38,18),
    max_position_usdt numeric(38,18),
    max_entry_slippage_pct numeric(20,10),
    max_consecutive_order_failures integer,
    parameters jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.market_pairs (
    id uuid primary key default gen_random_uuid(),
    pair_key text not null unique,
    local_exchange text not null,
    global_exchange text not null,
    local_symbol text not null,
    global_symbol text not null,
    base_asset text not null,
    local_quote_asset text not null,
    global_quote_asset text not null default 'USDT',
    price_tick numeric(38,18),
    lot_size numeric(38,18),
    is_enabled boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.scanner_opportunities (
    id uuid primary key default gen_random_uuid(),
    scan_id text,
    pair_key text not null,
    local_exchange text not null,
    global_exchange text not null,
    local_symbol text not null,
    global_symbol text not null,
    quote_asset text not null,
    execution_mode text,
    eligibility_status text not null,
    reason text,
    spot_price numeric(38,18),
    perp_price numeric(38,18),
    spread_pct numeric(20,10),
    funding_rate numeric(24,16),
    estimated_net_profit numeric(38,18),
    estimated_net_roi_pct numeric(20,10),
    executable_qty numeric(38,18),
    capacity_pct numeric(12,6),
    observed_at timestamptz not null,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create index if not exists idx_scanner_opportunities_pair_time
    on public.scanner_opportunities (pair_key, observed_at desc);
create index if not exists idx_scanner_opportunities_eligible_time
    on public.scanner_opportunities (eligibility_status, observed_at desc);

create table if not exists public.positions (
    id uuid primary key default gen_random_uuid(),
    pair_key text not null,
    local_exchange text not null,
    global_exchange text not null,
    local_symbol text not null,
    global_symbol text not null,
    base_asset text not null,
    quote_asset text not null,
    local_exchange_account_id uuid references public.exchange_accounts(id) on delete set null,
    global_exchange_account_id uuid references public.exchange_accounts(id) on delete set null,
    status text not null check (status in (
        'ENTRY_PENDING','ENTRY_PARTIAL','HEDGED_ACTIVE','EXIT_PENDING',
        'RECONCILIATION_REQUIRED','CLOSED','FAILED'
    )),
    execution_mode text not null default 'MAKER_TAKER',
    is_mock boolean not null default true,
    qty numeric(38,18) not null default 0 check (qty >= 0),
    initial_qty numeric(38,18) not null default 0 check (initial_qty >= 0),
    spot_entry_avg_price numeric(38,18),
    futures_short_entry_avg_price numeric(38,18),
    spot_cost_quote numeric(38,18),
    futures_margin_quote numeric(38,18),
    accumulated_funding numeric(38,18) not null default 0,
    funding_asset text not null default 'USDT',
    current_funding_rate numeric(24,16),
    last_funding_time timestamptz,
    next_funding_time timestamptz,
    opened_at timestamptz,
    closed_at timestamptz,
    state_snapshot jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_positions_pair_status
    on public.positions (pair_key, status);
create index if not exists idx_positions_opened_at
    on public.positions (opened_at desc);

create unique index if not exists uq_positions_one_active_pair
    on public.positions (pair_key, is_mock)
    where status in ('ENTRY_PENDING','ENTRY_PARTIAL','HEDGED_ACTIVE','EXIT_PENDING','RECONCILIATION_REQUIRED');

create table if not exists public.orders (
    id uuid primary key default gen_random_uuid(),
    position_id uuid references public.positions(id) on delete set null,
    pair_key text not null,
    exchange_name text not null,
    exchange_account_id uuid references public.exchange_accounts(id) on delete set null,
    market_type text not null check (market_type in ('spot','perpetual')),
    leg text not null check (leg in ('spot_entry','futures_entry','spot_exit','futures_exit','rollback','force_close')),
    exchange_order_id text,
    client_order_id text,
    symbol text not null,
    side text not null check (side in ('buy','sell')),
    order_type text not null,
    status text not null,
    requested_qty numeric(38,18) not null default 0,
    filled_qty numeric(38,18) not null default 0,
    remaining_qty numeric(38,18),
    limit_price numeric(38,18),
    average_fill_price numeric(38,18),
    quote_asset text,
    fee_amount numeric(38,18),
    fee_asset text,
    reduce_only boolean not null default false,
    submitted_at timestamptz,
    acknowledged_at timestamptz,
    completed_at timestamptz,
    last_reconciled_at timestamptz,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_orders_pair_created on public.orders (pair_key, created_at desc);
create index if not exists idx_orders_status on public.orders (status, updated_at desc);
create unique index if not exists uq_orders_exchange_client_id
    on public.orders (exchange_name, client_order_id)
    where client_order_id is not null;
create unique index if not exists uq_orders_exchange_order_id
    on public.orders (exchange_name, exchange_order_id)
    where exchange_order_id is not null;

create table if not exists public.order_fills (
    id uuid primary key default gen_random_uuid(),
    order_id uuid not null references public.orders(id) on delete cascade,
    exchange_name text not null,
    exchange_fill_id text,
    qty numeric(38,18) not null check (qty > 0),
    price numeric(38,18) not null check (price >= 0),
    quote_qty numeric(38,18),
    fee_amount numeric(38,18),
    fee_asset text,
    filled_at timestamptz not null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create unique index if not exists uq_order_fills_exchange_fill
    on public.order_fills (exchange_name, exchange_fill_id)
    where exchange_fill_id is not null;
create index if not exists idx_order_fills_order_time on public.order_fills (order_id, filled_at);

create table if not exists public.position_legs (
    id uuid primary key default gen_random_uuid(),
    position_id uuid not null references public.positions(id) on delete cascade,
    leg text not null check (leg in ('spot','perpetual')),
    exchange_name text not null,
    symbol text not null,
    side text not null check (side in ('long','short')),
    status text not null,
    qty numeric(38,18) not null default 0,
    average_entry_price numeric(38,18),
    average_exit_price numeric(38,18),
    realized_pnl numeric(38,18),
    fee_amount numeric(38,18),
    fee_asset text,
    updated_at timestamptz not null default now(),
    unique (position_id, leg)
);

create table if not exists public.funding_payments (
    id uuid primary key default gen_random_uuid(),
    position_id uuid references public.positions(id) on delete set null,
    exchange_name text not null,
    exchange_account_id uuid references public.exchange_accounts(id) on delete set null,
    symbol text not null,
    exchange_payment_id text,
    funding_rate numeric(24,16),
    position_qty numeric(38,18),
    mark_price numeric(38,18),
    amount numeric(38,18) not null,
    asset text not null default 'USDT',
    payment_status text not null default 'confirmed'
        check (payment_status in ('estimated','confirmed','reconciled','reversed')),
    settled_at timestamptz not null,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create unique index if not exists uq_funding_exchange_payment
    on public.funding_payments (exchange_name, exchange_payment_id)
    where exchange_payment_id is not null;
create index if not exists idx_funding_position_time
    on public.funding_payments (position_id, settled_at desc);

create table if not exists public.closed_trades (
    id uuid primary key default gen_random_uuid(),
    position_id uuid references public.positions(id) on delete set null,
    trade_key text not null unique,
    pair_key text not null,
    local_exchange text not null,
    global_exchange text not null,
    symbol text not null,
    qty numeric(38,18) not null,
    entry_price_spot numeric(38,18),
    exit_price_spot numeric(38,18),
    entry_price_perp numeric(38,18),
    exit_price_perp numeric(38,18),
    spot_pnl numeric(38,18) not null default 0,
    futures_pnl numeric(38,18) not null default 0,
    funding_pnl numeric(38,18) not null default 0,
    fees numeric(38,18) not null default 0,
    net_pnl numeric(38,18) not null default 0,
    pnl_asset text not null default 'USDT',
    net_roi_pct numeric(20,10),
    is_mock boolean not null default true,
    close_reason text,
    opened_at timestamptz,
    closed_at timestamptz not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create index if not exists idx_closed_trades_closed_at on public.closed_trades (closed_at desc);
create index if not exists idx_closed_trades_pair on public.closed_trades (pair_key, closed_at desc);

create table if not exists public.balance_snapshots (
    id uuid primary key default gen_random_uuid(),
    exchange_name text not null,
    exchange_account_id uuid references public.exchange_accounts(id) on delete set null,
    market_type text not null check (market_type in ('spot','perpetual')),
    asset text not null,
    total numeric(38,18) not null default 0,
    free numeric(38,18) not null default 0,
    used numeric(38,18) not null default 0,
    is_mock boolean not null default true,
    observed_at timestamptz not null default now(),
    raw_payload jsonb not null default '{}'::jsonb
);
create index if not exists idx_balance_snapshots_exchange_time
    on public.balance_snapshots (exchange_name, observed_at desc);

create table if not exists public.risk_events (
    id uuid primary key default gen_random_uuid(),
    event_key text not null unique,
    severity text not null check (severity in ('info','warning','error','critical')),
    event_type text not null,
    pair_key text,
    exchange_name text,
    message text not null,
    action_taken text,
    is_mock boolean not null default true,
    details jsonb not null default '{}'::jsonb,
    occurred_at timestamptz not null default now(),
    created_at timestamptz not null default now()
);
create index if not exists idx_risk_events_time on public.risk_events (occurred_at desc);
create index if not exists idx_risk_events_pair_time on public.risk_events (pair_key, occurred_at desc);

create table if not exists public.executor_status_snapshots (
    id uuid primary key default gen_random_uuid(),
    instance_id text not null,
    status text not null,
    mode text not null,
    is_mock boolean not null default true,
    snapshot jsonb not null,
    observed_at timestamptz not null default now()
);
create index if not exists idx_executor_status_instance_time
    on public.executor_status_snapshots (instance_id, observed_at desc);

create table if not exists public.executor_events (
    event_id text primary key,
    event_type text not null,
    pair_key text,
    position_id uuid references public.positions(id) on delete set null,
    order_id uuid references public.orders(id) on delete set null,
    is_mock boolean not null default true,
    occurred_at timestamptz not null default now(),
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);
create index if not exists idx_executor_events_pair_time
    on public.executor_events (pair_key, occurred_at desc);
create index if not exists idx_executor_events_type_time
    on public.executor_events (event_type, occurred_at desc);

-- No anon/authenticated access is granted. The backend must use the Supabase
-- service-role key from a secret manager/environment variable, never a browser.
do $$
declare
    tbl text;
begin
    foreach tbl in array array[
        'exchange_accounts','strategy_configs','risk_limits','market_pairs',
        'scanner_opportunities','positions','orders','order_fills','position_legs',
        'funding_payments','closed_trades','balance_snapshots','risk_events',
        'executor_events','executor_status_snapshots'
    ] loop
        execute format('alter table public.%I enable row level security', tbl);
        execute format('revoke all on table public.%I from anon, authenticated', tbl);
        execute format('grant select, insert, update, delete on table public.%I to service_role', tbl);
    end loop;
end $$;

comment on table public.orders is 'Exchange order lifecycle; idempotent client_order_id is recorded before/after exchange reconciliation.';
comment on table public.order_fills is 'Append-only exchange fills, deduplicated by exchange fill id.';
comment on table public.funding_payments is 'Actual funding settlements should be recorded as confirmed/reconciled; estimates must remain explicitly estimated.';
comment on table public.executor_events is 'Append-only event journal for recovery, audit, and operational traceability.';
