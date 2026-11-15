-- Immutable execution fill ledger (Tahap 4.1).
-- One row per venue execution per arbitrage leg. Rows are append-only: the
-- venue trade id is the identity, and a restated fill is rejected rather than
-- overwritten. Corrections arrive as adjustment rows, never as an UPDATE.
create table if not exists public.execution_fill_ledger (
    fill_id uuid primary key default gen_random_uuid(),
    pair_key text not null,
    position_id uuid references public.positions(id) on delete set null,
    session_id text,
    leg text not null check (
        leg in ('spot_entry', 'futures_entry', 'spot_exit', 'futures_exit')
    ),
    exchange_name text not null,
    market_type text not null check (market_type in ('spot', 'perpetual')),
    symbol text not null,
    side text not null check (side in ('buy', 'sell')),
    order_id uuid references public.orders(id) on delete set null,
    exchange_order_id text,
    client_order_id text,
    exchange_fill_id text,
    qty numeric(38,18) not null check (qty > 0),
    actual_vwap_price numeric(38,18) not null check (actual_vwap_price >= 0),
    expected_vwap_price numeric(38,18),
    quote_qty numeric(38,18),
    quote_asset text,
    actual_fee_paid numeric(38,18) not null default 0 check (actual_fee_paid >= 0),
    fee_currency text,
    fee_verified boolean not null default false,
    fee_source text not null default 'LEGACY_UNVERIFIED',
    fx_rate_applied numeric(38,18),
    fx_rate_source text,
    realized_funding_usd numeric(38,18) not null default 0,
    is_mock boolean not null default true,
    filled_at timestamptz not null,
    adjustment_of uuid references public.execution_fill_ledger(fill_id) on delete restrict,
    adjustment_reason text,
    raw_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

-- Venue trade ids are stable, so replaying the same execution is a no-op.
create unique index if not exists uq_execution_fill_ledger_venue_trade
    on public.execution_fill_ledger (exchange_name, market_type, exchange_fill_id)
    where exchange_fill_id is not null and adjustment_of is null;

create index if not exists idx_execution_fill_ledger_pair_leg
    on public.execution_fill_ledger (pair_key, leg, filled_at);
create index if not exists idx_execution_fill_ledger_live_time
    on public.execution_fill_ledger (filled_at desc)
    where is_mock = false;

alter table public.execution_fill_ledger enable row level security;
revoke all on table public.execution_fill_ledger from anon, authenticated;
grant select, insert on table public.execution_fill_ledger to service_role;

comment on table public.execution_fill_ledger is
    'Append-only per-execution ledger across all four arbitrage legs. Never updated or deleted; corrections are adjustment rows referencing the original.';

-- Expected-vs-realized attribution recorded once, when a position closes fully.
create table if not exists public.pair_pnl_deviations (
    deviation_id uuid primary key default gen_random_uuid(),
    pair_key text not null,
    position_id uuid references public.positions(id) on delete set null,
    trade_key text,
    expected_net_pnl_usd numeric(38,18) not null default 0,
    realized_net_pnl_usd numeric(38,18) not null default 0,
    net_pnl_deviation_usd numeric(38,18) not null default 0,
    expected_fees_usd numeric(38,18) not null default 0,
    realized_fees_usd numeric(38,18) not null default 0,
    fee_deviation_usd numeric(38,18) not null default 0,
    slippage_deviation_usd numeric(38,18) not null default 0,
    funding_deviation_usd numeric(38,18) not null default 0,
    expected_hold_hours numeric(24,10),
    realized_hold_hours numeric(24,10),
    is_mock boolean not null default true,
    closed_at timestamptz not null,
    detail jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create unique index if not exists uq_pair_pnl_deviations_trade
    on public.pair_pnl_deviations (trade_key)
    where trade_key is not null;
create index if not exists idx_pair_pnl_deviations_pair_time
    on public.pair_pnl_deviations (pair_key, closed_at desc);

alter table public.pair_pnl_deviations enable row level security;
revoke all on table public.pair_pnl_deviations from anon, authenticated;
grant select, insert on table public.pair_pnl_deviations to service_role;

comment on table public.pair_pnl_deviations is
    'Per-closed-trade attribution of expected vs realized net PnL, decomposed into fee and slippage deviation.';
