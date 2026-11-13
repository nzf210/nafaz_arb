-- Idempotency ledger for verified realized PnL. The unique event key prevents
-- duplicate close/replay callbacks from incrementing the UTC daily loss twice.
create table if not exists public.risk_pnl_events (
    event_id text primary key,
    pair_key text not null,
    pnl_usdt numeric(38,18) not null,
    loss_day_utc date not null,
    is_mock boolean not null default true,
    occurred_at timestamptz not null default now()
);
create index if not exists idx_risk_pnl_events_day
    on public.risk_pnl_events (is_mock, loss_day_utc, occurred_at);

alter table public.risk_pnl_events enable row level security;
revoke all on table public.risk_pnl_events from anon, authenticated;
grant select, insert, update, delete on table public.risk_pnl_events to service_role;
