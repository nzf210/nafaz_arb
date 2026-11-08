-- Keep estimated and exchange-reconciled PnL distinct.
-- The executor currently calculates fee-adjusted estimates from operator-supplied
-- per-venue fee profiles. Actual fill commission reconciliation is not yet wired
-- into the runtime, so realized_pnl intentionally remains NULL until that exists.
alter table public.closed_trades
    add column if not exists gross_pnl numeric(38,18),
    add column if not exists gross_pnl_asset text not null default 'USDT',
    add column if not exists estimated_fees numeric(38,18),
    add column if not exists estimated_fees_asset text not null default 'USDT',
    add column if not exists estimated_net_pnl numeric(38,18),
    add column if not exists estimated_net_pnl_asset text not null default 'USDT',
    add column if not exists realized_pnl numeric(38,18),
    add column if not exists realized_pnl_asset text,
    add column if not exists pnl_status text not null default 'estimated';

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'closed_trades_pnl_status_check'
          and conrelid = 'public.closed_trades'::regclass
    ) then
        alter table public.closed_trades
            add constraint closed_trades_pnl_status_check
            check (pnl_status in ('estimated', 'unverified', 'reconciled'));
    end if;
end $$;

comment on column public.closed_trades.gross_pnl is
    'Gross PnL before fees; not exchange-reconciled unless pnl_status=reconciled.';
comment on column public.closed_trades.estimated_fees is
    'Estimated fees calculated from configured venue fee profiles.';
comment on column public.closed_trades.estimated_net_pnl is
    'Gross PnL minus estimated fees; remains an estimate until fill commissions are reconciled.';
comment on column public.closed_trades.realized_pnl is
    'Nullable exchange-reconciled realized PnL; populated only after actual fills, fees and funding are reconciled.';
comment on column public.closed_trades.pnl_status is
    'estimated: fee-profile estimate; unverified: required FX or fill data missing; reconciled: exchange fills and actual fees confirmed.';
