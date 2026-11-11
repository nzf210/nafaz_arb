-- Mark the order analytics ledger's execution mode explicitly.
-- Historical rows are treated as mock/unknown so replay never attributes old
-- paper-trading fills to a live exchange account.
alter table public.orders
    add column if not exists is_mock boolean not null default true;

create index if not exists idx_orders_live_fill_reconciliation
    on public.orders (updated_at desc)
    where is_mock = false and filled_qty > 0;

-- Keep pre-existing cumulative snapshots distinguishable from verified fills.
-- Do not delete historical audit rows; consumers can explicitly select the
-- exchange_trade source when calculating execution-level quantities and fees.
alter table public.order_fills
    add column if not exists fill_source text not null default 'legacy_cumulative';

create index if not exists idx_order_fills_source_order_time
    on public.order_fills (fill_source, order_id, filled_at);
