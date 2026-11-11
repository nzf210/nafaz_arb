-- Mark the order analytics ledger's execution mode explicitly.
-- Historical rows are treated as mock/unknown so replay never attributes old
-- paper-trading fills to a live exchange account.
alter table public.orders
    add column if not exists is_mock boolean not null default true;

create index if not exists idx_orders_live_fill_reconciliation
    on public.orders (updated_at desc)
    where is_mock = false and filled_qty > 0;
