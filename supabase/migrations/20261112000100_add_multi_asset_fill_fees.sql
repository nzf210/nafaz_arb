-- Preserve every exchange commission component for a single execution.
-- Scalar fee_amount/fee_asset remain populated only for a single-currency fee.
alter table public.order_fills
    add column if not exists fee_components jsonb not null default '[]'::jsonb;

comment on column public.order_fills.fee_components is
    'All verified commission components for this fill; amounts in different assets must never be summed without conversion.';
