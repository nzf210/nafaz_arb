-- Store fee conversions in each fill's own quote asset.
-- IDR-to-USDT and third-asset FX are intentionally not inferred here.
alter table public.order_fills
    add column if not exists fee_quote_amount numeric(38,18),
    add column if not exists fee_quote_asset text,
    add column if not exists fee_conversion_verified boolean not null default false,
    add column if not exists fee_conversion_details jsonb not null default '[]'::jsonb;

comment on column public.order_fills.fee_quote_amount is
    'Fee total converted only into this fill market quote asset; null when any component needs an unavailable FX rate.';
comment on column public.order_fills.fee_conversion_verified is
    'True only when every fee component was converted to the market quote asset without an inferred FX rate.';
