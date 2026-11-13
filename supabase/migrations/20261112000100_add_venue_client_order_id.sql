-- Preserve the exact client-order-id spelling accepted by the venue.
-- Tokocrypto rewrites logical IDs; recovery needs both identities after restart.
alter table public.order_registry
    add column if not exists venue_client_order_id text;

create unique index if not exists uq_order_registry_venue_client_order_id
    on public.order_registry (lower(exchange_name), venue_client_order_id)
    where venue_client_order_id is not null and venue_client_order_id <> '';

comment on column public.order_registry.venue_client_order_id is
    'Exact client order ID echoed/accepted by the venue; distinct from the bot logical client_order_id.';
