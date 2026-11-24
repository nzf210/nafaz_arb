-- Durable local spot-market exposure claims.
-- The claim is committed atomically with PREPARED -> SUBMITTING while the
-- shared session advisory fence is held. It survives Redis loss and DB
-- disconnects during an in-flight venue request.
create table if not exists public.executor_market_exposure_reservations (
    reservation_key text primary key,
    local_exchange text not null,
    base_asset text not null,
    quote_asset text not null,
    pair_key text not null,
    source_client_order_id text unique,
    status text not null check (status in ('ACTIVE','RECONCILIATION_REQUIRED','RELEASED')),
    source text not null,
    release_evidence jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    released_at timestamptz
);

create unique index if not exists uq_executor_market_exposure_active_scope
    on public.executor_market_exposure_reservations
       (lower(local_exchange), upper(base_asset), upper(quote_asset))
    where status in ('ACTIVE','RECONCILIATION_REQUIRED');
create index if not exists idx_executor_market_exposure_pair
    on public.executor_market_exposure_reservations (pair_key, status);
create index if not exists idx_executor_market_exposure_source_client
    on public.executor_market_exposure_reservations (source_client_order_id)
    where source_client_order_id is not null;

alter table public.executor_market_exposure_reservations enable row level security;
revoke all on table public.executor_market_exposure_reservations from anon, authenticated;
grant select, insert, update, delete on table public.executor_market_exposure_reservations to service_role;

drop trigger if exists trg_executor_market_exposure_updated_at
    on public.executor_market_exposure_reservations;
create trigger trg_executor_market_exposure_updated_at
    before update on public.executor_market_exposure_reservations
    for each row execute function public.set_updated_at();

comment on table public.executor_market_exposure_reservations is
'Canonical cross-process local spot market admission claim. ACTIVE and RECONCILIATION_REQUIRED block new spot BUY exposure until definitive rejection or venue-verified spot and futures flat evidence releases the claim.';
