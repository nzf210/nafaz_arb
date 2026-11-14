-- Nafazbot V2 multi-venue registry and durable execution journal.
-- Additive migration: legacy exchange_accounts/orders/order_registry tables remain intact.
-- Every financial value is NUMERIC; credentials are references only.
-- This file is versioned by its UTC timestamp and applied after 20261113000200.

create table if not exists public.v2_exchanges (
    exchange_code text primary key
        check (exchange_code ~ '^[a-z][a-z0-9_-]{1,62}$'),
    display_name text not null,
    created_at timestamptz not null default now()
);

create table if not exists public.v2_venues (
    venue_id text primary key
        check (venue_id ~ '^[a-z][a-z0-9_]{1,62}$'),
    exchange_code text not null references public.v2_exchanges(exchange_code) on delete restrict,
    market_type text not null
        check (market_type in ('spot','linear_perpetual','inverse_perpetual')),
    adapter text not null
        check (adapter ~ '^[a-z][a-z0-9_-]{1,62}$'),
    enabled boolean not null default false,
    market_data_enabled boolean not null default false,
    execution_enabled boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (venue_id, market_type)
);

create table if not exists public.v2_accounts (
    account_id text primary key
        check (account_id ~ '^[a-z][a-z0-9_]{1,62}$'),
    venue_id text not null references public.v2_venues(venue_id) on delete restrict,
    label text not null,
    enabled boolean not null default false,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (venue_id, account_id),
    unique (venue_id, label)
);

-- Only environment/secret-manager references are stored. No credential value or ciphertext.
create table if not exists public.v2_account_secret_refs (
    account_id text not null references public.v2_accounts(account_id) on delete cascade,
    credential_name text not null
        check (credential_name in ('api_key','api_secret','passphrase')),
    secret_ref text not null
        check (
            secret_ref ~ '^[A-Z_][A-Z0-9_]{1,127}$'
            or secret_ref ~ '^secret://[A-Za-z0-9._/-]{1,240}$'
        ),
    created_at timestamptz not null default now(),
    primary key (account_id, credential_name)
);

create table if not exists public.v2_venue_capabilities (
    venue_id text not null references public.v2_venues(venue_id) on delete cascade,
    capability text not null
        check (capability ~ '^[a-z][a-z0-9_]{1,63}$'),
    status text not null default 'unknown'
        check (status in ('unknown','verified','unsupported')),
    evidence_ref text,
    verified_by text,
    verified_at timestamptz,
    primary key (venue_id, capability),
    check (
        status <> 'verified'
        or (length(trim(coalesce(evidence_ref,''))) > 0
            and verified_at is not null
            and length(trim(coalesce(verified_by,''))) > 0)
    )
);

create table if not exists public.v2_markets (
    market_id text not null
        check (market_id ~ '^[a-z][a-z0-9_:-]{1,127}$'),
    venue_id text not null references public.v2_venues(venue_id) on delete restrict,
    native_symbol text not null,
    canonical_base_asset text not null
        check (canonical_base_asset ~ '^[A-Z0-9]{2,24}$'),
    quote_asset text not null
        check (quote_asset ~ '^[A-Z0-9]{2,24}$'),
    settle_asset text
        check (settle_asset is null or settle_asset ~ '^[A-Z0-9]{2,24}$'),
    contract_size numeric(38,18)
        check (contract_size is null or contract_size > 0),
    amount_step numeric(38,18) not null check (amount_step > 0),
    minimum_amount numeric(38,18) not null default 0 check (minimum_amount >= 0),
    price_tick numeric(38,18) not null check (price_tick > 0),
    minimum_notional numeric(38,18) not null default 0 check (minimum_notional >= 0),
    metadata_version bigint not null default 1 check (metadata_version > 0),
    observed_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (venue_id, market_id),
    unique (venue_id, native_symbol)
);

create table if not exists public.v2_risk_profiles (
    risk_profile_id text primary key
        check (risk_profile_id ~ '^[a-z][a-z0-9_]{1,62}$'),
    max_daily_loss_usdt numeric(38,18) not null
        check (max_daily_loss_usdt > 0),
    max_exposure_per_account_usdt numeric(38,18) not null
        check (max_exposure_per_account_usdt > 0),
    max_exposure_per_route_usdt numeric(38,18) not null
        check (max_exposure_per_route_usdt > 0),
    max_entry_slippage_pct numeric(20,10) not null
        check (max_entry_slippage_pct > 0 and max_entry_slippage_pct <= 100),
    max_consecutive_order_failures integer not null
        check (max_consecutive_order_failures > 0),
    max_open_spot_orders integer not null check (max_open_spot_orders > 0),
    max_open_futures_orders integer not null check (max_open_futures_orders > 0),
    require_fresh_balance boolean not null default true check (require_fresh_balance),
    require_position_reconciliation boolean not null default true check (require_position_reconciliation),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists public.v2_trading_routes (
    route_id text primary key
        check (route_id ~ '^[a-z][a-z0-9_]{1,62}$'),
    spot_venue_id text not null,
    spot_account_id text not null,
    futures_venue_id text not null,
    futures_account_id text not null,
    risk_profile_id text not null references public.v2_risk_profiles(risk_profile_id) on delete restrict,
    enabled boolean not null default false,
    spot_quote_assets text[] not null
        check (cardinality(spot_quote_assets) > 0),
    futures_settle_assets text[] not null
        check (cardinality(futures_settle_assets) > 0),
    symbol_allowlist text[] not null
        check (cardinality(symbol_allowlist) > 0),
    symbol_denylist text[] not null default '{}',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (spot_venue_id, spot_account_id)
        references public.v2_accounts(venue_id, account_id) on delete restrict,
    foreign key (futures_venue_id, futures_account_id)
        references public.v2_accounts(venue_id, account_id) on delete restrict,
    check (spot_venue_id <> futures_venue_id),
    check (not (symbol_allowlist && symbol_denylist))
);

create or replace function public.validate_v2_trading_route()
returns trigger language plpgsql as $route$
declare
    spot_type text;
    futures_type text;
    spot_venue_enabled boolean;
    futures_venue_enabled boolean;
    spot_market_data_enabled boolean;
    futures_market_data_enabled boolean;
    spot_execution_enabled boolean;
    futures_execution_enabled boolean;
    spot_account_enabled boolean;
    futures_account_enabled boolean;
begin
    select market_type, enabled, market_data_enabled, execution_enabled
      into spot_type, spot_venue_enabled, spot_market_data_enabled, spot_execution_enabled
      from public.v2_venues where venue_id = new.spot_venue_id;
    select market_type, enabled, market_data_enabled, execution_enabled
      into futures_type, futures_venue_enabled, futures_market_data_enabled, futures_execution_enabled
      from public.v2_venues where venue_id = new.futures_venue_id;
    select enabled into spot_account_enabled
      from public.v2_accounts
     where venue_id = new.spot_venue_id and account_id = new.spot_account_id;
    select enabled into futures_account_enabled
      from public.v2_accounts
     where venue_id = new.futures_venue_id and account_id = new.futures_account_id;

    if spot_type is distinct from 'spot' then
        raise exception 'V2 route spot leg must reference a spot venue';
    end if;
    if futures_type not in ('linear_perpetual','inverse_perpetual') then
        raise exception 'V2 route futures leg must reference a perpetual venue';
    end if;
    if new.enabled and (
        spot_venue_enabled is distinct from true
        or futures_venue_enabled is distinct from true
        or spot_market_data_enabled is distinct from true
        or futures_market_data_enabled is distinct from true
        or spot_execution_enabled is distinct from true
        or futures_execution_enabled is distinct from true
        or spot_account_enabled is distinct from true
        or futures_account_enabled is distinct from true
    ) then
        raise exception 'enabled V2 route requires enabled market-data/execution venues and accounts';
    end if;
    if new.enabled and exists (
        select 1
          from unnest(array[
              'balance_read','open_orders_read','limit_order_create',
              'order_cancel','order_lookup_by_client_id','multi_venue','multi_account'
          ]::text[]) as required(capability)
         where not exists (
             select 1 from public.v2_venue_capabilities capability
              where capability.venue_id = new.spot_venue_id
                and capability.capability = required.capability
                and capability.status = 'verified'
         )
    ) then
        raise exception 'enabled V2 spot route requires verified adapter capabilities';
    end if;
    if new.enabled and exists (
        select 1
          from unnest(array[
              'balance_read','positions_read','market_order_create','reduce_only_order',
              'order_cancel','order_lookup_by_client_id','multi_venue','multi_account'
          ]::text[]) as required(capability)
         where not exists (
             select 1 from public.v2_venue_capabilities capability
              where capability.venue_id = new.futures_venue_id
                and capability.capability = required.capability
                and capability.status = 'verified'
         )
    ) then
        raise exception 'enabled V2 futures route requires verified adapter capabilities';
    end if;
    return new;
end;
$route$;;

drop trigger if exists trg_validate_v2_trading_route on public.v2_trading_routes;
create trigger trg_validate_v2_trading_route
before insert or update of spot_venue_id, spot_account_id, futures_venue_id,
    futures_account_id, enabled
on public.v2_trading_routes
for each row execute function public.validate_v2_trading_route();

create table if not exists public.v2_execution_intents (
    intent_id uuid primary key default gen_random_uuid(),
    idempotency_key text not null unique
        check (length(trim(idempotency_key)) between 1 and 160),
    route_id text not null references public.v2_trading_routes(route_id) on delete restrict,
    venue_id text not null,
    account_id text not null,
    market_id text not null,
    canonical_symbol text not null,
    order_role text not null
        check (order_role in ('ENTRY','HEDGE','EXIT_SPOT','EXIT_FUTURES','EMERGENCY','FORCE_CLOSE','ROLLBACK')),
    side text not null check (side in ('buy','sell')),
    order_type text not null check (order_type in ('limit','market')),
    state text not null default 'RECEIVED'
        check (state in (
            'RECEIVED','VALIDATING','AUTHORIZED','SUBMITTING','ACKNOWLEDGED',
            'PARTIALLY_FILLED','FILLED','CANCEL_PENDING','CANCELED','REJECTED',
            'UNKNOWN','RECOVERY_REQUIRED','CLOSED'
        )),
    client_order_id text not null check (length(trim(client_order_id)) between 1 and 128),
    exchange_order_id text,
    requested_qty numeric(38,18) not null check (requested_qty > 0),
    limit_price numeric(38,18) check (limit_price is null or limit_price > 0),
    quote_amount numeric(38,18) check (quote_amount is null or quote_amount >= 0),
    state_version bigint not null default 0 check (state_version >= 0),
    is_mock boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (venue_id, account_id)
        references public.v2_accounts(venue_id, account_id) on delete restrict,
    foreign key (venue_id, market_id)
        references public.v2_markets(venue_id, market_id) on delete restrict,
    check (order_type <> 'limit' or limit_price is not null)
);

create unique index if not exists uq_v2_intent_client_order
    on public.v2_execution_intents(venue_id, account_id, client_order_id);
create unique index if not exists uq_v2_intent_exchange_order
    on public.v2_execution_intents(venue_id, account_id, exchange_order_id)
    where exchange_order_id is not null;
create index if not exists idx_v2_intents_route_state
    on public.v2_execution_intents(route_id, state, updated_at desc);
create index if not exists idx_v2_intents_venue_state
    on public.v2_execution_intents(venue_id, state, updated_at desc);


create or replace function public.validate_v2_execution_intent()
returns trigger language plpgsql as $intent$
declare
    route_enabled boolean;
    route_spot_venue text;
    route_spot_account text;
    route_futures_venue text;
    route_futures_account text;
    venue_enabled boolean;
    venue_execution_enabled boolean;
    account_enabled boolean;
    control_state text;
    trading_enabled boolean;
    config_signals_enabled boolean;
begin
    select enabled, spot_venue_id, spot_account_id, futures_venue_id, futures_account_id
      into route_enabled, route_spot_venue, route_spot_account,
           route_futures_venue, route_futures_account
      from public.v2_trading_routes where route_id = new.route_id;
    if route_enabled is distinct from true then
        raise exception 'V2 execution intent requires an enabled route';
    end if;
    if not (
        (new.venue_id = route_spot_venue and new.account_id = route_spot_account)
        or (new.venue_id = route_futures_venue and new.account_id = route_futures_account)
    ) then
        raise exception 'V2 execution intent venue/account is not a leg of its route';
    end if;

    select enabled, execution_enabled
      into venue_enabled, venue_execution_enabled
      from public.v2_venues where venue_id = new.venue_id;
    select enabled into account_enabled
      from public.v2_accounts
     where venue_id = new.venue_id and account_id = new.account_id;
    if venue_enabled is distinct from true
       or venue_execution_enabled is distinct from true
       or account_enabled is distinct from true then
        raise exception 'V2 execution intent venue/account execution is disabled';
    end if;

    if not new.is_mock then
        select state, trading_enabled
          into control_state, trading_enabled
          from public.operator_control_state where singleton = true;
        if control_state is distinct from 'RUNNING' or trading_enabled is distinct from true then
            raise exception 'LIVE V2 execution intent denied by durable operator control state';
        end if;
        select (config_version.snapshot->'execution'->>'signals_enabled')::boolean
          into config_signals_enabled
          from public.active_config_version active
          join public.config_versions config_version on config_version.version = active.version
         where active.singleton = true;
        if config_signals_enabled is distinct from true then
            raise exception 'LIVE V2 execution intent denied while config signals_enabled is false or missing';
        end if;
    end if;
    return new;
end;
$intent$;

drop trigger if exists trg_validate_v2_execution_intent on public.v2_execution_intents;
create trigger trg_validate_v2_execution_intent
before insert or update of route_id, venue_id, account_id, is_mock
on public.v2_execution_intents
for each row execute function public.validate_v2_execution_intent();

create table if not exists public.v2_execution_journal (
    journal_id bigserial primary key,
    event_key text not null unique
        check (length(trim(event_key)) between 1 and 200),
    intent_id uuid not null references public.v2_execution_intents(intent_id) on delete restrict,
    event_type text not null,
    from_state text,
    to_state text,
    quantity numeric(38,18) check (quantity is null or quantity >= 0),
    price numeric(38,18) check (price is null or price >= 0),
    quote_amount numeric(38,18) check (quote_amount is null or quote_amount >= 0),
    fee_amount numeric(38,18) check (fee_amount is null or fee_amount >= 0),
    realized_pnl numeric(38,18),
    asset text,
    exchange_event_at timestamptz,
    occurred_at timestamptz not null default now(),
    payload jsonb not null default '{}'::jsonb
        check (jsonb_typeof(payload) = 'object')
);
create index if not exists idx_v2_journal_intent_time
    on public.v2_execution_journal(intent_id, occurred_at, journal_id);
create index if not exists idx_v2_journal_type_time
    on public.v2_execution_journal(event_type, occurred_at desc);

create or replace function public.reject_v2_execution_journal_mutation()
returns trigger language plpgsql as $journal$
begin
    raise exception 'V2 execution journal is append-only';
end;
$journal$;

drop trigger if exists trg_v2_execution_journal_append_only on public.v2_execution_journal;
create trigger trg_v2_execution_journal_append_only
before update or delete on public.v2_execution_journal
for each row execute function public.reject_v2_execution_journal_mutation();

create table if not exists public.v2_balance_snapshots (
    snapshot_id uuid primary key default gen_random_uuid(),
    venue_id text not null,
    account_id text not null,
    asset text not null check (asset ~ '^[A-Z0-9]{2,24}$'),
    total numeric(38,18) not null check (total >= 0),
    free numeric(38,18) not null check (free >= 0),
    used numeric(38,18) not null check (used >= 0),
    observed_at timestamptz not null,
    is_mock boolean not null default true,
    foreign key (venue_id, account_id)
        references public.v2_accounts(venue_id, account_id) on delete restrict,
    check (free + used <= total + 0.000000000000000001)
);
create index if not exists idx_v2_balance_account_time
    on public.v2_balance_snapshots(venue_id, account_id, observed_at desc);

create table if not exists public.v2_position_snapshots (
    snapshot_id uuid primary key default gen_random_uuid(),
    venue_id text not null,
    account_id text not null,
    market_id text not null,
    exchange_position_id text,
    position_side text not null check (position_side in ('long','short','net')),
    contracts numeric(38,18) not null check (contracts >= 0),
    base_quantity numeric(38,18) not null check (base_quantity >= 0),
    entry_price numeric(38,18) check (entry_price is null or entry_price >= 0),
    mark_price numeric(38,18) check (mark_price is null or mark_price >= 0),
    unrealized_pnl numeric(38,18),
    margin_used numeric(38,18) check (margin_used is null or margin_used >= 0),
    margin_asset text,
    observed_at timestamptz not null,
    is_mock boolean not null default true,
    foreign key (venue_id, account_id)
        references public.v2_accounts(venue_id, account_id) on delete restrict,
    foreign key (venue_id, market_id)
        references public.v2_markets(venue_id, market_id) on delete restrict
);
create unique index if not exists uq_v2_position_exchange_identity
    on public.v2_position_snapshots(venue_id, account_id, exchange_position_id)
    where exchange_position_id is not null;
create index if not exists idx_v2_position_account_time
    on public.v2_position_snapshots(venue_id, account_id, observed_at desc);

create table if not exists public.v2_reconciliation_runs (
    run_id uuid primary key default gen_random_uuid(),
    scope text not null check (scope in ('venue','account','route')),
    venue_id text references public.v2_venues(venue_id) on delete restrict,
    account_id text references public.v2_accounts(account_id) on delete restrict,
    route_id text references public.v2_trading_routes(route_id) on delete restrict,
    status text not null check (status in ('RUNNING','SUCCEEDED','MISMATCH','UNAVAILABLE','FAILED')),
    mismatch_count integer not null default 0 check (mismatch_count >= 0),
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    summary jsonb not null default '{}'::jsonb check (jsonb_typeof(summary) = 'object'),
    check (
        (scope = 'venue' and venue_id is not null and account_id is null and route_id is null)
        or (scope = 'account' and account_id is not null and route_id is null)
        or (scope = 'route' and route_id is not null)
    ),
    check (completed_at is null or completed_at >= started_at)
);
create index if not exists idx_v2_reconciliation_scope_time
    on public.v2_reconciliation_runs(scope, started_at desc);

create table if not exists public.v2_venue_halts (
    halt_id bigserial primary key,
    scope text not null check (scope in ('venue','account','route')),
    venue_id text references public.v2_venues(venue_id) on delete restrict,
    account_id text references public.v2_accounts(account_id) on delete restrict,
    route_id text references public.v2_trading_routes(route_id) on delete restrict,
    reason text not null check (length(trim(reason)) > 0),
    active boolean not null default true,
    created_at timestamptz not null default now(),
    resolved_at timestamptz,
    check (
        (scope = 'venue' and venue_id is not null and account_id is null and route_id is null)
        or (scope = 'account' and account_id is not null and route_id is null)
        or (scope = 'route' and route_id is not null)
    ),
    check ((active and resolved_at is null) or (not active and resolved_at is not null))
);
create unique index if not exists uq_v2_active_venue_halt
    on public.v2_venue_halts(venue_id) where active and scope = 'venue';
create unique index if not exists uq_v2_active_account_halt
    on public.v2_venue_halts(account_id) where active and scope = 'account';
create unique index if not exists uq_v2_active_route_halt
    on public.v2_venue_halts(route_id) where active and scope = 'route';

-- Reuse the existing transactional outbox; execution events are routed to a
-- separate Redis channel by the publisher and are not config invalidations.
alter table public.outbox_events
    drop constraint if exists outbox_events_aggregate_type_check;
alter table public.outbox_events
    add constraint outbox_events_aggregate_type_check
    check (aggregate_type in ('config','operator_control','execution'));

-- Add nullable V2 identities to the legacy recovery registry. Existing rows
-- remain valid and are not guessed/backfilled into a V2 route.
alter table public.order_registry
    add column if not exists v2_venue_id text references public.v2_venues(venue_id) on delete restrict;
alter table public.order_registry
    add column if not exists v2_account_id text references public.v2_accounts(account_id) on delete restrict;
alter table public.order_registry
    add column if not exists v2_route_id text references public.v2_trading_routes(route_id) on delete restrict;
alter table public.order_registry
    add column if not exists v2_market_id text;
create index if not exists idx_order_registry_v2_identity
    on public.order_registry(v2_venue_id, v2_account_id, v2_route_id)
    where v2_venue_id is not null;

-- Private service access only. The journal is append-only even for service_role.
do $security$
declare
    table_name text;
begin
    foreach table_name in array array[
        'v2_exchanges','v2_venues','v2_accounts','v2_account_secret_refs',
        'v2_venue_capabilities','v2_markets','v2_risk_profiles',
        'v2_trading_routes','v2_execution_intents','v2_execution_journal',
        'v2_balance_snapshots','v2_position_snapshots',
        'v2_reconciliation_runs','v2_venue_halts'
    ] loop
        execute format('alter table public.%I enable row level security', table_name);
        execute format('revoke all on table public.%I from public', table_name);
        if exists (select 1 from pg_roles where rolname = 'anon') then
            execute format('revoke all on table public.%I from anon', table_name);
        end if;
        if exists (select 1 from pg_roles where rolname = 'authenticated') then
            execute format('revoke all on table public.%I from authenticated', table_name);
        end if;
        if exists (select 1 from pg_roles where rolname = 'service_role') then
            if table_name = 'v2_execution_journal' then
                execute format('grant select, insert on table public.%I to service_role', table_name);
            else
                execute format('grant select, insert, update, delete on table public.%I to service_role', table_name);
            end if;
        end if;
    end loop;

    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant usage, select on sequence public.v2_execution_journal_journal_id_seq, public.v2_venue_halts_halt_id_seq to service_role';
        execute 'grant select, insert, update on public.outbox_events to service_role';
        execute 'grant select, insert, update on public.order_registry to service_role';
    end if;
end;
$security$;

do $v2_order_registry_fks$
begin
    if not exists (
        select 1 from pg_constraint
         where conname = 'fk_order_registry_v2_account_venue'
           and conrelid = 'public.order_registry'::regclass
    ) then
        alter table public.order_registry
            add constraint fk_order_registry_v2_account_venue
            foreign key (v2_venue_id, v2_account_id)
            references public.v2_accounts(venue_id, account_id) on delete restrict;
    end if;
    if not exists (
        select 1 from pg_constraint
         where conname = 'fk_order_registry_v2_market_venue'
           and conrelid = 'public.order_registry'::regclass
    ) then
        alter table public.order_registry
            add constraint fk_order_registry_v2_market_venue
            foreign key (v2_venue_id, v2_market_id)
            references public.v2_markets(venue_id, market_id) on delete restrict;
    end if;
end;
$v2_order_registry_fks$;

comment on table public.v2_venues is
    'V2 venue identity is separate from exchange brand and account; all flags default disabled.';
comment on table public.v2_account_secret_refs is
    'Secret reference names only. Credential material must remain in the executor secret environment/store.';
comment on table public.v2_venue_capabilities is
    'Capability evidence registry. Unknown/unverified capabilities never authorize a route.';
comment on table public.v2_markets is
    'Venue-bound market metadata; financial precision and contract quantities use NUMERIC.';
comment on table public.v2_trading_routes is
    'Explicit spot/perpetual pairing. Routes default disabled and are validated against venue market types.';
comment on table public.v2_execution_intents is
    'Durable idempotent order intent written before any external exchange submission.';
comment on table public.v2_execution_journal is
    'Append-only V2 execution state/fill journal; financial columns use NUMERIC.';
comment on table public.outbox_events is
    'Transactional outbox for config, operator-control and execution events; Redis remains transient.';
