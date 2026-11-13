-- Durable single-writer lease and atomic per-account/per-venue capacity reservations.
-- Postgres is authoritative; Redis remains a notification/cache layer.

create table if not exists public.execution_coordinator_leases (
    scope text primary key,
    owner_id uuid not null,
    fencing_token bigint not null check (fencing_token > 0),
    lease_expires_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table public.execution_coordinator_leases enable row level security;
revoke all on table public.execution_coordinator_leases from anon, authenticated;
grant select, insert, update, delete on table public.execution_coordinator_leases to service_role;
drop trigger if exists trg_execution_coordinator_leases_updated_at on public.execution_coordinator_leases;
create trigger trg_execution_coordinator_leases_updated_at
    before update on public.execution_coordinator_leases
    for each row execute function public.set_updated_at();

create or replace function public.acquire_execution_coordinator_lease(
    p_scope text,
    p_owner_id uuid,
    p_lease_seconds integer default 60
)
returns table(acquired boolean, fencing_token bigint, lease_expires_at timestamptz)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v_token bigint;
    v_expires timestamptz;
begin
    if nullif(btrim(p_scope), '') is null or p_owner_id is null then
        raise exception 'scope and owner_id are required';
    end if;
    if p_lease_seconds < 15 or p_lease_seconds > 300 then
        raise exception 'lease_seconds must be between 15 and 300';
    end if;

    insert into public.execution_coordinator_leases as current_lease
        (scope, owner_id, fencing_token, lease_expires_at)
    values
        (p_scope, p_owner_id, 1, clock_timestamp() + make_interval(secs => p_lease_seconds))
    on conflict (scope) do update
       set owner_id = excluded.owner_id,
           fencing_token = case
               when current_lease.lease_expires_at <= clock_timestamp()
                   then current_lease.fencing_token + 1
               else current_lease.fencing_token
           end,
           lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
     where current_lease.lease_expires_at <= clock_timestamp()
        or current_lease.owner_id = excluded.owner_id
    returning current_lease.fencing_token, current_lease.lease_expires_at
         into v_token, v_expires;

    if found then
        return query select true, v_token, v_expires;
        return;
    end if;

    select l.fencing_token, l.lease_expires_at
      into v_token, v_expires
      from public.execution_coordinator_leases l
     where l.scope = p_scope;
    return query select false, coalesce(v_token, 0), v_expires;
end;
$$;

create or replace function public.renew_execution_coordinator_lease(
    p_scope text,
    p_owner_id uuid,
    p_fencing_token bigint,
    p_lease_seconds integer default 60
)
returns boolean
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    if p_lease_seconds < 15 or p_lease_seconds > 300 then
        raise exception 'lease_seconds must be between 15 and 300';
    end if;
    update public.execution_coordinator_leases
       set lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds)
     where scope = p_scope
       and owner_id = p_owner_id
       and fencing_token = p_fencing_token
       and lease_expires_at > clock_timestamp();
    return found;
end;
$$;

create or replace function public.assert_execution_coordinator_lease(
    p_scope text,
    p_owner_id uuid,
    p_fencing_token bigint
)
returns boolean
language sql
security definer
set search_path = public, pg_temp
as $$
    select exists (
        select 1
          from public.execution_coordinator_leases
         where scope = p_scope
           and owner_id = p_owner_id
           and fencing_token = p_fencing_token
           and lease_expires_at > clock_timestamp()
    );
$$;

create or replace function public.release_execution_coordinator_lease(
    p_scope text,
    p_owner_id uuid,
    p_fencing_token bigint
)
returns boolean
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    update public.execution_coordinator_leases
       set lease_expires_at = clock_timestamp()
     where scope = p_scope
       and owner_id = p_owner_id
       and fencing_token = p_fencing_token
       and lease_expires_at > clock_timestamp();
    return found;
end;
$$;

create table if not exists public.execution_capacity_config (
    account_scope text not null,
    exchange_name text not null,
    max_open_spot_orders integer not null default 2
        check (max_open_spot_orders between 1 and 100),
    max_capital_usdt numeric(38,18)
        check (max_capital_usdt is null or max_capital_usdt > 0),
    updated_at timestamptz not null default now(),
    primary key (account_scope, exchange_name)
);

create table if not exists public.execution_capacity_reservations (
    reservation_id uuid primary key default gen_random_uuid(),
    account_scope text not null,
    exchange_name text not null,
    pair_key text not null,
    client_order_id text not null,
    reserved_amount_usdt numeric(38,18) not null check (reserved_amount_usdt > 0),
    status text not null default 'ACTIVE' check (status in ('ACTIVE','RELEASED')),
    created_at timestamptz not null default now(),
    released_at timestamptz
);

create unique index if not exists uq_execution_capacity_active_pair
    on public.execution_capacity_reservations (account_scope, lower(exchange_name), pair_key)
    where status = 'ACTIVE';
create unique index if not exists uq_execution_capacity_client_order
    on public.execution_capacity_reservations (account_scope, lower(exchange_name), client_order_id);

alter table public.execution_capacity_config enable row level security;
alter table public.execution_capacity_reservations enable row level security;
revoke all on table public.execution_capacity_config from anon, authenticated;
revoke all on table public.execution_capacity_reservations from anon, authenticated;
grant select, insert, update, delete on table public.execution_capacity_config to service_role;
grant select, insert, update, delete on table public.execution_capacity_reservations to service_role;
drop trigger if exists trg_execution_capacity_config_updated_at on public.execution_capacity_config;
create trigger trg_execution_capacity_config_updated_at
    before update on public.execution_capacity_config
    for each row execute function public.set_updated_at();

create or replace function public.set_execution_capacity_config(
    p_account_scope text,
    p_exchange_name text,
    p_max_open_spot_orders integer,
    p_max_capital_usdt numeric default null
)
returns table(account_scope text, exchange_name text, max_open_spot_orders integer, max_capital_usdt numeric)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    if nullif(btrim(p_account_scope), '') is null
       or nullif(btrim(p_exchange_name), '') is null then
        raise exception 'account_scope and exchange_name are required';
    end if;
    if p_max_open_spot_orders < 1 or p_max_open_spot_orders > 100 then
        raise exception 'max_open_spot_orders must be between 1 and 100';
    end if;
    if p_max_capital_usdt is not null and p_max_capital_usdt <= 0 then
        raise exception 'max_capital_usdt must be positive when provided';
    end if;
    insert into public.execution_capacity_config as current_config
        (account_scope, exchange_name, max_open_spot_orders, max_capital_usdt)
    values
        (p_account_scope, lower(p_exchange_name), p_max_open_spot_orders, p_max_capital_usdt)
    on conflict (account_scope, exchange_name) do update
       set max_open_spot_orders = excluded.max_open_spot_orders,
           max_capital_usdt = excluded.max_capital_usdt;
    return query
        select c.account_scope, c.exchange_name, c.max_open_spot_orders, c.max_capital_usdt
          from public.execution_capacity_config c
         where c.account_scope = p_account_scope
           and c.exchange_name = lower(p_exchange_name);
end;
$$;

create or replace function public.reserve_execution_capacity(
    p_account_scope text,
    p_exchange_name text,
    p_pair_key text,
    p_client_order_id text,
    p_reserved_amount_usdt numeric,
    p_max_capital_usdt numeric default null
)
returns table(accepted boolean, reservation_id uuid, reason text,
              active_spot_orders integer, reserved_capital_usdt numeric)
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v_max_orders integer;
    v_capital_limit numeric;
    v_active_count integer;
    v_reserved numeric;
    v_existing_id uuid;
begin
    if nullif(btrim(p_account_scope), '') is null
       or nullif(btrim(p_exchange_name), '') is null
       or nullif(btrim(p_pair_key), '') is null
       or nullif(btrim(p_client_order_id), '') is null then
        raise exception 'account, exchange, pair and client order ids are required';
    end if;
    if p_reserved_amount_usdt is null or p_reserved_amount_usdt <= 0 then
        raise exception 'reserved capital must be positive';
    end if;

    perform pg_advisory_xact_lock(hashtextextended(
        p_account_scope || ':' || lower(p_exchange_name), 0
    ));

    insert into public.execution_capacity_config(account_scope, exchange_name)
    values (p_account_scope, lower(p_exchange_name))
    on conflict (account_scope, exchange_name) do nothing;

    select c.max_open_spot_orders, coalesce(p_max_capital_usdt, c.max_capital_usdt)
      into v_max_orders, v_capital_limit
      from public.execution_capacity_config c
     where c.account_scope = p_account_scope
       and c.exchange_name = lower(p_exchange_name)
     for update;

    select r.reservation_id into v_existing_id
      from public.execution_capacity_reservations r
     where r.account_scope = p_account_scope
       and lower(r.exchange_name) = lower(p_exchange_name)
       and r.pair_key = p_pair_key
       and r.status = 'ACTIVE';
    if found then
        select count(*), coalesce(sum(r.reserved_amount_usdt), 0)
          into v_active_count, v_reserved
          from public.execution_capacity_reservations r
         where r.account_scope = p_account_scope
           and lower(r.exchange_name) = lower(p_exchange_name)
           and r.status = 'ACTIVE';
        return query select true, v_existing_id, 'already_reserved', v_active_count, v_reserved;
        return;
    end if;

    select count(*), coalesce(sum(r.reserved_amount_usdt), 0)
      into v_active_count, v_reserved
      from public.execution_capacity_reservations r
     where r.account_scope = p_account_scope
       and lower(r.exchange_name) = lower(p_exchange_name)
       and r.status = 'ACTIVE';

    if v_active_count >= v_max_orders then
        return query select false, null::uuid, 'spot_order_limit_reached', v_active_count, v_reserved;
        return;
    end if;
    if v_capital_limit is not null and v_reserved + p_reserved_amount_usdt > v_capital_limit then
        return query select false, null::uuid, 'capital_limit_reached', v_active_count, v_reserved;
        return;
    end if;

    insert into public.execution_capacity_reservations(
        account_scope, exchange_name, pair_key, client_order_id, reserved_amount_usdt
    ) values (
        p_account_scope, lower(p_exchange_name), p_pair_key, p_client_order_id, p_reserved_amount_usdt
    ) returning execution_capacity_reservations.reservation_id into v_existing_id;

    return query select true, v_existing_id, 'reserved', v_active_count + 1,
                         v_reserved + p_reserved_amount_usdt;
end;
$$;

create or replace function public.release_execution_capacity(
    p_reservation_id uuid
)
returns boolean
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    update public.execution_capacity_reservations
       set status = 'RELEASED', released_at = clock_timestamp()
     where reservation_id = p_reservation_id
       and status = 'ACTIVE';
    return found;
end;
$$;

revoke all on function public.acquire_execution_coordinator_lease(text, uuid, integer) from public, anon, authenticated;
revoke all on function public.renew_execution_coordinator_lease(text, uuid, bigint, integer) from public, anon, authenticated;
revoke all on function public.assert_execution_coordinator_lease(text, uuid, bigint) from public, anon, authenticated;
revoke all on function public.release_execution_coordinator_lease(text, uuid, bigint) from public, anon, authenticated;
revoke all on function public.set_execution_capacity_config(text, text, integer, numeric) from public, anon, authenticated;
revoke all on function public.reserve_execution_capacity(text, text, text, text, numeric, numeric) from public, anon, authenticated;
revoke all on function public.release_execution_capacity(uuid) from public, anon, authenticated;

grant execute on function public.acquire_execution_coordinator_lease(text, uuid, integer) to service_role;
grant execute on function public.renew_execution_coordinator_lease(text, uuid, bigint, integer) to service_role;
grant execute on function public.assert_execution_coordinator_lease(text, uuid, bigint) to service_role;
grant execute on function public.release_execution_coordinator_lease(text, uuid, bigint) to service_role;
grant execute on function public.set_execution_capacity_config(text, text, integer, numeric) to service_role;
grant execute on function public.reserve_execution_capacity(text, text, text, text, numeric, numeric) to service_role;
grant execute on function public.release_execution_capacity(uuid) to service_role;

comment on table public.execution_coordinator_leases is
    'Durable global execution-coordinator lease. fencing_token increments on each expired-lease takeover and is checked before every order mutation.';
comment on table public.execution_capacity_reservations is
    'Atomic per-account/per-spot-venue reservation for an open/pending spot pair and its committed capital. Only verified terminal lifecycle may release it.';
