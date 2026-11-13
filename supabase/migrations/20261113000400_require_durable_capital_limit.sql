-- Live capacity reservations must have an explicit pair-capital budget.
-- The reserved amount includes Spot notional plus the 1x Futures margin requirement.
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

    if v_capital_limit is null or v_capital_limit <= 0 then
        return query select false, null::uuid, 'capital_limit_not_configured', 0, 0::numeric;
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

