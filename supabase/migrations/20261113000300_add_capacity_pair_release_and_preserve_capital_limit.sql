-- Capacity configuration updates from Telegram must not clear a capital cap.
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
    on conflict on constraint execution_capacity_config_pkey do update
       set max_open_spot_orders = excluded.max_open_spot_orders,
           max_capital_usdt = coalesce(excluded.max_capital_usdt, current_config.max_capital_usdt);
    return query
        select c.account_scope, c.exchange_name, c.max_open_spot_orders, c.max_capital_usdt
          from public.execution_capacity_config c
         where c.account_scope = p_account_scope
           and c.exchange_name = lower(p_exchange_name);
end;
$$;

create or replace function public.release_execution_capacity_for_pair(
    p_account_scope text,
    p_exchange_name text,
    p_pair_key text
)
returns boolean
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
    update public.execution_capacity_reservations
       set status = 'RELEASED', released_at = clock_timestamp()
     where account_scope = p_account_scope
       and lower(exchange_name) = lower(p_exchange_name)
       and pair_key = p_pair_key
       and status = 'ACTIVE';
    return found;
end;
$$;

revoke all on function public.set_execution_capacity_config(text, text, integer, numeric) from public, anon, authenticated;
revoke all on function public.release_execution_capacity_for_pair(text, text, text) from public, anon, authenticated;
grant execute on function public.set_execution_capacity_config(text, text, integer, numeric) to service_role;
grant execute on function public.release_execution_capacity_for_pair(text, text, text) to service_role;
