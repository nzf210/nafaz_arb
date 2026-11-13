-- Release an entry reservation by its logical order identity. This is safer
-- than releasing by pair after a failed/ambiguous reservation RPC response.
create or replace function public.release_execution_capacity_for_order(
    p_account_scope text,
    p_exchange_name text,
    p_client_order_id text
)
returns boolean
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v_released integer;
begin
    if nullif(btrim(p_account_scope), '') is null
       or nullif(btrim(p_exchange_name), '') is null
       or nullif(btrim(p_client_order_id), '') is null then
        return false;
    end if;
    update public.execution_capacity_reservations
       set status = 'RELEASED', released_at = clock_timestamp()
     where account_scope = p_account_scope
       and lower(exchange_name) = lower(p_exchange_name)
       and client_order_id = p_client_order_id
       and status = 'ACTIVE';
    get diagnostics v_released = row_count;
    return v_released > 0;
end;
$$;

revoke all on function public.release_execution_capacity_for_order(text, text, text) from public;
grant execute on function public.release_execution_capacity_for_order(text, text, text) to service_role;

-- If a zero-fill Spot entry is rejected/cancelled/expired, release its slot in
-- the same Postgres transaction that commits the terminal order-registry state.
create or replace function public.release_unfilled_spot_entry_capacity()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v_account_scope text;
begin
    if lower(coalesce(new.market_type, '')) = 'spot'
       and upper(coalesce(new.order_role, '')) = 'ENTRY'
       and lower(coalesce(new.status, '')) in ('rejected', 'canceled', 'cancelled', 'expired')
       and coalesce(new.filled_qty, 0) <= 0
       and nullif(new.client_order_id, '') is not null
       and nullif(new.pair_key, '') is not null then
        v_account_scope := coalesce(new.recovery_context, '{}'::jsonb)->>'capacity_account_scope';
        if nullif(btrim(v_account_scope), '') is not null then
            update public.execution_capacity_reservations
               set status = 'RELEASED', released_at = clock_timestamp()
             where account_scope = v_account_scope
               and lower(exchange_name) = lower(new.exchange_name)
               and pair_key = new.pair_key
               and client_order_id = new.client_order_id
               and status = 'ACTIVE';
        end if;
    end if;
    return new;
end;
$$;

revoke all on function public.release_unfilled_spot_entry_capacity() from public;
drop trigger if exists trg_release_unfilled_spot_entry_capacity on public.order_registry;
create trigger trg_release_unfilled_spot_entry_capacity
    after insert or update of status on public.order_registry
    for each row execute function public.release_unfilled_spot_entry_capacity();
