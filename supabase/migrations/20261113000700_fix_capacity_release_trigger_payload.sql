-- The universal registry stores recovery metadata in raw_payload JSONB,
-- not in a standalone recovery_context column. Replace the earlier trigger
-- implementation so unfilled terminal Spot entries release their exact slot.
create or replace function public.release_unfilled_spot_entry_capacity()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
    v_payload jsonb;
    v_account_scope text;
begin
    if lower(coalesce(new.market_type, '')) = 'spot'
       and upper(coalesce(new.order_role, '')) = 'ENTRY'
       and lower(coalesce(new.status, '')) in ('rejected', 'canceled', 'cancelled', 'expired')
       and coalesce(new.filled_qty, 0) <= 0
       and nullif(new.client_order_id, '') is not null
       and nullif(new.pair_key, '') is not null then
        v_payload := coalesce(new.raw_payload, '{}'::jsonb);
        if jsonb_typeof(v_payload) = 'string' then
            begin
                v_payload := (v_payload #>> '{}')::jsonb;
            exception when others then
                v_payload := '{}'::jsonb;
            end;
        end if;
        v_account_scope := v_payload #>> '{recovery_context,capacity_account_scope}';
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
