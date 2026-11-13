-- Runs after the initial migration against a clean PostgreSQL instance.
-- Shared test fixtures keep repeated schema-smoke literals in one place.
create temporary table _smoke_constants as
select
    'public'::text as public_schema,
    'public.%I'::text as public_table_format,
    'schema-smoke-test'::text as smoke_config_name,
    'indodax'::text as local_exchange,
    'schema-smoke-idempotency'::text as idempotency_scan_id,
    'schema-smoke-idempotency:indodax:btc_idr->okx:btc/usdt:usdt'::text as idempotency_key,
    'indodax:btc_idr->okx:btc/usdt:usdt'::text as idempotency_pair_key,
    '11111111-1111-1111-1111-111111111111'::uuid as order_local_id,
    'smoke-entry-1'::text as order_client_id,
    'btc_idr'::text as local_symbol,
    'spot'::text as spot_market,
    'ENTRY'::text as entry_role,
    'buy'::text as buy_side,
    'limit'::text as limit_order_type;

do $$
declare
    tbl text;
    table_label text;
    mutable_tables text[] := array[
        'exchange_accounts','strategy_configs','risk_limits','market_pairs',
        'positions','orders','order_fills','position_legs','funding_payments',
        'closed_trades','balance_snapshots','risk_events','executor_events',
        'executor_status_snapshots','scanner_opportunities','order_registry','reconciliation_audit',
        'execution_coordinator_leases','execution_capacity_config','execution_capacity_reservations'
    ];
begin
    foreach tbl in array mutable_tables loop
        table_label := format((select public_table_format from pg_temp._smoke_constants), tbl);
        if not exists (
            select 1
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = (select public_schema from pg_temp._smoke_constants) and c.relname = tbl and c.relrowsecurity
        ) then
            raise exception 'RLS is not enabled on %', table_label;
        end if;
        if has_table_privilege('anon', format((select public_table_format from pg_temp._smoke_constants), tbl), 'select')
           or has_table_privilege('authenticated', format((select public_table_format from pg_temp._smoke_constants), tbl), 'select') then
            raise exception 'Unexpected client SELECT privilege on %', table_label;
        end if;
        if not has_table_privilege('service_role', format((select public_table_format from pg_temp._smoke_constants), tbl), 'select')
           or not has_table_privilege('service_role', format((select public_table_format from pg_temp._smoke_constants), tbl), 'insert') then
            raise exception 'service_role missing required access on %', table_label;
        end if;
    end loop;
end $$;

insert into public.strategy_configs (name, config)
values ((select smoke_config_name from pg_temp._smoke_constants), '{"source":"ci"}'::jsonb);

do $$
declare
    before_update timestamptz;
    after_update timestamptz;
begin
    select updated_at into before_update
    from public.strategy_configs where name = (select smoke_config_name from pg_temp._smoke_constants);
    perform pg_sleep(0.02);
    update public.strategy_configs
    set config = '{"source":"ci-updated"}'::jsonb
    where name = (select smoke_config_name from pg_temp._smoke_constants);
    select updated_at into after_update
    from public.strategy_configs where name = (select smoke_config_name from pg_temp._smoke_constants);
    if after_update <= before_update then
        raise exception 'updated_at trigger did not advance timestamp';
    end if;
end $$;

delete from public.strategy_configs where name = (select smoke_config_name from pg_temp._smoke_constants);


do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = (select public_schema from pg_temp._smoke_constants) and table_name = 'closed_trades'
          and column_name = 'spot_pnl_asset'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = (select public_schema from pg_temp._smoke_constants) and table_name = 'closed_trades'
          and column_name = 'entry_usdt_idr_rate'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = (select public_schema from pg_temp._smoke_constants) and table_name = 'positions'
          and column_name = 'futures_quote_asset'
    ) then
        raise exception 'trade currency / FX audit columns are missing';
    end if;
end $$;

-- The workflow inserts this row after the immutable baseline and before
-- the idempotency migration. It must survive with a NULL key.
do $$
begin
    if (
        select count(*) from public.scanner_opportunities
        where scan_id = 'legacy-before-idempotency' and idempotency_key is null
    ) <> 1 then
        raise exception 'additive idempotency migration did not preserve legacy scanner history';
    end if;
end $$;

-- A nullable unique key must continue accepting multiple historical rows.
insert into public.scanner_opportunities (
    scan_id, pair_key, local_exchange, global_exchange,
    local_symbol, global_symbol, quote_asset, eligibility_status,
    observed_at, payload
) values (
    'legacy-null-key-second', 'indodax:legacy_idr',
    (select local_exchange from pg_temp._smoke_constants), 'okx', 'legacy_idr', 'LEGACY/USDT:USDT',
    'IDR', 'EXCLUDED', now(), '{}'::jsonb
);

do $$
begin
    if (
        select count(*) from public.scanner_opportunities
        where pair_key = 'indodax:legacy_idr' and idempotency_key is null
    ) <> 2 then
        raise exception 'nullable unique key rejected or lost legacy rows';
    end if;
end $$;

delete from public.scanner_opportunities
where scan_id in ('legacy-before-idempotency', 'legacy-null-key-second');

-- Runtime scanner repository contract: nullable unique keys preserve historical rows
-- and deterministic keys make a retried scan an upsert rather than a duplicate.
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = (select public_schema from pg_temp._smoke_constants) and table_name = 'scanner_opportunities'
          and column_name = 'idempotency_key'
    ) then
        raise exception 'scanner opportunity idempotency_key column is missing';
    end if;
    if not exists (
        select 1 from pg_constraint
        where conrelid = 'public.scanner_opportunities'::regclass
          and conname = 'uq_scanner_opportunities_idempotency_key'
          and contype = 'u'
    ) then
        raise exception 'scanner opportunity idempotency constraint is missing';
    end if;
end $$;

insert into public.scanner_opportunities (
    scan_id, pair_key, idempotency_key, local_exchange, global_exchange,
    local_symbol, global_symbol, quote_asset, eligibility_status, observed_at, payload
) values (
    (select idempotency_scan_id from pg_temp._smoke_constants), (select idempotency_pair_key from pg_temp._smoke_constants),
    (select idempotency_key from pg_temp._smoke_constants), (select local_exchange from pg_temp._smoke_constants), 'okx',
    (select local_symbol from pg_temp._smoke_constants), 'BTC/USDT:USDT', 'IDR', 'ELIGIBLE', now(), '{"attempt":1}'::jsonb
)
on conflict (idempotency_key) do update
set payload = excluded.payload;

insert into public.scanner_opportunities (
    scan_id, pair_key, idempotency_key, local_exchange, global_exchange,
    local_symbol, global_symbol, quote_asset, eligibility_status, observed_at, payload
) values (
    (select idempotency_scan_id from pg_temp._smoke_constants), (select idempotency_pair_key from pg_temp._smoke_constants),
    (select idempotency_key from pg_temp._smoke_constants), (select local_exchange from pg_temp._smoke_constants), 'okx',
    (select local_symbol from pg_temp._smoke_constants), 'BTC/USDT:USDT', 'IDR', 'ELIGIBLE', now(), '{"attempt":2}'::jsonb
)
on conflict (idempotency_key) do update
set payload = excluded.payload;

do $$
begin
    if (
        select count(*) from public.scanner_opportunities
        where idempotency_key = (select idempotency_key from pg_temp._smoke_constants)
    ) <> 1 then
        raise exception 'scanner opportunity retry created duplicate rows';
    end if;
    if (
        select payload ->> 'attempt' from public.scanner_opportunities
        where idempotency_key = (select idempotency_key from pg_temp._smoke_constants)
    ) <> '2' then
        raise exception 'scanner opportunity upsert did not update the existing row';
    end if;
end $$;

delete from public.scanner_opportunities
where idempotency_key = (select idempotency_key from pg_temp._smoke_constants);

-- Universal order registry: idempotent upsert on the deterministic local id and
-- partial unique indexes that keep orphan rows (no ids) insertable.
insert into public.order_registry (
    local_order_id, client_order_id, exchange_order_id, pair_key, position_key,
    exchange_name, market_type, leg, order_role, symbol, side, order_type,
    status, requested_qty, filled_qty, remaining_qty, recovery_status, is_mock
) values (
    (select order_local_id from pg_temp._smoke_constants), (select order_client_id from pg_temp._smoke_constants), '9001',
    'indodax:btc_idr', 'position:indodax:btc_idr', (select local_exchange from pg_temp._smoke_constants), (select spot_market from pg_temp._smoke_constants),
    'spot_entry', (select entry_role from pg_temp._smoke_constants), (select local_symbol from pg_temp._smoke_constants), (select buy_side from pg_temp._smoke_constants), (select limit_order_type from pg_temp._smoke_constants), 'open', 1, 0, 1, 'ADOPTED', true
)
on conflict (local_order_id) do update
set status = excluded.status, filled_qty = excluded.filled_qty,
    remaining_qty = excluded.remaining_qty;

insert into public.order_registry (
    local_order_id, client_order_id, exchange_order_id, pair_key, position_key,
    exchange_name, market_type, leg, order_role, symbol, side, order_type,
    status, requested_qty, filled_qty, remaining_qty, recovery_status, is_mock
) values (
    (select order_local_id from pg_temp._smoke_constants), (select order_client_id from pg_temp._smoke_constants), '9001',
    'indodax:btc_idr', 'position:indodax:btc_idr', (select local_exchange from pg_temp._smoke_constants), (select spot_market from pg_temp._smoke_constants),
    'spot_entry', (select entry_role from pg_temp._smoke_constants), (select local_symbol from pg_temp._smoke_constants), (select buy_side from pg_temp._smoke_constants), (select limit_order_type from pg_temp._smoke_constants), 'closed', 1, 1, 0, 'ADOPTED', true
)
on conflict (local_order_id) do update
set status = excluded.status, filled_qty = excluded.filled_qty,
    remaining_qty = excluded.remaining_qty;

do $$
begin
    if (select count(*) from public.order_registry where client_order_id = (select order_client_id from pg_temp._smoke_constants)) <> 1 then
        raise exception 'order registry upsert created a duplicate row';
    end if;
    if (select status from public.order_registry where client_order_id = (select order_client_id from pg_temp._smoke_constants)) <> 'closed' then
        raise exception 'order registry upsert did not advance status';
    end if;
    -- Two orphans with no ids must both be insertable (partial unique indexes).
    insert into public.order_registry (
        local_order_id, exchange_name, market_type, order_role, symbol, side,
        order_type, status, recovery_status, is_mock
    ) values
        ('22222222-2222-2222-2222-222222222221', (select local_exchange from pg_temp._smoke_constants), (select spot_market from pg_temp._smoke_constants), (select entry_role from pg_temp._smoke_constants),
         'eth_idr', (select buy_side from pg_temp._smoke_constants), (select limit_order_type from pg_temp._smoke_constants), 'open', 'ORPHAN_ORDER', false),
        ('22222222-2222-2222-2222-222222222222', (select local_exchange from pg_temp._smoke_constants), (select spot_market from pg_temp._smoke_constants), (select entry_role from pg_temp._smoke_constants),
         'eth_idr', (select buy_side from pg_temp._smoke_constants), (select limit_order_type from pg_temp._smoke_constants), 'open', 'ORPHAN_ORDER', false);
end $$;

delete from public.order_registry
where client_order_id = (select order_client_id from pg_temp._smoke_constants)
   or local_order_id in (
        '22222222-2222-2222-2222-222222222221',
        '22222222-2222-2222-2222-222222222222'
   );


-- Reconciliation audit is append-only and idempotent under outbox replay.
insert into public.reconciliation_audit (
    event_id, event_type, pair_key, exchange_name, market_type,
    entity_type, entity_id, from_state, to_state, reason, actor,
    is_mock, occurred_at, payload
) values (
    'schema-smoke-reconcile-1', 'ORDER_RECONCILIATION_REQUIRED',
    'indodax:btc_idr', 'indodax', 'spot', 'order',
    'schema-smoke-order-1', 'OPEN', 'RECONCILIATION_REQUIRED',
    'ambiguous exchange acknowledgement', 'executor', false, now(),
    '{"source":"schema_smoke"}'::jsonb
)
on conflict (event_id) do nothing;

insert into public.reconciliation_audit (
    event_id, event_type, pair_key, exchange_name, market_type,
    entity_type, entity_id, from_state, to_state, reason, actor,
    is_mock, occurred_at, payload
) values (
    'schema-smoke-reconcile-1', 'ORDER_RECONCILIATION_REQUIRED',
    'indodax:btc_idr', 'indodax', 'spot', 'order',
    'schema-smoke-order-1', 'OPEN', 'RECONCILIATION_REQUIRED',
    'ambiguous exchange acknowledgement', 'executor', false, now(),
    '{"source":"schema_smoke_retry"}'::jsonb
)
on conflict (event_id) do nothing;

do $audit$
begin
    if (
        select count(*) from public.reconciliation_audit
        where event_id = 'schema-smoke-reconcile-1'
    ) <> 1 then
        raise exception 'reconciliation audit replay created a duplicate';
    end if;
    if has_table_privilege('service_role', 'public.reconciliation_audit', 'update')
       or has_table_privilege('service_role', 'public.reconciliation_audit', 'delete') then
        raise exception 'reconciliation audit is not append-only for service_role';
    end if;
end $audit$;

delete from public.reconciliation_audit
where event_id = 'schema-smoke-reconcile-1';


-- Durable coordinator lease: one active owner, monotonic fencing token on takeover.
do $coordinator$
declare
    owner_one uuid := '33333333-3333-3333-3333-333333333331';
    owner_two uuid := '33333333-3333-3333-3333-333333333332';
    acquired_value boolean;
    first_token bigint;
    second_token bigint;
begin
    select acquired, fencing_token into acquired_value, first_token
      from public.acquire_execution_coordinator_lease('schema-smoke-global', owner_one, 30);
    if not acquired_value or first_token < 1 then raise exception 'first coordinator lease was not acquired'; end if;
    select acquired into acquired_value
      from public.acquire_execution_coordinator_lease('schema-smoke-global', owner_two, 30);
    if acquired_value then raise exception 'second coordinator acquired an unexpired lease'; end if;
    if not public.assert_execution_coordinator_lease('schema-smoke-global', owner_one, first_token) then
        raise exception 'current coordinator fencing token was not accepted';
    end if;
    if not public.renew_execution_coordinator_lease('schema-smoke-global', owner_one, first_token, 30) then
        raise exception 'current coordinator lease could not be renewed';
    end if;
    if not public.release_execution_coordinator_lease('schema-smoke-global', owner_one, first_token) then
        raise exception 'current coordinator lease could not be released';
    end if;
    select acquired, fencing_token into acquired_value, second_token
      from public.acquire_execution_coordinator_lease('schema-smoke-global', owner_two, 30);
    if not acquired_value or second_token <= first_token then
        raise exception 'coordinator takeover did not advance the fencing token';
    end if;
    if public.assert_execution_coordinator_lease('schema-smoke-global', owner_one, first_token) then
        raise exception 'stale coordinator fencing token was accepted';
    end if;
    perform public.release_execution_coordinator_lease('schema-smoke-global', owner_two, second_token);
end
$coordinator$;

delete from public.execution_coordinator_leases where scope = 'schema-smoke-global';

-- Atomic capacity reservation: idempotent per pair, slot cap and capital cap
-- are checked under one account+venue transaction lock.
do $capacity$
declare
    accepted_value boolean;
    first_id uuid;
    returned_id uuid;
    reason_value text;
    active_count integer;
    capital_value numeric;
begin
    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-unconfigured', 'schema-smoke-order-unconfigured', 10, null
      );
    if accepted_value or reason_value <> 'capital_limit_not_configured' then
        raise exception 'live capacity reservation was accepted without a capital limit';
    end if;
    perform public.set_execution_capacity_config('schema-smoke-account', 'indodax', 2, 100);
    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, first_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-1', 'schema-smoke-order-1', 40, null
      );
    if not accepted_value or reason_value <> 'reserved' then raise exception 'first capacity reservation was rejected'; end if;

    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-1', 'schema-smoke-order-1', 40, null
      );
    if not accepted_value or returned_id <> first_id or active_count <> 1 then
        raise exception 'same pair reservation is not idempotent';
    end if;

    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-1', 'schema-smoke-order-1-retry', 40, null
      );
    if accepted_value or reason_value <> 'pair_already_reserved' or active_count <> 1 then
        raise exception 'different client order inherited an active pair reservation';
    end if;

    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-1', 'schema-smoke-order-1', 35, null
      );
    if accepted_value or reason_value <> 'reservation_amount_mismatch' or active_count <> 1 then
        raise exception 'same client order changed its committed reservation amount';
    end if;

    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-2', 'schema-smoke-order-2', 70, null
      );
    if accepted_value or reason_value <> 'capital_limit_reached' then
        raise exception 'capital limit did not reject excess reservation';
    end if;

    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-2', 'schema-smoke-order-2', 10, null
      );
    if not accepted_value or active_count <> 2 then raise exception 'second independent slot was not reserved'; end if;

    select accepted, reservation_id, reason, active_spot_orders, reserved_capital_usdt
      into accepted_value, returned_id, reason_value, active_count, capital_value
      from public.reserve_execution_capacity(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-3', 'schema-smoke-order-3', 10, null
      );
    if accepted_value or reason_value <> 'spot_order_limit_reached' then
        raise exception 'spot slot cap did not reject the third active pair';
    end if;
    perform public.set_execution_capacity_config('schema-smoke-account', 'indodax', 3, null);
    select c.max_capital_usdt into capital_value
      from public.execution_capacity_config c
     where c.account_scope = 'schema-smoke-account' and c.exchange_name = 'indodax';
    if capital_value <> 100 then raise exception 'spot limit update unexpectedly cleared capital cap'; end if;
    if not public.release_execution_capacity_for_pair(
        'schema-smoke-account', 'indodax', 'schema-smoke-pair-2'
    ) then raise exception 'capacity reservation release by pair failed'; end if;
    if not public.release_execution_capacity(first_id) then raise exception 'capacity reservation release failed'; end if;
    delete from public.execution_capacity_reservations where account_scope = 'schema-smoke-account';
    delete from public.execution_capacity_config where account_scope = 'schema-smoke-account';
end
$capacity$;


drop table pg_temp._smoke_constants;
