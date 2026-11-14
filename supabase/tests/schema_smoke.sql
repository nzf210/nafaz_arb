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
        'executor_status_snapshots','scanner_opportunities','order_registry','reconciliation_audit'
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

drop table pg_temp._smoke_constants;


-- Nafazbot V2 durable config and operator control schema.
begin;

do $$
declare
    tbl text;
begin
    foreach tbl in array array[
        'config_versions','active_config_version','config_audit_log',
        'operator_control_state','operator_actions','outbox_events','config_consumers'
    ] loop
        if not exists (
            select 1 from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'public' and c.relname = tbl and c.relrowsecurity
        ) then
            raise exception 'RLS is not enabled on public.%', tbl;
        end if;
        if has_table_privilege('anon', format('public.%I', tbl), 'select')
           or has_table_privilege('authenticated', format('public.%I', tbl), 'select') then
            raise exception 'client role unexpectedly has SELECT on public.%', tbl;
        end if;
        if not has_table_privilege('service_role', format('public.%I', tbl), 'select')
           or not has_table_privilege('service_role', format('public.%I', tbl), 'insert') then
            raise exception 'service_role lacks expected access on public.%', tbl;
        end if;
    end loop;
end $$;

insert into public.config_versions (
    version, schema_version, snapshot, content_sha256, status,
    created_by, change_reason, change_diff
) values (
    9223372036854770000, 2,
    '{"schema_version":2,"mode":"LIVE","secrets":{"okx_api_key":"secret://nafaz/okx/key"}}'::jsonb,
    repeat('a', 64), 'draft', 'schema-smoke', 'test immutable config snapshot',
    '{"mode":{"from":"MOCK","to":"LIVE"}}'::jsonb
);
update public.config_versions set status = 'validated',
    validated_by = 'schema-smoke', validated_at = now()
where version = 9223372036854770000;

do $$
begin
    begin
        update public.config_versions
        set snapshot = '{"schema_version":2,"mode":"MOCK"}'::jsonb
        where version = 9223372036854770000;
        raise exception 'immutable config snapshot update unexpectedly succeeded';
    exception when raise_exception then
        if sqlerrm = 'immutable config snapshot update unexpectedly succeeded' then
            raise;
        end if;
    end;
    if (select status from public.config_versions where version = 9223372036854770000) <> 'validated' then
        raise exception 'config validation metadata update did not persist';
    end if;
end $$;

insert into public.active_config_version(singleton, version, updated_by, reason)
values (true, 9223372036854770000, 'schema-smoke', 'activate test snapshot');

do $$
begin
    if not exists (
        select 1 from public.operator_control_state
        where singleton = true and state = 'STOPPED' and trading_enabled = false
    ) then
        raise exception 'operator control state must initialize STOPPED and disabled';
    end if;
    if (select count(*) from public.active_config_version) <> 1 then
        raise exception 'active config pointer must be a singleton';
    end if;
end $$;

insert into public.operator_actions (
    command_id, telegram_update_id, command, actor_user_id, actor_chat_id,
    reason, status, requested_config_version
) values (
    'schema-smoke-command-1', 9223372036854770000, 'trade_stop',
    10001, -10001, 'schema smoke test STOP', 'ACCEPTED', 9223372036854770000
)
on conflict (command_id) do nothing;

insert into public.operator_actions (
    command_id, telegram_update_id, command, actor_user_id, actor_chat_id,
    reason, status, requested_config_version
) values (
    'schema-smoke-command-1', 9223372036854770000, 'trade_stop',
    10001, -10001, 'schema smoke test duplicate', 'ACCEPTED', 9223372036854770000
)
on conflict (command_id) do nothing;

do $$
begin
    if (select count(*) from public.operator_actions
        where command_id = 'schema-smoke-command-1') <> 1 then
        raise exception 'operator command idempotency key admitted a duplicate';
    end if;
end $$;

insert into public.config_consumers (
    instance_id, process_id, status, acknowledged_version, acknowledged_sha256,
    last_ack_at, last_seen_at
) values (
    'schema-smoke-executor-1', 'schema-smoke-pid-1', 'ACTIVE',
    9223372036854770000, repeat('a', 64), now(), now()
);
do $consumer$
begin
    if not exists (
        select 1 from public.config_consumers
        where instance_id = 'schema-smoke-executor-1'
          and status = 'ACTIVE'
          and acknowledged_version = 9223372036854770000
          and acknowledged_sha256 = repeat('a', 64)
    ) then
        raise exception 'executor config acknowledgement was not persisted';
    end if;
    update public.config_consumers
       set status = 'STARTING', process_id = 'schema-smoke-pid-2'
     where instance_id = 'schema-smoke-executor-1';
    if (select status from public.config_consumers
        where instance_id = 'schema-smoke-executor-1') <> 'STARTING' then
        raise exception 'new executor process must return to STARTING before re-acknowledging';
    end if;
end $consumer$;

insert into public.config_audit_log (
    version, action, actor, reason, diff, request_id
) values (
    9223372036854770000, 'validated', 'schema-smoke',
    'validate test snapshot', '{"validated":true}'::jsonb, 'schema-smoke-config-1'
);
insert into public.outbox_events (
    event_key, aggregate_type, aggregate_version, event_type, payload
) values (
    'schema-smoke-config-event-1', 'config', 9223372036854770000,
    'config.activated', '{"version":9223372036854770000}'::jsonb
);

rollback;


-- V2 multi-venue schema, NUMERIC precision, route constraints and journal/outbox atomicity.
begin;

insert into public.v2_exchanges(exchange_code, display_name)
values ('indodax','Indodax'), ('okx','OKX')
on conflict (exchange_code) do nothing;

insert into public.v2_venues(
    venue_id, exchange_code, market_type, adapter, enabled,
    market_data_enabled, execution_enabled
) values
    ('smoke_spot','indodax','spot','indodax',true,true,true),
    ('smoke_perp','okx','linear_perpetual','okx',true,true,true);

insert into public.v2_accounts(account_id, venue_id, label, enabled)
values
    ('smoke_spot_account','smoke_spot','smoke spot',true),
    ('smoke_perp_account','smoke_perp','smoke perp',true);

insert into public.v2_account_secret_refs(account_id, credential_name, secret_ref)
values
    ('smoke_spot_account','api_key','SMOKE_INDODAX_API_KEY'),
    ('smoke_spot_account','api_secret','SMOKE_INDODAX_API_SECRET'),
    ('smoke_perp_account','api_key','SMOKE_OKX_API_KEY'),
    ('smoke_perp_account','api_secret','SMOKE_OKX_API_SECRET');

insert into public.v2_venue_capabilities(
    venue_id, capability, status, evidence_ref, verified_by, verified_at
) values
    ('smoke_spot','balance_read','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_spot','open_orders_read','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_spot','limit_order_create','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_spot','order_cancel','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_spot','order_lookup_by_client_id','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_spot','multi_venue','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_spot','multi_account','verified','test://schema-smoke/spot','ci',now()),
    ('smoke_perp','balance_read','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','positions_read','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','market_order_create','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','reduce_only_order','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','order_cancel','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','order_lookup_by_client_id','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','multi_venue','verified','test://schema-smoke/perp','ci',now()),
    ('smoke_perp','multi_account','verified','test://schema-smoke/perp','ci',now());

insert into public.v2_markets(
    market_id, venue_id, native_symbol, canonical_base_asset, quote_asset,
    settle_asset, amount_step, minimum_amount, price_tick, minimum_notional,
    observed_at
) values
    ('smoke_btc_idr','smoke_spot','btc_idr','BTC','IDR',null,
     0.00000001,0.00000001,1,1000,now()),
    ('smoke_btc_usdt_swap','smoke_perp','BTC/USDT:USDT','BTC','USDT','USDT',
     0.001,0.001,0.1,5,now());

insert into public.v2_risk_profiles(
    risk_profile_id, max_daily_loss_usdt, max_exposure_per_account_usdt,
    max_exposure_per_route_usdt, max_entry_slippage_pct,
    max_consecutive_order_failures, max_open_spot_orders,
    max_open_futures_orders, require_fresh_balance,
    require_position_reconciliation
) values ('smoke_default',5,100,50,0.2,5,2,2,true,true);

insert into public.v2_trading_routes(
    route_id, spot_venue_id, spot_account_id, futures_venue_id,
    futures_account_id, risk_profile_id, enabled, spot_quote_assets,
    futures_settle_assets, symbol_allowlist, symbol_denylist
) values (
    'smoke_route','smoke_spot','smoke_spot_account','smoke_perp',
    'smoke_perp_account','smoke_default',true,
    array['IDR','USDT'],array['USDT'],array['BTC'],array[]::text[]
);

insert into public.v2_execution_intents(
    intent_id, idempotency_key, route_id, venue_id, account_id, market_id,
    canonical_symbol, order_role, side, order_type, client_order_id,
    requested_qty, limit_price, is_mock
) values (
    '33333333-3333-3333-3333-333333333333',
    'schema-smoke-intent-1','smoke_route','smoke_spot',
    'smoke_spot_account','smoke_btc_idr','BTC','ENTRY','buy','limit',
    'schema-smoke-client-1',0.001,500000000,true
);

insert into public.v2_execution_journal(
    event_key, intent_id, event_type, to_state, quantity, price,
    quote_amount, payload
) values (
    'schema-smoke-execution-1',
    '33333333-3333-3333-3333-333333333333',
    'intent.received','RECEIVED',0.001,500000000,500000,
    '{"source":"schema_smoke"}'::jsonb
)
on conflict (event_key) do nothing;

insert into public.v2_execution_journal(
    event_key, intent_id, event_type, to_state, quantity, price,
    quote_amount, payload
) values (
    'schema-smoke-execution-1',
    '33333333-3333-3333-3333-333333333333',
    'intent.received','RECEIVED',0.001,500000000,500000,
    '{"source":"duplicate_replay"}'::jsonb
)
on conflict (event_key) do nothing;

insert into public.outbox_events(
    event_key, aggregate_type, aggregate_version, event_type, payload
) values (
    'execution:schema-smoke-execution-1','execution',0,
    'execution.intent.received',
    '{"event_key":"schema-smoke-execution-1","journal_id":1}'::jsonb
)
on conflict (event_key) do nothing;

insert into public.v2_balance_snapshots(
    venue_id, account_id, asset, total, free, used, observed_at, is_mock
) values ('smoke_spot','smoke_spot_account','IDR',1000000,900000,100000,now(),true);

insert into public.v2_position_snapshots(
    venue_id, account_id, market_id, position_side, contracts,
    base_quantity, entry_price, mark_price, unrealized_pnl,
    margin_used, margin_asset, observed_at, is_mock
) values (
    'smoke_perp','smoke_perp_account','smoke_btc_usdt_swap','short',
    0.001,0.001,50000,49900,0.1,50,'USDT',now(),true
);

do $v2_core$
declare
    journal_count integer;
    requested_qty_type text;
    journal_price_type text;
begin
    select count(*) into journal_count
      from public.v2_execution_journal
     where event_key = 'schema-smoke-execution-1';
    if journal_count <> 1 then
        raise exception 'V2 execution journal event key is not idempotent';
    end if;

    select data_type into requested_qty_type
      from information_schema.columns
     where table_schema = 'public'
       and table_name = 'v2_execution_intents'
       and column_name = 'requested_qty';
    select data_type into journal_price_type
      from information_schema.columns
     where table_schema = 'public'
       and table_name = 'v2_execution_journal'
       and column_name = 'price';
    if requested_qty_type <> 'numeric' or journal_price_type <> 'numeric' then
        raise exception 'V2 financial columns must use NUMERIC';
    end if;

    begin
        update public.v2_execution_journal
           set event_type = 'tampered'
         where event_key = 'schema-smoke-execution-1';
        raise exception 'journal mutation unexpectedly succeeded';
    exception when others then
        if sqlerrm = 'journal mutation unexpectedly succeeded' then
            raise;
        end if;
    end;

    begin
        insert into public.v2_trading_routes(
            route_id, spot_venue_id, spot_account_id, futures_venue_id,
            futures_account_id, risk_profile_id, enabled, spot_quote_assets,
            futures_settle_assets, symbol_allowlist
        ) values (
            'smoke_invalid_route','smoke_perp','smoke_perp_account',
            'smoke_spot','smoke_spot_account','smoke_default',false,
            array['USDT'],array['USDT'],array['BTC']
        );
        raise exception 'route market-type mismatch unexpectedly succeeded';
    exception when others then
        if sqlerrm = 'route market-type mismatch unexpectedly succeeded' then
            raise;
        end if;
    end;
end;
$v2_core$;



do $v2_live_gate$
begin
    begin
        insert into public.v2_execution_intents(
            intent_id, idempotency_key, route_id, venue_id, account_id, market_id,
            canonical_symbol, order_role, side, order_type, client_order_id,
            requested_qty, limit_price, is_mock
        ) values (
            '44444444-4444-4444-4444-444444444444',
            'schema-smoke-live-intent-1','smoke_route','smoke_spot',
            'smoke_spot_account','smoke_btc_idr','BTC','ENTRY','buy','limit',
            'schema-smoke-live-client-1',0.001,500000000,false
        );
        raise exception 'LIVE V2 intent unexpectedly passed STOP gate';
    exception when others then
        if sqlerrm = 'LIVE V2 intent unexpectedly passed STOP gate' then
            raise;
        end if;
    end;
end;
$v2_live_gate$;

rollback;
