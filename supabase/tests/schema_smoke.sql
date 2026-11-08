-- Runs after the initial migration against a clean PostgreSQL instance.
do $$
declare
    tbl text;
    mutable_tables text[] := array[
        'exchange_accounts','strategy_configs','risk_limits','market_pairs',
        'positions','orders','order_fills','position_legs','funding_payments',
        'closed_trades','balance_snapshots','risk_events','executor_events',
        'executor_status_snapshots','scanner_opportunities'
    ];
begin
    foreach tbl in array mutable_tables loop
        if not exists (
            select 1
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where n.nspname = 'public' and c.relname = tbl and c.relrowsecurity
        ) then
            raise exception 'RLS is not enabled on public.%', tbl;
        end if;
        if has_table_privilege('anon', format('public.%I', tbl), 'select')
           or has_table_privilege('authenticated', format('public.%I', tbl), 'select') then
            raise exception 'Unexpected client SELECT privilege on public.%', tbl;
        end if;
        if not has_table_privilege('service_role', format('public.%I', tbl), 'select')
           or not has_table_privilege('service_role', format('public.%I', tbl), 'insert') then
            raise exception 'service_role missing required access on public.%', tbl;
        end if;
    end loop;
end $$;

insert into public.strategy_configs (name, config)
values ('schema-smoke-test', '{"source":"ci"}'::jsonb);

do $$
declare
    before_update timestamptz;
    after_update timestamptz;
begin
    select updated_at into before_update
    from public.strategy_configs where name = 'schema-smoke-test';
    perform pg_sleep(0.02);
    update public.strategy_configs
    set config = '{"source":"ci-updated"}'::jsonb
    where name = 'schema-smoke-test';
    select updated_at into after_update
    from public.strategy_configs where name = 'schema-smoke-test';
    if after_update <= before_update then
        raise exception 'updated_at trigger did not advance timestamp';
    end if;
end $$;

delete from public.strategy_configs where name = 'schema-smoke-test';


do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'spot_pnl_asset'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'entry_usdt_idr_rate'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'positions'
          and column_name = 'futures_quote_asset'
    ) then
        raise exception 'trade currency / FX audit columns are missing';
    end if;
end $$;

-- Runtime repository contract: PostgREST uses this unique key for retry-safe
-- upserts when a scan response is retried after a transient network failure.
do $$
begin
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
    scan_id, pair_key, idempotency_key, local_exchange, global_exchange, local_symbol,
    global_symbol, quote_asset, eligibility_status, observed_at, payload
) values (
    'schema-smoke-idempotency', 'indodax:btc_idr', 'schema-smoke-idempotency:indodax:btc_idr', 'indodax', 'okx',
    'btc_idr', 'BTC/USDT:USDT', 'IDR', 'ELIGIBLE', now(), '{"attempt":1}'::jsonb
)
on conflict (idempotency_key) do update
set eligibility_status = excluded.eligibility_status,
    payload = excluded.payload;

insert into public.scanner_opportunities (
    scan_id, pair_key, local_exchange, global_exchange, local_symbol,
    global_symbol, quote_asset, eligibility_status, observed_at, payload
) values (
    'schema-smoke-idempotency', 'indodax:btc_idr', 'indodax', 'okx',
    'btc_idr', 'BTC/USDT:USDT', 'IDR', 'ELIGIBLE', now(), '{"attempt":2}'::jsonb
)
on conflict (scan_id, pair_key) do update
set eligibility_status = excluded.eligibility_status,
    payload = excluded.payload;

do $$
begin
    if (select count(*) from public.scanner_opportunities
        where idempotency_key = 'schema-smoke-idempotency:indodax:btc_idr') <> 1 then
        raise exception 'scanner opportunity retry created duplicate rows';
    end if;
end $$;

delete from public.scanner_opportunities where scan_id = 'schema-smoke-idempotency';

-- Runtime repository compatibility: normalized position, leg, trade, event and
-- risk-event writes must satisfy the deployed baseline foreign keys/constraints.
do $$
declare
    test_position_id uuid := '00000000-0000-0000-0000-000000000005';
begin
    insert into public.positions (
        id, pair_key, local_exchange, global_exchange, local_symbol, global_symbol,
        base_asset, quote_asset, futures_quote_asset, status, is_mock, qty,
        initial_qty, opened_at, state_snapshot
    ) values (
        test_position_id, 'indodax:btc_idr', 'indodax', 'okx', 'btc_idr',
        'BTC/USDT:USDT', 'BTC', 'IDR', 'USDT', 'HEDGED_ACTIVE', false,
        0.01, 0.01, now(), '{"event":"POSITION_OPENED"}'::jsonb
    )
    on conflict (id) do update set qty = excluded.qty, status = excluded.status;

    insert into public.position_legs (
        position_id, leg, exchange_name, symbol, side, status, qty
    ) values
        (test_position_id, 'spot', 'indodax', 'btc_idr', 'long', 'OPEN', 0.01),
        (test_position_id, 'perpetual', 'okx', 'BTC/USDT:USDT', 'short', 'OPEN', 0.01)
    on conflict (position_id, leg) do update set qty = excluded.qty;

    insert into public.closed_trades (
        position_id, trade_key, pair_key, local_exchange, global_exchange, symbol,
        qty, net_pnl, pnl_asset, closed_at
    ) values (
        test_position_id, 'schema-smoke-trade', 'indodax:btc_idr', 'indodax',
        'okx', 'btc_idr', 0.001, 0.01, 'USDT', now()
    )
    on conflict (trade_key) do update set net_pnl = excluded.net_pnl;

    insert into public.executor_events (
        event_id, event_type, pair_key, position_id, is_mock, payload
    ) values (
        'schema-smoke-event', 'POSITION_OPENED', 'indodax:btc_idr',
        test_position_id, false, '{"event":"POSITION_OPENED"}'::jsonb
    )
    on conflict (event_id) do nothing;

    insert into public.risk_events (
        event_key, severity, event_type, pair_key, message, is_mock, details
    ) values (
        'schema-smoke-risk', 'critical', 'HEDGE_STATE_UNKNOWN',
        'indodax:btc_idr', 'schema smoke risk event', false, '{"test":true}'::jsonb
    )
    on conflict (event_key) do nothing;

    if (select count(*) from public.position_legs where position_id = test_position_id) <> 2 then
        raise exception 'runtime position leg upsert failed';
    end if;
    if not exists (select 1 from public.closed_trades where trade_key = 'schema-smoke-trade') then
        raise exception 'runtime closed trade insert failed';
    end if;
    if not exists (select 1 from public.executor_events where event_id = 'schema-smoke-event') then
        raise exception 'runtime executor event insert failed';
    end if;
    if not exists (select 1 from public.risk_events where event_key = 'schema-smoke-risk') then
        raise exception 'runtime risk event insert failed';
    end if;

    delete from public.closed_trades where trade_key = 'schema-smoke-trade';
    delete from public.executor_events where event_id = 'schema-smoke-event';
    delete from public.risk_events where event_key = 'schema-smoke-risk';
    delete from public.positions where id = test_position_id;
end $$;
