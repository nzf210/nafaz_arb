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


-- Runtime persistence adapters must only send columns present in the versioned schema.
do $$
declare
    expected record;
begin
    for expected in
        select * from (values
            ('market_pairs','pair_key'),('market_pairs','local_exchange'),
            ('market_pairs','global_exchange'),('market_pairs','local_symbol'),
            ('market_pairs','global_symbol'),('market_pairs','base_asset'),
            ('market_pairs','local_quote_asset'),('market_pairs','global_quote_asset'),
            ('market_pairs','price_tick'),('market_pairs','is_enabled'),('market_pairs','metadata'),
            ('scanner_opportunities','id'),('scanner_opportunities','scan_id'),
            ('scanner_opportunities','pair_key'),('scanner_opportunities','local_exchange'),
            ('scanner_opportunities','global_exchange'),('scanner_opportunities','local_symbol'),
            ('scanner_opportunities','global_symbol'),('scanner_opportunities','quote_asset'),
            ('scanner_opportunities','execution_mode'),('scanner_opportunities','eligibility_status'),
            ('scanner_opportunities','reason'),('scanner_opportunities','spot_price'),
            ('scanner_opportunities','perp_price'),('scanner_opportunities','spread_pct'),
            ('scanner_opportunities','funding_rate'),('scanner_opportunities','estimated_net_profit'),
            ('scanner_opportunities','estimated_net_roi_pct'),('scanner_opportunities','executable_qty'),
            ('scanner_opportunities','capacity_pct'),('scanner_opportunities','observed_at'),
            ('scanner_opportunities','payload'),
            ('orders','id'),('orders','position_id'),('orders','pair_key'),
            ('orders','exchange_name'),('orders','market_type'),('orders','leg'),
            ('orders','exchange_order_id'),('orders','client_order_id'),('orders','symbol'),
            ('orders','side'),('orders','order_type'),('orders','status'),
            ('orders','requested_qty'),('orders','filled_qty'),('orders','remaining_qty'),
            ('orders','average_fill_price'),('orders','reduce_only'),('orders','raw_payload'),
            ('order_fills','id'),('order_fills','order_id'),('order_fills','exchange_fill_id'),
            ('order_fills','qty'),('order_fills','price'),('order_fills','quote_qty'),
            ('order_fills','fee_amount'),('order_fills','fee_asset'),('order_fills','filled_at'),
            ('funding_payments','id'),('funding_payments','position_id'),
            ('funding_payments','exchange_name'),('funding_payments','symbol'),
            ('funding_payments','exchange_payment_id'),('funding_payments','amount'),
            ('funding_payments','asset'),('funding_payments','payment_status'),
            ('funding_payments','settled_at'),('funding_payments','raw_payload'),
            ('balance_snapshots','id'),('balance_snapshots','exchange_name'),
            ('balance_snapshots','market_type'),('balance_snapshots','asset'),
            ('balance_snapshots','total'),('balance_snapshots','free'),
            ('balance_snapshots','used'),('balance_snapshots','is_mock'),
            ('balance_snapshots','observed_at'),
            ('risk_events','event_key'),('risk_events','severity'),
            ('risk_events','event_type'),('risk_events','pair_key'),
            ('risk_events','exchange_name'),('risk_events','message'),
            ('risk_events','action_taken'),('risk_events','is_mock'),
            ('risk_events','details'),('risk_events','occurred_at'),
            ('executor_status_snapshots','id'),('executor_status_snapshots','instance_id'),
            ('executor_status_snapshots','status'),('executor_status_snapshots','mode'),
            ('executor_status_snapshots','is_mock'),('executor_status_snapshots','snapshot'),
            ('executor_status_snapshots','observed_at')
        ) as columns_expected(table_name, column_name)
    loop
        if not exists (
            select 1 from information_schema.columns
            where table_schema = 'public'
              and table_name = expected.table_name
              and column_name = expected.column_name
        ) then
            raise exception 'Runtime persistence schema mismatch: public.%.% is missing',
                expected.table_name, expected.column_name;
        end if;
    end loop;
end $$;
