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

do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'gross_pnl'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'estimated_fees'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'estimated_net_pnl'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'realized_pnl'
    ) or not exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'closed_trades'
          and column_name = 'pnl_status'
    ) then
        raise exception 'explicit gross/estimated/reconciled PnL columns are missing';
    end if;

    if not exists (
        select 1 from pg_constraint
        where conname = 'closed_trades_pnl_status_check'
          and conrelid = 'public.closed_trades'::regclass
    ) then
        raise exception 'closed_trades PnL status constraint is missing';
    end if;
end $$;
