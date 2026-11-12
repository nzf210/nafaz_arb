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

-- Runtime writer contracts: division by zero fails CI if any required column is absent.
select 1 / case when count(*) = 0 then 1 else 0 end
from (
    select required.table_name, required.column_name
    from (values
        ('position_legs', 'position_id'),
        ('position_legs', 'leg'),
        ('position_legs', 'exchange_name'),
        ('position_legs', 'symbol'),
        ('position_legs', 'side'),
        ('position_legs', 'status'),
        ('position_legs', 'qty'),
        ('position_legs', 'average_entry_price'),
        ('funding_payments', 'position_id'),
        ('funding_payments', 'exchange_name'),
        ('funding_payments', 'symbol'),
        ('funding_payments', 'exchange_payment_id'),
        ('funding_payments', 'amount'),
        ('funding_payments', 'asset'),
        ('funding_payments', 'payment_status'),
        ('funding_payments', 'settled_at'),
        ('balance_snapshots', 'exchange_name'),
        ('balance_snapshots', 'market_type'),
        ('balance_snapshots', 'asset'),
        ('balance_snapshots', 'total'),
        ('balance_snapshots', 'free'),
        ('balance_snapshots', 'used'),
        ('balance_snapshots', 'is_mock'),
        ('balance_snapshots', 'observed_at'),
        ('executor_status_snapshots', 'instance_id'),
        ('executor_status_snapshots', 'status'),
        ('executor_status_snapshots', 'mode'),
        ('executor_status_snapshots', 'is_mock'),
        ('executor_status_snapshots', 'snapshot'),
        ('executor_status_snapshots', 'observed_at')
    ) as required(table_name, column_name)
    where not exists (
        select 1
        from information_schema.columns actual
        where actual.table_schema = 'public'
          and actual.table_name = required.table_name
          and actual.column_name = required.column_name
    )
) missing_runtime_columns;

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
    'indodax', 'okx', 'legacy_idr', 'LEGACY/USDT:USDT',
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
        where table_schema = 'public' and table_name = 'scanner_opportunities'
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
    'schema-smoke-idempotency', 'indodax:btc_idr->okx:btc/usdt:usdt',
    'schema-smoke-idempotency:indodax:btc_idr->okx:btc/usdt:usdt', 'indodax', 'okx',
    'btc_idr', 'BTC/USDT:USDT', 'IDR', 'ELIGIBLE', now(), '{"attempt":1}'::jsonb
)
on conflict (idempotency_key) do update
set payload = excluded.payload;

insert into public.scanner_opportunities (
    scan_id, pair_key, idempotency_key, local_exchange, global_exchange,
    local_symbol, global_symbol, quote_asset, eligibility_status, observed_at, payload
) values (
    'schema-smoke-idempotency', 'indodax:btc_idr->okx:btc/usdt:usdt',
    'schema-smoke-idempotency:indodax:btc_idr->okx:btc/usdt:usdt', 'indodax', 'okx',
    'btc_idr', 'BTC/USDT:USDT', 'IDR', 'ELIGIBLE', now(), '{"attempt":2}'::jsonb
)
on conflict (idempotency_key) do update
set payload = excluded.payload;

do $$
begin
    if (
        select count(*) from public.scanner_opportunities
        where idempotency_key = 'schema-smoke-idempotency:indodax:btc_idr->okx:btc/usdt:usdt'
    ) <> 1 then
        raise exception 'scanner opportunity retry created duplicate rows';
    end if;
    if (
        select payload ->> 'attempt' from public.scanner_opportunities
        where idempotency_key = 'schema-smoke-idempotency:indodax:btc_idr->okx:btc/usdt:usdt'
    ) <> '2' then
        raise exception 'scanner opportunity upsert did not update the existing row';
    end if;
end $$;

delete from public.scanner_opportunities
where idempotency_key = 'schema-smoke-idempotency:indodax:btc_idr->okx:btc/usdt:usdt';
