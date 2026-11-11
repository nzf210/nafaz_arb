-- Runs after the initial migration against a clean PostgreSQL instance.
do $$
declare
    tbl text;
    mutable_tables text[] := array[
        'exchange_accounts','strategy_configs','risk_limits','market_pairs',
        'positions','orders','order_fills','position_legs','funding_payments',
        'closed_trades','balance_snapshots','risk_events','executor_events',
        'executor_status_snapshots','scanner_opportunities','order_registry'
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

-- Universal order registry: idempotent upsert on the deterministic local id and
-- partial unique indexes that keep orphan rows (no ids) insertable.
insert into public.order_registry (
    local_order_id, client_order_id, exchange_order_id, pair_key, position_key,
    exchange_name, market_type, leg, order_role, symbol, side, order_type,
    status, requested_qty, filled_qty, remaining_qty, recovery_status, is_mock
) values (
    '11111111-1111-1111-1111-111111111111', 'smoke-entry-1', '9001',
    'indodax:btc_idr', 'position:indodax:btc_idr', 'indodax', 'spot',
    'spot_entry', 'ENTRY', 'btc_idr', 'buy', 'limit', 'open', 1, 0, 1, 'ADOPTED', true
)
on conflict (local_order_id) do update
set status = excluded.status, filled_qty = excluded.filled_qty,
    remaining_qty = excluded.remaining_qty;

insert into public.order_registry (
    local_order_id, client_order_id, exchange_order_id, pair_key, position_key,
    exchange_name, market_type, leg, order_role, symbol, side, order_type,
    status, requested_qty, filled_qty, remaining_qty, recovery_status, is_mock
) values (
    '11111111-1111-1111-1111-111111111111', 'smoke-entry-1', '9001',
    'indodax:btc_idr', 'position:indodax:btc_idr', 'indodax', 'spot',
    'spot_entry', 'ENTRY', 'btc_idr', 'buy', 'limit', 'closed', 1, 1, 0, 'ADOPTED', true
)
on conflict (local_order_id) do update
set status = excluded.status, filled_qty = excluded.filled_qty,
    remaining_qty = excluded.remaining_qty;

do $$
begin
    if (select count(*) from public.order_registry where client_order_id = 'smoke-entry-1') <> 1 then
        raise exception 'order registry upsert created a duplicate row';
    end if;
    if (select status from public.order_registry where client_order_id = 'smoke-entry-1') <> 'closed' then
        raise exception 'order registry upsert did not advance status';
    end if;
    -- Two orphans with no ids must both be insertable (partial unique indexes).
    insert into public.order_registry (
        local_order_id, exchange_name, market_type, order_role, symbol, side,
        order_type, status, recovery_status, is_mock
    ) values
        ('22222222-2222-2222-2222-222222222221', 'indodax', 'spot', 'ENTRY',
         'eth_idr', 'buy', 'limit', 'open', 'ORPHAN_ORDER', false),
        ('22222222-2222-2222-2222-222222222222', 'indodax', 'spot', 'ENTRY',
         'eth_idr', 'buy', 'limit', 'open', 'ORPHAN_ORDER', false);
end $$;

delete from public.order_registry
where client_order_id = 'smoke-entry-1'
   or local_order_id in (
        '22222222-2222-2222-2222-222222222221',
        '22222222-2222-2222-2222-222222222222'
   );
