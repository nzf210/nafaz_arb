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

-- Durable executor V3 schema: legacy provenance remains unknown, transition
-- events are append-only for service_role, and session evidence cannot cascade.
DO $$
DECLARE
    tbl TEXT;
    fk_delete_action "char";
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'execution_sessions', 'execution_legs', 'execution_session_events'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = tbl AND c.relrowsecurity
        ) THEN
            RAISE EXCEPTION 'RLS is not enabled on public.%', tbl;
        END IF;
        IF has_table_privilege('anon', format('public.%I', tbl), 'select')
           OR has_table_privilege('authenticated', format('public.%I', tbl), 'select') THEN
            RAISE EXCEPTION 'client role unexpectedly has SELECT on public.%', tbl;
        END IF;
        IF NOT has_table_privilege('service_role', format('public.%I', tbl), 'select')
           OR NOT has_table_privilege('service_role', format('public.%I', tbl), 'insert') THEN
            RAISE EXCEPTION 'service_role lacks required access on public.%', tbl;
        END IF;
    END LOOP;

    IF has_table_privilege('service_role', 'public.execution_session_events', 'update')
       OR has_table_privilege('service_role', 'public.execution_session_events', 'delete') THEN
        RAISE EXCEPTION 'execution_session_events is mutable by service_role';
    END IF;

    SELECT c.confdeltype INTO fk_delete_action
    FROM pg_constraint c
    WHERE c.conrelid = 'public.execution_legs'::regclass
      AND c.confrelid = 'public.execution_sessions'::regclass
      AND c.contype = 'f'
      AND c.conkey = ARRAY[
          (SELECT attnum FROM pg_attribute
           WHERE attrelid = 'public.execution_legs'::regclass
             AND attname = 'session_id' AND NOT attisdropped)
      ]::smallint[]
      AND c.confkey = ARRAY[
          (SELECT attnum FROM pg_attribute
           WHERE attrelid = 'public.execution_sessions'::regclass
             AND attname = 'session_id' AND NOT attisdropped)
      ]::smallint[];

    IF fk_delete_action IS NULL OR fk_delete_action NOT IN ('r', 'a') THEN
        RAISE EXCEPTION 'execution_legs session FK is missing or still cascading';
    END IF;
END $$;

DO $$
DECLARE
    missing_columns TEXT[];
BEGIN
    SELECT array_agg(required.column_name)
    INTO missing_columns
    FROM (VALUES
        ('execution_sessions', 'admission_key'),
        ('execution_sessions', 'account_scope_id'),
        ('execution_sessions', 'aggregate_version'),
        ('execution_sessions', 'fencing_generation'),
        ('execution_sessions', 'policy_snapshot'),
        ('execution_legs', 'native_symbol'),
        ('execution_legs', 'quantity_unit'),
        ('execution_legs', 'target_order_qty'),
        ('execution_legs', 'target_base_qty'),
        ('execution_legs', 'verified_exposure_base_qty')
    ) AS required(table_name, column_name)
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name = required.table_name
          AND c.column_name = required.column_name
    );

    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION 'V3 execution columns missing: %', missing_columns;
    END IF;
END $$;

-- A legacy session/leg must not receive fabricated account, policy, venue or
-- quantity provenance merely because the additive migration was applied.
INSERT INTO public.execution_sessions (session_id, pair_key)
VALUES ('schema-smoke-v3-legacy-session', 'legacy:unknown-pair');

INSERT INTO public.execution_legs (
    leg_id, session_id, leg_type, venue, market_type, symbol
) VALUES (
    'schema-smoke-v3-legacy-spot',
    'schema-smoke-v3-legacy-session',
    'SPOT', 'legacy-unknown', 'spot', 'LEGACY/UNKNOWN'
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.execution_sessions
        WHERE session_id = 'schema-smoke-v3-legacy-session'
          AND (admission_key IS NOT NULL OR account_scope_id IS NOT NULL
               OR fencing_generation IS NOT NULL OR policy_snapshot IS NOT NULL
               OR policy_snapshot_hash IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'legacy session received fabricated V3 provenance';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.execution_legs
        WHERE leg_id = 'schema-smoke-v3-legacy-spot'
          AND (native_symbol IS NOT NULL OR normalized_symbol IS NOT NULL
               OR quantity_unit IS NOT NULL OR target_order_qty IS NOT NULL
               OR target_base_qty IS NOT NULL OR verified_exposure_base_qty IS NOT NULL)
    ) THEN
        RAISE EXCEPTION 'legacy leg received fabricated V3 identity or quantity units';
    END IF;
END $$;

INSERT INTO public.execution_session_events (
    session_id, aggregate_version, entity_type, entity_id, entity_version,
    to_state, event_type, idempotency_key, event_payload
) VALUES (
    'schema-smoke-v3-legacy-session', 1, 'SESSION',
    'schema-smoke-v3-legacy-session', 1,
    'CREATED', 'SCHEMA_SMOKE', 'schema-smoke-v3-event-1', '{}'::jsonb
);

DO $$
BEGIN
    BEGIN
        DELETE FROM public.execution_sessions
        WHERE session_id = 'schema-smoke-v3-legacy-session';
        RAISE EXCEPTION 'session deletion unexpectedly succeeded despite retained event/leg evidence';
    EXCEPTION WHEN foreign_key_violation THEN
        NULL;
    END;
END $$;

DELETE FROM public.execution_session_events
WHERE session_id = 'schema-smoke-v3-legacy-session';
DELETE FROM public.execution_legs
WHERE session_id = 'schema-smoke-v3-legacy-session';
DELETE FROM public.execution_sessions
WHERE session_id = 'schema-smoke-v3-legacy-session';

