-- Enforce NAFAZ's asymmetric six-CEX architecture at the database boundary.
-- Local exchanges may only own Spot legs; foreign exchanges may only own
-- linear perpetual legs. Existing invalid rows intentionally fail migration.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'exchange_accounts_role_check') THEN
        ALTER TABLE public.exchange_accounts
            ADD CONSTRAINT exchange_accounts_role_check CHECK (
                (exchange_name IN ('indodax', 'tokocrypto') AND market_type = 'spot')
                OR
                (exchange_name IN ('mexc', 'okx', 'binance', 'bybit') AND market_type = 'perpetual')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'market_pairs_exchange_roles_check') THEN
        ALTER TABLE public.market_pairs
            ADD CONSTRAINT market_pairs_exchange_roles_check CHECK (
                local_exchange IN ('indodax', 'tokocrypto')
                AND global_exchange IN ('mexc', 'okx', 'binance', 'bybit')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'scanner_opportunities_exchange_roles_check') THEN
        ALTER TABLE public.scanner_opportunities
            ADD CONSTRAINT scanner_opportunities_exchange_roles_check CHECK (
                local_exchange IN ('indodax', 'tokocrypto')
                AND global_exchange IN ('mexc', 'okx', 'binance', 'bybit')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'positions_exchange_roles_check') THEN
        ALTER TABLE public.positions
            ADD CONSTRAINT positions_exchange_roles_check CHECK (
                local_exchange IN ('indodax', 'tokocrypto')
                AND global_exchange IN ('mexc', 'okx', 'binance', 'bybit')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'orders_exchange_role_check') THEN
        ALTER TABLE public.orders
            ADD CONSTRAINT orders_exchange_role_check CHECK (
                (exchange_name IN ('indodax', 'tokocrypto') AND market_type = 'spot')
                OR
                (exchange_name IN ('mexc', 'okx', 'binance', 'bybit') AND market_type = 'perpetual')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'position_legs_exchange_role_check') THEN
        ALTER TABLE public.position_legs
            ADD CONSTRAINT position_legs_exchange_role_check CHECK (
                (leg = 'spot' AND exchange_name IN ('indodax', 'tokocrypto'))
                OR
                (leg = 'perpetual' AND exchange_name IN ('mexc', 'okx', 'binance', 'bybit'))
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'funding_payments_foreign_exchange_check') THEN
        ALTER TABLE public.funding_payments
            ADD CONSTRAINT funding_payments_foreign_exchange_check CHECK (
                exchange_name IN ('mexc', 'okx', 'binance', 'bybit')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'closed_trades_exchange_roles_check') THEN
        ALTER TABLE public.closed_trades
            ADD CONSTRAINT closed_trades_exchange_roles_check CHECK (
                local_exchange IN ('indodax', 'tokocrypto')
                AND global_exchange IN ('mexc', 'okx', 'binance', 'bybit')
            );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'balance_snapshots_exchange_role_check') THEN
        ALTER TABLE public.balance_snapshots
            ADD CONSTRAINT balance_snapshots_exchange_role_check CHECK (
                (exchange_name IN ('indodax', 'tokocrypto') AND market_type = 'spot')
                OR
                (exchange_name IN ('mexc', 'okx', 'binance', 'bybit') AND market_type = 'perpetual')
            );
    END IF;
END
$$;
