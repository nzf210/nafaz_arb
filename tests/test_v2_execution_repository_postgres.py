"""Real PostgreSQL integration coverage for the V2 execution repository.

The test is enabled in the migration CI job after all versioned migrations have
been applied. It uses a nested transaction and rolls back all fixture rows.
"""
from __future__ import annotations

import os
import uuid
from decimal import Decimal

import pytest

from executor.v2_execution_repository import (
    ExecutionIntent,
    ExecutionRepositoryError,
    PostgresExecutionRepository,
)


class _Acquire:
    def __init__(self, connection):
        self._connection = connection

    async def __aenter__(self):
        return self._connection

    async def __aexit__(self, *_):
        return False


class _SingleConnectionPool:
    """Adapt one asyncpg connection to the repository's pool boundary."""

    def __init__(self, connection):
        self._connection = connection

    def acquire(self):
        return _Acquire(self._connection)


@pytest.mark.asyncio
async def test_postgres_repository_commits_intent_journal_and_outbox_atomically():
    database_url = os.getenv("NAFAZ_TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("NAFAZ_TEST_DATABASE_URL is only set by PostgreSQL integration CI")

    asyncpg = pytest.importorskip("asyncpg")
    connection = await asyncpg.connect(database_url)
    transaction = connection.transaction()
    await transaction.start()
    try:
        suffix = uuid.uuid4().hex[:12]
        spot_exchange = f"itspot_{suffix}"
        futures_exchange = f"itperp_{suffix}"
        spot_venue = f"it_spot_{suffix}"
        futures_venue = f"it_perp_{suffix}"
        spot_account = f"it_spot_account_{suffix}"
        futures_account = f"it_perp_account_{suffix}"
        spot_market = f"it_btc_idr_{suffix}"
        futures_market = f"it_btc_usdt_{suffix}"
        risk_profile = f"it_risk_{suffix}"
        route_id = f"it_route_{suffix}"

        await connection.executemany(
            """
            insert into public.v2_exchanges(exchange_code, display_name)
            values ($1, $2)
            """,
            [
                (spot_exchange, "Integration Spot"),
                (futures_exchange, "Integration Futures"),
            ],
        )
        await connection.executemany(
            """
            insert into public.v2_venues(
                venue_id, exchange_code, market_type, adapter, enabled,
                market_data_enabled, execution_enabled
            ) values ($1, $2, $3, $4, true, true, true)
            """,
            [
                (spot_venue, spot_exchange, "spot", "indodax"),
                (futures_venue, futures_exchange, "linear_perpetual", "okx"),
            ],
        )
        await connection.executemany(
            """
            insert into public.v2_accounts(account_id, venue_id, label, enabled)
            values ($1, $2, $3, true)
            """,
            [
                (spot_account, spot_venue, "integration spot"),
                (futures_account, futures_venue, "integration futures"),
            ],
        )

        spot_capabilities = (
            "balance_read",
            "open_orders_read",
            "limit_order_create",
            "order_cancel",
            "order_lookup_by_client_id",
            "multi_venue",
            "multi_account",
        )
        futures_capabilities = (
            "balance_read",
            "positions_read",
            "market_order_create",
            "reduce_only_order",
            "order_cancel",
            "order_lookup_by_client_id",
            "multi_venue",
            "multi_account",
        )
        await connection.executemany(
            """
            insert into public.v2_venue_capabilities(
                venue_id, capability, status, evidence_ref, verified_by, verified_at
            ) values ($1, $2, 'verified', $3, 'integration-test', now())
            """,
            [
                (spot_venue, capability, f"test://{suffix}/spot")
                for capability in spot_capabilities
            ] + [
                (futures_venue, capability, f"test://{suffix}/futures")
                for capability in futures_capabilities
            ],
        )
        await connection.executemany(
            """
            insert into public.v2_markets(
                market_id, venue_id, native_symbol, canonical_base_asset, quote_asset,
                settle_asset, contract_size, amount_step, minimum_amount,
                price_tick, minimum_notional, observed_at
            ) values ($1, $2, $3, 'BTC', $4, $5, $6, $7, $8, $9, $10, now())
            """,
            [
                (
                    spot_market, spot_venue, "btc_idr", "IDR", None, None,
                    Decimal("0.00000001"), Decimal("0.00000001"),
                    Decimal("1"), Decimal("1000"),
                ),
                (
                    futures_market, futures_venue, "BTC/USDT:USDT", "USDT", "USDT",
                    Decimal("0.001"), Decimal("0.001"), Decimal("0.001"),
                    Decimal("0.1"), Decimal("5"),
                ),
            ],
        )
        await connection.execute(
            """
            insert into public.v2_risk_profiles(
                risk_profile_id, max_daily_loss_usdt, max_exposure_per_account_usdt,
                max_exposure_per_route_usdt, max_entry_slippage_pct,
                max_consecutive_order_failures, max_open_spot_orders,
                max_open_futures_orders, require_fresh_balance,
                require_position_reconciliation
            ) values ($1, 5, 100, 50, 0.2, 5, 2, 2, true, true)
            """,
            risk_profile,
        )
        await connection.execute(
            """
            insert into public.v2_trading_routes(
                route_id, spot_venue_id, spot_account_id, futures_venue_id,
                futures_account_id, risk_profile_id, enabled, spot_quote_assets,
                futures_settle_assets, symbol_allowlist, symbol_denylist
            ) values ($1, $2, $3, $4, $5, $6, true, array['IDR'],
                      array['USDT'], array['BTC'], array[]::text[])
            """,
            route_id,
            spot_venue,
            spot_account,
            futures_venue,
            futures_account,
            risk_profile,
        )

        intent = ExecutionIntent(
            idempotency_key=f"it:{suffix}:btc:entry",
            route_id=route_id,
            venue_id=spot_venue,
            account_id=spot_account,
            market_id=spot_market,
            canonical_symbol="BTC",
            order_role="ENTRY",
            side="buy",
            order_type="limit",
            client_order_id=f"it-entry-{suffix}",
            requested_qty=Decimal("0.001"),
            limit_price=Decimal("500000000"),
            is_mock=True,
        )
        repository = PostgresExecutionRepository(_SingleConnectionPool(connection))
        created = await repository.create_intent(
            intent,
            payload={"source": "postgres-integration-test"},
        )
        replayed = await repository.create_intent(
            intent,
            payload={"source": "postgres-integration-test"},
        )

        assert created["created"] is True
        assert replayed["created"] is False
        assert replayed["intent_id"] == created["intent_id"]

        transition = await repository.transition_intent(
            created["intent_id"],
            expected_state="RECEIVED",
            next_state="VALIDATING",
            event_key=f"it:{suffix}:validating",
            event_type="intent.validating",
            payload={"validation": "passed"},
            quantity=Decimal("0.001"),
            price=Decimal("500000000"),
            quote_amount=Decimal("500000"),
            asset="IDR",
        )
        transition_replay = await repository.transition_intent(
            created["intent_id"],
            expected_state="RECEIVED",
            next_state="VALIDATING",
            event_key=f"it:{suffix}:validating",
            event_type="intent.validating",
            payload={"validation": "passed"},
            quantity=Decimal("0.001"),
            price=Decimal("500000000"),
            quote_amount=Decimal("500000"),
            asset="IDR",
        )

        assert transition["duplicate"] is False
        assert transition["state_version"] == 1
        assert transition_replay["duplicate"] is True

        state = await connection.fetchrow(
            """
            select state, state_version
              from public.v2_execution_intents
             where intent_id = $1::uuid
            """,
            created["intent_id"],
        )
        journal_count = await connection.fetchval(
            "select count(*) from public.v2_execution_journal where intent_id = $1::uuid",
            created["intent_id"],
        )
        outbox_count = await connection.fetchval(
            """
            select count(*) from public.outbox_events
             where aggregate_type = 'execution'
               and payload->>'intent_id' = $1
            """,
            created["intent_id"],
        )

        assert state["state"] == "VALIDATING"
        assert state["state_version"] == 1
        assert journal_count == 2
        assert outbox_count == 2

        # Omitting is_mock means LIVE admission, never an implicit MOCK bypass.
        live_intent = ExecutionIntent(
            idempotency_key=f"it:{suffix}:live-entry",
            route_id=route_id,
            venue_id=spot_venue,
            account_id=spot_account,
            market_id=spot_market,
            canonical_symbol="BTC",
            order_role="ENTRY",
            side="buy",
            order_type="limit",
            client_order_id=f"it-live-entry-{suffix}",
            requested_qty=Decimal("0.001"),
            limit_price=Decimal("500000000"),
        )
        assert live_intent.is_mock is False
        with pytest.raises(ExecutionRepositoryError):
            await repository.create_intent(
                live_intent,
                payload={"source": "postgres-live-gate-test"},
            )
        denied_count = await connection.fetchval(
            """
            select count(*) from public.v2_execution_intents
             where idempotency_key = $1
            """,
            live_intent.idempotency_key,
        )
        assert denied_count == 0
    finally:
        await transaction.rollback()
        await connection.close()
