"""Integration coverage for Executor V3 ownership leases and risk permissions."""
from __future__ import annotations

import asyncio
import os
import uuid
from types import SimpleNamespace

import pytest

from executor.global_stop import GlobalStopError, GlobalStopService
from executor.ownership import (
    OwnershipHeld,
    OwnershipLeaseService,
    StaleOwnership,
    account_scope_id_for,
)
from executor.risk_control import (
    RiskControlPlane,
    RiskControlConflict,
    RiskPermissionDenied,
)


def test_account_scope_identity_is_stable_and_credential_bound():
    config_a = SimpleNamespace(
        exchanges={"binance": SimpleNamespace(api_key="public-key-A", testnet=False)}
    )
    config_b = SimpleNamespace(
        exchanges={"binance": SimpleNamespace(api_key="public-key-B", testnet=False)}
    )
    first = account_scope_id_for(config_a, "BINANCE")
    assert first == account_scope_id_for(config_a, "binance")
    assert first != account_scope_id_for(config_b, "binance")
    assert "public-key-A" not in first
    explicit = SimpleNamespace(
        exchanges={
            "binance": SimpleNamespace(
                api_key="rotated-key", account_scope_id="stable-account-uuid"
            )
        }
    )
    assert account_scope_id_for(explicit, "binance") == "stable-account-uuid"


@pytest.fixture
async def postgres_pool():
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
    try:
        yield pool
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_ownership_generation_renews_reclaims_and_fences_old_owner(postgres_pool):
    suffix = uuid.uuid4().hex
    account = "ownership-test-account-" + suffix
    first = OwnershipLeaseService(owner_id="worker-a-" + suffix, lease_seconds=30)
    second = OwnershipLeaseService(owner_id="worker-b-" + suffix, lease_seconds=30)
    scope = {
        "account_scope_id": account,
        "venue": "binance",
        "market_type": "spot",
        "instrument": "BTC/USDT",
    }
    async with postgres_pool.acquire() as conn:
        lease_a = await first.acquire(conn, **scope)
        renewed = await first.acquire(conn, **scope)
        assert renewed.generation == lease_a.generation
        with pytest.raises(OwnershipHeld):
            await second.acquire(conn, **scope)

        await conn.execute(
            """
            UPDATE public.executor_ownership_leases
            SET lease_expires_at=clock_timestamp() - interval '1 second'
            WHERE scope_key=$1
            """,
            lease_a.scope_key,
        )
        lease_b = await second.acquire(conn, **scope)
        assert lease_b.generation == lease_a.generation + 1
        with pytest.raises(StaleOwnership):
            await first.validate(conn, lease_a)
        assert await second.validate(conn, lease_b) is None
        assert await second.release(conn, lease_b) is True

        events = await conn.fetch(
            """
            SELECT event_type, generation
            FROM public.executor_ownership_events
            WHERE scope_key=$1 ORDER BY created_at
            """,
            lease_a.scope_key,
        )
    assert [row["event_type"] for row in events] == [
        "ACQUIRED", "RENEWED", "RECLAIMED", "RELEASED"
    ]
    assert [row["generation"] for row in events] == [1, 1, 2, 2]


@pytest.mark.asyncio
async def test_entry_blocked_and_derisk_allowed_are_independent(postgres_pool):
    controls = RiskControlPlane()
    first_key = "risk-test-global-" + uuid.uuid4().hex
    account_key = "risk-test-account-" + uuid.uuid4().hex

    # A global risk stop rejects new entries but still permits a protective hedge.
    created = await controls.set_control(
        postgres_pool,
        scope_key="global",
        scope_type="GLOBAL",
        entry_blocked=True,
        derisk_allowed=True,
        actor_id="integration-test",
        event_type="ENTRY_BLOCKED",
        event_idempotency_key=first_key,
        expected_version=None,
        state={"reason": "test entry halt"},
    )
    assert created["version"] >= 1

    async with postgres_pool.acquire() as conn:
        with pytest.raises(RiskPermissionDenied, match="ENTRY_BLOCKED"):
            await controls.assert_allowed(conn, scope_keys=["global"], action_class="ENTRY")
        await controls.assert_allowed(conn, scope_keys=["global"], action_class="HEDGE")

    duplicate = await controls.set_control(
        postgres_pool,
        scope_key="global",
        scope_type="GLOBAL",
        entry_blocked=True,
        derisk_allowed=True,
        actor_id="integration-test",
        event_type="ENTRY_BLOCKED",
        event_idempotency_key=first_key,
        state={"reason": "test entry halt"},
    )
    assert duplicate["duplicate"] is True

    blocked = await controls.set_control(
        postgres_pool,
        scope_key="global",
        scope_type="GLOBAL",
        entry_blocked=True,
        derisk_allowed=False,
        actor_id="integration-test",
        event_type="DERISK_DISABLED",
        event_idempotency_key=account_key,
        expected_version=created["version"],
        state={"reason": "test protective-action restriction"},
    )
    async with postgres_pool.acquire() as conn:
        with pytest.raises(RiskPermissionDenied, match="DE_RISK_NOT_ALLOWED"):
            await controls.assert_allowed(conn, scope_keys=["global"], action_class="HEDGE")

    with pytest.raises(RiskControlConflict, match="version changed"):
        await controls.set_control(
            postgres_pool,
            scope_key="global",
            scope_type="GLOBAL",
            entry_blocked=False,
            derisk_allowed=True,
            actor_id="integration-test",
            event_type="STALE_WRITE",
            event_idempotency_key="stale-" + uuid.uuid4().hex,
            expected_version=created["version"],
        )

    # Restore the shared global test scope for subsequent integration cases.
    await controls.set_control(
        postgres_pool,
        scope_key="global",
        scope_type="GLOBAL",
        entry_blocked=False,
        derisk_allowed=True,
        actor_id="integration-test",
        event_type="TEST_RESET",
        event_idempotency_key="reset-" + uuid.uuid4().hex,
        expected_version=blocked["version"],
    )


async def _prepare_submit(
    service: GlobalStopService, *, client_id: str, account_scope_id: str,
    symbol: str, pair_key: str, exchange: str = "binance",
    market_type: str = "spot",
):
    await service.prepare_submit_attempt(
        client_order_id=client_id,
        exchange=exchange,
        market_type=market_type,
        symbol=symbol,
        side="buy",
        order_type="limit",
        quantity=0.001,
        price=100.0,
        pair_key=pair_key,
        account_scope_id=account_scope_id,
    )


@pytest.mark.asyncio
async def test_independent_instruments_submit_in_parallel_but_same_scope_is_exclusive():
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    suffix = uuid.uuid4().hex
    account = "parallel-account-" + suffix
    service_a = GlobalStopService(dsn, min_size=1, max_size=3)
    service_b = GlobalStopService(dsn, min_size=1, max_size=3)
    await service_a.start()
    await service_b.start()
    first_id = "scope-a-" + suffix
    second_id = "scope-b-" + suffix
    same_id = "scope-a-retry-" + suffix
    entered_a = asyncio.Event()
    release_a = asyncio.Event()

    async def hold_first_scope():
        async with service_a.order_submit_fence(
            client_order_id=first_id,
            scope_exchange="binance",
            scope_market_type="spot",
            scope_symbol="BTC/USDT",
            account_scope_id=account,
            action_class="ENTRY",
            scope_pair_key="pair-btc-" + suffix,
        ):
            entered_a.set()
            await release_a.wait()

    task_a = asyncio.create_task(hold_first_scope())
    try:
        await asyncio.wait_for(entered_a.wait(), timeout=5)

        await _prepare_submit(
            service_b, client_id=second_id, account_scope_id=account,
            symbol="ETH/USDT", pair_key="pair-eth-" + suffix,
        )
        async with service_b.order_submit_fence(
            client_order_id=second_id,
            scope_exchange="binance",
            scope_market_type="spot",
            scope_symbol="ETH/USDT",
            account_scope_id=account,
            action_class="ENTRY",
            scope_pair_key="pair-eth-" + suffix,
        ):
            pass

        await _prepare_submit(
            service_b, client_id=same_id, account_scope_id=account,
            symbol="BTC/USDT", pair_key="pair-btc-" + suffix,
        )
        with pytest.raises(GlobalStopError, match="another mutation currently owns"):
            async with service_b.order_submit_fence(
                client_order_id=same_id,
                scope_exchange="binance",
                scope_market_type="spot",
                scope_symbol="BTC/USDT",
                account_scope_id=account,
                action_class="ENTRY",
                scope_pair_key="pair-btc-" + suffix,
            ):
                pytest.fail("same instrument scope must not have two concurrent owners")
    finally:
        release_a.set()
        await task_a
        async with service_a.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM public.executor_submit_attempts WHERE client_order_id = any($1::text[])",
                [first_id, second_id, same_id],
            )
        await service_a.close()
        await service_b.close()

@pytest.mark.asyncio
async def test_global_stop_blocks_entry_but_allows_approved_protective_hedge():
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    suffix = uuid.uuid4().hex
    account = "risk-gate-account-" + suffix
    service = GlobalStopService(dsn, min_size=1, max_size=3)
    await service.start()
    entry_id = "risk-entry-" + suffix
    hedge_id = "risk-hedge-" + suffix
    controls = RiskControlPlane()
    control_version = None
    try:
        control = await controls.set_control(
            service.pool,
            scope_key="global",
            scope_type="GLOBAL",
            entry_blocked=True,
            derisk_allowed=True,
            actor_id="integration-test",
            event_type="ENTRY_BLOCKED",
            event_idempotency_key="risk-gate-" + suffix,
            state={"reason": "integration entry gate"},
        )
        control_version = control["version"]
        async with service.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE public.executor_global_control
                SET is_stopped=true, epoch=epoch+1, reason='TEST_GLOBAL_STOP',
                    source='integration-test', updated_at=now()
                WHERE singleton_id=1
                """
            )

        await _prepare_submit(
            service, client_id=entry_id, account_scope_id=account,
            symbol="BTC/IDR", pair_key="risk-entry-pair-" + suffix,
            exchange="indodax", market_type="spot",
        )
        with pytest.raises(GlobalStopError, match="ENTRY_BLOCKED"):
            async with service.order_submit_fence(
                client_order_id=entry_id,
                scope_exchange="indodax",
                scope_market_type="spot",
                scope_symbol="BTC/IDR",
                account_scope_id=account,
                action_class="ENTRY",
                scope_pair_key="risk-entry-pair-" + suffix,
            ):
                pytest.fail("new entry must remain blocked")

        await service.prepare_submit_attempt(
            client_order_id=hedge_id,
            exchange="okx",
            market_type="perpetual",
            symbol="BTC-USDT-SWAP",
            side="sell",
            order_type="market",
            quantity=1.0,
            price=50000.0,
            pair_key="risk-hedge-pair-" + suffix,
            account_scope_id=account,
        )
        async with service.order_submit_fence(
            client_order_id=hedge_id,
            scope_exchange="okx",
            scope_market_type="perpetual",
            scope_symbol="BTC-USDT-SWAP",
            account_scope_id=account,
            action_class="HEDGE",
            scope_pair_key="risk-hedge-pair-" + suffix,
        ):
            pass

        async with service.pool.acquire() as conn:
            statuses = await conn.fetch(
                """
                SELECT client_order_id, status FROM public.executor_submit_attempts
                WHERE client_order_id = any($1::text[])
                """,
                [entry_id, hedge_id],
            )
        by_id = {row["client_order_id"]: row["status"] for row in statuses}
        assert by_id == {entry_id: "PREPARED", hedge_id: "SUBMITTING"}
    finally:
        async with service.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE public.executor_global_control
                SET is_stopped=false, reason=null, source='integration-test-reset',
                    updated_at=now()
                WHERE singleton_id=1
                """
            )
            await conn.execute(
                "DELETE FROM public.executor_submit_attempts WHERE client_order_id = any($1::text[])",
                [entry_id, hedge_id],
            )
        if control_version is not None:
            current = await controls.set_control(
                service.pool,
                scope_key="global",
                scope_type="GLOBAL",
                entry_blocked=False,
                derisk_allowed=True,
                actor_id="integration-test",
                event_type="TEST_RESET",
                event_idempotency_key="risk-gate-reset-" + suffix,
                expected_version=control_version,
            )
            assert current["version"] == control_version + 1
        await service.close()
