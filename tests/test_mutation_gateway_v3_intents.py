"""V3 MutationGateway integration: intent-bound create path."""
from __future__ import annotations

import os
import uuid

import pytest

from executor.global_stop import GlobalStopService
from executor.mutation_gateway import MutationGateway, MutationResultType
from executor.order_intent_service import OrderIntentService


@pytest.fixture
async def postgres_pool():
    dsn = os.getenv("TEST_POSTGRES_DSN", "").strip()
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")
    import asyncpg

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=8)
    try:
        yield pool
    finally:
        await pool.close()


class _Venue:
    id = "okx"

    def __init__(self, *, response=None, error=None):
        self.response = response or {
            "id": "venue-order-" + uuid.uuid4().hex[:8],
            "status": "open", "filled": "0", "average": "65000",
        }
        self.error = error
        self.calls = []

    async def create_order_direct(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.response


async def _prepare_intent(pool, suffix, action_class="ENTRY"):
    intents = OrderIntentService(pool)
    intent = await intents.prepare_create(
        account_scope_id="gateway-account-" + suffix,
        idempotency_key="gateway-entry-" + suffix,
        venue="okx", market_type="swap",
        instrument="BTC-USDT-SWAP",
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        side="sell", order_type="limit",
        quantity="0.002", price="65000",
        reserved_base_qty="0.002", reserved_notional="130",
        reservation_asset="USDT", actor_id="gateway-test",
        request_metadata={
            "action_class": action_class,
            "risk_metrics": {
                "gross_exposure_usdt": 0, "net_exposure_usdt": 0,
                "deployed_capital_usdt": 0, "unhedged_notional_usdt": 0,
                "unhedged_duration_ms": 0, "open_order_count": 0,
                "daily_loss_usdt": 0, "drawdown_pct": 0,
                "slippage_bps": 0, "funding_cost_pct": 0,
                "market_data_age_ms": 0, "venue_health_score": 1,
            },
        },
    )
    return intents, intent


@pytest.mark.asyncio
async def test_gateway_create_intent_links_attempt_and_persists_ack_atomically(postgres_pool):
    suffix = uuid.uuid4().hex
    intents, intent = await _prepare_intent(postgres_pool, suffix)
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    venue = _Venue()
    gateway = MutationGateway(
        global_stop=global_stop, order_intent_service=intents,
    )

    result = await gateway.execute_create_intent(
        venue, intent_id=intent["intent_id"], actor_id="gateway-test",
    )
    assert result.status == MutationResultType.CONFIRMED
    assert len(venue.calls) == 1
    assert venue.calls[0]["params"]["client_order_id"] == intent["client_order_id"]

    current = await intents.get_intent(intent["intent_id"])
    assert current["status"] == "ACKNOWLEDGED"
    assert current["exchange_order_id"] == result.exchange_order_id
    assert current["owner_id"]
    assert current["risk_control_snapshot"]["action_class"] == "ENTRY"

    async with postgres_pool.acquire() as conn:
        attempt = await conn.fetchrow(
            """
            SELECT status, mutation_intent_id, client_order_id
            FROM public.executor_submit_attempts WHERE client_order_id=$1
            """,
            intent["client_order_id"],
        )
        event_types = await conn.fetch(
            """
            SELECT event_type FROM public.executor_mutation_intent_events
            WHERE intent_id=$1 ORDER BY event_version
            """,
            intent["intent_id"],
        )
        assert attempt["mutation_intent_id"] == intent["intent_id"]
        assert attempt["status"] == "ACKNOWLEDGED"
        assert [row["event_type"] for row in event_types] == [
            "CREATE_INTENT_PREPARED", "INTENT_SUBMITTING", "INTENT_ACKNOWLEDGED",
        ]
        await global_stop.ownership.release_all(conn)


@pytest.mark.asyncio
async def test_derisk_create_intent_is_admitted_under_global_stop(postgres_pool):
    suffix = uuid.uuid4().hex
    intents, intent = await _prepare_intent(postgres_pool, suffix, action_class="DERISK")
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    venue = _Venue()
    gateway = MutationGateway(
        global_stop=global_stop, order_intent_service=intents,
    )
    async with postgres_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.executor_global_control
            SET is_stopped=true, epoch=epoch+1, reason='derisk-create-under-stop-test'
            WHERE singleton_id=1
            """
        )
    try:
        result = await gateway.execute_create_intent(
            venue, intent_id=intent["intent_id"], actor_id="gateway-test",
        )
        assert result.status == MutationResultType.CONFIRMED
        assert len(venue.calls) == 1
        current = await intents.get_intent(intent["intent_id"])
        assert current["status"] == "ACKNOWLEDGED"
        assert current["risk_control_snapshot"]["action_class"] == "DERISK"
    finally:
        async with postgres_pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE public.executor_global_control
                SET is_stopped=false, epoch=epoch+1, reason=NULL
                WHERE singleton_id=1
                """
            )
            await global_stop.ownership.release_all(conn)


@pytest.mark.asyncio
async def test_gateway_never_retries_unknown_create_intent(postgres_pool):
    suffix = uuid.uuid4().hex
    intents, intent = await _prepare_intent(postgres_pool, suffix)
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    venue = _Venue(error=TimeoutError("venue response timed out"))
    gateway = MutationGateway(
        global_stop=global_stop, order_intent_service=intents,
    )

    first = await gateway.execute_create_intent(
        venue, intent_id=intent["intent_id"], actor_id="gateway-test",
    )
    second = await gateway.execute_create_intent(
        venue, intent_id=intent["intent_id"], actor_id="gateway-test",
    )
    assert first.status == MutationResultType.SUBMISSION_UNKNOWN
    assert second.status == MutationResultType.SUBMISSION_UNKNOWN
    assert len(venue.calls) == 1
    current = await intents.get_intent(intent["intent_id"])
    assert current["status"] == "UNKNOWN"
    async with postgres_pool.acquire() as conn:
        attempt = await conn.fetchrow(
            "SELECT status FROM public.executor_submit_attempts WHERE client_order_id=$1",
            intent["client_order_id"],
        )
        assert attempt["status"] == "UNKNOWN"
        await global_stop.ownership.release_all(conn)


@pytest.mark.asyncio
async def test_session_bound_entry_intent_fails_closed_without_admitted_session(postgres_pool):
    suffix = uuid.uuid4().hex
    intents = OrderIntentService(postgres_pool)
    intent = await intents.prepare_create(
        account_scope_id="gateway-session-account-" + suffix,
        idempotency_key="gateway-session-entry-" + suffix,
        venue="okx", market_type="swap",
        instrument="BTC-USDT-SWAP",
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        side="sell", order_type="limit",
        quantity="0.002", price="65000",
        reserved_base_qty="0.002", reserved_notional="130",
        reservation_asset="USDT", actor_id="gateway-test",
        session_id="missing-session-" + suffix,
        leg_id="missing-leg-" + suffix,
        request_metadata={"action_class": "ENTRY"},
    )
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    venue = _Venue()
    gateway = MutationGateway(
        global_stop=global_stop, order_intent_service=intents,
    )

    result = await gateway.execute_create_intent(
        venue, intent_id=intent["intent_id"], actor_id="gateway-test",
    )
    assert result.status == MutationResultType.REJECTED
    assert venue.calls == []
    current = await intents.get_intent(intent["intent_id"])
    assert current["status"] in {"PREPARED", "ABORTED"}
    async with postgres_pool.acquire() as conn:
        await global_stop.ownership.release_all(conn)


@pytest.mark.asyncio
async def test_session_bound_futures_sell_cannot_use_entry_action_class(postgres_pool):
    suffix = uuid.uuid4().hex
    intents = OrderIntentService(postgres_pool)
    intent = await intents.prepare_create(
        account_scope_id="gateway-futures-entry-account-" + suffix,
        idempotency_key="gateway-futures-entry-" + suffix,
        venue="okx", market_type="swap",
        instrument="BTC-USDT-SWAP",
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        side="sell", order_type="limit",
        quantity="1", price="65000",
        reserved_base_qty="0.001", reserved_notional="65",
        reservation_asset="USDT", actor_id="gateway-test",
        session_id="session-" + suffix,
        leg_id="future-" + suffix,
        request_metadata={"action_class": "ENTRY"},
    )
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    venue = _Venue()
    gateway = MutationGateway(global_stop=global_stop, order_intent_service=intents)

    result = await gateway.execute_create_intent(
        venue, intent_id=intent["intent_id"], actor_id="gateway-test",
    )
    assert result.status == MutationResultType.REJECTED
    assert venue.calls == []
    async with postgres_pool.acquire() as conn:
        attempt_status = await conn.fetchval(
            "SELECT status FROM public.executor_submit_attempts WHERE client_order_id=$1",
            intent["client_order_id"],
        )
        assert attempt_status == "ABORTED"
        await global_stop.ownership.release_all(conn)
