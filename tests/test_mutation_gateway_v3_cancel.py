"""V3 protective cancellation: durable intent, scoped fence and terminal evidence."""
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


class _CancelVenue:
    id = "okx"

    def __init__(self, *, final_status="canceled", cancel_error=None):
        self.status = "open"
        self.final_status = final_status
        self.cancel_error = cancel_error
        self.cancel_calls = 0
        self.fetch_calls = 0

    async def fetch_order(self, order_id, symbol):
        self.fetch_calls += 1
        return {
            "id": order_id, "symbol": symbol, "status": self.status,
            "clientOrderId": "client-" + order_id,
            "filled": "0", "remaining": "0.002",
        }

    async def cancel_order(self, order_id, symbol):
        self.cancel_calls += 1
        if self.cancel_error:
            raise self.cancel_error
        self.status = self.final_status
        return {"id": order_id, "status": "cancel-request-accepted"}


async def _prepare_cancel(pool, suffix):
    service = OrderIntentService(pool)
    intent = await service.prepare_cancel(
        account_scope_id="cancel-gateway-account-" + suffix,
        idempotency_key="cancel-gateway-" + suffix,
        venue="okx", market_type="swap", instrument="BTC-USDT-SWAP",
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        target_order_id="venue-order-" + suffix,
        actor_id="cancel-gateway-test",
    )
    return service, intent


@pytest.mark.asyncio
async def test_cancel_gateway_is_allowed_under_global_stop_and_requires_fill_evidence(postgres_pool):
    suffix = uuid.uuid4().hex
    intents, intent = await _prepare_cancel(postgres_pool, suffix)
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    gateway = MutationGateway(global_stop=global_stop, order_intent_service=intents)
    venue = _CancelVenue(final_status="canceled")

    async with postgres_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.executor_global_control
            SET is_stopped=true, epoch=epoch+1, reason='cancel-under-stop-test'
            WHERE singleton_id=1
            """
        )
    try:
        result = await gateway.execute_cancel_intent(
            venue, intent_id=intent["intent_id"], actor_id="cancel-gateway-test",
            fill_reconciler=lambda _intent, _before, _after: True,
        )
        assert result.status == MutationResultType.CONFIRMED
        assert venue.cancel_calls == 1
        current = await intents.get_intent(intent["intent_id"])
        assert current["status"] == "CANCEL_CONFIRMED"
        assert current["exchange_order_id"] == "venue-order-" + suffix
        assert current["risk_control_snapshot"]["action_class"] == "CANCEL"
        assert current["risk_control_snapshot"]["global_stop_active"] is True
        async with postgres_pool.acquire() as conn:
            events = await conn.fetch(
                """
                SELECT event_type FROM public.executor_mutation_intent_events
                WHERE intent_id=$1 ORDER BY event_version
                """,
                intent["intent_id"],
            )
            assert [row["event_type"] for row in events] == [
                "CANCEL_INTENT_PREPARED", "INTENT_SUBMITTING", "INTENT_CANCEL_CONFIRMED",
            ]
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
async def test_terminal_cancel_without_fill_reconciliation_stays_unknown(postgres_pool):
    suffix = uuid.uuid4().hex
    intents, intent = await _prepare_cancel(postgres_pool, suffix)
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    gateway = MutationGateway(global_stop=global_stop, order_intent_service=intents)
    venue = _CancelVenue(final_status="canceled")

    result = await gateway.execute_cancel_intent(
        venue, intent_id=intent["intent_id"], actor_id="cancel-gateway-test",
    )
    assert result.status == MutationResultType.SUBMISSION_UNKNOWN
    assert venue.cancel_calls == 1
    current = await intents.get_intent(intent["intent_id"])
    assert current["status"] == "UNKNOWN"
    async with postgres_pool.acquire() as conn:
        await global_stop.ownership.release_all(conn)


@pytest.mark.asyncio
async def test_cancel_gateway_keeps_timeout_unknown_and_never_retries(postgres_pool):
    suffix = uuid.uuid4().hex
    intents, intent = await _prepare_cancel(postgres_pool, suffix)
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    gateway = MutationGateway(global_stop=global_stop, order_intent_service=intents)
    venue = _CancelVenue(cancel_error=TimeoutError("cancel response timed out"))

    first = await gateway.execute_cancel_intent(
        venue, intent_id=intent["intent_id"], actor_id="cancel-gateway-test",
        fill_reconciler=lambda _intent, _before, _after: True,
    )
    second = await gateway.execute_cancel_intent(
        venue, intent_id=intent["intent_id"], actor_id="cancel-gateway-test",
        fill_reconciler=lambda _intent, _before, _after: True,
    )
    assert first.status == MutationResultType.SUBMISSION_UNKNOWN
    assert second.status == MutationResultType.SUBMISSION_UNKNOWN
    assert venue.cancel_calls == 1
    current = await intents.get_intent(intent["intent_id"])
    assert current["status"] == "UNKNOWN"
    async with postgres_pool.acquire() as conn:
        await global_stop.ownership.release_all(conn)
