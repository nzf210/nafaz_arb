"""Tests for read-only mutation intent reconciliation and fencing."""
from __future__ import annotations

import os
import uuid

import pytest

from executor.global_stop import GlobalStopService
from executor.mutation_intent_recovery import MutationIntentRecoveryWorker
from executor.order_intent_service import OrderIntentService
from executor.ownership import OwnershipLeaseService


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


class _ReadOnlyVenue:
    id = "okx"

    def __init__(self, order):
        self.order = order
        self.fetch_calls = 0

    async def fetch_order_by_client_id(self, client_order_id, symbol):
        self.fetch_calls += 1
        return self.order

    async def fetch_order(self, order_id, symbol):
        self.fetch_calls += 1
        return self.order


async def _prepare_create(pool, suffix):
    service = OrderIntentService(pool)
    intent = await service.prepare_create(
        account_scope_id="recovery-account-" + suffix,
        idempotency_key="recovery-create-" + suffix,
        venue="okx", market_type="swap", instrument="BTC-USDT-SWAP",
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        side="sell", order_type="limit", quantity="0.002", price="65000",
        reserved_base_qty="0.002", reserved_notional="130",
        reservation_asset="USDT", actor_id="recovery-test",
    )
    return service, intent


async def _prepare_cancel(pool, suffix):
    service = OrderIntentService(pool)
    intent = await service.prepare_cancel(
        account_scope_id="recovery-cancel-account-" + suffix,
        idempotency_key="recovery-cancel-" + suffix,
        venue="okx", market_type="swap", instrument="BTC-USDT-SWAP",
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        target_order_id="cancel-order-" + suffix,
        actor_id="recovery-test",
    )
    return service, intent


async def _submit_with_expired_owner(pool, service, intent, suffix, action_class="ENTRY"):
    old_owner = OwnershipLeaseService(owner_id="old-recovery-owner-" + suffix)
    async with pool.acquire() as conn:
        lease = await old_owner.acquire(
            conn, account_scope_id=intent["account_scope_id"],
            venue=intent["venue"], market_type=intent["market_type"],
            instrument=intent["instrument"],
        )
    await service.mark_submitting(
        intent["intent_id"], lease=lease, actor_id="old-worker",
        risk_snapshot={"action_class": action_class, "allowed": True},
    )
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.executor_ownership_leases
            SET lease_expires_at=clock_timestamp() - interval '1 second'
            WHERE scope_key=$1
            """,
            lease.scope_key,
        )


@pytest.mark.asyncio
async def test_recovery_finds_create_order_and_never_submits_again(postgres_pool):
    suffix = uuid.uuid4().hex
    service, intent = await _prepare_create(postgres_pool, suffix)
    await _submit_with_expired_owner(postgres_pool, service, intent, suffix)
    venue = _ReadOnlyVenue({
        "id": "recovered-order-" + suffix,
        "clientOrderId": intent["client_order_id"],
        "status": "open", "filled": "0", "remaining": "0.002",
    })
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    worker = MutationIntentRecoveryWorker(
        global_stop=global_stop, intent_service=service,
        venue_resolver=lambda _intent: venue,
    )

    result = await worker.reconcile_one(intent["intent_id"])
    current = await service.get_intent(intent["intent_id"])
    assert result["outcome"] == "ACKNOWLEDGED"
    assert current["status"] == "ACKNOWLEDGED"
    assert current["exchange_order_id"] == "recovered-order-" + suffix
    assert current["owner_generation"] > 1
    assert venue.fetch_calls == 1
    assert not hasattr(venue, "create_order")


@pytest.mark.asyncio
async def test_recovery_never_rejects_an_order_only_because_lookup_is_not_found(postgres_pool):
    suffix = uuid.uuid4().hex
    service, intent = await _prepare_create(postgres_pool, suffix)
    await _submit_with_expired_owner(postgres_pool, service, intent, suffix)
    venue = _ReadOnlyVenue(None)
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    worker = MutationIntentRecoveryWorker(
        global_stop=global_stop, intent_service=service,
        venue_resolver=lambda _intent: venue,
    )

    result = await worker.reconcile_one(intent["intent_id"])
    current = await service.get_intent(intent["intent_id"])
    assert result["outcome"] == "UNRESOLVED"
    assert current["status"] == "UNKNOWN"
    assert current["last_error"].find("NOT_FOUND") >= 0
    assert venue.fetch_calls == 1


@pytest.mark.asyncio
async def test_recovery_confirms_cancel_only_after_terminal_state_and_fill_reconciliation(postgres_pool):
    suffix = uuid.uuid4().hex
    service, intent = await _prepare_cancel(postgres_pool, suffix)
    await _submit_with_expired_owner(
        postgres_pool, service, intent, suffix, action_class="CANCEL",
    )
    venue = _ReadOnlyVenue({
        "id": "cancel-order-" + suffix, "status": "canceled",
        "filled": "0", "remaining": "0.002",
    })
    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    worker = MutationIntentRecoveryWorker(
        global_stop=global_stop, intent_service=service,
        venue_resolver=lambda _intent: venue,
        fill_reconciler=lambda _intent, _before, _after: True,
    )

    result = await worker.reconcile_one(intent["intent_id"])
    current = await service.get_intent(intent["intent_id"])
    assert result["outcome"] == "RESOLVED"
    assert current["status"] == "CANCEL_CONFIRMED"
    assert current["exchange_order_id"] == "cancel-order-" + suffix
    assert venue.fetch_calls == 1
