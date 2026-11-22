"""Tests for V3 logical order/mutation intents and fencing."""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from executor.order_intent_service import (
    OrderIntentConflict,
    OrderIntentError,
    OrderIntentReservationConflict,
    OrderIntentService,
    OrderIntentStaleOwner,
)
from executor.ownership import OwnershipLeaseService


def test_intent_identity_and_client_order_id_are_deterministic():
    service = object.__new__(OrderIntentService)
    first = service._identity("account-a", "dispatch-leg-entry")
    retry = service._identity("account-a", "dispatch-leg-entry")
    other_account = service._identity("account-b", "dispatch-leg-entry")
    other_intent = service._identity("account-a", "dispatch-leg-exit")
    assert first == retry
    assert first != other_account
    assert first != other_intent


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


def _create_args(suffix, **overrides):
    values = {
        "account_scope_id": "intent-account-" + suffix,
        "idempotency_key": "intent-entry-" + suffix,
        "venue": "okx",
        "market_type": "swap",
        "instrument": "BTC-USDT-SWAP",
        "pair_key": "indodax:btc_idr->okx:btc-usdt-swap",
        "side": "sell",
        "order_type": "limit",
        "quantity": "0.002",
        "price": "65000",
        "reserved_base_qty": "0.002",
        "reserved_notional": "130",
        "reservation_asset": "USDT",
        "actor_id": "intent-test",
    }
    values.update(overrides)
    return values


@pytest.mark.asyncio
async def test_create_intent_is_idempotent_and_rejects_parameter_collision(postgres_pool):
    service = OrderIntentService(postgres_pool)
    suffix = uuid.uuid4().hex
    first = await service.prepare_create(**_create_args(suffix))
    retry = await service.prepare_create(**_create_args(suffix))
    assert first["intent_id"] == retry["intent_id"]
    assert first["client_order_id"] == retry["client_order_id"]
    assert first["request_hash"] == retry["request_hash"]
    assert first["status"] == "PREPARED"

    with pytest.raises(OrderIntentConflict, match="different mutation request"):
        await service.prepare_create(**_create_args(suffix, quantity="0.003"))

    async with postgres_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM public.executor_mutation_intents WHERE intent_id=$1",
            first["intent_id"],
        )
        events = await conn.fetchval(
            "SELECT count(*) FROM public.executor_mutation_intent_events WHERE intent_id=$1",
            first["intent_id"],
        )
    assert count == 1
    assert events == 1


@pytest.mark.asyncio
async def test_concurrent_create_reservations_are_serialized(postgres_pool):
    service = OrderIntentService(postgres_pool)
    suffix = uuid.uuid4().hex
    base = _create_args(
        suffix,
        reserved_base_qty="0.6",
        reserved_notional="60",
        max_reserved_base_qty="1.0",
        max_reserved_notional="100",
    )
    first_args = {**base, "idempotency_key": "intent-a-" + suffix}
    second_args = {**base, "idempotency_key": "intent-b-" + suffix}
    results = await asyncio.gather(
        service.prepare_create(**first_args),
        service.prepare_create(**second_args),
        return_exceptions=True,
    )
    successes = [item for item in results if isinstance(item, dict)]
    failures = [item for item in results if isinstance(item, Exception)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], OrderIntentReservationConflict)


@pytest.mark.asyncio
async def test_notional_reservation_budgets_do_not_mix_quote_assets(postgres_pool):
    service = OrderIntentService(postgres_pool)
    suffix = uuid.uuid4().hex
    first = await service.prepare_create(**_create_args(
        suffix, idempotency_key="usdt-" + suffix,
        reserved_base_qty="0.1", reserved_notional="80",
        reservation_asset="USDT", max_reserved_base_qty="2",
        max_reserved_notional="100",
    ))
    second = await service.prepare_create(**_create_args(
        suffix, idempotency_key="usdc-" + suffix,
        reserved_base_qty="0.1", reserved_notional="80",
        reservation_asset="USDC", max_reserved_base_qty="2",
        max_reserved_notional="100",
    ))
    assert first["intent_id"] != second["intent_id"]


@pytest.mark.asyncio
async def test_cancel_intent_requires_terminal_fetch_and_fill_reconciliation(postgres_pool):
    service = OrderIntentService(postgres_pool)
    suffix = uuid.uuid4().hex
    intent = await service.prepare_cancel(
        account_scope_id="cancel-account-" + suffix,
        idempotency_key="cancel-order-" + suffix,
        venue="indodax", market_type="spot", instrument="BTC_IDR",
        target_order_id="venue-order-" + suffix,
        actor_id="cancel-test",
    )
    owner = OwnershipLeaseService(owner_id="cancel-worker-" + suffix)
    async with postgres_pool.acquire() as conn:
        lease = await owner.acquire(
            conn, account_scope_id=intent["account_scope_id"],
            venue="indodax", market_type="spot", instrument="BTC_IDR",
        )

    submitting = await service.mark_submitting(
        intent["intent_id"], lease=lease, actor_id="cancel-test",
        risk_snapshot={"action_class": "CANCEL", "allowed": True},
    )
    assert submitting["status"] == "SUBMITTING"
    assert submitting["owner_id"] == lease.owner_id
    assert submitting["risk_control_snapshot"] == {"action_class": "CANCEL", "allowed": True}
    replay = await service.mark_submitting(
        intent["intent_id"], lease=lease, actor_id="cancel-test",
        risk_snapshot={"action_class": "CANCEL", "allowed": True},
    )
    assert replay["attempt_count"] == 1

    acknowledged = await service.mark_acknowledged(
        intent["intent_id"], lease=lease, actor_id="cancel-test",
        exchange_order_id="venue-order-" + suffix,
        venue_response={"id": "venue-order-" + suffix, "status": "open"},
    )
    assert acknowledged["status"] == "ACKNOWLEDGED"

    with pytest.raises(OrderIntentError, match="fresh order state"):
        await service.mark_cancel_confirmed(
            intent["intent_id"], lease=lease, actor_id="cancel-test",
            final_status="canceled", fresh_fetch=False, fills_reconciled=True,
        )
    with pytest.raises(OrderIntentError, match="reconciled racing fills"):
        await service.mark_cancel_confirmed(
            intent["intent_id"], lease=lease, actor_id="cancel-test",
            final_status="canceled", fresh_fetch=True, fills_reconciled=False,
        )

    confirmed = await service.mark_cancel_confirmed(
        intent["intent_id"], lease=lease, actor_id="cancel-test",
        final_status="canceled", fresh_fetch=True, fills_reconciled=True,
    )
    assert confirmed["status"] == "CANCEL_CONFIRMED"
    assert confirmed["reserved_base_qty"] == 0
    assert confirmed["reserved_notional"] == 0
    async with postgres_pool.acquire() as conn:
        event_count = await conn.fetchval(
            "SELECT count(*) FROM public.executor_mutation_intent_events WHERE intent_id=$1",
            intent["intent_id"],
        )
        await owner.release(conn, lease)
    assert event_count == 4


@pytest.mark.asyncio
async def test_stale_mutation_owner_must_reclaim_before_reconciliation(postgres_pool):
    service = OrderIntentService(postgres_pool)
    suffix = uuid.uuid4().hex
    intent = await service.prepare_create(**_create_args(suffix))
    old_owner = OwnershipLeaseService(owner_id="old-intent-worker-" + suffix)
    async with postgres_pool.acquire() as conn:
        old_lease = await old_owner.acquire(
            conn, account_scope_id=intent["account_scope_id"],
            venue="okx", market_type="swap", instrument="BTC-USDT-SWAP",
        )
    await service.mark_submitting(
        intent["intent_id"], lease=old_lease, actor_id="old-worker",
        risk_snapshot={"action_class": "ENTRY", "allowed": True},
    )

    new_owner = OwnershipLeaseService(owner_id="new-intent-worker-" + suffix)
    async with postgres_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.executor_ownership_leases
            SET lease_expires_at=clock_timestamp() - interval '1 second'
            WHERE scope_key=$1
            """,
            old_lease.scope_key,
        )
        new_lease = await new_owner.acquire(
            conn, account_scope_id=intent["account_scope_id"],
            venue="okx", market_type="swap", instrument="BTC-USDT-SWAP",
        )

    with pytest.raises(OrderIntentStaleOwner):
        await service.mark_unknown(
            intent["intent_id"], lease=old_lease,
            actor_id="old-worker", error="old worker timeout",
        )
    reclaimed = await service.reclaim_for_reconciliation(
        intent["intent_id"], lease=new_lease, actor_id="recovery-worker",
    )
    assert reclaimed["owner_id"] == new_lease.owner_id
    assert reclaimed["owner_generation"] == new_lease.generation
    assert reclaimed["status"] == "SUBMITTING"

    unknown = await service.mark_unknown(
        intent["intent_id"], lease=new_lease,
        actor_id="recovery-worker", error="venue outcome still unknown",
    )
    assert unknown["status"] == "UNKNOWN"
    with pytest.raises(OrderIntentStaleOwner):
        await service.mark_acknowledged(
            intent["intent_id"], lease=old_lease, actor_id="old-worker",
            exchange_order_id="unexpected-order", venue_response={"id": "unexpected-order"},
        )
    async with postgres_pool.acquire() as conn:
        await new_owner.release(conn, new_lease)
