"""V3 durable fill service unit and PostgreSQL integration tests."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from executor.execution_fill_service import (
    ExecutionFillService, FillConflict, FillValidationError, _decimal, _timestamp,
)
from executor.fill_event_processor import FillEventProcessor
from executor.execution_hedge_obligations import ExecutionHedgeObligationService
from executor.ownership import OwnershipLeaseService
from executor.session_coordinator import SessionCoordinator


def _snapshots():
    now = datetime.now(timezone.utc)
    evidence = {
        name: {
            "status": "VERIFIED" if name != "fx_conversion" else "NOT_REQUIRED",
            "observed_at": now.isoformat(),
            "valid_until": (now + timedelta(minutes=5)).isoformat(),
        }
        for name in ("market_metadata", "fee_schedule", "fx_conversion")
    }
    return {
        "policy_snapshot": {"strategy": "spot-perp", "version": 3, "evidence": evidence},
        "execution_policy_snapshot": {"mode": "maker-taker"},
        "risk_policy_snapshot": {"max_unhedged_ms": 3000},
        "exit_policy_snapshot": {"take_profit": {"net_roi_pct": 0.3}},
    }


def _legs(suffix):
    return [
        {
            "leg_id": f"spot-{suffix}", "leg_type": "SPOT", "venue": "indodax",
            "market_type": "spot", "symbol": "btc_idr", "normalized_symbol": "BTC/IDR",
            "base_asset": "BTC", "quote_asset": "IDR", "quantity_unit": "BASE",
            "base_per_unit": "1", "order_quantity": "0.001",
            "exposure_quantity": "0.001", "target_qty": "0.001",
        },
        {
            "leg_id": f"future-{suffix}", "leg_type": "FUTURES", "venue": "okx",
            "market_type": "swap", "symbol": "BTC-USDT-SWAP",
            "normalized_symbol": "BTC/USDT:USDT", "base_asset": "BTC",
            "quote_asset": "USDT", "settlement_asset": "USDT",
            "contract_type": "LINEAR_PERPETUAL", "contract_size": "0.001",
            "position_side": "SHORT", "quantity_unit": "CONTRACTS",
            "base_per_unit": "0.001", "order_quantity": "1",
            "exposure_quantity": "0.001", "target_qty": "1",
        },
    ]


def test_fill_decimal_and_timestamp_validation_is_strict():
    assert _decimal("0.00000001", "quantity") == Decimal("0.00000001")
    with pytest.raises(FillValidationError, match="finite decimal"):
        _decimal("NaN", "quantity")
    with pytest.raises(FillValidationError, match="positive"):
        _decimal("0", "price")
    with pytest.raises(FillValidationError, match="timezone-aware"):
        _timestamp(datetime.now())


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


async def _admitted_session(pool, suffix):
    account = "fill-account-" + suffix
    owner = OwnershipLeaseService(owner_id="fill-worker-" + suffix)
    coordinator = SessionCoordinator(pool=pool, ownership=owner)
    admitted = await coordinator.admit(
        opportunity_id="fill-opportunity-" + suffix,
        source_dispatch_id="fill-dispatch-" + suffix,
        account_scope_id=account,
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        legs=_legs(suffix), actor_id="fill-integration-test", **_snapshots(),
    )
    lease = admitted["coordinator_lease"]
    session_id = admitted["session_id"]
    state_version, aggregate = 1, 3
    for state in ("VALIDATING", "ADMITTED", "ENTRY_IN_PROGRESS"):
        result = await coordinator.transition(
            coordinator_lease=lease, session_id=session_id, target_state=state,
            expected_state_version=state_version, expected_aggregate_version=aggregate,
            fencing_generation=lease.generation, event_type="SESSION_" + state,
            event_idempotency_key=f"{admitted['admission_key']}:{state.lower()}",
            actor_id="fill-integration-test",
        )
        state_version += 1
        aggregate = result["aggregate_version"]
    leg_version = 1
    for state in ("PREPARED", "SUBMITTING"):
        result = await coordinator.transition_leg(
            coordinator_lease=lease, session_id=session_id, leg_id=f"spot-{suffix}",
            target_state=state, expected_leg_state_version=leg_version,
            expected_aggregate_version=aggregate, fencing_generation=lease.generation,
            event_type="SPOT_LEG_" + state,
            event_idempotency_key=f"{admitted['admission_key']}:spot:{state.lower()}",
            actor_id="fill-integration-test",
        )
        leg_version += 1
        aggregate = result["aggregate_version"]
    return coordinator, admitted, leg_version, aggregate


def _fill_args(admitted, suffix, leg_version, aggregate):
    return {
        "session_id": admitted["session_id"], "leg_id": f"spot-{suffix}",
        "account_scope_id": "fill-account-" + suffix,
        "venue": "indodax", "market_type": "spot", "symbol": "btc_idr",
        "exchange_trade_id": "trade-" + suffix, "side": "buy",
        "quantity": "0.0004", "price": "1000000000", "quote_asset": "IDR",
        "quote_quantity": "400000", "fee_amount": "400",
        "fee_asset": "IDR", "fee_verified": True, "fee_source": "EXCHANGE_TRADE",
        "filled_at": datetime.now(timezone.utc), "actor_id": "fill-integration-test",
        "event_idempotency_key": "fill-event-" + suffix,
        "expected_leg_state_version": leg_version,
        "expected_aggregate_version": aggregate,
        "fencing_generation": admitted["coordinator_generation"],
        "coordinator_lease": admitted["coordinator_lease"],
        "source_event_id": "stream-event-" + suffix,
    }


@pytest.mark.asyncio
async def test_postgres_fill_is_exactly_once_and_updates_leg_atomically(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, leg_version, aggregate = await _admitted_session(postgres_pool, suffix)
    service = ExecutionFillService(postgres_pool)
    args = _fill_args(admitted, suffix, leg_version, aggregate)
    first = await service.record_fill(**args)
    assert first["duplicate"] is False
    assert first["leg_state"] == "PARTIALLY_FILLED"
    assert first["confirmed_filled_quantity"] == "0.0004"
    assert first["confirmed_quote_quantity"] == "400000"

    replay = await service.record_fill(**args)
    assert replay["duplicate"] is True
    assert replay["fill_id"] == first["fill_id"]

    async with postgres_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_fills WHERE session_id=$1",
            admitted["session_id"],
        )
        leg = await conn.fetchrow(
            """
            SELECT status, state_version, confirmed_filled_quantity,
                   confirmed_quote_quantity, confirmed_exposure_quantity,
                   average_fill_price
            FROM public.execution_legs WHERE leg_id=$1
            """,
            f"spot-{suffix}",
        )
        event_count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_session_events WHERE session_id=$1",
            admitted["session_id"],
        )
    assert count == 1
    assert leg["status"] == "PARTIALLY_FILLED"
    assert Decimal(str(leg["confirmed_filled_quantity"])) == Decimal("0.0004")
    assert Decimal(str(leg["average_fill_price"])) == Decimal("1000000000")
    assert Decimal(str(leg["confirmed_exposure_quantity"])) == Decimal("0.0004")
    assert event_count == 9


@pytest.mark.asyncio
async def test_postgres_fill_trade_identity_conflict_is_rejected(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, leg_version, aggregate = await _admitted_session(postgres_pool, suffix)
    service = ExecutionFillService(postgres_pool)
    args = _fill_args(admitted, suffix, leg_version, aggregate)
    await service.record_fill(**args)
    changed = {**args, "quantity": "0.0005", "quote_quantity": "500000"}
    with pytest.raises(FillConflict, match="changed immutable facts"):
        await service.record_fill(**changed)
    changed_same_trade = {**changed, "event_idempotency_key": "another-event-" + suffix}
    with pytest.raises(FillConflict, match="different immutable facts"):
        await service.record_fill(**changed_same_trade)
    changed_trade = {**args, "exchange_trade_id": "another-trade-" + suffix}
    with pytest.raises(FillConflict, match="idempotency key was reused"):
        await service.record_fill(**changed_trade)
    async with postgres_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_fills WHERE session_id=$1",
            admitted["session_id"],
        )
    assert count == 1


@pytest.mark.asyncio
async def test_postgres_fill_rejects_wrong_venue_symbol_or_account(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, leg_version, aggregate = await _admitted_session(postgres_pool, suffix)
    service = ExecutionFillService(postgres_pool)
    args = _fill_args(admitted, suffix, leg_version, aggregate)
    with pytest.raises(FillValidationError, match="venue-bound fill identity"):
        await service.record_fill(**{**args, "venue": "okx"})
    with pytest.raises(FillValidationError, match="account scope"):
        await service.record_fill(**{**args, "account_scope_id": "other-account"})


@pytest.mark.asyncio
async def test_postgres_private_event_inbox_is_durable_and_idempotent(postgres_pool):
    service = ExecutionFillService(postgres_pool)
    suffix = uuid.uuid4().hex
    args = {
        "account_scope_id": "inbox-account-" + suffix, "venue": "okx",
        "event_type": "ORDER_UPDATE", "source_event_id": "event-" + suffix,
        "session_id": "session-" + suffix, "leg_id": "leg-" + suffix,
        "payload": {"order_id": "venue-order-1", "status": "open", "filled": "0"},
    }
    first = await service.persist_inbox_event(**args)
    second = await service.persist_inbox_event(**args)
    assert first["accepted"] is True
    assert second["duplicate"] is True
    assert first["inbox_id"] == second["inbox_id"]
    assert await service.mark_inbox_event(inbox_id=first["inbox_id"], status="PROCESSING")
    assert await service.mark_inbox_event(inbox_id=first["inbox_id"], status="PROCESSED")
    assert not await service.mark_inbox_event(inbox_id=first["inbox_id"], status="RETRY")


@pytest.mark.asyncio
async def test_postgres_late_fill_after_completion_reopens_reconciliation(postgres_pool):
    suffix = uuid.uuid4().hex
    _, admitted, leg_version, aggregate = await _admitted_session(postgres_pool, suffix)
    async with postgres_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.execution_sessions
            SET status='COMPLETED', state_version=state_version+1
            WHERE session_id=$1
            """,
            admitted["session_id"],
        )
    args = _fill_args(admitted, suffix, leg_version, aggregate)
    # Re-read the aggregate cursor after the test's explicit terminal-state fixture.
    async with postgres_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state_version, aggregate_version FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
    args["expected_aggregate_version"] = int(row["aggregate_version"])
    result = await ExecutionFillService(postgres_pool).record_fill(**args)
    assert result["session_state"] == "RECONCILIATION_REQUIRED"
    assert result["leg_state"] == "RECONCILIATION_REQUIRED"
    async with postgres_pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
        leg_status = await conn.fetchval(
            "SELECT status FROM public.execution_legs WHERE leg_id=$1",
            f"spot-{suffix}",
        )
    assert status == "RECONCILIATION_REQUIRED"
    assert leg_status == "RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
async def test_postgres_futures_fill_keeps_contracts_separate_from_base_exposure(postgres_pool):
    suffix = uuid.uuid4().hex
    coordinator, admitted, _, aggregate = await _admitted_session(postgres_pool, suffix)
    lease = admitted["coordinator_lease"]
    leg_version = 1
    for state in ("PREPARED", "SUBMITTING"):
        result = await coordinator.transition_leg(
            coordinator_lease=lease, session_id=admitted["session_id"],
            leg_id=f"future-{suffix}", target_state=state,
            expected_leg_state_version=leg_version,
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation,
            event_type="FUTURES_LEG_" + state,
            event_idempotency_key=f"{admitted['admission_key']}:future:{state.lower()}",
            actor_id="fill-integration-test",
        )
        leg_version += 1
        aggregate = result["aggregate_version"]
    result = await ExecutionFillService(postgres_pool).record_fill(
        session_id=admitted["session_id"], leg_id=f"future-{suffix}",
        account_scope_id="fill-account-" + suffix,
        venue="okx", market_type="swap", symbol="BTC-USDT-SWAP",
        exchange_trade_id="future-trade-" + suffix, side="sell",
        quantity="2", price="100000", quote_asset="USDT",
        quote_quantity="200", fee_amount="0.02", fee_asset="USDT",
        fee_verified=True, fee_source="EXCHANGE_TRADE",
        filled_at=datetime.now(timezone.utc), actor_id="fill-integration-test",
        event_idempotency_key="future-fill-event-" + suffix,
        expected_leg_state_version=leg_version,
        expected_aggregate_version=aggregate,
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    assert result["confirmed_filled_quantity"] == "2"
    assert result["confirmed_exposure_quantity"] == "0.002"
    assert result["average_fill_price"] == "100000"


@pytest.mark.asyncio
async def test_fill_inbox_creates_spot_obligation_then_allocates_reserved_futures_fill(postgres_pool):
    suffix = uuid.uuid4().hex
    coordinator, admitted, spot_leg_version, aggregate = await _admitted_session(postgres_pool, suffix)
    lease = admitted["coordinator_lease"]
    account = "fill-account-" + suffix
    fills = ExecutionFillService(postgres_pool)
    processor = FillEventProcessor(postgres_pool)

    spot_inbox = await fills.persist_inbox_event(
        account_scope_id=account, venue="indodax", event_type="FILL",
        source_event_id="spot-source-" + suffix,
        session_id=admitted["session_id"], leg_id=f"spot-{suffix}",
        payload={
            "exchange_trade_id": "spot-trade-" + suffix,
            "side": "buy", "quantity": "0.001", "price": "1000000000",
            "quote_asset": "IDR", "quote_quantity": "1000000",
            "fee_amount": "200", "fee_asset": "IDR",
            "fee_verified": True, "fee_source": "EXCHANGE_TRADE",
            "filled_at": datetime.now(timezone.utc).isoformat(),
            "client_order_id": "spot-client-" + suffix,
        },
    )
    spot_result = await processor.process_inbox_event(
        inbox_id=spot_inbox["inbox_id"], coordinator_lease=lease,
        fencing_generation=lease.generation,
        actor_id="fill-event-processor-test", hedge_ratio="1",
    )
    assert spot_result["processed"] is True
    obligation_id = spot_result["obligation"]["obligation_id"]

    hedge_service = ExecutionHedgeObligationService(postgres_pool)
    reservation = await hedge_service.reserve(
        session_id=admitted["session_id"], futures_venue="okx",
        futures_market_type="swap", futures_symbol="BTC-USDT-SWAP",
        base_asset="BTC", position_side="SHORT",
        contract_type="LINEAR_PERPETUAL", contract_size="0.001",
        quantity_unit="CONTRACTS", base_per_unit="0.001",
        requested_base_quantity="0.001", minimum_base_quantity="0.001",
        idempotency_key="fill-event-reservation-" + suffix,
        actor_id="fill-event-processor-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    client_order_id = "hedge-client-" + suffix
    await hedge_service.mark_submitted(
        session_id=admitted["session_id"],
        reservation_id=reservation["reservation_id"],
        client_order_id=client_order_id,
        actor_id="fill-event-processor-test",
        event_idempotency_key="fill-event-reservation-submit-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
    )

    async with postgres_pool.acquire() as conn:
        current = await conn.fetchrow(
            """
            SELECT s.aggregate_version, l.state_version
            FROM public.execution_sessions s
            JOIN public.execution_legs l ON l.session_id=s.session_id
            WHERE s.session_id=$1 AND l.leg_id=$2
            """,
            admitted["session_id"], f"future-{suffix}",
        )
    aggregate = int(current["aggregate_version"])
    future_version = int(current["state_version"])
    for target in ("PREPARED", "SUBMITTING"):
        transitioned = await coordinator.transition_leg(
            coordinator_lease=lease, session_id=admitted["session_id"],
            leg_id=f"future-{suffix}", target_state=target,
            expected_leg_state_version=future_version,
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation,
            event_type="FUTURES_LEG_" + target,
            event_idempotency_key=f"future-leg-{target}-" + suffix,
            actor_id="fill-event-processor-test",
        )
        future_version += 1
        aggregate = int(transitioned["aggregate_version"])

    futures_inbox = await fills.persist_inbox_event(
        account_scope_id=account, venue="okx", event_type="FILL",
        source_event_id="future-source-" + suffix,
        session_id=admitted["session_id"], leg_id=f"future-{suffix}",
        payload={
            "exchange_trade_id": "future-trade-" + suffix,
            "side": "sell", "quantity": "1", "price": "100000",
            "quote_asset": "USDT", "quote_quantity": "100",
            "fee_amount": "0", "fee_asset": None,
            "fee_verified": False, "fee_source": "UNVERIFIED",
            "filled_at": datetime.now(timezone.utc).isoformat(),
            "client_order_id": client_order_id,
        },
    )
    futures_result = await processor.process_inbox_event(
        inbox_id=futures_inbox["inbox_id"], coordinator_lease=lease,
        fencing_generation=lease.generation,
        actor_id="fill-event-processor-test", hedge_ratio="1",
    )
    assert futures_result["processed"] is True
    assert futures_result["allocation"]["allocated_base_quantity"] == "0.001"

    async with postgres_pool.acquire() as conn:
        obligation = await conn.fetchrow(
            """
            SELECT target_base_quantity, confirmed_base_quantity,
                   remaining_base_quantity, status
            FROM public.execution_hedge_obligations
            WHERE obligation_id=$1::uuid
            """,
            obligation_id,
        )
        reservation_row = await conn.fetchrow(
            "SELECT status, allocated_base_quantity FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid",
            reservation["reservation_id"],
        )
        session_state = await conn.fetchval(
            "SELECT status FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
        inbox_states = await conn.fetch(
            """
            SELECT processing_status FROM public.execution_event_inbox
            WHERE inbox_id=ANY($1::uuid[])
            ORDER BY source_event_id
            """,
            [spot_inbox["inbox_id"], futures_inbox["inbox_id"]],
        )
    assert Decimal(str(obligation["target_base_quantity"])) == Decimal("0.001")
    assert Decimal(str(obligation["confirmed_base_quantity"])) == Decimal("0.001")
    assert Decimal(str(obligation["remaining_base_quantity"])) == Decimal("0")
    assert obligation["status"] == "HEDGED"
    assert reservation_row["status"] == "CONSUMED"
    assert Decimal(str(reservation_row["allocated_base_quantity"])) == Decimal("0.001")
    assert session_state == "PARTIALLY_HEDGED"
    assert [row["processing_status"] for row in inbox_states] == ["PROCESSED", "PROCESSED"]


@pytest.mark.asyncio
async def test_fill_inbox_claim_is_single_flight_and_expired_processing_lease_is_reclaimed(postgres_pool):
    suffix = uuid.uuid4().hex
    service = ExecutionFillService(postgres_pool)
    accepted = await service.persist_inbox_event(
        account_scope_id="inbox-lease-account-" + suffix,
        venue="okx", event_type="FILL",
        source_event_id="inbox-lease-source-" + suffix,
        payload={"exchange_trade_id": "trade-" + suffix},
    )
    first = await service.claim_inbox_event(inbox_id=accepted["inbox_id"])
    assert first["processing_status"] == "PROCESSING"
    assert first["attempts"] == 1
    assert await service.claim_inbox_event(inbox_id=accepted["inbox_id"]) is None

    async with postgres_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE public.execution_event_inbox
            SET processing_started_at=clock_timestamp() - interval '5 minutes'
            WHERE inbox_id=$1::uuid
            """,
            accepted["inbox_id"],
        )
    reclaimed = await service.claim_inbox_event(
        inbox_id=accepted["inbox_id"], processing_lease_seconds=60,
    )
    assert reclaimed["processing_status"] == "PROCESSING"
    assert reclaimed["attempts"] == 2
    assert await service.mark_inbox_event(
        inbox_id=accepted["inbox_id"], status="PROCESSED",
    ) is True
