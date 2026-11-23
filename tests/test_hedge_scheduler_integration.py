"""PostgreSQL integration coverage for durable hedge scheduling."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from dataclasses import replace

import pytest

from executor.execution_fill_service import ExecutionFillService
from executor.execution_hedge_obligations import ExecutionHedgeObligationService
from executor.hedge_liquidity_evaluator import (
    BookLevel, FXSnapshot, HedgeLiquidityDecision, HedgeLiquidityPolicy, OrderBookSnapshot,
    evaluate_hedge_liquidity,
)
from executor.hedge_scheduler import HedgeScheduler
from executor.execution_hedge_common import HedgeObligationError
from executor.ownership import OwnershipLeaseService
from executor.session_coordinator import SessionCoordinator


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
        "risk_policy_snapshot": {"max_unhedged_ms": 5000},
        "exit_policy_snapshot": {"take_profit": {"net_roi_pct": 0.3}},
    }


async def _make_obligation(pool, suffix):
    account = "scheduler-account-" + suffix
    coordinator = SessionCoordinator(
        pool=pool,
        ownership=OwnershipLeaseService(owner_id="scheduler-worker-" + suffix),
    )
    legs = [
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
    admitted = await coordinator.admit(
        opportunity_id="scheduler-opp-" + suffix,
        source_dispatch_id="scheduler-dispatch-" + suffix,
        account_scope_id=account,
        pair_key="indodax:btc_idr->okx:btc-usdt-swap",
        legs=legs, actor_id="scheduler-integration-test", **_snapshots(),
    )
    lease = admitted["coordinator_lease"]
    aggregate, state_version = 3, 1
    for state in ("VALIDATING", "ADMITTED", "ENTRY_IN_PROGRESS"):
        result = await coordinator.transition(
            coordinator_lease=lease, session_id=admitted["session_id"],
            target_state=state, expected_state_version=state_version,
            expected_aggregate_version=aggregate,
            fencing_generation=lease.generation, event_type="SESSION_" + state,
            event_idempotency_key=f"{admitted['admission_key']}:{state.lower()}",
            actor_id="scheduler-integration-test",
        )
        state_version += 1
        aggregate = result["aggregate_version"]
    for leg_id, kind in ((f"spot-{suffix}", "SPOT"), (f"future-{suffix}", "FUTURES")):
        version = 1
        for state in ("PREPARED", "SUBMITTING"):
            result = await coordinator.transition_leg(
                coordinator_lease=lease, session_id=admitted["session_id"],
                leg_id=leg_id, target_state=state,
                expected_leg_state_version=version,
                expected_aggregate_version=aggregate,
                fencing_generation=lease.generation,
                event_type=f"{kind}_LEG_{state}",
                event_idempotency_key=f"{admitted['admission_key']}:{kind}:{state}",
                actor_id="scheduler-integration-test",
            )
            version += 1
            aggregate = result["aggregate_version"]
    fill = await ExecutionFillService(pool).record_fill(
        session_id=admitted["session_id"], leg_id=f"spot-{suffix}",
        account_scope_id=account, venue="indodax", market_type="spot",
        symbol="btc_idr", exchange_trade_id="scheduler-spot-" + suffix,
        side="buy", quantity="0.001", price="1000000000",
        quote_asset="IDR", quote_quantity="1000000", fee_amount="200",
        fee_asset="IDR", fee_verified=True, fee_source="EXCHANGE_TRADE",
        filled_at=datetime.now(timezone.utc), actor_id="scheduler-integration-test",
        event_idempotency_key="scheduler-fill-" + suffix,
        expected_leg_state_version=3, expected_aggregate_version=aggregate,
        fencing_generation=lease.generation, coordinator_lease=lease,
    )
    obligation = await ExecutionHedgeObligationService(pool).create_from_spot_fill(
        session_id=admitted["session_id"], spot_fill_id=fill["fill_id"],
        hedge_ratio="1", actor_id="scheduler-integration-test",
        coordinator_lease=lease, fencing_generation=lease.generation,
    )
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT created_at FROM public.execution_hedge_obligations WHERE obligation_id=$1::uuid",
            obligation["obligation_id"],
        )
    return admitted, lease, obligation, row["created_at"]


def _policy():
    return HedgeLiquidityPolicy(
        internal_min_notional_usd=Decimal("5"), max_book_age_ms=500,
        max_fx_age_ms=1000, max_slippage_bps=Decimal("25"),
        taker_fee_bps=Decimal("5"), safety_buffer_bps=Decimal("5"),
        max_unhedged_notional_usd=Decimal("25"), max_unhedged_wait_ms=5000,
        max_adverse_move_bps=Decimal("100"),
    )


@pytest.mark.asyncio
async def test_postgres_scheduler_persists_deferral_and_wait_expiry(postgres_pool):
    suffix = uuid.uuid4().hex
    admitted, lease, obligation, created_at = await _make_obligation(postgres_pool, suffix)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    unhedged_since_ms = int(created_at.timestamp() * 1000)
    book = OrderBookSnapshot(
        bids=(BookLevel.make("100", "100"),),
        asks=(BookLevel.make("100.1", "100"),),
        observed_at_ms=now_ms - 2000, sequence="book-101",
    )
    fx = FXSnapshot(Decimal("1"), now_ms, "USDT/USD")
    policy = _policy()
    decision = evaluate_hedge_liquidity(
        required_base_quantity="0.001", already_reserved_base_quantity="0",
        base_per_unit="0.001", quantity_step="1", venue_min_order_quantity="1",
        venue_min_notional_quote="5", position_side="SHORT",
        book=book, fx=fx, policy=policy, now_ms=now_ms,
        unhedged_since_ms=unhedged_since_ms, unhedged_notional_usd="20",
    )
    assert decision.action == "DEFER"
    scheduler = HedgeScheduler(postgres_pool)
    first = await scheduler.record_decision(
        session_id=admitted["session_id"], obligation_id=obligation["obligation_id"],
        decision=decision, actor_id="scheduler-integration-test",
        idempotency_key="defer-" + suffix, coordinator_lease=lease,
        fencing_generation=lease.generation,
    )
    assert first["action"] == "DEFER"
    replay = await scheduler.record_decision(
        session_id=admitted["session_id"], obligation_id=obligation["obligation_id"],
        decision=decision, actor_id="scheduler-integration-test",
        idempotency_key="defer-" + suffix, coordinator_lease=lease,
        fencing_generation=lease.generation,
    )
    assert replay["duplicate"] is True

    async with postgres_pool.acquire() as conn:
        deferred = await conn.fetchrow(
            """
            SELECT deferred_reason, deferred_since, next_evaluate_at,
                   deferred_count, de_risk_required, max_wait_until
            FROM public.execution_hedge_obligations WHERE obligation_id=$1::uuid
            """,
            obligation["obligation_id"],
        )
        event_count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_hedge_scheduler_events WHERE obligation_id=$1::uuid",
            obligation["obligation_id"],
        )
    assert deferred["deferred_reason"] == "BOOK_STALE"
    assert deferred["next_evaluate_at"] is not None
    assert deferred["max_wait_until"] is not None
    assert deferred["deferred_count"] == 1
    assert deferred["de_risk_required"] is False
    assert event_count == 1

    expired_now = unhedged_since_ms + 6000
    expired = evaluate_hedge_liquidity(
        required_base_quantity="0.0002", already_reserved_base_quantity="0",
        base_per_unit="0.001", quantity_step="1", venue_min_order_quantity="1",
        venue_min_notional_quote="5", position_side="SHORT",
        book=book, fx=fx, policy=policy, now_ms=expired_now,
        unhedged_since_ms=unhedged_since_ms, unhedged_notional_usd="20",
    )
    assert expired.action == "DE_RISK_REQUIRED"
    blocked = await scheduler.record_decision(
        session_id=admitted["session_id"], obligation_id=obligation["obligation_id"],
        decision=expired, actor_id="scheduler-integration-test",
        idempotency_key="derisk-" + suffix, coordinator_lease=lease,
        fencing_generation=lease.generation,
    )
    assert blocked["obligation_status"] == "BLOCKED"
    async with postgres_pool.acquire() as conn:
        state = await conn.fetchrow(
            "SELECT status, de_risk_required, next_evaluate_at FROM public.execution_hedge_obligations WHERE obligation_id=$1::uuid",
            obligation["obligation_id"],
        )
    assert state["status"] == "BLOCKED"
    assert state["de_risk_required"] is True
    assert state["next_evaluate_at"] is None
    async with postgres_pool.acquire() as conn:
        session_status = await conn.fetchval(
            "SELECT status FROM public.execution_sessions WHERE session_id=$1",
            admitted["session_id"],
        )
    assert session_status == "BLOCKED"


@pytest.mark.asyncio
async def test_scheduler_rejects_forged_submit_quantity_above_durable_obligation(postgres_pool):
    suffix = uuid.uuid4().hex
    admitted, lease, obligation, created_at = await _make_obligation(postgres_pool, suffix)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    valid_defer = evaluate_hedge_liquidity(
        required_base_quantity="0.0002", already_reserved_base_quantity="0",
        base_per_unit="0.001", quantity_step="1", venue_min_order_quantity="1",
        venue_min_notional_quote="5", position_side="SHORT",
        book=OrderBookSnapshot(
            bids=(BookLevel.make("100", "100"),),
            asks=(BookLevel.make("100.1", "100"),),
            observed_at_ms=now_ms, sequence="book-forged-test",
        ),
        fx=FXSnapshot(Decimal("1"), now_ms, "USDT/USD"),
        policy=_policy(), now_ms=now_ms,
        unhedged_since_ms=int(created_at.timestamp() * 1000),
        unhedged_notional_usd="20",
    )
    forged = replace(
        valid_defer, action="SUBMIT", reason="forged",
        executable_base_quantity=Decimal("0.002"),
        executable_native_quantity=Decimal("2"),
        executable_vwap=Decimal("100"),
        executable_notional_quote=Decimal("100"),
        executable_notional_usd=Decimal("100"),
        slippage_bps=Decimal("0"),
        estimated_total_cost_bps=Decimal("10"),
        book_age_ms=0,
    )
    with pytest.raises(HedgeObligationError, match="exceeds the unreserved hedge obligation"):
        await HedgeScheduler(postgres_pool).record_decision(
            session_id=admitted["session_id"], obligation_id=obligation["obligation_id"],
            decision=forged, actor_id="scheduler-integration-test",
            idempotency_key="forged-submit-" + suffix,
            coordinator_lease=lease, fencing_generation=lease.generation,
        )
    async with postgres_pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM public.execution_hedge_scheduler_events WHERE obligation_id=$1::uuid",
            obligation["obligation_id"],
        )
    assert count == 0


@pytest.mark.asyncio
async def test_obligation_backed_hedge_reaches_gateway_with_exact_reservation(postgres_pool):
    from executor.global_stop import GlobalStopService
    from executor.mutation_gateway import MutationGateway, MutationResultType
    from executor.order_intent_service import OrderIntentService
    from executor.hedge_execution_coordinator import HedgeExecutionCoordinator

    suffix = uuid.uuid4().hex
    admitted, lease, obligation, created_at = await _make_obligation(postgres_pool, suffix)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    decision = evaluate_hedge_liquidity(
        required_base_quantity="0.001", already_reserved_base_quantity="0",
        base_per_unit="0.001", quantity_step="1", venue_min_order_quantity="1",
        venue_min_notional_quote="5", position_side="SHORT",
        book=OrderBookSnapshot(
            bids=(BookLevel.make("100000", "10"),),
            asks=(BookLevel.make("100001", "10"),),
            observed_at_ms=now_ms - 100, sequence="hedge-book-" + suffix,
        ),
        fx=FXSnapshot(Decimal("1"), now_ms - 50, "USDT/USD"),
        policy=HedgeLiquidityPolicy(
            internal_min_notional_usd=Decimal("5"), max_book_age_ms=500,
            max_fx_age_ms=1000, max_slippage_bps=Decimal("25"),
            max_total_cost_bps=Decimal("50"), taker_fee_bps=Decimal("5"),
            safety_buffer_bps=Decimal("5"),
            max_unhedged_notional_usd=Decimal("250"),
            max_unhedged_wait_ms=5000, max_adverse_move_bps=Decimal("100"),
        ),
        now_ms=now_ms, unhedged_since_ms=int(created_at.timestamp() * 1000),
        unhedged_notional_usd="100", reference_price="100000",
    )
    assert decision.action == "SUBMIT"

    global_stop = GlobalStopService("unused", require_scoped_submits=True)
    global_stop.pool = postgres_pool
    intents = OrderIntentService(postgres_pool)
    gateway = MutationGateway(global_stop=global_stop, order_intent_service=intents)

    class Venue:
        id = "okx"
        def __init__(self):
            self.calls = []
        async def create_order_direct(self, **kwargs):
            self.calls.append(kwargs)
            return {"id": "hedge-order-" + suffix, "status": "open", "filled": "0"}

    venue = Venue()
    coordinator = HedgeExecutionCoordinator(
        postgres_pool, gateway=gateway, order_intents=intents,
    )
    risk_metrics = {
        "gross_exposure_usdt": 100, "net_exposure_usdt": 0,
        "deployed_capital_usdt": 100, "unhedged_notional_usdt": 100,
        "unhedged_duration_ms": 100, "open_order_count": 0,
        "daily_loss_usdt": 0, "drawdown_pct": 0,
        "slippage_bps": 0, "funding_cost_pct": 0,
        "market_data_age_ms": 100, "venue_health_score": 1,
    }
    result = await coordinator.execute_decision(
        client=venue, session_id=admitted["session_id"],
        obligation_id=obligation["obligation_id"], decision=decision,
        actor_id="hedge-integration-test", idempotency_key="hedge-submit-" + suffix,
        coordinator_lease=lease, fencing_generation=lease.generation,
        risk_metrics=risk_metrics,
        risk_limits={"max_unhedged_notional_usdt": 250},
    )
    assert result["mutation"].status == MutationResultType.CONFIRMED
    assert result["submitted"] is True
    assert len(venue.calls) == 1
    assert venue.calls[0]["side"] == "sell"
    assert venue.calls[0]["amount"] == 1

    async with postgres_pool.acquire() as conn:
        reservation = await conn.fetchrow(
            """
            SELECT status, client_order_id, reserved_base_quantity
            FROM public.execution_hedge_reservations
            WHERE reservation_id=$1::uuid
            """,
            result["reservation"]["reservation_id"],
        )
        leg = await conn.fetchrow(
            "SELECT status FROM public.execution_legs WHERE session_id=$1 AND leg_id=$2",
            admitted["session_id"], f"future-{suffix}",
        )
        event = await conn.fetchrow(
            """
            SELECT event_type FROM public.execution_session_events
            WHERE session_id=$1 AND event_idempotency_key=$2
            """,
            admitted["session_id"],
            "mutation-submit:" + result["intent"]["intent_id"],
        )
        assert reservation["status"] == "SUBMITTED"
        assert reservation["client_order_id"] == result["intent"]["client_order_id"]
        assert Decimal(str(reservation["reserved_base_quantity"])) == Decimal("0.001")
        assert leg["status"] == "SUBMITTING"
        assert event["event_type"] == "HEDGE_LEG_SUBMISSION_STARTED"
        await global_stop.ownership.release_all(conn)
    await admitted["coordinator"].release(lease) if "coordinator" in admitted else None
