"""PostgreSQL integration coverage for durable hedge scheduling."""
from __future__ import annotations

import os
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from executor.execution_fill_service import ExecutionFillService, FillConflict
from executor.execution_hedge_obligations import ExecutionHedgeObligationService
from executor.execution_hedge_common import HedgeObligationError
from executor.hedge_liquidity_evaluator import (
    BookLevel, FXSnapshot, HedgeLiquidityPolicy, OrderBookSnapshot,
    evaluate_hedge_liquidity,
)
from executor.hedge_scheduler import HedgeScheduler
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
        side="buy", quantity="0.0002", price="1000000000",
        quote_asset="IDR", quote_quantity="200000", fee_amount="200",
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
        required_base_quantity="0.0002", already_reserved_base_quantity="0",
        base_per_unit="0.001", quantity_step="1", venue_min_order_quantity="1",
        venue_min_notional_quote="5", position_side="SHORT",
        book=book, fx=fx, policy=policy, now_ms=now_ms,
        unhedged_since_ms=unhedged_since_ms, unhedged_notional_usd="20",
    )
    assert decision.action == "DEFER"
    scheduler = HedgeScheduler(postgres_pool)
    forged_oversize = replace(
        decision, action="SUBMIT",
        executable_base_quantity=Decimal("0.0003"),
    )
    with pytest.raises(HedgeObligationError, match="exact unreserved obligation residual"):
        await scheduler.record_decision(
            session_id=admitted["session_id"], obligation_id=obligation["obligation_id"],
            decision=forged_oversize, actor_id="scheduler-integration-test",
            idempotency_key="forged-oversize-" + suffix,
            coordinator_lease=lease, fencing_generation=lease.generation,
        )
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
    with pytest.raises(FillConflict, match="idempotency key was reused with changed decision"):
        await scheduler.record_decision(
            session_id=admitted["session_id"], obligation_id=obligation["obligation_id"],
            decision=replace(decision, reason="changed decision"),
            actor_id="scheduler-integration-test", idempotency_key="defer-" + suffix,
            coordinator_lease=lease, fencing_generation=lease.generation,
        )

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
