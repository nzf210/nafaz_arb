"""Unit and component tests for Executor Architecture V2.

Covers:
- OrderSlotManager (atomic per-CEX spot slots)
- MutationGateway (pre-submit checks, client-id, timeouts -> SUBMISSION_UNKNOWN)
- LegManagers & TwoLegCoordinator (independent lifecycles, drift monitoring)
- PairPnLEngine & ExitDecisionEngine (combined two-leg net liquidation PnL)
- PostgresRiskStateService (direct PostgreSQL risk persistence)
"""
from __future__ import annotations

import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock

from executor.order_slots import OrderSlotManager, SlotExhaustedError
from executor.mutation_gateway import MutationGateway, MutationResultType
from executor.leg_managers import (
    SpotLegManager, FuturesLegManager, TwoLegCoordinator,
    LegStatus, HedgeDriftStatus,
)
from executor.pair_pnl_engine import PairPnLEngine, PairPnLSnapshot
from executor.exit_decision_engine import ExitDecisionEngine, ExitAction
from executor.postgres_risk import PostgresRiskStateService


# =====================================================================
# 1. OrderSlotManager Tests
# =====================================================================

@pytest.mark.asyncio
async def test_order_slot_reservation_cap_and_release():
    manager = OrderSlotManager()

    # Reserve up to default cap (2)
    s0 = await manager.reserve_slot("tokocrypto", client_order_id="cid-1")
    assert s0 == 0
    s1 = await manager.reserve_slot("tokocrypto", client_order_id="cid-2")
    assert s1 == 1

    # 3rd reservation must raise SlotExhaustedError
    with pytest.raises(SlotExhaustedError):
        await manager.reserve_slot("tokocrypto", client_order_id="cid-3")

    # Non-spot market does not consume spot slots
    futures_slot = await manager.reserve_slot("tokocrypto", client_order_id="cid-fut", market_type="futures")
    assert futures_slot == -1

    # Release cid-1, slot becomes available again
    await manager.release_slot("tokocrypto", client_order_id="cid-1")
    s_new = await manager.reserve_slot("tokocrypto", client_order_id="cid-4")
    assert s_new == 0


@pytest.mark.asyncio
async def test_order_slot_custom_cap():
    manager = OrderSlotManager()
    await manager.set_max_slots("indodax", 3)

    assert await manager.get_max_slots("indodax") == 3
    s0 = await manager.reserve_slot("indodax", client_order_id="idx-1")
    s1 = await manager.reserve_slot("indodax", client_order_id="idx-2")
    s2 = await manager.reserve_slot("indodax", client_order_id="idx-3")
    assert {s0, s1, s2} == {0, 1, 2}

    with pytest.raises(SlotExhaustedError):
        await manager.reserve_slot("indodax", client_order_id="idx-4")


# =====================================================================
# 2. MutationGateway Tests
# =====================================================================

@pytest.mark.asyncio
async def test_mutation_gateway_create_and_timeout():
    slot_mgr = OrderSlotManager()
    gateway = MutationGateway(global_stop=None, slot_manager=slot_mgr)

    # 1. Normal confirmed placement
    client_mock = AsyncMock()
    client_mock.create_order_direct.return_value = {
        "id": "ex-101",
        "filled": 1.5,
        "price": 100.0,
    }

    res = await gateway.execute_create_order(
        client_mock,
        exchange="tokocrypto",
        market_type="spot",
        symbol="BTC/BIDR",
        side="buy",
        order_type="limit",
        quantity=1.5,
        price=100.0,
        pair_key="tokocrypto:BTC_BIDR",
    )
    assert res.status == MutationResultType.CONFIRMED
    assert res.exchange_order_id == "ex-101"
    assert res.filled_qty == 1.5
    assert len(res.client_order_id) <= 32

    # Slot is active
    assert await slot_mgr.get_active_slots_count("tokocrypto") == 1

    # 2. Timed out create -> SUBMISSION_UNKNOWN (zero blind retries!)
    slow_client = AsyncMock()
    slow_client.create_order_direct.side_effect = asyncio.TimeoutError("gateway timeout")

    res_timeout = await gateway.execute_create_order(
        slow_client,
        exchange="tokocrypto",
        market_type="spot",
        symbol="ETH/BIDR",
        side="buy",
        order_type="limit",
        quantity=0.5,
        price=2000.0,
        pair_key="tokocrypto:ETH_BIDR",
    )
    assert res_timeout.status == MutationResultType.SUBMISSION_UNKNOWN
    # Slot is still held for discovery!
    assert await slot_mgr.get_active_slots_count("tokocrypto") == 2


@pytest.mark.asyncio
async def test_mutation_gateway_cancel():
    slot_mgr = OrderSlotManager()
    gateway = MutationGateway(global_stop=None, slot_manager=slot_mgr)

    cid = "cid-cancel-test"
    await slot_mgr.reserve_slot("tokocrypto", client_order_id=cid)
    assert await slot_mgr.get_active_slots_count("tokocrypto") == 1

    client_mock = AsyncMock()
    client_mock.cancel_order.return_value = {"id": "ex-101", "status": "canceled"}

    res = await gateway.execute_cancel_order(
        client_mock,
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-101",
        symbol="BTC/BIDR",
        client_order_id=cid,
    )
    assert res.status == MutationResultType.CONFIRMED
    # Slot released
    assert await slot_mgr.get_active_slots_count("tokocrypto") == 0


# =====================================================================
# 3. LegManagers & TwoLegCoordinator Tests
# =====================================================================

def test_two_leg_lifecycle_and_drift_detection():
    spot_mgr = SpotLegManager()
    fut_mgr = FuturesLegManager()
    coord = TwoLegCoordinator(spot_mgr, fut_mgr, max_hedge_latency_seconds=1.0, hard_drift_ratio=0.15)

    session_id = "sess-001"
    spot_leg = spot_mgr.create_leg(session_id, venue="tokocrypto", symbol="SOL/BIDR", target_qty=10.0)
    fut_leg = fut_mgr.create_leg(session_id, venue="okx", symbol="SOL/USDT:USDT", contract_size=1.0)

    # Initial state: no fill -> balanced
    report0 = coord.evaluate_hedge_drift(session_id)
    assert report0.status == HedgeDriftStatus.BALANCED
    assert report0.is_safe_for_new_exposure is True

    # Spot fills 5 SOL, Futures not yet hedged -> drift!
    spot_mgr.record_fill(spot_leg.leg_id, filled_qty=5.0, price=150.0)
    assert spot_leg.status == LegStatus.PARTIALLY_FILLED
    assert coord.calculate_unhedged_delta(session_id) == 5.0

    report1 = coord.evaluate_hedge_drift(session_id)
    assert report1.status == HedgeDriftStatus.HARD_DRIFT_BREACH
    assert report1.is_safe_for_new_exposure is False

    # Futures fills 5 contracts -> hedge balanced!
    fut_mgr.record_fill(fut_leg.leg_id, filled_contracts=5.0, price=150.5)
    assert coord.calculate_unhedged_delta(session_id) == 0.0

    report2 = coord.evaluate_hedge_drift(session_id)
    assert report2.status == HedgeDriftStatus.BALANCED
    assert report2.is_safe_for_new_exposure is True


# =====================================================================
# 4. PairPnLEngine & ExitDecisionEngine Tests
# =====================================================================

def test_pair_pnl_and_exit_decision():
    pnl_engine = PairPnLEngine()
    exit_engine = ExitDecisionEngine(min_profit_target_usdt=1.0, stop_loss_usdt=5.0)

    # Scenario A: Spot green (+3 USDT), Futures loss (-5 USDT) -> Pair is NET LOSS (-2 USDT)
    # Exit engine must NOT close Spot alone, must HOLD or wait
    snap_loss = pnl_engine.compute_pair_pnl(
        session_id="s1",
        pair_key="toko_okx:BTC",
        spot_qty=0.01,
        spot_entry_cost_quote=597.0,  # bought at 59700
        spot_executable_bid=60000.0,   # bid is 60000 (+300 on 0.01 = +3 gross)
        spot_exit_fee_rate=0.001,
        futures_qty=0.01,
        futures_entry_price=59800.0,  # shorted at 59800
        futures_executable_ask=60300.0, # ask to buy back is 60300 (-500 on 0.01 = -5 gross)
        futures_close_fee_rate=0.0005,
        funding_accrued=0.1,
    )
    assert snap_loss.spot_net_liquidation_pnl > 0
    assert snap_loss.futures_net_liquidation_pnl < 0
    assert snap_loss.pair_net_liquidation_pnl < 0  # Net negative!

    dec_loss = exit_engine.evaluate_exit(snap_loss, position_age_seconds=10.0)
    assert dec_loss.action == ExitAction.HOLD

    # Scenario B: Dynamic target reached (+2.5 USDT net) after dwell time
    snap_win = pnl_engine.compute_pair_pnl(
        session_id="s2",
        pair_key="toko_okx:BTC",
        spot_qty=0.01,
        spot_entry_cost_quote=59000.0 * 0.01,
        spot_executable_bid=60000.0,
        spot_exit_fee_rate=0.001,
        futures_qty=0.01,
        futures_entry_price=60200.0,
        futures_executable_ask=59500.0,
        futures_close_fee_rate=0.0005,
        funding_accrued=0.2,
    )
    assert snap_win.pair_net_liquidation_pnl >= 2.0
    dec_win = exit_engine.evaluate_exit(snap_win, position_age_seconds=15.0)
    assert dec_win.action == ExitAction.CLOSE_BOTH
    assert "TARGET_PROFIT_REACHED" in dec_win.reason

    # Scenario C: Stop loss breached (-6.0 USDT) -> Emergency Close Both
    snap_sl = PairPnLSnapshot(
        session_id="s3",
        pair_key="toko_okx:BTC",
        spot_net_liquidation_pnl=-3.0,
        futures_net_liquidation_pnl=-3.5,
        pair_net_liquidation_pnl=-6.5,
        pair_net_roi_pct=-5.0,
        total_entry_fee=0.2,
        expected_close_fee=0.2,
        funding_accrued=0.0,
    )
    dec_sl = exit_engine.evaluate_exit(snap_sl, position_age_seconds=10.0)
    assert dec_sl.action == ExitAction.CLOSE_BOTH
    assert "STOP_LOSS_TRIGGERED" in dec_sl.reason


# =====================================================================
# 5. PostgresRiskStateService Tests (Direct PostgreSQL Risk Persistence)
# =====================================================================

@pytest.mark.asyncio
async def test_postgres_risk_state_service():
    fake_pool = MagicMock()
    fake_conn = AsyncMock()

    # Mock fetchrow for daily loss
    fake_conn.fetchrow.return_value = {"total_loss": -12.50}
    # Mock fetch for active risk states
    fake_conn.fetch.return_value = [
        {
            "state_id": "state-1",
            "state_type": "trading_halt",
            "scope": "global",
            "exchange": None,
            "pair_key": None,
            "reason": "RESTORED_TEST_HALT",
            "value": '{"halt_reason": "RESTORED_TEST_HALT"}',
            "created_at": None,
            "updated_at": None,
            "resolved_at": None,
            "is_mock": True,
        }
    ]

    class _Ctx:
        async def __aenter__(self):
            return fake_conn
        async def __aexit__(self, *args):
            return False

    fake_pool.acquire.return_value = _Ctx()

    service = PostgresRiskStateService(pool=fake_pool)
    assert service.enabled is True

    # Test restoring active states
    states = await service.fetch_active_risk_states(is_mock=True)
    assert len(states) == 1
    assert states[0]["state_type"] == "trading_halt"
    assert states[0]["value"]["halt_reason"] == "RESTORED_TEST_HALT"

    # Test daily loss accumulator
    loss = await service.fetch_daily_realized_loss_usdt("2026-11-14", is_mock=True)
    assert loss == 12.50

    # Test persisting new state
    ok = await service.persist_risk_state(
        "exchange_halt", scope="exchange", exchange="tokocrypto", reason="maintenance", is_mock=True
    )
    assert ok is True
    assert fake_conn.execute.called
