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
from executor.opportunity_ranker import OpportunityRanker, OpportunityCandidate
from executor.command_processor import CommandProcessor, CommandType, CommandState
from executor.event_inbox import EventInbox, EventNormalizer, StreamEventType
from executor.financial_reconciliation import FinancialReconciliationEngine, ReconciliationSeverity
from executor.exchange_capabilities import ExchangeCapabilityRegistry, CapabilityStatus


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


# =====================================================================
# 6. OpportunityRanker Tests (Hard filters & Lexicographic Ranking)
# =====================================================================

def test_opportunity_ranker_hard_filters_and_scoring():
    ranker = OpportunityRanker(
        max_quote_age_ms=2000.0,
        min_net_profit_usdt=0.30,
        min_net_roi_pct=0.20,
    )
    now_ms = int(time.time() * 1000)

    # 1. Valid high-profit candidate
    c_good = OpportunityCandidate(
        opportunity_id="opp-1",
        pair_key="toko_okx:SOL",
        local_venue="tokocrypto",
        local_symbol="SOL/BIDR",
        global_venue="okx",
        global_symbol="SOL/USDT:USDT",
        local_bid=150.0,
        local_ask=150.1,
        global_bid=152.0,
        global_ask=152.1,
        gross_spread_pct=1.3,
        expected_net_profit_usdt=1.20,
        expected_net_roi_pct=0.80,
        allocated_capital_usdt=150.0,
        quote_age_ms=100.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
    )

    # 2. Stale quote candidate
    c_stale = OpportunityCandidate(
        opportunity_id="opp-2",
        pair_key="toko_okx:ETH",
        local_venue="tokocrypto",
        local_symbol="ETH/BIDR",
        global_venue="okx",
        global_symbol="ETH/USDT:USDT",
        local_bid=3000.0,
        local_ask=3001.0,
        global_bid=3020.0,
        global_ask=3021.0,
        gross_spread_pct=0.6,
        expected_net_profit_usdt=2.00,
        expected_net_roi_pct=0.50,
        allocated_capital_usdt=300.0,
        quote_age_ms=4500.0,  # Stale!
        timestamp_ms=now_ms - 4500,
        expiry_ms=now_ms + 5000,
    )

    # 3. Low profit candidate
    c_low = OpportunityCandidate(
        opportunity_id="opp-3",
        pair_key="toko_okx:XRP",
        local_venue="tokocrypto",
        local_symbol="XRP/BIDR",
        global_venue="okx",
        global_symbol="XRP/USDT:USDT",
        local_bid=0.50,
        local_ask=0.51,
        global_bid=0.51,
        global_ask=0.52,
        gross_spread_pct=0.2,
        expected_net_profit_usdt=0.05,  # Below 0.30!
        expected_net_roi_pct=0.10,
        allocated_capital_usdt=50.0,
        quote_age_ms=50.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
    )

    ranked = ranker.rank_candidates([c_good, c_stale, c_low])
    assert len(ranked) == 1
    assert ranked[0].opportunity_id == "opp-1"

    # Revalidation for admission
    admit_ok, reason = ranker.revalidate_for_admission(c_good, current_local_bid=150.0, current_global_ask=152.1)
    assert admit_ok is True

    # Slippage breach on admission
    admit_fail, reason = ranker.revalidate_for_admission(c_good, current_local_bid=140.0, current_global_ask=152.1)
    assert admit_fail is False
    assert "SLIPPED" in reason


# =====================================================================
# 7. CommandProcessor Tests (Durable Operator Commands)
# =====================================================================

@pytest.mark.asyncio
async def test_command_processor_pause_resume():
    mock_engine = MagicMock()
    mock_engine._halt_exposure_scope = MagicMock()
    mock_engine._persist_risk_state = MagicMock()

    proc = CommandProcessor(engine=mock_engine, pool=None)

    # Process PAUSE_NEW_ENTRIES
    pause_row = {
        "command_id": "cmd-pause-1",
        "command_type": CommandType.PAUSE_NEW_ENTRIES,
        "scope": "tokocrypto",
        "payload": {"reason": "Maintenance"},
    }
    ok_pause = await proc.process_one_command(pause_row)
    assert ok_pause is True
    assert mock_engine._halt_exposure_scope.called

    # Process RESUME_NEW_ENTRIES
    mock_engine._clear_resolved_pair_failure = AsyncMock()
    resume_row = {
        "command_id": "cmd-resume-1",
        "command_type": CommandType.RESUME_NEW_ENTRIES,
        "scope": "*",
        "payload": {},
    }
    ok_resume = await proc.process_one_command(resume_row)
    assert ok_resume is True
    assert mock_engine._trading_halted is False


# =====================================================================
# 8. EventInbox & EventNormalizer Tests
# =====================================================================

def test_event_normalizer_and_deduplicating_inbox():
    inbox = EventInbox()
    received = []
    inbox.register_handler(lambda ev: received.append(ev))

    raw_trade = {
        "id": "tr-999",
        "order": "ord-111",
        "symbol": "BTC/USDT",
        "side": "buy",
        "price": 60000.0,
        "amount": 0.05,
        "fee": {"cost": 0.00005, "currency": "BTC"},
    }

    norm = EventNormalizer.normalize_trade("okx", raw_trade)
    assert norm.event_type == StreamEventType.FILL
    assert norm.venue == "okx"
    assert norm.quantity == 0.05

    # First ingestion -> True
    assert inbox.ingest_event(norm) is True
    assert len(received) == 1

    # Exact duplicate ingestion -> False (deduped!)
    assert inbox.ingest_event(norm) is False
    assert len(received) == 1


# =====================================================================
# 9. FinancialReconciliationEngine Tests
# =====================================================================

def test_financial_reconciliation_detects_mismatches():
    engine = FinancialReconciliationEngine(material_qty_threshold=0.001)

    internal_fills = [{"amount": 0.05, "fee": {"cost": 0.5}}]
    # Exchange has partial trade missing -> mismatch!
    exchange_trades = [{"amount": 0.03, "fee": {"cost": 0.3}}]

    diffs = engine.reconcile_order_fills(
        venue="tokocrypto",
        symbol="BTC/BIDR",
        order_id="ord-1",
        internal_fills=internal_fills,
        exchange_trades=exchange_trades,
    )
    assert len(diffs) >= 1
    qty_diff = next(d for d in diffs if d.difference_type == "FILL_QTY_MISMATCH")
    assert qty_diff.severity == ReconciliationSeverity.MATERIAL_MISMATCH
    assert qty_diff.delta == pytest.approx(0.02)


# =====================================================================
# 10. ExchangeCapabilityRegistry Tests
# =====================================================================

def test_exchange_capability_registry():
    registry = ExchangeCapabilityRegistry()

    # Tokocrypto Spot is verified with limits
    toko = registry.get_profile("tokocrypto", "spot")
    assert toko is not None
    assert toko.status == CapabilityStatus.VERIFIED_WITH_LIMITS
    assert registry.is_operation_permitted("tokocrypto", "spot", "client_order_id") is True
    assert registry.is_operation_permitted("tokocrypto", "spot", "private_ws") is False  # REST only

    # OKX Futures supports reduce_only and private_ws
    okx = registry.get_profile("okx", "futures")
    assert okx is not None
    assert okx.status == CapabilityStatus.VERIFIED
    assert registry.is_operation_permitted("okx", "futures", "reduce_only") is True
    assert registry.is_operation_permitted("okx", "futures", "private_ws") is True

    # Unregistered venue blocked
    assert registry.is_operation_permitted("unknown_cex", "spot", "client_order_id") is False

