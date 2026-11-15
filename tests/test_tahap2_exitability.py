"""Tests for Tahap 2 (P1): Exitability, Probabilistic Maker/Taker, Chunked Exit & Capital Recovery."""

import asyncio
import json
import time
import pytest

from executor.models import ActivePosition, PositionState
from executor.exit_decision_engine import ExitDecisionEngine, ExitAction
from executor.exit_pricing import calculate_chunk_exit_qty, compute_vwap_exit_fill
from executor.pair_pnl_engine import PairPnLSnapshot
from tests.conftest import MockExchangeClient, engine, fake_redis, mock_client


def test_2_2_fill_probability_and_ev_calculation():
    """2.2: Test fill probability clamping and expected value formula."""
    engine = ExitDecisionEngine()

    # Denominator zero or negative clamps to 0.05
    p_zero = engine.calculate_fill_probability(spot_sell_turnover_30s_usd=0.0, queue_ahead_usd=0.0, order_notional_usd=0.0)
    assert p_zero == 0.05

    # Low turnover clamps to min_p (0.05)
    p_low = engine.calculate_fill_probability(spot_sell_turnover_30s_usd=1.0, queue_ahead_usd=100.0, order_notional_usd=50.0)
    assert p_low == 0.05

    # Very high turnover clamps to max_p (0.95)
    p_high = engine.calculate_fill_probability(spot_sell_turnover_30s_usd=500.0, queue_ahead_usd=50.0, order_notional_usd=50.0)
    assert p_high == 0.95

    # In-between: 60 / (100 + 50) = 60 / 150 = 0.40
    p_mid = engine.calculate_fill_probability(spot_sell_turnover_30s_usd=60.0, queue_ahead_usd=100.0, order_notional_usd=50.0)
    assert p_mid == pytest.approx(0.40)

    # EV Maker formula:
    # EV_Maker = P_fill * NetPnL_Maker + (1 - P_fill) * (NetPnL_Taker - Penalty)
    # Case A: High fill probability (0.90), Maker = $2.0, Taker = $1.0, Penalty = $0.10
    ev_maker_a = engine.calculate_ev_maker(p_fill=0.90, net_pnl_maker=2.0, net_pnl_taker=1.0, spread_decay_penalty=0.10)
    # 0.90 * 2.0 + 0.10 * (1.0 - 0.10) = 1.80 + 0.09 = 1.89
    assert ev_maker_a == pytest.approx(1.89)

    # Case B: Low fill probability (0.05), Maker = $2.0, Taker = $1.50, Penalty = $0.30
    ev_maker_b = engine.calculate_ev_maker(p_fill=0.05, net_pnl_maker=2.0, net_pnl_taker=1.50, spread_decay_penalty=0.30)
    # 0.05 * 2.0 + 0.95 * (1.50 - 0.30) = 0.10 + 0.95 * 1.20 = 0.10 + 1.14 = 1.24
    assert ev_maker_b == pytest.approx(1.24)
    # Taker EV is 1.50, which is > EV_Maker (1.24)!
    should_taker, ev_t, ev_m = engine.evaluate_maker_vs_taker(
        taker_roi_pct=0.25, net_pnl_taker=1.50,
        maker_roi_pct=0.35, net_pnl_maker=2.0,
        required_decay_roi=0.30, p_fill=0.05, spread_decay_penalty=0.30,
    )
    assert should_taker is True
    assert ev_t > ev_m


def test_2_3_calculate_chunk_exit_qty():
    """2.3: Test Chunked Taker Exit calculation with min futures unwind and min order notional."""
    # Scenario: Position is $30 (30 units at $1.00)
    # Orderbook top 2 bids absorb 12 units ($12)
    bids = [[1.0, 5.0], [0.999, 7.0], [0.95, 20.0]]
    unit_price = 1.0

    # Total available at top 2 bids is 12 units
    can_chunk, chunk_qty, status = calculate_chunk_exit_qty(
        orderbook_levels=bids[:2],
        total_qty=30.0,
        unit_price_usd=unit_price,
        min_futures_unwind_usd=5.50,
        min_order_notional_usd=5.50,
    )
    assert can_chunk is True
    assert chunk_qty == pytest.approx(12.0)
    assert status == "CHUNKED_EXIT"

    # Full exit if orderbook absorbs full position
    can_full, full_qty, status_full = calculate_chunk_exit_qty(
        orderbook_levels=bids,
        total_qty=30.0,
        unit_price_usd=unit_price,
        min_futures_unwind_usd=5.50,
        min_order_notional_usd=5.50,
    )
    assert can_full is True
    assert full_qty == pytest.approx(30.0)
    assert status_full == "FULL_EXIT"

    # Fails if chunk size < min_futures_unwind_usd ($4 < $5.50)
    small_bids = [[1.0, 4.0]]
    can_small, _, _ = calculate_chunk_exit_qty(
        orderbook_levels=small_bids,
        total_qty=30.0,
        unit_price_usd=unit_price,
        min_futures_unwind_usd=5.50,
        min_order_notional_usd=5.50,
    )
    assert can_small is False

    # Fails if remaining position < min_order_notional_usd ($27 chunk leaves $3 < $5.50)
    large_bids = [[1.0, 27.0]]
    can_large, _, _ = calculate_chunk_exit_qty(
        orderbook_levels=large_bids,
        total_qty=30.0,
        unit_price_usd=unit_price,
        min_futures_unwind_usd=5.50,
        min_order_notional_usd=5.50,
    )
    assert can_large is False


def test_2_4_four_phase_capital_recovery():
    """2.4: Test the 4-phase capital recovery logic in ExitDecisionEngine."""
    engine = ExitDecisionEngine(
        target_exit_net_roi_pct=0.50,
        decay_start_minutes=15.0,
        decay_end_minutes=45.0,
        decay_target_roi_pct=0.20,
        breakeven_minutes=90.0,
        breakeven_target_roi_pct=0.02,
        max_hold_minutes=90.0,
        hard_stop_hold_minutes=180.0,
        max_recovery_loss_pct=0.30,
    )

    # 1. Normal Phase (0 - 15m): Target = +0.50%
    roi_0 = engine.get_required_decay_roi(position_age_seconds=0.0)
    assert roi_0 == pytest.approx(0.50)
    roi_10 = engine.get_required_decay_roi(position_age_seconds=10 * 60.0)
    assert roi_10 == pytest.approx(0.50)

    # 2. Time-Decay Phase (15 - 45m): Linear decay towards +0.20%
    # At 30m (halfway between 15 and 45): (0.50 + 0.20) / 2 = 0.35%
    roi_30 = engine.get_required_decay_roi(position_age_seconds=30 * 60.0)
    assert roi_30 == pytest.approx(0.35)
    roi_45 = engine.get_required_decay_roi(position_age_seconds=45 * 60.0)
    assert roi_45 == pytest.approx(0.20)

    # 3. Breakeven / Scratch Phase (45 - 90m): +0.02%
    roi_60 = engine.get_required_decay_roi(position_age_seconds=60 * 60.0)
    assert roi_60 == pytest.approx(0.02)
    roi_89 = engine.get_required_decay_roi(position_age_seconds=89 * 60.0)
    assert roi_89 == pytest.approx(0.02)

    # 4. Capital Recovery Phase (> 90m):
    # If funding rate is negative -> tolerance = -0.30%
    roi_95_neg = engine.get_required_decay_roi(position_age_seconds=95 * 60.0, net_funding_rate=-0.0001)
    assert roi_95_neg == pytest.approx(-0.30)

    # If funding rate is positive and age < 180m -> hold at breakeven (+0.02%)
    roi_95_pos = engine.get_required_decay_roi(position_age_seconds=95 * 60.0, net_funding_rate=0.0002)
    assert roi_95_pos == pytest.approx(0.02)

    # If age >= 180m (hard stop hold), triggers capital recovery (-0.30%) even with positive funding
    roi_185 = engine.get_required_decay_roi(position_age_seconds=185 * 60.0, net_funding_rate=0.0002)
    assert roi_185 == pytest.approx(-0.30)

    # Evaluate decision in Capital Recovery phase:
    # Stale position at 100 minutes with negative funding and PnL at -0.15% (within -0.30% tolerance)
    snap_recovery = PairPnLSnapshot(
        session_id="rec-1",
        pair_key="toko_binance:ETH",
        spot_net_liquidation_pnl=-0.50,
        futures_net_liquidation_pnl=-0.25,
        pair_net_liquidation_pnl=-0.75,
        pair_net_roi_pct=-0.15,
        total_entry_fee=0.2,
        expected_close_fee=0.2,
        funding_accrued=-0.1,
    )
    dec_recovery = engine.evaluate_exit(
        snap_recovery,
        position_age_seconds=100 * 60.0,
        net_funding_rate=-0.0001,
    )
    assert dec_recovery.action == ExitAction.CLOSE_BOTH
    assert "CAPITAL_RECOVERY_EXIT" in dec_recovery.reason


@pytest.mark.asyncio
async def test_2_3_chunked_taker_exit_execution(engine, fake_redis, mock_client):
    """2.3: End-to-end Chunked Taker Exit executes $12 chunk and retains remaining $18."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # Position is 0.03 BTC ($30 value at $1000/BTC)
    # Entry price: Rp 16,000,000 (~$1000 at rate 16000)
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=16_000_000.0,
        short_entry_usdt=1000.0,
        qty=0.03,
        accumulated_funding_usdt=0.0,
        base_target_exit_spread_pct=0.30,
        local_price_tick=1000.0,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE

    # Orderbook: Top 2 bids have volume 0.012 BTC ($12), absorbing 40% of position
    # Price is 16,500,000 IDR (profitable taker ROI)
    snapshot = {
        "local_best_bid": 16_500_000.0,
        "local_best_ask": 16_600_000.0,
        "spot_bids_top5": [[16_500_000.0, 0.006], [16_490_000.0, 0.006]],
        "spot_asks_top5": [[16_600_000.0, 0.010]],
        "futures_bids_top5": [[1000.0, 1.0]],
        "futures_asks_top5": [[1000.5, 1.0]],
        "global_best_ask_usdt": 1000.5,
        "usdt_idr_rate": 16_000.0,
        "trade_velocity_1m_usd": 100.0,
        "timestamp_ms": now_ms,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(snapshot))

    await engine._check_dynamic_exit_pair_bound(pair_tag)

    # Chunk of 0.012 BTC executed via market sell
    taker_sells = [s for s in mock_client.placed_sells if s.get("type") == "market"]
    assert len(taker_sells) == 1
    assert taker_sells[0]["amount"] == pytest.approx(0.012)

    # Futures close buy executed for 0.012 BTC
    assert len(mock_client.global_close_buys) == 1
    assert mock_client.global_close_buys[0]["amount"] == pytest.approx(0.012)

    # Remaining position is 0.018 BTC ($18), still active!
    assert pair_tag in engine.active_positions
    assert engine.active_positions[pair_tag].qty == pytest.approx(0.018)
    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE
