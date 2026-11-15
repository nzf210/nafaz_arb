"""Tests for priority improvements: pair locking, entry venue lock, partial exit accumulation, and sniper/smart maker."""

import asyncio
import json
import time
import pytest

from executor.models import ActivePosition, PositionState
from tests.conftest import MockExchangeClient, engine, fake_redis, mock_client


@pytest.mark.asyncio
async def test_exit_pair_lock_guards_dynamic_exit_and_pending_exit(engine, fake_redis, mock_client):
    """Test 1: _pair_lock is acquired during dynamic exit check and pending exit execution."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
        base_target_exit_spread_pct=0.30,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE

    lock = engine._pair_lock(pair_tag)
    assert not lock.locked()

    lock_held_during_check = False

    orig_fetch = engine._fetch_live_price

    async def wrapped_fetch(pt):
        nonlocal lock_held_during_check
        lock_held_during_check = lock.locked()
        return await orig_fetch(pt)

    engine._fetch_live_price = wrapped_fetch
    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps({
            "global_best_ask_usdt": 65000.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": int(time.time() * 1000),
        }),
    )

    await engine._check_dynamic_exit_pair_bound(pair_tag)
    assert lock_held_during_check
    assert not lock.locked()


@pytest.mark.asyncio
async def test_exit_hedge_partial_fill_accumulation(engine, fake_redis, mock_client):
    """Test 3: Exit fill below min_partial_fill_usd accumulates until threshold or terminal."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.1,  # 0.1 BTC = $6,500
        base_target_exit_spread_pct=0.30,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.EXIT_PENDING
    engine.config.min_partial_fill_usd = 5.0  # $5 min hedge

    # Order info: 0.1 BTC limit sell at Rp 1,043,120,000 (USDT rate: 16,000)
    order_info = {
        "order_id": "801",
        "symbol": "btc_idr",
        "price": 1_040_000_000.0,
        "qty": 0.1,
        "filled": 0.0,
        "closed_short_qty": 0.0,
        "usdt_idr_rate": 16000.0,
    }
    engine.pending_exit_orders[pair_tag] = order_info

    # Fill tiny amount: 0.00001 BTC = Rp 10,400 = $0.65 (below $5 threshold)
    mock_client.orders["801"] = {
        "id": "801",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.00001,
    }

    await engine._process_pending_exit_bound(pair_tag)
    # Below min_partial_fill_usd and not terminal: no futures buy yet!
    assert len(mock_client.global_close_buys) == 0
    assert order_info["filled"] == 0.00001
    assert order_info["closed_short_qty"] == 0.0

    # Fill more: total 0.0001 BTC = Rp 104,000 = $6.50 (exceeds $5 threshold)
    mock_client.orders["801"]["filled"] = 0.0001
    await engine._process_pending_exit_bound(pair_tag)
    assert len(mock_client.global_close_buys) == 1
    assert mock_client.global_close_buys[0]["amount"] == pytest.approx(0.0001)
    assert order_info["closed_short_qty"] == pytest.approx(0.0001)

    # Now a small residual fill 0.00002 BTC (below $5), but order becomes terminal "closed"
    mock_client.orders["801"]["filled"] = 0.00012
    mock_client.orders["801"]["status"] = "closed"
    await engine._process_pending_exit_bound(pair_tag)
    assert len(mock_client.global_close_buys) == 2
    assert mock_client.global_close_buys[1]["amount"] == pytest.approx(0.00002)


@pytest.mark.asyncio
async def test_taker_sniper_exit_triggers_on_profitable_bid(engine, fake_redis, mock_client):
    """Test 4: Immediate Taker Sell executed when local_best_bid_idr reaches limit_sell_idr."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
        accumulated_funding_usdt=0.0,
        base_target_exit_spread_pct=0.30,
        local_price_tick=1000.0,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE

    # At global ask $65,000, USDT/IDR 16,000, target sell is ~1,043,120,000 IDR
    # Best bid surges to Rp 1,045,000,000 (above target exit price)
    live_snapshot = {
        "local_best_bid": 1_045_000_000.0,
        "local_best_ask": 1_046_000_000.0,
        "global_best_ask_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "timestamp_ms": now_ms,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(live_snapshot))

    await engine._check_dynamic_exit_pair_bound(pair_tag)

    # Should execute Taker Sniper Exit (market sell on spot + market buy close on futures)
    assert len(mock_client.placed_sells) == 1
    spot_sell = mock_client.placed_sells[0]
    assert spot_sell["type"] == "market"
    assert spot_sell["amount"] == 0.01

    assert len(mock_client.global_close_buys) == 1
    assert mock_client.global_close_buys[0]["amount"] == 0.01
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED


def test_smart_maker_ask_undercuts_best_ask(engine):
    """Test 4: Smart Maker Ask undercuts local_best_ask by tick_size when above limit_sell_idr."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
        base_target_exit_spread_pct=0.30,
        local_price_tick=1000.0,
    )
    global_ask = 65_000.0
    usdt_idr_rate = 16_000.0

    # Without best ask: pegged price (~1,043,120,000)
    base_price, tick = engine._calculate_limit_sell_idr(
        pair_tag, pos, global_ask, usdt_idr_rate, local_best_ask_idr=0.0
    )
    assert tick == 1000.0

    # Best ask is 1,050,000,000 (higher than floor): should undercut to 1,049,999,000
    smart_price, _ = engine._calculate_limit_sell_idr(
        pair_tag, pos, global_ask, usdt_idr_rate, local_best_ask_idr=1_050_000_000.0
    )
    assert smart_price == 1_049_999_000.0
    assert smart_price > base_price

    # Best ask is lower than floor (1,020,000,000): should stay at base_price floor
    floor_price, _ = engine._calculate_limit_sell_idr(
        pair_tag, pos, global_ask, usdt_idr_rate, local_best_ask_idr=1_020_000_000.0
    )
    assert floor_price == base_price


@pytest.mark.asyncio
async def test_pair_pnl_engine_includes_futures_entry_fee():
    """Verify compute_pair_pnl accounts for futures entry fee in PnL and total entry fee."""
    from executor.pair_pnl_engine import PairPnLEngine

    engine = PairPnLEngine(futures_taker_fee_rate=0.0005)
    # 0.01 BTC at $60,000 -> futures_entry_notional = $600
    # futures_entry_fee (0.05%) = $0.30
    # futures_close_fee (0.05% of $600) = $0.30
    # futures gross pnl = ($60,000 - $60,000) * 0.01 = $0.0
    # futures_net_pnl_native = 0.0 - 0.30 (entry) - 0.30 (close) = -0.60
    snap = engine.compute_pair_pnl(
        session_id="test_fee",
        pair_key="indodax:btc_idr",
        spot_qty=0.01,
        spot_entry_cost_quote=600.0,
        spot_executable_bid=600.0,
        spot_exit_fee_rate=0.001,
        spot_entry_fee_quote=0.60,
        futures_qty=0.01,
        futures_entry_price=60000.0,
        futures_executable_ask=60000.0,
        futures_close_fee_rate=0.0005,
    )
    assert snap.total_entry_fee == pytest.approx(0.60 + 0.30, rel=1e-3)
    assert snap.futures_net_liquidation_pnl == pytest.approx(-0.60, rel=1e-3)

    # With explicit position_state
    snap_explicit = engine.compute_pair_pnl(
        session_id="test_fee_explicit",
        pair_key="indodax:btc_idr",
        spot_qty=0.01,
        spot_entry_cost_quote=600.0,
        spot_executable_bid=600.0,
        spot_entry_fee_quote=0.60,
        futures_qty=0.01,
        futures_entry_price=60000.0,
        futures_executable_ask=60000.0,
        position_state={"futures_entry_fee": 0.45},
    )
    assert snap_explicit.total_entry_fee == pytest.approx(0.60 + 0.45, rel=1e-3)
    assert snap_explicit.futures_net_liquidation_pnl == pytest.approx(-0.75, rel=1e-3)


@pytest.mark.asyncio
async def test_dynamic_exit_skips_when_pair_lock_already_held(engine):
    """When _pair_lock is locked by another task, dynamic exit skips rather than blocking."""
    pair_tag = "indodax:btc_idr"
    lock = engine._pair_lock(pair_tag)
    await lock.acquire()
    assert lock.locked()

    inner_called = False
    async def fake_inner(pt):
        nonlocal inner_called
        inner_called = True

    engine._check_dynamic_exit_pair_inner = fake_inner
    try:
        await engine._check_dynamic_exit_pair_bound(pair_tag)
        assert not inner_called
    finally:
        lock.release()


@pytest.mark.asyncio
async def test_run_order_recovery_for_pair_holds_pair_lock(engine):
    """_run_order_recovery_for_pair executes under _pair_lock."""
    pair_tag = "indodax:btc_idr"
    lock = engine._pair_lock(pair_tag)

    lock_state_during = False
    async def fake_recovery(pt, rec):
        nonlocal lock_state_during
        lock_state_during = lock.locked()

    engine._execute_single_pair_recovery = fake_recovery
    await engine._run_order_recovery_for_pair(pair_tag, {"symbol": "btc_idr"})
    assert lock_state_during
    assert not lock.locked()


@pytest.mark.asyncio
async def test_execute_entry_signal_locks_pair_and_spot_venue(engine):
    """execute_entry_signal holds both pair_lock and spot_exchange_entry_lock."""
    pair_tag = "indodax:btc_idr"
    spot_venue = "indodax"
    p_lock = engine._pair_lock(pair_tag)
    s_lock = engine._spot_exchange_entry_lock(spot_venue)

    p_locked_during = False
    s_locked_during = False

    async def fake_get_balance(venue):
        nonlocal p_locked_during, s_locked_during
        p_locked_during = p_lock.locked()
        s_locked_during = s_lock.locked()
        return 500.0

    engine._get_available_spot_quote_balance = fake_get_balance
    result = await engine.execute_entry_signal({"spot_venue": spot_venue, "pair_tag": pair_tag})
    assert p_locked_during
    assert s_locked_during
    assert not p_lock.locked()
    assert not s_lock.locked()
    assert result.get("status") in ("SUCCESS", "SUBMITTED")

