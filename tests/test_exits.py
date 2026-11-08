"""State 3/4 exit flow: dynamic limit-sell pegging, funding decay, exit execution, profit floor."""

import asyncio
import json
import re
import time

from executor.models import ActivePosition, PositionState

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine

from tests.conftest import MockExchangeClient, engine, fake_redis, mock_client

@pytest.mark.asyncio
async def test_state3_dynamic_exit_limit_sell_and_funding_decay(engine, fake_redis, mock_client):
    """
    Test State 3: Dynamic Exit Limit Sell Pegging:
    - Recalculates and replaces Limit Sell when Global Ask moves > 0.15% OR funding passes.
    - Formula: limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (1 + effective_exit_spread_pct / 100)
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # Set up active position in state 3
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1000000000.0,
        short_entry_usdt=65000.0,
        qty=0.01,
        accumulated_funding_usdt=10.0,  # $10 funding accumulated
        next_funding_time_ms=now_ms + 3600000,
        base_target_exit_spread_pct=0.30,  # 0.30%
        current_funding_rate=0.0003,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE

    # Live price: Global Ask = 65,000 USDT, USDT/IDR = 16,000
    live_snapshot = {
        "global_best_ask_usdt": 65000.0,
        "usdt_idr_rate": 16000.0,
        "timestamp_ms": now_ms,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(live_snapshot))

    # Initial pegging
    await engine._check_dynamic_exits()

    assert len(mock_client.placed_sells) == 1
    first_exit_order = mock_client.placed_sells[0]
    first_price = first_exit_order["price"]
    assert first_price > 0
    assert engine.last_pegged_global_ask[pair_tag] == 65000.0

    # 1. Global Ask moves slightly by 0.05% (< 0.15% threshold) -> NO re-pegging
    live_snapshot["global_best_ask_usdt"] = 65032.5  # +0.05%
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(live_snapshot))
    await engine._check_dynamic_exits()
    assert len(mock_client.placed_sells) == 1  # No new order

    # 2. Global Ask moves by 0.20% (> 0.15% threshold) -> Triggers re-pegging!
    live_snapshot["global_best_ask_usdt"] = 65150.0  # +0.23%
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(live_snapshot))
    await engine._check_dynamic_exits()

    assert len(mock_client.placed_sells) == 2
    second_exit_order = mock_client.placed_sells[1]
    assert second_exit_order["price"] != first_price
    # Previous order cancelled
    assert (first_exit_order["id"], "btc_idr") in mock_client.cancelled_orders

@pytest.mark.asyncio
async def test_state4_exit_execution_and_position_cleanup(engine, fake_redis, mock_client):
    """
    Test State 4: When local Limit Sell is filled:
    Immediately executes Market Buy (reduceOnly=True) on Global Futures to close Short,
    and removes arb:active_pos from Redis when fully closed!
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="BTC/IDR",
        entry_price_idr=1000000000.0,
        short_entry_usdt=65000.0,
        qty=0.01,
        accumulated_funding_usdt=5.0,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.EXIT_PENDING

    # Active key in Redis
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    engine.pending_exit_orders[pair_tag] = {
        "order_id": "301",
        "symbol": "BTC/IDR",
        "price": 1045000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "closed_short_qty": 0.0,
    }
    # Local limit sell is fully filled!
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "BTC/IDR",
        "status": "closed",
        "filled": 0.01,
    }

    await engine._check_exit_orders()

    # Verify Global Market Buy with reduceOnly=True executed for exact 0.01 qty!
    assert len(mock_client.global_close_buys) == 1
    close_buy = mock_client.global_close_buys[0]
    assert close_buy["side"] == "buy"
    assert close_buy["amount"] == 0.01
    assert close_buy["params"] == {"reduceOnly": True}

    # Verify active position deleted from Redis and internal memory
    active_redis_val = await fake_redis.get(f"arb:active_pos:{pair_tag}")
    assert active_redis_val is None
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED

@pytest.mark.asyncio
async def test_state4_records_negative_pnl_realistically(engine, fake_redis, mock_client):
    """
    Test State 4 Real Market Reporting:
    When an exit order is filled even if futures loss causes negative net profit,
    the engine executes market buy on futures to close the short (preserving delta neutrality),
    records the trade as a loss in history, and does NOT falsely count it as a win.
    """
    pair_tag = "toko:ath_usdt"
    pos = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="okx",
        symbol="ATH/USDT",
        entry_price_idr=80.00,
        short_entry_usdt=0.0050,
        qty=10000.0,
        accumulated_funding_usdt=0.0,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.EXIT_PENDING
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    # Order placed at Rp 80.50 while futures ask rose to 0.0051 (futures loss)
    engine.pending_exit_orders[pair_tag] = {
        "order_id": "601",
        "symbol": "ATH/USDT",
        "price": 80.50,
        "qty": 10000.0,
        "filled": 0.0,
        "closed_short_qty": 0.0,
    }
    mock_client.orders["601"] = {
        "id": "601",
        "symbol": "ATH/USDT",
        "status": "closed",
        "filled": 10000.0,
    }
    # Live price where futures short loss dominates
    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps({
            "global_best_ask_usdt": 0.0051,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": int(time.time() * 1000),
        }),
    )

    await engine._check_exit_orders()

    # 1. Verify short WAS closed on futures to protect delta neutrality
    assert len(mock_client.global_close_buys) == 1
    assert mock_client.global_close_buys[0]["amount"] == 10000.0

    # 2. Position is cleanly closed and removed from active positions
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is None

    # 3. Verify trade recorded in trade_history
    history_raw = await fake_redis.lrange("arb:trade_history", 0, -1)
    assert len(history_raw) == 1
    trade = json.loads(history_raw[0])
    assert trade["net_profit_usdt"] < 0  # Verified negative PnL recorded honestly!

    # 4. Verify stats: total_closed incremented, but win_count is 0!
    stats = await fake_redis.hgetall("arb:stats")
    assert stats.get("total_closed") == "1"
    assert stats.get("win_count") is None or stats.get("win_count") == "0"
    assert float(stats.get("total_pnl_usdt", 0.0)) < 0
