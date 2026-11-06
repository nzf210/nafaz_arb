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

@pytest.mark.asyncio
async def test_guaranteed_profit_floor_in_dynamic_exit(engine, fake_redis, mock_client):
    """
    Test that _calculate_limit_sell_idr enforces a mathematical break-even floor:
    When futures ask rises (short suffers loss), limit_sell_idr is pushed higher
    such that Spot PnL covers Futures loss + fees + positive ROI buffer.
    """
    pair_tag = "toko:ath_usdt"
    # Entry: Spot bought at Rp 80.00, Short at 0.0050 USDT. Qty = 10,000.
    pos = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="okx",
        symbol="ATH/USDT",
        entry_price_idr=80.00,
        short_entry_usdt=0.0050,
        qty=10000.0,
        accumulated_funding_usdt=0.0,
        base_target_exit_spread_pct=0.30,
        local_price_tick=1.0,
    )
    engine.active_positions[pair_tag] = pos

    # Futures ask moves up to 0.0051 USDT (a +2% rise on futures, short loss = -1.00 USDT)
    global_ask = 0.0051
    usdt_idr_rate = 16000.0

    limit_sell_idr, tick_size = engine._calculate_limit_sell_idr(
        pair_tag, pos, global_ask, usdt_idr_rate
    )

    # Spot must sell at least at Rp 82.00 (Rp 81.00 would cause net loss of -0.0353 USDT)
    assert limit_sell_idr >= 82.00

    # Calculate net profit if filled at this limit_sell_idr
    spot_pnl_usdt = 10000.0 * (limit_sell_idr - 80.00) / usdt_idr_rate
    futures_pnl_usdt = 10000.0 * (0.0050 - 0.0051)
    fees_usdt = 10000.0 * (0.0050 + 0.0051) * 0.0005
    net_profit = spot_pnl_usdt + futures_pnl_usdt - fees_usdt

    # Net profit MUST be strictly positive!
    assert net_profit > 0

@pytest.mark.asyncio
async def test_active_position_floating_pnl_tracking(engine, fake_redis, mock_client):
    """
    Test State 3 Floating PnL Tracking:
    Verifies mark-to-market floating PnL calculation on active position for both
    Spot Long leg and Futures Short leg in real time.
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # Position: Spot bought at Rp 1,000,000,000, Futures short at $65,000 USDT. Qty = 0.01 BTC.
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1000000000.0,
        short_entry_usdt=65000.0,
        qty=0.01,
        accumulated_funding_usdt=2.0,
        next_funding_time_ms=now_ms + 3600000,
        base_target_exit_spread_pct=0.30,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE

    # Live market price:
    # Spot bid drops to Rp 990,000,000 (-10,000,000 IDR spot loss = -$6.25 USDT)
    # Futures ask drops to $64,000 USDT (+$10.00 USDT futures short profit!)
    # USDT/IDR = 16,000
    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps({
            "local_best_bid": 990000000.0,
            "local_best_ask": 995000000.0,
            "global_best_ask_usdt": 64000.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        }),
    )

    await engine._check_dynamic_exits()

    # Verify floating metrics updated
    updated_pos = engine.active_positions[pair_tag]
    assert updated_pos.current_spot_bid_idr == 990000000.0
    assert updated_pos.current_perp_ask_usdt == 64000.0
    assert updated_pos.spot_unrealized_pnl_idr == -100000.0  # 0.01 * -10,000,000
    assert updated_pos.futures_unrealized_pnl_usdt == 10.0  # 0.01 * (65000 - 64000)
    assert updated_pos.futures_unrealized_pnl_usdt > 0  # Futures short is profitable!

    # Check that Redis active_pos was updated with floating values
    saved_raw = await fake_redis.get(f"arb:active_pos:{pair_tag}")
    assert saved_raw is not None
    saved_dict = json.loads(saved_raw)
    assert saved_dict["futures_unrealized_pnl_usdt"] == 10.0

@pytest.mark.asyncio
async def test_bug10_short_exit_uses_actual_close_fill(fake_redis):
    """BUG-10: futures PnL must use the real close fill, not the last pegged ask."""
    pair_tag = "indodax:btc_idr"
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )

    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=32.5,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    engine.pending_exit_orders[pair_tag] = {
        "order_id": "401",
        "symbol": "btc_idr",
        "price": 1_000_000_000.0,
        "closed_short_qty": 0.0,
    }
    engine.last_pegged_global_ask[pair_tag] = 65_000.0

    # The short actually closes 200 USDT higher than the pegged ask; the old code
    # booked the PnL against 65_000 and understated the loss.
    async def close_at_65200(symbol, amount, client_order_id=None):
        return {"id": "close-1", "status": "closed", "average": 65_200.0, "filled": amount}

    client.close_global_short_market = close_at_65200
    client.orders["401"] = {"id": "401", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps(
            {
                "local_best_bid": 1_000_000_000.0,
                "local_best_ask": 1_000_500_000.0,
                "global_best_ask_usdt": 65_000.0,
                "usdt_idr_rate": 16000.0,
                "timestamp_ms": int(time.time() * 1000),
            }
        ),
    )

    await engine._check_exit_orders()

    trade = json.loads((await fake_redis.lrange("arb:trade_history", 0, 0))[0])
    assert trade["short_exit_usdt"] == 65_200.0


@pytest.mark.asyncio
async def test_funding_rollover_uses_actual_exchange_settlement(engine, fake_redis, mock_client):
    """Funding PnL must use the signed settlement amount, not qty * mark * rate."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="BTC/IDR",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
        accumulated_funding_usdt=1.0,
        current_funding_rate=0.0003,
        next_funding_time_ms=now_ms - 1_000,
    )
    mock_client.funding_history = [
        {
            "id": "funding-001",
            "timestamp": now_ms - 500,
            "amount": -0.025,
            "code": "USDT",
            "rate": 0.0003,
        }
    ]
    mock_client.funding_rate_data = {
        "fundingRate": 0.0002,
        "nextFundingTimestamp": now_ms + 8 * 60 * 60 * 1000,
    }

    applied = await engine._handle_funding_rollover(
        pair_tag, pos, global_ask_usdt=65_100.0, now_ms=now_ms
    )

    assert applied is True
    assert pos.accumulated_funding_usdt == pytest.approx(0.975)
    assert pos.last_funding_time_ms == now_ms - 500
    assert pos.current_funding_rate == pytest.approx(0.0002)
    assert pos.next_funding_time_ms == now_ms + 8 * 60 * 60 * 1000

    # Replaying the same cycle cannot credit the same settlement twice.
    applied_again = await engine._handle_funding_rollover(
        pair_tag, pos, global_ask_usdt=65_100.0, now_ms=now_ms + 1
    )
    assert applied_again is False
    assert pos.accumulated_funding_usdt == pytest.approx(0.975)


@pytest.mark.asyncio
async def test_funding_rollover_never_estimates_missing_settlement(engine, fake_redis, mock_client):
    """No ledger payment means no funding credit, even when a funding time elapsed."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="BTC/IDR",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
        accumulated_funding_usdt=0.0,
        current_funding_rate=0.0003,
        next_funding_time_ms=now_ms - 1_000,
    )
    mock_client.funding_history = []
    mock_client.funding_rate_data = {
        "fundingRate": 0.0001,
        "nextFundingTimestamp": now_ms + 4 * 60 * 60 * 1000,
    }

    applied = await engine._handle_funding_rollover(
        pair_tag, pos, global_ask_usdt=65_100.0, now_ms=now_ms
    )

    assert applied is False
    assert pos.accumulated_funding_usdt == 0.0
    assert pos.next_funding_time_ms == now_ms + 4 * 60 * 60 * 1000
