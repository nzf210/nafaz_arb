"""Adaptive exit-buffer sampling and active-position PnL tracking."""

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
async def test_dynamic_exit_buffer_requires_three_distinct_timestamped_samples(engine, fake_redis):
    pair = "indodax:btc_idr"
    now = int(time.time() * 1000)
    quiet = {"global_best_bid_usdt": 64999.0, "global_best_ask_usdt": 65001.0}

    first = await engine._calculate_dynamic_exit_slippage(
        pair, {**quiet, "timestamp_ms": now}, now
    )
    assert first == pytest.approx(engine.config.max_exit_slippage_pct)

    # Two observations are deliberately insufficient, even after a large move.
    second_snapshot = {
        "global_best_bid_usdt": 65100.0,
        "global_best_ask_usdt": 65200.0,
        "timestamp_ms": now + 15_000,
    }
    second = await engine._calculate_dynamic_exit_slippage(
        pair, second_snapshot, now + 15_000
    )
    assert second == pytest.approx(engine.config.max_exit_slippage_pct)

    third_snapshot = {
        "global_best_bid_usdt": 65500.0,
        "global_best_ask_usdt": 65600.0,
        "timestamp_ms": now + 30_000,
    }
    third = await engine._calculate_dynamic_exit_slippage(
        pair, third_snapshot, now + 30_000
    )
    assert third > second
    assert third <= engine.config.max_dynamic_exit_slippage_pct

    other = await engine._calculate_dynamic_exit_slippage(
        "indodax:eth_idr", {**quiet, "timestamp_ms": now + 30_000}, now + 30_000
    )
    assert other == pytest.approx(engine.config.max_exit_slippage_pct)


@pytest.mark.asyncio
async def test_dynamic_exit_buffer_deduplicates_repeated_market_timestamp(engine, fake_redis):
    pair = "indodax:duplicate_idr"
    now = int(time.time() * 1000)
    snapshot = {
        "global_best_bid_usdt": 99.9,
        "global_best_ask_usdt": 100.1,
        "timestamp_ms": now,
    }

    first = await engine._calculate_dynamic_exit_slippage(pair, snapshot, now)
    repeated = await engine._calculate_dynamic_exit_slippage(pair, snapshot, now + 1_000)

    assert first == pytest.approx(engine.config.max_exit_slippage_pct)
    assert repeated == pytest.approx(engine.config.max_exit_slippage_pct)
    samples = await fake_redis.lrange(f"arb:exit_slippage_samples:v2:{pair}", 0, -1)
    assert len(samples) == 1


@pytest.mark.asyncio
async def test_dynamic_exit_buffer_ignores_out_of_order_and_stale_samples(engine, fake_redis):
    pair = "indodax:stale_idr"
    now = int(time.time() * 1000)
    stale = {
        "global_best_bid_usdt": 99.0,
        "global_best_ask_usdt": 100.0,
        "timestamp_ms": now - 300_000,
    }
    result = await engine._calculate_dynamic_exit_slippage(pair, stale, now)
    assert result == pytest.approx(engine.config.max_exit_slippage_pct)
    samples = await fake_redis.lrange(f"arb:exit_slippage_samples:v2:{pair}", 0, -1)
    assert len(samples) == 0


@pytest.mark.asyncio
async def test_dynamic_exit_buffer_is_capped(engine, fake_redis):
    pair = "indodax:volatile_idr"
    now = int(time.time() * 1000)
    snapshots = [
        {"global_best_bid_usdt": 99.9, "global_best_ask_usdt": 100.1, "timestamp_ms": now},
        {"global_best_bid_usdt": 104.9, "global_best_ask_usdt": 105.1, "timestamp_ms": now + 30_000},
        {"global_best_bid_usdt": 109.0, "global_best_ask_usdt": 111.0, "timestamp_ms": now + 60_000},
    ]
    result = engine.config.max_exit_slippage_pct
    for offset, snapshot in zip((0, 30_000, 60_000), snapshots):
        result = await engine._calculate_dynamic_exit_slippage(
            pair, snapshot, now + offset
        )
    assert result == pytest.approx(engine.config.max_dynamic_exit_slippage_pct)


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
