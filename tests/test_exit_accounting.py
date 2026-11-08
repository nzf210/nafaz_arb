"""Exit fills, funding settlement, and reconciliation safety."""

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


@pytest.mark.asyncio
async def test_partial_futures_close_after_spot_fill_requires_reconciliation(engine, fake_redis, mock_client):
    """Do not reduce the recorded position when the futures leg did not fully close."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.EXIT_PENDING
    engine.pending_exit_orders[pair_tag] = {
        "order_id": "992",
        "symbol": "btc_idr",
        "price": 1_010_000_000.0,
        "qty": 0.01,
        "filled": 0.0,
        "closed_short_qty": 0.0,
    }
    mock_client.orders["992"] = {
        "id": "992", "symbol": "btc_idr", "status": "closed", "filled": 0.005
    }

    async def partial_close(symbol, amount, client_order_id=None):
        return {
            "id": "partial-close",
            "symbol": symbol,
            "status": "closed",
            "amount": amount,
            "filled": 0.002,
            "average": 65_100.0,
        }

    mock_client.close_global_short_market = partial_close
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine._check_exit_orders()

    assert pos.qty == pytest.approx(0.01)
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is not None
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "EXIT_HEDGE_RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
async def test_exit_price_snapshot_without_live_fx_is_rejected(engine, fake_redis):
    """IDR exit pricing must not use the old hardcoded USDT/IDR fallback."""
    pair_tag = "indodax:btc_idr"
    snapshot = {
        "global_best_ask_usdt": 65000.0,
        "timestamp_ms": int(time.time() * 1000),
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(snapshot))

    assert await engine._fetch_live_price(pair_tag) is None


def test_profit_floor_includes_exit_slippage_and_configured_round_trip_fees(engine):
    """The spot floor covers adverse futures buyback movement, fees and target ROI."""
    engine.config.estimated_round_trip_fee_pct = 0.001
    engine.config.max_exit_slippage_pct = 0.01
    pos = ActivePosition(
        local_exchange="tokocrypto", global_exchange="okx", symbol="ATH/USDT",
        entry_price_idr=80.0, short_entry_usdt=0.005, qty=10000.0,
        base_target_exit_spread_pct=0.30, local_price_tick=0.01,
    )
    ask, fx = 0.0051, 16000.0
    limit_price, _ = engine._calculate_limit_sell_idr("toko:ath_usdt", pos, ask, fx)
    conservative_buyback = ask * (1.0 + engine.config.max_exit_slippage_pct)
    spot_pnl = pos.qty * (limit_price - pos.entry_price_idr) / fx
    futures_pnl = pos.qty * (pos.short_entry_usdt - conservative_buyback)
    fee_reserve = pos.qty * pos.short_entry_usdt * engine.config.estimated_round_trip_fee_pct
    min_target = pos.qty * pos.short_entry_usdt * 0.00075
    assert spot_pnl + futures_pnl - fee_reserve >= min_target


@pytest.mark.asyncio
async def test_realized_combined_pnl_uses_actual_spot_average_and_shared_fee_reserve(fake_redis):
    """Realized two-leg PnL uses actual spot average, futures fill and shared fee reserve."""
    client = MockExchangeClient()
    cfg = ExecutorConfig(mock_mode=True, estimated_round_trip_fee_pct=0.001)
    engine = ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis)
    pair_tag = "toko:coin_idr"
    pos = ActivePosition(
        local_exchange="tokocrypto", global_exchange="okx", symbol="COIN/IDR",
        global_symbol="COIN/USDT:USDT", entry_price_idr=100.0,
        short_entry_usdt=1.0, qty=1.0,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.EXIT_PENDING
    engine.pending_exit_orders[pair_tag] = {
        "order_id": "spot-avg", "symbol": "COIN/IDR", "price": 105.0,
        "qty": 1.0, "closed_short_qty": 0.0,
    }
    client.orders["spot-avg"] = {
        "id": "spot-avg", "symbol": "COIN/IDR", "status": "closed",
        "filled": 1.0, "average": 110.0, "cost": 110.0,
    }
    async def close_short(symbol, amount, client_order_id=None):
        return {"id": "perp-close", "symbol": symbol, "status": "closed", "filled": amount, "average": 0.9}
    client.close_global_short_market = close_short
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps({
        "global_best_ask_usdt": 0.9, "usdt_idr_rate": 100.0,
        "timestamp_ms": int(time.time() * 1000),
    }))
    await engine._check_exit_orders()
    trade = json.loads((await fake_redis.lrange("arb:trade_history", 0, 0))[0])
    assert trade["exit_price_idr"] == pytest.approx(110.0)
    assert trade["spot_pnl_idr"] == pytest.approx(10.0)
    assert trade["futures_pnl_usdt"] == pytest.approx(0.1)
    assert trade["net_profit_usdt"] == pytest.approx(0.199)
