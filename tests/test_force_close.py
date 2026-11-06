"""Force-close directive: leg liquidation, partial-failure reconciliation, backoff helper."""

import asyncio
import json
import time

from executor.models import ActivePosition, PositionState

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine, retry_with_backoff

from tests.conftest import MockExchangeClient, SpotSellFailureClient, engine, fake_redis, mock_client

@pytest.mark.asyncio
async def test_force_close_directive(engine, fake_redis, mock_client):
    """Test Force Close: Cancels exit orders, closes short on futures, and clears Redis."""
    pair_tag = "indodax:btc_idr"

    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="BTC/IDR",
        entry_price_idr=1000000000.0,
        short_entry_usdt=65000.0,
        qty=0.015,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    engine.pending_exit_orders[pair_tag] = {
        "order_id": "501",
        "symbol": "BTC/IDR",
    }
    mock_client.orders["501"] = {"id": "501", "symbol": "BTC/IDR", "status": "open"}

    force_close_msg = json.dumps({
        "action": "FORCE_CLOSE",
        "pair_key": pair_tag,
    })
    await engine.handle_force_close(force_close_msg)

    # 1. Exit order was cancelled
    assert ("501", "BTC/IDR") in mock_client.cancelled_orders

    # 2. Global short market buy was executed
    assert len(mock_client.global_close_buys) == 1
    close_trade = mock_client.global_close_buys[0]
    assert close_trade["amount"] == 0.015
    assert close_trade["params"] == {"reduceOnly": True}

    # 3. Position removed from Redis and memory
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is None
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED

@pytest.mark.asyncio
async def test_p0_05_force_close_liquidates_spot_leg(engine, fake_redis, mock_client):
    """BUG-04/P0-05: force close must sell the spot leg, not just the futures short."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))
    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps(
            {
                "local_best_bid": 1_001_000_000.0,
                "local_best_ask": 1_002_000_000.0,
                "global_best_ask_usdt": 65_000.0,
                "usdt_idr_rate": 16000.0,
            }
        ),
    )

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    # Futures short closed
    assert len(mock_client.global_close_buys) == 1
    # Spot leg liquidated with a market sell
    assert len(mock_client.placed_sells) == 1
    spot_sell = mock_client.placed_sells[0]
    assert spot_sell["type"] == "market"
    assert spot_sell["amount"] == 0.005
    # Position removed
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED

    history = await fake_redis.lrange("arb:trade_history", 0, 0)
    trade = json.loads(history[0])
    assert trade["spot_liquidation_failed"] is False
    assert trade["exit_price_idr"] == 1000.0  # the actual spot fill, not the stale snapshot

@pytest.mark.asyncio
async def test_p0_05_force_close_records_failed_spot_liquidation(fake_redis):
    """A failed spot liquidation must be flagged in trade history for operators.

    When futures leg closes successfully but spot liquidation fails, the futures PnL
    must be recorded (with spot_liquidation_failed=True) so the operator knows exactly
    what happened and no realized PnL is silently lost from history.
    """
    pair_tag = "indodax:btc_idr"
    client = SpotSellFailureClient()
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
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    # Futures closed → partial trade record must exist in history with failure flag.
    history = await fake_redis.lrange("arb:trade_history", 0, -1)
    assert len(history) == 1, "partial force-close (futures OK, spot failed) must be recorded"
    record = json.loads(history[0])
    assert record["spot_liquidation_failed"] is True

    # P0 safety: Position must NOT be deleted when spot liquidation fails.
    assert pair_tag in engine.active_positions
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is not None
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "FORCE_CLOSE_PARTIAL_OR_FAILED"

@pytest.mark.asyncio
async def test_p0_05_force_close_retains_position_on_futures_close_failure(fake_redis):
    """P0: If futures short close fails during force close, position must NOT be removed from state or Redis."""
    pair_tag = "indodax:btc_idr"
    client = MockExchangeClient()

    async def fail_close_short(*args, **kwargs):
        raise RuntimeError("futures exchange connection error")

    client.close_global_short_market = fail_close_short

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
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert pair_tag in engine.active_positions
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is not None
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "FORCE_CLOSE_PARTIAL_OR_FAILED"

@pytest.mark.asyncio
async def test_p2_03_force_close_uses_backoff_helper(fake_redis, monkeypatch):
    """Force close must go through the shared retry helper, not an inline loop."""
    import executor.helpers as helpers_module

    seen = {"n": 0}
    real_helper = helpers_module.retry_with_backoff

    async def spy(fn, **kwargs):
        seen["n"] += 1
        return await real_helper(fn, **kwargs)

    # Patch the module the force-close path resolves the helper through.
    monkeypatch.setattr(helpers_module, "retry_with_backoff", spy)

    pair_tag = "indodax:btc_idr"
    client = SpotSellFailureClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    engine.active_positions[pair_tag] = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        accumulated_funding_usdt=0.0,
        next_funding_time_ms=int(time.time() * 1000) + 3_600_000,
        base_target_exit_spread_pct=0.001,
        current_funding_rate=0.0001,
        spot_cost_usdt=312.5,
        futures_margin_usdt=312.5,
    )

    await engine.handle_force_close(pair_tag)

    assert seen["n"] == 1
