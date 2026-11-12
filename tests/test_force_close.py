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

def test_force_close_pnl_uses_position_fee_snapshot_and_splits_costs():
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.01,
        spot_entry_fee_pct=0.1,
        spot_exit_fee_pct=0.2,
        perp_entry_fee_pct=0.03,
        perp_exit_fee_pct=0.04,
        fee_source="EXCHANGE_API",
        fee_verified=True,
        fee_fetched_at_ms=123,
        fee_asset_surcharge_pct=0.01,
    )
    pnl = ArbitrageEngine._compute_force_close_pnl(
        pos,
        current_ask=64_900.0,
        usdt_rate=16_000.0,
        current_spot_bid=1_010_000_000.0,
    )
    expected_spot = 0.01 * (
        62_500.0 * 0.11 + 63_125.0 * 0.21
    ) / 100.0
    expected_futures = 0.01 * (
        65_000.0 * 0.04 + 64_900.0 * 0.05
    ) / 100.0
    assert pnl["spot_fees_usdt"] == pytest.approx(expected_spot)
    assert pnl["futures_fees_usdt"] == pytest.approx(expected_futures)
    assert pnl["fees_usdt"] == pytest.approx(expected_spot + expected_futures)
    assert pnl["fee_source"] == "EXCHANGE_API"
    assert pnl["fee_verified"] is True


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
                "timestamp_ms": int(time.time() * 1000),
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
    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps({
            "local_best_bid": 1_000_000_000.0,
            "local_best_ask": 1_001_000_000.0,
            "global_best_ask_usdt": 65_000.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": int(time.time() * 1000),
        }),
    )

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
async def test_force_close_submits_emergency_spot_sell_once(fake_redis):
    """A non-idempotent emergency sell is submitted once, never blindly retried."""
    attempts = []
    pair_tag = "indodax:btc_idr"
    client = SpotSellFailureClient()
    original = client.place_local_market_sell

    async def count_submit(*args, **kwargs):
        attempts.append("submit")
        return await original(*args, **kwargs)

    client.place_local_market_sell = count_submit
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

    assert attempts == ["submit"]


@pytest.mark.asyncio
async def test_force_close_requires_verified_flat_futures_position(fake_redis):
    """A successful-looking close response is not enough while a short remains open."""
    client = MockExchangeClient()

    async def residual_short(symbol):
        return [{"symbol": symbol, "contracts": 0.005, "side": "short"}]

    client.fetch_positions = residual_short
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert len(client.global_close_buys) == 1
    assert client.placed_sells == []
    assert pair_tag in engine.active_positions
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is not None
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_force_close_blocks_when_pending_order_cancel_is_not_confirmed(fake_redis):
    """Force close must not trade against a resting local entry/exit order."""
    client = MockExchangeClient()
    client.orders["991"] = {
        "id": "991", "symbol": "btc_idr", "status": "open", "filled": 0.0
    }

    async def cancel_without_effect(order_id, symbol):
        return {"id": order_id, "status": "pending"}

    client.cancel_local_order = cancel_without_effect
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    engine.active_positions[pair_tag] = pos
    engine.pending_exit_orders[pair_tag] = {
        "order_id": "991", "symbol": "btc_idr", "closed_short_qty": 0.0
    }
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert client.global_close_buys == []
    assert client.placed_sells == []
    assert pair_tag in engine.active_positions
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_force_close_still_sells_spot_when_futures_fill_price_is_missing(fake_redis):
    """Flat futures exposure must not leave spot unhedged merely because PnL price is absent."""
    client = MockExchangeClient()

    async def close_without_price(symbol, amount, client_order_id=None, amount_spec=None):
        return {
            "id": "close-no-price",
            "symbol": symbol,
            "status": "closed",
            "amount": amount,
            "filled": amount,
        }

    client.close_global_short_market = close_without_price
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.HEDGED_ACTIVE
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert len(client.global_close_buys) == 0  # custom close method used
    assert len(client.placed_sells) == 1
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED


@pytest.mark.asyncio
async def test_force_close_retry_skips_already_flat_futures_and_retries_spot(fake_redis):
    """A prior partial close must allow a spot-only retry when futures are already flat."""
    client = MockExchangeClient()
    futures_close_calls = {"count": 0}

    async def unexpected_futures_close(symbol, amount, client_order_id=None, amount_spec=None):
        futures_close_calls["count"] += 1
        raise AssertionError("must not submit a second futures close when already flat")

    client.close_global_short_market = unexpected_futures_close
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True),
        client=client,
        redis_client=fake_redis,
    )
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert futures_close_calls["count"] == 0
    assert len(client.placed_sells) == 1
    assert pair_tag not in engine.active_positions
    assert engine.states[pair_tag] == PositionState.CLOSED


@pytest.mark.asyncio
async def test_force_close_retry_does_not_resubmit_when_short_remains_open(fake_redis):
    """An unresolved previous futures close must not be blindly submitted again."""
    client = MockExchangeClient()
    close_calls = {"count": 0}

    async def unexpected_futures_close(symbol, amount, client_order_id=None, amount_spec=None):
        close_calls["count"] += 1
        raise AssertionError("duplicate futures close must be blocked")

    async def still_open_short(symbol):
        return [{"symbol": symbol, "contracts": 0.005, "contractSize": 1.0, "side": "short"}]

    client.close_global_short_market = unexpected_futures_close
    client.fetch_positions = still_open_short
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True),
        client=client,
        redis_client=fake_redis,
    )
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    engine.active_positions[pair_tag] = pos
    engine.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert close_calls["count"] == 0
    assert client.placed_sells == []
    assert pair_tag in engine.active_positions
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


def test_force_close_pnl_is_unverified_without_live_fx():
    """Do not present futures-only PnL as complete when spot FX is unavailable."""
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )

    pnl = ArbitrageEngine._compute_force_close_pnl(
        pos, current_ask=64_900.0, usdt_rate=0.0, current_spot_bid=1_010_000_000.0
    )

    assert pnl["pnl_complete"] is False
    assert pnl["pnl_unverified"] is True
