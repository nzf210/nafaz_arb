"""Startup recovery, hedge-failure rollback, emergency cancel, engine lifecycle, backoff helper."""

import asyncio
import json
import re
import time

from executor.models import ActivePosition, PositionState

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine, RetryExhausted, retry_with_backoff

from tests.conftest import BlockingSellClient, FailingHedgeClient, MockExchangeClient, OpenShortPositionClient, _entry_order_info, _fresh_signal_payload, engine, fake_redis, mock_client

@pytest.mark.asyncio
async def test_p0_01_hedge_failure_rolls_back_spot_leg(fake_redis):
    """BUG-02/P0-01: a failed hedge must market-sell the unhedged spot leg back."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = FailingHedgeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    pubsub = fake_redis.pubsub()
    await pubsub.subscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

    await engine._check_entry_orders()

    # No position recorded, but the spot leg was sold back
    assert pair_tag not in engine.active_positions
    assert len(client.placed_sells) == 1
    assert client.placed_sells[0]["type"] == "market"
    assert client.placed_sells[0]["amount"] == 0.005
    assert engine.states[pair_tag] == PositionState.CLOSED

    msg = None
    for _ in range(10):
        m = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if m and m.get("type") == "message":
            msg = json.loads(m["data"])
            break
    assert msg is not None
    assert msg["event"] == "HEDGE_FAILED"
    assert msg["rollback_succeeded"] is True
    assert msg["symbol"] == "btc_idr"

@pytest.mark.asyncio
async def test_p0_01_hedge_failure_with_failed_rollback_retries_and_alerts(fake_redis):
    """Rollback retries 5 times and emits a manual-intervention alert when it cannot sell."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = BlockingSellClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    attempts = 0
    original = client.place_local_market_sell

    async def counting_sell(symbol, amount, client_order_id=None):
        nonlocal attempts
        attempts += 1
        return await original(symbol, amount, client_order_id=client_order_id)

    client.place_local_market_sell = counting_sell

    pubsub = fake_redis.pubsub()
    await pubsub.subscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

    await engine._check_entry_orders()

    assert attempts == 5
    # A failed rollback leaves coins unhedged: the pair must surface as FAILED,
    # not as a clean CLOSED (P1-04).
    assert engine.states[pair_tag] == PositionState.FAILED
    assert pair_tag not in engine.active_positions
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "HEDGE_FAILED"

    msg = None
    for _ in range(10):
        m = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if m and m.get("type") == "message":
            msg = json.loads(m["data"])
            break
    assert msg is not None
    assert msg["event"] == "HEDGE_FAILED"
    assert msg["rollback_succeeded"] is False
    assert "local spot exchange unavailable" in msg["rollback_error"]

@pytest.mark.asyncio
async def test_p0_hedge_ambiguity_open_short_blocks_rollback(engine, fake_redis):
    """Timeout hedge with an actually-open short must NOT market-sell the spot."""
    pair_tag = "indodax:btc_idr"
    client = OpenShortPositionClient()
    client.amount_unused = 0.01
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    order_info = {
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
    }

    await engine._rollback_hedge_failure(pair_tag, order_info, 0.01, 1e9, 16000.0, RuntimeError("timeout"))

    assert len(client.placed_sells) == 0
    assert engine.states[pair_tag] == PositionState.FAILED
    assert engine._failure_reasons[pair_tag]["reason"] == "HEDGE_STATE_UNKNOWN"

@pytest.mark.asyncio
async def test_p0_hedge_ambiguity_zero_position_rolls_back(engine, fake_redis):
    """Confirmed zero futures position proceeds with the spot rollback."""
    pair_tag = "indodax:btc_idr"
    client = FailingHedgeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    order_info = {
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
    }

    await engine._rollback_hedge_failure(pair_tag, order_info, 0.01, 1e9, 16000.0, RuntimeError("timeout"))

    assert len(client.placed_sells) == 1

@pytest.mark.asyncio
async def test_hedge_ambiguity_binance_position_amt_blocks_rollback(engine, fake_redis):
    """Binance signed positionAmt must be treated as an open short, not flat."""
    class BinancePositionClient(FailingHedgeClient):
        async def fetch_positions(self, symbol: str):
            return [{"symbol": symbol, "info": {"positionAmt": "-0.01"}}]

    client = BinancePositionClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    order_info = {
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
    }

    await engine._rollback_hedge_failure(
        "indodax:btc_idr", order_info, 0.01, 1e9, 16000.0, RuntimeError("timeout")
    )

    assert client.placed_sells == []
    assert engine.states["indodax:btc_idr"] == PositionState.FAILED
    assert engine._failure_reasons["indodax:btc_idr"]["reason"] == "HEDGE_STATE_UNKNOWN"


@pytest.mark.asyncio
async def test_partial_spot_rollback_does_not_release_reserve_or_close(engine, fake_redis):
    """A partial market sell is not a completed rollback and must remain FAILED."""
    class PartialRollbackClient(FailingHedgeClient):
        async def place_local_market_sell(self, symbol, amount, client_order_id=None):
            return {
                "id": "partial-1",
                "symbol": symbol,
                "status": "closed",
                "filled": amount / 2,
                "average": 1000.0,
            }

    client = PartialRollbackClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    order_info = {
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
    }

    await engine._rollback_hedge_failure(
        "indodax:btc_idr", order_info, 0.01, 1e9, 16000.0, RuntimeError("hedge rejected")
    )

    assert engine.states["indodax:btc_idr"] == PositionState.FAILED
    assert engine._failure_reasons["indodax:btc_idr"]["reason"] == "HEDGE_FAILED"


@pytest.mark.asyncio
async def test_p0_hedge_ambiguity_query_failure_blocks_rollback(engine, fake_redis):
    """If the position query itself fails, rollback must be blocked (fail-safe)."""
    pair_tag = "indodax:btc_idr"

    class BrokenPositionClient(FailingHedgeClient):
        async def fetch_positions(self, symbol: str):
            raise RuntimeError("position endpoint down")

    client = BrokenPositionClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    order_info = {
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
    }

    await engine._rollback_hedge_failure(pair_tag, order_info, 0.01, 1e9, 16000.0, RuntimeError("timeout"))

    assert len(client.placed_sells) == 0
    assert engine.states[pair_tag] == PositionState.FAILED

@pytest.mark.asyncio
async def test_p0_03_startup_recovery_restores_active_positions(fake_redis, mock_client):
    """BUG-03/P0-03: positions persisted in Redis must be reloaded on initialise()."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=50.0,
    )
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=mock_client, redis_client=fake_redis
    )
    assert engine.active_positions == {}

    await engine.initialize()

    assert pair_tag in engine.active_positions
    restored = engine.active_positions[pair_tag]
    assert restored.qty == 0.005
    assert restored.short_entry_usdt == 65_000.0
    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE

@pytest.mark.asyncio
async def test_p0_03_recovery_preserves_reconciliation_required_state(fake_redis, mock_client):
    """Positions persisted with RECONCILIATION_REQUIRED must not be treated as HEDGED_ACTIVE on recovery."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=50.0,
        state="RECONCILIATION_REQUIRED",
    )
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=mock_client, redis_client=fake_redis
    )
    await engine.initialize()

    assert pair_tag in engine.active_positions
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "RESTORED_RECONCILIATION_REQUIRED"

@pytest.mark.asyncio
async def test_p0_03_recovery_retains_zero_qty_position_for_reconciliation(fake_redis, mock_client):
    """A zero Redis quantity is not proof the exchange has no residual exposure."""
    pair_tag = "indodax:btc_idr"
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.0,
    )
    await fake_redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=mock_client, redis_client=fake_redis
    )
    await engine.initialize()

    assert pair_tag in engine.active_positions
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is not None

@pytest.mark.asyncio
async def test_p0_pending_entry_persisted_and_cleared(engine, fake_redis):
    """A placed entry order is persisted to Redis and cleared when resolved."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    await engine.handle_signal_notification(pair_tag)

    raw = await fake_redis.get("arb:pending_entry:indodax:btc_idr")
    assert raw is not None
    stored = json.loads(raw)
    assert stored["client_order_id"] == list(engine.pending_entry_orders.values())[0]["client_order_id"]

    # Resolve the order (closed) -> persisted record must be removed.
    order_id = list(engine.pending_entry_orders.keys())[0]
    engine.pending_entry_orders[order_id]["order_id"] = "1"
    engine.orders_pending = None
    mock_order = engine.pending_entry_orders[order_id]
    mock_order["order_id"] = "501"
    mock_client_order = mock_order
    client = engine.client
    client.orders["501"] = {"id": "501", "status": "closed", "filled": 0.5}
    await engine._check_entry_orders()
    assert await fake_redis.get("arb:pending_entry:indodax:btc_idr") is None

@pytest.mark.asyncio
async def test_p0_pending_entry_adopted_on_startup(engine, fake_redis, mock_client):
    """A persisted pending entry still open on the venue is re-adopted at startup."""
    order_info = {
        "order_id": "777",
        "client_order_id": "indodax-btc-idr-entry-5",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1_000_000_000.0,
        "qty": 0.0001,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "placed_time": time.time(),
        "spot_cost_usdt": 10.0,
        "futures_margin_usdt": 10.0,
    }
    await fake_redis.set("arb:pending_entry:indodax:btc_idr", json.dumps(order_info))
    mock_client.orders["777"] = {"id": "777", "status": "open", "filled": 0.0}

    await engine._recover_pending_entry_orders()

    assert "indodax:btc_idr__binance:btc_usdt" in engine.pending_entry_orders
    assert engine.states["indodax:btc_idr__binance:btc_usdt"] == PositionState.ENTRY_PENDING
    # Re-adoption must remain durable across another process restart.
    persisted = await fake_redis.get("arb:pending_entry:indodax:btc_idr__binance:btc_usdt")
    assert persisted is not None
    assert json.loads(persisted)["order_id"] == "777"

@pytest.mark.asyncio
async def test_p0_pending_entry_closed_on_venue_cleared_at_startup(engine, fake_redis, mock_client):
    """A persisted pending entry no longer open on the venue is cleaned up."""
    order_info = {
        "order_id": "888",
        "symbol": "btc_idr",
        "placed_time": time.time(),
        "spot_cost_usdt": 10.0,
        "futures_margin_usdt": 10.0,
    }
    await fake_redis.set("arb:pending_entry:indodax:btc_idr", json.dumps(order_info))
    mock_client.orders["888"] = {"id": "888", "status": "canceled", "filled": 0.0}

    await engine._recover_pending_entry_orders()

    assert "indodax:btc_idr" not in engine.pending_entry_orders
    assert await fake_redis.get("arb:pending_entry:indodax:btc_idr") is None

@pytest.mark.asyncio
async def test_p0_order_seq_seeded_from_clock(mock_client):
    """A fresh engine must not start its client-order-id sequence at 1."""
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=mock_client, redis_client=fake_aioredis.FakeRedis(decode_responses=True)
    )
    assert engine._order_seq > 1000

@pytest.mark.asyncio
async def test_emergency_cancel_on_negative_funding(engine, fake_redis, mock_client):
    """Test Emergency Cancel: Cancels pending entry order on negative funding signal."""
    pair_tag = "indodax:btc_idr"

    engine.pending_entry_orders[pair_tag] = {
        "order_id": "401",
        "symbol": "btc_idr",
        "price": 1000000000.0,
        "qty": 0.01,
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["401"] = {"id": "401", "symbol": "btc_idr", "status": "open"}

    cancel_msg = json.dumps({
        "action": "CANCEL_ALL_NEGATIVE_FUNDING",
        "pair_key": pair_tag,
        "funding_rate": -0.0001,
    })
    await engine.handle_emergency_cancel(cancel_msg)

    assert ("401", "btc_idr") in mock_client.cancelled_orders
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.IDLE

@pytest.mark.asyncio
async def test_engine_lifecycle_start_stop(engine):
    """Test engine start and stop coroutine lifecycle to prevent TypeError."""
    await engine.start()
    assert engine._running is True
    assert len(engine._tasks) == 6
    await engine.stop()
    assert engine._running is False

@pytest.mark.asyncio
async def test_p2_03_retry_with_backoff_retries_until_success():
    from executor.engine import retry_with_backoff

    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    result = await retry_with_backoff(flaky, max_retries=4, base_delay=0.0)

    assert result == "ok"
    assert calls["n"] == 3

@pytest.mark.asyncio
async def test_p2_03_retry_with_backoff_raises_after_exhaustion():
    from executor.engine import RetryExhausted, retry_with_backoff

    calls = {"n": 0}

    async def always_fails():
        calls["n"] += 1
        raise RuntimeError(f"boom-{calls['n']}")

    with pytest.raises(RetryExhausted) as exc_info:
        await retry_with_backoff(always_fails, max_retries=2, base_delay=0.0)

    # max_retries=2 means 3 total attempts
    assert calls["n"] == 3
    assert exc_info.value.attempts == 3
    assert "boom-3" in str(exc_info.value.last_error)


@pytest.mark.asyncio
async def test_p0_pending_entry_terminal_partial_fill_is_hedged_on_recovery(engine, fake_redis, mock_client):
    """A canceled order with a final partial fill must hedge that fill before clearing state."""
    pair_tag = "indodax:btc_idr__binance:btc_usdt"
    order_info = {
        "order_id": "889",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1_000_000_000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "global_ask_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "next_funding_time_ms": int(time.time() * 1000) + 3_600_000,
        "base_target_exit_spread_pct": 0.3,
        "current_funding_rate": 0.0003,
        "tick_size": 1.0,
        "spot_cost_usdt": 625.0,
        "futures_margin_usdt": 625.0,
    }
    await fake_redis.set("arb:pending_entry:" + pair_tag, json.dumps(order_info))
    mock_client.orders["889"] = {
        "id": "889", "symbol": "btc_idr", "status": "canceled", "filled": 0.004
    }

    await engine._recover_pending_entry_orders()

    assert pair_tag in engine.active_positions
    assert engine.active_positions[pair_tag].qty == pytest.approx(0.004)
    assert len(mock_client.global_short_sells) == 1
    assert mock_client.global_short_sells[0]["amount"] == pytest.approx(0.004)
    assert await fake_redis.get("arb:pending_entry:" + pair_tag) is None


@pytest.mark.asyncio
async def test_p0_pending_entry_unknown_venue_state_is_retained_and_blocked(engine, fake_redis, mock_client):
    """An unresolvable venue order must remain durable and block the pair."""
    pair_tag = "indodax:btc_idr"
    order_info = {
        "order_id": "890",
        "symbol": "btc_idr",
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
    }
    await fake_redis.set("arb:pending_entry:" + pair_tag, json.dumps(order_info))

    async def fetch_unknown(order_id, symbol):
        raise TimeoutError("exchange API unavailable")

    mock_client.fetch_local_order = fetch_unknown
    await engine._recover_pending_entry_orders()

    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert pair_tag in engine.pending_entry_orders
    assert await fake_redis.get("arb:pending_entry:" + pair_tag) is not None
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "ENTRY_ORDER_STATE_UNKNOWN"


@pytest.mark.asyncio
async def test_partial_futures_hedge_fill_is_not_recorded_as_fully_hedged(fake_redis):
    """A partial futures fill blocks spot rollback and forces manual reconciliation."""
    client = OpenShortPositionClient()
    client.amount_unused = 0.002

    async def partial_hedge(symbol, amount, price=0.0, client_order_id=None):
        return {
            "id": "partial-hedge",
            "symbol": symbol,
            "status": "closed",
            "amount": amount,
            "filled": 0.002,
            "average": price,
        }

    client.open_global_short_market = partial_hedge
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True),
        client=client,
        redis_client=fake_redis,
    )
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    await engine._execute_entry_hedge(
        pair_tag,
        order_info,
        unhedged_delta=0.005,
        price_idr=1_000_000_000.0,
        usdt_rate=16_000.0,
    )

    assert pair_tag not in engine.active_positions
    assert client.placed_sells == []
    assert engine.states[pair_tag] == PositionState.FAILED
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "HEDGE_STATE_UNKNOWN"


@pytest.mark.asyncio
async def test_live_startup_recovery_verifies_futures_short_quantity(fake_redis):
    """A restored live position is active only when the exchange confirms its short size."""
    client = MockExchangeClient()
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    async def matching_short(symbol):
        return [{"symbol": symbol, "contracts": 0.005, "contractSize": 1.0, "side": "short"}]
    client.fetch_positions = matching_short
    await fake_redis.set("arb:active_pos:indodax:btc_idr", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False), client=client, redis_client=fake_redis
    )
    await engine.initialize()

    assert engine.states["indodax:btc_idr"] == PositionState.HEDGED_ACTIVE


@pytest.mark.asyncio
async def test_live_startup_recovery_blocks_position_quantity_mismatch(fake_redis):
    """A Redis snapshot must not resume automatic exits if exchange size differs."""
    client = MockExchangeClient()
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    async def mismatched_short(symbol):
        return [{"symbol": symbol, "contracts": 0.003, "contractSize": 1.0, "side": "short"}]
    client.fetch_positions = mismatched_short
    await fake_redis.set("arb:active_pos:indodax:btc_idr", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False), client=client, redis_client=fake_redis
    )
    await engine.initialize()

    assert engine.states["indodax:btc_idr"] == PositionState.RECONCILIATION_REQUIRED
    restored = json.loads(await fake_redis.get("arb:active_pos:indodax:btc_idr"))
    assert restored["state"] == "RECONCILIATION_REQUIRED"
    assert engine.status_snapshot()["failures"]["indodax:btc_idr"]["reason"] == "RECOVERY_POSITION_MISMATCH"


@pytest.mark.asyncio
async def test_malformed_active_position_record_is_retained_and_force_close_blocked(fake_redis):
    """A corrupted snapshot must never be mistaken for an empty/flat position."""
    client = MockExchangeClient()
    pair_tag = "indodax:btc_idr"
    key = "arb:active_pos:" + pair_tag
    await fake_redis.set(key, "{invalid-json")

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    await engine.initialize()

    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert await fake_redis.get(key) == "{invalid-json"

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert client.global_close_buys == []
    assert client.placed_sells == []
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_fetch_positions_rejects_missing_symbol_in_exchange_payload():
    """A malformed non-empty positions response must not be mistaken for flat."""
    from executor.exchange_client import ExchangeClient

    class GlobalFutures:
        async def fetch_positions(self, symbols=None):
            return [{"contracts": 0.01, "contractSize": 1.0, "side": "short"}]

    client = ExchangeClient(local_exchange=object(), global_exchange=GlobalFutures())
    with pytest.raises(ValueError, match="missing its symbol"):
        await client.fetch_positions("BTC/USDT:USDT")


@pytest.mark.asyncio
async def test_fetch_positions_returns_empty_only_for_valid_empty_payload():
    """An actual empty exchange response remains the only valid empty result."""
    from executor.exchange_client import ExchangeClient

    class GlobalFutures:
        async def fetch_positions(self, symbols=None):
            return []

    client = ExchangeClient(local_exchange=object(), global_exchange=GlobalFutures())
    assert await client.fetch_positions("BTC/USDT:USDT") == []


@pytest.mark.asyncio
async def test_live_exchange_balance_snapshot_normalizes_idr_to_usdt():
    """Live sizing converts local IDR quote balance using the current scanner FX rate."""
    from executor.exchange_client import ExchangeClient

    class LocalSpot:
        async def fetch_balance(self):
            return {
                "free": {"IDR": 1_600_000.0},
                "used": {"IDR": 400_000.0},
                "total": {"IDR": 2_000_000.0},
            }

    class GlobalFutures:
        async def fetch_balance(self):
            return {
                "free": {"USDT": 75.0},
                "used": {"USDT": 25.0},
                "total": {"USDT": 100.0},
            }

    client = ExchangeClient(LocalSpot(), GlobalFutures())
    balances = await client.get_balances(
        local_quote_asset="IDR",
        global_quote_asset="USDT",
        usdt_idr_rate=16_000.0,
    )

    assert balances["spot_available"] == pytest.approx(100.0)
    assert balances["spot_total"] == pytest.approx(125.0)
    assert balances["futures_available"] == pytest.approx(75.0)
    assert balances["futures_total"] == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_live_exchange_balance_snapshot_requires_live_idr_fx():
    """Missing FX must fail closed instead of treating IDR as USDT."""
    from executor.exchange_client import ExchangeClient

    class LocalSpot:
        async def fetch_balance(self):
            return {"free": {"IDR": 1_600_000.0}, "total": {"IDR": 1_600_000.0}}

    class GlobalFutures:
        async def fetch_balance(self):
            return {"free": {"USDT": 100.0}, "total": {"USDT": 100.0}}

    client = ExchangeClient(LocalSpot(), GlobalFutures())
    with pytest.raises(ValueError, match="live USDT/IDR rate"):
        await client.get_balances(
            local_quote_asset="IDR",
            global_quote_asset="USDT",
            usdt_idr_rate=0.0,
        )


@pytest.mark.asyncio
async def test_fetch_positions_matches_ccxt_contract_symbol_aliases():
    """Equivalent CCXT futures symbols must not be mistaken for an empty position."""
    from executor.exchange_client import ExchangeClient

    class GlobalFutures:
        async def fetch_positions(self, symbols=None):
            return [{
                "symbol": "BTC/USDT:USDT",
                "contracts": 0.005,
                "contractSize": 1.0,
                "side": "short",
            }]

    client = ExchangeClient(local_exchange=object(), global_exchange=GlobalFutures())
    positions = await client.fetch_positions("BTCUSDT")
    assert len(positions) == 1
    assert positions[0]["side"] == "short"


@pytest.mark.asyncio
async def test_fetch_balance_report_returns_all_assets_and_isolates_venue_errors():
    from executor.exchange_client import ExchangeClient

    class LocalSpot:
        id = "indodax"

        async def fetch_balance(self):
            return {
                "free": {"IDR": 1_200_000.0, "BTC": 0.01, "XRP": 5.0},
                "used": {"IDR": 300_000.0, "BTC": 0.002},
                "total": {"IDR": 1_500_000.0, "BTC": 0.012, "XRP": 8.0},
            }

    class GlobalFutures:
        id = "okx"

        async def fetch_balance(self):
            raise RuntimeError("private API unavailable")

    reports = await ExchangeClient(LocalSpot(), GlobalFutures()).fetch_balance_report()

    assert reports[0]["name"] == "indodax"
    assert reports[0]["status"] == "ok"
    assert {item["asset"] for item in reports[0]["assets"]} == {"IDR", "BTC", "XRP"}
    btc = next(item for item in reports[0]["assets"] if item["asset"] == "BTC")
    assert btc == {"asset": "BTC", "free": 0.01, "used": 0.002, "total": 0.012}
    xrp = next(item for item in reports[0]["assets"] if item["asset"] == "XRP")
    assert xrp == {"asset": "XRP", "free": 5.0, "used": 3.0, "total": 8.0}
    assert reports[1]["name"] == "okx"
    assert reports[1]["status"] == "error"
    assert "private API unavailable" in reports[1]["error"]


@pytest.mark.asyncio
async def test_paper_balance_report_is_explicitly_simulated():
    from executor.exchange_client import PaperTradingExchangeClient

    reports = await PaperTradingExchangeClient().fetch_balance_report()

    assert len(reports) == 2
    assert all(report["status"] == "ok" for report in reports)
    assert all(report["assets"][0]["asset"] == "USDT" for report in reports)
