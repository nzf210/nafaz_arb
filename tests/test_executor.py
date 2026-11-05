import asyncio
import json
import pytest
import time
from unittest.mock import AsyncMock, MagicMock
import fakeredis.aioredis as fake_aioredis

from executor.config import ExecutorConfig
from executor.exchange_client import ExchangeClient
from executor.engine import ArbitrageEngine, build_client_order_id, clean_pair_key, round_to_tick
from executor.models import (
    PositionState,
    PairInfo,
    PricingSnapshot,
    FundingMetrics,
    ExecutionDirectives,
    ExecutorPayload,
    ActivePosition,
)


class MockExchangeClient:
    """Mock Exchange Client implementing required methods."""

    def __init__(self):
        self.orders = {}
        self.order_counter = 100
        self.placed_buys = []
        self.placed_sells = []
        self.cancelled_orders = []
        self.global_short_sells = []
        self.global_close_buys = []
        self.market_prices = {}
        self.client_order_ids = []

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id=None):
        self.order_counter += 1
        order_id = str(self.order_counter)
        order = {
            "id": order_id,
            "symbol": symbol,
            "side": "buy",
            "type": "limit",
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "status": "open",
        }
        order["client_order_id"] = client_order_id
        self.client_order_ids.append(client_order_id)
        self.orders[order_id] = order
        self.placed_buys.append(order)
        return order

    async def place_local_limit_sell(self, symbol: str, amount: float, price: float, client_order_id=None):
        self.order_counter += 1
        order_id = str(self.order_counter)
        order = {
            "id": order_id,
            "symbol": symbol,
            "side": "sell",
            "type": "limit",
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "status": "open",
        }
        self.orders[order_id] = order
        self.placed_sells.append(order)
        return order

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id=None):
        self.order_counter += 1
        order_id = str(self.order_counter)
        order = {
            "id": order_id,
            "symbol": symbol,
            "side": "sell",
            "type": "market",
            "amount": amount,
            "price": 1000.0,
            "average": 1000.0,
            "filled": amount,
            "status": "closed",
        }
        order["client_order_id"] = client_order_id
        self.client_order_ids.append(client_order_id)
        self.orders[order_id] = order
        self.placed_sells.append(order)
        return order

    async def cancel_local_order(self, order_id: str, symbol: str):
        self.cancelled_orders.append((order_id, symbol))
        if order_id in self.orders:
            self.orders[order_id]["status"] = "canceled"
        return {"id": order_id, "status": "canceled"}

    async def fetch_local_order(self, order_id: str, symbol: str):
        if order_id in self.orders:
            return self.orders[order_id]
        return {"id": order_id, "status": "open", "filled": 0.0}

    async def fetch_positions(self, symbol: str):
        # Default mock: no futures positions open anywhere.
        return []

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        record = {
            "symbol": symbol,
            "side": "sell",
            "amount": amount,
            "price": price,
            "average": price,
            "client_order_id": client_order_id,
        }
        self.global_short_sells.append(record)
        return record

    async def close_global_short_market(self, symbol: str, amount: float, client_order_id=None):
        record = {
            "symbol": symbol,
            "side": "buy",
            "amount": amount,
            "price": 65050.0,
            "params": {"reduceOnly": True},
            "client_order_id": client_order_id,
        }
        self.global_close_buys.append(record)
        return record

    def update_market_price(self, symbol: str, best_bid: float, best_ask: float, last_price: float = 0.0):
        clean = symbol.lower().replace("/", "_").replace("-", "_")
        self.market_prices[clean] = {"bid": float(best_bid), "ask": float(best_ask)}

    async def fetch_global_ticker(self, symbol: str):
        clean = symbol.lower().replace("/", "_").replace("-", "_")
        price = self.market_prices.get(clean) or {}
        return {"symbol": symbol, "bid": price.get("bid", 0.0), "ask": price.get("ask", 0.0)}

    async def close(self):
        pass


@pytest.fixture
def fake_redis():
    return fake_aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def mock_client():
    return MockExchangeClient()


@pytest.fixture
def engine(fake_redis, mock_client):
    cfg = ExecutorConfig(
        max_stale_latency_ms=500,
        min_partial_fill_usd=10.0,
        exit_price_move_threshold=0.0015,
    )
    return ArbitrageEngine(config=cfg, client=mock_client, redis_client=fake_redis)


@pytest.mark.asyncio
async def test_state1_entry_order_pegging_fresh_vs_stale(engine, fake_redis, mock_client):
    """Test State 1: Fresh signal places limit buy, stale signal (> 500ms) is rejected."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # 1. Fresh signal
    fresh_payload = {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "binance",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1000.0,
        },
        "pricing_snapshot": {
            "local_best_bid": 1000000000.0,
            "local_best_ask": 1002000000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms - 50,  # 50ms latency (fresh!)
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3600000,
            "minutes_to_next_funding": 60.0,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1000001000.0,
            "entry_spread_pct": 0.35,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 500.0,
            "cancel_if_global_bid_drops_below_usdt": 64800.0,
        },
    }

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(fresh_payload))
    await engine.handle_signal_notification(pair_tag)

    # Order should be placed on local exchange
    assert len(mock_client.placed_buys) == 1
    placed = mock_client.placed_buys[0]
    assert placed["symbol"] == "btc_idr"
    assert placed["price"] == 1000001000.0
    assert engine.states[pair_tag] == PositionState.ENTRY_PENDING

    # 2. Stale signal test (> 500ms)
    stale_tag = "indodax:eth_idr"
    stale_payload = dict(fresh_payload)
    stale_payload["pair_info"] = dict(fresh_payload["pair_info"])
    stale_payload["pair_info"]["local_symbol"] = "eth_idr"
    stale_payload["pricing_snapshot"] = dict(fresh_payload["pricing_snapshot"])
    stale_payload["pricing_snapshot"]["timestamp_ms"] = now_ms - 800  # 800ms old!

    await fake_redis.set(f"arb:signal:{stale_tag}", json.dumps(stale_payload))
    await engine.handle_signal_notification(stale_tag)

    # No new buy placed
    assert len(mock_client.placed_buys) == 1


@pytest.mark.asyncio
async def test_state1_adverse_selection_cancellation(engine, fake_redis, mock_client):
    """Test State 1: When Global Bid drops below cancel threshold, cancel local Limit Buy."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # Place entry order
    engine.pending_entry_orders[pair_tag] = {
        "order_id": "101",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1000001000.0,
        "qty": 0.008,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "cancel_threshold_usdt": 64800.0,
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["101"] = {
        "id": "101",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0,
    }

    # Simulate Global Bid dropping to 64700 (below 64800 threshold)
    live_snapshot = {
        "global_best_bid_usdt": 64700.0,
        "global_best_ask_usdt": 64710.0,
        "usdt_idr_rate": 16000.0,
        "timestamp_ms": now_ms,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(live_snapshot))

    # Run check
    await engine._check_entry_orders()

    # Verify order was cancelled
    assert ("101", "btc_idr") in mock_client.cancelled_orders
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.IDLE


@pytest.mark.asyncio
async def test_state2_partial_fill_and_instant_short_hedge(engine, fake_redis, mock_client):
    """
    Test State 2: When local order has partial fill >= $10,
    immediately executes Market Sell (Short 1x) on Global Futures for exact coin qty!
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.pending_entry_orders[pair_tag] = {
        "order_id": "201",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1000000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "cancel_threshold_usdt": 60000.0,
        "usdt_idr_rate": 16000.0,
        "next_funding_time_ms": now_ms + 3600000,
        "base_target_exit_spread_pct": 0.3,
        "current_funding_rate": 0.0003,
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Local order receives partial fill of 0.005 BTC (~$312.50 USD > $10)
    mock_client.orders["201"] = {
        "id": "201",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.005,
    }

    await engine._check_entry_orders()

    # Verify Global Market Short executed for exact 0.005 BTC
    assert len(mock_client.global_short_sells) == 1
    short_exec = mock_client.global_short_sells[0]
    assert short_exec["symbol"] == "BTCUSDT"
    assert short_exec["side"] == "sell"
    assert short_exec["amount"] == 0.005

    # Verify active position created and written to Redis arb:active_pos:indodax:btc_idr
    active_raw = await fake_redis.get(f"arb:active_pos:{pair_tag}")
    assert active_raw is not None
    pos_data = json.loads(active_raw)
    assert pos_data["qty"] == 0.005
    assert pos_data["symbol"] == "btc_idr"
    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE


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
async def test_engine_lifecycle_start_stop(engine):
    """Test engine start and stop coroutine lifecycle to prevent TypeError."""
    await engine.start()
    assert engine._running is True
    assert len(engine._tasks) == 5
    await engine.stop()
    assert engine._running is False


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
async def test_paper_trading_client_fill_only_when_price_touches():
    """
    Test PaperTradingExchangeClient:
    Verifies that mock orders DO NOT auto-fill on timer.
    They ONLY fill when the market price actually touches or crosses the limit order price.
    """
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")

    # 1. Place Limit Buy at Rp 1000.0
    buy_order = await client.place_local_limit_buy("btc_idr", 0.5, 1000.0)
    oid = buy_order["id"]

    # Market ask is Rp 1010.0 (higher than our buy limit) -> Order must stay OPEN, filled 0.0
    client.update_market_price("btc_idr", best_bid=995.0, best_ask=1010.0)
    fetched = await client.fetch_local_order(oid, "btc_idr")
    assert fetched["status"] == "open"
    assert fetched["filled"] == 0.0

    # Market ask drops to Rp 1000.0 (touches our limit buy) -> Order must be FILLED!
    client.update_market_price("btc_idr", best_bid=995.0, best_ask=1000.0)
    fetched = await client.fetch_local_order(oid, "btc_idr")
    assert fetched["status"] == "closed"
    assert fetched["filled"] == 0.5

    # 2. Place Limit Sell at Rp 1020.0
    sell_order = await client.place_local_limit_sell("btc_idr", 0.5, 1020.0)
    soid = sell_order["id"]

    # Market bid is Rp 1015.0 (below our sell limit) -> Order must stay OPEN, filled 0.0
    client.update_market_price("btc_idr", best_bid=1015.0, best_ask=1025.0)
    fetched_sell = await client.fetch_local_order(soid, "btc_idr")
    assert fetched_sell["status"] == "open"
    assert fetched_sell["filled"] == 0.0

    # Market bid rises to Rp 1020.0 (touches our limit sell) -> Order must be FILLED!
    client.update_market_price("btc_idr", best_bid=1020.0, best_ask=1025.0)
    fetched_sell = await client.fetch_local_order(soid, "btc_idr")
    assert fetched_sell["status"] == "closed"
    assert fetched_sell["filled"] == 0.5


@pytest.mark.asyncio
async def test_entry_order_ttl_expiration(engine, fake_redis, mock_client):
    """
    Test State 1 Order TTL Expiration:
    When a maker limit buy order is placed, but the market never touches it
    and max_order_ttl_seconds expires, the order must be cancelled cleanly.
    """
    pair_tag = "indodax:btc_idr"
    engine.config.max_order_ttl_seconds = 10

    # Order placed 15 seconds ago without fill
    engine.pending_entry_orders[pair_tag] = {
        "order_id": "701",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "price": 1000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "placed_time": time.time() - 15.0,  # 15s elapsed > 10s TTL
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["701"] = {
        "id": "701",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0,
    }

    await engine._check_entry_orders()

    # Order was cancelled due to TTL
    assert ("701", "btc_idr") in mock_client.cancelled_orders
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.IDLE


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
async def test_balance_constraint_and_realized_trade_updates(fake_redis):
    """
    Test $100 Balance per Exchange Constraint:
    1. Initial balance: Spot $100, Futures $100.
    2. Opens position 1 ($50): Locks $50 on Spot and Futures. Available = $50.
    3. Opens position 2 ($50): Locks $50 on Spot and Futures. Available = $0.
    4. Position 3 signal arrives: REJECTED due to $0 available balance!
    5. Position 1 exits with profit: Locked funds released, balances credited with PnL,
       and Redis arb:balances is updated with new equity.
    """
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(
        local_name="indodax",
        global_name="okx",
        initial_balance_usdt=100.0,
    )
    cfg = ExecutorConfig(
        max_stale_latency_ms=1000,
        min_partial_fill_usd=10.0,
        initial_exchange_balance_usdt=100.0,
        allocation_per_pair=0.5,  # 50% max per pair
        min_allocated_capital=10.0,
        # This test covers the capital-constraint flow, not fee handling: keep the
        # requested $50 budget intact so the locked-funds numbers stay exact.
        estimated_round_trip_fee_pct=0.0,
    )
    engine = ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis)

    now_ms = int(time.time() * 1000)

    # 1. Check initial balances
    bals = client.get_balances()
    assert bals["spot_total"] == 100.0
    assert bals["spot_available"] == 100.0
    assert bals["futures_total"] == 100.0
    assert bals["futures_available"] == 100.0

    # 2. First Signal for btc_idr ($50 position)
    sig1 = {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "okx",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1000.0,
        },
        "pricing_snapshot": {
            "local_best_bid": 1000000000.0,
            "local_best_ask": 1001000000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3600000,
            "minutes_to_next_funding": 60.0,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1000001000.0,
            "entry_spread_pct": 0.35,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 50.0,
            "cancel_if_global_bid_drops_below_usdt": 64800.0,
        },
    }
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig1))
    await engine.handle_signal_notification("indodax:btc_idr")

    # Order 1 placed -> Funds locked
    bals1 = client.get_balances()
    assert bals1["spot_locked"] == 50.0
    assert bals1["futures_locked"] == 50.0
    assert bals1["spot_available"] == 50.0
    assert bals1["futures_available"] == 50.0

    # 3. Second Signal for eth_idr ($50 position)
    sig2 = dict(sig1)
    sig2["pair_info"] = dict(sig1["pair_info"])
    sig2["pair_info"]["local_symbol"] = "eth_idr"
    sig2["pair_info"]["global_symbol"] = "ETHUSDT"
    sig2["execution_directives"] = dict(sig1["execution_directives"])
    sig2["execution_directives"]["recommended_limit_buy_idr"] = 50000000.0
    sig2["execution_directives"]["max_position_usdt"] = 50.0

    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig2))
    await engine.handle_signal_notification("indodax:eth_idr")

    bals2 = client.get_balances()
    assert bals2["spot_locked"] == 100.0
    assert bals2["futures_locked"] == 100.0
    assert bals2["spot_available"] == 0.0
    assert bals2["futures_available"] == 0.0

    # 4. Third Signal for sol_idr ($50 position) -> MUST BE REJECTED!
    sig3 = dict(sig1)
    sig3["pair_info"] = dict(sig1["pair_info"])
    sig3["pair_info"]["local_symbol"] = "sol_idr"
    sig3["pair_info"]["global_symbol"] = "SOLUSDT"

    await fake_redis.set("arb:signal:indodax:sol_idr", json.dumps(sig3))
    await engine.handle_signal_notification("indodax:sol_idr")

    # Verify 3rd order was NOT placed
    assert "indodax:sol_idr" not in engine.pending_entry_orders
    assert "indodax:sol_idr" not in engine.states

    # 5. Simulate fill and close on position 1 (btc_idr) with positive realized PnL
    # Put live market price in Redis touching the Limit Buy price
    await fake_redis.set(
        "arb:live_price:indodax:btc_idr",
        json.dumps({
            "local_best_bid": 999000000.0,
            "local_best_ask": 1000001000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        }),
    )
    await engine._check_entry_orders()
    assert "indodax:btc_idr" in engine.active_positions

    # Trigger Limit Sell placement & calculate exit price
    await engine._check_dynamic_exits()
    assert "indodax:btc_idr" in engine.pending_exit_orders

    exit_price = engine.pending_exit_orders["indodax:btc_idr"]["price"]

    # Market bid in Redis rises to exit_price -> Fills Limit Sell!
    await fake_redis.set(
        "arb:live_price:indodax:btc_idr",
        json.dumps({
            "local_best_bid": exit_price,
            "local_best_ask": exit_price + 1000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65000.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        }),
    )
    await engine._check_exit_orders()

    # Position 1 is now closed
    assert "indodax:btc_idr" not in engine.active_positions

    # 6. Check updated balances after trade completion:
    # Locked funds for btc_idr (50 USDT) were released, spot & futures received realized gains
    final_bals = client.get_balances()
    assert final_bals["spot_locked"] == 50.0  # Only eth_idr still locked
    assert final_bals["spot_available"] > 50.0  # Spot gained profit!
    assert final_bals["futures_available"] > 40.0  # Futures unlocked 50 USDT minus basis delta
    assert final_bals["total_equity"] > 200.0  # Overall portfolio equity grew!

    # Verify Redis arb:balances is present
    redis_bals_raw = await fake_redis.get("arb:balances")
    assert redis_bals_raw is not None
    redis_bals = json.loads(redis_bals_raw)
    assert redis_bals["total_equity"] > 200.0






class FailingHedgeClient(MockExchangeClient):
    """Client whose futures short always fails, to exercise the hedge rollback path."""

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        raise RuntimeError("global futures unavailable")


class BlockingSellClient(MockExchangeClient):
    """Client whose hedge AND emergency spot sell always fail, simulating total liquidity loss."""

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        raise RuntimeError("global futures unavailable")

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id=None):
        raise RuntimeError("local spot exchange unavailable")


class SpotSellFailureClient(MockExchangeClient):
    """Client whose futures work but whose spot market sell always fails."""

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id=None):
        raise RuntimeError("local spot exchange unavailable")


def _entry_order_info(pair_tag: str, now_ms: int) -> dict:
    return {
        "order_id": "301",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1000000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "cancel_threshold_usdt": 60000.0,
        "tick_size": 1.0,
        "usdt_idr_rate": 16000.0,
        "global_ask_usdt": 65000.0,
        "next_funding_time_ms": now_ms + 3600000,
        "base_target_exit_spread_pct": 0.3,
        "current_funding_rate": 0.0003,
        "spot_cost_usdt": 50.0,
        "futures_margin_usdt": 50.0,
    }


@pytest.mark.asyncio
async def test_p0_04_short_entry_uses_futures_ask_not_spot_price(engine, fake_redis, mock_client):
    """BUG-01/P0-04: the recorded short entry must come from the futures ask, never spot."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    pos = engine.active_positions[pair_tag]
    # spot price would be 1_000_000_000 / 16_000 = 62_500 USDT; futures ask is 65_000
    assert pos.short_entry_usdt == 65_000.0
    assert mock_client.global_short_sells[0]["price"] == 65_000.0


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
async def test_p0_03_recovery_drops_zero_qty_position(fake_redis, mock_client):
    """Recovery must not resurrect a degenerate zero-quantity position."""
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

    assert pair_tag not in engine.active_positions
    assert await fake_redis.get(f"arb:active_pos:{pair_tag}") is None


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
    """A failed spot liquidation must be flagged in trade history for operators."""
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

    history = await fake_redis.lrange("arb:trade_history", 0, 0)
    trade = json.loads(history[0])
    assert trade["spot_liquidation_failed"] is True
    # P0 safety: Position must NOT be deleted from active positions or Redis when liquidation fails
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
async def test_paper_client_market_sell_fills_at_bid():
    """P0-01/P0-05 helper: paper client market sell must fill immediately at the live bid."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    client.update_market_price("btc_idr", best_bid=999_000_000.0, best_ask=1_000_000_000.0)

    order = await client.place_local_market_sell("btc_idr", 0.25)

    assert order["type"] == "market"
    assert order["side"] == "sell"
    assert order["status"] == "closed"
    assert order["filled"] == 0.25
    assert order["average"] == 999_000_000.0
    assert order["price"] == 999_000_000.0


@pytest.mark.asyncio
async def test_paper_client_market_sell_rejects_non_positive_amount():
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    with pytest.raises(ValueError):
        await client.place_local_market_sell("btc_idr", 0.0)


@pytest.mark.asyncio
async def test_paper_client_short_returns_reference_price():
    """P0-04: the paper client must echo back the caller-supplied futures price."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    trade = await client.open_global_short_market("BTCUSDT", 0.01, price=65_000.0)

    assert trade["price"] == 65_000.0
    assert trade["average"] == 65_000.0
    assert trade["expected_price"] == 65_000.0


@pytest.mark.asyncio
async def test_p0_04_missing_futures_price_aborts_and_rolls_back(fake_redis):
    """
    If the futures short returns no usable price, the engine must not fall back to
    the spot price. The hedge is aborted and the spot leg rolled back instead.
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    class NoPriceShortClient(MockExchangeClient):
        async def open_global_short_market(self, symbol, amount, price=0.0, client_order_id=None):
            record = {"symbol": symbol, "side": "sell", "amount": amount}
            self.global_short_sells.append(record)
            return record

    client = NoPriceShortClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert pair_tag not in engine.active_positions
    assert len(client.placed_sells) == 1
    assert engine.states[pair_tag] == PositionState.CLOSED


# ---------------------------------------------------------------------------
# Fase 1 (P1) regression tests
# ---------------------------------------------------------------------------


class SlowLimitBuyClient(MockExchangeClient):
    """Counts placed entry orders and yields control mid-placement.

    The yield is what makes the race reproducible: without a per-pair lock the
    second signal reads the empty pending list while the first is still inside
    place_local_limit_buy().
    """

    def __init__(self):
        super().__init__()
        self.entry_orders = 0

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id=None):
        self.entry_orders += 1
        await asyncio.sleep(0.05)
        return await super().place_local_limit_buy(symbol, amount, price)


def _fresh_signal_payload(now_ms: int) -> dict:
    return {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "binance",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1.0,
            "local_min_qty": 0.00001,
        },
        "pricing_snapshot": {
            "local_best_bid": 999_000_000.0,
            "local_best_ask": 1_000_000_000.0,
            "global_best_bid_usdt": 64_990.0,
            "global_best_ask_usdt": 65_000.0,
            "usdt_idr_rate": 16_000.0,
            "timestamp_ms": now_ms,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3_600_000,
            "minutes_to_next_funding": 60,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1_000_000_000.0,
            "entry_spread_pct": 0.25,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 100.0,
            "cancel_if_global_bid_drops_below_usdt": 60_000.0,
        },
    }


@pytest.mark.asyncio
async def test_p1_01_concurrent_signals_place_only_one_entry_order(fake_redis):
    """BUG-05/P1-01: two simultaneous signals must not double the spot position."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = SlowLimitBuyClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    await asyncio.gather(
        engine.handle_signal_notification(pair_tag),
        engine.handle_signal_notification(pair_tag),
        engine.handle_signal_notification(pair_tag),
    )

    assert client.entry_orders == 1
    assert len(engine.pending_entry_orders) == 1


@pytest.mark.asyncio
async def test_p1_03_daily_loss_limit_halts_new_entries(fake_redis):
    """BUG-08/P1-03: cumulative daily loss beyond the limit stops new entries."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_daily_loss_usdt=10.0),
        client=client,
        redis_client=fake_redis,
    )
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    # A loss under the limit must not halt anything.
    engine.record_realized_pnl(-4.0)
    assert engine._trading_halted is False

    await engine.handle_signal_notification(pair_tag)
    assert pair_tag in engine.pending_entry_orders
    client.entry_orders = 0

    # Crossing the limit halts entries.
    engine.record_realized_pnl(-7.0)
    assert engine._trading_halted is True
    assert engine.status_snapshot()["trading_halted"] is True
    assert engine.status_snapshot()["daily_realized_loss_usdt"] == pytest.approx(11.0)

    other_pair = "indodax:eth_idr"
    await fake_redis.set(f"arb:signal:{other_pair}", json.dumps(_fresh_signal_payload(now_ms)))
    await engine.handle_signal_notification(other_pair)
    assert other_pair not in engine.pending_entry_orders

    # Manual resume lets trading continue.
    await engine.resume_trading("operator reviewed")
    assert engine._trading_halted is False


@pytest.mark.asyncio
async def test_p1_03_profits_do_not_halt_and_zero_limit_disables(fake_redis):
    """A zero limit disables the breaker; profits never trip it."""
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_daily_loss_usdt=0.0),
        client=client,
        redis_client=fake_redis,
    )
    # A disabled limit never halts, however large the loss.
    engine.record_realized_pnl(-1_000.0)
    assert engine._trading_halted is False
    assert engine._daily_realized_loss_usdt == pytest.approx(1_000.0)

    # With a limit set, a profitable trade must not add to the loss tally.
    engine.config.max_daily_loss_usdt = 5.0
    engine._daily_realized_loss_usdt = 0.0
    engine.record_realized_pnl(100.0)
    assert engine._trading_halted is False
    assert engine._daily_realized_loss_usdt == 0.0


@pytest.mark.asyncio
async def test_p1_04_status_snapshot_reports_failures(fake_redis):
    """P1-04: a failed pair is queryable through the status snapshot."""
    client = MockExchangeClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    engine._mark_failed("indodax:btc_idr", "HEDGE_FAILED", "rollback failed", {"qty": 0.005})
    engine.states["indodax:eth_idr"] = PositionState.HEDGED_ACTIVE

    snapshot = engine.status_snapshot()
    assert snapshot["failed_pairs"] == 1
    assert snapshot["positions"]["indodax:btc_idr"]["state"] == "FAILED"
    assert snapshot["failures"]["indodax:btc_idr"]["reason"] == "HEDGE_FAILED"

    await engine.publish_status()
    raw = await fake_redis.get("arb:executor:status")
    assert raw is not None
    assert json.loads(raw)["failed_pairs"] == 1

    engine.clear_failure("indodax:btc_idr")
    assert engine.states["indodax:btc_idr"] == PositionState.IDLE
    assert engine.status_snapshot()["failed_pairs"] == 0


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
async def test_bug10_paper_client_close_reports_fill_price():
    """The paper client must report a close fill price so paper PnL stays honest."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    client.update_market_price("btc_usdt", best_bid=64_990.0, best_ask=65_010.0)

    order = await client.close_global_short_market("btc_usdt", 0.01)

    assert order["average"] == 65_010.0
    assert order["price"] == 65_010.0
    assert order["params"]["reduceOnly"] is True

# -------------------------------------------------------------
# Fase 2 — P2-01: ENTRY_PARTIAL state
# -------------------------------------------------------------
@pytest.mark.asyncio
async def test_p2_01_partial_fill_below_threshold_sets_entry_partial(engine, fake_redis, mock_client):
    """BUG-08/P2-01: below-threshold partial fills must be visible as ENTRY_PARTIAL."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    order_info = _entry_order_info(pair_tag, now_ms)
    # $3 of value is well under the $10 hedge threshold.
    order_info["qty"] = 0.1
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.000048,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.ENTRY_PARTIAL
    # Nothing was hedged, so no position and no order left the pending list.
    assert pair_tag not in engine.active_positions
    assert mock_client.global_short_sells == []
    assert pair_tag in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_p2_01_partial_fill_above_threshold_hedges_and_activates(engine, fake_redis, mock_client):
    """Over-threshold fills hedge immediately and never linger in ENTRY_PARTIAL."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE
    assert pair_tag in engine.active_positions

# -------------------------------------------------------------
# Fase 2 — P2-03: retry_with_backoff helper
# -------------------------------------------------------------
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
async def test_p2_03_force_close_uses_backoff_helper(fake_redis, monkeypatch):
    """Force close must go through the shared retry helper, not an inline loop."""
    import executor.engine as engine_module

    seen = {"n": 0}
    real_helper = engine_module.retry_with_backoff

    async def spy(fn, **kwargs):
        seen["n"] += 1
        return await real_helper(fn, **kwargs)

    monkeypatch.setattr(engine_module, "retry_with_backoff", spy)

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

# -------------------------------------------------------------
# Fase 2 — P2-02: entry slippage guard
# -------------------------------------------------------------
@pytest.mark.asyncio
async def test_p2_02_excessive_slippage_aborts_and_rolls_back(fake_redis):
    """An ask far above the expected short price must abort the hedge, not open a bad position."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    # Expected 65_000, config allows 0.3% -> limit 65_195. Simulate a 2% gap.
    client.update_market_price("btcusdt", best_bid=66_300.0, best_ask=66_300.0)
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.003),
        client=client,
        redis_client=fake_redis,
    )

    order_info = _entry_order_info(pair_tag, now_ms)
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    # No short was opened and the spot leg was sold back
    assert client.global_short_sells == []
    assert pair_tag not in engine.active_positions
    assert len(client.placed_sells) == 1
    assert client.placed_sells[0]["type"] == "market"
    assert order_info["hedged_short_qty"] == 0.0


@pytest.mark.asyncio
async def test_p2_02_acceptable_slippage_still_hedges(fake_redis):
    """A small gap inside the tolerance must not block a legitimate hedge."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    # 65_100 is +0.154%, inside the 0.3% limit.
    client.update_market_price("btcusdt", best_bid=65_090.0, best_ask=65_100.0)
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.003),
        client=client,
        redis_client=fake_redis,
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert len(client.global_short_sells) == 1
    assert pair_tag in engine.active_positions
    assert client.placed_sells == []


@pytest.mark.asyncio
async def test_p2_02_missing_live_ask_fails_open(fake_redis):
    """Without a readable live ask the guard must not block the hedge."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()  # no market price seeded -> ask == 0
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.003),
        client=client,
        redis_client=fake_redis,
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert len(client.global_short_sells) == 1
    assert pair_tag in engine.active_positions


@pytest.mark.asyncio
async def test_p2_02_zero_slippage_disables_guard(fake_redis):
    """max_entry_slippage_pct=0 disables the guard entirely."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    client.update_market_price("btcusdt", best_bid=80_000.0, best_ask=80_000.0)
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.0),
        client=client,
        redis_client=fake_redis,
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert len(client.global_short_sells) == 1

# -------------------------------------------------------------
# Fase 2 — P2-06: fee buffer before ordering
# -------------------------------------------------------------
def _fee_buffer_engine(fake_redis, fee_pct: float):
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(
        local_name="indodax", global_name="okx", initial_balance_usdt=100.0
    )
    cfg = ExecutorConfig(
        min_partial_fill_usd=10.0,
        initial_exchange_balance_usdt=100.0,
        min_allocated_capital=10.0,
        estimated_round_trip_fee_pct=fee_pct,
    )
    return ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis), client


@pytest.mark.asyncio
async def test_p2_06_fee_buffer_shrinks_locked_capital(fake_redis):
    """A 1% round-trip fee must reserve 1% of the budget instead of locking it all."""
    engine, client = _fee_buffer_engine(fake_redis, fee_pct=0.01)
    sig = _fresh_signal_payload(int(time.time() * 1000))
    sig["execution_directives"]["max_position_usdt"] = 50.0
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))

    await engine.handle_signal_notification("indodax:btc_idr")

    assert "indodax:btc_idr" in engine.pending_entry_orders
    # 50 * (1 - 0.01) = 49.5, floored to the lot size.
    reserved = engine.pending_entry_orders["indodax:btc_idr"]["spot_cost_usdt"]
    assert reserved <= 49.5 + 1e-9
    assert reserved > 49.0
    assert client.get_balances()["spot_locked"] == reserved


@pytest.mark.asyncio
async def test_p2_06_zero_fee_pct_disables_buffer(fake_redis):
    """estimated_round_trip_fee_pct=0 must lock the full requested budget."""
    engine, _ = _fee_buffer_engine(fake_redis, fee_pct=0.0)
    sig = _fresh_signal_payload(int(time.time() * 1000))
    sig["execution_directives"]["max_position_usdt"] = 50.0
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))

    await engine.handle_signal_notification("indodax:btc_idr")

    reserved = engine.pending_entry_orders["indodax:btc_idr"]["spot_cost_usdt"]
    assert reserved == pytest.approx(50.0, abs=0.01)


@pytest.mark.asyncio
async def test_p2_06_fee_buffer_can_drop_below_min_capital(fake_redis):
    """If the fee buffer pushes the size under the minimum, the entry is rejected."""
    engine, _ = _fee_buffer_engine(fake_redis, fee_pct=0.01)
    sig = _fresh_signal_payload(int(time.time() * 1000))
    sig["execution_directives"]["max_position_usdt"] = 10.0  # min capital is 10.0
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))

    await engine.handle_signal_notification("indodax:btc_idr")

    # 10.0 * 0.99 = 9.9 < 10.0 -> rejected rather than silently undersized
    assert "indodax:btc_idr" not in engine.pending_entry_orders

# -------------------------------------------------------------
# Fase 2 — P2-04: max exposure per exchange
# -------------------------------------------------------------
def _exposure_engine(fake_redis, cap: float):
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(
        local_name="indodax", global_name="okx", initial_balance_usdt=100.0
    )
    cfg = ExecutorConfig(
        min_partial_fill_usd=10.0,
        initial_exchange_balance_usdt=100.0,
        min_allocated_capital=10.0,
        max_exposure_per_exchange_usdt=cap,
        estimated_round_trip_fee_pct=0.0,
    )
    return ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis), client


def _half_budget_signal(local_symbol: str, global_symbol: str, now_ms: int) -> dict:
    sig = _fresh_signal_payload(now_ms)
    sig["pair_info"] = dict(sig["pair_info"])
    sig["pair_info"]["local_symbol"] = local_symbol
    sig["pair_info"]["global_symbol"] = global_symbol
    sig["execution_directives"] = dict(sig["execution_directives"])
    sig["execution_directives"]["max_position_usdt"] = 50.0
    return sig


@pytest.mark.asyncio
async def test_p2_04_exposure_cap_rejects_second_position(fake_redis):
    """Once one $50 position is open, a $50 order against a $60 cap must be rejected."""
    engine, _ = _exposure_engine(fake_redis, cap=60.0)
    now_ms = int(time.time() * 1000)

    sig1 = _half_budget_signal("btc_idr", "BTCUSDT", now_ms)
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig1))
    await engine.handle_signal_notification("indodax:btc_idr")
    assert "indodax:btc_idr" in engine.pending_entry_orders

    sig2 = _half_budget_signal("eth_idr", "ETHUSDT", now_ms)
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig2))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:eth_idr" not in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_p2_04_exposure_cap_counts_active_positions(fake_redis):
    """Open positions (not just pending entries) consume the cap."""
    engine, _ = _exposure_engine(fake_redis, cap=60.0)
    engine.active_positions["indodax:btc_idr"] = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=50.0,
    )

    sig = _half_budget_signal("eth_idr", "ETHUSDT", int(time.time() * 1000))
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:eth_idr" not in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_p2_04_exposure_cap_allows_within_budget(fake_redis):
    """A $50 order against a $120 cap must go through."""
    engine, _ = _exposure_engine(fake_redis, cap=120.0)
    engine.active_positions["indodax:btc_idr"] = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
        spot_cost_usdt=50.0,
        futures_margin_usdt=50.0,
    )

    sig = _half_budget_signal("eth_idr", "ETHUSDT", int(time.time() * 1000))
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:eth_idr" in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_p2_04_zero_cap_disables_check(fake_redis):
    """max_exposure_per_exchange_usdt=0 disables the exposure cap."""
    engine, _ = _exposure_engine(fake_redis, cap=0.0)
    for tag, sym, gsym in (
        ("indodax:btc_idr", "btc_idr", "BTCUSDT"),
        ("indodax:eth_idr", "eth_idr", "ETHUSDT"),
    ):
        sig = _half_budget_signal(sym, gsym, int(time.time() * 1000))
        await fake_redis.set(f"arb:signal:{tag}", json.dumps(sig))
        await engine.handle_signal_notification(tag)

    assert "indodax:btc_idr" in engine.pending_entry_orders
    assert "indodax:eth_idr" in engine.pending_entry_orders

# ---------------------------------------------------------------------------
# Fase 3
# ---------------------------------------------------------------------------

def test_p3_02_client_order_ids_are_unique_and_venue_safe():
    """Ids must stay alphanumeric+dash and never repeat within a process."""
    ids = [build_client_order_id("indodax:btc_idr", "entry", i) for i in range(1, 6)]
    assert len(set(ids)) == len(ids)
    assert ids[0] == "indodax-btc-idr-entry-1"
    for oid in ids:
        assert oid == oid.lower()
        assert all(c.isalnum() or c == "-" for c in oid)


def test_p3_02_client_order_id_retry_is_stable():
    """A retried order must reuse the same id so the venue can drop the duplicate."""
    first = build_client_order_id("indodax:btc_idr", "rollback", 7)
    assert first == build_client_order_id("indodax:btc_idr", "rollback", 7)


def test_p3_02_client_order_id_is_bounded():
    """Exchange limits are tight; an over-long pair must not blow past 36 chars."""
    oid = build_client_order_id("a" * 60 + ":" + "b" * 60, "force-close", 123456)
    assert len(oid) <= 36


@pytest.mark.asyncio
async def test_p3_02_entry_order_carries_client_order_id(fake_redis):
    engine, client = _exposure_engine(fake_redis, cap=0.0)
    sig = _half_budget_signal("btc_idr", "BTCUSDT", int(time.time() * 1000))
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:btc_idr")

    entries = [o for o in client.orders.values() if o.get("side") == "buy"]
    assert entries, "expected a maker entry buy"
    assert entries[0]["client_order_id"].startswith("indodax-btc-idr-entry-")


@pytest.mark.asyncio
async def test_p3_02_hedge_carries_client_order_id(fake_redis):
    """The futures hedge must be idempotent too: a retried hedge must not double-short."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(initial_balance_usdt=100.0)
    cfg = ExecutorConfig(
        min_allocated_capital=10.0,
        initial_exchange_balance_usdt=100.0,
        estimated_round_trip_fee_pct=0.0,
        max_entry_slippage_pct=0.0,
    )
    engine = ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis)

    sig = _half_budget_signal("btc_idr", "BTCUSDT", int(time.time() * 1000))
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:btc_idr")

    client.update_market_price("btc_idr", 1_000_000_000.0, 1_000_000_100.0)
    await fake_redis.set("arb:signal:indodax:btc_idr", "")
    pending = engine.pending_entry_orders.get("indodax:btc_idr")
    assert pending is not None

    # Simulate the maker buy filling so the hedge path runs.
    order = client.orders[pending["order_id"]]
    order["status"] = "closed"
    order["filled"] = order["amount"]
    await engine._check_entry_orders()

    assert client.trades, "expected a hedge short"
    hedge_id = client.trades[-1]["client_order_id"]
    assert hedge_id.startswith("indodax-btc-idr-hedge-")

def test_p3_04_sanitize_clamps_out_of_range_values():
    cfg = ExecutorConfig(
        allocation_per_pair=1.5,
        max_stale_latency_ms=-5,
        max_entry_slippage_pct=2.0,
    )
    warnings = cfg.sanitize()

    assert cfg.allocation_per_pair == 1.0
    assert cfg.max_stale_latency_ms == 1
    assert cfg.max_entry_slippage_pct == 1.0
    assert any("allocation_per_pair" in w for w in warnings)
    assert any("max_stale_latency_ms" in w for w in warnings)
    assert any("max_entry_slippage_pct" in w for w in warnings)


def test_p3_04_sanitize_replaces_nan_and_garbage():
    cfg = ExecutorConfig(max_daily_loss_usdt=float("nan"))
    cfg.sanitize()
    assert cfg.max_daily_loss_usdt == 50.0

    cfg = ExecutorConfig()
    cfg.exit_price_move_threshold = "not-a-number"
    cfg.sanitize()
    assert cfg.exit_price_move_threshold == 0.0015


def test_p3_04_sanitize_keeps_valid_defaults_quiet():
    assert ExecutorConfig().sanitize() == []


def test_p3_04_sanitize_announces_disabled_safety_nets():
    cfg = ExecutorConfig(estimated_round_trip_fee_pct=0.0, max_entry_slippage_pct=0.0)
    warnings = cfg.sanitize()

    assert any("fee buffer" in w for w in warnings)
    assert any("slippage" in w for w in warnings)
    # 0 is a legal "disabled" value, so it must survive sanitize untouched.
    assert cfg.estimated_round_trip_fee_pct == 0.0
    assert cfg.max_entry_slippage_pct == 0.0


def test_p3_02_live_client_sends_params_only_when_id_present():
    """Without an id the params argument must be omitted entirely, not sent empty."""
    from executor.exchange_client import _client_order_params

    assert _client_order_params(None) == ()
    assert _client_order_params("abc-1") == ({"clientOrderId": "abc-1"},)

class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_p3_05_cache_serves_fresh_orders():
    from executor.private_stream import PrivateStreamCache

    clock = _Clock()
    cache = PrivateStreamCache(max_age_seconds=5.0, clock=clock)
    cache.update_order({"id": "1", "status": "closed", "filled": 0.5})

    assert cache.get_order("1")["filled"] == 0.5
    assert cache.stats["hits"] == 1


def test_p3_05_cache_rejects_stale_orders():
    from executor.private_stream import PrivateStreamCache

    clock = _Clock()
    cache = PrivateStreamCache(max_age_seconds=5.0, clock=clock)
    cache.update_order({"id": "1", "status": "open"})

    clock.now += 6.0
    assert cache.get_order("1") is None
    assert cache.stats["misses"] == 1


def test_p3_05_cache_ignores_orders_without_id():
    from executor.private_stream import PrivateStreamCache

    cache = PrivateStreamCache()
    cache.update_order({"status": "open"})
    assert cache.stats["orders"] == 0


def test_p3_05_cache_forget_order_frees_the_slot():
    from executor.private_stream import PrivateStreamCache

    cache = PrivateStreamCache()
    cache.update_order({"id": "9", "status": "closed"})
    cache.forget_order("9")
    assert cache.stats["orders"] == 0


@pytest.mark.asyncio
async def test_p3_05_fetch_prefers_cache_then_falls_back_to_rest():
    from executor.private_stream import PrivateStreamCache, fetch_order_preferring_stream

    clock = _Clock()
    cache = PrivateStreamCache(max_age_seconds=5.0, clock=clock)
    rest_calls = []

    async def rest():
        rest_calls.append(1)
        return {"id": "7", "status": "open", "filled": 0.0}

    # Empty cache -> REST, and the result is remembered.
    order = await fetch_order_preferring_stream(cache, rest, "7")
    assert order["status"] == "open"
    assert len(rest_calls) == 1

    # Second check hits the cache instead of the venue.
    order = await fetch_order_preferring_stream(cache, rest, "7")
    assert order["status"] == "open"
    assert len(rest_calls) == 1

    # Once stale, REST is used again rather than trusting an old fill.
    clock.now += 10.0
    await fetch_order_preferring_stream(cache, rest, "7")
    assert len(rest_calls) == 2


@pytest.mark.asyncio
async def test_p3_05_disabled_by_default_keeps_rest_polling(fake_redis):
    engine, _ = _exposure_engine(fake_redis, cap=0.0)
    assert engine.private_stream is None


@pytest.mark.asyncio
async def test_p3_05_enabled_engine_uses_stream_cache(fake_redis):
    engine, client = _exposure_engine(fake_redis, cap=0.0)
    engine.config.private_stream_enabled = True
    from executor.private_stream import PrivateStreamCache

    engine.private_stream = PrivateStreamCache(max_age_seconds=30.0)

    engine.pending_entry_orders["indodax:btc_idr"] = _entry_order_info(
        "indodax:btc_idr", int(time.time() * 1000)
    )
    info = engine.pending_entry_orders["indodax:btc_idr"]
    # The feed reports the fill before REST would have noticed it.
    engine.private_stream.update_order(
        {"id": info["order_id"], "status": "closed", "filled": info["qty"], "price": info["price"]}
    )

    calls = []
    original = client.fetch_local_order

    async def counting_fetch(order_id, symbol):
        calls.append(order_id)
        return await original(order_id, symbol)

    client.fetch_local_order = counting_fetch
    await engine._process_pending_entry("indodax:btc_idr", info)

    assert calls == [], "fresh stream data must avoid a REST poll"


# -------------------------------------------------------------
# Entry re-validation (executor-side pre-entry spread guard)
# -------------------------------------------------------------

def _revalidation_payload(now_ms: int, *, min_spread_pct: float = 0.10) -> dict:
    base_bid_usdt = 65000.0
    return {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "okx",
            "local_symbol": "btc_idr",
            "global_symbol": "BTC_USDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1000.0,
        },
        "pricing_snapshot": {
            "local_best_bid": 1000000000.0,
            "local_best_ask": 1002000000.0,
            "global_best_bid_usdt": base_bid_usdt,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms - 50,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3600000,
            "minutes_to_next_funding": 60.0,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": (base_bid_usdt - 65.0) * 16000.0,  # 0.1% below bid
            "entry_spread_pct": 0.10,
            "min_entry_spread_pct": min_spread_pct,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 500.0,
            "cancel_if_global_bid_drops_below_usdt": 64800.0,
            "scanner_validated": True,
        },
    }


@pytest.mark.asyncio
async def test_entry_revalidation_rejects_collapsed_spread(engine, fake_redis, mock_client):
    """Global bid drops after snapshot: live spread < floor, entry must be rejected."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    # Live bid falls to 64350: (64350 - 64935) / 64350 ≈ -0.91% < 0.10% floor.
    mock_client.update_market_price("BTC/USDT", 64350.0, 64360.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 0
    assert pair_tag not in engine.states


@pytest.mark.asyncio
async def test_entry_revalidation_passes_healthy_spread(engine, fake_redis, mock_client):
    """Global bid unchanged: live spread meets the floor, entry proceeds."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    mock_client.update_market_price("BTC/USDT", 65000.0, 65010.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 1
    assert engine.states[pair_tag] == PositionState.ENTRY_PENDING


@pytest.mark.asyncio
async def test_entry_revalidation_fails_open_on_ticker_error(engine, fake_redis, mock_client):
    """A ticker read failure must not block the entry the scanner already validated."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    async def boom(symbol):
        raise RuntimeError("ticker down")

    mock_client.fetch_global_ticker = boom

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 1


@pytest.mark.asyncio
async def test_entry_revalidation_disabled_by_zero_floor(engine, fake_redis, mock_client):
    """Floor 0 in config AND payload: guard off, no ticker re-quote consulted."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms, min_spread_pct=0.0)

    called = []
    original = mock_client.fetch_global_ticker

    async def spy(symbol):
        called.append(symbol)
        return await original(symbol)

    mock_client.fetch_global_ticker = spy

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    await engine.handle_signal_notification(pair_tag)

    assert called == []
    assert len(mock_client.placed_buys) == 1

@pytest.mark.asyncio
async def test_modular_exchanges_config_and_env():
    """Verify modular exchanges loaded from file and dynamic env vars."""
    import os
    cfg = ExecutorConfig()
    cfg.local_exchange_name = "indodax"
    cfg.global_exchange_name = "okx"

    # Test modular exchanges dictionary via _apply_file logic
    import tempfile
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f:
        json.dump({
            "exchanges": {
                "okx": {
                    "api_key": "okx_key_1",
                    "api_secret": "okx_secret_1",
                    "passphrase": "pass_1",
                    "testnet": True,
                    "options": {"defaultType": "swap"}
                },
                "bybit": {
                    "api_key": "bybit_key_1",
                    "api_secret": "bybit_secret_1"
                }
            }
        }, f)
        f_path = f.name

    try:
        cfg._apply_file(f_path)
        assert "okx" in cfg.exchanges
        assert cfg.exchanges["okx"].api_key == "okx_key_1"
        assert cfg.exchanges["okx"].options.get("defaultType") == "swap"
        assert "bybit" in cfg.exchanges
        assert cfg.exchanges["bybit"].api_key == "bybit_key_1"

        # Test dynamic env overrides: e.g. BINANCE_API_KEY
        os.environ["BINANCE_API_KEY"] = "binance_key_env"
        os.environ["BINANCE_API_SECRET"] = "binance_sec_env"
        cfg._apply_env()
        assert "binance" in cfg.exchanges
        assert cfg.exchanges["binance"].api_key == "binance_key_env"
        assert cfg.exchanges["binance"].api_secret == "binance_sec_env"

        # Verify global credentials synced from okx
        assert cfg.global_credentials.api_key == "okx_key_1"
        assert cfg.global_credentials.passphrase == "pass_1"
        assert cfg.global_credentials.testnet is True

        # Test Supabase config from file
        cfg_sup = ExecutorConfig()
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f2:
            json.dump({
                "supabase": {
                    "enabled": True,
                    "url": "https://xyz.supabase.co",
                    "key": "secret_key_123"
                }
            }, f2)
            f2_path = f2.name
        try:
            cfg_sup._apply_file(f2_path)
            assert cfg_sup.supabase.enabled is True
            assert cfg_sup.supabase.url == "https://xyz.supabase.co"
            assert cfg_sup.supabase.key == "secret_key_123"
        finally:
            if os.path.exists(f2_path):
                os.remove(f2_path)
    finally:
        os.environ.pop("BINANCE_API_KEY", None)
        os.environ.pop("BINANCE_API_SECRET", None)
        if os.path.exists(f_path):
            os.remove(f_path)


@pytest.mark.asyncio
async def test_entry_revalidation_config_floor_overrides_payload(engine, fake_redis, mock_client):
    """An explicit config floor wins over the payload's min_entry_spread_pct."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms, min_spread_pct=0.0)  # payload floor off

    engine.config.entry_revalidation_min_spread_pct = 5.0  # config floor on
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    # Spread ≈ 0.1% < 5% config floor → rejected.
    mock_client.update_market_price("BTC/USDT", 65000.0, 65010.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 0


def test_execution_directives_scanner_validated_defaults_false():
    """Old payloads without scanner_validated must not be trusted as validated."""
    d = ExecutionDirectives.from_dict({"action": "PLACE_MAKER_BUY"})
    assert d.scanner_validated is False


# -------------------------------------------------------------
# G. Risk Control: kill switch, balance mismatch, per-exchange breaker
# -------------------------------------------------------------

async def fake_redis_set(engine, key, now_ms):
    await engine.redis.set(key, json.dumps(_fresh_signal_payload(now_ms)))


class BalancesMockClient(MockExchangeClient):

    def __init__(self, spot_available=1000.0, futures_available=1000.0):
        super().__init__()
        self.spot_available = spot_available
        self.futures_available = futures_available
        self.fail_next_buy = False

    def get_balances(self):
        return {
            "spot_available": self.spot_available,
            "futures_available": self.futures_available,
        }

    def lock_funds(self, spot, futures):
        return True

    async def place_local_limit_buy(self, symbol, amount, price, client_order_id=None):
        if self.fail_next_buy:
            raise RuntimeError("exchange down")
        return await super().place_local_limit_buy(symbol, amount, price, client_order_id)


@pytest.mark.asyncio
async def test_kill_switch_on_halt_and_off_resume(engine, fake_redis):
    """Kill switch ON halts entries; only an explicit OFF clears it (no UTC auto-reset)."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_fresh_signal_payload(now_ms)))

    await engine.handle_kill_switch(json.dumps({"action": "on", "reason": "TEST"}))
    assert engine._trading_halted is True
    assert engine._halt_reason == "KILL_SWITCH"

    await engine.handle_signal_notification(pair_tag)
    assert pair_tag not in engine.pending_entry_orders

    # OFF resumes
    await engine.handle_kill_switch(json.dumps({"action": "off"}))
    assert engine._trading_halted is False

    # Plain text forms also work
    await engine.handle_kill_switch("on")
    assert engine._halt_reason == "KILL_SWITCH"
    await engine.handle_kill_switch("off")
    assert engine._trading_halted is False


@pytest.mark.asyncio
async def test_kill_switch_survives_utc_day_rollover(engine):
    """DAILY_LOSS_LIMIT clears on rollover; KILL_SWITCH must not."""
    engine._trading_halted = True
    engine._halt_reason = "KILL_SWITCH"
    engine._loss_day = "2000-01-01"
    engine._roll_loss_day_if_needed()
    assert engine._trading_halted is True
    assert engine._halt_reason == "KILL_SWITCH"

    engine._halt_reason = "DAILY_LOSS_LIMIT"
    engine._loss_day = "2000-01-01"
    engine._roll_loss_day_if_needed()
    assert engine._trading_halted is False


@pytest.mark.asyncio
async def test_balance_mismatch_halts_venue(engine):
    """A large unexplained balance drop halts the venue for new entries."""
    engine.config.balance_mismatch_fraction = 0.20
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = client

    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis_set(engine, f"arb:signal:{pair_tag}", now_ms)

    # Baseline observation: no drop.
    await engine.handle_signal_notification(pair_tag)
    assert engine._exchange_halted == {}

    # Simulate 500 USDT vanishing with no committed capital to explain it.
    client.spot_available = 500.0
    await engine.handle_signal_notification(pair_tag)
    assert "indodax" in engine._exchange_halted
    assert "balance mismatch" in engine._exchange_halted["indodax"]

    # A drop that stays inside the fraction threshold must not halt.
    engine.config.balance_mismatch_fraction = 0.20
    engine2_client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = engine2_client
    engine._exchange_halted.clear()
    engine._last_balances.clear()
    engine2_client.spot_available = 900.0  # 10% drop < 20%
    await engine.handle_signal_notification(pair_tag)
    assert engine._exchange_halted == {}


@pytest.mark.asyncio
async def test_consecutive_order_failures_halt_exchange(engine):
    """N consecutive entry placement failures halt that exchange only."""
    engine.config.max_consecutive_order_failures = 2
    client = BalancesMockClient(spot_available=1000.0, futures_available=1000.0)
    engine.client = client

    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis_set(engine, f"arb:signal:{pair_tag}", now_ms)

    client.fail_next_buy = True
    await engine.handle_signal_notification(pair_tag)
    assert "indodax" not in engine._exchange_halted
    client.fail_next_buy = True
    await engine.handle_signal_notification(pair_tag)
    assert "indodax" in engine._exchange_halted
    assert "2 consecutive order failures" in engine._exchange_halted["indodax"]

    # Halted venue rejects further entries before even reaching placement.
    client.fail_next_buy = False
    orders_before = len(client.placed_buys)
    await engine.handle_signal_notification(pair_tag)
    assert len(client.placed_buys) == orders_before


def test_resume_exchange_clears_halt_and_failures(engine):
    engine._exchange_halted["okx"] = "test"
    engine._consecutive_order_failures["okx"] = 3
    assert engine.resume_exchange("okx") is True
    assert "okx" not in engine._exchange_halted
    assert "okx" not in engine._consecutive_order_failures
    assert engine.resume_exchange("okx") is False


# -------------------------------------------------------------
# P0: pending-entry persistence + hedge ambiguity guard
# -------------------------------------------------------------

class OpenShortPositionClient(MockExchangeClient):
    """Client whose futures short is already open despite raising on the hedge call."""

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        raise RuntimeError("timeout after venue accepted the order")

    async def fetch_positions(self, symbol: str):
        return [{"symbol": symbol, "contracts": amount_unused, "side": "short"}]

    amount_unused = 0.0


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

    assert "indodax:btc_idr" in engine.pending_entry_orders
    assert engine.states["indodax:btc_idr"] == PositionState.ENTRY_PENDING
    assert await fake_redis.get("arb:pending_entry:indodax:btc_idr") is None


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
async def test_p0_order_seq_seeded_from_clock(mock_client):
    """A fresh engine must not start its client-order-id sequence at 1."""
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=mock_client, redis_client=fake_aioredis.FakeRedis(decode_responses=True)
    )
    assert engine._order_seq > 1000
