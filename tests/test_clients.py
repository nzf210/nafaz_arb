"""Exchange client behaviour: paper-trading fills, market sells, order-id propagation to live clients."""

import asyncio
import json
import time

from executor.exchange_client import PaperTradingExchangeClient

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine, build_client_order_id

from tests.conftest import _exposure_engine, _half_budget_signal, engine, fake_redis

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
async def test_bug10_paper_client_close_reports_fill_price():
    """The paper client must report a close fill price so paper PnL stays honest."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    client.update_market_price("btc_usdt", best_bid=64_990.0, best_ask=65_010.0)

    order = await client.close_global_short_market("btc_usdt", 0.01)

    assert order["average"] == 65_010.0
    assert order["price"] == 65_010.0
    assert order["params"]["reduceOnly"] is True

def test_p3_02_live_client_sends_params_only_when_id_present():
    """Without an id the params argument must be omitted entirely, not sent empty."""
    from executor.exchange_client import _client_order_params

    assert _client_order_params(None) == ()
    assert _client_order_params("abc-1") == ({"clientOrderId": "abc-1"},)

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
async def test_live_exchange_client_routes_each_symbol_to_its_role_venue():
    from executor.exchange_client import ExchangeClient

    class FakeVenue:
        def __init__(self, venue_id, market_id, unified_symbol):
            self.id = venue_id
            self.markets_by_id = {market_id: {"symbol": unified_symbol}}
            self.markets = {unified_symbol: {"symbol": unified_symbol}}
            self.calls = []

        async def load_markets(self):
            return self.markets

        async def create_limit_buy_order(self, symbol, amount, price, *params):
            self.calls.append(("buy", symbol, amount, price))
            return {"id": "spot-1"}

        async def create_market_sell_order(self, symbol, amount, *params):
            self.calls.append(("spot_sell", symbol, amount))
            return {"id": "spot-sell-1"}

        async def create_market_sell_order(self, symbol, amount, *params):
            self.calls.append(("market_sell", symbol, amount))
            return {"id": "short-1"}

        async def create_market_buy_order(self, symbol, amount, params):
            self.calls.append(("market_buy", symbol, amount, params))
            return {"id": "close-1"}

        async def fetch_balance(self):
            return {"free": {"USDT": 120.0}, "total": {"USDT": 150.0}}

        async def fetch_positions(self, symbols=None):
            self.calls.append(("positions", symbols))
            return [{"symbol": "BTC/USDT:USDT", "side": "short", "contracts": 0.01}]

    indodax = FakeVenue("indodax", "btcidr", "BTC/IDR")
    tokocrypto = FakeVenue("tokocrypto", "BTC_USDT", "BTC/USDT")
    okx = FakeVenue("okx", "BTC-USDT-SWAP", "BTC/USDT:USDT")
    binance = FakeVenue("binance", "BTCUSDT", "BTC/USDT:USDT")
    # CCXT derivative amount is contracts; engine amount is always base-asset units.
    okx.markets["BTC/USDT:USDT"].update({"contract": True, "contractSize": 1.0})
    okx.markets_by_id["BTC-USDT-SWAP"].update({"contract": True, "contractSize": 1.0})
    binance.markets["BTC/USDT:USDT"].update({"contract": True, "contractSize": 0.001})
    binance.markets_by_id["BTCUSDT"].update({"contract": True, "contractSize": 0.001})
    client = ExchangeClient(
        indodax, okx,
        local_exchanges={"indodax": indodax, "tokocrypto": tokocrypto},
        global_exchanges={"okx": okx, "binance": binance},
        local_default_name="indodax",
        global_default_name="okx",
    )

    await client.place_local_limit_buy("toko:BTC_USDT", 0.01, 60000)
    await client.open_global_short_market("binance:BTCUSDT", 0.01)

    assert tokocrypto.calls[0][0:2] == ("buy", "BTC/USDT")
    assert binance.calls[0][0:2] == ("market_sell", "BTC/USDT:USDT")
    assert binance.calls[0][2] == 10.0, "0.01 BTC must be converted to 10 contracts at 0.001 BTC/contract"
    positions = await client.fetch_positions("binance:BTCUSDT")
    assert len(positions) == 1
    assert positions[0]["side"] == "short"

    balances = await client.get_balances(
        local_quote_asset="USDT",
        global_quote_asset="USDT",
        local_exchange="tokocrypto",
        global_exchange="binance",
    )
    assert balances["spot_available"] == 120.0
    assert balances["futures_available"] == 120.0

    with pytest.raises(ValueError, match="foreign Futures symbol"):
        client._local_for("binance:BTCUSDT")
    with pytest.raises(ValueError, match="local Spot symbol"):
        client._global_for("toko:BTC_USDT")
