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
    client.update_market_price("btc_idr", 1_037_000_000.0, 1_038_000_000.0)
    client.update_market_price("BTCUSDT", 65_000.0, 65_010.0)
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
async def test_live_ccxt_preflight_resolves_symbols_and_aligns_contract_quantity():
    from executor.exchange_client import ExchangeClient

    class FakeCCXT:
        def __init__(self, venue_id, market_id, unified_symbol, market, contract=False):
            self.id = venue_id
            self.markets_by_id = {market_id: {"symbol": unified_symbol, **market}}
            self.markets = {unified_symbol: {"symbol": unified_symbol, **market}}
            self.contract_precision = contract
            self.calls = []
            self.load_calls = []
            self.leverage_calls = []

        async def load_markets(self, reload=False):
            self.load_calls.append(reload)
            return self.markets

        def market(self, symbol):
            return self.markets[symbol]

        def amount_to_precision(self, symbol, amount):
            if self.contract_precision:
                return str(int(float(amount)))
            return f"{int(float(amount) * 1000) / 1000:.3f}"

        def price_to_precision(self, symbol, price):
            if self.id == "indodax":
                return str(int(float(price) / 100) * 100)
            return f"{float(price):.1f}"

        async def create_limit_buy_order(self, symbol, amount, price, *params):
            self.calls.append(("spot_buy", symbol, amount, price, params))
            return {"id": "spot-entry", "status": "open", "amount": amount, "filled": 0.0}

        async def create_market_sell_order(self, symbol, amount, *params):
            self.calls.append(("perp_open", symbol, amount, params))
            return {"id": "short", "status": "closed", "amount": amount, "filled": amount, "average": 65_000.0}

        async def create_market_buy_order(self, symbol, amount, params):
            self.calls.append(("perp_close", symbol, amount, params))
            return {"id": "close", "status": "closed", "amount": amount, "filled": amount, "average": 64_900.0}

        async def set_leverage(self, leverage, symbol):
            self.leverage_calls.append((leverage, symbol))

        async def fetch_positions(self, symbols=None):
            self.calls.append(("fetch_positions", symbols))
            return [{"symbol": "BTC/USDT:USDT", "contracts": 2.0, "side": "short"}]

        async def fetch_ticker(self, symbol):
            self.calls.append(("ticker", symbol))
            if self.id == "indodax":
                return {"symbol": symbol, "bid": 1_039_000.0, "ask": 1_040_000.0, "timestamp": int(time.time() * 1000)}
            return {"symbol": symbol, "bid": 64_990.0, "ask": 65_000.0, "timestamp": int(time.time() * 1000)}

    spot_market = {
        "active": True, "spot": True, "type": "spot", "quote": "IDR",
        "limits": {"amount": {"min": 0.001}, "cost": {"min": 10_000.0}},
    }
    perp_market = {
        "active": True, "contract": True, "linear": True, "swap": True,
        "quote": "USDT", "settle": "USDT", "contractSize": 0.01,
        "limits": {"amount": {"min": 1.0}, "cost": {"min": 10.0}},
    }
    spot = FakeCCXT("indodax", "btcidr", "BTC/IDR", spot_market)
    perp = FakeCCXT("okx", "BTC-USDT-SWAP", "BTC/USDT:USDT", perp_market, contract=True)
    client = ExchangeClient(
        spot, perp,
        local_default_name="indodax",
        global_default_name="okx",
    )

    amount, price_idr = await client.validate_entry_order(
        local_symbol="btcidr",
        global_symbol="BTC-USDT-SWAP",
        amount=0.023,
        price_idr=1_000_050.0,
        usdt_idr_rate=16_000.0,
        global_ask_quote=65_000.0,
        local_exchange="indodax",
        global_exchange="okx",
    )
    assert amount == pytest.approx(0.02), "Spot size must be reduced to an exact Futures contract multiple"
    assert price_idr == pytest.approx(1_000_000.0), "buy limit precision must round down"
    assert spot.load_calls[0] is True and perp.load_calls[0] is True, "preflight must refresh both market catalogs"

    spot_order = await client.place_local_limit_buy(
        "btcidr", amount, price_idr, client_order_id="entry-1", usdt_idr_rate=16_000.0
    )
    assert spot_order["id"] == "spot-entry"
    assert spot.calls[-1][1:4] == ("BTC/IDR", 0.02, 1_000_000.0)

    short = await client.open_global_short_market(
        "BTC-USDT-SWAP", amount, price=65_000.0, client_order_id="hedge-1"
    )
    assert short["filled"] == pytest.approx(0.02)
    assert short["filled_contracts"] == pytest.approx(2.0)
    assert perp.calls[-1][0:3] == ("perp_open", "BTC/USDT:USDT", 2.0)
    assert perp.leverage_calls == [(1, "BTC/USDT:USDT")]

    closed = await client.close_global_short_market("BTC-USDT-SWAP", amount, client_order_id="close-1")
    assert closed["filled"] == pytest.approx(0.02)
    assert perp.calls[-1][0:3] == ("perp_close", "BTC/USDT:USDT", 2.0)
    assert perp.calls[-1][3]["reduceOnly"] is True

    positions = await client.fetch_positions("BTC-USDT-SWAP")
    assert positions[0]["contracts_contracts"] == pytest.approx(2.0)
    assert positions[0]["contracts"] == pytest.approx(0.02), "position quantities exposed to engine must be base units"


@pytest.mark.asyncio
async def test_live_ccxt_preflight_fails_closed_when_market_or_minimum_is_unknown():
    from executor.exchange_client import ExchangeClient

    class MinimalVenue:
        def __init__(self, venue_id, market_id, symbol, market, contract=False):
            self.id = venue_id
            self.markets_by_id = {market_id: {"symbol": symbol, **market}}
            self.markets = {symbol: {"symbol": symbol, **market}}
            self.contract = contract

        async def load_markets(self, reload=False):
            return self.markets

        def market(self, symbol):
            return self.markets[symbol]

        def amount_to_precision(self, symbol, amount):
            return str(int(amount)) if self.contract else str(amount)

        def price_to_precision(self, symbol, price):
            return str(price)

    spot_market = {
        "active": False, "spot": True, "type": "spot", "quote": "IDR",
        "limits": {"amount": {"min": 0.001}, "cost": {"min": 10_000.0}},
    }
    perp_market = {
        "active": True, "contract": True, "linear": True, "swap": True,
        "quote": "USDT", "settle": "USDT", "contractSize": 0.01,
        "limits": {"amount": {"min": 1.0}, "cost": {"min": 10.0}},
    }
    spot = MinimalVenue("indodax", "btcidr", "BTC/IDR", spot_market)
    perp = MinimalVenue("okx", "BTC-USDT-SWAP", "BTC/USDT:USDT", perp_market, contract=True)
    client = ExchangeClient(spot, perp, local_default_name="indodax", global_default_name="okx")

    with pytest.raises(ValueError, match="not confirmed active"):
        await client.validate_entry_order("btcidr", "BTC-USDT-SWAP", 0.02, 1_000_000.0, 16_000.0, 65_000.0)

    spot.markets["BTC/IDR"]["active"] = True
    spot.markets_by_id["btcidr"]["active"] = True
    spot.markets["BTC/IDR"]["limits"]["cost"]["min"] = None
    spot.markets_by_id["btcidr"]["limits"]["cost"]["min"] = None
    with pytest.raises(ValueError, match="no verified cost.min"):
        await client.validate_entry_order("btcidr", "BTC-USDT-SWAP", 0.02, 1_000_000.0, 16_000.0, 65_000.0)
