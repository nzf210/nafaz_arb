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
async def test_live_exchange_client_routes_are_isolated_by_async_context():
    """Concurrent route contexts dispatch spot and futures orders to their named venues."""
    from executor.exchange_client import ExchangeClient

    class FakeVenue:
        def __init__(self, name):
            self.id = name
            self.calls = []
        async def create_limit_buy_order(self, *args):
            self.calls.append(args)
            return {"id": self.id}
        async def create_market_sell_order(self, *args):
            self.calls.append(args)
            return {"id": self.id}
        async def close(self):
            pass

    indodax, tokocrypto = FakeVenue("indodax"), FakeVenue("tokocrypto")
    okx, mexc = FakeVenue("okx"), FakeVenue("mexc")
    client = ExchangeClient(
        spot_clients={"indodax": indodax, "tokocrypto": tokocrypto},
        perpetual_clients={"okx": okx, "mexc": mexc},
        default_spot="indodax",
        default_perpetual="okx",
    )

    async def execute_route(spot, perp):
        client.set_route(spot, perp)
        spot_order = await client.place_local_limit_buy("BTC/IDR", 1, 100)
        perp_order = await client.open_global_short_market("BTC/USDT:USDT", 1)
        return spot_order["id"], perp_order["id"]

    results = await asyncio.gather(
        execute_route("indodax", "okx"),
        execute_route("tokocrypto", "mexc"),
    )
    assert results == [("indodax", "okx"), ("tokocrypto", "mexc")]
    assert len(indodax.calls) == len(tokocrypto.calls) == 1
    assert len(okx.calls) == len(mexc.calls) == 1


def test_live_exchange_client_fails_closed_for_unknown_route():
    from executor.exchange_client import ExchangeClient

    client = ExchangeClient(spot_clients={}, perpetual_clients={})
    with pytest.raises(RuntimeError, match="spot exchange route is not configured"):
        client.set_route("unknown-spot", "unknown-perp")
