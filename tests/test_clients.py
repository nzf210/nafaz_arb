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

BASE_AMOUNT_SPEC = {
    "unit": "BASE",
    "step": 0.00000001,
    "min": 0.00000001,
    "base_per_unit": 1.0,
}

@pytest.mark.asyncio
async def test_live_balance_report_reads_each_configured_exchange():
    from executor.exchange_client import ExchangeClient

    class FakeExchange:
        def __init__(self, name, balances):
            self.id, self.balances, self.closed = name, balances, False
        async def fetch_balance(self):
            return self.balances
        async def close(self):
            self.closed = True

    spot = FakeExchange("indodax", {"free": {"IDR": 1200000}, "used": {"IDR": 300000}, "total": {"IDR": 1500000}})
    futures = FakeExchange("okx", {"free": {"USDT": 50}, "used": {"USDT": 10}, "total": {"USDT": 60}})
    toko = FakeExchange("tokocrypto", {"free": {"IDR": 800000, "BTC": 0.001}, "used": {"IDR": 200000}, "total": {"IDR": 1000000, "BTC": 0.001}})
    client = ExchangeClient(spot, futures, {
        "indodax": {"market": "spot", "client": spot},
        "okx": {"market": "futures", "client": futures},
        "tokocrypto": {"market": "spot", "client": toko},
        "mexc": {"market": "futures", "client": None, "error": "API key/secret not configured"},
    })
    reports = await client.fetch_balance_report()
    by_name = {report["name"]: report for report in reports}
    assert by_name["tokocrypto"]["status"] == "ok"
    assert by_name["tokocrypto"]["market"] == "spot"
    assert {asset["asset"] for asset in by_name["tokocrypto"]["assets"]} == {"BTC", "IDR"}
    assert by_name["mexc"]["status"] == "not_configured"
    assert by_name["indodax"]["status"] == "ok" and by_name["okx"]["status"] == "ok"
    await client.close()
    assert spot.closed and futures.closed and toko.closed


@pytest.mark.asyncio
async def test_live_futures_order_converts_base_quantity_to_contracts_and_back():
    from executor.exchange_client import ExchangeClient

    class Futures:
        def __init__(self):
            self.calls = []

        async def create_market_sell_order(self, symbol, amount, *params):
            self.calls.append((symbol, amount, params))
            return {"symbol": symbol, "amount": amount, "filled": amount, "status": "closed"}

        async def create_market_buy_order(self, symbol, amount, params):
            self.calls.append((symbol, amount, params))
            return {"symbol": symbol, "amount": amount, "filled": amount, "status": "closed"}

    futures = Futures()
    client = ExchangeClient(object(), futures)
    spec = {
        "unit": "CONTRACT",
        "step": 1.0,
        "min": 1.0,
        "base_per_unit": 0.001,
    }

    opened = await client.open_global_short_market(
        "BTC/USDT:USDT", 0.003, amount_spec=spec
    )
    closed = await client.close_global_short_market(
        "BTC/USDT:USDT", 0.003, amount_spec=spec
    )

    assert futures.calls[0][1] == 3
    assert opened["filled"] == pytest.approx(0.003)
    assert futures.calls[1][1] == 3
    assert futures.calls[1][2]["reduceOnly"] is True
    assert closed["filled"] == pytest.approx(0.003)


@pytest.mark.asyncio
async def test_live_client_resolves_scanner_market_ids_before_orders():
    from types import SimpleNamespace

    from executor.exchange_client import ExchangeClient

    class Spot:
        id = "tokocrypto"
        markets = {
            "BTC/IDR": {
                "id": "BTCIDR", "symbol": "BTC/IDR", "base": "BTC",
                "quote": "IDR", "spot": True,
            },
        }

        def __init__(self):
            self.calls = []

        async def create_limit_buy_order(self, symbol, amount, price, *params):
            self.calls.append((symbol, amount, price))
            return {"symbol": symbol, "amount": amount, "filled": amount}

    class Futures:
        id = "okx"
        markets = {
            "BTC/USDT:USDT": {
                "id": "BTC-USDT-SWAP", "symbol": "BTC/USDT:USDT",
                "base": "BTC", "quote": "USDT", "swap": True,
            },
        }

        def __init__(self):
            self.calls = []

        async def create_market_sell_order(self, symbol, amount, *params):
            self.calls.append((symbol, amount))
            return {"symbol": symbol, "amount": amount, "filled": amount}

    spot, futures = Spot(), Futures()
    pair = SimpleNamespace(
        base_asset="BTC",
        quote_asset="IDR",
        global_quote_asset="USDT",
    )
    client = ExchangeClient(spot, futures, pair_info=pair)
    await client.place_local_limit_buy(
        "toko:BTC_IDR", 0.001, 1_000_000_000, amount_spec=BASE_AMOUNT_SPEC
    )
    await client.open_global_short_market(
        "okx:BTC-USDT-SWAP", 0.001, amount_spec=BASE_AMOUNT_SPEC
    )

    assert spot.calls[0][0] == "BTC/IDR"
    assert futures.calls[0][0] == "BTC/USDT:USDT"

@pytest.mark.asyncio
async def test_tokocrypto_order_carries_registered_broker_prefix():
    """A configured broker id prefixes the client id; error 3703 otherwise."""
    from types import SimpleNamespace

    from executor.exchange_client import ExchangeClient, venue_client_order_id

    BROKER_ID = "x-TKT5PX2F"

    class TokoSpot:
        id = "tokocrypto"
        # The registered broker id travels with the account (ccxt options),
        # populated from the exchange credentials by init_ccxt_exchange.
        options = {"broker": {"marketType": BROKER_ID}}
        markets = {
            "FLOKI/IDR": {
                "id": "FLOKIIDR", "symbol": "FLOKI/IDR", "base": "FLOKI",
                "quote": "IDR", "spot": True,
            },
        }

        def __init__(self):
            self.calls = []

        async def create_limit_buy_order(self, symbol, amount, price, *params):
            self.calls.append((symbol, params))
            return {"symbol": symbol, "amount": amount, "filled": amount}

    spot = TokoSpot()
    pair = SimpleNamespace(
        base_asset="FLOKI",
        quote_asset="IDR",
        global_quote_asset="USDT",
    )
    client = ExchangeClient(spot, object(), pair_info=pair)
    raw_id = build_client_order_id(
        "tokocrypto:toko:floki_idr__okx:floki_usdt_swap", "entry", 1
    )
    await client.place_local_limit_buy(
        "toko:FLOKI_IDR", 1.0, 1_000.0,
        client_order_id=raw_id,
        amount_spec=BASE_AMOUNT_SPEC,
    )

    params = spot.calls[0][1][0]
    sent_id = params["clientOrderId"]
    assert sent_id.startswith(BROKER_ID)
    # The deterministic "-{leg}-{seq}" tail survives trimming, so a retried
    # submit is still recognised as a duplicate by the venue.
    assert sent_id.endswith("-entry-1")
    # The final on-the-wire id must fit the venue's 36-character limit even
    # with the broker prefix.
    assert len(sent_id) <= 36

    # Venues outside Binance Cloud keep the plain id; ids that already carry
    # the prefix are left untouched.
    assert venue_client_order_id(spot, f"{BROKER_ID}abc", BROKER_ID) == f"{BROKER_ID}abc"

    class IndodaxSpot:
        id = "indodax"

    assert venue_client_order_id(IndodaxSpot(), "indodax-btc-idr-entry-1") == (
        "indodax-btc-idr-entry-1"
    )

    # With no registered broker id, no prefix is fabricated: the id is sent
    # unchanged so ccxt mints a venue-native one instead of guessing a prefix
    # the venue rejects with error 3703.
    class UnregisteredTokoSpot:
        id = "tokocrypto"
        options = {}

    assert venue_client_order_id(UnregisteredTokoSpot(), raw_id) == raw_id

@pytest.mark.asyncio
async def test_tokocrypto_broker_prefix_travels_from_credentials_to_wire():
    """The broker id set in exchange credentials must prefix the sent id."""
    from executor.config import ExchangeCredentials
    from executor.exchange_client import ExchangeClient, init_ccxt_exchange
    from executor.helpers import build_client_order_id
    from executor.quantity import AmountSpec

    creds = ExchangeCredentials(
        api_key="k", api_secret="s", options={"broker_id": "x-TKT5PX2F"}
    )
    local = init_ccxt_exchange("tokocrypto", creds, "spot")
    sent = {}

    async def create_limit_buy_order(symbol, amount, price, *params):
        sent["params"] = params[0]
        return {"symbol": symbol, "amount": amount, "filled": amount}

    local.create_limit_buy_order = create_limit_buy_order
    local.markets = {"FLOKI/IDR": {"id": "FLOKIIDR", "symbol": "FLOKI/IDR", "spot": True}}

    client = ExchangeClient(local, object())
    assert client._local_broker_prefix == "x-TKT5PX2F"

    await client.place_local_limit_buy(
        "FLOKI/IDR", 1.0, 1_000.0,
        client_order_id=build_client_order_id(
            "tokocrypto:toko:floki_idr__okx:floki_usdt_swap", "entry", 1
        ),
        amount_spec=AmountSpec(unit="BASE", step=0.001, minimum=0.0, base_per_unit=1.0),
    )
    sent_id = sent["params"]["clientOrderId"]
    assert sent_id.startswith("x-TKT5PX2F")
    assert sent_id.endswith("-entry-1")
    assert len(sent_id) <= 36


@pytest.mark.asyncio
async def test_dual_leg_sizing_caps_by_thinner_balance():
    """Live sizing must cap to min(spot, futures) so both legs stay fundable."""
    from types import SimpleNamespace

    from executor.risk import RiskMixin

    class Engine(RiskMixin):
        def __init__(self):
            self.config = SimpleNamespace(
                mock_mode=False,
                min_allocated_capital=10.0,
                max_exposure_per_exchange_usdt=1000.0,
                estimated_round_trip_fee_pct=0.0,
            )
            self._balance_snapshots = {}
            self._last_balances = {}
            self._exchange_halted = {}

        async def _get_balance_snapshot(self, **kwargs):
            return {"spot_available": 80.0, "futures_available": 30.0}

        def _exposure_allows(self, pair_tag, pair_info, planned):
            return True

        async def _publish_execution_failure(self, *args, **kwargs):
            raise AssertionError("a fundable size must not be rejected")

    engine = Engine()
    payload = SimpleNamespace(
        execution_directives=SimpleNamespace(max_position_usdt=50.0),
        pair_info=SimpleNamespace(),
        pricing_snapshot=SimpleNamespace(usdt_idr_rate=16000.0),
    )
    sized = await engine._compute_entry_sizing("pair", payload)
    assert sized is not None
    # Directive asked for $50 but futures only has $30, so the entry is capped.
    assert sized[0] == 30.0


@pytest.mark.asyncio
async def test_live_sizing_probe_failure_does_not_halt_venues():
    """A transient live balance read must not halt venues; the preflight decides."""
    from types import SimpleNamespace

    from executor.risk import RiskMixin

    class Engine(RiskMixin):
        def __init__(self):
            self.config = SimpleNamespace(
                mock_mode=False,
                min_allocated_capital=10.0,
                max_exposure_per_exchange_usdt=1000.0,
                estimated_round_trip_fee_pct=0.0,
            )
            self._balance_snapshots = {}
            self._last_balances = {}
            self._exchange_halted = {}

        async def _get_balance_snapshot(self, **kwargs):
            raise RuntimeError("venue timeout")

        def _exposure_allows(self, pair_tag, pair_info, planned):
            return True

        async def _publish_execution_failure(self, *args, **kwargs):
            raise AssertionError("an unreadable balance must not reject here")

    engine = Engine()
    payload = SimpleNamespace(
        execution_directives=SimpleNamespace(max_position_usdt=50.0),
        pair_info=SimpleNamespace(),
        pricing_snapshot=SimpleNamespace(usdt_idr_rate=16000.0),
    )
    sized = await engine._compute_entry_sizing("pair", payload)
    assert sized is not None
    # Sizing is left uncapped and the funding preflight owns the decision.
    assert sized[0] == 50.0
    assert engine._exchange_halted == {}


@pytest.mark.asyncio
async def test_dual_leg_underfunded_reports_fundable_cap():
    """A shortfall reports the per-leg cap the caller can actually use."""
    from types import SimpleNamespace

    from executor.risk import RiskMixin

    published = []

    class Engine(RiskMixin):
        def __init__(self):
            self.config = SimpleNamespace(mock_mode=False)

        async def _get_balance_snapshot(self, **kwargs):
            return {"spot_available": 40.0, "futures_available": 12.0}

        async def _publish_execution_failure(self, *args, **kwargs):
            published.append((args, kwargs))

    engine = Engine()
    ok = await engine._verify_dual_leg_funding("pair", SimpleNamespace(), 50.0, 16000.0)
    assert ok is False
    args, kwargs = published[-1]
    assert args[1] == "DUAL_LEG_UNDERFUNDED"
    assert kwargs["detail"]["fundable_usdt_per_leg"] == 12.0


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
    buy_order = await client.place_local_limit_buy("btc_idr", 0.5, 1000.0, amount_spec=BASE_AMOUNT_SPEC)
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
    sell_order = await client.place_local_limit_sell("btc_idr", 0.5, 1020.0, amount_spec=BASE_AMOUNT_SPEC)
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

    order = await client.place_local_market_sell("btc_idr", 0.25, amount_spec=BASE_AMOUNT_SPEC)

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
        await client.place_local_market_sell("btc_idr", 0.0, amount_spec=BASE_AMOUNT_SPEC)

@pytest.mark.asyncio
async def test_paper_client_short_returns_reference_price():
    """P0-04: the paper client must echo back the caller-supplied futures price."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    trade = await client.open_global_short_market(
        "BTCUSDT", 0.01, price=65_000.0, amount_spec=BASE_AMOUNT_SPEC
    )

    assert trade["price"] == 65_000.0
    assert trade["average"] == 65_000.0
    assert trade["expected_price"] == 65_000.0

@pytest.mark.asyncio
async def test_bug10_paper_client_close_reports_fill_price():
    """The paper client must report a close fill price so paper PnL stays honest."""
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(local_name="indodax", global_name="okx")
    client.update_market_price("btc_usdt", best_bid=64_990.0, best_ask=65_010.0)

    order = await client.close_global_short_market(
        "btc_usdt", 0.01, amount_spec=BASE_AMOUNT_SPEC
    )

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


def test_client_order_id_preserves_sequence_for_long_pairs():
    """Long venue-qualified pairs must not truncate the unique sequence suffix."""
    pair = "indodax:auctionidr__okx:auction_usdt_swap"
    first = build_client_order_id(pair, "entry", 794266239)
    second = build_client_order_id(pair, "entry", 794266240)

    assert first != second
    assert first.endswith("-entry-794266239")
    assert second.endswith("-entry-794266240")
    assert len(first) <= 36
    assert len(second) <= 36


def test_client_order_id_hashes_truncated_pair_identity():
    """Long pairs sharing a prefix must keep distinct venue-safe IDs."""
    first = build_client_order_id("same-prefix:" + "a" * 50, "entry", 123)
    second = build_client_order_id("same-prefix:" + "b" * 50, "entry", 123)

    assert first != second
    assert len(first) <= 36
    assert len(second) <= 36


def test_live_exchange_client_resolves_exact_pair_without_mutating_default_clients():
    from types import SimpleNamespace
    from executor.exchange_client import ExchangeClient

    toko = object()
    indo = object()
    okx = object()
    mexc = object()
    balances = {
        "tokocrypto": {"market": "spot", "client": toko},
        "indodax": {"market": "spot", "client": indo},
        "okx": {"market": "futures", "client": okx},
        "mexc": {"market": "futures", "client": mexc},
    }
    default = ExchangeClient(indo, okx, balances)
    toko_okx = default.for_pair(SimpleNamespace(
        local_exchange="tokocrypto", global_exchange="okx"
    ))
    indo_mexc = default.for_pair(SimpleNamespace(
        local_exchange="indodax", global_exchange="mexc"
    ))

    assert toko_okx.local is toko
    assert toko_okx.global_fut is okx
    assert indo_mexc.local is indo
    assert indo_mexc.global_fut is mexc
    assert default.local is indo
    assert default.global_fut is okx


def test_live_exchange_client_rejects_unconfigured_pair_venue():
    from types import SimpleNamespace
    from executor.exchange_client import ExchangeClient

    spot = object()
    futures = object()
    client = ExchangeClient(spot, futures, {
        "tokocrypto": {"market": "spot", "client": spot},
        "okx": {"market": "futures", "client": futures},
        "mexc": {"market": "futures", "client": None, "error": "credentials missing"},
    })
    with pytest.raises(LookupError, match="futures client unavailable"):
        client.for_pair(SimpleNamespace(
            local_exchange="tokocrypto", global_exchange="mexc"
        ))


def test_engine_pair_client_binding_is_task_local_and_restores_previous_binding():
    from types import SimpleNamespace
    from executor.exchange_client import ExchangeClient

    venues = {
        "tokocrypto": {"market": "spot", "client": object()},
        "indodax": {"market": "spot", "client": object()},
        "okx": {"market": "futures", "client": object()},
        "mexc": {"market": "futures", "client": object()},
    }
    base = ExchangeClient(venues["indodax"]["client"], venues["okx"]["client"], venues)
    config = ExecutorConfig(mock_mode=False)
    engine = ArbitrageEngine(config=config, client=base)

    toko_okx = SimpleNamespace(
        local_exchange="tokocrypto", local_symbol="BTC/IDR",
        global_exchange="okx", global_symbol="BTC/USDT",
    )
    indo_mexc = SimpleNamespace(
        local_exchange="indodax", local_symbol="BTC/IDR",
        global_exchange="mexc", global_symbol="BTC/USDT",
    )

    token_a = engine._bind_pair_client(toko_okx)
    toko_client = engine.client
    token_b = engine._bind_pair_client(indo_mexc)
    assert engine.client.local is venues["indodax"]["client"]
    assert engine.client.global_fut is venues["mexc"]["client"]

    engine._reset_pair_client(token_b)
    assert engine.client is toko_client
    engine._reset_pair_client(token_a)
    assert engine.client is base

@pytest.mark.asyncio
async def test_spot_order_history_lookup_matches_exact_client_id():
    """The adapter can recover a filled/closed order when its create ack was lost."""
    from executor.exchange_client import ExchangeClient

    class Spot:
        id = "okx"

        async def fetch_open_orders(self, symbol):
            return []

        async def fetch_closed_orders(self, symbol, since=None, limit=None):
            return [{
                "id": "ack-spot-1",
                "clientOrderId": "pair-entry-77",
                "symbol": symbol,
                "side": "buy",
                "type": "limit",
                "amount": 0.01,
                "filled": 0.01,
                "remaining": 0.0,
                "status": "closed",
                "average": 100.0,
            }]

    client = ExchangeClient(Spot(), object())
    order = await client.fetch_local_order_by_client_id(
        "pair-entry-77", "BTC/USDT"
    )

    assert order["id"] == "ack-spot-1"
    assert order["client_order_id"] == "pair-entry-77"
    assert order["status"] == "closed"


@pytest.mark.asyncio
async def test_futures_order_history_lookup_resolves_client_id_through_private_trade():
    """A filled market hedge can be found through the private trade's client id."""
    from executor.exchange_client import ExchangeClient

    class Futures:
        id = "okx"

        async def fetch_open_orders(self, symbol):
            return []

        async def fetch_closed_orders(self, symbol, since=None, limit=None):
            raise NotSupported("closed order history unavailable")

        async def fetch_canceled_orders(self, symbol, since=None, limit=None):
            raise NotSupported("canceled order history unavailable")

        async def fetch_my_trades(self, symbol, since=None, limit=None):
            return [{
                "id": "trade-1",
                "order": "ack-hedge-1",
                "clientOrderId": "pair-hedge-78",
                "symbol": symbol,
            }]

        async def fetch_order(self, order_id, symbol):
            assert order_id == "ack-hedge-1"
            return {
                "id": order_id,
                "symbol": symbol,
                "side": "sell",
                "type": "market",
                "amount": 0.01,
                "filled": 0.01,
                "remaining": 0.0,
                "status": "closed",
                "average": 101.0,
            }

    class NotSupported(Exception):
        pass

    client = ExchangeClient(object(), Futures())
    order = await client.fetch_global_order_by_client_id(
        "pair-hedge-78", "BTC/USDT"
    )

    assert order["id"] == "ack-hedge-1"
    assert order["client_order_id"] == "pair-hedge-78"
    assert order["filled"] == pytest.approx(0.01)

@pytest.mark.asyncio
async def test_tokocrypto_broker_prefixed_client_id_is_normalized_for_recovery():
    """The logical registry id must match the broker-prefixed id returned by venue APIs."""
    from executor.exchange_client import ExchangeClient

    broker_prefix = "x-TKT5PX2F"
    logical_id = "pair-entry-91"
    wire_id = broker_prefix + logical_id

    class TokoSpot:
        id = "tokocrypto"
        options = {"broker": {"marketType": broker_prefix}}

        async def fetch_open_orders(self, symbol):
            return [{
                "id": "toko-open-1",
                "clientOrderId": wire_id,
                "symbol": symbol,
                "side": "buy",
                "type": "limit",
                "amount": 0.01,
                "filled": 0.0,
                "remaining": 0.01,
                "status": "open",
            }]

        async def fetch_closed_orders(self, symbol, since=None, limit=None):
            return [{
                "id": "toko-closed-1",
                "clientOrderId": wire_id,
                "symbol": symbol,
                "side": "buy",
                "type": "limit",
                "amount": 0.01,
                "filled": 0.01,
                "remaining": 0.0,
                "status": "closed",
            }]

    client = ExchangeClient(TokoSpot(), object())
    recovered = await client.fetch_local_order_by_client_id(
        logical_id, "FLOKI/IDR"
    )
    opened = await client.fetch_local_open_orders("FLOKI/IDR")

    assert recovered["id"] == "toko-closed-1"
    assert recovered["client_order_id"] == logical_id
    assert opened[0]["client_order_id"] == logical_id
