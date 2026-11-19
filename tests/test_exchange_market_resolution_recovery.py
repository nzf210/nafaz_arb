"""Regression coverage for pair-independent venue market recovery."""

import pytest

from executor.exchange_orders import ExchangeOrderMixin


def test_exact_venue_market_id_wins_over_stale_pair_base_during_recovery():
    market = {
        "id": "RIVER-USDT-SWAP",
        "symbol": "RIVER/USDT:USDT",
        "base": "RIVER",
        "quote": "USDT",
        "swap": True,
        "contract": True,
    }

    assert ExchangeOrderMixin._market_matches_pair(
        market,
        normalized_id="RIVERUSDTSWAP",
        pair_base="UMA",
        pair_quote="USDT",
        market_kind="futures",
    )


def test_exact_market_id_still_requires_correct_quote_and_market_kind():
    market = {
        "id": "RIVER-USDT-SWAP",
        "symbol": "RIVER/USDT:USDT",
        "base": "RIVER",
        "quote": "USDT",
        "swap": True,
        "contract": True,
    }

    assert not ExchangeOrderMixin._market_matches_pair(
        market, "RIVERUSDTSWAP", "UMA", "BTC", "futures"
    )
    assert not ExchangeOrderMixin._market_matches_pair(
        market, "RIVERUSDTSWAP", "UMA", "USDT", "spot"
    )


def test_market_id_must_match_exactly_even_when_pair_base_is_ignored():
    market = {
        "id": "RIVER-USDT-SWAP",
        "base": "RIVER",
        "quote": "USDT",
        "swap": True,
        "contract": True,
    }
    assert not ExchangeOrderMixin._market_matches_pair(
        market, "RIVERUSDTSWAPX", "UMA", "USDT", "futures"
    )


@pytest.mark.asyncio
async def test_tokocrypto_account_open_orders_and_canceled_order_fallback():
    from executor.exchange_client import ExchangeClient

    class FakeToko:
        id = "tokocrypto"
        options = {}
        markets = {
            "SOON/IDR": {
                "id": "SOON_IDR",
                "symbol": "SOON/IDR",
                "base": "SOON",
                "quote": "IDR",
                "spot": True,
            }
        }

        def market(self, symbol):
            return self.markets[symbol]

        async def fetch_order(self, order_id, symbol):
            return {"id": None, "status": None, "info": {}}

        async def fetch_open_orders(self, symbol=None):
            raise AssertionError("tokocrypto fetchOrders() requires a symbol argument")

        async def privateGetOpenV1Orders(self, params):
            if params.get("type") == 1:
                return {"code": 0, "data": {"list": [{"orderId": "101", "symbol": "SOON_IDR"}]}}
            return {
                "code": 0,
                "data": {
                    "list": [
                        {
                            "orderId": "926124683",
                            "clientId": "968f2019052137d994entry794479586",
                            "symbol": "SOON_IDR",
                            "status": 3,
                        }
                    ]
                },
            }

        def parse_orders(self, rows):
            return [
                {"id": str(r["orderId"]), "symbol": "SOON/IDR", "status": "open", "amount": 10.0, "filled": 0.0}
                for r in rows
            ]

        def parse_order(self, row, market=None):
            return {"id": str(row["orderId"]), "clientOrderId": row["clientId"], "symbol": "SOON/IDR", "status": "canceled"}

    client = ExchangeClient(FakeToko(), object())
    open_orders = await client.fetch_local_open_orders(None)
    assert len(open_orders) == 1
    assert open_orders[0]["id"] == "101"

    order = await client.fetch_local_order("926124683", "toko:SOON_IDR")
    assert order["id"] == "926124683"
    assert order["status"] == "canceled"


@pytest.mark.asyncio
async def test_client_for_exchange_prefers_venue_neutral_for_venue_over_other_pair():
    from types import SimpleNamespace
    from executor.exchange_client import ExchangeClient
    from executor.recovery_discovery import RecoveryDiscoveryMixin

    class FakeToko:
        id = "tokocrypto"
        options = {}
        markets = {
            "SOON/IDR": {"id": "SOON_IDR", "symbol": "SOON/IDR", "base": "SOON", "quote": "IDR", "spot": True},
            "ATH/USDT": {"id": "ATH_USDT", "symbol": "ATH/USDT", "base": "ATH", "quote": "USDT", "spot": True},
        }

    class FakeRegistry:
        def keys(self):
            return [SimpleNamespace(local_exchange="tokocrypto", global_exchange="okx")]

        def resolve(self, _key):
            return ExchangeClient(
                FakeToko(),
                object(),
                pair_info=SimpleNamespace(base_asset="ATH", quote_asset="USDT", global_quote_asset="USDT"),
            )

    class Harness(RecoveryDiscoveryMixin):
        config = SimpleNamespace(mock_mode=False, local_exchange_name="indodax", global_exchange_name="okx")
        _pair_client_registry = FakeRegistry()
        _base_client = ExchangeClient(
            SimpleNamespace(id="indodax", options={}),
            SimpleNamespace(id="okx", options={}),
            balance_exchanges={
                "tokocrypto": {"market": "spot", "client": FakeToko()},
                "okx": {"market": "futures", "client": SimpleNamespace(id="okx", options={})},
            },
        )

    owner = Harness()._client_for_exchange("tokocrypto")
    assert await owner._resolve_symbol(owner.local, "toko:SOON_IDR", "spot") == "SOON/IDR"


@pytest.mark.asyncio
async def test_spot_balance_lookup_never_falls_back_to_another_venue():
    from executor.exchange_client import ExchangeClient

    class FakeBalanceVenue:
        def __init__(self, venue_id, total):
            self.id = venue_id
            self.options = {}
            self.total = total
            self.calls = 0

        async def fetch_balance(self):
            self.calls += 1
            return {"free": {"BTC": self.total}, "total": {"BTC": self.total}}

    indodax = FakeBalanceVenue("indodax", 1.0)
    tokocrypto = FakeBalanceVenue("tokocrypto", 0.005)
    futures = FakeBalanceVenue("okx", 0.0)
    client = ExchangeClient(
        indodax,
        futures,
        balance_exchanges={
            "indodax": {"market": "spot", "client": indodax},
            "tokocrypto": {"market": "spot", "client": tokocrypto},
            "okx": {"market": "futures", "client": futures},
        },
    )

    assert await client.fetch_spot_total_balance("BTC", venue="tokocrypto") == pytest.approx(0.005)
    assert await client.fetch_spot_free_balance("BTC", venue="tokocrypto") == pytest.approx(0.005)
    assert tokocrypto.calls == 2
    assert indodax.calls == 0


@pytest.mark.asyncio
async def test_spot_balance_lookup_fails_closed_when_requested_venue_client_missing():
    from executor.exchange_client import ExchangeClient

    class FakeBalanceVenue:
        id = "indodax"
        options = {}

        def __init__(self):
            self.calls = 0

        async def fetch_balance(self):
            self.calls += 1
            return {"free": {"BTC": 1.0}, "total": {"BTC": 1.0}}

    indodax = FakeBalanceVenue()
    client = ExchangeClient(
        indodax,
        object(),
        balance_exchanges={
            "indodax": {"market": "spot", "client": indodax},
            "tokocrypto": {
                "market": "spot",
                "client": None,
                "error": "API key/secret not configured",
            },
        },
    )

    with pytest.raises(LookupError, match="tokocrypto"):
        await client.fetch_spot_total_balance("BTC", venue="tokocrypto")
    assert indodax.calls == 0
