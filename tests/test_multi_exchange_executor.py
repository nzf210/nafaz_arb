"""Regression coverage for strict multi-exchange live executor routing."""

import hashlib

import pytest

from executor.config import ExchangeCredentials, ExecutorConfig
from executor.exchange_client import ExchangeClient, create_exchange_client


def _fixture_credential(exchange: str, purpose: str) -> str:
    return hashlib.sha256(f"{exchange}:{purpose}".encode()).hexdigest()


class FakeVenue:
    def __init__(self, name):
        self.id = name
        self.calls = []

    async def create_limit_buy_order(self, *args):
        self.calls.append(("spot_buy", args))
        return {"id": self.id}

    async def create_market_sell_order(self, *args):
        self.calls.append(("perp_sell", args))
        return {"id": self.id}

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_strict_live_client_requires_explicit_route():
    spot = FakeVenue("indodax")
    perp = FakeVenue("okx")
    client = ExchangeClient(
        spot_clients={"indodax": spot},
        perpetual_clients={"okx": perp},
        default_spot="indodax",
        default_perpetual="okx",
        strict_routing=True,
    )

    with pytest.raises(RuntimeError, match="route must be selected"):
        await client.place_local_limit_buy("BTC/IDR", 0.001, 1_000_000_000)

    with pytest.raises(RuntimeError, match="route must be selected"):
        await client.open_global_short_market("BTC/USDT:USDT", 0.001)

    client.set_route("indodax", "okx")
    assert (await client.place_local_limit_buy("BTC/IDR", 0.001, 1_000_000_000))["id"] == "indodax"
    assert (await client.open_global_short_market("BTC/USDT:USDT", 0.001))["id"] == "okx"


def test_live_factory_builds_each_venue_with_correct_market_type(monkeypatch):
    import executor.exchange_client as module

    calls = []
    clients = {}

    def fake_spot(name, creds, default_type="spot"):
        calls.append(("spot", name, default_type))
        clients[("spot", name)] = FakeVenue(name)
        return clients[("spot", name)]

    def fake_perp(name, creds, default_type="spot"):
        calls.append(("perpetual", name, default_type))
        clients[("perpetual", name)] = FakeVenue(name)
        return clients[("perpetual", name)]

    monkeypatch.setattr(module, "init_local_exchange", fake_spot)
    monkeypatch.setattr(module, "init_ccxt_exchange", fake_perp)

    cfg = ExecutorConfig(mock_mode=False)
    cfg.local_exchange_name = "indodax"
    cfg.global_exchange_name = "okx"
    cfg.spot_exchanges = ["indodax", "tokocrypto"]
    cfg.perpetual_exchanges = ["okx", "mexc", "binance", "bybit"]
    cfg.exchanges = {
        name: ExchangeCredentials(api_key=_fixture_credential(name, "key"),
                                  api_secret=_fixture_credential(name, "secret"),
                                  passphrase=_fixture_credential(name, "passphrase") if name == "okx" else None)
        for name in ("indodax", "tokocrypto", "okx", "mexc", "binance", "bybit")
    }

    client = create_exchange_client(cfg)

    assert ("perpetual", "okx", "swap") in calls
    assert ("perpetual", "mexc", "swap") in calls
    assert ("perpetual", "bybit", "swap") in calls
    assert ("perpetual", "binance", "future") in calls
    assert client._strict_routing is True
    assert set(client.spot_clients) == {"indodax", "tokocrypto"}
    assert set(client.perpetual_clients) == {"okx", "mexc", "binance", "bybit"}


def test_legacy_single_exchange_client_keeps_default_route_compatibility():
    spot = FakeVenue("indodax")
    perp = FakeVenue("okx")
    client = ExchangeClient(local_exchange=spot, global_exchange=perp)
    assert client.local is spot
    assert client.global_fut is perp
