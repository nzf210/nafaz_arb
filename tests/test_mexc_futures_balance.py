"""Regression coverage for MEXC futures balance routing through CCXT swap markets."""

import pytest

from executor.config import ExchangeCredentials
from executor import exchange_client


class _FakeCCXTExchange:
    def __init__(self, config):
        self.config = config
        self.id = "mexc" if config.get("apiKey") == "mexc-key" else "tokocrypto"
        self.balance_calls = []

    async def fetch_balance(self, params=None):
        params = params or {}
        self.balance_calls.append(params)
        if self.id == "mexc":
            # MEXC futures balances are only returned when CCXT selects swap.
            if params.get("type") != "swap":
                raise AssertionError("MEXC futures balance must explicitly select type=swap")
            return {
                "free": {"USDT": 125.0},
                "used": {"USDT": 25.0},
                "total": {"USDT": 150.0},
            }
        return {
            "free": {"USDT": 300.0},
            "used": {"USDT": 0.0},
            "total": {"USDT": 300.0},
        }


@pytest.mark.parametrize(
    ("exchange_name", "requested_type", "expected_type"),
    [
        ("mexc", "future", "swap"),
        ("MEXC", "futures", "swap"),
        ("mexc", "spot", "spot"),
        ("okx", "future", "future"),
        ("binance", "future", "future"),
    ],
)
def test_resolve_default_type_is_venue_specific(
    exchange_name, requested_type, expected_type
):
    assert (
        exchange_client._resolve_default_type(exchange_name, requested_type)
        == expected_type
    )


@pytest.mark.asyncio
async def test_mexc_futures_factory_and_balance_reads_use_swap(monkeypatch):
    monkeypatch.setattr(
        exchange_client.ccxt, "mexc", _FakeCCXTExchange, raising=False
    )
    monkeypatch.setattr(
        exchange_client.ccxt, "tokocrypto", _FakeCCXTExchange, raising=False
    )

    local_credentials = ExchangeCredentials(
        api_key="toko-key",
        api_secret="toko-secret",
    )
    # Simulate a legacy config that accidentally persisted defaultType=future.
    global_credentials = ExchangeCredentials(
        api_key="mexc-key",
        api_secret="mexc-secret",
        options={"defaultType": "future"},
    )

    client = exchange_client.create_ccxt_client(
        "tokocrypto",
        "mexc",
        local_credentials,
        global_credentials,
    )

    assert client.global_fut.config["options"]["defaultType"] == "swap"

    reports = await client.fetch_balance_report()
    mexc_report = next(report for report in reports if report["name"] == "mexc")
    assert mexc_report["market"] == "futures"
    assert mexc_report["status"] == "ok"
    assert mexc_report["assets"] == [
        {"asset": "USDT", "free": 125.0, "used": 25.0, "total": 150.0}
    ]
    assert client.global_fut.balance_calls[-1] == {"type": "swap"}

    balances = await client.get_balances(
        local_quote_asset="USDT",
        global_quote_asset="USDT",
    )
    assert balances["spot_available"] == 300.0
    assert balances["futures_available"] == 125.0
    assert client.global_fut.balance_calls[-1] == {"type": "swap"}
