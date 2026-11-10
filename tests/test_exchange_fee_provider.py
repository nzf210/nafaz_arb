"""Authenticated fee-provider contract tests for the supported venue adapters."""
import pytest

from executor.exchange_client import ExchangeClient


class FeeExchange:
    def __init__(self, exchange_id, maker=0.0002, taker=0.0005):
        self.id = exchange_id
        self.markets = {}
        self.maker = maker
        self.taker = taker

    async def fetch_trading_fee(self, symbol):
        return {"symbol": symbol, "maker": self.maker, "taker": self.taker}


class IndodaxAdapter:
    id = "indodax"
    markets = {}


class TokocryptoExchange:
    id = "tokocrypto"
    markets = {}

    async def private_get_open_v1_account_spot(self):
        # Official Tokocrypto account endpoint reports maker/taker commissions in bips.
        return {"code": 0, "data": {"makerCommission": "10", "takerCommission": "10"}}


@pytest.mark.asyncio
async def test_indodax_unsupported_fee_endpoint_does_not_hide_perpetual_dynamic_rate():
    client = ExchangeClient(IndodaxAdapter(), FeeExchange("okx"))
    profile = await client.fetch_pair_fee_rates(
        type("Pair", (), {"local_symbol": "BTC_IDR", "global_symbol": "BTC/USDT:USDT"})()
    )
    assert profile["spot"] is None
    assert profile["perp"] == {"maker": pytest.approx(0.02), "taker": pytest.approx(0.05)}
    assert profile["source"] == "PARTIAL_EXCHANGE_API"
    assert profile["verified"] is False


@pytest.mark.asyncio
async def test_tokocrypto_account_commission_bips_are_converted_to_percent():
    client = ExchangeClient(TokocryptoExchange(), FeeExchange("bybit"))
    profile = await client.fetch_pair_fee_rates(
        type("Pair", (), {"local_symbol": "BTC_USDT", "global_symbol": "BTC/USDT:USDT"})()
    )
    assert profile["spot"] == {"maker": pytest.approx(0.1), "taker": pytest.approx(0.1)}
    assert profile["perp"] == {"maker": pytest.approx(0.02), "taker": pytest.approx(0.05)}
    assert profile["source"] == "EXCHANGE_API"
    assert profile["verified"] is True

@pytest.mark.asyncio
async def test_fresh_symbol_fee_cache_prevents_duplicate_private_api_calls():
    spot = FeeExchange("tokocrypto")
    perp = FeeExchange("okx")
    client = ExchangeClient(spot, perp)
    cached = {
        "spot": {"maker": 0.1, "taker": 0.1, "fetched_at_ms": 1_800_000_000_000},
        "perp": {"maker": 0.02, "taker": 0.05, "fetched_at_ms": 1_800_000_000_000},
    }
    pair = type("Pair", (), {
        "local_symbol": "BTC_USDT",
        "global_symbol": "BTC/USDT:USDT",
    })()
    profile = await client.fetch_pair_fee_rates(pair, cached)
    assert profile["source"] == "EXCHANGE_API"
    assert profile["verified"] is True
    assert profile["spot"]["taker"] == pytest.approx(0.1)
    assert profile["perp"]["taker"] == pytest.approx(0.05)
