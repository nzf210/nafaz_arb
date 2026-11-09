import pytest

from executor.pair_registry import ExchangePairKey, PairClientRegistry


@pytest.mark.parametrize(
    ("spot", "futures"),
    [
        ("tokocrypto", "okx"), ("tokocrypto", "mexc"),
        ("tokocrypto", "binance"), ("tokocrypto", "bybit"),
        ("indodax", "okx"), ("indodax", "mexc"),
        ("indodax", "binance"), ("indodax", "bybit"),
    ],
)
def test_pair_key_supports_all_eight_venue_routes(spot, futures):
    key = ExchangePairKey.create(spot, "BTC/IDR", futures, "BTC/USDT")
    assert key.local_exchange == spot
    assert key.global_exchange == futures
    assert key.local_symbol == "BTC/IDR"
    assert key.global_symbol == "BTC/USDT"


def test_pair_identity_distinguishes_futures_venue_for_same_spot_market():
    okx = ExchangePairKey.create("tokocrypto", "BTC/IDR", "okx", "BTC/USDT")
    mexc = ExchangePairKey.create("tokocrypto", "BTC/IDR", "mexc", "BTC/USDT")
    assert okx != mexc
    assert okx.tag != mexc.tag


def test_pair_key_rejects_base_asset_mismatch():
    with pytest.raises(ValueError, match="base asset mismatch"):
        ExchangePairKey.create("tokocrypto", "BTC/IDR", "okx", "ETH/USDT")


def test_pair_key_rejects_unsupported_venue():
    with pytest.raises(ValueError, match="unsupported futures venue"):
        ExchangePairKey.create("tokocrypto", "BTC/IDR", "unknown", "BTC/USDT")


def test_registry_resolves_exact_pair_and_fails_closed_for_missing_pair():
    registry = PairClientRegistry()
    okx_key = ExchangePairKey.create("tokocrypto", "BTC/IDR", "okx", "BTC/USDT")
    mexc_key = ExchangePairKey.create("tokocrypto", "BTC/IDR", "mexc", "BTC/USDT")
    okx_client, mexc_client = object(), object()
    registry.register(okx_key, okx_client)
    registry.register(mexc_key, mexc_client)
    assert registry.resolve(okx_key) is okx_client
    assert registry.resolve(mexc_key) is mexc_client
    with pytest.raises(LookupError):
        registry.resolve(ExchangePairKey.create("indodax", "BTC/IDR", "okx", "BTC/USDT"))


def test_registry_rejects_replacing_existing_pair_client():
    key = ExchangePairKey.create("indodax", "BTC/IDR", "bybit", "BTC/USDT")
    registry = PairClientRegistry()
    registry.register(key, object())
    with pytest.raises(ValueError, match="already registered"):
        registry.register(key, object())
