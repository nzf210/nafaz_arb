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


def test_pair_key_can_be_built_from_executor_pair_info_mapping():
    key = ExchangePairKey.from_pair_info({
        "local_exchange": "Tokocrypto",
        "local_symbol": "btc_idr",
        "global_exchange": "OKX",
        "global_symbol": "BTCUSDT",
    })
    assert key.tag == "tokocrypto:BTC_IDR__okx:BTC_USDT"


def test_pair_key_can_be_built_from_pair_info_object():
    class PairInfo:
        local_exchange = "indodax"
        local_symbol = "ETH/IDR"
        global_exchange = "bybit"
        global_symbol = "ETH/USDT"

    key = ExchangePairKey.from_pair_info(PairInfo())
    assert key.local_symbol == "ETH/IDR"
    assert key.global_symbol == "ETH/USDT"


def test_pair_key_rejects_missing_pair_info_fields():
    with pytest.raises(ValueError, match="local_exchange is required"):
        ExchangePairKey.from_pair_info({})


@pytest.mark.parametrize(
    ("spot_symbol", "futures_symbol", "expected_spot", "expected_futures"),
    [
        ("btc_idr", "BTCUSDT", "BTC/IDR", "BTC/USDT"),
        ("ETH/IDR", "ETH-USDC", "ETH/IDR", "ETH/USDC"),
        ("SOL-IDR", "SOLUSDT", "SOL/IDR", "SOL/USDT"),
    ],
)
def test_pair_key_normalizes_compact_and_delimited_symbols(
    spot_symbol, futures_symbol, expected_spot, expected_futures
):
    key = ExchangePairKey.create(
        "tokocrypto", spot_symbol, "okx", futures_symbol
    )
    assert key.local_symbol == expected_spot
    assert key.global_symbol == expected_futures



@pytest.mark.parametrize(
    ("spot_symbol", "futures_symbol", "expected_base"),
    [
        ("toko:ATH_USDT", "ATH-USDT-SWAP", "ATH"),
        ("toko:FLOKI_IDR", "FLOKI-USDT-SWAP", "FLOKI"),
        ("toko:SOLBIDR", "SOL-USDT-SWAP", "SOL"),
        ("BTC_IDR", "BTC-USDT-SWAP", "BTC"),
    ],
)
def test_pair_key_normalizes_real_tokocrypto_and_okx_symbols(
    spot_symbol, futures_symbol, expected_base
):
    key = ExchangePairKey.create(
        "tokocrypto", spot_symbol, "okx", futures_symbol
    )
    assert key.local_symbol.split("/", 1)[0] == expected_base
    assert key.global_symbol.split("/", 1)[0] == expected_base
    assert key.tag == (
        f"tokocrypto:{expected_base}_"
        f"{'IDR' if spot_symbol.endswith(('IDR', 'BIDR')) else 'USDT'}"
        f"__okx:{expected_base}_USDT"
    )


def test_pair_key_strips_mexc_adapter_namespace_only_for_mexc():
    key = ExchangePairKey.create(
        "tokocrypto", "toko:ATH_USDT", "mexc", "mexc:ATH_USDT"
    )
    assert key.local_symbol == "ATH/USDT"
    assert key.global_symbol == "ATH/USDT"


def test_pair_key_does_not_strip_unknown_namespace_as_asset_prefix():
    with pytest.raises(ValueError, match="base asset mismatch"):
        ExchangePairKey.create(
            "tokocrypto", "toko:BTC_USDT", "okx", "UNKNOWN:BTC-USDT-SWAP"
        )
