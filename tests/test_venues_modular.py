"""Tests for modular executor venue implementations."""
import pytest
from executor.venues import (
    get_venue,
    BaseExchangeVenue,
    IndodaxVenue,
    OKXVenue,
    TokocryptoVenue,
    MEXCVenue,
)


def test_venue_registry_retrieval():
    indodax = get_venue("indodax")
    assert isinstance(indodax, IndodaxVenue)
    assert indodax.venue_name == "indodax"
    assert indodax.default_market_type == "spot"

    okx = get_venue("okx")
    assert isinstance(okx, OKXVenue)
    assert okx.venue_name == "okx"
    assert okx.default_market_type == "futures"

    toko = get_venue("tokocrypto")
    assert isinstance(toko, TokocryptoVenue)
    assert toko.venue_name == "tokocrypto"
    assert toko.default_market_type == "spot"

    mexc = get_venue("mexc")
    assert isinstance(mexc, MEXCVenue)
    assert mexc.venue_name == "mexc"
    assert mexc.default_market_type == "futures"

    unknown = get_venue("unknown_cex")
    assert isinstance(unknown, BaseExchangeVenue)


def test_okx_position_side_params():
    okx = get_venue("okx")
    assert okx.position_side_params("short") == {"posSide": "short"}
    assert okx.position_side_params("long") == {"posSide": "long"}
    with pytest.raises(ValueError, match="unsupported futures position side"):
        okx.position_side_params("invalid_side")


def test_tokocrypto_client_order_id_formatting():
    toko = get_venue("tokocrypto")
    # Rewrites dashed logical id to max 32 chars
    formatted = toko.format_client_order_id("btc-usdt-leg1-1", broker_prefix="B1_")
    assert len(formatted) <= 32
    assert formatted.startswith("B1")


def test_tokocrypto_wire_symbol():
    toko = get_venue("tokocrypto")
    assert toko.format_wire_symbol(None, "BTC/USDT") == "BTCUSDT"
    assert toko.format_wire_symbol(None, "BTC_USDT") == "BTC_USDT"


def test_mexc_balance_fetch_params():
    mexc = get_venue("mexc")
    assert mexc.balance_fetch_params("futures") == {"type": "swap"}
    assert mexc.balance_fetch_params("spot") == {}
