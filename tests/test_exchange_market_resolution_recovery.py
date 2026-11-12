"""Regression coverage for pair-independent venue market recovery."""

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
