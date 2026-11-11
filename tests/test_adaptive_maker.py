"""Unit tests for Indodax IDR adaptive maker pricing."""

import pytest

from executor.adaptive_maker import (
    aggressive_maker_price_idr,
    dynamic_max_buy_price_idr,
)


def test_dynamic_max_buy_price_retains_fee_adjusted_profit_floor():
    ceiling = dynamic_max_buy_price_idr(
        scanner_price_idr=1_000_000_000,
        estimated_net_profit_usdt=10.0,
        quantity=0.01,
        usdt_idr_rate=16_000,
        spot_entry_fee_pct=0.1,
        fee_surcharge_pct=0.0,
        min_net_profit_pct=0.1,
    )
    baseline_notional = 0.01 * 1_000_000_000 / 16_000
    target = baseline_notional * 0.1 / 100
    consumed_edge = 0.01 * (ceiling - 1_000_000_000) / 16_000 * 1.001
    assert 10.0 - consumed_edge == pytest.approx(target)


def test_dynamic_max_buy_price_rejects_non_positive_profit_edge():
    assert dynamic_max_buy_price_idr(
        1_000_000_000, 0.001, 0.01, 16_000, 0.1, 0.0, 0.1
    ) == 0.0


def test_aggressive_maker_pegs_one_tick_below_best_ask_within_cap():
    price = aggressive_maker_price_idr(
        bids=[[1000, 5]], asks=[[1010, 3]], tick_size=1,
        max_buy_price_idr=1008,
    )
    assert price == 1008
    assert price < 1010


def test_aggressive_maker_does_not_cross_or_exceed_profit_ceiling():
    price = aggressive_maker_price_idr(
        bids=[[1000, 5]], asks=[[1005, 3]], tick_size=1,
        max_buy_price_idr=2000,
    )
    assert price == 1004


def test_aggressive_maker_skips_when_profit_ceiling_is_below_best_bid():
    assert aggressive_maker_price_idr(
        bids=[[1000, 5]], asks=[[1010, 3]], tick_size=1,
        max_buy_price_idr=999,
    ) is None


def test_aggressive_maker_skips_invalid_or_crossed_books():
    assert aggressive_maker_price_idr([], [[1010, 3]], 1, 1005) is None
    assert aggressive_maker_price_idr(
        [[1010, 3]], [[1000, 3]], 1, 1020
    ) is None
