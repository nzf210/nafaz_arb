"""Regression matrix for every supported NAFAZ spot/perpetual venue pairing."""
import pytest

from executor.fee_engine import (
    FeeRates,
    required_spot_exit_usdt,
    round_trip_fee_breakdown_usdt,
    round_trip_fees_usdt,
)


SPOT = {
    "indodax": {"maker": 0.10, "taker": 0.30},
    "tokocrypto": {"maker": 0.10, "taker": 0.10},
}
PERP = {
    "okx": {"maker": 0.02, "taker": 0.05},
    "mexc": {"maker": 0.00, "taker": 0.02},
    "binance": {"maker": 0.02, "taker": 0.05},
    "bybit": {"maker": 0.02, "taker": 0.055},
}


@pytest.mark.parametrize("spot_exchange,spot", SPOT.items())
@pytest.mark.parametrize("perp_exchange,perp", PERP.items())
@pytest.mark.parametrize("mode", ["MAKER_TAKER", "TAKER_TAKER"])
def test_all_venue_pairs_and_execution_modes_have_complete_round_trip_fees(
    spot_exchange, spot, perp_exchange, perp, mode
):
    spot_entry = spot["maker"] if mode == "MAKER_TAKER" else spot["taker"]
    rates = FeeRates(
        spot_entry_pct=spot_entry,
        spot_exit_pct=spot["taker"],
        perp_entry_pct=perp["taker"],
        perp_exit_pct=perp["taker"],
        source="TEST_PROFILE",
        verified=True,
        fetched_at_ms=1,
    )
    actual = round_trip_fees_usdt(
        2.0, 100.0, 102.0, 101.0, 99.0, rates
    )
    expected = 2.0 * (
        100.0 * spot_entry + 102.0 * spot["taker"]
        + 101.0 * perp["taker"] + 99.0 * perp["taker"]
    ) / 100.0
    assert actual == pytest.approx(expected), (
        spot_exchange, perp_exchange, mode
    )
    floor = required_spot_exit_usdt(
        2.0, 100.0, 101.0, 99.0, 0.0, 0.5, rates
    )
    assert floor > 0.0
    net_at_floor = (
        2.0 * (floor - 100.0)
        + 2.0 * (101.0 - 99.0)
        - round_trip_fees_usdt(2.0, 100.0, floor, 101.0, 99.0, rates)
    )
    assert net_at_floor == pytest.approx(0.5)


def test_round_trip_fee_breakdown_splits_spot_and_perpetual_costs():
    rates = FeeRates(0.1, 0.2, 0.03, 0.04, surcharge_pct=0.01)
    breakdown = round_trip_fee_breakdown_usdt(
        2.0, 100.0, 110.0, 105.0, 95.0, rates
    )
    expected_spot = 2.0 * (100.0 * 0.11 + 110.0 * 0.21) / 100.0
    expected_futures = 2.0 * (105.0 * 0.04 + 95.0 * 0.05) / 100.0
    assert breakdown["spot_fees_usdt"] == pytest.approx(expected_spot)
    assert breakdown["futures_fees_usdt"] == pytest.approx(expected_futures)
    assert breakdown["total_fees_usdt"] == pytest.approx(
        breakdown["spot_fees_usdt"] + breakdown["futures_fees_usdt"]
    )
    assert round_trip_fees_usdt(
        2.0, 100.0, 110.0, 105.0, 95.0, rates
    ) == pytest.approx(breakdown["total_fees_usdt"])


def test_explicit_zero_maker_fee_is_preserved():
    rates = FeeRates(0.0, 0.3, 0.05, 0.05, "CONFIG", True, 10)
    assert round_trip_fees_usdt(1, 100, 100, 100, 100, rates) == pytest.approx(0.4)


@pytest.mark.parametrize("bad", [-0.01, 100.01, float("nan"), float("inf")])
def test_invalid_fee_rates_are_rejected(bad):
    rates = FeeRates(bad, 0, 0, 0)
    with pytest.raises(ValueError):
        rates.validate()


def test_spot_exit_fee_is_included_in_solved_profit_floor():
    rates = FeeRates(0.1, 0.3, 0.05, 0.05, "TEST", True, 1)
    floor = required_spot_exit_usdt(1, 100, 100, 100, 0, 0.1, rates)
    fees = round_trip_fees_usdt(1, 100, floor, 100, 100, rates)
    net = (floor - 100) + (100 - 100) - fees
    assert net == pytest.approx(0.1, abs=1e-9)

def test_fee_asset_surcharge_is_applied_to_all_four_fill_notionals():
    base = FeeRates(0.1, 0.2, 0.03, 0.04)
    with_surcharge = FeeRates(0.1, 0.2, 0.03, 0.04, surcharge_pct=0.01)
    baseline = round_trip_fees_usdt(1, 100, 110, 105, 95, base)
    actual = round_trip_fees_usdt(1, 100, 110, 105, 95, with_surcharge)
    expected_delta = (100 + 110 + 105 + 95) * 0.01 / 100
    assert actual - baseline == pytest.approx(expected_delta)
