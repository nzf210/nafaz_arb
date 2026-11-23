"""Verified Decimal liquidation valuation tests for Executor V3."""
from decimal import Decimal

import pytest

from executor.pair_pnl_engine import (
    PairPnLInputError, PnLBookLevel, PnLFXEvidence, compute_pair_pnl_v3,
)


def _levels(*rows):
    return tuple(PnLBookLevel(Decimal(price), Decimal(quantity)) for price, quantity in rows)


def _fx(*, observed=9900, rate="1", source="verified-usdt-idr"):
    return PnLFXEvidence(
        quote_to_reporting_rate=Decimal(rate), source=source,
        observed_at_ms=observed, verified=True, max_age_ms=30000,
    )


def _valuation(**overrides):
    values = {
        "session_id": "v3-session-pnl-1",
        "pair_key": "indodax:btc_idr->okx:btc-usdt-swap",
        "reporting_currency": "USDT",
        "now_ms": 10000,
        "max_book_age_ms": 1000,
        "spot_quantity_base": "0.1",
        "spot_entry_cost_quote": "10",
        "spot_entry_fee_quote": "0.01",
        "spot_entry_fee_verified": True,
        "spot_entry_fee_source": "verified-spot-fill-fee",
        "spot_bid_levels": _levels(("99", "0.05"), ("98", "0.05")),
        "spot_book_observed_at_ms": 9950,
        "spot_book_sequence": "spot-seq-100",
        "spot_book_checksum_valid": True,
        "spot_fx": _fx(),
        "spot_exit_fee_rate": "0.001",
        "spot_tax_rate": "0.001",
        "futures_quantity_native": "100",
        "futures_contract_size": "0.001",
        "futures_entry_price": "100",
        "futures_entry_fee_quote": "0.005",
        "futures_entry_fee_verified": True,
        "futures_entry_fee_source": "verified-futures-fill-fee",
        "futures_ask_levels": _levels(("101", "100")),
        "futures_book_observed_at_ms": 9960,
        "futures_book_sequence": "futures-seq-200",
        "futures_book_checksum_valid": True,
        "futures_fx": _fx(),
        "futures_exit_fee_rate": "0.0005",
        "exit_fee_policy_verified": True,
        "exit_fee_policy_source": "session-exit-fee-snapshot-v3",
        "funding_reporting_amount": "0.02",
        "funding_verified": True,
        "funding_source": "okx-funding-settlement-123",
        "conversion_cost_reporting": "0.01",
        "actual_futures_margin_reporting": "2",
        "fee_reserve_reporting": "0.1",
    }
    values.update(overrides)
    return compute_pair_pnl_v3(**values)


def test_v3_pair_valuation_uses_decimal_depth_fees_tax_fx_and_deployed_capital():
    result = _valuation()
    assert result.confidence == "VERIFIED"
    assert result.spot_exit_vwap == Decimal("98.5")
    assert result.futures_exit_vwap == Decimal("101")
    assert result.spot_estimated_liquidation_pnl == Decimal("-0.17970")
    assert result.futures_estimated_liquidation_pnl == Decimal("-0.1100500")
    assert result.estimated_net_liquidation_pnl == Decimal("-0.2797500")
    assert result.entry_fees_reporting == Decimal("0.015")
    assert result.estimated_exit_fees_reporting == Decimal("0.014900")
    assert result.estimated_tax_reporting == Decimal("0.009850")
    assert result.strategy_roi_pct == Decimal("-2.797500")
    assert result.total_deployed_capital_reporting == Decimal("12.1")
    assert result.return_on_deployed_capital_pct == Decimal("-2.311983471074380165289256198")
    assert result.hedge_drift_base_quantity == Decimal("0.000")


def test_missing_actual_margin_marks_deployed_capital_roi_unknown_not_zero():
    result = _valuation(actual_futures_margin_reporting=None)
    assert result.confidence == "PARTIAL"
    assert result.total_deployed_capital_reporting is None
    assert result.return_on_deployed_capital_pct is None
    assert result.strategy_roi_pct == Decimal("-2.797500")


def test_insufficient_depth_fails_closed_instead_of_assuming_a_price():
    with pytest.raises(PairPnLInputError, match="depth is insufficient"):
        _valuation(spot_bid_levels=_levels(("99", "0.05")))


def test_stale_or_unverified_fx_and_book_data_fail_closed():
    with pytest.raises(PairPnLInputError, match="Spot FX evidence is stale"):
        _valuation(spot_fx=_fx(observed=1))
    with pytest.raises(PairPnLInputError, match="Futures order book is stale"):
        _valuation(futures_book_observed_at_ms=100)


def test_unverified_fees_or_funding_are_never_treated_as_zero():
    with pytest.raises(PairPnLInputError, match="Spot entry fee valuation must be verified"):
        _valuation(spot_entry_fee_verified=False)
    with pytest.raises(PairPnLInputError, match="funding amount must have verified"):
        _valuation(funding_verified=False)


def test_book_checksum_and_sequence_are_required():
    with pytest.raises(PairPnLInputError, match="checksum"):
        _valuation(spot_book_checksum_valid=False)
    with pytest.raises(PairPnLInputError, match="sequence"):
        _valuation(futures_book_sequence="")
