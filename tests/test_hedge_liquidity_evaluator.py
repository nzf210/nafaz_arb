"""Pure Decimal tests for V3 hedge liquidity and bounded-wait policy."""
from decimal import Decimal

import pytest

from executor.hedge_liquidity_evaluator import (
    BookLevel, FXSnapshot, HedgeLiquidityPolicy, LiquidityInputError,
    OrderBookSnapshot, evaluate_hedge_liquidity,
)


def _book(*, bids=None, asks=None, observed=9900, sequence_valid=True, checksum_valid=True):
    return OrderBookSnapshot(
        bids=tuple(BookLevel.make(*x) for x in (bids or [("100", "200")])),
        asks=tuple(BookLevel.make(*x) for x in (asks or [("100.1", "200")])),
        observed_at_ms=observed, sequence="seq-10",
        sequence_valid=sequence_valid, checksum_valid=checksum_valid,
    )


def _fx(*, observed=9950, rate="1", source="USDT/USD"):
    return FXSnapshot(quote_to_usd_rate=Decimal(rate), observed_at_ms=observed, source=source)


def _policy(**overrides):
    values = {
        "internal_min_notional_usd": Decimal("5"),
        "max_book_age_ms": 500,
        "max_fx_age_ms": 1000,
        "max_slippage_bps": Decimal("25"),
        "taker_fee_bps": Decimal("5"),
        "safety_buffer_bps": Decimal("5"),
        "max_unhedged_notional_usd": Decimal("25"),
        "max_unhedged_wait_ms": 5000,
        "max_adverse_move_bps": Decimal("100"),
    }
    values.update(overrides)
    return HedgeLiquidityPolicy(**values)


def _evaluate(**overrides):
    values = {
        "required_base_quantity": "0.1",
        "already_reserved_base_quantity": "0",
        "base_per_unit": "0.001",
        "quantity_step": "1",
        "venue_min_order_quantity": "1",
        "venue_min_notional_quote": "5",
        "position_side": "SHORT",
        "book": _book(),
        "fx": _fx(),
        "policy": _policy(),
        "now_ms": 10000,
        "unhedged_since_ms": 9000,
        "unhedged_notional_usd": "10",
        "reference_price": "100",
    }
    values.update(overrides)
    return evaluate_hedge_liquidity(**values)


def test_healthy_depth_returns_full_submit_with_decimal_economics():
    result = _evaluate()
    assert result.action == "SUBMIT"
    assert result.executable_native_quantity == Decimal("100")
    assert result.executable_base_quantity == Decimal("0.100")
    assert result.executable_notional_usd == Decimal("10.000")
    assert result.slippage_bps == Decimal("0")
    assert result.estimated_total_cost_bps == Decimal("10")
    assert result.book_sequence == "seq-10"


def test_partial_depth_above_minimum_returns_partial_submit():
    result = _evaluate(
        required_base_quantity="0.3",
        book=_book(bids=[("100", "100")]),
    )
    assert result.action == "PARTIAL_SUBMIT"
    assert result.executable_base_quantity == Decimal("0.100")


@pytest.mark.parametrize(
    ("book", "fx", "reason"),
    [
        (_book(observed=9000), _fx(), "BOOK_STALE"),
        (_book(sequence_valid=False), _fx(), "BOOK_SEQUENCE_GAP"),
        (_book(checksum_valid=False), _fx(), "BOOK_CHECKSUM_INVALID"),
        (_book(bids=[("100", "0.5")]), _fx(), "INSUFFICIENT_EXECUTABLE_DEPTH"),
        (_book(bids=[("100", "50"), ("98", "50")]), _fx(), "SLIPPAGE_LIMIT_EXCEEDED"),
        (_book(bids=[("100", "100")]), _fx(observed=8000), "FX_STALE_OR_UNVERIFIED"),
    ],
)
def test_unhealthy_or_insufficient_liquidity_defers(book, fx, reason):
    result = _evaluate(required_base_quantity="0.1", book=book, fx=fx)
    assert result.action == "DEFER"
    assert result.reason == reason
    assert result.next_evaluate_at_ms > 10000


def test_expired_wait_budget_escalates_to_derisk():
    result = _evaluate(
        book=_book(observed=9000),
        now_ms=16000,
        unhedged_since_ms=9000,
    )
    assert result.action == "DE_RISK_REQUIRED"
    assert result.wait_remaining_ms == 0


def test_unhedged_notional_limit_escalates_immediately():
    result = _evaluate(unhedged_notional_usd="26")
    assert result.action == "DE_RISK_REQUIRED"
    assert result.reason == "UNHEDGED_NOTIONAL_LIMIT_EXCEEDED"


def test_adverse_price_move_defers_and_long_uses_ask_depth():
    adverse = _evaluate(
        book=_book(bids=[("98", "200")]),
        reference_price="100",
    )
    assert adverse.action == "DEFER"
    assert adverse.reason == "ADVERSE_PRICE_MOVE"

    long_result = _evaluate(
        position_side="LONG",
        book=_book(asks=[("100.1", "200")]),
    )
    assert long_result.action == "SUBMIT"
    assert long_result.executable_vwap == Decimal("100.1")


def test_native_lot_step_rounds_down_without_overhedging():
    result = _evaluate(required_base_quantity="0.1005")
    assert result.action == "PARTIAL_SUBMIT"
    assert result.executable_base_quantity == Decimal("0.100")


def test_invalid_position_or_nonfinite_book_values_fail_closed():
    with pytest.raises(LiquidityInputError, match="position_side"):
        _evaluate(position_side="SIDEWAYS")
    with pytest.raises(LiquidityInputError, match="finite decimal"):
        BookLevel.make("NaN", "1")

def test_all_in_fee_and_buffer_cost_ceiling_is_enforced():
    result = _evaluate(
        policy=_policy(taker_fee_bps=Decimal("30"), safety_buffer_bps=Decimal("25"),
                       max_total_cost_bps=Decimal("50")),
    )
    assert result.action == "DEFER"
    assert result.reason == "TOTAL_COST_LIMIT_EXCEEDED"

def test_missing_sequence_or_reference_price_never_authorizes_submit():
    missing_sequence = _evaluate(
        book=OrderBookSnapshot(
            bids=(BookLevel.make("100", "200"),),
            asks=(BookLevel.make("100.1", "200"),),
            observed_at_ms=9900, sequence="",
        ),
    )
    assert missing_sequence.action == "DEFER"
    assert missing_sequence.reason == "BOOK_SEQUENCE_MISSING"

    missing_reference = _evaluate(reference_price=None)
    assert missing_reference.action == "DEFER"
    assert missing_reference.reason == "REFERENCE_PRICE_UNAVAILABLE"
