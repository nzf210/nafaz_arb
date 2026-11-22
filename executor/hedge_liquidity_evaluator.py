"""Decimal-only V3 hedge liquidity evaluation.

This is a pure decision component. It consumes an already validated book
snapshot and never calls an exchange or mutates an order.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Iterable


class LiquidityInputError(ValueError):
    """A market-data or quantity input is invalid and cannot authorize a hedge."""


def _d(value, name: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LiquidityInputError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result < 0 if allow_zero else result <= 0):
        raise LiquidityInputError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
    return result


def _round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self):
        object.__setattr__(self, "price", _d(self.price, "book.price"))
        object.__setattr__(self, "quantity", _d(self.quantity, "book.quantity"))

    @classmethod
    def make(cls, price, quantity) -> "BookLevel":
        return cls(_d(price, "book.price"), _d(quantity, "book.quantity"))


@dataclass(frozen=True)
class OrderBookSnapshot:
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at_ms: int
    sequence: str
    sequence_valid: bool = True
    checksum_valid: bool = True


@dataclass(frozen=True)
class FXSnapshot:
    quote_to_usd_rate: Decimal
    observed_at_ms: int
    source: str

    def __post_init__(self):
        object.__setattr__(self, "quote_to_usd_rate", _d(self.quote_to_usd_rate, "fx.quote_to_usd_rate"))


@dataclass(frozen=True)
class HedgeLiquidityPolicy:
    internal_min_notional_usd: Decimal = Decimal("5")
    max_book_age_ms: int = 1500
    max_fx_age_ms: int = 30000
    max_future_clock_skew_ms: int = 250
    max_slippage_bps: Decimal = Decimal("25")
    taker_fee_bps: Decimal = Decimal("5")
    safety_buffer_bps: Decimal = Decimal("5")
    max_unhedged_notional_usd: Decimal = Decimal("25")
    max_unhedged_wait_ms: int = 5000
    max_adverse_move_bps: Decimal = Decimal("100")

    def __post_init__(self):
        for name in ("internal_min_notional_usd", "max_unhedged_notional_usd"):
            object.__setattr__(self, name, _d(getattr(self, name), name))
        for name in ("max_slippage_bps", "taker_fee_bps", "safety_buffer_bps", "max_adverse_move_bps"):
            object.__setattr__(self, name, _d(getattr(self, name), name, allow_zero=True))
        if min(self.max_book_age_ms, self.max_fx_age_ms, self.max_unhedged_wait_ms) <= 0:
            raise LiquidityInputError("liquidity time limits must be positive")
        if self.max_future_clock_skew_ms < 0:
            raise LiquidityInputError("future clock skew limit must be non-negative")


@dataclass(frozen=True)
class HedgeLiquidityDecision:
    action: str
    reason: str
    executable_base_quantity: Decimal
    executable_native_quantity: Decimal
    executable_vwap: Decimal | None
    executable_notional_quote: Decimal
    executable_notional_usd: Decimal
    slippage_bps: Decimal | None
    estimated_fee_bps: Decimal
    estimated_total_cost_bps: Decimal | None
    book_age_ms: int | None
    wait_elapsed_ms: int
    wait_remaining_ms: int
    book_sequence: str
    next_evaluate_at_ms: int


def _defer_or_derisk(
    *, reason: str, now_ms: int, unhedged_since_ms: int,
    policy: HedgeLiquidityPolicy, book: OrderBookSnapshot,
    unhedged_notional_usd: Decimal,
) -> HedgeLiquidityDecision:
    elapsed = max(0, now_ms - unhedged_since_ms)
    remaining = max(0, policy.max_unhedged_wait_ms - elapsed)
    action = (
        "DE_RISK_REQUIRED"
        if elapsed >= policy.max_unhedged_wait_ms
        or unhedged_notional_usd > policy.max_unhedged_notional_usd
        else "DEFER"
    )
    delay = min(1000, max(100, remaining)) if action == "DEFER" else 0
    return HedgeLiquidityDecision(
        action=action, reason=reason,
        executable_base_quantity=Decimal("0"),
        executable_native_quantity=Decimal("0"),
        executable_vwap=None,
        executable_notional_quote=Decimal("0"),
        executable_notional_usd=Decimal("0"),
        slippage_bps=None,
        estimated_fee_bps=taker_fee,
        estimated_total_cost_bps=None,
        book_age_ms=max(0, now_ms - book.observed_at_ms),
        wait_elapsed_ms=elapsed, wait_remaining_ms=remaining,
        book_sequence=book.sequence,
        next_evaluate_at_ms=now_ms + delay,
    )


def evaluate_hedge_liquidity(
    *,
    required_base_quantity,
    already_reserved_base_quantity,
    base_per_unit,
    quantity_step,
    venue_min_order_quantity,
    venue_min_notional_quote,
    position_side: str,
    book: OrderBookSnapshot,
    fx: FXSnapshot,
    policy: HedgeLiquidityPolicy,
    now_ms: int,
    unhedged_since_ms: int,
    unhedged_notional_usd,
    reference_price=None,
) -> HedgeLiquidityDecision:
    """Return SUBMIT/PARTIAL_SUBMIT/DEFER/DE_RISK_REQUIRED without side effects."""
    required = _d(required_base_quantity, "required_base_quantity")
    reserved = _d(already_reserved_base_quantity, "already_reserved_base_quantity", allow_zero=True)
    conversion = _d(base_per_unit, "base_per_unit")
    step = _d(quantity_step, "quantity_step")
    min_native = _d(venue_min_order_quantity, "venue_min_order_quantity", allow_zero=True)
    min_quote = _d(venue_min_notional_quote, "venue_min_notional_quote", allow_zero=True)
    unhedged_usd = _d(unhedged_notional_usd, "unhedged_notional_usd", allow_zero=True)
    side = str(position_side or "").strip().upper()
    if side not in {"LONG", "SHORT"}:
        raise LiquidityInputError("position_side must be LONG or SHORT")
    if policy.max_book_age_ms <= 0 or policy.max_fx_age_ms <= 0 or policy.max_unhedged_wait_ms <= 0:
        raise LiquidityInputError("liquidity time limits must be positive")
    max_unhedged = _d(policy.max_unhedged_notional_usd, "max_unhedged_notional_usd")
    min_internal = _d(policy.internal_min_notional_usd, "internal_min_notional_usd")
    max_slippage = _d(policy.max_slippage_bps, "max_slippage_bps", allow_zero=True)
    taker_fee = _d(policy.taker_fee_bps, "taker_fee_bps", allow_zero=True)
    safety_buffer = _d(policy.safety_buffer_bps, "safety_buffer_bps", allow_zero=True)
    max_adverse = _d(policy.max_adverse_move_bps, "max_adverse_move_bps", allow_zero=True)
    remaining_base = required - reserved
    if remaining_base <= 0:
        return HedgeLiquidityDecision(
            action="DEFER", reason="NO_UNRESERVED_OBLIGATION",
            executable_base_quantity=Decimal("0"),
            executable_native_quantity=Decimal("0"), executable_vwap=None,
            executable_notional_quote=Decimal("0"), executable_notional_usd=Decimal("0"),
            slippage_bps=None, estimated_fee_bps=policy.taker_fee_bps,
            estimated_total_cost_bps=None,
            book_age_ms=max(0, now_ms - book.observed_at_ms),
            wait_elapsed_ms=max(0, now_ms - unhedged_since_ms),
            wait_remaining_ms=max(0, policy.max_unhedged_wait_ms - max(0, now_ms - unhedged_since_ms)),
            book_sequence=book.sequence, next_evaluate_at_ms=now_ms + 500,
        )

    if unhedged_usd > max_unhedged:
        return _defer_or_derisk(
            reason="UNHEDGED_NOTIONAL_LIMIT_EXCEEDED", now_ms=now_ms,
            unhedged_since_ms=unhedged_since_ms, policy=policy,
            book=book, unhedged_notional_usd=unhedged_usd,
        )
    age = now_ms - int(book.observed_at_ms)
    fx_age = now_ms - int(fx.observed_at_ms)
    if book.observed_at_ms > now_ms + policy.max_future_clock_skew_ms:
        return _defer_or_derisk(reason="BOOK_TIMESTAMP_IN_FUTURE", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)
    if age < 0 or age > policy.max_book_age_ms:
        return _defer_or_derisk(reason="BOOK_STALE", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)
    if not book.sequence_valid:
        return _defer_or_derisk(reason="BOOK_SEQUENCE_GAP", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)
    if not book.checksum_valid:
        return _defer_or_derisk(reason="BOOK_CHECKSUM_INVALID", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)
    if fx_age < 0 or fx_age > policy.max_fx_age_ms or not str(fx.source or "").strip():
        return _defer_or_derisk(reason="FX_STALE_OR_UNVERIFIED", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)
    fx_rate = _d(fx.quote_to_usd_rate, "fx.quote_to_usd_rate")
    bids = tuple(sorted(book.bids, key=lambda x: x.price, reverse=True))
    asks = tuple(sorted(book.asks, key=lambda x: x.price))
    if not bids or not asks or bids[0].price >= asks[0].price:
        return _defer_or_derisk(reason="BOOK_EMPTY_OR_CROSSED", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)

    is_short = side == "SHORT"
    levels = bids if is_short else asks
    best = levels[0].price
    if reference_price is not None:
        reference = _d(reference_price, "reference_price")
        adverse_bps = (
            (reference - best) / reference * Decimal("10000")
            if is_short else (best - reference) / reference * Decimal("10000")
        )
        if adverse_bps > max_adverse:
            return _defer_or_derisk(reason="ADVERSE_PRICE_MOVE", now_ms=now_ms,
                                    unhedged_since_ms=unhedged_since_ms, policy=policy,
                                    book=book, unhedged_notional_usd=unhedged_usd)

    target_native = _round_down(remaining_base / conversion, step)
    if target_native <= 0:
        return _defer_or_derisk(reason="BELOW_NATIVE_LOT_STEP", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)
    available_native = sum((level.quantity for level in levels), Decimal("0"))
    executable_native = _round_down(min(target_native, available_native), step)
    if executable_native < min_native:
        return _defer_or_derisk(reason="INSUFFICIENT_EXECUTABLE_DEPTH", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)

    remaining_native = executable_native
    quote_notional = Decimal("0")
    native_filled = Decimal("0")
    for level in levels:
        take = min(remaining_native, level.quantity)
        if take <= 0:
            continue
        quote_notional += take * conversion * level.price
        native_filled += take
        remaining_native -= take
        if remaining_native <= 0:
            break
    if native_filled <= 0 or remaining_native > 0:
        return _defer_or_derisk(reason="DEPTH_SWEEP_INCOMPLETE", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)

    base_filled = native_filled * conversion
    vwap = quote_notional / base_filled
    slippage = (
        (best - vwap) / best * Decimal("10000")
        if is_short else (vwap - best) / best * Decimal("10000")
    )
    slippage = max(Decimal("0"), slippage)
    notional_usd = quote_notional * fx_rate
    min_notional_usd = max(
        min_internal,
        min_quote * fx_rate,
    )
    elapsed = max(0, now_ms - unhedged_since_ms)
    wait_remaining = max(0, policy.max_unhedged_wait_ms - elapsed)
    total_cost = slippage + taker_fee + safety_buffer
    if notional_usd < min_notional_usd:
        return _defer_or_derisk(reason="BELOW_INTERNAL_OR_VENUE_MIN_NOTIONAL",
                                now_ms=now_ms, unhedged_since_ms=unhedged_since_ms,
                                policy=policy, book=book, unhedged_notional_usd=unhedged_usd)
    if slippage > max_slippage:
        return _defer_or_derisk(reason="SLIPPAGE_LIMIT_EXCEEDED", now_ms=now_ms,
                                unhedged_since_ms=unhedged_since_ms, policy=policy,
                                book=book, unhedged_notional_usd=unhedged_usd)

    full = base_filled >= remaining_base
    action = "SUBMIT" if full else "PARTIAL_SUBMIT"
    return HedgeLiquidityDecision(
        action=action, reason="EXECUTABLE_DEPTH_AND_COST_WITHIN_POLICY",
        executable_base_quantity=base_filled,
        executable_native_quantity=native_filled,
        executable_vwap=vwap,
        executable_notional_quote=quote_notional,
        executable_notional_usd=notional_usd,
        slippage_bps=slippage, estimated_fee_bps=policy.taker_fee_bps,
        estimated_total_cost_bps=total_cost,
        book_age_ms=max(0, age), wait_elapsed_ms=elapsed,
        wait_remaining_ms=wait_remaining, book_sequence=book.sequence,
        next_evaluate_at_ms=now_ms,
    )
