"""V3 Decimal position PnL calculator; the V2 API remains in pair_pnl_engine."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Sequence

# Executor V3 position accounting. The V2 float API above remains unchanged.
from decimal import Decimal, InvalidOperation
from typing import Mapping, Sequence


def _v3_decimal(value, name: str, *, allow_zero: bool = True) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result < 0 if allow_zero else result <= 0):
        raise ValueError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
    return result


@dataclass(frozen=True)
class V3FillFact:
    leg_type: str
    side: str
    quantity: Decimal
    price: Decimal
    base_per_unit: Decimal
    base_asset: str
    quote_asset: str
    filled_at_ms: int
    fee_amount: Decimal = Decimal("0")
    fee_asset: str = ""
    fee_verified: bool = False
    fee_fx_rate_to_reporting: Decimal | None = None
    fee_fx_verified: bool = False
    fx_rate_to_reporting: Decimal | None = None
    fx_verified: bool = False
    position_side: str = "SHORT"
    contract_type: str = "LINEAR_PERPETUAL"


@dataclass(frozen=True)
class V3FundingFact:
    amount: Decimal
    asset: str
    paid_at_ms: int
    verified: bool
    fx_rate_to_reporting: Decimal | None = None
    fx_verified: bool = False


@dataclass(frozen=True)
class V3MarketSnapshot:
    spot_exit_vwap: Decimal | None
    futures_close_vwap: Decimal | None
    observed_at_ms: int
    spot_depth_sufficient: bool
    futures_depth_sufficient: bool
    spot_fx_rate_to_reporting: Decimal | None
    futures_fx_rate_to_reporting: Decimal | None
    fx_observed_at_ms: int
    fx_verified: bool


@dataclass(frozen=True)
class V3PnLPolicy:
    reporting_currency: str = "USDT"
    max_mark_age_ms: int = 1500
    max_fx_age_ms: int = 30000
    spot_exit_fee_rate: Decimal = Decimal("0.001")
    futures_exit_fee_rate: Decimal = Decimal("0.0005")
    spot_tax_rate: Decimal = Decimal("0")
    exit_slippage_reserve_bps: Decimal = Decimal("5")
    fee_reserve: Decimal = Decimal("0")
    actual_futures_margin: Decimal | None = None


@dataclass(frozen=True)
class V3PnLResult:
    realized_pnl_gross: Decimal | None
    realized_pnl_net: Decimal | None
    unrealized_pnl_gross: Decimal | None
    estimated_exit_fees: Decimal | None
    exit_slippage_reserve: Decimal | None
    tax_reserve: Decimal | None
    funding_accrued: Decimal | None
    fees_paid: Decimal | None
    estimated_liquidation_pnl: Decimal | None
    spot_open_base_quantity: Decimal
    futures_open_base_quantity: Decimal
    spot_entry_notional: Decimal | None
    total_deployed_capital: Decimal | None
    strategy_roi_pct: Decimal | None
    return_on_deployed_capital_pct: Decimal | None
    confidence: str
    fees_complete: bool
    funding_complete: bool
    fx_complete: bool
    depth_complete: bool
    marks_fresh: bool
    missing_evidence: tuple[str, ...]
    reporting_currency: str


def _v3_position(fills: Sequence[V3FillFact], *, leg_type: str, reporting_currency: str):
    """Weighted-average position accounting; realized PnL applies only to closed qty."""
    ordered = sorted(
        (fill for fill in fills if fill.leg_type.upper() == leg_type),
        key=lambda fill: fill.filled_at_ms,
    )
    open_quantity = Decimal("0")
    average_entry = Decimal("0")
    realized = Decimal("0")
    entry_notional = Decimal("0")
    realized_complete = True
    entry_notional_complete = True
    quote_asset = ""
    base_asset = ""
    for fill in ordered:
        quantity = _v3_decimal(fill.quantity, "fill.quantity", allow_zero=False)
        price = _v3_decimal(fill.price, "fill.price", allow_zero=False)
        conversion = _v3_decimal(fill.base_per_unit, "fill.base_per_unit", allow_zero=False)
        base_quantity = quantity * conversion
        fill_side = fill.side.lower()
        quote = fill.quote_asset.upper()
        base = fill.base_asset.upper()
        if quote_asset and quote_asset != quote:
            raise ValueError(f"{leg_type} quote asset changed within one session")
        if base_asset and base_asset != base:
            raise ValueError(f"{leg_type} base asset changed within one session")
        quote_asset, base_asset = quote, base
        if leg_type == "SPOT":
            open_side = "buy"
            position_side = "LONG"
        else:
            position_side = fill.position_side.upper()
            if position_side not in {"LONG", "SHORT"}:
                raise ValueError("Futures position_side must be LONG or SHORT")
            if fill.contract_type.upper() not in {"LINEAR", "LINEAR_PERPETUAL"}:
                raise ValueError("V3 PnL currently requires linear Futures contract accounting")
            if any(existing.position_side.upper() != position_side for existing in ordered):
                raise ValueError("Futures position_side changed within one session")
            open_side = "buy" if position_side == "LONG" else "sell"
        close_side = "sell" if open_side == "buy" else "buy"
        if fill_side == open_side:
            new_quantity = open_quantity + base_quantity
            average_entry = (
                (open_quantity * average_entry + base_quantity * price) / new_quantity
            )
            open_quantity = new_quantity
            if leg_type == "SPOT":
                if quote == reporting_currency:
                    entry_fx = Decimal("1")
                elif fill.fx_verified and fill.fx_rate_to_reporting is not None:
                    entry_fx = _v3_decimal(fill.fx_rate_to_reporting, "fill.fx_rate", allow_zero=False)
                else:
                    entry_notional_complete = False
                    entry_fx = None
                if entry_fx is not None:
                    entry_notional += base_quantity * price * entry_fx
        elif fill_side == close_side:
            if base_quantity > open_quantity:
                raise ValueError(f"{leg_type} close fill exceeds verified open position")
            realized_native = (
                (price - average_entry) * base_quantity
                if position_side == "LONG"
                else (average_entry - price) * base_quantity
            )
            if quote == reporting_currency:
                close_fx = Decimal("1")
            elif fill.fx_verified and fill.fx_rate_to_reporting is not None:
                close_fx = _v3_decimal(fill.fx_rate_to_reporting, "fill.fx_rate", allow_zero=False)
            else:
                close_fx = None
                realized_complete = False
            if close_fx is not None:
                realized += realized_native * close_fx
            open_quantity -= base_quantity
            if open_quantity == 0:
                average_entry = Decimal("0")
        else:
            raise ValueError(f"unsupported {leg_type} fill side {fill_side}")
    return {
        "open_quantity": open_quantity,
        "average_entry": average_entry,
        "realized": realized,
        "entry_notional": entry_notional,
        "entry_notional_complete": entry_notional_complete,
        "realized_complete": realized_complete,
        "quote_asset": quote_asset,
        "base_asset": base_asset,
        "position_side": (
            next((f.position_side.upper() for f in ordered if leg_type == "FUTURES"), "SHORT")
        ),
    }


def compute_position_pnl_v3(
    *,
    fills: Sequence[V3FillFact],
    funding: Sequence[V3FundingFact],
    market: V3MarketSnapshot,
    policy: V3PnLPolicy,
    now_ms: int,
    funding_history_complete: bool,
) -> V3PnLResult:
    """Reproduce verified realized PnL and confidence-gated net liquidation PnL."""
    missing: list[str] = []
    reporting = policy.reporting_currency.upper()
    spot = _v3_position(fills, leg_type="SPOT", reporting_currency=reporting)
    futures = _v3_position(fills, leg_type="FUTURES", reporting_currency=reporting)
    if policy.max_mark_age_ms <= 0 or policy.max_fx_age_ms <= 0:
        raise ValueError("PnL evidence age limits must be positive")
    fees_paid = Decimal("0")
    fees_complete = True
    fx_complete = True

    for fill in fills:
        fee = _v3_decimal(fill.fee_amount, "fill.fee_amount")
        if not fill.fee_verified:
            fees_complete = False
            missing.append("UNVERIFIED_FILL_FEE")
            continue
        if fee == 0:
            continue
        fee_asset = fill.fee_asset.upper()
        quote_asset = fill.quote_asset.upper()
        if not fee_asset:
            fees_complete = False
            missing.append("FEE_ASSET_UNKNOWN")
            continue
        if fee_asset == reporting:
            fee_rate = Decimal("1")
        elif fee_asset == quote_asset:
            if quote_asset == reporting:
                fee_rate = Decimal("1")
            elif fill.fx_verified and fill.fx_rate_to_reporting is not None:
                fee_rate = _v3_decimal(fill.fx_rate_to_reporting, "fill.fx_rate", allow_zero=False)
            else:
                fees_complete = False
                fx_complete = False
                missing.append("FEE_QUOTE_FX_UNKNOWN")
                continue
        elif fee_asset == fill.base_asset.upper():
            if quote_asset == reporting:
                fee_rate = _v3_decimal(fill.price, "fill.price", allow_zero=False)
            elif fill.fx_verified and fill.fx_rate_to_reporting is not None:
                fee_rate = _v3_decimal(fill.price, "fill.price", allow_zero=False) * _v3_decimal(
                    fill.fx_rate_to_reporting, "fill.fx_rate", allow_zero=False
                )
            else:
                fees_complete = False
                fx_complete = False
                missing.append("FEE_BASE_FX_UNKNOWN")
                continue
        elif fill.fee_fx_verified and fill.fee_fx_rate_to_reporting is not None:
            fee_rate = _v3_decimal(fill.fee_fx_rate_to_reporting, "fee_fx_rate", allow_zero=False)
        else:
            fees_complete = False
            fx_complete = False
            missing.append("THIRD_ASSET_FEE_FX_UNKNOWN")
            continue
        fees_paid += fee * fee_rate

    funding_complete = bool(funding_history_complete)
    funding_total = Decimal("0")
    if not funding_history_complete:
        missing.append("FUNDING_HISTORY_INCOMPLETE")
    for payment in funding:
        amount = Decimal(str(payment.amount))
        if not payment.verified:
            funding_complete = False
            missing.append("UNVERIFIED_FUNDING_PAYMENT")
            continue
        asset = payment.asset.upper()
        if asset == reporting:
            rate = Decimal("1")
        elif payment.fx_verified and payment.fx_rate_to_reporting is not None:
            rate = _v3_decimal(payment.fx_rate_to_reporting, "funding.fx_rate", allow_zero=False)
        else:
            funding_complete = False
            fx_complete = False
            missing.append("FUNDING_FX_UNKNOWN")
            continue
        funding_total += amount * rate

    has_open_position = spot["open_quantity"] > 0 or futures["open_quantity"] > 0
    mark_age = now_ms - int(market.observed_at_ms)
    marks_fresh = not has_open_position or 0 <= mark_age <= policy.max_mark_age_ms
    fx_age = now_ms - int(market.fx_observed_at_ms)
    market_fx_fresh = not has_open_position or (0 <= fx_age <= policy.max_fx_age_ms and market.fx_verified)
    if not marks_fresh:
        missing.append("MARK_STALE")
    if not market_fx_fresh:
        fx_complete = False
        missing.append("MARK_FX_STALE_OR_UNVERIFIED")

    spot_open = spot["open_quantity"]
    futures_open = futures["open_quantity"]
    depth_complete = True
    unrealized = Decimal("0")
    estimated_exit_fees = Decimal("0")
    slippage_reserve = Decimal("0")
    tax_reserve = Decimal("0")
    if spot_open > 0:
        if not marks_fresh or not market.spot_depth_sufficient or market.spot_exit_vwap is None:
            depth_complete = False
            missing.append("SPOT_EXIT_DEPTH_INSUFFICIENT")
        elif spot["quote_asset"] == reporting:
            spot_exit = _v3_decimal(market.spot_exit_vwap, "spot_exit_vwap", allow_zero=False)
            spot_fx = Decimal("1")
        elif not market_fx_fresh or market.spot_fx_rate_to_reporting is None:
            fx_complete = False
            missing.append("SPOT_MARK_FX_UNKNOWN")
        else:
            spot_exit = _v3_decimal(market.spot_exit_vwap, "spot_exit_vwap", allow_zero=False)
            spot_fx = _v3_decimal(market.spot_fx_rate_to_reporting, "spot_fx_rate", allow_zero=False)
            spot_exit_notional = spot_open * spot_exit * spot_fx
            unrealized += (spot_exit - spot["average_entry"]) * spot_open * spot_fx
            tax_reserve = spot_exit_notional * _v3_decimal(policy.spot_tax_rate, "spot_tax_rate")
            estimated_exit_fees += spot_exit_notional * _v3_decimal(policy.spot_exit_fee_rate, "spot_exit_fee_rate")
            slippage_reserve += spot_exit_notional * _v3_decimal(policy.exit_slippage_reserve_bps, "exit_slippage_reserve_bps") / Decimal("10000")
    if futures_open > 0:
        if not marks_fresh or not market.futures_depth_sufficient or market.futures_close_vwap is None:
            depth_complete = False
            missing.append("FUTURES_CLOSE_DEPTH_INSUFFICIENT")
        elif futures["quote_asset"] == reporting:
            futures_exit = _v3_decimal(market.futures_close_vwap, "futures_close_vwap", allow_zero=False)
            futures_fx = Decimal("1")
        elif not market_fx_fresh or market.futures_fx_rate_to_reporting is None:
            fx_complete = False
            missing.append("FUTURES_MARK_FX_UNKNOWN")
        else:
            futures_exit = _v3_decimal(market.futures_close_vwap, "futures_close_vwap", allow_zero=False)
            futures_fx = _v3_decimal(market.futures_fx_rate_to_reporting, "futures_fx_rate", allow_zero=False)
            futures_exit_notional = futures_open * futures_exit * futures_fx
            futures_unrealized = (
                (futures["average_entry"] - futures_exit) * futures_open
                if futures["position_side"] == "SHORT"
                else (futures_exit - futures["average_entry"]) * futures_open
            )
            unrealized += futures_unrealized * futures_fx
            estimated_exit_fees += futures_exit_notional * _v3_decimal(policy.futures_exit_fee_rate, "futures_exit_fee_rate")
            slippage_reserve += futures_exit_notional * _v3_decimal(policy.exit_slippage_reserve_bps, "exit_slippage_reserve_bps") / Decimal("10000")

    realized_complete = spot["realized_complete"] and futures["realized_complete"]
    entry_notional_complete = spot["entry_notional_complete"]
    if not realized_complete:
        fx_complete = False
        missing.append("REALIZED_PNL_FX_UNKNOWN")
    if not entry_notional_complete:
        fx_complete = False
        missing.append("SPOT_ENTRY_FX_UNKNOWN")
    realized_gross = (
        spot["realized"] + futures["realized"] if realized_complete else None
    )
    spot_entry_notional = spot["entry_notional"] if entry_notional_complete else None
    realized_net = (
        realized_gross + funding_total - fees_paid
        if realized_gross is not None and fees_complete and funding_complete else None
    )
    exit_complete = depth_complete and marks_fresh and fx_complete
    liquidation = (
        realized_gross + unrealized + funding_total - fees_paid
        - estimated_exit_fees - slippage_reserve - tax_reserve
        if realized_gross is not None and exit_complete and fees_complete and funding_complete else None
    )
    capital = None
    strategy_roi = None
    deployed_roi = None
    if policy.actual_futures_margin is not None:
        margin = _v3_decimal(policy.actual_futures_margin, "actual_futures_margin")
        fee_reserve = _v3_decimal(policy.fee_reserve, "fee_reserve")
        capital = (
            spot_entry_notional + margin + fee_reserve
            if spot_entry_notional is not None else None
        )
        if liquidation is not None and spot_entry_notional is not None and spot_entry_notional > 0:
            strategy_roi = liquidation / spot_entry_notional * Decimal("100")
        if liquidation is not None and capital is not None and capital > 0:
            deployed_roi = liquidation / capital * Decimal("100")
    else:
        missing.append("ACTUAL_FUTURES_MARGIN_UNKNOWN")

    if liquidation is None:
        missing.append("NET_LIQUIDATION_PNL_INCOMPLETE")
    confidence = "HIGH" if (
        fees_complete and funding_complete and fx_complete and depth_complete
        and marks_fresh and liquidation is not None
    ) else "LOW"
    return V3PnLResult(
        realized_pnl_gross=realized_gross,
        realized_pnl_net=realized_net,
        unrealized_pnl_gross=unrealized if depth_complete and marks_fresh else None,
        estimated_exit_fees=estimated_exit_fees if depth_complete and marks_fresh else None,
        exit_slippage_reserve=slippage_reserve if depth_complete and marks_fresh else None,
        tax_reserve=tax_reserve if depth_complete and marks_fresh else None,
        funding_accrued=funding_total if funding_complete else None,
        fees_paid=fees_paid if fees_complete else None,
        estimated_liquidation_pnl=liquidation,
        spot_open_base_quantity=spot_open,
        futures_open_base_quantity=futures_open,
        spot_entry_notional=spot_entry_notional,
        total_deployed_capital=capital,
        strategy_roi_pct=strategy_roi,
        return_on_deployed_capital_pct=deployed_roi,
        confidence=confidence,
        fees_complete=fees_complete, funding_complete=funding_complete,
        fx_complete=fx_complete, depth_complete=depth_complete,
        marks_fresh=marks_fresh, missing_evidence=tuple(sorted(set(missing))),
        reporting_currency=reporting,
    )
