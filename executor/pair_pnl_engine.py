"""Pair PnL Engine (Executor V2).

Computes combined economic net liquidation value for two-leg Spot + Futures arbitrage:
- Spot proceeds at executable bid depth minus exit fee and entry cost
- Futures PnL at executable ask depth minus exit fee plus accrued funding
- Normalizes all components into standard reporting currency (USDT)
- Eliminates naive separate leg profit summation
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Optional, Sequence

logger = logging.getLogger("executor.pair_pnl_engine")


@dataclass
class PairPnLSnapshot:
    session_id: str
    pair_key: str
    spot_net_liquidation_pnl: float
    futures_net_liquidation_pnl: float
    pair_net_liquidation_pnl: float
    pair_net_roi_pct: float
    total_entry_fee: float
    expected_close_fee: float
    funding_accrued: float
    currency: str = "USDT"
    spot_executable_bid: float = 0.0
    futures_executable_ask: float = 0.0
    net_spread_roi_pct: float = 0.0
    return_on_deployed_capital_pct: float = 0.0
    total_capital_deployed: float = 0.0
    total_fees_usd: float = 0.0
    spot_entry_fee_usd: float = 0.0
    futures_entry_fee_usd: float = 0.0
    spot_exit_fee_usd: float = 0.0
    futures_exit_fee_usd: float = 0.0


class PairPnLEngine:
    """Calculates true combined economic liquidation value for two-leg positions."""

    futures_taker_fee_rate: float = 0.0005

    def __init__(self, futures_taker_fee_rate: float = 0.0005) -> None:
        self.futures_taker_fee_rate = futures_taker_fee_rate

    def compute_pair_pnl(
        self,
        *,
        session_id: str,
        pair_key: str,
        spot_qty: float,
        spot_entry_cost_quote: float,
        spot_executable_bid: float,
        spot_exit_fee_rate: float = 0.001,
        spot_entry_fee_quote: float = 0.0,
        futures_qty: float,
        futures_entry_price: float,
        futures_executable_ask: float,
        futures_close_fee_rate: float = 0.0005,
        futures_entry_fee_rate: Optional[float] = None,
        futures_entry_fee: Optional[float] = None,
        position_state: Optional[dict] = None,
        contract_size: float = 1.0,
        funding_accrued: float = 0.0,
        fx_spot_to_report: float = 1.0,
        fx_futures_to_report: float = 1.0,
        reporting_currency: str = "USDT",
        effective_leverage: float = 1.0,
        actual_initial_margin_usd: Optional[float] = None,
        fee_reserve_usd: float = 0.0,
        spot_tax_rate: float = 0.0,
    ) -> PairPnLSnapshot:
        """Calculate combined two-leg net liquidation PnL if closed immediately at orderbook depth."""
        pos_state = position_state if isinstance(position_state, dict) else {}
        if actual_initial_margin_usd is None:
            raw_margin = (
                pos_state.get("actual_initial_margin_usd")
                or pos_state.get("futures_initial_margin")
                or pos_state.get("initial_margin")
            )
            if raw_margin is not None:
                try:
                    actual_initial_margin_usd = float(raw_margin)
                except (TypeError, ValueError):
                    actual_initial_margin_usd = None

        if effective_leverage == 1.0:
            raw_lev = pos_state.get("effective_leverage") or pos_state.get("leverage")
            if raw_lev is not None:
                try:
                    effective_leverage = float(raw_lev)
                except (TypeError, ValueError):
                    effective_leverage = 1.0

        if fee_reserve_usd == 0.0:
            raw_reserve = pos_state.get("fee_reserve_usd") or pos_state.get("fee_reserve")
            if raw_reserve is not None:
                try:
                    fee_reserve_usd = float(raw_reserve)
                except (TypeError, ValueError):
                    fee_reserve_usd = 0.0

        if spot_tax_rate == 0.0:
            raw_tax = pos_state.get("spot_tax_rate") or pos_state.get("tax_rate")
            if raw_tax is not None:
                try:
                    spot_tax_rate = float(raw_tax)
                except (TypeError, ValueError):
                    spot_tax_rate = 0.0

        # 1. Spot Economics
        # Gross proceeds if sold at current executable bid depth
        spot_effective_exit_fee_rate = spot_exit_fee_rate + spot_tax_rate
        spot_gross_proceeds = spot_qty * spot_executable_bid
        spot_close_fee = spot_gross_proceeds * spot_effective_exit_fee_rate
        spot_net_proceeds = spot_gross_proceeds - spot_close_fee
        spot_net_pnl_native = spot_net_proceeds - spot_entry_cost_quote - spot_entry_fee_quote
        spot_net_pnl_report = spot_net_pnl_native * fx_spot_to_report

        # 2. Futures Economics (Short Perpetual Hedge)
        # Closing short means BUYING back at ask depth
        base_exposure = futures_qty * contract_size
        futures_gross_close_notional = base_exposure * futures_executable_ask
        futures_close_fee = futures_gross_close_notional * futures_close_fee_rate
        futures_exit_fee_native = futures_close_fee

        futures_entry_notional = base_exposure * futures_entry_price
        fee_rate = (
            futures_entry_fee_rate
            if futures_entry_fee_rate is not None
            else getattr(self, "futures_taker_fee_rate", 0.0005)
        )
        if futures_entry_fee is not None:
            futures_entry_fee_native = float(futures_entry_fee)
        else:
            futures_entry_fee_native = float(
                pos_state.get("futures_entry_fee", 0.0)
                or (futures_entry_notional * fee_rate)
            )

        # Gross PnL from short: (Entry Price - Exit Ask Price) * Base Quantity
        futures_gross_pnl_native = (futures_entry_price - futures_executable_ask) * base_exposure
        funding_fee_net_native = funding_accrued
        futures_net_pnl_native = (
            futures_gross_pnl_native
            - futures_entry_fee_native
            - futures_exit_fee_native
            + funding_fee_net_native
        )
        futures_net_pnl_report = futures_net_pnl_native * fx_futures_to_report

        # 3. Combined Economics
        pair_net_pnl = spot_net_pnl_report + futures_net_pnl_report

        # 4-Fill Unified Fee: Sum all four legs explicitly in reporting currency
        spot_entry_fee_usd = spot_entry_fee_quote * fx_spot_to_report
        futures_entry_fee_usd = futures_entry_fee_native * fx_futures_to_report
        spot_exit_fee_usd = spot_close_fee * fx_spot_to_report
        futures_exit_fee_usd = futures_close_fee * fx_futures_to_report
        total_entry_fee = spot_entry_fee_usd + futures_entry_fee_usd
        expected_close_fee = spot_exit_fee_usd + futures_exit_fee_usd
        total_fees_usd = spot_entry_fee_usd + futures_entry_fee_usd + spot_exit_fee_usd + futures_exit_fee_usd

        # Capital basis: Actual deployed capital without hardcoded margin assumption
        spot_entry_notional_usd = spot_entry_cost_quote * fx_spot_to_report
        futures_entry_notional_usd = futures_entry_notional * fx_futures_to_report
        lev = max(float(effective_leverage or 1.0), 1.0)
        if actual_initial_margin_usd is not None and actual_initial_margin_usd > 0:
            futures_margin_used_usd = float(actual_initial_margin_usd)
        else:
            futures_margin_used_usd = futures_entry_notional_usd / lev

        total_capital_deployed_usd = (
            spot_entry_notional_usd + futures_margin_used_usd + float(fee_reserve_usd or 0.0)
        )

        # Two ROI metrics:
        # 1. net_spread_roi_pct: per-leg efficiency vs spot entry notional
        net_spread_roi_pct = (
            (pair_net_pnl / spot_entry_notional_usd * 100.0)
            if spot_entry_notional_usd > 0 else 0.0
        )
        # 2. return_on_deployed_capital_pct: return on total locked/deployed capital
        return_on_deployed_capital_pct = (
            (pair_net_pnl / total_capital_deployed_usd * 100.0)
            if total_capital_deployed_usd > 0 else 0.0
        )
        pair_roi_pct = return_on_deployed_capital_pct

        return PairPnLSnapshot(
            session_id=session_id,
            pair_key=pair_key,
            spot_net_liquidation_pnl=round(spot_net_pnl_report, 4),
            futures_net_liquidation_pnl=round(futures_net_pnl_report, 4),
            pair_net_liquidation_pnl=round(pair_net_pnl, 4),
            pair_net_roi_pct=round(pair_roi_pct, 4),
            total_entry_fee=round(total_entry_fee, 4),
            expected_close_fee=round(expected_close_fee, 4),
            funding_accrued=round(funding_accrued * fx_futures_to_report, 4),
            currency=reporting_currency,
            spot_executable_bid=spot_executable_bid,
            futures_executable_ask=futures_executable_ask,
            net_spread_roi_pct=round(net_spread_roi_pct, 4),
            return_on_deployed_capital_pct=round(return_on_deployed_capital_pct, 4),
            total_capital_deployed=round(total_capital_deployed_usd, 4),
            total_fees_usd=round(total_fees_usd, 4),
            spot_entry_fee_usd=round(spot_entry_fee_usd, 4),
            futures_entry_fee_usd=round(futures_entry_fee_usd, 4),
            spot_exit_fee_usd=round(spot_exit_fee_usd, 4),
            futures_exit_fee_usd=round(futures_exit_fee_usd, 4),
        )


class PairPnLInputError(ValueError):
    """A valuation input is missing, stale, unverified, or inconsistent."""


def _pnl_decimal(value, name: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PairPnLInputError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result < 0 if allow_zero else result <= 0):
        raise PairPnLInputError(
            f"{name} must be {'non-negative' if allow_zero else 'positive'}"
        )
    return result


def _pnl_signed_decimal(value, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PairPnLInputError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise PairPnLInputError(f"{name} must be a finite decimal")
    return result


@dataclass(frozen=True)
class PnLBookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self):
        object.__setattr__(self, "price", _pnl_decimal(self.price, "book.price"))
        object.__setattr__(self, "quantity", _pnl_decimal(self.quantity, "book.quantity"))


@dataclass(frozen=True)
class PnLFXEvidence:
    quote_to_reporting_rate: Decimal
    source: str
    observed_at_ms: int
    verified: bool
    max_age_ms: int = 30000

    def __post_init__(self):
        object.__setattr__(
            self, "quote_to_reporting_rate",
            _pnl_decimal(self.quote_to_reporting_rate, "fx.quote_to_reporting_rate"),
        )
        if not str(self.source or "").strip() or self.verified is not True:
            raise PairPnLInputError("FX evidence must have a source and be verified")
        if isinstance(self.observed_at_ms, bool) or not isinstance(self.observed_at_ms, int):
            raise PairPnLInputError("FX observed_at_ms must be an integer timestamp")
        if isinstance(self.max_age_ms, bool) or not isinstance(self.max_age_ms, int) or self.max_age_ms <= 0:
            raise PairPnLInputError("FX max_age_ms must be a positive integer")


@dataclass(frozen=True)
class PairPnLSnapshotV3:
    session_id: str
    pair_key: str
    reporting_currency: str
    evaluated_at_ms: int
    spot_estimated_liquidation_pnl: Decimal
    futures_estimated_liquidation_pnl: Decimal
    funding_reporting_amount: Decimal
    entry_fees_reporting: Decimal
    estimated_exit_fees_reporting: Decimal
    estimated_tax_reporting: Decimal
    conversion_cost_reporting: Decimal
    estimated_net_liquidation_pnl: Decimal
    strategy_roi_pct: Decimal
    return_on_deployed_capital_pct: Decimal | None
    total_deployed_capital_reporting: Decimal | None
    spot_exit_vwap: Decimal
    futures_exit_vwap: Decimal
    spot_slippage_bps: Decimal
    futures_slippage_bps: Decimal
    spot_executable_base_quantity: Decimal
    futures_executable_base_quantity: Decimal
    hedge_drift_base_quantity: Decimal
    confidence: str
    provenance: dict


def _sweep_pnl_depth(
    levels: Sequence[PnLBookLevel], quantity: Decimal, *, side: str,
    contract_size: Decimal = Decimal("1"),
) -> tuple[Decimal, Decimal, Decimal]:
    if not levels:
        raise PairPnLInputError("executable order-book depth is empty")
    ordered = sorted(levels, key=lambda level: level.price, reverse=(side == "SELL"))
    best = ordered[0].price
    remaining = quantity
    quote_notional = Decimal("0")
    filled = Decimal("0")
    for level in ordered:
        take = min(remaining, level.quantity)
        if take <= 0:
            continue
        quote_notional += take * contract_size * level.price
        filled += take
        remaining -= take
        if remaining <= 0:
            break
    if remaining > 0 or filled != quantity:
        raise PairPnLInputError("executable order-book depth is insufficient for full liquidation")
    base_quantity = filled * contract_size
    vwap = quote_notional / base_quantity
    slippage = (
        (best - vwap) / best * Decimal("10000")
        if side == "SELL" else (vwap - best) / best * Decimal("10000")
    )
    return vwap, quote_notional, max(Decimal("0"), slippage)


def compute_pair_pnl_v3(
    *,
    session_id: str,
    pair_key: str,
    reporting_currency: str,
    now_ms: int,
    max_book_age_ms: int,
    spot_quantity_base,
    spot_entry_cost_quote,
    spot_entry_fee_quote,
    spot_entry_fee_verified: bool,
    spot_entry_fee_source: str,
    spot_bid_levels: Sequence[PnLBookLevel],
    spot_book_observed_at_ms: int,
    spot_book_sequence: str,
    spot_book_checksum_valid: bool,
    spot_fx: PnLFXEvidence,
    spot_exit_fee_rate,
    spot_tax_rate,
    futures_quantity_native,
    futures_contract_size,
    futures_entry_price,
    futures_entry_fee_quote,
    futures_entry_fee_verified: bool,
    futures_entry_fee_source: str,
    futures_ask_levels: Sequence[PnLBookLevel],
    futures_book_observed_at_ms: int,
    futures_book_sequence: str,
    futures_book_checksum_valid: bool,
    futures_fx: PnLFXEvidence,
    futures_exit_fee_rate,
    exit_fee_policy_verified: bool,
    exit_fee_policy_source: str,
    funding_reporting_amount,
    funding_verified: bool,
    funding_source: str,
    conversion_cost_reporting=Decimal("0"),
    actual_futures_margin_reporting=None,
    fee_reserve_reporting=Decimal("0"),
) -> PairPnLSnapshotV3:
    """Compute estimated pair liquidation PnL from verified fills and executable depth.

    This is a valuation only. It never labels estimated liquidation as realized PnL.
    Missing/stale books, fee verification, FX, or funding evidence fail closed.
    """
    if not str(session_id or "").strip() or not str(pair_key or "").strip() or not str(reporting_currency or "").strip():
        raise PairPnLInputError("session_id, pair_key and reporting_currency are required")
    if isinstance(now_ms, bool) or not isinstance(now_ms, int):
        raise PairPnLInputError("now_ms must be an integer timestamp")
    if isinstance(max_book_age_ms, bool) or not isinstance(max_book_age_ms, int) or max_book_age_ms <= 0:
        raise PairPnLInputError("max_book_age_ms must be a positive integer")
    if not isinstance(spot_entry_fee_verified, bool) or spot_entry_fee_verified is not True or not str(spot_entry_fee_source or "").strip():
        raise PairPnLInputError("Spot entry fee valuation must be verified with a source")
    if not isinstance(futures_entry_fee_verified, bool) or futures_entry_fee_verified is not True or not str(futures_entry_fee_source or "").strip():
        raise PairPnLInputError("Futures entry fee valuation must be verified with a source")
    if exit_fee_policy_verified is not True or not str(exit_fee_policy_source or "").strip():
        raise PairPnLInputError("exit fee policy must be verified with a source")
    if not isinstance(funding_verified, bool) or funding_verified is not True or not str(funding_source or "").strip():
        raise PairPnLInputError("funding amount must have verified settlement provenance")
    if spot_book_checksum_valid is not True or futures_book_checksum_valid is not True:
        raise PairPnLInputError("order-book checksum must be valid")
    if not str(spot_book_sequence or "").strip() or not str(futures_book_sequence or "").strip():
        raise PairPnLInputError("order-book sequence is required")
    for name, observed in (
        ("Spot", spot_book_observed_at_ms),
        ("Futures", futures_book_observed_at_ms),
    ):
        if isinstance(observed, bool) or not isinstance(observed, int):
            raise PairPnLInputError(f"{name} book timestamp must be an integer")
        age = now_ms - observed
        if age < 0 or age > max_book_age_ms:
            raise PairPnLInputError(f"{name} order book is stale or from the future")
    for name, fx in (("Spot", spot_fx), ("Futures", futures_fx)):
        age = now_ms - fx.observed_at_ms
        if age < 0 or age > fx.max_age_ms:
            raise PairPnLInputError(f"{name} FX evidence is stale or from the future")

    spot_qty = _pnl_decimal(spot_quantity_base, "spot_quantity_base")
    spot_cost = _pnl_decimal(spot_entry_cost_quote, "spot_entry_cost_quote")
    spot_entry_fee = _pnl_decimal(spot_entry_fee_quote, "spot_entry_fee_quote", allow_zero=True)
    spot_exit_rate = _pnl_decimal(spot_exit_fee_rate, "spot_exit_fee_rate", allow_zero=True)
    tax_rate = _pnl_decimal(spot_tax_rate, "spot_tax_rate", allow_zero=True)
    if spot_exit_rate >= 1 or tax_rate >= 1:
        raise PairPnLInputError("Spot fee and tax rates must be below 100%")
    futures_qty = _pnl_decimal(futures_quantity_native, "futures_quantity_native")
    contract_size = _pnl_decimal(futures_contract_size, "futures_contract_size")
    futures_entry = _pnl_decimal(futures_entry_price, "futures_entry_price")
    futures_entry_fee = _pnl_decimal(futures_entry_fee_quote, "futures_entry_fee_quote", allow_zero=True)
    futures_exit_rate = _pnl_decimal(futures_exit_fee_rate, "futures_exit_fee_rate", allow_zero=True)
    if futures_exit_rate >= 1:
        raise PairPnLInputError("Futures exit fee rate must be below 100%")
    funding = _pnl_signed_decimal(funding_reporting_amount, "funding_reporting_amount")
    conversion_cost = _pnl_decimal(conversion_cost_reporting, "conversion_cost_reporting", allow_zero=True)
    reserve = _pnl_decimal(fee_reserve_reporting, "fee_reserve_reporting", allow_zero=True)

    spot_vwap, spot_exit_notional, spot_slippage = _sweep_pnl_depth(
        spot_bid_levels, spot_qty, side="SELL",
    )
    futures_vwap, futures_exit_notional, futures_slippage = _sweep_pnl_depth(
        futures_ask_levels, futures_qty, side="BUY", contract_size=contract_size,
    )
    futures_base_qty = futures_qty * contract_size

    spot_exit_fee_quote = spot_exit_notional * spot_exit_rate
    spot_tax_quote = spot_exit_notional * tax_rate
    futures_exit_fee_quote = futures_exit_notional * futures_exit_rate

    spot_entry_fee_reporting = spot_entry_fee * spot_fx.quote_to_reporting_rate
    spot_exit_fee_reporting = spot_exit_fee_quote * spot_fx.quote_to_reporting_rate
    spot_tax_reporting = spot_tax_quote * spot_fx.quote_to_reporting_rate
    spot_pnl = (
        spot_exit_notional - spot_exit_fee_quote - spot_tax_quote
        - spot_cost - spot_entry_fee
    ) * spot_fx.quote_to_reporting_rate

    futures_entry_fee_reporting = futures_entry_fee * futures_fx.quote_to_reporting_rate
    futures_exit_fee_reporting = futures_exit_fee_quote * futures_fx.quote_to_reporting_rate
    futures_pnl = (
        (futures_entry - futures_vwap) * futures_base_qty
        - futures_entry_fee - futures_exit_fee_quote
    ) * futures_fx.quote_to_reporting_rate

    entry_fees = spot_entry_fee_reporting + futures_entry_fee_reporting
    exit_fees = spot_exit_fee_reporting + futures_exit_fee_reporting
    total_net = (
        spot_pnl + futures_pnl + funding
        - conversion_cost
    )
    spot_notional_reporting = spot_cost * spot_fx.quote_to_reporting_rate
    strategy_roi = (
        total_net / spot_notional_reporting * Decimal("100")
        if spot_notional_reporting > 0 else Decimal("0")
    )

    deployed_capital = None
    deployed_roi = None
    if actual_futures_margin_reporting is not None:
        margin = _pnl_decimal(
            actual_futures_margin_reporting, "actual_futures_margin_reporting",
            allow_zero=True,
        )
        deployed_capital = spot_notional_reporting + margin + reserve
        if deployed_capital > 0:
            deployed_roi = total_net / deployed_capital * Decimal("100")

    return PairPnLSnapshotV3(
        session_id=str(session_id), pair_key=str(pair_key),
        reporting_currency=str(reporting_currency or "").upper(),
        evaluated_at_ms=now_ms,
        spot_estimated_liquidation_pnl=spot_pnl,
        futures_estimated_liquidation_pnl=futures_pnl,
        funding_reporting_amount=funding,
        entry_fees_reporting=entry_fees,
        estimated_exit_fees_reporting=exit_fees,
        estimated_tax_reporting=spot_tax_reporting,
        conversion_cost_reporting=conversion_cost,
        estimated_net_liquidation_pnl=total_net,
        strategy_roi_pct=strategy_roi,
        return_on_deployed_capital_pct=deployed_roi,
        total_deployed_capital_reporting=deployed_capital,
        spot_exit_vwap=spot_vwap, futures_exit_vwap=futures_vwap,
        spot_slippage_bps=spot_slippage, futures_slippage_bps=futures_slippage,
        spot_executable_base_quantity=spot_qty,
        futures_executable_base_quantity=futures_base_qty,
        hedge_drift_base_quantity=abs(spot_qty - futures_base_qty),
        confidence="VERIFIED" if actual_futures_margin_reporting is not None else "PARTIAL",
        provenance={
            "spot_entry_fee_source": spot_entry_fee_source,
            "futures_entry_fee_source": futures_entry_fee_source,
            "exit_fee_policy_source": exit_fee_policy_source,
            "capital_basis_complete": actual_futures_margin_reporting is not None,
            "spot_book_sequence": spot_book_sequence,
            "spot_book_observed_at_ms": spot_book_observed_at_ms,
            "futures_book_sequence": futures_book_sequence,
            "futures_book_observed_at_ms": futures_book_observed_at_ms,
            "spot_fx_source": spot_fx.source,
            "spot_fx_observed_at_ms": spot_fx.observed_at_ms,
            "futures_fx_source": futures_fx.source,
            "futures_fx_observed_at_ms": futures_fx.observed_at_ms,
            "funding_source": funding_source,
            "funding_verified": funding_verified,
        },
    )
