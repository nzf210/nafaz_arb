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
from typing import Optional

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
