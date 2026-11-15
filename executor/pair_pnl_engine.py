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
    ) -> PairPnLSnapshot:
        """Calculate combined two-leg net liquidation PnL if closed immediately at orderbook depth."""
        # 1. Spot Economics
        # Gross proceeds if sold at current executable bid depth
        spot_gross_proceeds = spot_qty * spot_executable_bid
        spot_close_fee = spot_gross_proceeds * spot_exit_fee_rate
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
        pos_state = position_state if isinstance(position_state, dict) else {}
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

        # Capital basis: Spot entry cost + Futures initial margin (typically ~10% for 10x or 100% unleveraged)
        total_capital = (spot_entry_cost_quote * fx_spot_to_report) + (
            futures_entry_notional * fx_futures_to_report * 0.2
        )
        pair_roi_pct = (pair_net_pnl / total_capital * 100.0) if total_capital > 0 else 0.0

        spot_entry_fee_usd = spot_entry_fee_quote * fx_spot_to_report
        futures_entry_fee_usd = futures_entry_fee_native * fx_futures_to_report
        total_entry_fee = spot_entry_fee_usd + futures_entry_fee_usd

        spot_exit_fee_usd = spot_close_fee * fx_spot_to_report
        futures_exit_fee_usd = futures_close_fee * fx_futures_to_report
        total_exit_fee = spot_exit_fee_usd + futures_exit_fee_usd
        expected_close_fee = total_exit_fee
        total_fees = total_entry_fee + total_exit_fee

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
        )
