"""Market Data Quality & Execution Readiness Gate (Executor V2).

Implements Section 23.1 and 23.2 of EXECUTOR-ARCHITECTURE-V2.md:
- Pre-admission market data quality validation: age, gap, crossed/locked book
- Execution cost & slippage prediction: executable VWAP calculation across book depth
- Emits structured readiness verdicts: READY, DEGRADED, REJECTED
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

logger = logging.getLogger("executor.market_data_gate")


class GateVerdict(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    REJECTED = "REJECTED"


@dataclass
class QualityCheckResult:
    verdict: GateVerdict
    reason: str
    quote_age_ms: float
    executable_vwap: float
    expected_slippage_pct: float
    is_depth_sufficient: bool


class MarketDataGate:
    """Validates orderbook quality and computes executable VWAP slippage."""

    def __init__(
        self,
        *,
        max_quote_age_ms: float = 3000.0,
        max_acceptable_slippage_pct: float = 0.005,  # 0.5% max
    ):
        self.max_quote_age_ms = max_quote_age_ms
        self.max_slippage = max_acceptable_slippage_pct

    def compute_executable_vwap(
        self, orders: List[List[float]], target_quantity: float
    ) -> Tuple[float, float, bool]:
        """Calculate volume-weighted average price across orderbook depth.

        orders: [[price, qty], ...] sorted best to worst
        Returns: (vwap, slippage_pct, is_sufficient)
        """
        if not orders or target_quantity <= 0:
            return 0.0, 0.0, False

        best_price = float(orders[0][0])
        accumulated_qty = 0.0
        accumulated_cost = 0.0

        for price, qty in orders:
            price = float(price)
            qty = float(qty)
            needed = target_quantity - accumulated_qty
            fill_qty = min(qty, needed)
            accumulated_qty += fill_qty
            accumulated_cost += fill_qty * price
            if accumulated_qty >= target_quantity - 1e-9:
                break

        if accumulated_qty < target_quantity - 1e-9:
            # Depth exhausted
            return 0.0, 1.0, False

        vwap = accumulated_cost / accumulated_qty
        slippage_pct = abs(vwap - best_price) / best_price if best_price > 0 else 0.0
        return vwap, slippage_pct, True

    def validate_book(
        self,
        *,
        bids: List[List[float]],
        asks: List[List[float]],
        book_timestamp_ms: int,
        target_quantity: float,
        side: str = "buy",
    ) -> QualityCheckResult:
        """Validate book health and calculate executable execution cost."""
        now_ms = int(time.time() * 1000)
        age_ms = max(0.0, float(now_ms - book_timestamp_ms))

        # 1. Age check
        if age_ms > self.max_quote_age_ms:
            return QualityCheckResult(
                verdict=GateVerdict.REJECTED,
                reason=f"BOOK_STALE ({age_ms:.0f}ms > {self.max_quote_age_ms:.0f}ms)",
                quote_age_ms=age_ms,
                executable_vwap=0.0,
                expected_slippage_pct=0.0,
                is_depth_sufficient=False,
            )

        # 2. Empty book check
        if not bids or not asks:
            return QualityCheckResult(
                verdict=GateVerdict.REJECTED,
                reason="EMPTY_ORDERBOOK",
                quote_age_ms=age_ms,
                executable_vwap=0.0,
                expected_slippage_pct=0.0,
                is_depth_sufficient=False,
            )

        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])

        # 3. Crossed book check
        if best_bid >= best_ask:
            return QualityCheckResult(
                verdict=GateVerdict.REJECTED,
                reason=f"CROSSED_BOOK (bid {best_bid} >= ask {best_ask})",
                quote_age_ms=age_ms,
                executable_vwap=0.0,
                expected_slippage_pct=0.0,
                is_depth_sufficient=False,
            )

        # 4. VWAP depth and slippage calculation
        book_side = asks if side.lower() == "buy" else bids
        vwap, slippage, sufficient = self.compute_executable_vwap(book_side, target_quantity)

        if not sufficient:
            return QualityCheckResult(
                verdict=GateVerdict.REJECTED,
                reason=f"INSUFFICIENT_DEPTH for target qty {target_quantity}",
                quote_age_ms=age_ms,
                executable_vwap=0.0,
                expected_slippage_pct=slippage,
                is_depth_sufficient=False,
            )

        if slippage > self.max_slippage:
            return QualityCheckResult(
                verdict=GateVerdict.DEGRADED,
                reason=f"EXCESSIVE_SLIPPAGE ({slippage * 100:.3f}% > {self.max_slippage * 100:.3f}%)",
                quote_age_ms=age_ms,
                executable_vwap=vwap,
                expected_slippage_pct=slippage,
                is_depth_sufficient=True,
            )

        return QualityCheckResult(
            verdict=GateVerdict.READY,
            reason="HEALTHY",
            quote_age_ms=age_ms,
            executable_vwap=vwap,
            expected_slippage_pct=slippage,
            is_depth_sufficient=True,
        )
