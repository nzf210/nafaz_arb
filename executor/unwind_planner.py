"""Adaptive Exit & Unwind Risk Planner (Executor V2).

Implements Section 23.5 of EXECUTOR-ARCHITECTURE-V2.md:
- Analyzes orderbook liquidity and exchange capabilities to plan multi-leg position unwind
- Selects optimal leg closure sequence:
  CONCURRENT_MARKET, SPOT_FIRST_THEN_FUTURES, FUTURES_FIRST_THEN_SPOT
- Evaluates reduceOnly constraints, liquidation risk, and slippage impact
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logger = logging.getLogger("executor.unwind_planner")


class UnwindSequence(str, Enum):
    CONCURRENT_MARKET = "CONCURRENT_MARKET"
    SPOT_FIRST_THEN_FUTURES = "SPOT_FIRST_THEN_FUTURES"
    FUTURES_FIRST_THEN_SPOT = "FUTURES_FIRST_THEN_SPOT"


@dataclass
class UnwindPlan:
    sequence: UnwindSequence
    spot_order_type: str
    futures_order_type: str
    futures_reduce_only: bool
    reason: str
    estimated_total_slippage_pct: float
    max_execution_window_seconds: float


class UnwindPlanner:
    """Plans safe position unwinding sequence based on liquidity and capabilities."""

    def __init__(
        self,
        *,
        thin_liquidity_slippage_threshold: float = 0.003,
        emergency_timeout_seconds: float = 5.0,
        normal_timeout_seconds: float = 30.0,
    ):
        self.thin_threshold = thin_liquidity_slippage_threshold
        self.emergency_timeout = emergency_timeout_seconds
        self.normal_timeout = normal_timeout_seconds

    def plan_unwind(
        self,
        *,
        pair_key: str,
        spot_slippage_pct: float,
        futures_slippage_pct: float,
        futures_supports_reduce_only: bool = True,
        is_emergency: bool = False,
    ) -> UnwindPlan:
        """Formulate execution sequence to unwind both legs with minimal directional risk."""
        # 1. Emergency unwind -> immediate concurrent close
        if is_emergency:
            return UnwindPlan(
                sequence=UnwindSequence.CONCURRENT_MARKET,
                spot_order_type="market",
                futures_order_type="market",
                futures_reduce_only=futures_supports_reduce_only,
                reason="EMERGENCY_IMMEDIATE_CONCURRENT_CLOSE",
                estimated_total_slippage_pct=spot_slippage_pct + futures_slippage_pct,
                max_execution_window_seconds=self.emergency_timeout,
            )

        # 2. Thin Spot liquidity: Spot limit sell first to avoid naked directional futures exposure
        if spot_slippage_pct > self.thin_threshold and spot_slippage_pct > futures_slippage_pct * 2:
            return UnwindPlan(
                sequence=UnwindSequence.SPOT_FIRST_THEN_FUTURES,
                spot_order_type="limit",
                futures_order_type="market",
                futures_reduce_only=futures_supports_reduce_only,
                reason="SPOT_LIQUIDITY_THIN_EXECUTE_SPOT_FIRST",
                estimated_total_slippage_pct=futures_slippage_pct,
                max_execution_window_seconds=self.normal_timeout,
            )

        # 3. High Futures volatility or high margin risk: close Futures short first
        if futures_slippage_pct > self.thin_threshold * 1.5:
            return UnwindPlan(
                sequence=UnwindSequence.FUTURES_FIRST_THEN_SPOT,
                spot_order_type="market",
                futures_order_type="market",
                futures_reduce_only=futures_supports_reduce_only,
                reason="FUTURES_VOLATILE_EXECUTE_FUTURES_FIRST",
                estimated_total_slippage_pct=spot_slippage_pct + futures_slippage_pct,
                max_execution_window_seconds=self.normal_timeout,
            )

        # 4. Standard balanced liquidity -> concurrent market close
        return UnwindPlan(
            sequence=UnwindSequence.CONCURRENT_MARKET,
            spot_order_type="market",
            futures_order_type="market",
            futures_reduce_only=futures_supports_reduce_only,
            reason="BALANCED_LIQUIDITY_CONCURRENT_CLOSE",
            estimated_total_slippage_pct=spot_slippage_pct + futures_slippage_pct,
            max_execution_window_seconds=self.normal_timeout,
        )
