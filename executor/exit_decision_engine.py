"""Exit Decision Engine (Executor V2).

Evaluates whether to exit or hold two-leg arbitrage positions based on
combined net liquidation value, dynamic profit targets, fee decay,
and risk boundaries.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from executor.pair_pnl_engine import PairPnLSnapshot

logger = logging.getLogger("executor.exit_decision_engine")


class ExitAction(str, Enum):
    HOLD = "HOLD"
    CANCEL_REMAINING_ENTRY = "CANCEL_REMAINING_ENTRY"
    REDUCE_SPOT = "REDUCE_SPOT"
    CLOSE_FUTURES = "CLOSE_FUTURES"
    CLOSE_SPOT = "CLOSE_SPOT"
    CLOSE_BOTH = "CLOSE_BOTH"
    REPAIR_HEDGE = "REPAIR_HEDGE"
    EMERGENCY_UNWIND = "EMERGENCY_UNWIND"
    WAIT_FOR_RECONCILIATION = "WAIT_FOR_RECONCILIATION"


@dataclass
class ExitDecision:
    action: ExitAction
    reason: str
    pair_net_pnl: float
    pair_roi_pct: float
    confidence: float = 1.0
    deadline_ms: int = 0


class ExitDecisionEngine:
    """Central authority for determining two-leg position exits."""

    def __init__(
        self,
        *,
        min_profit_target_usdt: float = 0.50,
        stop_loss_usdt: float = 5.0,
        min_hold_dwell_seconds: float = 5.0,
        max_position_age_seconds: float = 3600.0,
    ):
        self.min_profit_target_usdt = min_profit_target_usdt
        self.stop_loss_usdt = stop_loss_usdt
        self.min_hold_dwell = min_hold_dwell_seconds
        self.max_position_age = max_position_age_seconds

    def evaluate_exit(
        self,
        snapshot: PairPnLSnapshot,
        *,
        position_age_seconds: float,
        has_active_entry_orders: bool = False,
        unhedged_exposure: float = 0.0,
        dynamic_target_usdt: Optional[float] = None,
        is_emergency: bool = False,
    ) -> ExitDecision:
        """Evaluate two-leg position exit based on combined economics and risk gates."""
        now_ms = int(time.time() * 1000)
        target = dynamic_target_usdt if dynamic_target_usdt is not None else self.min_profit_target_usdt

        # 1. Emergency or Hard Stop Loss
        if is_emergency:
            return ExitDecision(
                action=ExitAction.EMERGENCY_UNWIND,
                reason="EMERGENCY_CIRCUIT_OPEN",
                pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                pair_roi_pct=snapshot.pair_net_roi_pct,
                deadline_ms=now_ms + 5000,
            )

        if snapshot.pair_net_liquidation_pnl <= -abs(self.stop_loss_usdt):
            return ExitDecision(
                action=ExitAction.CLOSE_BOTH,
                reason=f"STOP_LOSS_TRIGGERED (loss={snapshot.pair_net_liquidation_pnl:.2f} <= -{self.stop_loss_usdt:.2f})",
                pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                pair_roi_pct=snapshot.pair_net_roi_pct,
                deadline_ms=now_ms + 10000,
            )

        # 2. Unhedged Exposure Guard
        if unhedged_exposure > 1e-6:
            return ExitDecision(
                action=ExitAction.REPAIR_HEDGE,
                reason=f"RESIDUAL_UNHEDGED_EXPOSURE ({unhedged_exposure:.6f})",
                pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                pair_roi_pct=snapshot.pair_net_roi_pct,
                deadline_ms=now_ms + 5000,
            )

        # 3. Dynamic Profit Target Reached
        if snapshot.pair_net_liquidation_pnl >= target:
            # Check minimum dwell time to prevent churn unless strong profit
            if position_age_seconds >= self.min_hold_dwell or snapshot.pair_net_liquidation_pnl >= target * 1.5:
                return ExitDecision(
                    action=ExitAction.CLOSE_BOTH,
                    reason=f"TARGET_PROFIT_REACHED (pnl={snapshot.pair_net_liquidation_pnl:.2f} >= target={target:.2f})",
                    pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                    pair_roi_pct=snapshot.pair_net_roi_pct,
                    deadline_ms=now_ms + 15000,
                )

        # 4. Max Time in Trade Exceeded
        if position_age_seconds >= self.max_position_age:
            if snapshot.pair_net_liquidation_pnl > 0:
                return ExitDecision(
                    action=ExitAction.CLOSE_BOTH,
                    reason=f"TIME_STOP_PROFITABLE_CLOSE (age={position_age_seconds:.0f}s >= max={self.max_position_age:.0f}s)",
                    pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                    pair_roi_pct=snapshot.pair_net_roi_pct,
                    deadline_ms=now_ms + 30000,
                )

        # 5. Default Hold
        return ExitDecision(
            action=ExitAction.HOLD,
            reason="CONTINUE_HOLDING",
            pair_net_pnl=snapshot.pair_net_liquidation_pnl,
            pair_roi_pct=snapshot.pair_net_roi_pct,
            deadline_ms=0,
        )
