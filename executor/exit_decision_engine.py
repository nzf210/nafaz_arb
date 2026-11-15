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
        stop_loss_pct: Optional[float] = None,
        min_hold_dwell_seconds: float = 5.0,
        max_position_age_seconds: float = 3600.0,
        target_exit_net_roi_pct: float = 0.50,
        decay_start_minutes: float = 15.0,
        decay_end_minutes: float = 45.0,
        decay_target_roi_pct: float = 0.20,
        breakeven_minutes: float = 90.0,
        breakeven_target_roi_pct: float = 0.02,
        max_hold_minutes: float = 90.0,
        hard_stop_hold_minutes: float = 180.0,
        max_recovery_loss_pct: float = 0.30,
        spread_decay_penalty_pct: float = 0.05,
    ):
        self.min_profit_target_usdt = min_profit_target_usdt
        self.stop_loss_usdt = stop_loss_usdt
        self.stop_loss_pct = stop_loss_pct
        self.min_hold_dwell = min_hold_dwell_seconds
        self.max_position_age = max_position_age_seconds
        self.target_exit_net_roi_pct = target_exit_net_roi_pct
        self.decay_start_minutes = decay_start_minutes
        self.decay_end_minutes = decay_end_minutes
        self.decay_target_roi_pct = decay_target_roi_pct
        self.breakeven_minutes = breakeven_minutes
        self.breakeven_target_roi_pct = breakeven_target_roi_pct
        self.max_hold_minutes = max_hold_minutes
        self.hard_stop_hold_minutes = hard_stop_hold_minutes
        self.max_recovery_loss_pct = abs(max_recovery_loss_pct)
        self.spread_decay_penalty_pct = spread_decay_penalty_pct

    def get_required_decay_roi(
        self,
        position_age_seconds: float,
        target_exit_net_roi_pct: Optional[float] = None,
        net_funding_rate: float = 0.0,
    ) -> float:
        """Computes phase-based required Net ROI %:
        1. Normal Phase (0 - 15m): full target (e.g. +0.50%)
        2. Time-Decay Phase (15 - 45m): linear decay towards +0.20%
        3. Breakeven Phase (45 - 90m): +0.02% (covers all 4 fees)
        4. Capital Recovery (> 90m / max_hold_minutes):
           - if negative funding OR age >= hard_stop_hold_minutes: -abs(max_recovery_loss_pct) (e.g. -0.30%)
           - else hold at breakeven (+0.02%)
        """
        age_min = max(0.0, float(position_age_seconds or 0.0)) / 60.0
        target_roi = float(
            target_exit_net_roi_pct
            if target_exit_net_roi_pct is not None
            else self.target_exit_net_roi_pct
        )
        if age_min < self.decay_start_minutes:
            return target_roi
        elif age_min <= self.decay_end_minutes:
            span = self.decay_end_minutes - self.decay_start_minutes
            alpha = (age_min - self.decay_start_minutes) / span if span > 0 else 1.0
            alpha = max(0.0, min(1.0, alpha))
            return (1.0 - alpha) * target_roi + alpha * self.decay_target_roi_pct
        elif age_min < self.max_hold_minutes:
            return self.breakeven_target_roi_pct
        else:
            # Phase 4: Capital Recovery (> 90m or > max_hold_minutes)
            if net_funding_rate < 0 or age_min >= self.hard_stop_hold_minutes:
                return -abs(self.max_recovery_loss_pct)
            return self.breakeven_target_roi_pct

    def calculate_fill_probability(
        self,
        spot_sell_turnover_30s_usd: float,
        queue_ahead_usd: float,
        order_notional_usd: float,
        min_p: float = 0.05,
        max_p: float = 0.95,
    ) -> float:
        """Fill Probability P_hat = clamp(SpotSellTurnover30sUSD / (QueueAheadUSD + OrderNotionalUSD), 0.05, 0.95)"""
        denom = max(0.0, float(queue_ahead_usd or 0.0)) + max(0.0, float(order_notional_usd or 0.0))
        if denom <= 0:
            return min_p
        turnover = max(0.0, float(spot_sell_turnover_30s_usd or 0.0))
        ratio = turnover / denom
        return max(min_p, min(max_p, ratio))

    def calculate_ev_maker(
        self,
        p_fill: float,
        net_pnl_maker: float,
        net_pnl_taker: float,
        spread_decay_penalty: float = 0.0,
    ) -> float:
        """Expected value of maker order factoring fill probability and spread decay penalty."""
        p = max(0.0, min(1.0, float(p_fill or 0.0)))
        taker_decay = float(net_pnl_taker) - max(0.0, float(spread_decay_penalty or 0.0))
        return p * float(net_pnl_maker) + (1.0 - p) * taker_decay

    def evaluate_maker_vs_taker(
        self,
        taker_roi_pct: float,
        net_pnl_taker: float,
        maker_roi_pct: float,
        net_pnl_maker: float,
        required_decay_roi: float,
        p_fill: float,
        spread_decay_penalty: float = 0.0,
    ) -> tuple[bool, float, float]:
        """Returns (should_taker_exit, ev_taker, ev_maker)."""
        _ = maker_roi_pct
        ev_taker = float(net_pnl_taker)
        ev_maker = self.calculate_ev_maker(p_fill, net_pnl_maker, net_pnl_taker, spread_decay_penalty)
        should_taker = (taker_roi_pct >= required_decay_roi) or (ev_taker > ev_maker)
        return should_taker, ev_taker, ev_maker

    def evaluate_exit(
        self,
        snapshot: PairPnLSnapshot,
        *,
        position_age_seconds: float,
        has_active_entry_orders: bool = False,
        unhedged_exposure: float = 0.0,
        dynamic_target_usdt: Optional[float] = None,
        is_emergency: bool = False,
        net_funding_rate: float = 0.0,
        target_exit_net_roi_pct: Optional[float] = None,
    ) -> ExitDecision:
        """Evaluate two-leg position exit based on combined economics, decay phases, and risk gates."""
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

        if snapshot.pair_net_liquidation_pnl <= -abs(self.stop_loss_usdt) or (
            self.stop_loss_pct is not None and snapshot.pair_net_roi_pct <= self.stop_loss_pct
        ):
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

        # 3. Phase-based Profit / Decay / Recovery Target
        age_min = max(0.0, float(position_age_seconds or 0.0)) / 60.0
        required_decay_roi = self.get_required_decay_roi(
            position_age_seconds,
            target_exit_net_roi_pct=target_exit_net_roi_pct,
            net_funding_rate=net_funding_rate,
        )

        # Phase 4: Capital Recovery (> 90m / max_hold_minutes)
        if age_min >= self.max_hold_minutes:
            is_recovery_eligible = (net_funding_rate < 0 or age_min >= self.hard_stop_hold_minutes)
            if is_recovery_eligible and snapshot.pair_net_roi_pct >= required_decay_roi:
                return ExitDecision(
                    action=ExitAction.CLOSE_BOTH,
                    reason=f"CAPITAL_RECOVERY_EXIT (age={age_min:.1f}m, roi={snapshot.pair_net_roi_pct:.3f}% >= {required_decay_roi:.3f}%, funding={net_funding_rate:.6f})",
                    pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                    pair_roi_pct=snapshot.pair_net_roi_pct,
                    deadline_ms=now_ms + 15000,
                )
            if snapshot.pair_net_roi_pct >= self.breakeven_target_roi_pct:
                return ExitDecision(
                    action=ExitAction.CLOSE_BOTH,
                    reason=f"BREAKEVEN_SCRATCH_EXIT (age={age_min:.1f}m, roi={snapshot.pair_net_roi_pct:.3f}% >= {self.breakeven_target_roi_pct:.3f}%)",
                    pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                    pair_roi_pct=snapshot.pair_net_roi_pct,
                    deadline_ms=now_ms + 15000,
                )

        # Phase 3: Breakeven / Scratch (45 - 90m)
        elif age_min >= self.decay_end_minutes:
            if snapshot.pair_net_roi_pct >= self.breakeven_target_roi_pct:
                return ExitDecision(
                    action=ExitAction.CLOSE_BOTH,
                    reason=f"BREAKEVEN_SCRATCH_EXIT (age={age_min:.1f}m, roi={snapshot.pair_net_roi_pct:.3f}% >= {self.breakeven_target_roi_pct:.3f}%)",
                    pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                    pair_roi_pct=snapshot.pair_net_roi_pct,
                    deadline_ms=now_ms + 15000,
                )

        # Phase 2: Time-Decay (15 - 45m)
        elif age_min >= self.decay_start_minutes:
            if snapshot.pair_net_roi_pct >= required_decay_roi or snapshot.pair_net_liquidation_pnl >= target:
                return ExitDecision(
                    action=ExitAction.CLOSE_BOTH,
                    reason=f"TIME_DECAY_PROFIT_REACHED (age={age_min:.1f}m, roi={snapshot.pair_net_roi_pct:.3f}% >= {required_decay_roi:.3f}%)",
                    pair_net_pnl=snapshot.pair_net_liquidation_pnl,
                    pair_roi_pct=snapshot.pair_net_roi_pct,
                    deadline_ms=now_ms + 15000,
                )

        # Phase 1: Normal (0 - 15m)
        else:
            if snapshot.pair_net_liquidation_pnl >= target or snapshot.pair_net_roi_pct >= required_decay_roi:
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
