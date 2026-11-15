"""Opportunity Evaluation, Hard Filters, and Lexicographic Ranker (Executor V2).

Implements Section 3 of EXECUTOR-ARCHITECTURE-V2.md:
- Hard filters: book freshness, crossed book, minimum notional, risk gates
- Net profit estimation: fees, slippage, and funding deduction
- Lexicographic scoring and deterministic priority queue ordering
- Final coordinator revalidation before admission
"""
from __future__ import annotations

import heapq
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("executor.opportunity_ranker")


@dataclass(order=True)
class OpportunityCandidate:
    # Priority queue ordering: highest score first -> inverted score
    sort_key: Tuple[float, float, int, str] = field(init=False, repr=False)

    opportunity_id: str = field(compare=False)
    pair_key: str = field(compare=False)
    local_venue: str = field(compare=False)
    local_symbol: str = field(compare=False)
    global_venue: str = field(compare=False)
    global_symbol: str = field(compare=False)
    local_bid: float = field(compare=False)
    local_ask: float = field(compare=False)
    global_bid: float = field(compare=False)
    global_ask: float = field(compare=False)
    gross_spread_pct: float = field(compare=False)
    expected_net_profit_usdt: float = field(compare=False)
    expected_net_roi_pct: float = field(compare=False)
    allocated_capital_usdt: float = field(compare=False)
    quote_age_ms: float = field(compare=False)
    timestamp_ms: int = field(compare=False)
    expiry_ms: int = field(compare=False)
    ranking_score: float = field(default=0.0, compare=False)
    metadata: Dict[str, Any] = field(default_factory=dict, compare=False)
    p_entry: Optional[float] = field(default=None, compare=False)
    p_exit: Optional[float] = field(default=None, compare=False)
    estimated_slippage_and_fee_risk_usdt: Optional[float] = field(default=None, compare=False)
    estimated_hold_hours: Optional[float] = field(default=None, compare=False)
    trade_velocity_usd_per_min: Optional[float] = field(default=None, compare=False)
    mean_reversion_frequency_per_hour: Optional[float] = field(default=None, compare=False)

    def __post_init__(self):
        # Tie-breaker: (inverted score, inverted net profit, timestamp, opportunity_id)
        self.sort_key = (
            -self.ranking_score,
            -self.expected_net_profit_usdt,
            self.timestamp_ms,
            self.opportunity_id,
        )


class OpportunityRanker:
    """Evaluates candidates against hard filters and sorts by net economic value."""

    def __init__(
        self,
        *,
        max_quote_age_ms: float = 3000.0,
        min_net_profit_usdt: float = 0.20,
        min_net_roi_pct: float = 0.15,
        max_entry_slippage_pct: float = 0.005,
    ):
        self.max_quote_age_ms = max_quote_age_ms
        self.min_net_profit_usdt = min_net_profit_usdt
        self.min_net_roi_pct = min_net_roi_pct
        self.max_entry_slippage_pct = max_entry_slippage_pct

    def check_hard_filters(self, candidate: OpportunityCandidate, risk_engine: Any = None) -> Tuple[bool, str]:
        """Apply non-negotiable validity gates before scoring."""
        now_ms = int(time.time() * 1000)

        # 1. Expiry and freshness
        if now_ms >= candidate.expiry_ms:
            return False, "CANDIDATE_EXPIRED"
        if candidate.quote_age_ms > self.max_quote_age_ms:
            return False, f"STALE_QUOTE ({candidate.quote_age_ms:.0f}ms > {self.max_quote_age_ms:.0f}ms)"

        # 2. Book integrity (no crossed books)
        if candidate.local_bid >= candidate.local_ask:
            return False, "LOCAL_BOOK_CROSSED"
        if candidate.global_bid >= candidate.global_ask:
            return False, "GLOBAL_BOOK_CROSSED"

        # 3. Minimum economics
        if candidate.expected_net_profit_usdt < self.min_net_profit_usdt:
            return False, f"BELOW_MIN_NET_PROFIT ({candidate.expected_net_profit_usdt:.3f} < {self.min_net_profit_usdt:.3f})"
        if candidate.expected_net_roi_pct < self.min_net_roi_pct:
            return False, f"BELOW_MIN_NET_ROI ({candidate.expected_net_roi_pct:.2f}% < {self.min_net_roi_pct:.2f}%)"

        # 4. Risk engine circuit check if provided
        if risk_engine is not None:
            if getattr(risk_engine, "_trading_halted", False):
                return False, f"TRADING_HALTED: {getattr(risk_engine, '_halt_reason', '')}"
            if candidate.local_venue in getattr(risk_engine, "_exchange_halted", {}):
                return False, f"VENUE_HALTED: {candidate.local_venue}"
            if candidate.global_venue in getattr(risk_engine, "_exchange_halted", {}):
                return False, f"VENUE_HALTED: {candidate.global_venue}"
            if candidate.pair_key in getattr(risk_engine, "_pair_halted", {}):
                return False, f"PAIR_HALTED: {candidate.pair_key}"

        return True, "PASSED"

    def compute_score(self, candidate: OpportunityCandidate) -> float:
        """Calculate ranking score based on Expected Realized Profit per Capital-Hour."""
        # 1. P_entry (Probability of successful entry fill)
        p_entry = candidate.p_entry
        if p_entry is None and "p_entry" in candidate.metadata:
            p_entry = float(candidate.metadata["p_entry"])
        if p_entry is None:
            vel_score = float(candidate.metadata.get("trade_velocity_score", 1.0))
            p_entry = min(0.98, max(0.2, 0.5 + 0.3 * min(1.5, vel_score)))
        p_entry = max(0.01, min(0.99, p_entry))

        # 2. P_exit (Probability of successful exit / high exitability)
        p_exit = candidate.p_exit
        if p_exit is None and "p_exit" in candidate.metadata:
            p_exit = float(candidate.metadata["p_exit"])
        if p_exit is None and "exitability_score" in candidate.metadata:
            p_exit = float(candidate.metadata["exitability_score"])
        if p_exit is None:
            p_exit = 0.85
        p_exit = max(0.01, min(0.99, p_exit))

        # 3. Estimated Slippage & Fee Risk (USD)
        risk_usdt = candidate.estimated_slippage_and_fee_risk_usdt
        if risk_usdt is None and "estimated_slippage_and_fee_risk_usdt" in candidate.metadata:
            risk_usdt = float(candidate.metadata["estimated_slippage_and_fee_risk_usdt"])
        if risk_usdt is None:
            fees = float(candidate.metadata.get("estimated_fees_usdt", candidate.allocated_capital_usdt * 0.002))
            slippage_risk = candidate.allocated_capital_usdt * (self.max_entry_slippage_pct * 2.0)
            risk_usdt = fees + slippage_risk
        risk_usdt = max(0.05, risk_usdt)

        # 4. Expected Realized Profit (USD)
        p_joint = p_entry * p_exit
        expected_net_profit = candidate.expected_net_profit_usdt
        expected_realized_profit = (p_joint * expected_net_profit) - ((1.0 - p_joint) * risk_usdt)

        # 5. Estimated Hold Hours (ratio of order size to TradeVelocityUSDPerMin + spread mean-reversion frequency)
        hold_hours = candidate.estimated_hold_hours
        if hold_hours is None and "estimated_hold_hours" in candidate.metadata:
            hold_hours = float(candidate.metadata["estimated_hold_hours"])
        if hold_hours is None:
            vel_usd_per_min = candidate.trade_velocity_usd_per_min
            if vel_usd_per_min is None and "trade_velocity_usd_per_min" in candidate.metadata:
                vel_usd_per_min = float(candidate.metadata["trade_velocity_usd_per_min"])
            if vel_usd_per_min is None and "spot_turnover_5m_usd" in candidate.metadata:
                vel_usd_per_min = float(candidate.metadata["spot_turnover_5m_usd"]) / 5.0
            if vel_usd_per_min is None and "trade_velocity_1m_usd" in candidate.metadata:
                vel_usd_per_min = float(candidate.metadata["trade_velocity_1m_usd"])
            if vel_usd_per_min is None or vel_usd_per_min <= 0:
                vel_usd_per_min = max(1.0, candidate.allocated_capital_usdt / 10.0)

            order_size = max(1.0, candidate.allocated_capital_usdt)
            fill_hours = (order_size / max(0.1, vel_usd_per_min)) / 60.0

            reversion_freq = candidate.mean_reversion_frequency_per_hour
            if reversion_freq is None and "mean_reversion_frequency_per_hour" in candidate.metadata:
                reversion_freq = float(candidate.metadata["mean_reversion_frequency_per_hour"])
            if reversion_freq is None and "spread_volatility_factor" in candidate.metadata:
                vol_factor = float(candidate.metadata["spread_volatility_factor"])
                reversion_freq = max(0.2, vol_factor * 1.5)
            if reversion_freq is None or reversion_freq <= 0:
                reversion_freq = 1.0

            reversion_cycle_hours = 1.0 / max(0.1, reversion_freq)
            hold_hours = fill_hours + reversion_cycle_hours

        hold_hours = max(0.05, min(72.0, hold_hours))

        # 6. Capital-Hour Score
        total_capital = max(1.0, candidate.allocated_capital_usdt)
        capital_hour_score = expected_realized_profit / (total_capital * hold_hours)

        # 7. Quote staleness discount
        latency_discount = max(0.5, 1.0 - (candidate.quote_age_ms / self.max_quote_age_ms) * 0.2)
        # ponytail: proportional latency discount on capital-hour score; upgrade to exponential decay if staleness non-linear.
        final_score = max(0.0, capital_hour_score * latency_discount)

        candidate.metadata["p_entry"] = p_entry
        candidate.metadata["p_exit"] = p_exit
        candidate.metadata["estimated_risk_usdt"] = risk_usdt
        candidate.metadata["expected_realized_profit_usdt"] = expected_realized_profit
        candidate.metadata["estimated_hold_hours"] = hold_hours
        candidate.metadata["capital_hour_score"] = capital_hour_score

        candidate.ranking_score = round(final_score, 6)
        candidate.__post_init__()
        return candidate.ranking_score

    def rank_candidates(
        self, candidates: List[OpportunityCandidate], risk_engine: Any = None
    ) -> List[OpportunityCandidate]:
        """Filter, score, and return sorted candidates (highest value first)."""
        valid_heap = []
        for c in candidates:
            passed, reason = self.check_hard_filters(c, risk_engine)
            if not passed:
                logger.debug("Rejected candidate %s (%s): %s", c.opportunity_id, c.pair_key, reason)
                continue
            self.compute_score(c)
            heapq.heappush(valid_heap, c)

        ranked = []
        while valid_heap:
            ranked.append(heapq.heappop(valid_heap))
        return ranked

    def revalidate_for_admission(
        self,
        candidate: OpportunityCandidate,
        current_local_bid: float,
        current_global_ask: float,
    ) -> Tuple[bool, str]:
        """Coordinator final admission check immediately before order intent generation."""
        now_ms = int(time.time() * 1000)
        if now_ms >= candidate.expiry_ms:
            return False, "CANDIDATE_EXPIRED_BEFORE_ADMISSION"

        # Check price slippage from snapshot
        if current_local_bid < candidate.local_bid * (1.0 - self.max_entry_slippage_pct):
            return False, "LOCAL_BID_SLIPPED"
        if current_global_ask > candidate.global_ask * (1.0 + self.max_entry_slippage_pct):
            return False, "GLOBAL_ASK_SLIPPED"

        return True, "ADMITTED"
