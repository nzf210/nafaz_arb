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
        """Calculate lexicographic ranking score based on ROI, profit, and risk penalties."""
        base_score = (candidate.expected_net_roi_pct * 0.7) + (
            (candidate.expected_net_profit_usdt / max(1.0, candidate.allocated_capital_usdt)) * 100.0 * 0.3
        )
        # Latency penalty: penalize older quotes
        latency_penalty = (candidate.quote_age_ms / self.max_quote_age_ms) * 0.05
        final_score = max(0.0, base_score - latency_penalty)
        candidate.ranking_score = round(final_score, 4)
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
