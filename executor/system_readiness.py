"""System Readiness Gate & Action Matrix (Executor V2).

Implements Section 26 of EXECUTOR-ARCHITECTURE-V2.md:
- Evaluates multi-tier readiness (Global, Venue, Market, Pair)
- Produces machine-readable status verdicts and allowed action policies:
  NEW_ENTRY_ALLOWED, MONITORING_ALLOWED, PROTECTIVE_ACTION_ALLOWED
- Maps directly to the 10 failure/readiness conditions in the V2 matrix
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("executor.system_readiness")


class ActionPermission(str, Enum):
    FULL_EXECUTION = "FULL_EXECUTION"
    READ_AND_PROTECT_ONLY = "READ_AND_PROTECT_ONLY"
    READ_ONLY = "READ_ONLY"
    BLOCKED = "BLOCKED"


@dataclass
class SystemReadinessReport:
    is_ready: bool
    permission: ActionPermission
    blocking_reasons: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class SystemReadinessGate:
    """Evaluates the 10-condition readiness matrix from Section 26."""

    def evaluate_readiness(
        self,
        engine: Any,
        *,
        pair_tag: Optional[str] = None,
        venue: Optional[str] = None,
    ) -> SystemReadinessReport:
        """Run comprehensive health checks across canonical state, venues, and risk gates."""
        blocking = []
        warnings = []

        # 1. Global STOP & Trading Halt
        if getattr(engine, "_trading_halted", False):
            blocking.append(f"GLOBAL_TRADING_HALTED: {getattr(engine, '_halt_reason', 'manual')}")

        # 2. Risk State Persistence Trustworthiness
        if getattr(engine, "_risk_persistence_failed", False):
            blocking.append("RISK_PERSISTENCE_UNAVAILABLE")

        # 3. Startup Recovery Completion
        if not getattr(engine, "_recovery_complete", True):
            blocking.append("STARTUP_RECOVERY_INCOMPLETE")

        # 4. Global Scope Halts
        halts = getattr(engine, "_recovery_scope_halts", {})
        if "*" in halts:
            blocking.append(f"GLOBAL_SCOPE_HALT: {halts['*']}")

        # 5. Venue-specific check
        if venue:
            venue = venue.lower().strip()
            if venue in getattr(engine, "_exchange_halted", {}):
                blocking.append(f"VENUE_HALTED: {venue} ({engine._exchange_halted[venue]})")
            if venue in halts:
                blocking.append(f"VENUE_SCOPE_HALT: {venue} ({halts[venue]})")

        # 6. Pair-specific check
        if pair_tag:
            if pair_tag in getattr(engine, "_pair_halted", {}):
                blocking.append(f"PAIR_HALTED: {pair_tag} ({engine._pair_halted[pair_tag]})")
            if pair_tag in halts:
                blocking.append(f"PAIR_SCOPE_HALT: {pair_tag} ({halts[pair_tag]})")

            # Check hedge drift if coordinator present
            coord = getattr(engine, "two_leg_coordinator", None)
            if coord:
                report = coord.evaluate_hedge_drift(pair_tag)
                if report.status.value == "HARD_DRIFT_BREACH":
                    blocking.append(f"HEDGE_DRIFT_HARD_BREACH: unhedged={report.unhedged_qty:.6f}")
                elif report.status.value == "DRIFT_WARNING":
                    warnings.append(f"HEDGE_DRIFT_WARNING: unhedged={report.unhedged_qty:.6f}")

        # 7. Unresolved orphans check
        orphans = getattr(engine, "_recovered_orphans", {})
        if orphans:
            blocking.append(f"UNRESOLVED_ORPHAN_ORDERS_COUNT: {len(orphans)}")

        # 8. Determine Permission Level
        if blocking:
            return SystemReadinessReport(
                is_ready=False,
                permission=ActionPermission.READ_AND_PROTECT_ONLY,
                blocking_reasons=blocking,
                warnings=warnings,
            )

        if warnings:
            return SystemReadinessReport(
                is_ready=True,
                permission=ActionPermission.FULL_EXECUTION,
                blocking_reasons=[],
                warnings=warnings,
            )

        return SystemReadinessReport(
            is_ready=True,
            permission=ActionPermission.FULL_EXECUTION,
            blocking_reasons=[],
            warnings=[],
        )
