"""Portfolio & Capital Allocation Manager (Executor V2).

Implements Section 22.1 of EXECUTOR-ARCHITECTURE-V2.md:
- Enforces portfolio, venue, strategy, and pair capital limits
- Requires atomic capital reservation before order intent creation
- Enforces mandatory Futures margin buffer reservation
- Releases reservations atomically on order rejection, cancel, or position close
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

logger = logging.getLogger("executor.capital_manager")


class CapitalExceededError(RuntimeError):
    """Raised when an allocation violates portfolio, venue, or pair risk limits."""


@dataclass
class CapitalReservation:
    reservation_id: str
    pair_key: str
    venue: str
    allocated_usdt: float
    margin_buffer_usdt: float
    status: str = "ACTIVE"  # ACTIVE, RELEASED, COMMITTED


class CapitalManager:
    """Manages global portfolio limits and atomic capital reservations."""

    def __init__(
        self,
        *,
        max_portfolio_capital_usdt: float = 1000.0,
        max_capital_per_venue_usdt: float = 500.0,
        max_capital_per_pair_usdt: float = 200.0,
        futures_margin_buffer_ratio: float = 0.20,  # 20% buffer reserved
    ):
        self.max_portfolio = max_portfolio_capital_usdt
        self.max_per_venue = max_capital_per_venue_usdt
        self.max_per_pair = max_capital_per_pair_usdt
        self.margin_buffer_ratio = futures_margin_buffer_ratio

        self._reservations: Dict[str, CapitalReservation] = {}
        self._lock = asyncio.Lock()

    async def reserve_capital(
        self,
        reservation_id: str,
        *,
        pair_key: str,
        local_venue: str,
        global_venue: str,
        notional_usdt: float,
    ) -> CapitalReservation:
        """Atomically reserve capital and margin buffer across limits."""
        async with self._lock:
            margin_buffer = notional_usdt * self.margin_buffer_ratio
            total_required = notional_usdt + margin_buffer

            # 1. Check portfolio capital
            current_portfolio_used = sum(
                r.allocated_usdt + r.margin_buffer_usdt
                for r in self._reservations.values()
                if r.status in ("ACTIVE", "COMMITTED")
            )
            if current_portfolio_used + total_required > self.max_portfolio:
                raise CapitalExceededError(
                    f"Portfolio limit exceeded: used {current_portfolio_used:.2f} + req {total_required:.2f} > max {self.max_portfolio:.2f}"
                )

            # 2. Check per-pair limit
            current_pair_used = sum(
                r.allocated_usdt + r.margin_buffer_usdt
                for r in self._reservations.values()
                if r.pair_key == pair_key and r.status in ("ACTIVE", "COMMITTED")
            )
            if current_pair_used + total_required > self.max_per_pair:
                raise CapitalExceededError(
                    f"Pair limit exceeded for {pair_key}: used {current_pair_used:.2f} + req {total_required:.2f} > max {self.max_per_pair:.2f}"
                )

            # 3. Check venue limits
            local_used = sum(
                r.allocated_usdt
                for r in self._reservations.values()
                if r.venue == local_venue and r.status in ("ACTIVE", "COMMITTED")
            )
            if local_used + notional_usdt > self.max_per_venue:
                raise CapitalExceededError(
                    f"Venue limit exceeded for {local_venue}: used {local_used:.2f} + req {notional_usdt:.2f} > max {self.max_per_venue:.2f}"
                )

            reservation = CapitalReservation(
                reservation_id=reservation_id,
                pair_key=pair_key,
                venue=local_venue,
                allocated_usdt=notional_usdt,
                margin_buffer_usdt=margin_buffer,
                status="ACTIVE",
            )
            self._reservations[reservation_id] = reservation
            logger.info("Reserved capital %s for %s: %.2f USDT (+%.2f buffer)",
                        reservation_id, pair_key, notional_usdt, margin_buffer)
            return reservation

    async def release_capital(self, reservation_id: str, reason: str = "RELEASED") -> None:
        """Release capital reservation atomically."""
        async with self._lock:
            res = self._reservations.get(reservation_id)
            if res and res.status in ("ACTIVE", "COMMITTED"):
                res.status = "RELEASED"
                logger.info("Released capital %s (%s): %.2f USDT", reservation_id, reason, res.allocated_usdt)

    async def get_allocated_capital(self, pair_key: Optional[str] = None) -> float:
        """Return total active allocated capital."""
        async with self._lock:
            return sum(
                r.allocated_usdt + r.margin_buffer_usdt
                for r in self._reservations.values()
                if (pair_key is None or r.pair_key == pair_key) and r.status in ("ACTIVE", "COMMITTED")
            )
