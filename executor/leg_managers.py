"""Independent Spot and Futures Leg Managers + Two-Leg Coordinator (Executor V2).

Implements:
- SpotLegManager: independent Spot lifecycle, fill tracking, cost-basis accounting
- FuturesLegManager: independent Futures lifecycle, contract size conversion, reduceOnly
- TwoLegCoordinator: delta controller, hedge obligation tracking, hedge drift monitor
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger("executor.leg_managers")


class LegStatus(str, Enum):
    CREATED = "CREATED"
    ENTRY_WORKING = "ENTRY_WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNHEDGED = "UNHEDGED"
    DEGRADED = "DEGRADED"


class HedgeDriftStatus(str, Enum):
    BALANCED = "BALANCED"
    DRIFT_WARNING = "DRIFT_WARNING"
    HARD_DRIFT_BREACH = "HARD_DRIFT_BREACH"
    UNKNOWN = "UNKNOWN"


@dataclass
class SpotLeg:
    leg_id: str
    session_id: str
    venue: str
    symbol: str
    target_qty: float
    filled_qty: float = 0.0
    cost_basis: float = 0.0
    verified_exposure: float = 0.0
    status: LegStatus = LegStatus.CREATED
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    last_update_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class FuturesLeg:
    leg_id: str
    session_id: str
    venue: str
    symbol: str
    contract_size: float = 1.0
    target_qty: float = 0.0
    filled_qty: float = 0.0
    verified_exposure: float = 0.0
    average_entry_price: float = 0.0
    status: LegStatus = LegStatus.CREATED
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    last_update_ms: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class HedgeDriftReport:
    session_id: str
    spot_exposure: float
    futures_exposure: float
    unhedged_qty: float
    drift_ratio: float
    status: HedgeDriftStatus
    exposure_age_seconds: float
    is_safe_for_new_exposure: bool


class SpotLegManager:
    """Manages independent Spot leg lifecycle, fills, and verified exposure."""

    def __init__(self):
        self.legs: Dict[str, SpotLeg] = {}

    def create_leg(self, session_id: str, venue: str, symbol: str, target_qty: float) -> SpotLeg:
        leg_id = f"spot-{session_id}"
        leg = SpotLeg(leg_id=leg_id, session_id=session_id, venue=venue, symbol=symbol, target_qty=target_qty)
        self.legs[leg_id] = leg
        return leg

    def record_fill(self, leg_id: str, filled_qty: float, price: float) -> None:
        leg = self.legs.get(leg_id)
        if not leg:
            return
        leg.filled_qty += filled_qty
        leg.cost_basis += filled_qty * price
        leg.verified_exposure += filled_qty
        leg.last_update_ms = int(time.time() * 1000)
        if leg.filled_qty >= leg.target_qty - 1e-8:
            leg.status = LegStatus.FILLED
        else:
            leg.status = LegStatus.PARTIALLY_FILLED


class FuturesLegManager:
    """Manages independent Futures hedge leg lifecycle, contract multipliers, and position."""

    def __init__(self):
        self.legs: Dict[str, FuturesLeg] = {}

    def create_leg(
        self, session_id: str, venue: str, symbol: str, contract_size: float = 1.0
    ) -> FuturesLeg:
        leg_id = f"futures-{session_id}"
        leg = FuturesLeg(
            leg_id=leg_id, session_id=session_id, venue=venue, symbol=symbol, contract_size=contract_size
        )
        self.legs[leg_id] = leg
        return leg

    def record_fill(self, leg_id: str, filled_contracts: float, price: float) -> None:
        leg = self.legs.get(leg_id)
        if not leg:
            return
        leg.filled_qty += filled_contracts
        equivalent_base_qty = filled_contracts * leg.contract_size
        total_qty = leg.verified_exposure + equivalent_base_qty
        if total_qty > 0:
            leg.average_entry_price = (
                (leg.verified_exposure * leg.average_entry_price) + (equivalent_base_qty * price)
            ) / total_qty
        leg.verified_exposure += equivalent_base_qty
        leg.last_update_ms = int(time.time() * 1000)
        if leg.target_qty > 0 and leg.filled_qty >= leg.target_qty - 1e-8:
            leg.status = LegStatus.FILLED
        else:
            leg.status = LegStatus.PARTIALLY_FILLED


class TwoLegCoordinator:
    """Coordinates Spot and Futures lifecycles, hedge obligations, and drift bounds."""

    def __init__(
        self,
        spot_manager: Optional[SpotLegManager] = None,
        futures_manager: Optional[FuturesLegManager] = None,
        *,
        max_hedge_latency_seconds: float = 5.0,
        warning_drift_ratio: float = 0.05,
        hard_drift_ratio: float = 0.15,
    ):
        self.spot_manager = spot_manager or SpotLegManager()
        self.futures_manager = futures_manager or FuturesLegManager()
        self.max_hedge_latency = max_hedge_latency_seconds
        self.warning_drift = warning_drift_ratio
        self.hard_drift = hard_drift_ratio

    def calculate_unhedged_delta(self, session_id: str) -> float:
        """Calculate unhedged base asset quantity: Spot exposure - Futures exposure."""
        spot_leg = self.spot_manager.legs.get(f"spot-{session_id}")
        futures_leg = self.futures_manager.legs.get(f"futures-{session_id}")

        spot_exp = spot_leg.verified_exposure if spot_leg else 0.0
        futures_exp = futures_leg.verified_exposure if futures_leg else 0.0

        return max(0.0, spot_exp - futures_exp)

    def evaluate_hedge_drift(self, session_id: str) -> HedgeDriftReport:
        """Evaluate hedge coverage ratio and drift status for the two-leg session."""
        spot_leg = self.spot_manager.legs.get(f"spot-{session_id}")
        futures_leg = self.futures_manager.legs.get(f"futures-{session_id}")

        if not spot_leg:
            return HedgeDriftReport(
                session_id=session_id,
                spot_exposure=0.0,
                futures_exposure=0.0,
                unhedged_qty=0.0,
                drift_ratio=0.0,
                status=HedgeDriftStatus.BALANCED,
                exposure_age_seconds=0.0,
                is_safe_for_new_exposure=True,
            )

        spot_exp = spot_leg.verified_exposure
        futures_exp = futures_leg.verified_exposure if futures_leg else 0.0
        unhedged = max(0.0, spot_exp - futures_exp)

        age_seconds = (time.time() * 1000 - spot_leg.last_update_ms) / 1000.0

        if spot_exp <= 1e-8:
            return HedgeDriftReport(
                session_id=session_id,
                spot_exposure=spot_exp,
                futures_exposure=futures_exp,
                unhedged_qty=0.0,
                drift_ratio=0.0,
                status=HedgeDriftStatus.BALANCED,
                exposure_age_seconds=age_seconds,
                is_safe_for_new_exposure=True,
            )

        drift_ratio = unhedged / spot_exp

        if drift_ratio >= self.hard_drift or (unhedged > 1e-6 and age_seconds > self.max_hedge_latency):
            status = HedgeDriftStatus.HARD_DRIFT_BREACH
            safe = False
        elif drift_ratio >= self.warning_drift:
            status = HedgeDriftStatus.DRIFT_WARNING
            safe = True
        else:
            status = HedgeDriftStatus.BALANCED
            safe = True

        return HedgeDriftReport(
            session_id=session_id,
            spot_exposure=spot_exp,
            futures_exposure=futures_exp,
            unhedged_qty=unhedged,
            drift_ratio=drift_ratio,
            status=status,
            exposure_age_seconds=age_seconds,
            is_safe_for_new_exposure=safe,
        )
