"""Exchange Capability Profiles & Certification Registry (Executor V2).

Implements Section 22.4 of EXECUTOR-ARCHITECTURE-V2.md:
- Explicit capability declaration per venue, account, and market type
- States: UNVERIFIED, TESTING, VERIFIED, VERIFIED_WITH_LIMITS, DEGRADED, DISABLED
- Prevents live order execution on unverified capabilities
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Set

logger = logging.getLogger("executor.exchange_capabilities")


class CapabilityStatus(str, Enum):
    UNVERIFIED = "UNVERIFIED"
    TESTING = "TESTING"
    VERIFIED = "VERIFIED"
    VERIFIED_WITH_LIMITS = "VERIFIED_WITH_LIMITS"
    DEGRADED = "DEGRADED"
    DISABLED = "DISABLED"


@dataclass
class VenueCapabilityProfile:
    venue: str
    market_type: str
    status: CapabilityStatus = CapabilityStatus.UNVERIFIED
    supports_client_order_id: bool = True
    supports_private_ws: bool = False
    supports_reduce_only: bool = False
    supports_post_only: bool = True
    max_client_id_len: int = 32
    min_notional_usdt: float = 10.0
    supported_order_types: Set[str] = field(default_factory=lambda: {"limit", "market"})


class ExchangeCapabilityRegistry:
    """Registry maintaining certified exchange capabilities."""

    def __init__(self):
        self._profiles: Dict[str, VenueCapabilityProfile] = {}
        self._seed_baseline_profiles()

    def _seed_baseline_profiles(self) -> None:
        """Seed baseline capabilities according to Section 10.4 audit."""
        # Tokocrypto Spot
        self.register_profile(
            VenueCapabilityProfile(
                venue="tokocrypto",
                market_type="spot",
                status=CapabilityStatus.VERIFIED_WITH_LIMITS,
                supports_client_order_id=True,
                max_client_id_len=32,
                supports_private_ws=False,
                supports_post_only=True,
                min_notional_usdt=5.0,
            )
        )
        # Indodax Spot
        self.register_profile(
            VenueCapabilityProfile(
                venue="indodax",
                market_type="spot",
                status=CapabilityStatus.VERIFIED_WITH_LIMITS,
                supports_client_order_id=True,
                max_client_id_len=64,
                supports_private_ws=False,
                supports_post_only=True,
                min_notional_usdt=1.0,
            )
        )
        # OKX Futures
        self.register_profile(
            VenueCapabilityProfile(
                venue="okx",
                market_type="futures",
                status=CapabilityStatus.VERIFIED,
                supports_client_order_id=True,
                supports_reduce_only=True,
                supports_private_ws=True,
                supports_post_only=True,
                min_notional_usdt=10.0,
            )
        )
        # MEXC Futures
        self.register_profile(
            VenueCapabilityProfile(
                venue="mexc",
                market_type="futures",
                status=CapabilityStatus.VERIFIED,
                supports_client_order_id=True,
                supports_reduce_only=True,
                supports_private_ws=False,
                supports_post_only=True,
                min_notional_usdt=5.0,
            )
        )

    def register_profile(self, profile: VenueCapabilityProfile) -> None:
        key = f"{profile.venue.lower()}:{profile.market_type.lower()}"
        self._profiles[key] = profile

    def get_profile(self, venue: str, market_type: str = "spot") -> Optional[VenueCapabilityProfile]:
        key = f"{venue.lower().strip()}:{market_type.lower().strip()}"
        return self._profiles.get(key)

    def is_operation_permitted(
        self, venue: str, market_type: str, operation: str
    ) -> bool:
        """Verify whether an operation is permitted under current capability certification."""
        profile = self.get_profile(venue, market_type)
        if profile is None:
            return False
        if profile.status in (CapabilityStatus.DISABLED, CapabilityStatus.UNVERIFIED):
            return False
        if operation == "client_order_id" and not profile.supports_client_order_id:
            return False
        if operation == "reduce_only" and not profile.supports_reduce_only:
            return False
        if operation == "private_ws" and not profile.supports_private_ws:
            return False
        return True
