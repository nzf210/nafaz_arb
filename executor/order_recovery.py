"""Composable universal order recovery for the arbitrage engine."""

from executor.recovery_adoption import RecoveryAdoptionMixin
from executor.recovery_discovery import RecoveryDiscoveryMixin
from executor.recovery_orphans import RecoveryOrphanMixin
from executor.recovery_registry import RecoveryRegistryMixin
from executor.recovery_utils import ACKNOWLEDGED_EXIT_ROLES, _spec_dict


class OrderRecoveryMixin(
    RecoveryRegistryMixin,
    RecoveryDiscoveryMixin,
    RecoveryAdoptionMixin,
    RecoveryOrphanMixin,
):
    """Compatibility facade preserving the existing engine method surface."""
