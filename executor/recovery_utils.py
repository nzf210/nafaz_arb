"""Shared constants and small conversions for order recovery."""

from typing import Any, Optional

from executor.models import OrderRole

ACKNOWLEDGED_EXIT_ROLES = (
    OrderRole.EXIT_FUTURES.value,
    OrderRole.ROLLBACK.value,
    OrderRole.EMERGENCY.value,
    OrderRole.FORCE_CLOSE.value,
)


def _spec_dict(amount_spec: Any) -> Optional[dict]:
    """Normalize an amount spec (dataclass or mapping) to a plain mapping."""
    if amount_spec is None:
        return None
    if hasattr(amount_spec, "to_dict"):
        return amount_spec.to_dict()
    return amount_spec
