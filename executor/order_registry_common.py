"""Shared order-registry role catalogue and logging."""

import logging

from executor.models import OrderRole

logger = logging.getLogger("executor.order_registry")

# Exit and risk-reducing orders remain recoverable even after their position closes.
_EXIT_ROLES = (
    OrderRole.EXIT_SPOT.value,
    OrderRole.EXIT_FUTURES.value,
    OrderRole.EMERGENCY.value,
    OrderRole.FORCE_CLOSE.value,
    OrderRole.ROLLBACK.value,
)
