"""Exit lifecycle facade preserving the engine's existing mixin API."""

from executor.exit_execution import ExitExecutionMixin
from executor.exit_orders import ExitOrderManagementMixin
from executor.exit_pricing import ExitPricingMixin


class ExitMixin(ExitPricingMixin, ExitOrderManagementMixin, ExitExecutionMixin):
    """Composed exit pricing, order management, and fill execution."""
