"""Compatibility facade for modular exit behavior."""

from executor.exit_pricing import ExitPricingMixin
from executor.exit_orders import ExitOrderMixin
from executor.exit_fills import ExitFillMixin
from executor.exit_settlement import ExitSettlementMixin


class ExitMixin(ExitPricingMixin, ExitOrderMixin, ExitFillMixin, ExitSettlementMixin):
    """Composes pricing, order lifecycle, and fill accounting behaviors."""
