"""Compatibility facade for modular exit behavior."""

from executor.exit_pricing import ExitPricingMixin
from executor.exit_orders import ExitOrderMixin
from executor.exit_fills import ExitFillMixin


class ExitMixin(ExitPricingMixin, ExitOrderMixin, ExitFillMixin):
    """Composes pricing, order lifecycle, and fill accounting behaviors."""
