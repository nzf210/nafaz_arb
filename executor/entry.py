"""Entry lifecycle facade preserving the engine's existing mixin API."""

from executor.entry_hedge import EntryHedgeMixin
from executor.entry_monitoring import EntryMonitoringMixin
from executor.entry_orders import EntryOrderManagementMixin


class EntryMixin(
    EntryMonitoringMixin,
    EntryOrderManagementMixin,
    EntryHedgeMixin,
):
    """Composed entry monitoring, order lifecycle, and hedge execution."""
