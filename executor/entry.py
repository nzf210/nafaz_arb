"""Compatibility facade for entry order and hedge behavior."""

from executor.entry_orders import EntryOrderMixin
from executor.entry_hedge import EntryHedgeMixin


class EntryMixin(EntryOrderMixin, EntryHedgeMixin):
    """Composes entry order lifecycle and hedge execution."""
