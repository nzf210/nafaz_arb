"""Compatibility facade composing force-close behavior."""

from executor.force_close_legs import ForceCloseLegsMixin
from executor.force_close_handler import ForceCloseHandlerMixin


class ForceCloseMixin(ForceCloseHandlerMixin, ForceCloseLegsMixin):
    """Composes leg execution and force-close orchestration."""
