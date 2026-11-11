"""Force-close facade preserving the engine's existing mixin API."""

from executor.force_close_legs import ForceCloseLegMixin
from executor.force_close_lifecycle import ForceCloseLifecycleMixin


class ForceCloseMixin(ForceCloseLifecycleMixin, ForceCloseLegMixin):
    """Composed force-close leg execution and lifecycle cleanup."""
