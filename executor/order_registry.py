"""Universal order registry facade preserving the existing public API."""

from typing import Any, Dict

from executor.models import OrderRecord
from executor.order_registry_common import _EXIT_ROLES, logger
from executor.order_registry_recovery import OrderRegistryRecoveryMixin
from executor.order_registry_storage import OrderRegistryStorageMixin


class UniversalOrderRegistry(OrderRegistryStorageMixin, OrderRegistryRecoveryMixin):
    """Durable, idempotent registry of every order the executor submits."""

    def __init__(self, redis: Any, supabase: Any = None):
        self.redis = redis
        self.supabase = supabase
        # Hot cache of adopted records, so the startup pass does not re-read Redis.
        self.records: Dict[str, OrderRecord] = {}

    # ------------------------------------------------------------ venue truth
