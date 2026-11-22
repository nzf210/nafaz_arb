"""Shared constants and error types for PostgreSQL executor fencing."""
import json
import logging
from typing import Any, Optional

logger = logging.getLogger("executor.global_stop")

FENCE_LOCK_KEY = 731_904_221
DERISK_LOCK_KEY = 731_904_222
STOP_CHANNEL = "arb:global_stop"


class GlobalStopError(RuntimeError):
    """Raised when global admission cannot be proven safe."""


def _json_mapping(value: Any) -> Optional[dict]:
    """Decode JSONB values consistently across asyncpg codecs and test doubles."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return None
    return value if isinstance(value, dict) else None
