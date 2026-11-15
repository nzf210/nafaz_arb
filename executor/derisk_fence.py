"""De-risking submit fencing helper (Tahap 4.2).

Every create-order call must be serialized against global STOP. Risk-reducing
legs (exit, unwind, rollback, force-close) are allowed to proceed while STOP is
active, but they still take the shared PostgreSQL serialization lock and still
respect an unresolved in-flight submit.

Compatibility: when no GlobalStopService is configured (mock/paper, or an
instance without POSTGRES_DSN), the helper yields without fencing rather than
blocking de-risking. Entry submission keeps its own hard fail-closed rule.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from executor.global_stop import GlobalStopError
from executor.engine_common import logger


@asynccontextmanager
async def derisk_fence(engine: Any, *, client_order_id: str) -> AsyncIterator[Any]:
    """Serialize one risk-reducing submit against the global STOP authority."""
    service = getattr(engine, "global_stop", None)
    if service is None:
        # De-risking must never be blocked by a missing fence: closing risk is
        # always safer than leaving it open because accounting is unavailable.
        yield None
        return
    try:
        async with service.derisk_submit_fence(client_order_id=client_order_id) as conn:
            yield conn
    except GlobalStopError as exc:
        logger.error(
            "[De-risk Fence] %s blocked by global STOP authority: %s",
            client_order_id, exc,
        )
        raise
