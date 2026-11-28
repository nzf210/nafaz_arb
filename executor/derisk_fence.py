"""De-risking submit fencing helper (Tahap 4.2).

Risk-reducing legs (exit, unwind, rollback, force-close) are allowed while
Global STOP is active. They use the shared PostgreSQL session lock when it is
available, but a transient loss of that lock authority must not strand an
existing position: entry paths remain fail-closed while de-risking can proceed
with a loud audit log and no shared serialization.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from executor.global_stop import GlobalStopError
from executor.engine_common import logger


def _database_unavailable(service: Any, exc: BaseException) -> bool:
    """Recognize failures while acquiring/releasing the DB fence.

    This helper is called only around the fence context manager's enter/exit;
    exceptions raised by the venue/caller inside the yielded block bypass it.
    A missing control row, schema error, permission error, or disconnected
    PostgreSQL session means serialization is unavailable, not that a
    risk-reducing order should be denied.
    """
    if isinstance(exc, GlobalStopError):
        # The caller validates client_order_id before asking the service for a
        # fence. GlobalStopError here therefore means the optional de-risk
        # serialization authority could not be established.
        return True
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    # asyncpg exceptions raised before yielding are database-fence failures
    # (including missing control rows/tables and grants), not venue failures.
    module = type(exc).__module__.lower()
    if module.startswith("asyncpg"):
        return True
    checker = getattr(service, "_is_connection_error", None)
    if callable(checker):
        try:
            return bool(checker(exc))
        except Exception:
            return False
    return False


@asynccontextmanager
async def derisk_fence(engine: Any, *, client_order_id: str) -> AsyncIterator[Any]:
    """Serialize a risk-reducing submit when possible; never let DB outage block it."""
    if not isinstance(client_order_id, str) or not client_order_id.strip() or len(client_order_id) > 128:
        raise GlobalStopError("valid stable client_order_id is required before de-risk submit")

    service = getattr(engine, "global_stop", None)
    if service is None:
        # Mock/paper mode or an explicitly unconfigured authority.
        yield None
        return

    fence = service.derisk_submit_fence(client_order_id=client_order_id)
    try:
        conn = await fence.__aenter__()
    except Exception as exc:
        if not _database_unavailable(service, exc):
            raise
        # New exposure is still blocked by order_submit_fence/engine readiness.
        # A close/rollback is safer than leaving the already-open leg untouched.
        logger.critical(
            "[De-risk Fence] PostgreSQL unavailable for %s; proceeding without "
            "shared serialization. New exposure remains fail-closed; reconcile "
            "this de-risk order by its exact client ID.",
            client_order_id,
        )
        yield None
        return

    try:
        yield conn
    except BaseException as exc:
        # Preserve the venue/caller error (especially an ambiguous POST error);
        # cleanup must not replace the error that drives exact-ID reconciliation.
        try:
            await fence.__aexit__(type(exc), exc, exc.__traceback__)
        except BaseException:
            logger.exception(
                "[De-risk Fence] cleanup failed after submit error for %s",
                client_order_id,
            )
        raise
    else:
        try:
            await fence.__aexit__(None, None, None)
        except Exception as exc:
            if not _database_unavailable(service, exc):
                raise
            # The venue call returned successfully; a lost advisory-lock release
            # must not make the caller treat an accepted close as a failed submit.
            logger.critical(
                "[De-risk Fence] PostgreSQL connection was lost while releasing "
                "the fence after %s; venue acknowledgement was already returned",
                client_order_id,
            )
