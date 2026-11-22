"""Fail-closed PostgreSQL/Redis ownership for risk-reducing mutations."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from executor.global_stop import GlobalStopError
from executor.engine_common import logger
from executor.emergency_derisk import EmergencyDeriskError, EmergencyDeriskLease
from executor.ownership import account_scope_id_for


def _database_unavailable(service: Any, exc: BaseException) -> bool:
    """Recognize only a failed fencing authority, never caller/venue errors."""
    if (
        isinstance(exc, GlobalStopError)
        and "serialization is unavailable" in str(exc).lower()
    ):
        return True
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    checker = getattr(service, "_is_connection_error", None)
    if callable(checker):
        try:
            return bool(checker(exc))
        except Exception:
            return False
    return False


async def _acquire_emergency_lease(
    engine: Any, *, client_order_id: str, account_scope_id: Optional[str],
    venue: str, market_type: str, instrument: str,
) -> EmergencyDeriskLease:
    config = engine.config
    owner_id = (
        str(getattr(config, "executor_instance_id", "") or "").strip()
        or os.getenv("EXECUTOR_INSTANCE_ID", "").strip()
        or os.getenv("HOSTNAME", "").strip()
    )
    designated_owner_id = (
        str(getattr(config, "emergency_derisk_owner_id", "") or "").strip()
        or os.getenv("EMERGENCY_DERISK_OWNER_ID", "").strip()
    )
    resolved_scope = account_scope_id or account_scope_id_for(config, venue)
    ttl_ms = int(getattr(config, "emergency_derisk_lease_ttl_ms", 120000) or 120000)
    try:
        lease = await EmergencyDeriskLease.acquire(
            getattr(engine, "redis", None),
            owner_id=owner_id,
            designated_owner_id=designated_owner_id,
            client_order_id=client_order_id,
            account_scope_id=resolved_scope,
            venue=venue,
            market_type=market_type,
            instrument=instrument,
            ttl_ms=ttl_ms,
        )
    except EmergencyDeriskError as exc:
        raise GlobalStopError(
            f"emergency de-risk ownership unavailable; refusing unfenced mutation: {exc}"
        ) from exc
    logger.critical(
        "[De-risk Fence] PostgreSQL unavailable; designated owner %s acquired "
        "Redis emergency lease for %s (%s/%s/%s). New exposure remains blocked.",
        owner_id, client_order_id, venue, market_type, instrument,
    )
    return lease


@asynccontextmanager
async def derisk_fence(
    engine: Any, *, client_order_id: str, venue: str,
    market_type: str, instrument: str,
    account_scope_id: Optional[str] = None,
) -> AsyncIterator[Any]:
    """Use PostgreSQL fencing normally and a designated Redis lease only on DB outage."""
    if (
        not isinstance(client_order_id, str)
        or not client_order_id.strip()
        or len(client_order_id) > 128
    ):
        raise GlobalStopError("valid stable client_order_id is required before de-risk submit")
    if not all(str(value or "").strip() for value in (venue, market_type, instrument)):
        raise GlobalStopError("venue, market_type and instrument are required for de-risk fencing")

    config = engine.config
    service = getattr(engine, "global_stop", None)
    if service is None and bool(getattr(config, "mock_mode", False)):
        yield None
        return

    if service is not None:
        fence = service.derisk_submit_fence(client_order_id=client_order_id)
        try:
            conn = await fence.__aenter__()
        except Exception as exc:
            if not _database_unavailable(service, exc):
                raise
        else:
            try:
                yield conn
            except BaseException as exc:
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
                    logger.critical(
                        "[De-risk Fence] PostgreSQL connection was lost while releasing "
                        "the fence after %s; venue acknowledgement was already returned",
                        client_order_id,
                    )
            return

    lease = await _acquire_emergency_lease(
        engine,
        client_order_id=client_order_id,
        account_scope_id=account_scope_id,
        venue=venue,
        market_type=market_type,
        instrument=instrument,
    )
    async with lease:
        yield lease
