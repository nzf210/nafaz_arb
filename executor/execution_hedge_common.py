"""Shared durable hedge lifecycle primitives."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from executor.execution_repository import ExecutionRepositoryError
from executor.execution_repository_fencing import (
    insert_session_event, validate_coordinator_lease,
)


class HedgeObligationError(ExecutionRepositoryError):
    """A hedge obligation or reservation violates its durable invariants."""


def _qty(value: Any, name: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result < 0 if allow_zero else result <= 0):
        raise ValueError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
    return result


def _text(value: Any, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), "f")


class HedgeServiceBase:
    """Common transaction, lease, and event-journal boundary for hedge services."""

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        self.pool = pool

    async def _lock_session(self, conn: Any, *, session_id: str, lease: Any, generation: int):
        row = await conn.fetchrow(
            """
            SELECT status, state_version, aggregate_version, fencing_generation,
                   account_scope_id, pair_key, coordinator_scope_key, coordinator_owner_id
            FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
            """,
            session_id,
        )
        if row is None:
            raise HedgeObligationError("execution session does not exist")
        await validate_coordinator_lease(
            conn, lease, session_id=session_id,
            account_scope_id=str(row["account_scope_id"]),
            expected_generation=generation,
            session_scope_key=row["coordinator_scope_key"],
            session_owner_id=row["coordinator_owner_id"],
        )
        return row

    async def _append_event(
        self, conn: Any, *, session_id: str, session: Any,
        event_type: str, idempotency_key: str, actor_id: str,
        generation: int, payload: dict, reason: str = "",
        target_state: str | None = None,
    ) -> int:
        version = int(session["aggregate_version"]) + 1
        current_state = str(session["status"]).upper()
        next_state = str(target_state or current_state).upper()
        next_state_version = int(session["state_version"]) + (
            1 if next_state != current_state else 0
        )
        updated = await conn.fetchrow(
            """
            UPDATE public.execution_sessions
            SET status=$2, state_version=$3, aggregate_version=$4,
                updated_at=clock_timestamp()
            WHERE session_id=$1 AND aggregate_version=$5
              AND state_version=$6 AND fencing_generation=$7
            RETURNING aggregate_version
            """,
            session_id, next_state, next_state_version, version,
            int(session["aggregate_version"]), int(session["state_version"]),
            generation,
        )
        if updated is None:
            raise HedgeObligationError("session aggregate cursor compare-and-swap lost")
        await insert_session_event(
            conn, session_id=session_id, aggregate_version=version,
            entity_type="SESSION", entity_id=session_id,
            entity_version=next_state_version,
            event_type=event_type, idempotency_key=idempotency_key,
            actor_id=actor_id, fencing_generation=generation,
            reason=reason, correlation_id=session_id, payload=payload,
        )
        return version
