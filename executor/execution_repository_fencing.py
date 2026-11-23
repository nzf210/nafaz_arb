"""Session-scoped ownership and admission risk-scope helpers."""
from __future__ import annotations

from typing import Any, Mapping

from executor.ownership import build_scope_key
from executor.risk_control import risk_scope_keys


def admission_risk_scopes(
    *, account_scope_id: str, pair_key: str, session_id: str,
    legs: list[Mapping[str, Any]],
) -> list[str]:
    scopes = {
        "global", f"account:{account_scope_id}",
        f"pair:{pair_key}", f"session:{session_id}",
    }
    for leg in legs:
        scopes.update(risk_scope_keys(
            account_scope_id=account_scope_id,
            venue=str(leg["venue"]),
            market_type=str(leg["market_type"]),
            instrument=str(leg["symbol"]),
        ))
    return sorted(scopes)


async def validate_coordinator_lease(
    conn: Any, lease: Any, *, session_id: str,
    account_scope_id: str, expected_generation: int,
    session_scope_key: str | None = None,
    session_owner_id: str | None = None,
) -> None:
    def value(name: str) -> Any:
        return lease.get(name) if isinstance(lease, Mapping) else getattr(lease, name, None)

    owner_id = str(value("owner_id") or "").strip()
    scope_key = str(value("scope_key") or "").strip()
    generation = int(value("generation") or 0)
    expected_key = build_scope_key(
        account_scope_id, "coordinator", "session", session_id
    )
    if (
        not owner_id or scope_key != expected_key or generation <= 0
        or generation != int(expected_generation)
        or (session_scope_key is not None and str(session_scope_key) != scope_key)
        or (session_owner_id is not None and str(session_owner_id) != owner_id)
    ):
        from executor.execution_repository import StaleAggregate
        raise StaleAggregate("coordinator lease identity or generation is invalid")
    row = await conn.fetchrow(
        """
        SELECT 1 FROM public.executor_ownership_leases
        WHERE scope_key=$1 AND account_scope_id=$2
          AND venue='coordinator' AND market_type='session'
          AND instrument=$3 AND owner_id=$4 AND generation=$5
          AND lease_expires_at > clock_timestamp()
        FOR UPDATE
        """,
        scope_key, account_scope_id, session_id, owner_id, generation,
    )
    if row is None:
        from executor.execution_repository import StaleAggregate
        raise StaleAggregate("coordinator lease is expired, missing or superseded")

def assert_aggregate_versions(
    row: Any, expected_state_version: int | None,
    expected_aggregate_version: int, fencing_generation: int,
    *, entity_version_check: bool = True,
) -> None:
    from executor.execution_repository import ExecutionRepositoryError, StaleAggregate

    if row is None:
        raise ExecutionRepositoryError("execution session does not exist")
    if int(row["aggregate_version"]) != int(expected_aggregate_version):
        raise StaleAggregate("session aggregate version is stale")
    if entity_version_check and int(row["state_version"]) != int(expected_state_version):
        raise StaleAggregate("session state version is stale")
    current_fence = row["fencing_generation"]
    if current_fence is None or int(current_fence) != int(fencing_generation):
        raise StaleAggregate("ownership fencing generation is missing or stale")


async def insert_session_event(
    conn: Any, *, session_id: str, aggregate_version: int,
    entity_type: str, entity_id: str, entity_version: int,
    event_type: str, idempotency_key: str, actor_id: str,
    fencing_generation: int, reason: str,
    correlation_id: str | None, payload: Mapping[str, Any],
) -> None:
    from executor.execution_serialization import _canonical_json

    await conn.execute(
        """
        INSERT INTO public.execution_session_events (
            session_id, aggregate_version, entity_type, entity_id,
            entity_version, event_type, event_idempotency_key, actor_id,
            fencing_generation, reason, correlation_id, payload
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
        """,
        session_id, aggregate_version, entity_type, entity_id,
        entity_version, event_type, idempotency_key, actor_id,
        fencing_generation, reason[:2000], correlation_id,
        _canonical_json(payload),
    )


async def transition_leg_in_transaction(
    conn: Any, *, session_id: str, leg_id: str,
    expected_state: str, target_state: str, fencing_generation: int,
    event_type: str, idempotency_key: str, actor_id: str,
    correlation_id: str | None = None, payload: Mapping[str, Any] | None = None,
) -> dict:
    """CAS one leg and its session aggregate inside an existing transaction.

    The caller must already hold the appropriate coordinator/session and
    venue mutation fences. Keeping this helper transaction-local lets the
    mutation boundary commit SUBMITTING before the exchange request.
    """
    from executor.execution_repository import ExecutionRepositoryError, StaleAggregate
    from executor.execution_states import LEG_TRANSITIONS

    session = await conn.fetchrow(
        """
        SELECT aggregate_version, fencing_generation
        FROM public.execution_sessions
        WHERE session_id=$1 FOR UPDATE
        """,
        session_id,
    )
    if session is None:
        raise ExecutionRepositoryError("execution session does not exist")
    if (
        session["fencing_generation"] is None
        or int(session["fencing_generation"]) != int(fencing_generation)
    ):
        raise StaleAggregate("session fencing generation is stale")
    leg = await conn.fetchrow(
        """
        SELECT status, state_version
        FROM public.execution_legs
        WHERE session_id=$1 AND leg_id=$2 FOR UPDATE
        """,
        session_id, leg_id,
    )
    if leg is None:
        raise ExecutionRepositoryError("execution leg does not exist in the session")
    old_state = str(leg["status"]).upper()
    target = str(target_state).upper()
    if old_state != str(expected_state).upper():
        raise StaleAggregate(
            f"leg state changed before transition: expected {expected_state}, found {old_state}"
        )
    if target not in LEG_TRANSITIONS.get(old_state, set()):
        raise ExecutionRepositoryError(f"leg transition {old_state}->{target} is not allowed")
    leg_version = int(leg["state_version"]) + 1
    aggregate_version = int(session["aggregate_version"]) + 1
    updated_leg = await conn.fetchrow(
        """
        UPDATE public.execution_legs
        SET status=$3, state_version=$4, updated_at=clock_timestamp()
        WHERE session_id=$1 AND leg_id=$2 AND state_version=$5
        RETURNING state_version
        """,
        session_id, leg_id, target, leg_version, int(leg["state_version"]),
    )
    if updated_leg is None:
        raise StaleAggregate("leg state compare-and-swap lost")
    updated_session = await conn.fetchrow(
        """
        UPDATE public.execution_sessions
        SET aggregate_version=$2, updated_at=clock_timestamp()
        WHERE session_id=$1 AND aggregate_version=$3
          AND fencing_generation=$4
        RETURNING aggregate_version
        """,
        session_id, aggregate_version, int(session["aggregate_version"]),
        int(fencing_generation),
    )
    if updated_session is None:
        raise StaleAggregate("session aggregate compare-and-swap lost")
    await insert_session_event(
        conn, session_id=session_id, aggregate_version=aggregate_version,
        entity_type="LEG", entity_id=leg_id, entity_version=leg_version,
        event_type=event_type, idempotency_key=idempotency_key,
        actor_id=actor_id, fencing_generation=int(fencing_generation),
        reason="", correlation_id=correlation_id,
        payload={**dict(payload or {}), "from_state": old_state, "target_state": target},
    )
    return {
        "state": target, "state_version": leg_version,
        "aggregate_version": aggregate_version,
    }


async def append_leg_event_in_transaction(
    conn: Any, *, session_id: str, leg_id: str,
    fencing_generation: int, event_type: str, idempotency_key: str,
    actor_id: str, correlation_id: str | None = None,
    payload: Mapping[str, Any] | None = None,
) -> dict:
    """Append an event for a leg without changing its aggregate lifecycle state."""
    from executor.execution_repository import ExecutionRepositoryError, StaleAggregate

    session = await conn.fetchrow(
        """
        SELECT aggregate_version, fencing_generation
        FROM public.execution_sessions
        WHERE session_id=$1 FOR UPDATE
        """,
        session_id,
    )
    if session is None:
        raise ExecutionRepositoryError("execution session does not exist")
    if (
        session["fencing_generation"] is None
        or int(session["fencing_generation"]) != int(fencing_generation)
    ):
        raise StaleAggregate("session fencing generation is stale")
    leg = await conn.fetchrow(
        """
        SELECT status, state_version FROM public.execution_legs
        WHERE session_id=$1 AND leg_id=$2 FOR UPDATE
        """,
        session_id, leg_id,
    )
    if leg is None:
        raise ExecutionRepositoryError("execution leg does not exist in the session")
    aggregate_version = int(session["aggregate_version"]) + 1
    updated = await conn.fetchrow(
        """
        UPDATE public.execution_sessions
        SET aggregate_version=$2, updated_at=clock_timestamp()
        WHERE session_id=$1 AND aggregate_version=$3
          AND fencing_generation=$4
        RETURNING aggregate_version
        """,
        session_id, aggregate_version, int(session["aggregate_version"]),
        int(fencing_generation),
    )
    if updated is None:
        raise StaleAggregate("session aggregate compare-and-swap lost")
    await insert_session_event(
        conn, session_id=session_id, aggregate_version=aggregate_version,
        entity_type="LEG", entity_id=leg_id,
        entity_version=int(leg["state_version"]),
        event_type=event_type, idempotency_key=idempotency_key,
        actor_id=actor_id, fencing_generation=int(fencing_generation),
        reason="", correlation_id=correlation_id,
        payload={**dict(payload or {}), "leg_state": str(leg["status"]).upper()},
    )
    return {
        "state": str(leg["status"]).upper(),
        "state_version": int(leg["state_version"]),
        "aggregate_version": aggregate_version,
    }
