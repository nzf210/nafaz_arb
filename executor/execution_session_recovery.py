"""Durable session lease reclaim and recovery fencing."""
from __future__ import annotations

from typing import Any

from executor.execution_repository import (
    ExecutionRepositoryError, StaleAggregate, ExecutionSessionRepository,
)
from executor.execution_repository_fencing import validate_coordinator_lease


class ExecutionSessionRecovery:
    """Reclaims aggregate ownership without rebuilding or mutating its policy snapshot."""

    def __init__(self, pool: Any, repository: ExecutionSessionRepository):
        self.pool = pool
        self.repository = repository

    async def reclaim(
        self, *, session_id: str, account_scope_id: str,
        coordinator_lease: Any, actor_id: str,
    ) -> dict:
        actor = str(actor_id or "").strip()
        if not actor:
            raise ValueError("actor_id is required for session recovery")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT status, state_version, aggregate_version,
                           fencing_generation, account_scope_id
                    FROM public.execution_sessions
                    WHERE session_id=$1
                    FOR UPDATE
                    """,
                    session_id,
                )
                if row is None:
                    raise ExecutionRepositoryError("execution session does not exist")
                if str(row["account_scope_id"] or "") != str(account_scope_id):
                    raise StaleAggregate("session account scope does not match recovery request")
                await validate_coordinator_lease(
                    conn, coordinator_lease, session_id=session_id,
                    account_scope_id=account_scope_id,
                    expected_generation=int(coordinator_lease.generation),
                )
                status = str(row["status"]).upper()
                if status in {"COMPLETED", "REJECTED"}:
                    raise ExecutionRepositoryError(
                        f"terminal {status} session does not require coordinator recovery"
                    )
                old_generation = row["fencing_generation"]
                if old_generation is None:
                    raise StaleAggregate(
                        "cannot recover a session without durable prior fencing provenance"
                    )
                old_generation = int(old_generation)
                generation = int(coordinator_lease.generation)
                if old_generation > generation:
                    raise StaleAggregate("recovery lease is older than the session fencing generation")
                state_version = int(row["state_version"])
                aggregate_version = int(row["aggregate_version"])
                reclaimed = old_generation < generation
                if reclaimed:
                    next_aggregate_version = aggregate_version + 1
                    updated = await conn.fetchrow(
                        """
                        UPDATE public.execution_sessions
                        SET fencing_generation=$2, aggregate_version=$3,
                            updated_at=clock_timestamp()
                        WHERE session_id=$1 AND fencing_generation=$4
                          AND state_version=$5 AND aggregate_version=$6
                        RETURNING aggregate_version
                        """,
                        session_id, generation, next_aggregate_version,
                        old_generation, state_version, aggregate_version,
                    )
                    if updated is None:
                        raise StaleAggregate("session recovery compare-and-swap lost")
                    await self.repository._insert_event(
                        conn, session_id=session_id,
                        aggregate_version=next_aggregate_version,
                        entity_type="SESSION", entity_id=session_id,
                        entity_version=state_version,
                        event_type="COORDINATOR_LEASE_RECLAIMED",
                        idempotency_key=f"coordinator-recovery:{session_id}:{generation}",
                        actor_id=actor, fencing_generation=generation,
                        reason="expired coordinator lease reclaimed",
                        correlation_id=session_id,
                        payload={
                            "previous_generation": old_generation,
                            "current_generation": generation,
                            "status": status,
                        },
                    )
                    aggregate_version = next_aggregate_version
                return {
                    "session_id": session_id,
                    "status": status,
                    "state_version": state_version,
                    "aggregate_version": aggregate_version,
                    "fencing_generation": generation,
                    "reclaimed": reclaimed,
                }
