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
