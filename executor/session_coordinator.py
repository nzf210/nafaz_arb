"""Durable V3 session admission coordinator.

This service owns aggregate admission only. It deliberately has no exchange
client dependency and cannot submit, cancel, hedge, or exit venue orders.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping

from executor.execution_repository import ExecutionSessionRepository
from executor.ownership import OwnershipLeaseService


class SessionAdmissionError(RuntimeError):
    """Admission input is incomplete or cannot be safely canonicalized."""


def _canonical(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False, default=str,
        )
    except (TypeError, ValueError) as exc:
        raise SessionAdmissionError("admission payload is not canonical JSON data") from exc


def build_admission_identity(
    *, account_scope_id: str, source_dispatch_id: str,
    opportunity_id: str, pair_key: str,
) -> tuple[str, str]:
    identity = {
        "account_scope_id": str(account_scope_id or "").strip(),
        "source_dispatch_id": str(source_dispatch_id or "").strip(),
        "opportunity_id": str(opportunity_id or "").strip(),
        "pair_key": str(pair_key or "").strip(),
    }
    if not all(identity.values()):
        raise SessionAdmissionError(
            "account_scope_id, source_dispatch_id, opportunity_id and pair_key are required"
        )
    idempotency_identity = {
        "account_scope_id": identity["account_scope_id"],
        "source_dispatch_id": identity["source_dispatch_id"],
        "pair_key": identity["pair_key"],
    }
    digest = hashlib.sha256(
        ("nafaz:executor-v3:admission:v1:" + _canonical(idempotency_identity)).encode("utf-8")
    ).hexdigest()
    return "v3-adm-" + digest, "v3-session-" + digest[:32]


def _require_snapshot(name: str, value: Mapping[str, Any]) -> dict:
    if not isinstance(value, Mapping) or not value:
        raise SessionAdmissionError(f"{name} must be a non-empty immutable mapping")
    return json.loads(_canonical(value))


def _validate_policy_evidence(snapshot: Mapping[str, Any]) -> None:
    evidence = snapshot.get("evidence")
    required = ("market_metadata", "fee_schedule", "fx_conversion")
    if not isinstance(evidence, Mapping):
        raise SessionAdmissionError("policy_snapshot.evidence is required")
    now = datetime.now(timezone.utc)
    for name in required:
        item = evidence.get(name)
        if not isinstance(item, Mapping):
            raise SessionAdmissionError(f"policy evidence {name} is missing")
        status = str(item.get("status") or "").upper()
        allowed = {"VERIFIED", "NOT_REQUIRED"} if name == "fx_conversion" else {"VERIFIED"}
        if status not in allowed:
            raise SessionAdmissionError(f"policy evidence {name} is not verified")
        try:
            observed = datetime.fromisoformat(str(item["observed_at"]).replace("Z", "+00:00"))
            valid_until = datetime.fromisoformat(str(item["valid_until"]).replace("Z", "+00:00"))
        except (KeyError, TypeError, ValueError) as exc:
            raise SessionAdmissionError(f"policy evidence {name} has invalid timestamps") from exc
        if observed.tzinfo is None or valid_until.tzinfo is None:
            raise SessionAdmissionError(f"policy evidence {name} timestamps must include timezone")
        if observed > now or valid_until <= now or valid_until <= observed:
            raise SessionAdmissionError(f"policy evidence {name} is stale or from the future")


class SessionCoordinator:
    """Orchestrates lease acquisition, durable admission and fenced transitions."""

    def __init__(
        self, *, pool: Any,
        repository: ExecutionSessionRepository | None = None,
        ownership: OwnershipLeaseService | None = None,
    ) -> None:
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL pool is required")
        self.pool = pool
        self.repository = repository or ExecutionSessionRepository(pool)
        self.ownership = ownership or OwnershipLeaseService()

    async def admit(
        self, *, opportunity_id: str, source_dispatch_id: str,
        account_scope_id: str, pair_key: str,
        legs: list[Mapping[str, Any]], actor_id: str,
        policy_snapshot: Mapping[str, Any],
        execution_policy_snapshot: Mapping[str, Any],
        risk_policy_snapshot: Mapping[str, Any],
        exit_policy_snapshot: Mapping[str, Any],
        risk_metrics: Mapping[str, Any] | None = None,
        risk_limits: Mapping[str, Any] | None = None,
    ) -> dict:
        admission_key, session_id = build_admission_identity(
            account_scope_id=account_scope_id,
            source_dispatch_id=source_dispatch_id,
            opportunity_id=opportunity_id,
            pair_key=pair_key,
        )
        if not str(actor_id or "").strip():
            raise SessionAdmissionError("actor_id is required")
        snapshots = {
            "policy_snapshot": _require_snapshot("policy_snapshot", policy_snapshot),
            "execution_policy_snapshot": _require_snapshot(
                "execution_policy_snapshot", execution_policy_snapshot
            ),
            "risk_policy_snapshot": _require_snapshot(
                "risk_policy_snapshot", risk_policy_snapshot
            ),
            "exit_policy_snapshot": _require_snapshot(
                "exit_policy_snapshot", exit_policy_snapshot
            ),
        }
        _validate_policy_evidence(snapshots["policy_snapshot"])
        if not isinstance(legs, list) or len(legs) != 2:
            raise SessionAdmissionError("exactly two venue-bound legs are required")

        lease = None
        try:
            async with self.pool.acquire() as conn:
                lease = await self.ownership.acquire(
                    conn,
                    account_scope_id=account_scope_id,
                    venue="coordinator",
                    market_type="session",
                    instrument=session_id,
                )
            result = await self.repository.create_session(
                session_id=session_id,
                opportunity_id=opportunity_id,
                source_dispatch_id=source_dispatch_id,
                admission_key=admission_key,
                account_scope_id=account_scope_id,
                pair_key=pair_key,
                policy_snapshot=snapshots["policy_snapshot"],
                execution_policy_snapshot=snapshots["execution_policy_snapshot"],
                risk_policy_snapshot=snapshots["risk_policy_snapshot"],
                exit_policy_snapshot=snapshots["exit_policy_snapshot"],
                legs=legs,
                actor_id=actor_id,
                fencing_generation=lease.generation,
                coordinator_lease=lease,
                risk_metrics=dict(risk_metrics or {}),
                risk_limits=dict(risk_limits or {}),
            )
            return {
                **result,
                "admission_key": admission_key,
                "coordinator_lease": lease,
                "coordinator_generation": lease.generation,
            }
        except Exception:
            if lease is not None:
                async with self.pool.acquire() as conn:
                    await self.ownership.release(conn, lease)
            raise

    async def transition(
        self, *, coordinator_lease: Any, **transition: Any,
    ) -> dict:
        if coordinator_lease is None:
            raise SessionAdmissionError("a live coordinator lease is required")
        return await self.repository.transition_session(
            **transition, coordinator_lease=coordinator_lease,
        )

    async def transition_leg(
        self, *, coordinator_lease: Any, **transition: Any,
    ) -> dict:
        if coordinator_lease is None:
            raise SessionAdmissionError("a live coordinator lease is required")
        return await self.repository.transition_leg(
            **transition, coordinator_lease=coordinator_lease,
        )

    async def release(self, coordinator_lease: Any) -> bool:
        if coordinator_lease is None:
            return False
        async with self.pool.acquire() as conn:
            return await self.ownership.release(conn, coordinator_lease)
