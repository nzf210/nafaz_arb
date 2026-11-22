"""Durable PostgreSQL repository for Executor V3 session aggregates.

This module deliberately owns only session/leg admission and aggregate transitions.
Exchange mutations, fill accounting, hedge obligations and exit execution remain
separate services. All writes to an aggregate projection and its event cursor are
committed in the same PostgreSQL transaction.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Optional

from executor.execution_states import LEG_TRANSITIONS, SESSION_TRANSITIONS
from executor.execution_leg_validation import validate_execution_legs
from executor.execution_serialization import (
    _canonical_json, _required_text, _same_json, _same_value,
)
from executor.risk_control import RiskControlPlane
from executor.execution_repository_fencing import (
    admission_risk_scopes, validate_coordinator_lease,
    assert_aggregate_versions, insert_session_event,
)


class ExecutionRepositoryError(RuntimeError):
    """Base error for durable execution aggregate operations."""


class AdmissionConflict(ExecutionRepositoryError):
    """An idempotency key was reused for a different admission request."""


class StaleAggregate(ExecutionRepositoryError):
    """The caller's aggregate/entity version or fencing generation is stale."""


class InvalidTransition(ExecutionRepositoryError):
    """A requested lifecycle transition is not allowed."""


class ExecutionSessionRepository:
    """PostgreSQL repository using an asyncpg-compatible pool."""

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        self.pool = pool

    _validate_legs = staticmethod(validate_execution_legs)

    async def create_session(
        self, *, session_id: str, opportunity_id: str, source_dispatch_id: str,
        admission_key: str, account_scope_id: str, pair_key: str,
        policy_snapshot: Mapping[str, Any], legs: list[Mapping[str, Any]],
        actor_id: str, fencing_generation: int,
        execution_policy_snapshot: Optional[Mapping[str, Any]] = None,
        risk_policy_snapshot: Optional[Mapping[str, Any]] = None,
        exit_policy_snapshot: Optional[Mapping[str, Any]] = None,
        coordinator_lease: Any = None, risk_metrics: Optional[Mapping[str, Any]] = None,
        risk_limits: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        """Create one durable session and its two legs, idempotently by admission key."""
        session_id = _required_text(session_id, "session_id")
        opportunity_id = _required_text(opportunity_id, "opportunity_id")
        source_dispatch_id = _required_text(source_dispatch_id, "source_dispatch_id")
        admission_key = _required_text(admission_key, "admission_key")
        account_scope_id = _required_text(account_scope_id, "account_scope_id")
        pair_key = _required_text(pair_key, "pair_key")
        actor_id = _required_text(actor_id, "actor_id")
        if not isinstance(policy_snapshot, Mapping) or not policy_snapshot:
            raise ValueError("a non-empty immutable policy_snapshot is required")
        generation = int(fencing_generation)
        if generation <= 0:
            raise ValueError("a positive, acquired fencing_generation is required")
        validated_legs = self._validate_legs(list(legs))
        policy_json = _canonical_json(policy_snapshot)
        policy_hash = hashlib.sha256(policy_json.encode("utf-8")).hexdigest()
        execution_json = _canonical_json(execution_policy_snapshot or {})
        risk_json = _canonical_json(risk_policy_snapshot or {})
        exit_json = _canonical_json(exit_policy_snapshot or {})
        event_key = f"admission:{admission_key}"
        coordinator_scope_key = (
            str(coordinator_lease.scope_key) if coordinator_lease is not None else None
        )
        coordinator_owner_id = (
            str(coordinator_lease.owner_id) if coordinator_lease is not None else None
        )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                risk_decision = None
                if coordinator_lease is not None:
                    await validate_coordinator_lease(
                        conn, coordinator_lease, session_id=session_id,
                        account_scope_id=account_scope_id,
                        expected_generation=generation,
                    )
                row = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_sessions (
                        session_id, opportunity_id, pair_key, status,
                        admission_key, source_dispatch_id, account_scope_id,
                        state_version, aggregate_version, fencing_generation,
                        strategy_snapshot, policy_snapshot, policy_snapshot_hash,
                        execution_policy_snapshot, risk_policy_snapshot,
                        exit_policy_snapshot, coordinator_scope_key, coordinator_owner_id
                    ) VALUES (
                        $1,$2,$3,'CREATED',$4,$5,$6,1,3,$7,
                        $8::jsonb,$9::jsonb,$10,$11::jsonb,$12::jsonb,$13::jsonb,$14,$15
                    )
                    ON CONFLICT (admission_key) WHERE admission_key IS NOT NULL
                    DO NOTHING
                    RETURNING session_id, state_version, aggregate_version
                    """,
                    session_id, opportunity_id, pair_key, admission_key,
                    source_dispatch_id, account_scope_id, generation,
                    policy_json, policy_json, policy_hash,
                    execution_json, risk_json, exit_json,
                    coordinator_scope_key, coordinator_owner_id,
                )
                if row is None:
                    existing = await conn.fetchrow(
                        """
                        SELECT session_id, opportunity_id, source_dispatch_id,
                               account_scope_id, pair_key, policy_snapshot_hash,
                               execution_policy_snapshot, risk_policy_snapshot,
                               exit_policy_snapshot, state_version, aggregate_version,
                               fencing_generation, coordinator_scope_key, coordinator_owner_id
                        FROM public.execution_sessions
                        WHERE admission_key=$1
                        FOR UPDATE
                        """,
                        admission_key,
                    )
                    existing_risk_snapshot = (
                        existing["risk_policy_snapshot"] if existing is not None else {}
                    )
                    if isinstance(existing_risk_snapshot, str):
                        existing_risk_snapshot = json.loads(existing_risk_snapshot)
                    existing_risk_snapshot = dict(existing_risk_snapshot or {})
                    existing_risk_snapshot.pop("admission_decision", None)
                    if existing is None or (
                        str(existing["opportunity_id"]) != opportunity_id
                        or str(existing["source_dispatch_id"]) != source_dispatch_id
                        or str(existing["account_scope_id"]) != account_scope_id
                        or str(existing["pair_key"]) != pair_key
                        or str(existing["policy_snapshot_hash"]) != policy_hash
                        or not _same_json(existing["execution_policy_snapshot"], execution_json)
                        or not _same_json(existing_risk_snapshot, _canonical_json(risk_policy_snapshot or {}))
                        or not _same_json(existing["exit_policy_snapshot"], exit_json)
                    ):
                        raise AdmissionConflict("admission_key is already bound to a different request")
                    existing_legs = await conn.fetch(
                        """
                        SELECT leg_type, venue, market_type, symbol, normalized_symbol,
                               base_asset, quote_asset, settlement_asset, contract_type,
                               contract_size, position_side, quantity_unit, base_per_unit,
                               order_quantity, exposure_quantity, target_qty
                        FROM public.execution_legs
                        WHERE session_id=$1
                        ORDER BY leg_type
                        """,
                        str(existing["session_id"]),
                    )
                    expected_legs = sorted(validated_legs, key=lambda item: item["leg_type"])
                    actual_legs = [dict(item) for item in existing_legs]
                    identity_fields = (
                        "leg_type", "venue", "market_type", "symbol", "normalized_symbol",
                        "base_asset", "quote_asset", "settlement_asset", "contract_type",
                        "contract_size", "position_side", "quantity_unit", "base_per_unit",
                        "order_quantity", "exposure_quantity", "target_qty",
                    )
                    if len(actual_legs) != len(expected_legs) or any(
                        any(not _same_value(actual.get(field), expected.get(field), field) for field in identity_fields)
                        for actual, expected in zip(actual_legs, expected_legs)
                    ):
                        raise AdmissionConflict("admission_key is already bound to different venue/market legs")
                    existing_generation = existing["fencing_generation"]
                    if coordinator_lease is not None and (
                        existing_generation is None or int(existing_generation) != generation
                        or str(existing["coordinator_scope_key"] or "") != coordinator_scope_key
                        or str(existing["coordinator_owner_id"] or "") != coordinator_owner_id
                    ):
                        raise StaleAggregate(
                            "existing session has a different coordinator generation; use recovery reclaim"
                        )
                    existing_aggregate_version = int(existing["aggregate_version"])
                    return {
                        "session_id": str(existing["session_id"]),
                        "created": False,
                        "aggregate_version": existing_aggregate_version,
                        "fencing_generation": int(existing["fencing_generation"] or 0),
                    }

                if coordinator_lease is not None:
                    scopes = admission_risk_scopes(
                        account_scope_id=account_scope_id, pair_key=pair_key,
                        session_id=session_id, legs=validated_legs,
                    )
                    for scope_key in sorted(scopes):
                        await conn.execute(
                            "select pg_advisory_xact_lock(hashtextextended('nafaz:risk-control:' || $1, 0))",
                            scope_key,
                        )
                    risk_decision = await RiskControlPlane.assert_allowed(
                        conn, scope_keys=scopes, action_class="ENTRY",
                        risk_metrics=risk_metrics, risk_limits=risk_limits,
                    )
                    risk_json = _canonical_json({
                        **dict(risk_policy_snapshot or {}),
                        "admission_decision": risk_decision,
                    })
                    await conn.execute(
                        "UPDATE public.execution_sessions SET risk_policy_snapshot=$2::jsonb WHERE session_id=$1",
                        session_id, risk_json,
                    )

                for leg in validated_legs:
                    await conn.execute(
                        """
                        INSERT INTO public.execution_legs (
                            leg_id, session_id, leg_type, venue, market_type, symbol,
                            normalized_symbol, base_asset, quote_asset, settlement_asset,
                            contract_type, contract_size, position_side, quantity_unit,
                            base_per_unit, order_quantity, exposure_quantity, target_qty,
                            status, state_version, account_scope_id
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                            $15,$16,$17,$18,'CREATED',1,$19
                        )
                        """,
                        leg["leg_id"], session_id, leg["leg_type"], leg["venue"],
                        leg["market_type"], leg["symbol"], leg["normalized_symbol"],
                        leg["base_asset"], leg["quote_asset"], leg["settlement_asset"],
                        leg["contract_type"], leg["contract_size"], leg["position_side"],
                        leg["quantity_unit"], leg["base_per_unit"], leg["order_quantity"],
                        leg["exposure_quantity"], leg["target_qty"], account_scope_id,
                    )
                admission_payload = {
                    "opportunity_id": opportunity_id,
                    "source_dispatch_id": source_dispatch_id,
                    "account_scope_id": account_scope_id,
                    "pair_key": pair_key,
                    "policy_snapshot_hash": policy_hash,
                    "execution_policy_snapshot": execution_json,
                    "risk_policy_snapshot": risk_json,
                    "exit_policy_snapshot": exit_json,
                }
                await insert_session_event(
                    conn, session_id=session_id, aggregate_version=1,
                    entity_type="SESSION", entity_id=session_id,
                    entity_version=1, event_type="SESSION_CREATED",
                    idempotency_key=f"{event_key}:session", actor_id=actor_id,
                    fencing_generation=generation, reason="",
                    correlation_id=source_dispatch_id, payload=admission_payload,
                )
                for aggregate_version, leg in enumerate(
                    sorted(validated_legs, key=lambda item: item["leg_type"]), start=2
                ):
                    await insert_session_event(
                        conn, session_id=session_id, aggregate_version=aggregate_version,
                        entity_type="LEG", entity_id=leg["leg_id"],
                        entity_version=1, event_type=f"{leg['leg_type']}_LEG_CREATED",
                        idempotency_key=f"{event_key}:{leg['leg_type'].lower()}",
                        actor_id=actor_id, fencing_generation=generation, reason="",
                        correlation_id=source_dispatch_id,
                        payload={"session_id": session_id, **leg},
                    )
        result = {"session_id": session_id, "created": True, "aggregate_version": 3}
        if risk_decision is not None:
            result["risk_decision"] = risk_decision
        return result

    async def _existing_event(
        self, conn: Any, *, session_id: str, idempotency_key: str,
        entity_type: str, entity_id: str, event_type: str,
        payload: Mapping[str, Any],
    ) -> Optional[dict]:
        row = await conn.fetchrow(
            """
            SELECT event_id, aggregate_version, entity_type, entity_id, event_type, payload
            FROM public.execution_session_events
            WHERE session_id=$1 AND event_idempotency_key=$2
            """,
            session_id, idempotency_key,
        )
        if row is None:
            return None
        if (
            str(row["entity_type"]) != entity_type
            or str(row["entity_id"]) != entity_id
            or str(row["event_type"]) != event_type
            or not _same_json(row["payload"], _canonical_json(payload))
        ):
            raise AdmissionConflict("event idempotency key was reused for a different transition")
        return {
            "duplicate": True,
            "event_id": str(row["event_id"]),
            "aggregate_version": int(row["aggregate_version"]),
        }

    async def transition_session(
        self, *, session_id: str, target_state: str,
        expected_state_version: int, expected_aggregate_version: int,
        fencing_generation: int, event_type: str, event_idempotency_key: str,
        actor_id: str, reason: str = "", payload: Optional[Mapping[str, Any]] = None,
        correlation_id: Optional[str] = None,
        completion_evidence: Optional[Mapping[str, bool]] = None,
        coordinator_lease: Any = None,
    ) -> dict:
        target = _required_text(target_state, "target_state").upper()
        key = _required_text(event_idempotency_key, "event_idempotency_key")
        actor = _required_text(actor_id, "actor_id")
        event_type = _required_text(event_type, "event_type")
        if target == "COMPLETED":
            required = {
                "spot_flat", "futures_flat", "orders_terminal",
                "no_unknown_submits", "fills_accounted", "obligations_zero",
                "allocations_zero", "pnl_reconciled", "no_open_incident",
            }
            if not completion_evidence or not required.issubset(completion_evidence):
                raise ValueError("COMPLETED requires the full verified-flatness evidence set")
            if not all(completion_evidence[name] is True for name in required):
                raise ValueError("COMPLETED rejected: one or more flatness gates are unverified")
        event_payload = dict(payload or {})
        if completion_evidence is not None:
            event_payload["completion_evidence"] = dict(completion_evidence)
        event_payload["target_state"] = target
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                current = await conn.fetchrow(
                    """
                    SELECT status, state_version, aggregate_version, fencing_generation, account_scope_id,
                           coordinator_scope_key, coordinator_owner_id
                    FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                if current is None:
                    raise ExecutionRepositoryError("execution session does not exist")
                if coordinator_lease is not None:
                    await validate_coordinator_lease(
                        conn, coordinator_lease, session_id=session_id,
                        account_scope_id=str(current["account_scope_id"]),
                        expected_generation=fencing_generation,
                        session_scope_key=current["coordinator_scope_key"],
                        session_owner_id=current["coordinator_owner_id"],
                    )
                duplicate = await self._existing_event(
                    conn, session_id=session_id, idempotency_key=key,
                    entity_type="SESSION", entity_id=session_id,
                    event_type=event_type, payload=event_payload,
                )
                if duplicate:
                    return duplicate
                assert_aggregate_versions(
                    current, expected_state_version, expected_aggregate_version,
                    fencing_generation,
                )
                old_state = str(current["status"]).upper()
                if target not in SESSION_TRANSITIONS.get(old_state, set()):
                    raise InvalidTransition(f"session transition {old_state}->{target} is not allowed")
                new_state_version = int(current["state_version"]) + 1
                new_aggregate_version = int(current["aggregate_version"]) + 1
                updated = await conn.fetchrow(
                    """
                    UPDATE public.execution_sessions
                    SET status=$2, state_version=$3, aggregate_version=$4, updated_at=now(),
                        completed_at=CASE WHEN $2='COMPLETED' THEN now() ELSE completed_at END
                    WHERE session_id=$1 AND state_version=$5 AND aggregate_version=$6
                      AND fencing_generation=$7
                    RETURNING state_version, aggregate_version
                    """,
                    session_id, target, new_state_version, new_aggregate_version,
                    expected_state_version, expected_aggregate_version, fencing_generation,
                )
                if updated is None:
                    raise StaleAggregate("session compare-and-swap lost")
                await insert_session_event(
                    conn, session_id=session_id, aggregate_version=new_aggregate_version,
                    entity_type="SESSION", entity_id=session_id,
                    entity_version=new_state_version, event_type=event_type,
                    idempotency_key=key, actor_id=actor,
                    fencing_generation=fencing_generation, reason=reason,
                    correlation_id=correlation_id, payload=event_payload,
                )
        return {
            "duplicate": False, "state": target,
            "state_version": new_state_version,
            "aggregate_version": new_aggregate_version,
        }

    async def transition_leg(
        self, *, session_id: str, leg_id: str, target_state: str,
        expected_leg_state_version: int, expected_aggregate_version: int,
        fencing_generation: int, event_type: str, event_idempotency_key: str,
        actor_id: str, reason: str = "", payload: Optional[Mapping[str, Any]] = None,
        correlation_id: Optional[str] = None,
        coordinator_lease: Any = None,
    ) -> dict:
        target = _required_text(target_state, "target_state").upper()
        key = _required_text(event_idempotency_key, "event_idempotency_key")
        actor = _required_text(actor_id, "actor_id")
        event_type = _required_text(event_type, "event_type")
        event_payload = dict(payload or {})
        event_payload["target_state"] = target
        if target == "CLOSED":
            required = {"verified_flat", "orders_terminal", "no_unknown_submits"}
            if not required.issubset(event_payload) or not all(event_payload[name] is True for name in required):
                raise ValueError("CLOSED requires verified flatness and terminal-order evidence")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await conn.fetchrow(
                    """
                    SELECT aggregate_version, fencing_generation, account_scope_id,
                           coordinator_scope_key, coordinator_owner_id
                    FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                if coordinator_lease is not None:
                    await validate_coordinator_lease(
                        conn, coordinator_lease, session_id=session_id,
                        account_scope_id=str(session["account_scope_id"]),
                        expected_generation=fencing_generation,
                        session_scope_key=session["coordinator_scope_key"],
                        session_owner_id=session["coordinator_owner_id"],
                    )
                duplicate = await self._existing_event(
                    conn, session_id=session_id, idempotency_key=key,
                    entity_type="LEG", entity_id=leg_id,
                    event_type=event_type, payload=event_payload,
                )
                if duplicate:
                    return duplicate
                assert_aggregate_versions(
                    session, None, expected_aggregate_version, fencing_generation,
                    entity_version_check=False,
                )
                leg = await conn.fetchrow(
                    """
                    SELECT status, state_version FROM public.execution_legs
                    WHERE session_id=$1 AND leg_id=$2 FOR UPDATE
                    """,
                    session_id, leg_id,
                )
                if leg is None:
                    raise ExecutionRepositoryError("execution leg does not exist in the session")
                if int(leg["state_version"]) != int(expected_leg_state_version):
                    raise StaleAggregate("leg state version is stale")
                old_state = str(leg["status"]).upper()
                if target not in LEG_TRANSITIONS.get(old_state, set()):
                    raise InvalidTransition(f"leg transition {old_state}->{target} is not allowed")
                new_leg_version = int(leg["state_version"]) + 1
                new_aggregate_version = int(session["aggregate_version"]) + 1
                updated_leg = await conn.fetchrow(
                    """
                    UPDATE public.execution_legs SET status=$3, state_version=$4, updated_at=now()
                    WHERE session_id=$1 AND leg_id=$2 AND state_version=$5
                    RETURNING state_version
                    """,
                    session_id, leg_id, target, new_leg_version, expected_leg_state_version,
                )
                if updated_leg is None:
                    raise StaleAggregate("leg compare-and-swap lost")
                updated_session = await conn.fetchrow(
                    """
                    UPDATE public.execution_sessions
                    SET aggregate_version=$2, updated_at=now()
                    WHERE session_id=$1 AND aggregate_version=$3
                      AND fencing_generation=$4
                    RETURNING aggregate_version
                    """,
                    session_id, new_aggregate_version, expected_aggregate_version,
                    fencing_generation,
                )
                if updated_session is None:
                    raise StaleAggregate("session aggregate compare-and-swap lost")
                await insert_session_event(
                    conn, session_id=session_id, aggregate_version=new_aggregate_version,
                    entity_type="LEG", entity_id=leg_id,
                    entity_version=new_leg_version, event_type=event_type,
                    idempotency_key=key, actor_id=actor,
                    fencing_generation=fencing_generation, reason=reason,
                    correlation_id=correlation_id, payload=event_payload,
                )
        return {
            "duplicate": False, "state": target,
            "state_version": new_leg_version,
            "aggregate_version": new_aggregate_version,
        }

