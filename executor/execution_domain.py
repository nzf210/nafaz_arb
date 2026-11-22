"""Transactional PostgreSQL repository for the NAFAZ Executor V3 domain.

This module owns only durable session/leg projections and their transition
journal/outbox. Venue order submission, fills, hedge sizing and exit policy
remain in their existing domain services until their dedicated migration PRs.
"""
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional, Sequence

from executor.execution_domain_models import (
    ExecutionAdmissionConflict,
    ExecutionDomainError,
    ExecutionIdempotencyConflict,
    ExecutionLegNotFound,
    ExecutionSessionNotFound,
    InvalidExecutionTransition,
    SessionCreationResult,
    StaleExecutionFence,
    StaleExecutionVersion,
    TransitionResult,
    _LEG_TRANSITIONS,
    _SESSION_TRANSITIONS,
    _positive_decimal,
    _validate_transition_evidence,
    canonical_policy_hash,
)


class ExecutionDomainRepository:
    """PostgreSQL transaction boundary for session/leg state and event outbox."""

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a live asyncpg-compatible pool is required")
        self.pool = pool

    async def create_session(
        self,
        *,
        session_id: str,
        admission_key: str,
        account_scope_id: str,
        source_dispatch_id: str,
        pair_key: str,
        opportunity_id: str,
        policy_snapshot: Mapping[str, Any],
        fencing_generation: int,
        legs: Sequence[Mapping[str, Any]],
        actor_id: str = "",
        worker_id: str = "",
        correlation_id: str = "",
    ) -> SessionCreationResult:
        """Create one V3 session and its Spot/Futures legs with initial events."""
        if not all((session_id, admission_key, account_scope_id, source_dispatch_id, pair_key)):
            raise ValueError("session_id, admission_key, account_scope_id, source_dispatch_id and pair_key are required")
        if fencing_generation <= 0:
            raise StaleExecutionFence("new V3 sessions require a positive fencing generation")
        if not isinstance(policy_snapshot, Mapping) or not policy_snapshot:
            raise ValueError("a non-empty immutable policy_snapshot is required")
        if len(legs) != 2 or {str(x.get("leg_type", "")).upper() for x in legs} != {"SPOT", "FUTURES"}:
            raise ValueError("exactly one SPOT and one FUTURES leg are required")

        policy = dict(policy_snapshot)
        policy_hash = canonical_policy_hash(policy)
        prepared_legs = []
        for raw in legs:
            leg = dict(raw)
            kind = str(leg.get("leg_type", "")).upper()
            leg["leg_type"] = kind
            leg["venue"] = str(leg.get("venue") or "").lower()
            leg["market_type"] = str(leg.get("market_type") or "").lower()
            leg["base_asset"] = str(leg.get("base_asset") or "").upper()
            leg["quote_asset"] = str(leg.get("quote_asset") or "").upper()
            leg["settle_asset"] = str(leg.get("settle_asset") or "").upper() or None
            leg["contract_type"] = str(leg.get("contract_type") or "").lower() or None
            leg["position_side"] = str(leg.get("position_side") or "").upper() or None
            leg["quantity_unit"] = str(leg.get("quantity_unit") or "").upper()
            required = (
                "leg_id", "venue", "market_type", "symbol", "native_symbol",
                "normalized_symbol", "base_asset", "quote_asset",
                "quantity_unit", "target_order_qty", "target_base_qty",
            )
            missing = [key for key in required if leg.get(key) in (None, "")]
            if missing:
                raise ValueError(f"{kind} leg missing required fields: {', '.join(missing)}")
            leg["target_order_qty"] = _positive_decimal(leg["target_order_qty"], f"{kind}.target_order_qty")
            leg["target_base_qty"] = _positive_decimal(leg["target_base_qty"], f"{kind}.target_base_qty")
            if kind == "SPOT" and leg["market_type"] != "spot":
                raise ValueError("SPOT leg market_type must be spot")
            if kind == "FUTURES":
                if leg["market_type"] == "spot":
                    raise ValueError("FUTURES leg market_type cannot be spot")
                if not leg["settle_asset"]:
                    raise ValueError("FUTURES leg requires an explicit settle_asset")
                if leg["contract_type"] not in ("linear", "inverse"):
                    raise ValueError("FUTURES leg contract_type must be linear or inverse")
                if leg["position_side"] not in ("LONG", "SHORT"):
                    raise ValueError("FUTURES leg position_side must be LONG or SHORT")
                leg["contract_size"] = _positive_decimal(leg.get("contract_size"), "FUTURES.contract_size")
            prepared_legs.append(leg)

        if prepared_legs[0]["base_asset"] != prepared_legs[1]["base_asset"]:
            raise ValueError("Spot and Futures legs must reference the same base asset")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_sessions (
                        session_id, opportunity_id, pair_key, status,
                        admission_key, account_scope_id, source_dispatch_id,
                        state_version, aggregate_version, fencing_generation,
                        policy_snapshot, policy_snapshot_hash
                    ) VALUES ($1,$2,$3,'CREATED',$4,$5,$6,1,3,$7,$8::jsonb,$9)
                    ON CONFLICT (admission_key) WHERE admission_key IS NOT NULL
                    DO NOTHING
                    RETURNING session_id
                    """,
                    session_id, opportunity_id or None, pair_key, admission_key,
                    account_scope_id, source_dispatch_id or None,
                    fencing_generation,
                    json.dumps(policy, sort_keys=True, separators=(",", ":"), allow_nan=False),
                    policy_hash,
                )
                if inserted is None:
                    existing = await conn.fetchrow(
                        """
                        SELECT session_id, account_scope_id, source_dispatch_id, opportunity_id,
                               pair_key, policy_snapshot_hash, aggregate_version, fencing_generation
                        FROM public.execution_sessions
                        WHERE admission_key = $1
                        FOR UPDATE
                        """,
                        admission_key,
                    )
                    if existing is None:
                        raise ExecutionAdmissionConflict("admission key conflict could not be resolved")
                    if (
                        existing["account_scope_id"] != account_scope_id
                        or existing["source_dispatch_id"] != (source_dispatch_id or None)
                        or existing["opportunity_id"] != (opportunity_id or None)
                        or existing["pair_key"] != pair_key
                        or existing["policy_snapshot_hash"] != policy_hash
                        or existing["fencing_generation"] is None
                        or int(existing["fencing_generation"]) != fencing_generation
                    ):
                        raise ExecutionAdmissionConflict(
                            "admission key was already used for a different account, pair or policy"
                        )
                    existing_legs = await conn.fetch(
                        """
                        SELECT leg_type, venue, market_type, symbol, native_symbol,
                               normalized_symbol, base_asset, quote_asset, settle_asset,
                               contract_type, contract_size, position_side, quantity_unit,
                               target_order_qty, target_base_qty, leg_id
                        FROM public.execution_legs
                        WHERE session_id = $1 ORDER BY leg_type
                        """,
                        existing["session_id"],
                    )
                    if len(existing_legs) != 2:
                        raise ExecutionAdmissionConflict("existing admission is incomplete; manual reconciliation required")
                    requested_by_type = {str(item["leg_type"]).upper(): item for item in prepared_legs}
                    for stored in existing_legs:
                        requested = requested_by_type.get(str(stored["leg_type"]).upper())
                        if requested is None:
                            raise ExecutionAdmissionConflict("existing admission leg types differ")
                        for field in (
                            "venue", "market_type", "symbol", "native_symbol",
                            "normalized_symbol", "base_asset", "quote_asset",
                            "settle_asset", "contract_type", "position_side", "quantity_unit",
                        ):
                            stored_value = stored[field]
                            requested_value = requested.get(field)
                            if stored_value != requested_value:
                                raise ExecutionAdmissionConflict(f"admission retry changed leg field {field}")
                        try:
                            if Decimal(str(stored["target_order_qty"])) != Decimal(str(requested["target_order_qty"])):
                                raise ExecutionAdmissionConflict("admission retry changed target_order_qty")
                            if Decimal(str(stored["target_base_qty"])) != Decimal(str(requested["target_base_qty"])):
                                raise ExecutionAdmissionConflict("admission retry changed target_base_qty")
                            stored_contract = stored["contract_size"]
                            requested_contract = requested.get("contract_size")
                            if (stored_contract is None) != (requested_contract is None):
                                raise ExecutionAdmissionConflict("admission retry changed contract_size")
                            if stored_contract is not None and Decimal(str(stored_contract)) != Decimal(str(requested_contract)):
                                raise ExecutionAdmissionConflict("admission retry changed contract_size")
                        except (InvalidOperation, TypeError) as exc:
                            raise ExecutionAdmissionConflict("admission retry contains invalid decimal metadata") from exc
                    return SessionCreationResult(
                        session_id=existing["session_id"], admission_key=admission_key,
                        policy_snapshot_hash=policy_hash,
                        aggregate_version=int(existing["aggregate_version"]),
                        leg_ids=tuple(row["leg_id"] for row in existing_legs),
                        duplicate=True,
                    )

                event_specs = [
                    ("SESSION", session_id, 1, None, "CREATED", "SESSION_CREATED",
                     {"policy_snapshot_hash": policy_hash}),
                ]
                for leg in prepared_legs:
                    kind = str(leg["leg_type"]).upper()
                    await conn.execute(
                        """
                        INSERT INTO public.execution_legs (
                            leg_id, session_id, leg_type, venue, market_type, symbol,
                            status, state_version, account_scope_id, native_symbol,
                            normalized_symbol, base_asset, quote_asset, settle_asset,
                            contract_type, contract_size, position_side, quantity_unit,
                            target_order_qty, filled_order_qty, remaining_order_qty,
                            target_base_qty, filled_base_qty, verified_exposure_base_qty
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,'NOT_STARTED',1,$7,$8,$9,$10,$11,$12,
                            $13,$14,$15,$16,$17,0,$17,$18,0,0
                        )
                        """,
                        str(leg["leg_id"]), session_id, kind, str(leg["venue"]).lower(),
                        str(leg["market_type"]).lower(), str(leg["symbol"]),
                        account_scope_id, str(leg["native_symbol"]),
                        str(leg["normalized_symbol"]), str(leg["base_asset"]).upper(),
                        str(leg["quote_asset"]).upper(),
                        str(leg.get("settle_asset") or "").upper() or None,
                        str(leg.get("contract_type") or "").lower() or None,
                        leg.get("contract_size"), str(leg.get("position_side") or "").upper() or None,
                        str(leg["quantity_unit"]).upper(), leg["target_order_qty"],
                        leg["target_base_qty"],
                    )
                    event_specs.append((
                        "LEG", str(leg["leg_id"]), len(event_specs) + 1,
                        None, "NOT_STARTED", f"{kind}_LEG_CREATED",
                        {
                            "venue": str(leg["venue"]).lower(),
                            "market_type": str(leg["market_type"]).lower(),
                            "native_symbol": str(leg["native_symbol"]),
                            "normalized_symbol": str(leg["normalized_symbol"]),
                            "quantity_unit": str(leg["quantity_unit"]).upper(),
                        },
                    ))

                for entity_type, entity_id, aggregate_version, from_state, to_state, event_type, payload in event_specs:
                    event = await conn.fetchrow(
                        """
                        INSERT INTO public.execution_session_events (
                            session_id, aggregate_version, entity_type, entity_id,
                            entity_version, from_state, to_state, event_type,
                            actor_id, worker_id, fencing_generation, correlation_id,
                            idempotency_key, event_payload
                        ) VALUES ($1,$2,$3,$4,1,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)
                        RETURNING event_id
                        """,
                        session_id, aggregate_version, entity_type, entity_id,
                        from_state, to_state, event_type, actor_id or None,
                        worker_id or None, fencing_generation, correlation_id or None,
                        f"{admission_key}:{entity_type}:{entity_id}",
                        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
                    )
                    await conn.execute(
                        """
                        INSERT INTO public.execution_outbox (
                            event_id, session_id, aggregate_version, topic, payload
                        ) VALUES ($1,$2,$3,$4,$5::jsonb)
                        """,
                        event["event_id"], session_id, aggregate_version,
                        "execution.session.created",
                        json.dumps({
                            "event_id": str(event["event_id"]),
                            "session_id": session_id,
                            "aggregate_version": aggregate_version,
                            "entity_type": entity_type,
                            "entity_id": entity_id,
                            "to_state": to_state,
                            "event_type": event_type,
                        }, sort_keys=True, separators=(",", ":")),
                    )

                return SessionCreationResult(
                    session_id=session_id, admission_key=admission_key,
                    policy_snapshot_hash=policy_hash, aggregate_version=3,
                    leg_ids=tuple(str(x["leg_id"]) for x in prepared_legs),
                )

    async def transition(
        self,
        *,
        session_id: str,
        entity_type: str,
        entity_id: str,
        expected_state: str,
        next_state: str,
        expected_entity_version: int,
        expected_aggregate_version: int,
        fencing_generation: int,
        event_type: str,
        idempotency_key: str,
        actor_id: str = "",
        worker_id: str = "",
        correlation_id: str = "",
        reason: str = "",
        evidence: Optional[Mapping[str, Any]] = None,
    ) -> TransitionResult:
        kind = str(entity_type).upper()
        expected_state = str(expected_state).upper()
        next_state = str(next_state).upper()
        event_type = str(event_type or "").strip()
        if kind not in ("SESSION", "LEG"):
            raise ValueError("entity_type must be SESSION or LEG")
        if not session_id or not entity_id:
            raise ValueError("session_id and entity_id are required")
        if not idempotency_key:
            raise ValueError("idempotency_key is required")
        if not event_type:
            raise ValueError("event_type is required")
        if fencing_generation <= 0:
            raise StaleExecutionFence("V3 transitions require an acquired positive fencing generation")
        transitions = _SESSION_TRANSITIONS if kind == "SESSION" else _LEG_TRANSITIONS
        if next_state not in transitions.get(expected_state, set()):
            raise InvalidExecutionTransition(f"transition {expected_state} -> {next_state} is not allowed")

        payload = dict(evidence or {})
        _validate_transition_evidence(kind, next_state, payload)
        if reason:
            payload["reason"] = reason

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await conn.fetchrow(
                    """
                    SELECT session_id, status, state_version, aggregate_version,
                           fencing_generation
                    FROM public.execution_sessions
                    WHERE session_id = $1
                    FOR UPDATE
                    """,
                    session_id,
                )
                if session is None:
                    raise ExecutionSessionNotFound(session_id)
                if session["fencing_generation"] is None or int(session["fencing_generation"]) != fencing_generation:
                    raise StaleExecutionFence("session fencing generation is absent or no longer current")

                prior = await conn.fetchrow(
                    """
                    SELECT event_id, entity_type, entity_id, from_state, to_state,
                           event_type, entity_version, aggregate_version
                    FROM public.execution_session_events
                    WHERE session_id = $1 AND idempotency_key = $2
                    """,
                    session_id, idempotency_key,
                )
                if prior is not None:
                    if (
                        prior["entity_type"] != kind
                        or prior["entity_id"] != entity_id
                        or prior["from_state"] != expected_state
                        or prior["to_state"] != next_state
                        or prior["event_type"] != event_type
                    ):
                        raise ExecutionIdempotencyConflict(idempotency_key)
                    return TransitionResult(
                        session_id=session_id, entity_type=kind, entity_id=entity_id,
                        from_state=str(prior["from_state"] or ""),
                        to_state=prior["to_state"],
                        entity_version=int(prior["entity_version"]),
                        aggregate_version=int(prior["aggregate_version"]),
                        event_id=str(prior["event_id"]), duplicate=True,
                    )

                if int(session["aggregate_version"]) != expected_aggregate_version:
                    raise StaleExecutionVersion(
                        f"aggregate version {session['aggregate_version']} != {expected_aggregate_version}"
                    )

                if kind == "SESSION":
                    if entity_id != session_id:
                        raise ValueError("SESSION entity_id must equal session_id")
                    current_state = str(session["status"])
                    current_version = int(session["state_version"])
                    if current_state != expected_state or current_version != expected_entity_version:
                        raise StaleExecutionVersion("session state/version compare-and-swap failed")
                    updated = await conn.fetchrow(
                        """
                        UPDATE public.execution_sessions
                        SET status = $1, state_version = state_version + 1,
                            aggregate_version = aggregate_version + 1,
                            state_reason = $2, updated_at = now()
                        WHERE session_id = $3 AND status = $4
                          AND state_version = $5 AND aggregate_version = $6
                          AND fencing_generation = $7
                        RETURNING state_version, aggregate_version
                        """,
                        next_state, reason or None, session_id, expected_state,
                        expected_entity_version, expected_aggregate_version,
                        fencing_generation,
                    )
                else:
                    leg = await conn.fetchrow(
                        """
                        SELECT leg_id, status, state_version
                        FROM public.execution_legs
                        WHERE session_id = $1 AND leg_id = $2
                        FOR UPDATE
                        """,
                        session_id, entity_id,
                    )
                    if leg is None:
                        raise ExecutionLegNotFound(entity_id)
                    if str(leg["status"]) != expected_state or int(leg["state_version"]) != expected_entity_version:
                        raise StaleExecutionVersion("leg state/version compare-and-swap failed")
                    updated = await conn.fetchrow(
                        """
                        UPDATE public.execution_legs
                        SET status = $1, state_version = state_version + 1,
                            state_reason = $2, updated_at = now()
                        WHERE session_id = $3 AND leg_id = $4 AND status = $5
                          AND state_version = $6
                        RETURNING state_version
                        """,
                        next_state, reason or None, session_id, entity_id,
                        expected_state, expected_entity_version,
                    )
                    if updated is not None:
                        parent = await conn.fetchrow(
                            """
                            UPDATE public.execution_sessions
                            SET aggregate_version = aggregate_version + 1,
                                updated_at = now()
                            WHERE session_id = $1 AND aggregate_version = $2
                              AND fencing_generation = $3
                            RETURNING aggregate_version
                            """,
                            session_id, expected_aggregate_version, fencing_generation,
                        )
                        if parent is None:
                            raise StaleExecutionVersion("parent aggregate compare-and-swap failed")
                        updated = {**dict(updated), "aggregate_version": parent["aggregate_version"]}

                if updated is None:
                    raise StaleExecutionVersion("state compare-and-swap failed")

                new_entity_version = int(updated["state_version"])
                new_aggregate_version = int(updated["aggregate_version"])
                event = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_session_events (
                        session_id, aggregate_version, entity_type, entity_id,
                        entity_version, from_state, to_state, event_type,
                        actor_id, worker_id, fencing_generation, correlation_id,
                        idempotency_key, event_payload
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb
                    )
                    RETURNING event_id
                    """,
                    session_id, new_aggregate_version, kind, entity_id,
                    new_entity_version, expected_state, next_state, event_type,
                    actor_id or None, worker_id or None, fencing_generation,
                    correlation_id or None, idempotency_key,
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
                )
                outbox_payload = {
                    "event_id": str(event["event_id"]),
                    "session_id": session_id,
                    "aggregate_version": new_aggregate_version,
                    "entity_type": kind,
                    "entity_id": entity_id,
                    "entity_version": new_entity_version,
                    "from_state": expected_state,
                    "to_state": next_state,
                    "event_type": event_type,
                    "correlation_id": correlation_id or None,
                }
                await conn.execute(
                    """
                    INSERT INTO public.execution_outbox (
                        event_id, session_id, aggregate_version, topic, payload
                    ) VALUES ($1,$2,$3,$4,$5::jsonb)
                    ON CONFLICT (event_id, topic) DO NOTHING
                    """,
                    event["event_id"], session_id, new_aggregate_version,
                    "execution.session.transition",
                    json.dumps(outbox_payload, sort_keys=True, separators=(",", ":")),
                )
                return TransitionResult(
                    session_id=session_id, entity_type=kind, entity_id=entity_id,
                    from_state=expected_state, to_state=next_state,
                    entity_version=new_entity_version,
                    aggregate_version=new_aggregate_version,
                    event_id=str(event["event_id"]),
                )
