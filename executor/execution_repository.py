"""Durable PostgreSQL repository for Executor V3 session aggregates.

This module deliberately owns only session/leg admission and aggregate transitions.
Exchange mutations, fill accounting, hedge obligations and exit execution remain
separate services. All writes to an aggregate projection and its event cursor are
committed in the same PostgreSQL transaction.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional


class ExecutionRepositoryError(RuntimeError):
    """Base error for durable execution aggregate operations."""


class AdmissionConflict(ExecutionRepositoryError):
    """An idempotency key was reused for a different admission request."""


class StaleAggregate(ExecutionRepositoryError):
    """The caller's aggregate/entity version or fencing generation is stale."""


class InvalidTransition(ExecutionRepositoryError):
    """A requested lifecycle transition is not allowed."""


SESSION_TRANSITIONS = {
    "CREATED": {"VALIDATING", "REJECTED", "BLOCKED"},
    "VALIDATING": {"ADMITTED", "REJECTED", "BLOCKED"},
    "ADMITTED": {"ENTRY_IN_PROGRESS", "BLOCKED"},
    "ENTRY_IN_PROGRESS": {
        "HEDGE_PENDING", "PARTIALLY_HEDGED", "HEDGED",
        "RECONCILIATION_REQUIRED", "BLOCKED", "FAILED", "ABORTED",
    },
    "HEDGE_PENDING": {
        "PARTIALLY_HEDGED", "HEDGED", "RECONCILIATION_REQUIRED",
        "BLOCKED", "FAILED", "ABORTED",
    },
    "PARTIALLY_HEDGED": {
        "HEDGE_PENDING", "HEDGED", "EXIT_IN_PROGRESS",
        "RECONCILIATION_REQUIRED", "BLOCKED",
    },
    "HEDGED": {"EXIT_IN_PROGRESS", "RECONCILIATION_REQUIRED", "BLOCKED"},
    "EXIT_IN_PROGRESS": {
        "EXIT_PARTIAL", "COMPLETED", "RECONCILIATION_REQUIRED", "BLOCKED",
    },
    "EXIT_PARTIAL": {
        "EXIT_IN_PROGRESS", "COMPLETED", "RECONCILIATION_REQUIRED", "BLOCKED",
    },
    "RECONCILIATION_REQUIRED": {"RECOVERY_IN_PROGRESS", "BLOCKED"},
    "RECOVERY_IN_PROGRESS": {
        "ENTRY_IN_PROGRESS", "HEDGE_PENDING", "PARTIALLY_HEDGED", "HEDGED",
        "EXIT_IN_PROGRESS", "EXIT_PARTIAL", "COMPLETED", "BLOCKED",
    },
    "BLOCKED": {
        "VALIDATING", "ENTRY_IN_PROGRESS", "HEDGE_PENDING",
        "EXIT_IN_PROGRESS", "RECONCILIATION_REQUIRED",
    },
    "FAILED": {"RECONCILIATION_REQUIRED", "RECOVERY_IN_PROGRESS"},
    "ABORTED": {"RECONCILIATION_REQUIRED", "RECOVERY_IN_PROGRESS"},
}
LEG_TRANSITIONS = {
    "CREATED": {"PREPARED", "REJECTED", "UNKNOWN"},
    "NOT_STARTED": {"PREPARED", "REJECTED", "UNKNOWN"},
    "PREPARED": {"SUBMITTING", "REJECTED", "UNKNOWN"},
    "SUBMITTING": {
        "OPEN", "PARTIALLY_FILLED", "FILLED", "REJECTED",
        "SUBMISSION_UNKNOWN", "UNKNOWN",
    },
    "SUBMISSION_UNKNOWN": {"OPEN", "PARTIALLY_FILLED", "FILLED", "REJECTED", "UNKNOWN"},
    "OPEN": {"PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "CLOSING", "UNKNOWN"},
    "PARTIALLY_FILLED": {
        "FILLED", "CANCEL_PENDING", "CLOSING", "UNKNOWN",
        "RECONCILIATION_REQUIRED",
    },
    "FILLED": {"CLOSING", "CLOSED", "RECONCILIATION_REQUIRED"},
    "CANCEL_PENDING": {
        "CANCELED", "PARTIALLY_FILLED", "FILLED", "UNKNOWN",
        "RECONCILIATION_REQUIRED",
    },
    "CANCELED": {"CLOSING", "CLOSED", "RECONCILIATION_REQUIRED"},
    "CLOSING": {"PARTIALLY_FILLED", "CLOSED", "UNKNOWN", "RECONCILIATION_REQUIRED"},
    "UNKNOWN": {"OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED", "CLOSING", "CLOSED"},
    "RECONCILIATION_REQUIRED": {
        "OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED", "CLOSING", "CLOSED",
    },
}


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _positive_decimal(value: Any, field: str, *, allow_zero: bool = False) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not number.is_finite() or (number < 0 if allow_zero else number <= 0):
        comparator = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {comparator}")
    return format(number, "f")


def _required_text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


def _same_json(left: Any, right: str) -> bool:
    if isinstance(left, str):
        try:
            left = json.loads(left)
        except (TypeError, ValueError):
            return False
    return left == json.loads(right)


class ExecutionSessionRepository:
    """PostgreSQL repository using an asyncpg-compatible pool."""

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        self.pool = pool

    @staticmethod
    def _validate_legs(legs: list[Mapping[str, Any]]) -> list[dict]:
        if len(legs) != 2 or {str(x.get("leg_type", "")).upper() for x in legs} != {"SPOT", "FUTURES"}:
            raise ValueError("a new session requires exactly one SPOT and one FUTURES leg")
        result = []
        for raw in legs:
            leg = dict(raw)
            leg_type = str(leg.get("leg_type", "")).upper()
            quantity_unit = _required_text(leg.get("quantity_unit"), "leg.quantity_unit").upper()
            if quantity_unit not in {"BASE", "QUOTE", "CONTRACTS"}:
                raise ValueError("leg.quantity_unit must be BASE, QUOTE or CONTRACTS")
            base_per_unit = (
                _positive_decimal(leg["base_per_unit"], "leg.base_per_unit")
                if leg.get("base_per_unit") is not None else None
            )
            if quantity_unit in {"BASE", "CONTRACTS"} and base_per_unit is None:
                raise ValueError("BASE/CONTRACTS legs require explicit base_per_unit conversion metadata")
            if leg_type == "FUTURES":
                _required_text(leg.get("settlement_asset"), "Futures settlement_asset")
                _positive_decimal(leg.get("contract_size"), "Futures contract_size")
            result.append({
                "leg_id": _required_text(leg.get("leg_id"), "leg.leg_id"),
                "leg_type": leg_type,
                "venue": _required_text(leg.get("venue"), "leg.venue").lower(),
                "market_type": _required_text(leg.get("market_type"), "leg.market_type").lower(),
                "symbol": _required_text(leg.get("symbol"), "leg.symbol"),
                "normalized_symbol": _required_text(leg.get("normalized_symbol"), "leg.normalized_symbol"),
                "base_asset": _required_text(leg.get("base_asset"), "leg.base_asset").upper(),
                "quote_asset": _required_text(leg.get("quote_asset"), "leg.quote_asset").upper(),
                "settlement_asset": str(leg.get("settlement_asset") or "").upper() or None,
                "contract_type": str(leg.get("contract_type") or "") or None,
                "contract_size": (
                    _positive_decimal(leg["contract_size"], "leg.contract_size")
                    if leg.get("contract_size") is not None else None
                ),
                "position_side": str(leg.get("position_side") or "").upper() or None,
                "quantity_unit": quantity_unit,
                "base_per_unit": base_per_unit,
                "order_quantity": (
                    _positive_decimal(leg["order_quantity"], "leg.order_quantity", allow_zero=True)
                    if leg.get("order_quantity") is not None else None
                ),
                "exposure_quantity": (
                    _positive_decimal(leg["exposure_quantity"], "leg.exposure_quantity", allow_zero=True)
                    if leg.get("exposure_quantity") is not None else None
                ),
                "target_qty": _positive_decimal(
                    leg.get("target_qty", 0), "leg.target_qty", allow_zero=True
                ),
            })
        return result

    async def create_session(
        self, *, session_id: str, opportunity_id: str, source_dispatch_id: str,
        admission_key: str, account_scope_id: str, pair_key: str,
        policy_snapshot: Mapping[str, Any], legs: list[Mapping[str, Any]],
        actor_id: str, fencing_generation: int,
        execution_policy_snapshot: Optional[Mapping[str, Any]] = None,
        risk_policy_snapshot: Optional[Mapping[str, Any]] = None,
        exit_policy_snapshot: Optional[Mapping[str, Any]] = None,
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

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_sessions (
                        session_id, opportunity_id, pair_key, status,
                        admission_key, source_dispatch_id, account_scope_id,
                        state_version, aggregate_version, fencing_generation,
                        strategy_snapshot, policy_snapshot, policy_snapshot_hash,
                        execution_policy_snapshot, risk_policy_snapshot,
                        exit_policy_snapshot
                    ) VALUES (
                        $1,$2,$3,'CREATED',$4,$5,$6,1,1,$7,
                        $8::jsonb,$9::jsonb,$10,$11::jsonb,$12::jsonb,$13::jsonb
                    )
                    ON CONFLICT (admission_key) WHERE admission_key IS NOT NULL
                    DO NOTHING
                    RETURNING session_id, state_version, aggregate_version
                    """,
                    session_id, opportunity_id, pair_key, admission_key,
                    source_dispatch_id, account_scope_id, generation,
                    policy_json, policy_json, policy_hash,
                    execution_json, risk_json, exit_json,
                )
                if row is None:
                    existing = await conn.fetchrow(
                        """
                        SELECT session_id, opportunity_id, source_dispatch_id,
                               account_scope_id, policy_snapshot_hash,
                               execution_policy_snapshot, risk_policy_snapshot,
                               exit_policy_snapshot
                        FROM public.execution_sessions
                        WHERE admission_key=$1
                        FOR UPDATE
                        """,
                        admission_key,
                    )
                    if existing is None or (
                        str(existing["opportunity_id"]) != opportunity_id
                        or str(existing["source_dispatch_id"]) != source_dispatch_id
                        or str(existing["account_scope_id"]) != account_scope_id
                        or str(existing["policy_snapshot_hash"]) != policy_hash
                        or not _same_json(existing["execution_policy_snapshot"], execution_json)
                        or not _same_json(existing["risk_policy_snapshot"], risk_json)
                        or not _same_json(existing["exit_policy_snapshot"], exit_json)
                    ):
                        raise AdmissionConflict("admission_key is already bound to a different request")
                    existing_legs = await conn.fetch(
                        """
                        SELECT leg_type, venue, market_type, symbol, normalized_symbol,
                               base_asset, quote_asset, settlement_asset, contract_type,
                               contract_size, position_side, quantity_unit, base_per_unit
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
                    )
                    if len(actual_legs) != len(expected_legs) or any(
                        any(str(actual.get(field) or "") != str(expected.get(field) or "") for field in identity_fields)
                        for actual, expected in zip(actual_legs, expected_legs)
                    ):
                        raise AdmissionConflict("admission_key is already bound to different venue/market legs")
                    return {
                        "session_id": str(existing["session_id"]),
                        "created": False,
                        "aggregate_version": None,
                    }

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
                await conn.execute(
                    """
                    INSERT INTO public.execution_session_events (
                        session_id, aggregate_version, entity_type, entity_id,
                        entity_version, event_type, event_idempotency_key,
                        actor_id, fencing_generation, payload
                    ) VALUES ($1,1,'SESSION',$1,1,'SESSION_CREATED',$2,$3,$4,$5::jsonb)
                    """,
                    session_id, event_key, actor_id, generation,
                    _canonical_json({
                        "opportunity_id": opportunity_id,
                        "source_dispatch_id": source_dispatch_id,
                        "account_scope_id": account_scope_id,
                        "policy_snapshot_hash": policy_hash,
                    }),
                )
        return {"session_id": session_id, "created": True, "aggregate_version": 1}

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
                duplicate = await self._existing_event(
                    conn, session_id=session_id, idempotency_key=key,
                    entity_type="SESSION", entity_id=session_id,
                    event_type=event_type, payload=event_payload,
                )
                if duplicate:
                    return duplicate
                current = await conn.fetchrow(
                    """
                    SELECT status, state_version, aggregate_version, fencing_generation
                    FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                self._assert_versions(
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
                await self._insert_event(
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
                duplicate = await self._existing_event(
                    conn, session_id=session_id, idempotency_key=key,
                    entity_type="LEG", entity_id=leg_id,
                    event_type=event_type, payload=event_payload,
                )
                if duplicate:
                    return duplicate
                session = await conn.fetchrow(
                    """
                    SELECT aggregate_version, fencing_generation
                    FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                self._assert_versions(
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
                await self._insert_event(
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

    @staticmethod
    def _assert_versions(
        row: Any, expected_state_version: Optional[int],
        expected_aggregate_version: int, fencing_generation: int,
        *, entity_version_check: bool = True,
    ) -> None:
        if row is None:
            raise ExecutionRepositoryError("execution session does not exist")
        if int(row["aggregate_version"]) != int(expected_aggregate_version):
            raise StaleAggregate("session aggregate version is stale")
        if entity_version_check and int(row["state_version"]) != int(expected_state_version):
            raise StaleAggregate("session state version is stale")
        current_fence = row["fencing_generation"]
        if current_fence is None or int(current_fence) != int(fencing_generation):
            raise StaleAggregate("ownership fencing generation is missing or stale")

    @staticmethod
    async def _insert_event(
        conn: Any, *, session_id: str, aggregate_version: int,
        entity_type: str, entity_id: str, entity_version: int,
        event_type: str, idempotency_key: str, actor_id: str,
        fencing_generation: int, reason: str,
        correlation_id: Optional[str], payload: Mapping[str, Any],
    ) -> None:
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
