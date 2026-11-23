"""Prepare the initial Spot entry intent for a V3 session.

The Futures leg is deliberately not submitted as a parallel entry order.
Confirmed Spot fills create durable hedge obligations; later hedge orders are
sized from those obligations and routed through reservations and the gateway.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from executor.execution_repository_fencing import validate_coordinator_lease
from executor.order_intent_service import OrderIntentService


class SessionEntryIntentError(RuntimeError):
    """The session cannot safely prepare its initial Spot entry intent."""


def _positive_decimal(value: Any, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise SessionEntryIntentError(f"{field} must be a finite positive decimal") from exc
    if not number.is_finite() or number <= 0:
        raise SessionEntryIntentError(f"{field} must be a finite positive decimal")
    return number


def _spot_spec(
    legs: list[Mapping[str, Any]], specs: list[Mapping[str, Any]],
) -> tuple[dict, dict]:
    if not isinstance(specs, list) or len(specs) != 1:
        raise SessionEntryIntentError(
            "exactly one initial Spot entry intent specification is required; Futures is hedged from fills"
        )
    spot_legs = [leg for leg in legs if str(leg.get("leg_type") or "").upper() == "SPOT"]
    if len(spot_legs) != 1:
        raise SessionEntryIntentError("session must contain exactly one SPOT leg")
    raw = specs[0]
    if not isinstance(raw, Mapping):
        raise SessionEntryIntentError("Spot entry intent specification must be a mapping")
    leg = dict(spot_legs[0])
    spec = dict(raw)
    if str(spec.get("leg_id") or "").strip() != str(leg["leg_id"]):
        raise SessionEntryIntentError("initial entry intent must map to the session SPOT leg only")
    return leg, spec


class SessionEntryIntentCoordinator:
    """Idempotently prepare only the Spot entry intent for one admitted session."""

    def __init__(
        self, pool: Any, *,
        order_intents: OrderIntentService | None = None,
        repository: Any = None,
    ) -> None:
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL pool is required")
        self.pool = pool
        self.order_intents = order_intents or OrderIntentService(pool)
        self.repository = repository

    async def prepare_entry(
        self, *, session_id: str, coordinator_lease: Any,
        specs: list[Mapping[str, Any]], actor_id: str,
    ) -> dict:
        actor = str(actor_id or "").strip()
        if not actor or coordinator_lease is None:
            raise SessionEntryIntentError("actor_id and a live coordinator lease are required")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await conn.fetchrow(
                    """
                    SELECT session_id, status, state_version, aggregate_version,
                           account_scope_id, pair_key, fencing_generation,
                           coordinator_scope_key, coordinator_owner_id
                    FROM public.execution_sessions
                    WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                if session is None:
                    raise SessionEntryIntentError("execution session does not exist")
                state = str(session["status"]).upper()
                if state not in {"ADMITTED", "ENTRY_IN_PROGRESS"}:
                    raise SessionEntryIntentError(
                        f"session state {state} cannot prepare Spot entry intent"
                    )
                generation = int(session["fencing_generation"] or 0)
                await validate_coordinator_lease(
                    conn, coordinator_lease, session_id=session_id,
                    account_scope_id=str(session["account_scope_id"]),
                    expected_generation=generation,
                    session_scope_key=session["coordinator_scope_key"],
                    session_owner_id=session["coordinator_owner_id"],
                )
                rows = await conn.fetch(
                    """
                    SELECT leg_id, leg_type, status, state_version, venue,
                           market_type, symbol, base_asset, quote_asset,
                           settlement_asset, contract_type, contract_size,
                           position_side, quantity_unit, base_per_unit,
                           order_quantity
                    FROM public.execution_legs
                    WHERE session_id=$1
                    ORDER BY leg_type
                    FOR UPDATE
                    """,
                    session_id,
                )
                legs = [dict(row) for row in rows]
                if len(legs) != 2 or {str(x["leg_type"]).upper() for x in legs} != {"SPOT", "FUTURES"}:
                    raise SessionEntryIntentError("session must contain exactly one SPOT and one FUTURES leg")
                spot_leg, spec = _spot_spec(legs, specs)
                snapshot = {
                    "state": state,
                    "state_version": int(session["state_version"]),
                    "aggregate_version": int(session["aggregate_version"]),
                    "account_scope_id": str(session["account_scope_id"]),
                    "pair_key": str(session["pair_key"]),
                    "generation": generation,
                    "spot_leg": spot_leg,
                    "spec": spec,
                }

        leg_id = str(snapshot["spot_leg"]["leg_id"])
        leg = snapshot["spot_leg"]
        side = str(spec.get("side") or "").strip().lower()
        if side != "buy":
            raise SessionEntryIntentError("initial Spot entry side must be buy")
        if str(leg.get("quantity_unit") or "").upper() != "BASE":
            raise SessionEntryIntentError("initial Spot entry quantity must use BASE units")
        quantity = _positive_decimal(spec.get("quantity"), "SPOT.quantity")
        expected_quantity = _positive_decimal(leg.get("order_quantity"), "SPOT.order_quantity")
        if quantity != expected_quantity:
            raise SessionEntryIntentError(
                "Spot intent quantity differs from the admitted leg order_quantity"
            )
        price = _positive_decimal(spec.get("price"), "SPOT.price")
        idempotency_key = str(spec.get("idempotency_key") or "").strip()
        if not idempotency_key:
            raise SessionEntryIntentError("SPOT.idempotency_key is required")
        order_type = str(spec.get("order_type") or "").strip().lower()
        if order_type not in {"limit", "market"}:
            raise SessionEntryIntentError("SPOT.order_type must be limit or market")
        metadata = dict(spec.get("request_metadata") or {})
        metadata.setdefault("action_class", "ENTRY")
        if str(metadata["action_class"]).upper() != "ENTRY":
            raise SessionEntryIntentError("initial Spot entry intent must use action_class=ENTRY")

        intent = await self.order_intents.prepare_create(
            account_scope_id=snapshot["account_scope_id"],
            idempotency_key=idempotency_key,
            venue=str(leg["venue"]),
            market_type=str(leg["market_type"]),
            instrument=str(leg["symbol"]),
            pair_key=snapshot["pair_key"],
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            actor_id=actor,
            session_id=session_id,
            leg_id=leg_id,
            reserved_base_qty=spec.get("reserved_base_qty", 0),
            reserved_notional=spec.get("reserved_notional", 0),
            reservation_asset=spec.get("reservation_asset"),
            max_reserved_base_qty=spec.get("max_reserved_base_qty"),
            max_reserved_notional=spec.get("max_reserved_notional"),
            request_metadata=metadata,
        )
        intent_status = str(intent.get("status") or "").upper()
        if intent_status not in {"PREPARED", "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED", "COMPLETED"}:
            raise SessionEntryIntentError(
                f"Spot mutation intent {intent.get('intent_id')} is not executable: {intent_status}"
            )
        if (
            str(intent.get("session_id") or "") != session_id
            or str(intent.get("leg_id") or "") != leg_id
            or str(intent.get("venue") or "").lower() != str(leg["venue"]).lower()
            or str(intent.get("instrument") or "").upper() != str(leg["symbol"]).upper()
            or str(intent.get("pair_key") or "") != snapshot["pair_key"]
            or str(intent.get("account_scope_id") or "") != snapshot["account_scope_id"]
        ):
            raise SessionEntryIntentError("prepared mutation intent does not match the durable Spot leg")

        aggregate_version = snapshot["aggregate_version"]
        current_leg_state = str(leg["status"]).upper()
        if current_leg_state != "PREPARED":
            if current_leg_state not in {"CREATED", "NOT_STARTED"}:
                if snapshot["state"] != "ENTRY_IN_PROGRESS":
                    raise SessionEntryIntentError(
                        f"Spot leg state {current_leg_state} cannot be prepared for entry"
                    )
            else:
                transition = await self._transition_leg(
                    session_id=session_id, leg_id=leg_id,
                    expected_leg_state_version=int(leg["state_version"]),
                    expected_aggregate_version=aggregate_version,
                    fencing_generation=snapshot["generation"],
                    coordinator_lease=coordinator_lease,
                    actor_id=actor,
                    idempotency_key=f"entry-intents:{session_id}:{leg_id}",
                )
                aggregate_version = int(transition["aggregate_version"])

        if snapshot["state"] == "ADMITTED":
            await self._transition_session(
                session_id=session_id,
                expected_state_version=snapshot["state_version"],
                expected_aggregate_version=aggregate_version,
                fencing_generation=snapshot["generation"],
                coordinator_lease=coordinator_lease,
                actor_id=actor,
                idempotency_key=f"entry-intents:{session_id}:entry-started",
                payload={"prepared_spot_intent_id": str(intent["intent_id"])},
            )
            aggregate_version += 1
        return {
            "session_id": session_id,
            "state": "ENTRY_IN_PROGRESS",
            "spot_intent": intent,
            "intents": {leg_id: intent},
            "aggregate_version": aggregate_version,
        }

    def _repository(self) -> Any:
        if self.repository is None:
            from executor.execution_repository import ExecutionSessionRepository
            self.repository = ExecutionSessionRepository(self.pool)
        return self.repository

    async def _transition_leg(
        self, *, session_id: str, leg_id: str,
        expected_leg_state_version: int, expected_aggregate_version: int,
        fencing_generation: int, coordinator_lease: Any,
        actor_id: str, idempotency_key: str,
    ) -> dict:
        repository = self._repository()
        return await repository.transition_leg(
            session_id=session_id, leg_id=leg_id, target_state="PREPARED",
            expected_leg_state_version=expected_leg_state_version,
            expected_aggregate_version=expected_aggregate_version,
            fencing_generation=fencing_generation,
            event_type="SPOT_ENTRY_INTENT_PREPARED",
            event_idempotency_key=idempotency_key,
            actor_id=actor_id,
            payload={"intent_prepared": True, "leg_type": "SPOT"},
            coordinator_lease=coordinator_lease,
        )

    async def _transition_session(
        self, *, session_id: str, expected_state_version: int,
        expected_aggregate_version: int, fencing_generation: int,
        coordinator_lease: Any, actor_id: str, idempotency_key: str,
        payload: Mapping[str, Any],
    ) -> dict:
        repository = self._repository()
        return await repository.transition_session(
            session_id=session_id, target_state="ENTRY_IN_PROGRESS",
            expected_state_version=expected_state_version,
            expected_aggregate_version=expected_aggregate_version,
            fencing_generation=fencing_generation,
            event_type="SPOT_ENTRY_INTENT_PREPARED",
            event_idempotency_key=idempotency_key,
            actor_id=actor_id, payload=payload,
            coordinator_lease=coordinator_lease,
        )
