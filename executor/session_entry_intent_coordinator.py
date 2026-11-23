"""Prepare both V3 entry intents before advancing a session into execution.

This coordinator persists logical intents only. It never calls an exchange client.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from executor.execution_repository_fencing import validate_coordinator_lease
from executor.order_intent_service import OrderIntentService


class SessionEntryIntentError(RuntimeError):
    """The session cannot safely prepare its two entry-leg intents."""


def _positive_decimal(value: Any, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise SessionEntryIntentError(f"{field} must be a finite positive decimal") from exc
    if not number.is_finite() or number <= 0:
        raise SessionEntryIntentError(f"{field} must be a finite positive decimal")
    return number


def _index_specs(
    legs: list[Mapping[str, Any]], specs: list[Mapping[str, Any]],
) -> dict[str, dict]:
    if not isinstance(specs, list) or len(specs) != 2:
        raise SessionEntryIntentError("exactly two entry intent specifications are required")
    leg_ids = {str(leg["leg_id"]) for leg in legs}
    indexed: dict[str, dict] = {}
    for raw in specs:
        if not isinstance(raw, Mapping):
            raise SessionEntryIntentError("each entry intent specification must be a mapping")
        leg_id = str(raw.get("leg_id") or "").strip()
        if not leg_id or leg_id not in leg_ids or leg_id in indexed:
            raise SessionEntryIntentError("entry intent specifications must map one-to-one to session legs")
        indexed[leg_id] = dict(raw)
    if set(indexed) != leg_ids:
        raise SessionEntryIntentError("an entry intent specification is missing for a session leg")
    return indexed


class SessionEntryIntentCoordinator:
    """Idempotently prepare the Spot and Futures intents for one admitted session."""

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
                        f"session state {state} cannot prepare entry intents"
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
                by_leg = _index_specs(legs, specs)
                snapshot = {
                    "state": state,
                    "state_version": int(session["state_version"]),
                    "aggregate_version": int(session["aggregate_version"]),
                    "account_scope_id": str(session["account_scope_id"]),
                    "pair_key": str(session["pair_key"]),
                    "generation": generation,
                    "legs": legs,
                    "specs": by_leg,
                }

        validated: dict[str, dict] = {}
        # Validate both legs completely before writing either intent. This avoids
        # leaving a one-sided reservation because the second specification was
        # malformed.
        for leg in snapshot["legs"]:
            leg_id = str(leg["leg_id"])
            spec = snapshot["specs"][leg_id]
            leg_type = str(leg["leg_type"]).upper()
            expected_side = "buy" if leg_type == "SPOT" else "sell"
            side = str(spec.get("side") or "").strip().lower()
            if side != expected_side:
                raise SessionEntryIntentError(f"{leg_type} entry side must be {expected_side}")
            if leg_type == "FUTURES" and str(leg.get("position_side") or "").upper() != "SHORT":
                raise SessionEntryIntentError("Futures entry leg must be SHORT for this Spot/Perpetual strategy")
            quantity = _positive_decimal(spec.get("quantity"), f"{leg_type}.quantity")
            expected_quantity = _positive_decimal(leg.get("order_quantity"), f"{leg_type}.order_quantity")
            if quantity != expected_quantity:
                raise SessionEntryIntentError(
                    f"{leg_type} intent quantity differs from the admitted leg order_quantity"
                )
            price = _positive_decimal(spec.get("price"), f"{leg_type}.price")
            idempotency_key = str(spec.get("idempotency_key") or "").strip()
            if not idempotency_key:
                raise SessionEntryIntentError(f"{leg_type}.idempotency_key is required")
            order_type = str(spec.get("order_type") or "").strip().lower()
            if order_type not in {"limit", "market"}:
                raise SessionEntryIntentError(f"{leg_type}.order_type must be limit or market")
            metadata = dict(spec.get("request_metadata") or {})
            metadata.setdefault("action_class", "ENTRY")
            if str(metadata["action_class"]).upper() != "ENTRY":
                raise SessionEntryIntentError("session entry intents must use action_class=ENTRY")
            validated[leg_id] = {
                "side": side, "quantity": quantity, "price": price,
                "idempotency_key": idempotency_key, "order_type": order_type,
                "metadata": metadata,
            }

        idempotency_keys = [item["idempotency_key"] for item in validated.values()]
        if len(set(idempotency_keys)) != len(idempotency_keys):
            raise SessionEntryIntentError("each entry leg requires a distinct idempotency key")

        prepared: dict[str, dict] = {}
        for leg in snapshot["legs"]:
            leg_id = str(leg["leg_id"])
            spec = snapshot["specs"][leg_id]
            values = validated[leg_id]
            intent = await self.order_intents.prepare_create(
                account_scope_id=snapshot["account_scope_id"],
                idempotency_key=values["idempotency_key"],
                venue=str(leg["venue"]),
                market_type=str(leg["market_type"]),
                instrument=str(leg["symbol"]),
                pair_key=snapshot["pair_key"],
                side=values["side"],
                order_type=values["order_type"],
                quantity=values["quantity"],
                price=values["price"],
                actor_id=actor,
                session_id=session_id,
                leg_id=leg_id,
                reserved_base_qty=spec.get("reserved_base_qty", 0),
                reserved_notional=spec.get("reserved_notional", 0),
                reservation_asset=spec.get("reservation_asset"),
                max_reserved_base_qty=spec.get("max_reserved_base_qty"),
                max_reserved_notional=spec.get("max_reserved_notional"),
                request_metadata=values["metadata"],
            )
            intent_status = str(intent.get("status") or "").upper()
            if intent_status not in {"PREPARED", "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED", "COMPLETED"}:
                raise SessionEntryIntentError(
                    f"entry mutation intent {intent.get('intent_id')} is not executable: {intent_status}"
                )
            if (
                str(intent.get("session_id") or "") != session_id
                or str(intent.get("leg_id") or "") != leg_id
                or str(intent.get("venue") or "").lower() != str(leg["venue"]).lower()
                or str(intent.get("instrument") or "").upper() != str(leg["symbol"]).upper()
                or str(intent.get("pair_key") or "") != snapshot["pair_key"]
                or str(intent.get("account_scope_id") or "") != snapshot["account_scope_id"]
            ):
                raise SessionEntryIntentError("prepared mutation intent does not match its durable session leg")
            prepared[leg_id] = intent

        # Aggregate transitions are separate idempotent transactions. If the
        # process stops between them, retrying the same specs returns the same
        # intents and continues from the persisted leg/session versions.
        aggregate_version = snapshot["aggregate_version"]
        for leg in snapshot["legs"]:
            leg_id = str(leg["leg_id"])
            current_leg_state = str(leg["status"]).upper()
            if current_leg_state == "PREPARED":
                continue
            if current_leg_state not in {"CREATED", "NOT_STARTED"}:
                if snapshot["state"] == "ENTRY_IN_PROGRESS":
                    continue
                raise SessionEntryIntentError(
                    f"leg {leg_id} state {current_leg_state} cannot be prepared for entry"
                )
            transition = await self._transition_leg(
                session_id=session_id, leg_id=leg_id,
                expected_leg_state_version=int(leg["state_version"]),
                expected_aggregate_version=aggregate_version,
                fencing_generation=snapshot["generation"],
                coordinator_lease=coordinator_lease,
                actor_id=actor, idempotency_key=f"entry-intents:{session_id}:{leg_id}",
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
                payload={"prepared_intent_ids": sorted(
                    str(item["intent_id"]) for item in prepared.values()
                )},
            )
        return {
            "session_id": session_id,
            "state": "ENTRY_IN_PROGRESS",
            "intents": prepared,
            "aggregate_version": aggregate_version + (1 if snapshot["state"] == "ADMITTED" else 0),
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
            event_type="ENTRY_INTENT_PREPARED",
            event_idempotency_key=idempotency_key,
            actor_id=actor_id,
            payload={"intent_prepared": True},
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
            event_type="ENTRY_INTENTS_PREPARED",
            event_idempotency_key=idempotency_key,
            actor_id=actor_id, payload=payload,
            coordinator_lease=coordinator_lease,
        )
