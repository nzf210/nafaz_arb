"""Durable hedge reservation lifecycle."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Mapping

from executor.execution_fill_service import FillConflict
from executor.execution_hedge_common import (
    HedgeServiceBase, HedgeObligationError, _qty, _text, _fmt,
)


def _proof(value: Any, name: str, required_fields: tuple[str, ...]) -> dict:
    if not isinstance(value, Mapping) or not value:
        raise HedgeObligationError(f"{name} must be a non-empty evidence mapping")
    result = dict(value)
    for field in required_fields:
        if not str(result.get(field) or "").strip():
            raise HedgeObligationError(f"{name}.{field} is required")
    return result


def _terminal_order_proof(value: Any) -> dict:
    proof = _proof(
        value, "terminal_order_evidence",
        ("evidence_id", "source", "verified_at"),
    )
    status = str(proof.get("terminal_status") or "").strip().lower()
    if status not in {"closed", "canceled", "cancelled", "filled", "rejected", "expired"}:
        raise HedgeObligationError("terminal_order_evidence.terminal_status must be terminal")
    if not str(proof.get("exchange_order_id") or proof.get("client_order_id") or "").strip():
        raise HedgeObligationError("terminal_order_evidence requires an order identifier")
    return proof


def _fill_reconciliation_proof(value: Any) -> dict:
    proof = _proof(
        value, "fill_reconciliation_evidence",
        ("evidence_id", "source", "reconciled_at"),
    )
    if proof.get("fills_checked") is not True:
        raise HedgeObligationError("fill_reconciliation_evidence.fills_checked must be true")
    if not str(proof.get("exchange_order_id") or proof.get("client_order_id") or "").strip():
        raise HedgeObligationError("fill_reconciliation_evidence requires an order identifier")
    return proof


class HedgeReservationMixin(HedgeServiceBase):
    async def reserve(
        self, *, session_id: str, futures_venue: str, futures_market_type: str,
        futures_symbol: str, base_asset: str, position_side: str,
        contract_type: str | None, contract_size: Any | None,
        quantity_unit: str, base_per_unit: Any,
        requested_base_quantity: Any, minimum_base_quantity: Any,
        idempotency_key: str, actor_id: str,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Reserve compatible open obligations FIFO before any Futures submit."""
        requested = _qty(requested_base_quantity, "requested_base_quantity")
        minimum = _qty(minimum_base_quantity, "minimum_base_quantity")
        unit = _qty(base_per_unit, "base_per_unit")
        contract = _qty(contract_size, "contract_size") if contract_size is not None else None
        key, actor = _text(idempotency_key, "idempotency_key"), _text(actor_id, "actor_id")
        route = {
            "venue": _text(futures_venue, "futures_venue").lower(),
            "market": _text(futures_market_type, "futures_market_type").lower(),
            "symbol": _text(futures_symbol, "futures_symbol"),
            "base": _text(base_asset, "base_asset").upper(),
            "side": _text(position_side, "position_side").upper(),
            "unit": _text(quantity_unit, "quantity_unit").upper(),
        }
        if route["side"] not in {"LONG", "SHORT"}:
            raise HedgeObligationError("position_side must be LONG or SHORT")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await self._lock_session(
                    conn, session_id=session_id, lease=coordinator_lease,
                    generation=fencing_generation,
                )
                prior = await conn.fetchrow(
                    "SELECT * FROM public.execution_hedge_reservations WHERE session_id=$1 AND idempotency_key=$2 FOR UPDATE",
                    session_id, key,
                )
                if prior is not None:
                    same = (
                        str(prior["futures_venue"]) == route["venue"]
                        and str(prior["futures_market_type"]) == route["market"]
                        and str(prior["futures_symbol"]) == route["symbol"]
                        and str(prior["base_asset"]).upper() == route["base"]
                        and str(prior["position_side"]).upper() == route["side"]
                        and str(prior["quantity_unit"]).upper() == route["unit"]
                        and str(prior["contract_type"] or "") == str(contract_type or "")
                        and (Decimal(str(prior["contract_size"])) if prior["contract_size"] is not None else None) == contract
                        and Decimal(str(prior["base_per_unit"])) == unit
                        and Decimal(str(prior["requested_base_quantity"])) == requested
                        and Decimal(str(prior["minimum_base_quantity"])) == minimum
                    )
                    if not same:
                        raise FillConflict("hedge reservation idempotency key was reused with a different request")
                    return {
                        "duplicate": True, "reservation_id": str(prior["reservation_id"]),
                        "reserved_base_quantity": _fmt(Decimal(str(prior["reserved_base_quantity"]))),
                        "status": str(prior["status"]),
                        "aggregate_version": int(session["aggregate_version"]),
                    }
                if str(session["status"]).upper() not in {"HEDGE_PENDING", "PARTIALLY_HEDGED"}:
                    raise HedgeObligationError(
                        f"cannot reserve hedge obligations while session is {session['status']}"
                    )
                obligations = await conn.fetch(
                    """
                    SELECT * FROM public.execution_hedge_obligations
                    WHERE session_id=$1 AND account_scope_id=$2
                      AND futures_venue=$3 AND futures_market_type=$4
                      AND futures_symbol=$5 AND base_asset=$6 AND position_side=$7
                      AND contract_type IS NOT DISTINCT FROM $8
                      AND contract_size IS NOT DISTINCT FROM $9
                      AND quantity_unit=$10 AND base_per_unit=$11
                      AND status IN ('OPEN','PARTIALLY_HEDGED')
                      AND target_base_quantity > confirmed_base_quantity + reserved_base_quantity
                    ORDER BY created_at, obligation_id
                    FOR UPDATE
                    """,
                    session_id, session["account_scope_id"], route["venue"],
                    route["market"], route["symbol"], route["base"], route["side"],
                    contract_type, contract, route["unit"], unit,
                )
                available = sum((
                    Decimal(str(row["target_base_quantity"]))
                    - Decimal(str(row["confirmed_base_quantity"]))
                    - Decimal(str(row["reserved_base_quantity"]))
                    for row in obligations
                ), Decimal("0"))
                total = min(requested, available)
                if total < minimum:
                    return {
                        "reserved": False, "reason": "BELOW_MINIMUM_HEDGE",
                        "available_base_quantity": _fmt(available),
                        "minimum_base_quantity": _fmt(minimum),
                        "aggregate_version": int(session["aggregate_version"]),
                    }
                reservation = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_hedge_reservations (
                        session_id, account_scope_id, futures_venue, futures_market_type,
                        futures_symbol, base_asset, contract_type, contract_size,
                        quantity_unit, base_per_unit, position_side,
                        requested_base_quantity, minimum_base_quantity, reserved_base_quantity,
                        idempotency_key, actor_id
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                    RETURNING reservation_id
                    """,
                    session_id, session["account_scope_id"], route["venue"], route["market"],
                    route["symbol"], route["base"], contract_type, contract,
                    route["unit"], unit, route["side"], requested, minimum, total, key, actor,
                )
                remaining = total
                item_payload = []
                for obligation in obligations:
                    if remaining <= 0:
                        break
                    target = Decimal(str(obligation["target_base_quantity"]))
                    confirmed = Decimal(str(obligation["confirmed_base_quantity"]))
                    reserved = Decimal(str(obligation["reserved_base_quantity"]))
                    take = min(remaining, target - confirmed - reserved)
                    if take <= 0:
                        continue
                    await conn.execute(
                        """
                        INSERT INTO public.execution_hedge_reservation_items (
                            session_id, reservation_id, obligation_id, reserved_base_quantity
                        ) VALUES ($1,$2::uuid,$3::uuid,$4)
                        """,
                        session_id, reservation["reservation_id"], obligation["obligation_id"], take,
                    )
                    updated_obligation = await conn.execute(
                        """
                        UPDATE public.execution_hedge_obligations
                        SET reserved_base_quantity=reserved_base_quantity+$2,
                            updated_at=clock_timestamp()
                        WHERE obligation_id=$1
                          AND target_base_quantity >= confirmed_base_quantity + reserved_base_quantity + $2
                        """,
                        obligation["obligation_id"], take,
                    )
                    if not updated_obligation.endswith(" 1"):
                        raise HedgeObligationError("hedge obligation reservation compare-and-swap failed")
                    item_payload.append({
                        "obligation_id": str(obligation["obligation_id"]),
                        "reserved_base_quantity": _fmt(take),
                    })
                    remaining -= take
                if remaining != 0:
                    raise HedgeObligationError("reservation allocation did not conserve requested quantity")
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_RESERVATION_CREATED", idempotency_key=key,
                    actor_id=actor, generation=fencing_generation,
                    payload={
                        "reservation_id": str(reservation["reservation_id"]),
                        "requested_base_quantity": _fmt(requested),
                        "reserved_base_quantity": _fmt(total),
                        "items": item_payload,
                    },
                )
                return {
                    "reserved": True, "duplicate": False,
                    "reservation_id": str(reservation["reservation_id"]),
                    "reserved_base_quantity": _fmt(total),
                    "items": item_payload, "aggregate_version": version,
                }

    async def mark_submitted(
        self, *, session_id: str, reservation_id: str, client_order_id: str,
        actor_id: str, event_idempotency_key: str,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Bind a durable reservation to the client order ID before gateway submission."""
        actor = _text(actor_id, "actor_id")
        client_id = _text(client_order_id, "client_order_id")
        key = _text(event_idempotency_key, "event_idempotency_key")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await self._lock_session(
                    conn, session_id=session_id, lease=coordinator_lease,
                    generation=fencing_generation,
                )
                reservation = await conn.fetchrow(
                    "SELECT * FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid AND session_id=$2 FOR UPDATE",
                    reservation_id, session_id,
                )
                if reservation is None:
                    raise HedgeObligationError("hedge reservation does not exist")
                if reservation["status"] == "SUBMITTED" and str(reservation["client_order_id"]) == client_id:
                    return {"duplicate": True, "status": "SUBMITTED",
                            "aggregate_version": int(session["aggregate_version"])}
                if reservation["status"] != "RESERVED":
                    raise HedgeObligationError(f"cannot submit reservation in {reservation['status']} state")
                if str(session["status"]).upper() not in {"HEDGE_PENDING", "PARTIALLY_HEDGED"}:
                    raise HedgeObligationError(
                        f"cannot submit hedge reservation while session is {session['status']}"
                    )
                await conn.execute(
                    """
                    UPDATE public.execution_hedge_reservations
                    SET status='SUBMITTED', client_order_id=$2, updated_at=clock_timestamp()
                    WHERE reservation_id=$1::uuid
                    """,
                    reservation_id, client_id,
                )
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_RESERVATION_SUBMITTED",
                    idempotency_key=key, actor_id=actor,
                    generation=fencing_generation,
                    payload={"reservation_id": reservation_id, "client_order_id": client_id},
                )
                return {"duplicate": False, "status": "SUBMITTED", "aggregate_version": version}

    async def mark_unknown(
        self, *, session_id: str, reservation_id: str, reason: str,
        actor_id: str, event_idempotency_key: str,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Keep in-flight hedge quantity reserved when submission outcome is ambiguous."""
        actor = _text(actor_id, "actor_id")
        key = _text(event_idempotency_key, "event_idempotency_key")
        reason = _text(reason, "reason")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await self._lock_session(
                    conn, session_id=session_id, lease=coordinator_lease,
                    generation=fencing_generation,
                )
                reservation = await conn.fetchrow(
                    "SELECT status FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid AND session_id=$2 FOR UPDATE",
                    reservation_id, session_id,
                )
                if reservation is None:
                    raise HedgeObligationError("hedge reservation does not exist")
                if reservation["status"] == "UNKNOWN":
                    return {"duplicate": True, "status": "UNKNOWN",
                            "aggregate_version": int(session["aggregate_version"])}
                if reservation["status"] not in {"SUBMITTED", "PARTIALLY_FILLED"}:
                    raise HedgeObligationError("only submitted hedge reservations can become UNKNOWN")
                await conn.execute(
                    "UPDATE public.execution_hedge_reservations SET status='UNKNOWN', updated_at=clock_timestamp() WHERE reservation_id=$1::uuid",
                    reservation_id,
                )
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_RESERVATION_UNKNOWN",
                    idempotency_key=key,
                    actor_id=actor, generation=fencing_generation,
                    reason=reason,
                    payload={"reservation_id": reservation_id, "status": "UNKNOWN"},
                )
                return {"duplicate": False, "status": "UNKNOWN", "aggregate_version": version}


    async def release(
        self, *, session_id: str, reservation_id: str,
        actor_id: str, event_idempotency_key: str,
        terminal_order_verified: bool = False, fills_reconciled: bool = False,
        terminal_order_evidence: Mapping[str, Any] | None = None,
        fill_reconciliation_evidence: Mapping[str, Any] | None = None,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Release only proven-unfilled quantity; unknown orders remain reserved."""
        actor = _text(actor_id, "actor_id")
        key = _text(event_idempotency_key, "event_idempotency_key")
        if not isinstance(terminal_order_verified, bool) or not isinstance(fills_reconciled, bool):
            raise HedgeObligationError("terminal order and fill reconciliation evidence must be booleans")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await self._lock_session(
                    conn, session_id=session_id, lease=coordinator_lease,
                    generation=fencing_generation,
                )
                reservation = await conn.fetchrow(
                    "SELECT * FROM public.execution_hedge_reservations WHERE reservation_id=$1::uuid AND session_id=$2 FOR UPDATE",
                    reservation_id, session_id,
                )
                if reservation is None:
                    raise HedgeObligationError("hedge reservation does not exist")
                status = str(reservation["status"])
                if status == "RELEASED":
                    return {"duplicate": True, "status": status,
                            "aggregate_version": int(session["aggregate_version"])}
                if status in {"CONSUMED", "RECONCILIATION_REQUIRED"}:
                    raise HedgeObligationError(f"cannot release reservation in {status} state")
                release_evidence = {}
                if status != "RESERVED":
                    if not (terminal_order_verified and fills_reconciled):
                        raise HedgeObligationError("submitted reservation release requires terminal order and fill proof")
                    release_evidence = {
                        "terminal_order": _terminal_order_proof(terminal_order_evidence),
                        "fill_reconciliation": _fill_reconciliation_proof(fill_reconciliation_evidence),
                    }
                items = await conn.fetch(
                    """
                    SELECT i.reservation_id, i.obligation_id,
                           i.reserved_base_quantity AS item_reserved_base_quantity,
                           i.allocated_base_quantity AS item_allocated_base_quantity,
                           o.confirmed_base_quantity,
                           o.reserved_base_quantity AS obligation_reserved_base_quantity,
                           o.target_base_quantity
                    FROM public.execution_hedge_reservation_items i
                    JOIN public.execution_hedge_obligations o USING (obligation_id)
                    WHERE i.reservation_id=$1::uuid
                    FOR UPDATE OF i, o
                    """,
                    reservation_id,
                )
                released_total = Decimal("0")
                for item in items:
                    unfilled = (
                        Decimal(str(item["item_reserved_base_quantity"]))
                        - Decimal(str(item["item_allocated_base_quantity"]))
                    )
                    if unfilled <= 0:
                        continue
                    released_obligation = await conn.execute(
                        """
                        UPDATE public.execution_hedge_obligations
                        SET reserved_base_quantity=reserved_base_quantity-$2,
                            remaining_base_quantity=target_base_quantity-confirmed_base_quantity,
                            status=CASE
                                WHEN confirmed_base_quantity >= target_base_quantity THEN 'HEDGED'
                                WHEN confirmed_base_quantity > 0 THEN 'PARTIALLY_HEDGED'
                                ELSE 'OPEN'
                            END,
                            updated_at=clock_timestamp()
                        WHERE obligation_id=$1::uuid AND reserved_base_quantity >= $2
                        """,
                        item["obligation_id"], unfilled,
                    )
                    if not released_obligation.endswith(" 1"):
                        raise HedgeObligationError("hedge obligation release compare-and-swap failed")
                    released_total += unfilled
                await conn.execute(
                    """
                    UPDATE public.execution_hedge_reservations
                    SET status='RELEASED', release_evidence=$2::jsonb,
                        updated_at=clock_timestamp()
                    WHERE reservation_id=$1::uuid
                    """,
                    reservation_id, json.dumps(release_evidence, sort_keys=True, default=str),
                )
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_RESERVATION_RELEASED",
                    idempotency_key=key, actor_id=actor,
                    generation=fencing_generation,
                    payload={
                        "reservation_id": reservation_id,
                        "released_base_quantity": _fmt(released_total),
                        "release_evidence": release_evidence,
                    },
                )
                return {
                    "duplicate": False, "status": "RELEASED",
                    "released_base_quantity": _fmt(released_total),
                    "aggregate_version": version,
                }
