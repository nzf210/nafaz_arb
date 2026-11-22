"""Durable V3 hedge obligations, reservations, and fill allocations.

All quantities are base-equivalent Decimal values at the obligation boundary.
The service is session-scoped and never calls an exchange adapter.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from executor.execution_fill_service import FillConflict, FillValidationError
from executor.execution_repository import ExecutionRepositoryError
from executor.execution_repository_fencing import (
    insert_session_event, validate_coordinator_lease,
)


class HedgeObligationError(ExecutionRepositoryError):
    """A hedge obligation or reservation violates its durable invariants."""


def _qty(value: Any, name: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise FillValidationError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result < 0 if allow_zero else result <= 0):
        raise FillValidationError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
    return result


def _text(value: Any, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise FillValidationError(f"{name} is required")
    return result


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), "f")


class ExecutionHedgeObligationService:
    """PostgreSQL service for per-session Spot-fill obligations and Futures coverage."""

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        self.pool = pool

    async def _lock_session(self, conn: Any, *, session_id: str, lease: Any, generation: int):
        row = await conn.fetchrow(
            """
            SELECT status, state_version, aggregate_version, fencing_generation,
                   account_scope_id, pair_key, coordinator_scope_key, coordinator_owner_id
            FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
            """,
            session_id,
        )
        if row is None:
            raise HedgeObligationError("execution session does not exist")
        await validate_coordinator_lease(
            conn, lease, session_id=session_id,
            account_scope_id=str(row["account_scope_id"]),
            expected_generation=generation,
            session_scope_key=row["coordinator_scope_key"],
            session_owner_id=row["coordinator_owner_id"],
        )
        return row

    async def _append_event(
        self, conn: Any, *, session_id: str, session: Any,
        event_type: str, idempotency_key: str, actor_id: str,
        generation: int, payload: dict, reason: str = "",
    ) -> int:
        version = int(session["aggregate_version"]) + 1
        updated = await conn.fetchrow(
            """
            UPDATE public.execution_sessions
            SET aggregate_version=$2, updated_at=clock_timestamp()
            WHERE session_id=$1 AND aggregate_version=$3
              AND fencing_generation=$4
            RETURNING aggregate_version
            """,
            session_id, version, int(session["aggregate_version"]), generation,
        )
        if updated is None:
            raise HedgeObligationError("session aggregate cursor compare-and-swap lost")
        await insert_session_event(
            conn, session_id=session_id, aggregate_version=version,
            entity_type="SESSION", entity_id=session_id,
            entity_version=int(session["state_version"]),
            event_type=event_type, idempotency_key=idempotency_key,
            actor_id=actor_id, fencing_generation=generation,
            reason=reason, correlation_id=session_id, payload=payload,
        )
        return version

    async def create_from_spot_fill(
        self, *, session_id: str, spot_fill_id: str, hedge_ratio: Any,
        actor_id: str, coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Create one hedge obligation from one confirmed Spot entry fill."""
        ratio = _qty(hedge_ratio, "hedge_ratio")
        actor = _text(actor_id, "actor_id")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await self._lock_session(
                    conn, session_id=session_id, lease=coordinator_lease,
                    generation=fencing_generation,
                )
                fill = await conn.fetchrow(
                    """
                    SELECT f.fill_id, f.session_id, f.account_scope_id, f.venue,
                           f.market_type, f.symbol, f.side, f.quantity,
                           f.exchange_trade_id, spot.leg_id AS spot_leg_id,
                           spot.leg_type AS spot_leg_type, spot.base_asset,
                           spot.base_per_unit AS spot_base_per_unit,
                           futures.leg_id AS futures_leg_id,
                           futures.venue AS futures_venue,
                           futures.market_type AS futures_market_type,
                           futures.symbol AS futures_symbol,
                           futures.base_asset AS futures_base_asset,
                           futures.contract_type, futures.contract_size,
                           futures.position_side, futures.quantity_unit,
                           futures.base_per_unit AS futures_base_per_unit
                    FROM public.execution_fills f
                    JOIN public.execution_legs spot
                      ON spot.session_id=f.session_id AND spot.leg_id=f.leg_id
                    JOIN public.execution_legs futures
                      ON futures.session_id=f.session_id AND futures.leg_type='FUTURES'
                    WHERE f.fill_id=$1::uuid AND f.session_id=$2
                    FOR UPDATE OF f, spot, futures
                    """,
                    spot_fill_id, session_id,
                )
                if fill is None:
                    raise HedgeObligationError("confirmed Spot fill or paired Futures leg does not exist")
                if str(fill["account_scope_id"]) != str(session["account_scope_id"]):
                    raise HedgeObligationError("source fill account scope does not match session")
                if str(fill["spot_leg_type"]) != "SPOT" or str(fill["market_type"]).lower() != "spot":
                    raise HedgeObligationError("source fill is not bound to a Spot leg")
                if str(fill["side"]).lower() != "buy":
                    raise HedgeObligationError("only confirmed Spot entry buys create hedge obligations")
                if str(fill["position_side"] or "").upper() != "SHORT":
                    raise HedgeObligationError("paired Futures leg is not configured as SHORT")
                if str(fill["base_asset"]).upper() != str(fill["futures_base_asset"]).upper():
                    raise HedgeObligationError("Spot/Futures base asset identity mismatch")

                spot_unit = _qty(fill["spot_base_per_unit"], "spot_base_per_unit")
                future_unit = _qty(fill["futures_base_per_unit"], "futures_base_per_unit")
                target = _qty(fill["quantity"], "spot_fill_quantity") * spot_unit * ratio
                existing = await conn.fetchrow(
                    "SELECT * FROM public.execution_hedge_obligations WHERE source_fill_id=$1::uuid FOR UPDATE",
                    spot_fill_id,
                )
                if existing is not None:
                    same = (
                        str(existing["session_id"]) == session_id
                        and Decimal(str(existing["hedge_ratio"])) == ratio
                        and Decimal(str(existing["target_base_quantity"])) == target
                        and str(existing["futures_symbol"]) == str(fill["futures_symbol"])
                    )
                    if not same:
                        raise FillConflict("Spot fill already has a different hedge obligation")
                    return {
                        "duplicate": True, "obligation_id": str(existing["obligation_id"]),
                        "target_base_quantity": _fmt(target),
                        "aggregate_version": int(session["aggregate_version"]),
                    }
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_hedge_obligations (
                        session_id, source_fill_id, spot_leg_id, futures_leg_id,
                        account_scope_id, pair_key, base_asset, futures_venue,
                        futures_market_type, futures_symbol, contract_type,
                        contract_size, position_side, quantity_unit, base_per_unit,
                        hedge_ratio, target_base_quantity, remaining_base_quantity
                    ) VALUES (
                        $1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                        $15,$16,$17,$17
                    )
                    RETURNING obligation_id
                    """,
                    session_id, spot_fill_id, fill["spot_leg_id"], fill["futures_leg_id"],
                    session["account_scope_id"], session["pair_key"], fill["base_asset"],
                    fill["futures_venue"], fill["futures_market_type"], fill["futures_symbol"],
                    fill["contract_type"], fill["contract_size"], fill["position_side"],
                    fill["quantity_unit"], future_unit, ratio, target,
                )
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_OBLIGATION_CREATED",
                    idempotency_key=f"hedge-obligation:{spot_fill_id}",
                    actor_id=actor, generation=fencing_generation,
                    payload={
                        "obligation_id": str(inserted["obligation_id"]),
                        "source_fill_id": spot_fill_id,
                        "target_base_quantity": _fmt(target),
                        "hedge_ratio": _fmt(ratio),
                        "futures_symbol": str(fill["futures_symbol"]),
                    },
                )
                return {
                    "duplicate": False, "obligation_id": str(inserted["obligation_id"]),
                    "target_base_quantity": _fmt(target), "aggregate_version": version,
                }

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
            raise FillValidationError("position_side must be LONG or SHORT")
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
                    )
                    if not same:
                        raise FillConflict("hedge reservation idempotency key was reused with a different request")
                    return {
                        "duplicate": True, "reservation_id": str(prior["reservation_id"]),
                        "reserved_base_quantity": _fmt(Decimal(str(prior["reserved_base_quantity"]))),
                        "status": str(prior["status"]),
                        "aggregate_version": int(session["aggregate_version"]),
                    }
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
                        requested_base_quantity, reserved_base_quantity,
                        idempotency_key, actor_id
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
                    RETURNING reservation_id
                    """,
                    session_id, session["account_scope_id"], route["venue"], route["market"],
                    route["symbol"], route["base"], contract_type, contract,
                    route["unit"], unit, route["side"], requested, total, key, actor,
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
                            reservation_id, obligation_id, reserved_base_quantity
                        ) VALUES ($1::uuid,$2::uuid,$3)
                        """,
                        reservation["reservation_id"], obligation["obligation_id"], take,
                    )
                    await conn.execute(
                        """
                        UPDATE public.execution_hedge_obligations
                        SET reserved_base_quantity=reserved_base_quantity+$2,
                            updated_at=clock_timestamp()
                        WHERE obligation_id=$1
                        """,
                        obligation["obligation_id"], take,
                    )
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
                    idempotency_key=event_idempotency_key,
                    actor_id=actor_id, generation=fencing_generation,
                    reason=reason,
                    payload={"reservation_id": reservation_id, "status": "UNKNOWN"},
                )
                return {"duplicate": False, "status": "UNKNOWN", "aggregate_version": version}

    async def allocate_futures_fill(
        self, *, session_id: str, reservation_id: str, futures_fill_id: str,
        actor_id: str, event_idempotency_key: str,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Allocate one confirmed Futures fill FIFO across the reserved obligations."""
        actor = _text(actor_id, "actor_id")
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
                fill = await conn.fetchrow(
                    """
                    SELECT f.*, l.leg_type, l.venue AS leg_venue,
                           l.market_type AS leg_market_type, l.symbol AS leg_symbol,
                           l.base_asset, l.base_per_unit, l.position_side,
                           l.contract_type, l.contract_size, l.quantity_unit
                    FROM public.execution_fills f
                    JOIN public.execution_legs l
                      ON l.session_id=f.session_id AND l.leg_id=f.leg_id
                    WHERE f.fill_id=$1::uuid AND f.session_id=$2
                    FOR UPDATE OF f, l
                    """,
                    futures_fill_id, session_id,
                )
                if fill is None or str(fill["leg_type"]) != "FUTURES":
                    raise HedgeObligationError("confirmed Futures fill does not exist in this session")
                if (
                    str(fill["account_scope_id"]) != str(reservation["account_scope_id"])
                    or str(fill["venue"]).lower() != str(reservation["futures_venue"])
                    or str(fill["market_type"]).lower() != str(reservation["futures_market_type"])
                    or str(fill["symbol"]) != str(reservation["futures_symbol"])
                    or str(fill["base_asset"]).upper() != str(reservation["base_asset"])
                    or str(fill["position_side"]).upper() != str(reservation["position_side"])
                    or str(fill["contract_type"] or "") != str(reservation["contract_type"] or "")
                    or (Decimal(str(fill["contract_size"])) if fill["contract_size"] is not None else None)
                       != (Decimal(str(reservation["contract_size"])) if reservation["contract_size"] is not None else None)
                    or str(fill["quantity_unit"]).upper() != str(reservation["quantity_unit"]).upper()
                    or Decimal(str(fill["base_per_unit"])) != Decimal(str(reservation["base_per_unit"]))
                ):
                    raise HedgeObligationError("Futures fill route/contract identity does not match reservation")
                expected_side = "sell" if str(reservation["position_side"]).upper() == "SHORT" else "buy"
                if str(fill["side"]).lower() != expected_side:
                    raise HedgeObligationError("Futures fill side does not increase the configured hedge position")
                base_quantity = Decimal(str(fill["quantity"])) * Decimal(str(fill["base_per_unit"]))
                prior = await conn.fetch(
                    """
                    SELECT reservation_id, allocated_base_quantity
                    FROM public.execution_hedge_allocations
                    WHERE futures_fill_id=$1::uuid
                    """,
                    futures_fill_id,
                )
                if prior:
                    prior_total = sum((Decimal(str(x["allocated_base_quantity"])) for x in prior), Decimal("0"))
                    if any(str(x["reservation_id"]) != reservation_id for x in prior) or prior_total != base_quantity:
                        raise FillConflict("Futures fill was already allocated to a different reservation or quantity")
                    return {"duplicate": True, "allocated_base_quantity": _fmt(prior_total),
                            "aggregate_version": int(session["aggregate_version"])}
                if reservation["status"] not in {"SUBMITTED", "PARTIALLY_FILLED", "UNKNOWN"}:
                    raise HedgeObligationError(f"cannot allocate a fill to reservation in {reservation['status']} state")
                reserved_total = Decimal(str(reservation["reserved_base_quantity"]))
                allocated_total = Decimal(str(reservation["allocated_base_quantity"]))
                remaining_reservation = reserved_total - allocated_total
                if base_quantity <= 0 or base_quantity > remaining_reservation:
                    raise HedgeObligationError("Futures fill exceeds the remaining reserved hedge quantity")

                items = await conn.fetch(
                    """
                    SELECT i.reservation_id, i.obligation_id,
                           i.reserved_base_quantity AS item_reserved_base_quantity,
                           i.allocated_base_quantity AS item_allocated_base_quantity,
                           o.target_base_quantity, o.confirmed_base_quantity,
                           o.reserved_base_quantity AS obligation_reserved_base_quantity
                    FROM public.execution_hedge_reservation_items i
                    JOIN public.execution_hedge_obligations o USING (obligation_id)
                    WHERE i.reservation_id=$1::uuid
                      AND i.reserved_base_quantity > i.allocated_base_quantity
                    ORDER BY i.created_at, i.obligation_id
                    FOR UPDATE OF i, o
                    """,
                    reservation_id,
                )
                remaining_fill = base_quantity
                allocation_rows = []
                for item in items:
                    available = (
                        Decimal(str(item["item_reserved_base_quantity"]))
                        - Decimal(str(item["item_allocated_base_quantity"]))
                    )
                    take = min(remaining_fill, available)
                    if take <= 0:
                        continue
                    new_item_allocated = Decimal(str(item["item_allocated_base_quantity"])) + take
                    new_confirmed = Decimal(str(item["confirmed_base_quantity"])) + take
                    await conn.execute(
                        """
                        UPDATE public.execution_hedge_reservation_items
                        SET allocated_base_quantity=$3, updated_at=clock_timestamp()
                        WHERE reservation_id=$1::uuid AND obligation_id=$2::uuid
                        """,
                        reservation_id, item["obligation_id"], new_item_allocated,
                    )
                    await conn.execute(
                        """
                        UPDATE public.execution_hedge_obligations
                        SET confirmed_base_quantity=confirmed_base_quantity+$2,
                            reserved_base_quantity=reserved_base_quantity-$2,
                            remaining_base_quantity=target_base_quantity-(confirmed_base_quantity+$2),
                            status=CASE
                                WHEN target_base_quantity <= confirmed_base_quantity+$2 THEN 'HEDGED'
                                WHEN confirmed_base_quantity+$2 > 0 THEN 'PARTIALLY_HEDGED'
                                ELSE 'OPEN'
                            END,
                            updated_at=clock_timestamp()
                        WHERE obligation_id=$1::uuid
                          AND reserved_base_quantity >= $2
                        """,
                        item["obligation_id"], take,
                    )
                    allocation = await conn.fetchrow(
                        """
                        INSERT INTO public.execution_hedge_allocations (
                            session_id, reservation_id, futures_fill_id,
                            obligation_id, allocated_base_quantity,
                            idempotency_key, actor_id
                        ) VALUES ($1,$2::uuid,$3::uuid,$4::uuid,$5,$6,$7)
                        RETURNING allocation_id
                        """,
                        session_id, reservation_id, futures_fill_id,
                        item["obligation_id"], take, f"{key}:{item['obligation_id']}", actor,
                    )
                    allocation_rows.append({
                        "allocation_id": str(allocation["allocation_id"]),
                        "obligation_id": str(item["obligation_id"]),
                        "allocated_base_quantity": _fmt(take),
                    })
                    remaining_fill -= take
                    if remaining_fill == 0:
                        break
                if remaining_fill != 0:
                    raise HedgeObligationError("reservation items cannot cover the confirmed Futures fill")
                new_allocated_total = allocated_total + base_quantity
                new_reservation_status = (
                    "CONSUMED" if new_allocated_total == reserved_total else "PARTIALLY_FILLED"
                )
                await conn.execute(
                    """
                    UPDATE public.execution_hedge_reservations
                    SET allocated_base_quantity=$2, status=$3, updated_at=clock_timestamp()
                    WHERE reservation_id=$1::uuid
                    """,
                    reservation_id, new_allocated_total, new_reservation_status,
                )
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_FILL_ALLOCATED", idempotency_key=key,
                    actor_id=actor, generation=fencing_generation,
                    payload={
                        "reservation_id": reservation_id,
                        "futures_fill_id": futures_fill_id,
                        "allocated_base_quantity": _fmt(base_quantity),
                        "allocations": allocation_rows,
                    },
                )
                return {
                    "duplicate": False, "allocated_base_quantity": _fmt(base_quantity),
                    "allocations": allocation_rows, "reservation_status": new_reservation_status,
                    "aggregate_version": version,
                }

    async def release(
        self, *, session_id: str, reservation_id: str,
        actor_id: str, event_idempotency_key: str,
        terminal_order_verified: bool = False, fills_reconciled: bool = False,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Release only proven-unfilled quantity; unknown orders remain reserved."""
        actor = _text(actor_id, "actor_id")
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
                status = str(reservation["status"])
                if status == "RELEASED":
                    return {"duplicate": True, "status": status,
                            "aggregate_version": int(session["aggregate_version"])}
                if status in {"CONSUMED", "RECONCILIATION_REQUIRED"}:
                    raise HedgeObligationError(f"cannot release reservation in {status} state")
                if status != "RESERVED" and not (terminal_order_verified and fills_reconciled):
                    raise HedgeObligationError("submitted reservation release requires terminal order and fill proof")
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
                    await conn.execute(
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
                    released_total += unfilled
                await conn.execute(
                    """
                    UPDATE public.execution_hedge_reservations
                    SET status='RELEASED', updated_at=clock_timestamp()
                    WHERE reservation_id=$1::uuid
                    """,
                    reservation_id,
                )
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type="HEDGE_RESERVATION_RELEASED",
                    idempotency_key=key, actor_id=actor,
                    generation=fencing_generation,
                    payload={
                        "reservation_id": reservation_id,
                        "released_base_quantity": _fmt(released_total),
                    },
                )
                return {
                    "duplicate": False, "status": "RELEASED",
                    "released_base_quantity": _fmt(released_total),
                    "aggregate_version": version,
                }
