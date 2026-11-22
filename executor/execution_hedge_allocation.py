"""Confirmed Futures-fill allocation to reserved Spot obligations."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from executor.execution_fill_service import FillConflict
from executor.execution_hedge_common import (
    HedgeServiceBase, HedgeObligationError, _qty, _text, _fmt,
)


class HedgeAllocationMixin(HedgeServiceBase):
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
                        "session_state": (
                            "PARTIALLY_HEDGED"
                            if str(session["status"]).upper() == "HEDGE_PENDING"
                            else str(session["status"]).upper()
                        ),
                    },
                    target_state=(
                        "PARTIALLY_HEDGED"
                        if str(session["status"]).upper() == "HEDGE_PENDING"
                        else str(session["status"]).upper()
                    ),
                )
                return {
                    "duplicate": False, "allocated_base_quantity": _fmt(base_quantity),
                    "allocations": allocation_rows, "reservation_status": new_reservation_status,
                    "aggregate_version": version,
                }


