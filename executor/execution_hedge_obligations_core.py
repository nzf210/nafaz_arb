"""Spot-fill to durable hedge-obligation projection."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from executor.execution_fill_service import FillConflict
from executor.execution_hedge_common import (
    HedgeServiceBase, HedgeObligationError, _qty, _text, _fmt,
)


class HedgeObligationMixin(HedgeServiceBase):
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
                           spot.venue AS spot_venue, spot.market_type AS spot_market_type,
                           spot.symbol AS spot_symbol,
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
                if (
                    str(fill["spot_leg_type"]) != "SPOT"
                    or str(fill["market_type"]).lower() != "spot"
                    or str(fill["spot_market_type"]).lower() != "spot"
                    or str(fill["venue"]).lower() != str(fill["spot_venue"]).lower()
                    or str(fill["symbol"]) != str(fill["spot_symbol"])
                ):
                    raise HedgeObligationError("source fill is not bound to its exact Spot leg identity")
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
                current_state = str(session["status"]).upper()
                if current_state == "ENTRY_IN_PROGRESS":
                    target_state = "HEDGE_PENDING"
                elif current_state == "HEDGED":
                    target_state = "RECONCILIATION_REQUIRED"
                elif current_state in {"COMPLETED", "FAILED", "ABORTED", "REJECTED", "EXIT_IN_PROGRESS", "EXIT_PARTIAL"}:
                    target_state = "RECONCILIATION_REQUIRED"
                elif current_state in {"HEDGE_PENDING", "PARTIALLY_HEDGED", "BLOCKED", "RECONCILIATION_REQUIRED"}:
                    target_state = current_state
                else:
                    raise HedgeObligationError(f"cannot create a hedge obligation while session is {current_state}")
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
                        "session_state": target_state,
                    },
                    target_state=target_state,
                )
                return {
                    "duplicate": False, "obligation_id": str(inserted["obligation_id"]),
                    "target_base_quantity": _fmt(target), "aggregate_version": version,
                }


