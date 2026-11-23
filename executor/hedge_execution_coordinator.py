"""Obligation-backed V3 Futures hedge execution through the single mutation gateway.

This coordinator never talks to an exchange directly. It converts a persisted
liquidity decision into a durable reservation and CREATE intent, then delegates
the external mutation to MutationGateway.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from executor.execution_hedge_obligations import ExecutionHedgeObligationService
from executor.hedge_liquidity_evaluator import HedgeLiquidityDecision
from executor.hedge_scheduler import HedgeScheduler
from executor.mutation_gateway_types import MutationResultType
from executor.order_intent_service import OrderIntentService


class HedgeExecutionError(RuntimeError):
    """A liquidity decision cannot safely become a durable hedge order."""


class HedgeExecutionCoordinator:
    def __init__(
        self, pool: Any, *, gateway: Any,
        order_intents: OrderIntentService | None = None,
        obligations: ExecutionHedgeObligationService | None = None,
        scheduler: HedgeScheduler | None = None,
    ) -> None:
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        if gateway is None or not callable(getattr(gateway, "execute_create_intent", None)):
            raise ValueError("the centralized MutationGateway is required")
        self.pool = pool
        self.gateway = gateway
        self.order_intents = order_intents or OrderIntentService(pool)
        self.obligations = obligations or ExecutionHedgeObligationService(pool)
        self.scheduler = scheduler or HedgeScheduler(pool)

    async def execute_decision(
        self, *, client: Any, session_id: str, obligation_id: str,
        decision: HedgeLiquidityDecision, actor_id: str,
        idempotency_key: str, coordinator_lease: Any,
        fencing_generation: int, risk_metrics: Mapping[str, Any],
        risk_limits: Mapping[str, Any] | None = None,
    ) -> dict:
        actor = str(actor_id or "").strip()
        key = str(idempotency_key or "").strip()
        if not actor or not key or coordinator_lease is None:
            raise HedgeExecutionError("actor, idempotency key, and coordinator lease are required")
        if not isinstance(risk_metrics, Mapping):
            raise HedgeExecutionError("risk_metrics are required for a Futures hedge mutation")

        scheduled = await self.scheduler.record_decision(
            session_id=session_id, obligation_id=obligation_id,
            decision=decision, actor_id=actor,
            idempotency_key=f"{key}:decision",
            coordinator_lease=coordinator_lease,
            fencing_generation=fencing_generation,
        )
        action = str(decision.action).upper()
        if action not in {"SUBMIT", "PARTIAL_SUBMIT"}:
            return {"scheduled": scheduled, "submitted": False, "reason": decision.reason}

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT o.account_scope_id, o.pair_key, o.futures_leg_id,
                       o.futures_venue, o.futures_market_type, o.futures_symbol,
                       o.base_asset, o.position_side, o.contract_type,
                       o.contract_size, o.quantity_unit, o.base_per_unit,
                       l.quote_asset, l.settlement_asset
                FROM public.execution_hedge_obligations o
                JOIN public.execution_legs l
                  ON l.session_id=o.session_id AND l.leg_id=o.futures_leg_id
                WHERE o.session_id=$1 AND o.obligation_id=$2::uuid
                """,
                session_id, obligation_id,
            )
        if row is None:
            raise HedgeExecutionError("durable hedge obligation or paired Futures leg is missing")
        if str(row["position_side"]).upper() != "SHORT":
            raise HedgeExecutionError("this Spot-first executor only creates Futures SHORT hedge orders")
        base_per_unit = Decimal(str(row["base_per_unit"]))
        native_quantity = Decimal(str(decision.executable_native_quantity))
        base_quantity = Decimal(str(decision.executable_base_quantity))
        if native_quantity <= 0 or base_quantity <= 0 or native_quantity * base_per_unit != base_quantity:
            raise HedgeExecutionError("liquidity decision native/base quantities do not match the admitted contract")

        reservation = await self.obligations.reserve(
            session_id=session_id,
            futures_venue=str(row["futures_venue"]),
            futures_market_type=str(row["futures_market_type"]),
            futures_symbol=str(row["futures_symbol"]),
            base_asset=str(row["base_asset"]),
            position_side=str(row["position_side"]),
            contract_type=row["contract_type"],
            contract_size=row["contract_size"],
            quantity_unit=str(row["quantity_unit"]),
            base_per_unit=base_per_unit,
            requested_base_quantity=base_quantity,
            minimum_base_quantity=base_quantity,
            idempotency_key=f"{key}:reservation",
            actor_id=actor,
            coordinator_lease=coordinator_lease,
            fencing_generation=fencing_generation,
        )
        if reservation.get("reserved") is False:
            return {
                "scheduled": scheduled, "submitted": False,
                "reservation": reservation,
            }
        reservation_id = str(reservation["reservation_id"])
        if reservation.get("duplicate") and str(reservation.get("status") or "").upper() not in {
            "RESERVED", "SUBMITTED",
        }:
            return {
                "scheduled": scheduled, "submitted": False,
                "reservation": reservation,
                "reason": "existing reservation is already in flight or terminal; no duplicate order",
            }

        quote_asset = str(row["settlement_asset"] or row["quote_asset"] or "").upper()
        if not quote_asset:
            raise HedgeExecutionError("Futures settlement/quote asset is missing")
        intent = await self.order_intents.prepare_create(
            account_scope_id=str(row["account_scope_id"]),
            idempotency_key=f"{key}:intent",
            venue=str(row["futures_venue"]),
            market_type=str(row["futures_market_type"]),
            instrument=str(row["futures_symbol"]),
            pair_key=str(row["pair_key"]),
            side="sell",
            order_type="limit",
            quantity=native_quantity,
            price=Decimal(str(decision.executable_vwap)),
            actor_id=actor,
            session_id=session_id,
            leg_id=str(row["futures_leg_id"]),
            reserved_base_qty=base_quantity,
            reserved_notional=Decimal(str(decision.executable_notional_quote)),
            reservation_asset=quote_asset,
            request_metadata={
                "action_class": "HEDGE",
                "hedge_reservation_id": reservation_id,
                "hedge_obligation_id": obligation_id,
                "hedge_base_quantity": str(base_quantity),
                "hedge_base_per_unit": str(base_per_unit),
                "risk_metrics": dict(risk_metrics),
                "risk_limits": dict(risk_limits or {}),
            },
        )
        intent_status = str(intent.get("status") or "").upper()
        if intent_status in {"REJECTED", "ABORTED"}:
            released = await self.obligations.release(
                session_id=session_id, reservation_id=reservation_id,
                actor_id=actor, event_idempotency_key=f"{key}:reservation-released",
                terminal_order_verified=True, fills_reconciled=True,
                coordinator_lease=coordinator_lease,
                fencing_generation=fencing_generation,
            )
            return {
                "scheduled": scheduled, "reservation": reservation,
                "intent": intent, "submitted": False,
                "reason": f"terminal intent {intent_status}; reservation released",
                "release": released,
            }
        submitted = await self.obligations.mark_submitted(
            session_id=session_id, reservation_id=reservation_id,
            client_order_id=str(intent["client_order_id"]),
            actor_id=actor, event_idempotency_key=f"{key}:reservation-submitted",
            coordinator_lease=coordinator_lease,
            fencing_generation=fencing_generation,
        )
        result = await self.gateway.execute_create_intent(
            client, intent_id=str(intent["intent_id"]), actor_id=actor,
        )

        if result.status == MutationResultType.SUBMISSION_UNKNOWN:
            try:
                await self.obligations.mark_unknown(
                    session_id=session_id, reservation_id=reservation_id,
                    reason=result.error_message or "ambiguous HEDGE submit",
                    actor_id=actor, event_idempotency_key=f"{key}:reservation-unknown",
                    coordinator_lease=coordinator_lease,
                    fencing_generation=fencing_generation,
                )
            except Exception:
                # The reservation is already durable as SUBMITTED; failure to
                # annotate UNKNOWN must never release its reserved exposure.
                pass
        elif result.status == MutationResultType.REJECTED:
            await self.obligations.release(
                session_id=session_id, reservation_id=reservation_id,
                actor_id=actor, event_idempotency_key=f"{key}:reservation-released",
                terminal_order_verified=True, fills_reconciled=True,
                coordinator_lease=coordinator_lease,
                fencing_generation=fencing_generation,
            )
        return {
            "scheduled": scheduled,
            "reservation": {**reservation, **submitted, "reservation_id": reservation_id},
            "intent": intent,
            "mutation": result,
            "submitted": result.status == MutationResultType.CONFIRMED,
        }
