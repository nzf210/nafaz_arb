"""Read-only reconciliation worker for durable V3 mutation intents.

This worker never submits or cancels orders. It only fetches exact venue state,
rebinds ownership to a current generation, and advances the durable intent when
venue evidence is authoritative.
"""
from __future__ import annotations

from typing import Any, Mapping

from executor.global_stop import GlobalStopError
from executor.order_intent_service import OrderIntentService


class MutationIntentRecoveryWorker:
    TERMINAL_STATES = {"canceled", "cancelled", "closed", "filled", "rejected", "expired"}

    def __init__(
        self, *, global_stop: Any, intent_service: OrderIntentService,
        venue_resolver: Any, fill_reconciler: Any = None,
        actor_id: str = "executor-intent-recovery",
    ):
        if global_stop is None or intent_service is None or not callable(venue_resolver):
            raise ValueError("global_stop, intent_service and venue_resolver are required")
        self.global_stop = global_stop
        self.intent_service = intent_service
        self.venue_resolver = venue_resolver
        self.fill_reconciler = fill_reconciler
        self.actor_id = str(actor_id or "executor-intent-recovery")

    async def reconcile_one(self, intent_id: str) -> dict:
        intent = await self.intent_service.get_intent(intent_id)
        if intent is None:
            return {"intent_id": intent_id, "outcome": "MISSING"}
        status = str(intent["status"]).upper()
        if status == "PREPARED":
            return {"intent_id": intent_id, "status": status, "outcome": "NOT_SUBMITTED"}
        if status not in {"SUBMITTING", "UNKNOWN", "ACKNOWLEDGED"}:
            return {"intent_id": intent_id, "status": status, "outcome": "TERMINAL"}

        try:
            venue_client = self.venue_resolver(intent)
            if hasattr(venue_client, "__await__"):
                venue_client = await venue_client
        except Exception as exc:
            return self._result(intent, "UNRESOLVED", f"venue resolution failed: {exc}")
        if venue_client is None:
            return self._result(intent, "UNRESOLVED", "venue client is unavailable")

        venue = str(intent["venue"]).strip().lower()
        market_type = str(intent["market_type"]).strip().lower()
        local = getattr(venue_client, "local", None)
        resolved_venue = str(
            getattr(local, "id", "") or getattr(venue_client, "id", "")
            or getattr(venue_client, "exchange", "")
        ).strip().lower()
        if resolved_venue != venue:
            return self._result(intent, "UNRESOLVED", "resolved venue does not match intent venue")

        fetch_order = (
            getattr(venue_client, "fetch_local_order", None)
            if market_type == "spot" else None
        ) or getattr(venue_client, "fetch_order", None)
        lookup = (
            getattr(venue_client, "fetch_local_order_by_client_id", None)
            if market_type == "spot" else None
        ) or getattr(venue_client, "fetch_order_by_client_id", None)
        if not callable(fetch_order):
            return self._result(intent, "UNRESOLVED", "exact venue fetch capability is unavailable")

        try:
            async with self.global_stop.mutation_reconciliation_fence(
                intent_id=intent_id, intent_service=self.intent_service,
                actor_id=self.actor_id,
            ) as (_conn, lease, current):
                order_id, client_order_id = self._target(current)
                symbol = str(current["instrument"])
                if not order_id:
                    if not client_order_id or not callable(lookup):
                        return self._result(current, "UNRESOLVED", "no exact order ID or client-ID lookup capability")
                    order = await lookup(client_order_id, symbol)
                else:
                    order = await fetch_order(order_id, symbol)

                if not isinstance(order, Mapping) or not (order.get("id") or order.get("orderId")):
                    if str(current["status"]).upper() != "UNKNOWN":
                        current = await self.intent_service.mark_unknown(
                            intent_id, lease=lease, actor_id=self.actor_id,
                            error="exact venue lookup returned NOT_FOUND; absence is not proof of rejection",
                            event_idempotency_key=self._event_key(current, lease, "not-found"),
                        )
                    return self._result(current, "UNRESOLVED", "venue order is not currently observable")

                observed_id = str(order.get("id") or order.get("orderId") or "")
                observed_client_id = str(order.get("clientOrderId") or order.get("client_order_id") or "")
                if order_id and observed_id != order_id:
                    current = await self._mark_unknown_if_needed(
                        current, lease, "venue lookup returned a different exchange order ID",
                    )
                    return self._result(current, "UNRESOLVED", "venue order ID mismatch")
                if client_order_id and observed_client_id and observed_client_id != client_order_id:
                    current = await self._mark_unknown_if_needed(
                        current, lease, "venue lookup returned a different client order ID",
                    )
                    return self._result(current, "UNRESOLVED", "venue client order ID mismatch")

                current_status = str(current["status"]).upper()
                final_status = str(order.get("status") or "").strip().lower()
                exchange_order_id = observed_id
                if current_status in {"SUBMITTING", "UNKNOWN"}:
                    current = await self.intent_service.mark_acknowledged(
                        intent_id, lease=lease, actor_id=self.actor_id,
                        exchange_order_id=exchange_order_id, venue_response=dict(order),
                        event_idempotency_key=self._event_key(current, lease, "ack"),
                    )
                    current_status = "ACKNOWLEDGED"

                if final_status == "rejected" and str(current["operation"]).upper() == "CREATE":
                    current = await self.intent_service.mark_rejected(
                        intent_id, lease=lease, actor_id=self.actor_id,
                        error="venue reconciliation proves definitive rejected order state",
                        definitive=True,
                        event_idempotency_key=self._event_key(current, lease, "rejected"),
                    )
                    return self._result(current, "RESOLVED", "venue definitively rejected the create order")

                if final_status in self.TERMINAL_STATES:
                    fills_reconciled = await self._fills_reconciled(current, order)
                    if fills_reconciled:
                        if str(current["operation"]).upper() == "CANCEL":
                            current = await self.intent_service.mark_cancel_confirmed(
                                intent_id, lease=lease, actor_id=self.actor_id,
                                final_status=final_status, fresh_fetch=True,
                                fills_reconciled=True, exchange_order_id=exchange_order_id,
                                venue_response=dict(order),
                                event_idempotency_key=self._event_key(current, lease, "cancel-terminal"),
                            )
                        else:
                            current = await self.intent_service.mark_completed(
                                intent_id, lease=lease, actor_id=self.actor_id,
                                terminal_status=final_status, fills_reconciled=True,
                                event_idempotency_key=self._event_key(current, lease, "create-terminal"),
                            )
                        return self._result(current, "RESOLVED", f"terminal venue state reconciled: {final_status}")
                    return self._result(
                        current, "UNRESOLVED",
                        "terminal order observed but fill reconciliation is incomplete",
                    )

                if final_status in {"open", "new", "partially_filled", "pending"}:
                    return self._result(current, "ACKNOWLEDGED", "venue confirms the order remains active")
                return self._result(
                    current, "UNRESOLVED",
                    f"venue order status is not classifiable: {final_status or 'missing'}",
                )
        except GlobalStopError as exc:
            return self._result(intent, "BUSY", str(exc))
        except Exception as exc:
            return self._result(intent, "UNRESOLVED", f"reconciliation failed: {exc}")

    async def reconcile_batch(self, *, limit: int = 100) -> list[dict]:
        intents = await self.intent_service.list_unresolved(limit=limit)
        results = []
        for intent in intents:
            if str(intent["status"]).upper() in {"SUBMITTING", "UNKNOWN", "ACKNOWLEDGED"}:
                results.append(await self.reconcile_one(str(intent["intent_id"])))
        return results

    @staticmethod
    def _target(intent: Mapping[str, Any]) -> tuple[str, str]:
        if str(intent["operation"]).upper() == "CREATE":
            return (
                str(intent.get("exchange_order_id") or "").strip(),
                str(intent.get("client_order_id") or "").strip(),
            )
        return (
            str(intent.get("target_order_id") or "").strip(),
            str(intent.get("target_client_order_id") or "").strip(),
        )

    async def _fills_reconciled(self, intent: Mapping[str, Any], order: Mapping[str, Any]) -> bool:
        if not callable(self.fill_reconciler):
            return False
        try:
            result = self.fill_reconciler(intent, order, order)
            if hasattr(result, "__await__"):
                result = await result
            return bool(result)
        except Exception:
            return False

    async def _mark_unknown_if_needed(self, intent: Mapping[str, Any], lease: Any, reason: str) -> dict:
        if str(intent["status"]).upper() == "UNKNOWN":
            return dict(intent)
        return await self.intent_service.mark_unknown(
            str(intent["intent_id"]), lease=lease, actor_id=self.actor_id,
            error=reason, event_idempotency_key=self._event_key(intent, lease, "unknown"),
        )

    @staticmethod
    def _event_key(intent: Mapping[str, Any], lease: Any, event: str) -> str:
        return (
            f"{intent['intent_id']}:reconcile:{int(lease.generation)}:"
            f"{int(intent['state_version'])}:{event}"
        )

    @staticmethod
    def _result(intent: Mapping[str, Any], outcome: str, reason: str = "") -> dict:
        return {
            "intent_id": str(intent["intent_id"]),
            "operation": str(intent["operation"]),
            "status": str(intent["status"]),
            "outcome": outcome,
            "reason": reason,
        }
