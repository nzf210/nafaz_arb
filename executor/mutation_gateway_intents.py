"""V3 intent-bound create entrypoint for the mutation gateway."""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

from executor.mutation_gateway_types import MutationResult, MutationResultType
from executor.order_intent_service import OrderIntentService


class MutationGatewayIntentMixin:
    async def execute_create_intent(
        self, client: Any, *, intent_id: str, actor_id: str = "executor",
    ) -> MutationResult:
        """Execute only a PREPARED V3 CREATE intent; never retry an ambiguous intent."""
        if self.order_intent_service is None:
            if self.global_stop is None or not await self.global_stop.ensure_pool():
                return MutationResult(
                    status=MutationResultType.REJECTED, client_order_id="",
                    error_message="durable order intent repository is unavailable",
                )
            self.order_intent_service = OrderIntentService(self.global_stop.pool)
        try:
            intent = await self.order_intent_service.get_intent(intent_id)
        except Exception as exc:
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id="",
                error_message=f"durable mutation intent lookup failed: {exc}",
            )
        if intent is None or str(intent["operation"]).upper() != "CREATE":
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id="",
                error_message="CREATE mutation intent does not exist",
            )
        client_order_id = str(intent["client_order_id"] or "")
        status = str(intent["status"]).upper()
        if status in {"SUBMITTING", "UNKNOWN"}:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=client_order_id,
                exchange_order_id=str(intent["exchange_order_id"] or "") or None,
                error_message="intent outcome is unresolved; reconcile exact venue client-order-id before any retry",
            )
        if status in {"ACKNOWLEDGED", "COMPLETED"}:
            return MutationResult(
                status=MutationResultType.CONFIRMED,
                client_order_id=client_order_id,
                exchange_order_id=str(intent["exchange_order_id"] or "") or None,
                raw_response=intent.get("venue_response"),
            )
        if status in {"REJECTED", "ABORTED"}:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=str(intent.get("last_error") or status),
            )
        if status != "PREPARED":
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=client_order_id,
                error_message=f"unsupported CREATE intent state: {status}",
            )

        client_venue = str(getattr(client, "id", None) or getattr(client, "exchange", "")).lower()
        expected_venue = str(intent["venue"]).lower()
        if client_venue and client_venue != expected_venue:
            await self.order_intent_service.mark_aborted(
                intent_id, actor_id=actor_id,
                reason="venue client does not match immutable mutation intent",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=client_order_id,
                error_message="venue client does not match immutable mutation intent",
            )
        request = intent["request_payload"]
        if isinstance(request, str):
            request = json.loads(request)
        metadata = request.get("request_metadata") or {}
        action_class = str(metadata.get("action_class") or "ENTRY").upper()
        if action_class not in {"ENTRY", "HEDGE", "DERISK"}:
            await self.order_intent_service.mark_aborted(
                intent_id, actor_id=actor_id,
                reason=f"unsupported action class for create-intent gateway: {action_class}",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=client_order_id,
                error_message=f"unsupported action class for create-intent gateway: {action_class}",
            )
        result = await self.execute_create_order(
            client,
            exchange=expected_venue,
            market_type=str(intent["market_type"]),
            symbol=str(intent["instrument"]),
            side=str(intent["side"]),
            order_type=str(intent["order_type"]),
            quantity=float(intent["quantity"]),
            price=float(intent["price"]),
            pair_key=str(intent["pair_key"] or ""),
            session_id=str(intent["session_id"] or "") or None,
            account_scope_id=str(intent["account_scope_id"]),
            action_class=action_class,
            mutation_intent_id=intent_id,
            risk_metrics=metadata.get("risk_metrics"),
            risk_limits=metadata.get("risk_limits"),
            extra_params={
                "client_order_id": client_order_id,
                "mutation_intent_id": intent_id,
                "intent_actor_id": actor_id,
                "account_scope_id": str(intent["account_scope_id"]),
                "amount_spec": metadata.get("amount_spec") or {},
                "recovery_context": metadata.get("recovery_context") or {},
            },
        )
        try:
            current = await self.order_intent_service.get_intent(intent_id)
        except Exception as exc:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=client_order_id,
                exchange_order_id=result.exchange_order_id,
                error_message=f"venue mutation may have occurred but intent state lookup failed: {exc}",
                raw_response=result.raw_response,
            )
        current_status = str(current["status"]).upper() if current else "MISSING"
        if result.status == MutationResultType.REJECTED and current_status == "PREPARED":
            try:
                await self.order_intent_service.mark_aborted(
                    intent_id, actor_id=actor_id,
                    reason=result.error_message or "rejected before venue mutation",
                )
            except Exception:
                logger.exception("failed to abort V3 intent rejected before venue boundary")
        if result.status == MutationResultType.CONFIRMED and current_status not in {"ACKNOWLEDGED", "COMPLETED"}:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=client_order_id,
                exchange_order_id=result.exchange_order_id,
                error_message="venue create returned but durable logical intent is not acknowledged",
                raw_response=result.raw_response,
            )
        return result


