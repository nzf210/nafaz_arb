"""Fail-closed exact-order cancellation and terminal verification."""
from __future__ import annotations

import logging
from typing import Any, Optional

from executor.order_intent_service import OrderIntentService

from executor.mutation_gateway_types import MutationResult, MutationResultType

logger = logging.getLogger("executor.mutation_gateway")


class MutationGatewayCancelMixin:
    async def execute_cancel_intent(
        self, client: Any, *, intent_id: str, actor_id: str = "executor",
        fill_reconciler: Any = None,
    ) -> MutationResult:
        """Execute a STOP-safe cancel intent and require terminal/fill evidence."""
        service = getattr(self, "order_intent_service", None)
        if service is None:
            global_stop = getattr(self, "global_stop", None)
            if global_stop is None or not await global_stop.ensure_pool():
                return MutationResult(
                    status=MutationResultType.REJECTED, client_order_id="",
                    error_message="durable cancel intent repository is unavailable",
                )
            service = OrderIntentService(global_stop.pool)
            self.order_intent_service = service
        try:
            intent = await service.get_intent(intent_id)
        except Exception as exc:
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id="",
                error_message=f"durable cancel intent lookup failed: {exc}",
            )
        if intent is None or str(intent["operation"]).upper() != "CANCEL":
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id="",
                error_message="CANCEL mutation intent does not exist",
            )
        order_id = str(intent["target_order_id"] or "").strip()
        target_client_id = str(intent["target_client_order_id"] or "").strip()
        result_client_id = target_client_id or order_id or str(intent_id)
        status = str(intent["status"]).upper()
        if status == "CANCEL_CONFIRMED":
            return MutationResult(
                status=MutationResultType.CONFIRMED,
                client_order_id=result_client_id,
                exchange_order_id=str(intent["exchange_order_id"] or order_id) or None,
            )
        if status in {"SUBMITTING", "UNKNOWN", "ACKNOWLEDGED"}:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=result_client_id,
                exchange_order_id=str(intent["exchange_order_id"] or order_id) or None,
                error_message="cancel outcome is unresolved; reconcile exact venue order state before retry",
            )
        if status in {"REJECTED", "ABORTED"}:
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                exchange_order_id=order_id or None,
                error_message=str(intent.get("last_error") or status),
            )
        if status != "PREPARED":
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                error_message=f"unsupported CANCEL intent state: {status}",
            )

        exchange = str(intent["venue"]).strip().lower()
        market_type = str(intent["market_type"]).strip().lower()
        symbol = str(intent["instrument"]).strip()
        resolver = getattr(client, "for_venue", None)
        try:
            venue_client = resolver(exchange, market_type) if callable(resolver) else client
            if hasattr(venue_client, "__await__"):
                venue_client = await venue_client
        except Exception as exc:
            await service.mark_aborted(
                intent_id, actor_id=actor_id,
                reason=f"venue client resolution failed before cancel: {exc}",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                error_message=f"venue client resolution failed: {exc}",
            )
        if venue_client is None:
            await service.mark_aborted(
                intent_id, actor_id=actor_id, reason="requested venue client is not configured",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                error_message=f"no configured client for {exchange}/{market_type}",
            )
        local = getattr(venue_client, "local", None)
        resolved_venue = str(
            getattr(local, "id", "") or getattr(venue_client, "id", "")
            or getattr(venue_client, "exchange", "")
        ).strip().lower()
        if resolved_venue != exchange:
            await service.mark_aborted(
                intent_id, actor_id=actor_id,
                reason="resolved venue client does not match immutable cancel intent",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                error_message=f"resolved venue mismatch: expected {exchange}, got {resolved_venue or 'unknown'}",
            )

        fetch_order = (
            getattr(venue_client, "fetch_local_order", None)
            if market_type == "spot" else None
        ) or getattr(venue_client, "fetch_order", None)
        cancel_order = (
            getattr(venue_client, "cancel_local_order", None)
            if market_type == "spot" else None
        ) or getattr(venue_client, "cancel_order", None)
        lookup = (
            getattr(venue_client, "fetch_local_order_by_client_id", None)
            if market_type == "spot" else None
        ) or getattr(venue_client, "fetch_order_by_client_id", None)
        if not callable(fetch_order) or not callable(cancel_order) or (not order_id and not callable(lookup)):
            await service.mark_aborted(
                intent_id, actor_id=actor_id,
                reason="exact cancel/fetch capability is unavailable before venue boundary",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                error_message="venue client lacks exact cancel/fetch capability",
            )

        global_stop = getattr(self, "global_stop", None)
        if global_stop is None:
            await service.mark_aborted(
                intent_id, actor_id=actor_id, reason="global mutation fence is unavailable",
            )
            return MutationResult(
                status=MutationResultType.REJECTED, client_order_id=result_client_id,
                error_message="global mutation fence is unavailable",
            )

        lease = None
        mutation_started = False
        try:
            async with global_stop.cancel_submit_fence(
                intent_id=intent_id, intent_service=service, actor_id=actor_id,
            ) as (_fence_conn, lease):
                mutation_started = True
                if not order_id:
                    found = await lookup(target_client_id, symbol)
                    if not isinstance(found, dict) or not found.get("id"):
                        raise RuntimeError("exact order lookup by client ID returned no order")
                    found_client_id = str(found.get("clientOrderId") or found.get("client_order_id") or "")
                    if found_client_id and found_client_id != target_client_id:
                        raise RuntimeError("client-ID lookup returned a different venue order")
                    order_id = str(found["id"])

                before = await fetch_order(order_id, symbol)
                if not isinstance(before, dict):
                    raise RuntimeError("pre-cancel fetch returned no authoritative order state")
                before_id = str(before.get("id") or before.get("orderId") or "")
                if before_id and before_id != order_id:
                    raise RuntimeError("pre-cancel fetch returned a different venue order ID")
                before_status = str(before.get("status") or "").strip().lower()
                terminal_states = {
                    "canceled", "cancelled", "closed", "filled", "rejected", "expired",
                }
                if before_status not in terminal_states and before_status not in {
                    "open", "new", "partially_filled", "pending",
                }:
                    raise RuntimeError(f"pre-cancel order state is not classifiable: {before_status or 'missing'}")

                cancel_error = None
                cancel_response = None
                if before_status not in terminal_states:
                    try:
                        cancel_response = await cancel_order(order_id, symbol)
                    except Exception as exc:
                        cancel_error = exc
                        logger.warning(
                            "V3 cancel acknowledgement uncertain on %s/%s: %s",
                            exchange, order_id, exc,
                        )

                after = before
                if before_status not in terminal_states:
                    after = await fetch_order(order_id, symbol)
                if not isinstance(after, dict):
                    raise RuntimeError("post-cancel fetch returned no authoritative order state")
                final_id = str(after.get("id") or after.get("orderId") or "")
                if final_id and final_id != order_id:
                    raise RuntimeError("post-cancel fetch returned a different venue order ID")
                final_status = str(after.get("status") or "").strip().lower()

                if final_status in terminal_states:
                    fills_reconciled = False
                    if callable(fill_reconciler):
                        try:
                            reconciled = fill_reconciler(intent, before, after)
                            if hasattr(reconciled, "__await__"):
                                reconciled = await reconciled
                            fills_reconciled = bool(reconciled)
                        except Exception:
                            logger.exception("V3 cancel fill reconciliation failed")
                    if fills_reconciled:
                        confirmed = await service.mark_cancel_confirmed(
                            intent_id, lease=lease, actor_id=actor_id,
                            final_status=final_status, fresh_fetch=True,
                            fills_reconciled=True,
                        )
                        if market_type == "spot" and target_client_id:
                            await self.slot_manager.release_slot(
                                exchange, target_client_id,
                                reason=f"V3_CANCEL_TERMINAL_{final_status.upper()}",
                            )
                        return MutationResult(
                            status=MutationResultType.CONFIRMED,
                            client_order_id=result_client_id,
                            exchange_order_id=order_id,
                            error_message=f"verified terminal order state: {final_status}",
                            raw_response=after,
                        )
                    await service.mark_unknown(
                        intent_id, lease=lease, actor_id=actor_id,
                        error="terminal venue state observed but racing fills are not reconciled",
                    )
                    return MutationResult(
                        status=MutationResultType.SUBMISSION_UNKNOWN,
                        client_order_id=result_client_id, exchange_order_id=order_id,
                        error_message="terminal order state observed; fill reconciliation is still required",
                        raw_response=after,
                    )

                if final_status in {"open", "new", "partially_filled", "pending"}:
                    if cancel_error is not None and self._is_definitive_order_rejection(cancel_error):
                        await service.mark_rejected(
                            intent_id, lease=lease, actor_id=actor_id,
                            error=str(cancel_error), definitive=True,
                        )
                        return MutationResult(
                            status=MutationResultType.REJECTED,
                            client_order_id=result_client_id, exchange_order_id=order_id,
                            error_message=f"venue definitively rejected cancel: {cancel_error}",
                            raw_response=after,
                        )
                    if cancel_error is None:
                        await service.mark_acknowledged(
                            intent_id, lease=lease, actor_id=actor_id,
                            exchange_order_id=order_id,
                            venue_response={"cancel_response": cancel_response, "final_order": after},
                        )
                    else:
                        await service.mark_unknown(
                            intent_id, lease=lease, actor_id=actor_id,
                            error=f"cancel response ambiguous and order remains active: {cancel_error}",
                        )
                    return MutationResult(
                        status=MutationResultType.SUBMISSION_UNKNOWN,
                        client_order_id=result_client_id, exchange_order_id=order_id,
                        error_message="cancel is not terminal; keep the intent unresolved and do not retry",
                        raw_response=after,
                    )
                raise RuntimeError(f"post-cancel order state is not classifiable: {final_status or 'missing'}")
        except Exception as exc:
            if mutation_started and lease is not None:
                try:
                    current = await service.get_intent(intent_id)
                    if current and current["status"] == "SUBMITTING":
                        await service.mark_unknown(
                            intent_id, lease=lease, actor_id=actor_id, error=str(exc),
                        )
                except Exception:
                    logger.exception("failed to persist V3 cancel UNKNOWN outcome")
                return MutationResult(
                    status=MutationResultType.SUBMISSION_UNKNOWN,
                    client_order_id=result_client_id, exchange_order_id=order_id or None,
                    error_message=f"cancel outcome requires reconciliation: {exc}",
                )
            try:
                current = await service.get_intent(intent_id)
                if current and current["status"] == "PREPARED":
                    await service.mark_aborted(
                        intent_id, actor_id=actor_id,
                        reason=f"cancel fence failed before venue boundary: {exc}",
                    )
            except Exception:
                logger.exception("failed to abort pre-submit V3 cancel intent")
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=result_client_id, exchange_order_id=order_id or None,
                error_message=f"cancel blocked before venue boundary: {exc}",
            )

    async def execute_cancel_order(
        self,
        client: Any,
        *,
        exchange: str,
        market_type: str,
        order_id: str,
        symbol: str,
        client_order_id: Optional[str] = None,
    ) -> MutationResult:
        """Cancel an exact venue order and confirm its terminal state.

        A successful cancel HTTP response is only an acknowledgement. The order
        is considered complete only after a fresh venue read proves it terminal.
        Unknown outcomes retain the Spot slot for reconciliation.
        """
        exchange = str(exchange or "").strip().lower()
        market_type = str(market_type or "").strip().lower()
        if market_type in {"future", "perpetual"}:
            market_type = "futures"
        order_id = str(order_id or "").strip()
        client_order_id = str(client_order_id or "").strip()
        symbol = str(symbol or "").strip()
        cid = client_order_id or order_id

        def result(
            status: MutationResultType,
            *,
            resolved_order_id: str = "",
            detail: str = "",
            raw: Optional[dict] = None,
        ) -> MutationResult:
            return MutationResult(
                status=status,
                client_order_id=cid,
                exchange_order_id=resolved_order_id or order_id or None,
                error_message=detail,
                raw_response=raw,
            )

        if not exchange or market_type != "spot" or not symbol:
            return result(
                MutationResultType.REJECTED,
                detail="cancel requires an exact exchange, spot market, and symbol",
            )
        if not order_id and not client_order_id:
            return result(
                MutationResultType.REJECTED,
                detail="exchange_order_id or client_order_id is required",
            )

        # Resolve the requested venue/market explicitly. Never let a generic
        # facade silently route a cancel to whichever account happens to be first.
        resolver = getattr(client, "for_venue", None)
        if callable(resolver):
            try:
                venue_client = resolver(exchange, market_type)
            except Exception as exc:
                return result(
                    MutationResultType.REJECTED,
                    detail=f"venue resolution failed for {exchange}/{market_type}: {exc}",
                )
            if venue_client is None:
                return result(
                    MutationResultType.REJECTED,
                    detail=f"no configured {market_type} client for venue {exchange}",
                )
        else:
            local = getattr(client, "local", None)
            local_venue = str(getattr(local, "id", "") or "").lower()
            if local_venue != exchange:
                return result(
                    MutationResultType.REJECTED,
                    detail=f"client is not bound to requested spot venue {exchange}",
                )
            venue_client = client

        local = getattr(venue_client, "local", None)
        resolved_venue = str(getattr(local, "id", "") or "").lower()
        if resolved_venue != exchange:
            return result(
                MutationResultType.REJECTED,
                detail=f"resolved spot client venue mismatch: expected {exchange}, got {resolved_venue or 'unknown'}",
            )

        fetch_order = getattr(venue_client, "fetch_local_order", None)
        cancel_order = getattr(venue_client, "cancel_local_order", None)
        if not callable(fetch_order) or not callable(cancel_order):
            return result(
                MutationResultType.REJECTED,
                detail=f"spot client for {exchange} lacks cancel/fetch order capability",
            )

        resolved_order_id = order_id
        try:
            if not resolved_order_id:
                lookup = getattr(venue_client, "fetch_local_order_by_client_id", None)
                if not callable(lookup):
                    return result(
                        MutationResultType.REJECTED,
                        detail=f"spot client for {exchange} cannot resolve client order IDs",
                    )
                found = await lookup(client_order_id, symbol)
                if not isinstance(found, dict) or not found.get("id"):
                    return result(
                        MutationResultType.REJECTED,
                        detail=f"order not found for client_order_id {client_order_id}",
                    )
                resolved_order_id = str(found["id"])

            before = await fetch_order(resolved_order_id, symbol)
            if not isinstance(before, dict):
                return result(
                    MutationResultType.SUBMISSION_UNKNOWN,
                    resolved_order_id=resolved_order_id,
                    detail="pre-cancel order lookup returned no authoritative order state",
                )
            observed_id = str(before.get("id") or before.get("orderId") or "")
            if observed_id and observed_id != resolved_order_id:
                return result(
                    MutationResultType.REJECTED,
                    resolved_order_id=resolved_order_id,
                    detail=f"pre-cancel lookup returned a different order ID: {observed_id}",
                )

            terminal_states = {
                "canceled", "cancelled", "closed", "filled", "rejected", "expired",
            }
            before_status = str(before.get("status") or "").strip().lower()
            if before_status in terminal_states:
                if market_type == "spot" and client_order_id:
                    await self.slot_manager.release_slot(
                        exchange, client_order_id, reason=f"TERMINAL_{before_status.upper()}"
                    )
                return result(
                    MutationResultType.CONFIRMED,
                    resolved_order_id=resolved_order_id,
                    detail=f"order was already terminal ({before_status})",
                    raw=before,
                )
            if before_status not in {"open", "new", "partially_filled", "pending"}:
                return result(
                    MutationResultType.SUBMISSION_UNKNOWN,
                    resolved_order_id=resolved_order_id,
                    detail=f"pre-cancel order status is not safely classifiable: {before_status or 'missing'}",
                    raw=before,
                )

            cancel_error = ""
            try:
                await cancel_order(resolved_order_id, symbol)
            except Exception as exc:
                # A timeout/disconnect may happen after the venue accepted the
                # cancel. Always read the order again before classifying outcome.
                cancel_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "Cancel acknowledgement uncertain on %s (%s): %s",
                    exchange, resolved_order_id, cancel_error,
                )

            try:
                after = await fetch_order(resolved_order_id, symbol)
            except Exception as exc:
                return result(
                    MutationResultType.SUBMISSION_UNKNOWN,
                    resolved_order_id=resolved_order_id,
                    detail=(
                        "cancel was sent but terminal state could not be verified: "
                        f"{type(exc).__name__}: {exc}"
                        + (f"; cancel error: {cancel_error}" if cancel_error else "")
                    ),
                )

            if not isinstance(after, dict):
                return result(
                    MutationResultType.SUBMISSION_UNKNOWN,
                    resolved_order_id=resolved_order_id,
                    detail="cancel was sent but venue returned no final order state",
                )
            final_id = str(after.get("id") or after.get("orderId") or "")
            if final_id and final_id != resolved_order_id:
                return result(
                    MutationResultType.SUBMISSION_UNKNOWN,
                    resolved_order_id=resolved_order_id,
                    detail=f"final lookup returned a different order ID: {final_id}",
                    raw=after,
                )

            final_status = str(after.get("status") or "").strip().lower()
            if final_status in terminal_states:
                if market_type == "spot" and client_order_id:
                    await self.slot_manager.release_slot(
                        exchange, client_order_id, reason=f"TERMINAL_{final_status.upper()}"
                    )
                return result(
                    MutationResultType.CONFIRMED,
                    resolved_order_id=resolved_order_id,
                    detail=f"verified terminal order state: {final_status}",
                    raw=after,
                )

            return result(
                MutationResultType.SUBMISSION_UNKNOWN,
                resolved_order_id=resolved_order_id,
                detail=(
                    f"cancel outcome is not terminal; venue reports {final_status or 'unknown'}"
                    + (f"; cancel error: {cancel_error}" if cancel_error else "")
                ),
                raw=after,
            )
        except Exception as exc:
            logger.exception("Cancel reconciliation failed on %s (%s)", exchange, resolved_order_id or cid)
            return result(
                MutationResultType.SUBMISSION_UNKNOWN,
                resolved_order_id=resolved_order_id,
                detail=f"cancel/reconciliation outcome is uncertain: {type(exc).__name__}: {exc}",
            )

