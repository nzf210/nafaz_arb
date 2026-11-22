"""Fail-closed exact-order cancellation and terminal verification."""
from __future__ import annotations

import logging
from typing import Any, Optional

from executor.mutation_gateway_types import MutationResult, MutationResultType

logger = logging.getLogger("executor.mutation_gateway")


class MutationGatewayCancelMixin:
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

