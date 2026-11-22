"""Central Mutation Gateway (Executor V2).

Regulates all private create and cancel mutations:
- Enforces pre-submit fencing via PostgreSQL advisory locks
- Enforces per-CEX Spot order slot caps
- Generates deterministic, unique client_order_id
- Converts timeouts into SUBMISSION_UNKNOWN (zero blind retries)
- Maintains audit trail of all mutations
"""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from executor.global_stop import GlobalStopError, GlobalStopService
from executor.helpers import is_explicit_rate_limit_rejection
from executor.order_slots import OrderSlotManager, SlotExhaustedError
from executor.exchange_common import venue_client_order_id
from executor.order_intent_service import OrderIntentService
from types import SimpleNamespace

logger = logging.getLogger("executor.mutation_gateway")


class MutationResultType(str, Enum):
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    DEFERRED = "DEFERRED"


@dataclass
class MutationResult:
    status: MutationResultType
    client_order_id: str
    exchange_order_id: Optional[str] = None
    filled_qty: float = 0.0
    average_price: float = 0.0
    error_message: str = ""
    raw_response: Optional[dict] = None


class MutationGateway:
    """Gateway orchestrating safe order mutations across exchanges."""

    def __init__(
        self,
        global_stop: Optional[GlobalStopService] = None,
        slot_manager: Optional[OrderSlotManager] = None,
        *,
        client_id_prefix: str = "nfz",
        order_intent_service: Optional[OrderIntentService] = None,
    ):
        self.global_stop = global_stop
        self.slot_manager = slot_manager or OrderSlotManager()
        self.client_id_prefix = client_id_prefix
        self.order_intent_service = order_intent_service

    def generate_client_order_id(self, exchange: str, market_type: str = "spot") -> str:
        """Generate unique, structured client order ID conforming to venue limits."""
        # Clean prefix and ts
        ts_ms = int(time.time() * 1000)
        rand_suffix = uuid.uuid4().hex[:8]
        raw_id = f"{self.client_id_prefix}-{exchange[:3]}-{market_type[:1]}-{ts_ms}-{rand_suffix}"
        # Ensure length adheres to exchange constraints (e.g. Tokocrypto 32 chars, OKX 32 chars)
        if len(raw_id) > 32:
            raw_id = f"{self.client_id_prefix[:2]}{exchange[:3]}{market_type[:1]}{ts_ms % 1000000000}{rand_suffix}"
        return raw_id

    @staticmethod
    def _is_explicit_ip_ban(exc: Exception) -> bool:
        text = f"{type(exc).__name__} {exc}".lower()
        markers = (
            "ip banned", "ip address banned", "ip is banned",
            "ip restricted", "ip address restricted", "ip access blocked",
        )
        if any(marker in text for marker in markers):
            return True
        has_418 = "http_418" in text or "status code 418" in text
        return has_418 and ("ban" in text or "ip" in text)

    @staticmethod
    def _is_definitive_order_rejection(exc: Exception) -> bool:
        """Only explicit venue-side rejection may retire a SUBMITTING attempt."""
        error_name = type(exc).__name__.lower()
        error_text = str(exc).lower()
        definitive_types = {
            "insufficientfunds", "invalidorder", "badrequest",
            "authenticationerror", "permissiondenied", "invalidsymbol",
            "marketclosed", "argumentsrequired",
        }
        definitive_messages = (
            "insufficient funds", "insufficient balance", "insufficientfunds",
            "min_notional", "minimum notional", "invalid symbol",
            "unknown symbol", "market is not available",
            "does not have trading permission",
        )
        return (
            is_explicit_rate_limit_rejection(exc)
            or MutationGateway._is_explicit_ip_ban(exc)
            or error_name in definitive_types
            or any(marker in error_text for marker in definitive_messages)
        )

    async def _mark_unknown_best_effort(self, client_order_id: str, error: Exception) -> None:
        try:
            await self.global_stop.mark_submit_unknown(
                client_order_id, f"{type(error).__name__}: {error}"
            )
        except Exception:
            logger.exception(
                "CRITICAL: submit %s remains SUBMITTING after UNKNOWN persistence failed",
                client_order_id,
            )

    async def execute_create_order(
        self,
        client: Any,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float,
        pair_key: str,
        session_id: Optional[str] = None,
        extra_params: Optional[dict] = None,
        account_scope_id: Optional[str] = None,
        action_class: str = "ENTRY",
        mutation_intent_id: Optional[str] = None,
    ) -> MutationResult:
        """Create through the durable PREPARED/SUBMITTING + shared session fence."""
        exchange = exchange.lower().strip()
        market_type = market_type.lower().strip()
        side = side.lower().strip()
        order_type = order_type.lower().strip()
        params = dict(extra_params or {})
        mutation_intent_id = str(mutation_intent_id or params.get("mutation_intent_id") or "").strip() or None
        intent_service = self.order_intent_service if mutation_intent_id else None
        intent_actor_id = str(params.get("intent_actor_id") or "executor").strip()
        mutation_lease = None

        def _capture_mutation_lease(lease: Any) -> None:
            nonlocal mutation_lease
            mutation_lease = lease

        # Cancellation may use this gateway without entry admission; creates may
        # never degrade to an unfenced compatibility path.
        if self.global_stop is None or not getattr(self.global_stop, "enabled", False):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message="PostgreSQL Global STOP fencing is not initialized; create-order blocked",
            )

        account_scope_id = str(
            account_scope_id or params.get("account_scope_id") or ""
        ).strip()
        if not account_scope_id:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message="account_scope_id is required for scoped create-order ownership",
            )

        client_order_id = str(
            params.get("client_order_id") or params.get("clientOrderId")
            or self.generate_client_order_id(exchange, market_type)
        ).strip()
        if not client_order_id:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message="stable client order ID is required",
            )
        params["client_order_id"] = client_order_id
        params["clientOrderId"] = venue_client_order_id(
            SimpleNamespace(id=exchange), client_order_id
        )

        if not callable(getattr(client, "create_order_direct", None)) and not callable(
            getattr(client, "create_order", None)
        ):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=f"Client for {exchange} lacks create_order capability",
            )

        slot_reserved = False
        try:
            if market_type == "spot":
                await self.slot_manager.reserve_slot(
                    exchange, client_order_id=client_order_id,
                    session_id=session_id, market_type=market_type,
                )
                slot_reserved = True
        except SlotExhaustedError as exc:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=str(exc),
            )

        try:
            await self.global_stop.prepare_submit_attempt(
                client_order_id=client_order_id,
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                pair_key=pair_key,
                recovery_context=params.get("recovery_context"),
                amount_spec=params.get("amount_spec"),
                account_scope_id=account_scope_id,
                mutation_intent_id=mutation_intent_id,
            )
        except Exception as exc:
            if slot_reserved:
                await self.slot_manager.release_slot(
                    exchange, client_order_id, reason="PREPARE_FAILED"
                )
            logger.error("Durable submit preparation failed on %s: %s", exchange, exc)
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=f"Durable submit preparation failed: {exc}",
            )

        async def _call_venue():
            if callable(getattr(client, "create_order_direct", None)):
                return await client.create_order_direct(
                    symbol=symbol, order_type=order_type, side=side,
                    amount=quantity, price=price, params=params,
                )
            return await client.create_order(
                symbol=symbol, type=order_type, side=side,
                amount=quantity, price=price, params=params,
            )

        ip_ban_state = None
        try:
            async with self.global_stop.order_submit_fence(
                client_order_id=client_order_id,
                scope_exchange=exchange,
                scope_symbol=symbol,
                scope_market_type=market_type,
                account_scope_id=account_scope_id,
                action_class=action_class,
                scope_pair_key=pair_key,
                scope_session_id=session_id,
                mutation_intent_id=mutation_intent_id,
                mutation_intent_service=intent_service,
                mutation_intent_actor_id=intent_actor_id,
                mutation_intent_lease_callback=_capture_mutation_lease if mutation_intent_id else None,
            ) as fence_conn:
                try:
                    raw = await _call_venue()
                except Exception as exc:
                    if self._is_explicit_ip_ban(exc):
                        try:
                            ip_ban_state = await self.global_stop.request_stop_in_transaction(
                                fence_conn, "CEX_IP_BLOCK",
                                source="exchange:" + exchange,
                                metadata={
                                    "exchange": exchange, "symbol": symbol,
                                    "error_type": type(exc).__name__,
                                    "exchange_error": str(exc)[:1000],
                                },
                            )
                        except Exception:
                            logger.exception(
                                "CRITICAL: failed to persist CEX_IP_BLOCK inside submit fence"
                            )
                            await self._mark_unknown_best_effort(client_order_id, exc)
                            return MutationResult(
                                status=MutationResultType.SUBMISSION_UNKNOWN,
                                client_order_id=client_order_id,
                                error_message="explicit IP-ban response; canonical STOP persistence failed",
                            )

                    if self._is_definitive_order_rejection(exc):
                        try:
                            await self.global_stop.mark_submit_rejected(client_order_id, str(exc))
                        except Exception:
                            await self._mark_unknown_best_effort(client_order_id, exc)
                            return MutationResult(
                                status=MutationResultType.SUBMISSION_UNKNOWN,
                                client_order_id=client_order_id,
                                error_message="explicit venue rejection but durable rejection persistence failed",
                            )
                        if slot_reserved:
                            await self.slot_manager.release_slot(
                                exchange, client_order_id, reason="REJECTED"
                            )
                        return MutationResult(
                            status=MutationResultType.REJECTED,
                            client_order_id=client_order_id,
                            error_message=str(exc),
                        )

                    await self._mark_unknown_best_effort(client_order_id, exc)
                    return MutationResult(
                        status=MutationResultType.SUBMISSION_UNKNOWN,
                        client_order_id=client_order_id,
                        error_message=f"Ambiguous venue submit: {type(exc).__name__}: {exc}",
                    )

                if not isinstance(raw, dict):
                    malformed = RuntimeError("venue create-order acknowledgement is not an object")
                    await self._mark_unknown_best_effort(client_order_id, malformed)
                    return MutationResult(
                        status=MutationResultType.SUBMISSION_UNKNOWN,
                        client_order_id=client_order_id,
                        error_message=str(malformed),
                    )

                exchange_order_id = str(raw.get("id") or raw.get("orderId") or "")
                if not exchange_order_id:
                    malformed = RuntimeError("venue create-order acknowledgement has no exchange order ID")
                    await self._mark_unknown_best_effort(client_order_id, malformed)
                    return MutationResult(
                        status=MutationResultType.SUBMISSION_UNKNOWN,
                        client_order_id=client_order_id,
                        error_message=str(malformed),
                        raw_response=raw,
                    )

                try:
                    await self.global_stop.mark_submit_acknowledged(client_order_id, raw)
                except Exception as exc:
                    await self._mark_unknown_best_effort(client_order_id, exc)
                    return MutationResult(
                        status=MutationResultType.SUBMISSION_UNKNOWN,
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        error_message="venue ACK received but durable ACK persistence failed; no retry permitted",
                        raw_response=raw,
                    )

                try:
                    filled_qty = float(raw.get("filled") or raw.get("executedQty") or 0.0)
                except (TypeError, ValueError):
                    filled_qty = 0.0
                try:
                    average_price = float(raw.get("average") or raw.get("price") or price)
                except (TypeError, ValueError):
                    average_price = float(price or 0.0)
                if slot_reserved:
                    try:
                        await self.slot_manager.activate_slot(exchange, client_order_id)
                    except Exception:
                        logger.exception("Order %s ACKNOWLEDGED but local slot activation failed", client_order_id)
                return MutationResult(
                    status=MutationResultType.CONFIRMED,
                    client_order_id=client_order_id,
                    exchange_order_id=exchange_order_id,
                    filled_qty=filled_qty,
                    average_price=average_price,
                    raw_response=raw,
                )
        except GlobalStopError as exc:
            if slot_reserved:
                await self.slot_manager.release_slot(
                    exchange, client_order_id, reason="GLOBAL_STOP_BLOCKED"
                )
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=f"Global stop blocked: {exc}",
            )
        except Exception as exc:
            # A disconnect around fence entry can happen after SUBMITTING commits.
            # Keep slot + unresolved ledger; never infer that the venue was not called.
            await self._mark_unknown_best_effort(client_order_id, exc)
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=client_order_id,
                error_message=f"Submit fence outcome is uncertain: {type(exc).__name__}: {exc}",
            )
        finally:
            if ip_ban_state is not None:
                await self.global_stop.notify_state(ip_ban_state)

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
