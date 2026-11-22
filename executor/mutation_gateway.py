"""Central Mutation Gateway (Executor V2).

Regulates all private create and cancel mutations:
- Enforces pre-submit fencing via PostgreSQL advisory locks
- Enforces per-CEX Spot order slot caps
- Generates deterministic, unique client_order_id
- Converts timeouts into SUBMISSION_UNKNOWN (zero blind retries)
- Maintains audit trail of all mutations
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from executor.global_stop import GlobalStopError, GlobalStopService
from executor.helpers import is_explicit_rate_limit_rejection
from executor.order_slots import OrderSlotManager, SlotExhaustedError
from executor.exchange_common import venue_client_order_id
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
    ):
        self.global_stop = global_stop
        self.slot_manager = slot_manager or OrderSlotManager()
        self.client_id_prefix = client_id_prefix

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

    @staticmethod
    def _resolve_create_target(client: Any, exchange: str, market_type: str):
        """Resolve the exact venue + market client before persisting or submitting."""
        if market_type in {"spot", "local"}:
            market_scope = "spot"
        elif market_type in {"future", "futures", "perpetual", "swap"}:
            market_scope = "futures"
        else:
            raise ValueError(f"unsupported create-order market type: {market_type or 'empty'}")

        target = client
        resolver = getattr(client, "for_venue", None)
        if callable(resolver):
            facade = resolver(exchange, market_scope)
            if facade is None:
                raise ValueError(
                    f"no configured {market_scope} client for venue {exchange}"
                )
            facade_id = str(getattr(facade, "id", "") or "").strip().lower()
            if facade_id == exchange:
                target = facade
            else:
                target_attr = "local" if market_scope == "spot" else "global_fut"
                target = getattr(facade, target_attr, None)

        actual_exchange = str(getattr(target, "id", "") or "").strip().lower()
        if not actual_exchange:
            raise ValueError(
                "resolved create-order client does not expose a verifiable venue id"
            )
        if actual_exchange != exchange:
            raise ValueError(
                f"create-order venue mismatch: requested {exchange}, resolved {actual_exchange}"
            )
        return target, market_scope

    @staticmethod
    def _is_reduce_only(params: dict) -> bool:
        value = params.get("reduceOnly", params.get("reduce_only", False))
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no", ""}:
                return False
            raise ValueError("reduceOnly must be a boolean value")
        return bool(value)

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
        pre_submit_guard: Optional[Any] = None,
        extra_params: Optional[dict] = None,
    ) -> MutationResult:
        """Create through the durable PREPARED/SUBMITTING + shared session fence."""
        exchange = exchange.lower().strip()
        requested_market_type = market_type.lower().strip()
        side = side.lower().strip()
        order_type = order_type.lower().strip()
        params = dict(extra_params or {})

        # This method is deliberately the exposure-increasing lane. Spot SELL
        # and futures reduceOnly orders must use the independent de-risk path so
        # a global entry STOP can never strand an exit/close.
        if side not in {"buy", "sell"}:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message="create-order side must be buy or sell",
            )
        try:
            reduce_only = self._is_reduce_only(params)
            venue_client, market_scope = self._resolve_create_target(
                client, exchange, requested_market_type
            )
        except Exception as exc:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message=f"create-order venue/market validation failed: {exc}",
            )
        if (market_scope == "spot" and side == "sell") or (
            market_scope == "futures" and reduce_only
        ):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message=(
                    "risk-reducing create-order must use the dedicated de-risk path; "
                    "it is not subject to the entry Global STOP fence"
                ),
            )
        market_type = "spot" if market_scope == "spot" else "perpetual"

        # Cancellation may use this gateway without entry admission; creates may
        # never degrade to an unfenced compatibility path.
        if self.global_stop is None or not getattr(self.global_stop, "enabled", False):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id="",
                error_message="PostgreSQL Global STOP fencing is not initialized; create-order blocked",
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
        if not callable(pre_submit_guard):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=(
                    "Generic create-order requires a pre-submit active-market admission guard"
                ),
            )

        if not callable(getattr(venue_client, "create_order_direct", None)) and not callable(
            getattr(venue_client, "create_order", None)
        ):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=f"Client for {exchange} lacks create_order capability",
            )

        # The durable PREPARED row must exist before any slot reservation. The
        # slot sweeper uses this row to distinguish abandoned reservations from
        # an entry that is still being admitted or recovered.
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
            )
        except Exception as exc:
            logger.error("Durable submit preparation failed on %s: %s", exchange, exc)
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=f"Durable submit preparation failed: {exc}",
            )

        async def _abort_prepared(reason: str) -> None:
            # This helper is used only before the venue boundary. If persistence
            # fails, PREPARED remains safe for the stale-intent sweeper to retire.
            try:
                await self.global_stop.mark_submit_aborted(client_order_id, reason)
            except Exception:
                logger.exception(
                    "Could not abort pre-submit intent %s; PREPARED remains durable",
                    client_order_id,
                )
            if market_type == "spot":
                try:
                    await self.slot_manager.release_slot(
                        exchange, client_order_id, reason="PRE_SUBMIT_ABORTED"
                    )
                except Exception:
                    logger.exception(
                        "Could not release pre-submit slot %s after abort",
                        client_order_id,
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
            await _abort_prepared(f"spot slot reservation rejected: {exc}")
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=str(exc),
            )
        except Exception as exc:
            await _abort_prepared(f"spot slot reservation failed: {type(exc).__name__}: {exc}")
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=client_order_id,
                error_message=f"Spot slot reservation failed: {exc}",
            )

        async def _call_venue():
            if callable(getattr(venue_client, "create_order_direct", None)):
                return await venue_client.create_order_direct(
                    symbol=symbol, order_type=order_type, side=side,
                    amount=quantity, price=price, params=params,
                )
            return await venue_client.create_order(
                symbol=symbol, type=order_type, side=side,
                amount=quantity, price=price, params=params,
            )

        ip_ban_state = None
        submission_started = False
        try:
            async with self.global_stop.order_submit_fence(
                client_order_id=client_order_id,
                pre_submit_guard=pre_submit_guard,
                scope_exchange=exchange,
                scope_symbol=symbol,
            ) as fence_conn:
                try:
                    # From this point the venue adapter may cross its HTTP
                    # boundary. Any exception is ambiguous unless explicitly
                    # classified as a definitive venue rejection.
                    submission_started = True
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
            if submission_started:
                # A GlobalStopError raised after entering the venue boundary
                # cannot prove that the exchange rejected the request.
                await self._mark_unknown_best_effort(client_order_id, exc)
                return MutationResult(
                    status=MutationResultType.SUBMISSION_UNKNOWN,
                    client_order_id=client_order_id,
                    error_message=f"Submit outcome is ambiguous after venue boundary: {exc}",
                )
            await _abort_prepared(f"Global STOP blocked before venue submit: {exc}")
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
        """Cancel through the correct venue facade and verify terminal state.

        Sending cancel is never blocked by Global STOP. A cancel ACK alone is
        not proof that the order is terminal or that no fill raced the cancel;
        the spot slot is released only after an exact order read confirms a
        terminal zero-fill result.
        """
        exchange = exchange.lower().strip()
        market_type = market_type.lower().strip()
        is_spot = market_type in {"spot", "local"}
        is_futures = market_type in {"future", "futures", "perpetual", "swap"}
        cid = str(client_order_id or order_id or "").strip()

        if not exchange or not symbol or not (is_spot or is_futures):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=cid,
                exchange_order_id=str(order_id or ""),
                error_message="exchange, supported market type and symbol are required for cancel",
            )

        target = client
        resolver = getattr(client, "for_venue", None)
        if callable(resolver):
            try:
                target = resolver(exchange, "spot" if is_spot else "futures")
            except Exception as exc:
                return MutationResult(
                    status=MutationResultType.REJECTED,
                    client_order_id=cid,
                    exchange_order_id=str(order_id or ""),
                    error_message=f"Could not resolve cancel venue {exchange}/{market_type}: {exc}",
                )
            if target is None:
                return MutationResult(
                    status=MutationResultType.REJECTED,
                    client_order_id=cid,
                    exchange_order_id=str(order_id or ""),
                    error_message=f"No {market_type} client configured for cancel venue {exchange}",
                )
        else:
            bound_id = str(getattr(client, "id", "") or "").lower()
            if bound_id and bound_id != exchange:
                return MutationResult(
                    status=MutationResultType.REJECTED,
                    client_order_id=cid,
                    exchange_order_id=str(order_id or ""),
                    error_message=f"Cancel client venue mismatch: expected {exchange}, got {bound_id}",
                )

        if is_spot:
            canceler = getattr(target, "cancel_local_order", None)
            fetcher = getattr(target, "fetch_local_order", None)
            lookup = getattr(target, "fetch_local_order_by_client_id", None)
            raw_venue = getattr(target, "local", None)
        else:
            canceler = getattr(target, "cancel_global_order", None)
            fetcher = getattr(target, "fetch_global_order", None)
            lookup = getattr(target, "fetch_global_order_by_client_id", None)
            raw_venue = getattr(target, "global_fut", None)

        if not callable(canceler):
            canceler = getattr(raw_venue, "cancel_order", None)
            fetcher = getattr(raw_venue, "fetch_order", None)
        if not callable(canceler):
            canceler = getattr(target, "cancel_order", None)
            fetcher = getattr(target, "fetch_order", None)
        if not callable(canceler):
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=cid,
                exchange_order_id=str(order_id or ""),
                error_message=f"Client for {exchange}/{market_type} has no cancel capability",
            )

        resolved_order_id = str(order_id or "").strip()
        if client_order_id and (
            not resolved_order_id or resolved_order_id == str(client_order_id)
        ):
            if not callable(lookup):
                return MutationResult(
                    status=MutationResultType.SUBMISSION_UNKNOWN,
                    client_order_id=cid,
                    error_message="Exchange order ID is missing and exact client-ID lookup is unavailable",
                )
            try:
                found = await lookup(str(client_order_id), symbol)
            except Exception as exc:
                return MutationResult(
                    status=MutationResultType.SUBMISSION_UNKNOWN,
                    client_order_id=cid,
                    error_message=f"Exact client-ID lookup before cancel failed: {exc}",
                )
            if not isinstance(found, dict) or not found.get("id"):
                return MutationResult(
                    status=MutationResultType.SUBMISSION_UNKNOWN,
                    client_order_id=cid,
                    error_message="Exact client-ID lookup did not return a verifiable exchange order",
                )
            resolved_order_id = str(found["id"])

        if not resolved_order_id:
            return MutationResult(
                status=MutationResultType.REJECTED,
                client_order_id=cid,
                error_message="A verified exchange order ID is required for cancel",
            )

        cancel_response = None
        cancel_error = ""
        try:
            cancel_response = await canceler(resolved_order_id, symbol)
        except Exception as exc:
            # Cancel may have reached the venue despite a lost response. Read
            # exact order state before deciding whether the operation resolved.
            cancel_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Cancel request on %s (%s) returned an error; checking exact order state: %s",
                exchange, resolved_order_id, cancel_error,
            )

        if not callable(fetcher):
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                error_message="Cancel was sent but exact final order-state lookup is unavailable; slot retained",
            )

        terminal_statuses = {"closed", "filled", "canceled", "cancelled", "rejected", "expired"}
        final_order = None
        final_error = ""
        for attempt in range(3):
            try:
                final_order = await fetcher(resolved_order_id, symbol)
            except Exception as exc:
                final_order = None
                final_error = f"{type(exc).__name__}: {exc}"
            if (
                isinstance(final_order, dict)
                and str(final_order.get("id") or "") == resolved_order_id
                and str(final_order.get("status") or "").lower() in terminal_statuses
            ):
                break
            if attempt < 2:
                await asyncio.sleep(0.1)

        if (
            not isinstance(final_order, dict)
            or str(final_order.get("id") or "") != resolved_order_id
        ):
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                error_message=(
                    "Cancel final state is not verifiable; slot retained"
                    + (f" ({final_error})" if final_error else "")
                ),
                raw_response=final_order if isinstance(final_order, dict) else None,
            )

        final_status = str(final_order.get("status") or "").lower()
        if final_status not in terminal_statuses:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                error_message=f"Order remains non-terminal after cancel (status={final_status}); slot retained",
                raw_response=final_order,
            )

        filled_raw = final_order.get("filled")
        try:
            filled_qty = float(filled_raw)
        except (TypeError, ValueError):
            filled_qty = float("nan")
        if not math.isfinite(filled_qty) or filled_qty < 0:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                error_message="Terminal cancel state has invalid/missing filled quantity; slot retained",
                raw_response=final_order,
            )
        if filled_qty > 0:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                filled_qty=filled_qty,
                error_message=(
                    "Cancel reached a terminal order with executed fills; "
                    "slot retained until entry/hedge fill reconciliation completes"
                ),
                raw_response=final_order,
            )
        if final_status not in {"canceled", "cancelled", "rejected", "expired"}:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                error_message=(
                    f"Terminal order status {final_status!r} does not prove a zero-fill cancel; "
                    "slot retained for lifecycle reconciliation"
                ),
                raw_response=final_order,
            )

        if is_spot and not client_order_id:
            return MutationResult(
                status=MutationResultType.SUBMISSION_UNKNOWN,
                client_order_id=cid,
                exchange_order_id=resolved_order_id,
                error_message=(
                    "Cancel is terminal with zero fill, but the logical client order ID "
                    "is missing; slot cannot be safely correlated or released"
                ),
                raw_response=final_order,
            )

        if is_spot:
            try:
                await self.slot_manager.release_slot(
                    exchange, cid, reason="CANCELLED_TERMINAL_ZERO_FILL"
                )
            except Exception as exc:
                return MutationResult(
                    status=MutationResultType.SUBMISSION_UNKNOWN,
                    client_order_id=cid,
                    exchange_order_id=resolved_order_id,
                    error_message=f"Order is terminal but spot slot release failed: {exc}",
                    raw_response=final_order,
                )

        response = {"cancel": cancel_response, "final_order": final_order}
        if cancel_error:
            response["cancel_error"] = cancel_error
        return MutationResult(
            status=MutationResultType.CONFIRMED,
            client_order_id=cid,
            exchange_order_id=resolved_order_id,
            filled_qty=filled_qty,
            raw_response=response,
        )
