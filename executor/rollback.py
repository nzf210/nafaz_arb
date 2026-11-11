"""Hedge-failure rollback: spot liquidate, ambiguity guard, reserve release.

Extracted from executor/entry.py; mixed into ArbitrageEngine so the self.*
state and helper methods stay on one object.
"""

import json
import logging
import math
import time
from typing import Any, Dict, Optional

from executor import helpers
from executor.helpers import (
    RetryExhausted,
    build_client_order_id,
    retry_with_backoff,
)
from executor.models import OrderRole, PositionState
from executor.quantity import AmountSpec

logger = logging.getLogger("executor.engine")


class RollbackMixin:
    """Emergency unwind when the global futures short cannot be opened."""

    async def _liquidate_unhedged_spot(
        self,
        pair_tag: str,
        symbol: str,
        unhedged_delta: float,
        amount_spec: Any = None,
        legacy_amount_step: float = 0.0,
    ) -> tuple[Optional[Dict[str, Any]], Optional[Exception]]:
        """
        Market-sells the unhedged spot quantity back to quote.

        Returns ``(order, error)``: the order is None when every attempt failed,
        and ``error`` then carries the last failure so the operator event can say
        what went wrong.
        """
        max_retries = 4  # 5 total attempts
        # One id for the whole retry sequence: every attempt is the same logical
        # order, so the venue can reject duplicates rather than sell twice.
        seq = self._next_order_seq()
        rollback_coid = build_client_order_id(pair_tag, "rollback", seq)
        await self._register_order_intent(
            pair_tag, rollback_coid,
            exchange=getattr(self.config, "local_exchange_name", ""),
            market_type="spot", symbol=symbol,
            role=OrderRole.ROLLBACK.value, side="sell",
            order_type="market", price=0.0, quantity=unhedged_delta,
            local_amount_spec=amount_spec,
            required=False,
        )
        try:
            emergency_order = await retry_with_backoff(
                lambda: self.client.place_local_market_sell(
                    symbol,
                    unhedged_delta,
                    client_order_id=rollback_coid,
                    amount_spec=amount_spec
                    or (
                        AmountSpec(
                            unit="BASE",
                            step=legacy_amount_step,
                            minimum=legacy_amount_step,
                            base_per_unit=1.0,
                        )
                        if legacy_amount_step > 0
                        else None
                    ),
                ),
                max_retries=max_retries,
                base_delay=0.5,
                label=f"hedge rollback sell {symbol} for {pair_tag}",
            )
        except RetryExhausted as exhausted:
            logger.critical(
                f"[Hedge Rollback] FAILED to liquidate spot leg for {pair_tag} after "
                f"{exhausted.attempts} attempts: {exhausted.last_error}. "
                f"MANUAL INTERVENTION REQUIRED: {unhedged_delta} {symbol} is UNHEDGED."
            )
            return None, exhausted.last_error
        if not isinstance(emergency_order, dict):
            return None, RuntimeError("spot rollback returned no verifiable order")
        self.supabase.order(
            pair_tag,
            "rollback",
            getattr(self.config, "local_exchange_name", ""),
            "spot",
            symbol,
            emergency_order,
            client_order_id=rollback_coid,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        await self._register_order(
            pair_tag,
            build_client_order_id(pair_tag, "rollback", seq),
            emergency_order,
            exchange=getattr(self.config, "local_exchange_name", ""),
            market_type="spot",
            symbol=symbol,
            role=OrderRole.ROLLBACK.value,
            side="sell",
            order_type="market",
            price=helpers.extract_fill_price(emergency_order),
            quantity=unhedged_delta,
        )
        status = str(emergency_order.get("status", "")).lower()
        if status not in ("closed", "filled"):
            return None, RuntimeError(
                f"spot rollback is not terminal (status={status or 'unknown'})"
            )
        try:
            filled = float(emergency_order.get("filled") or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        tolerance = max(1e-12, unhedged_delta * 1e-8)
        if filled + tolerance < unhedged_delta:
            return None, RuntimeError(
                f"spot rollback fill incomplete: requested={unhedged_delta}, filled={filled}"
            )
        return emergency_order, None

    async def _hedge_may_be_open(
        self, pair_tag: str, global_symbol: str, error: Exception
    ) -> bool:
        """
        Returns True when a futures short MIGHT be open after a hedge error.

        Only a venue-confirmed zero position is safe to roll back against. Any
        query failure or unexpected payload blocks the rollback (fail-safe):
        a wrongly-cancelled rollback leaves unhedged spot (recoverable by an
        operator), whereas a wrong rollback on an open short inverts exposure.
        """
        if not global_symbol:
            return True
        try:
            positions = await self.client.fetch_positions(global_symbol)
        except AttributeError:
            # Client without fetch_positions (e.g. some mocks): cannot confirm.
            logger.warning(
                f"[Hedge Ambiguity] {pair_tag}: client lacks fetch_positions; treating hedge state as unknown"
            )
            return True
        except Exception as exc:
            logger.warning(
                f"[Hedge Ambiguity] {pair_tag}: position query failed ({exc}); treating hedge state as unknown"
            )
            return True
        if not positions:
            return False
        exposure = 0.0
        for position in positions:
            if not isinstance(position, dict):
                logger.critical(
                    f"[Hedge Ambiguity] {pair_tag}: malformed futures position; "
                    "spot rollback blocked"
                )
                return True
            info = position.get("info") or {}
            if not isinstance(info, dict):
                logger.critical(
                    f"[Hedge Ambiguity] {pair_tag}: malformed futures position info; "
                    "spot rollback blocked"
                )
                return True
            raw_contracts = position.get("contracts")
            if raw_contracts is not None:
                try:
                    qty = abs(float(raw_contracts)) * abs(
                        float(position.get("contractSize") or 1.0)
                    )
                except (TypeError, ValueError):
                    return True
                if not math.isfinite(qty):
                    return True
                if qty <= 1e-12:
                    continue
                side = str(
                    position.get("side")
                    or position.get("positionSide")
                    or info.get("posSide")
                    or info.get("positionSide")
                    or ""
                ).lower()
                if side not in {"short", "long"}:
                    return True
                exposure += qty
                continue

            raw_amount = position.get("positionAmt", info.get("positionAmt"))
            if raw_amount is None:
                return True
            try:
                signed_amount = float(raw_amount)
            except (TypeError, ValueError):
                return True
            if not math.isfinite(signed_amount):
                return True
            # Binance-style positionAmt is signed: negative is short, positive long.
            exposure += abs(signed_amount)
        if exposure > 1e-12:
            logger.critical(
                f"[Hedge Ambiguity] {pair_tag}: venue reports non-flat futures exposure "
                f"(gross exposure {exposure}) on {global_symbol} despite hedge error "
                f"({error}) - spot NOT rolled back"
            )
            return True
        return False

    async def _verify_ambiguous_hedge_absent(
        self, pair_tag: str, order_info: dict, client_order_id: str,
        error: Exception, requested_quantity: float,
    ) -> tuple[bool, str]:
        """Prove both hedge order absence and flat futures exposure before unwind."""
        global_symbol = str(order_info.get("global_symbol") or "")
        lookup = getattr(self.client, "fetch_global_order_by_client_id", None)
        if not global_symbol or not client_order_id or not callable(lookup):
            return False, "futures order lookup is unavailable"
        try:
            order = await lookup(client_order_id, global_symbol, order_info.get("global_amount_spec"))
        except Exception as exc:
            return False, f"futures order lookup failed: {type(exc).__name__}: {exc}"
        if order is not None:
            if not isinstance(order, dict) or not order.get("id"):
                return False, "futures order lookup returned a malformed order"
            returned_client_id = str(order.get("client_order_id") or order.get("clientOrderId") or "")
            if returned_client_id != client_order_id:
                return False, "futures order lookup returned a different client order id"
            status = str(order.get("status") or "").lower()
            if status not in {"closed", "filled", "canceled", "cancelled", "rejected", "expired"}:
                return False, f"futures hedge order is still non-terminal (status={status or 'unknown'})"
            try:
                filled = float(order.get("filled") or 0.0)
            except (TypeError, ValueError):
                return False, "futures hedge order has an invalid filled quantity"
            if not math.isfinite(filled) or filled < 0:
                return False, "futures hedge order has a non-finite/negative filled quantity"
            tolerance = max(1e-12, requested_quantity * 1e-8)
            if filled > tolerance:
                return False, f"futures hedge order has fills ({filled}); rollback is unsafe"
        if await self._hedge_may_be_open(pair_tag, global_symbol, error):
            return False, "futures position is non-flat or could not be verified"
        await self._resolve_zero_fill_hedge_intent(str(order_info.get("global_exchange") or ""), client_order_id)
        return True, "no hedge order/fill and futures position confirmed flat"

    async def _resolve_zero_fill_hedge_intent(self, exchange: str, client_order_id: str) -> None:
        """Close the durable submit intent after venue absence is independently proven."""
        registry = getattr(self, "order_registry", None)
        if registry is None or not exchange or not client_order_id:
            return
        record = await registry.find_by_client_id(exchange, client_order_id)
        if record is None:
            return
        record.status = "canceled"
        record.recovery_status = "MANAGED"
        await registry.register(record)
        await registry.clear_pending_recovery(record.local_order_id)

    async def _release_unhedged_reserve(
        self,
        pair_tag: str,
        order_info: dict,
        unhedged_delta: float,
        price_idr: float,
        usdt_rate: float,
    ) -> None:
        """Releases the capital reserved for the spot leg that never got hedged."""
        if not hasattr(self.client, "unlock_funds"):
            return
        spot_cost = (unhedged_delta * price_idr) / usdt_rate if usdt_rate > 0 else 0.0
        self.client.unlock_funds(spot_cost, 0.0)
        try:
            balances = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                symbol=order_info.get("symbol"),
                usdt_idr_rate=usdt_rate,
            )
            if balances is not None:
                await self.redis.set(BALANCES_KEY, json.dumps(balances))
        except Exception:
            logger.exception("failed to persist balances after hedge rollback")

    async def _rollback_hedge_failure(
        self,
        pair_tag: str,
        order_info: dict,
        unhedged_delta: float,
        price_idr: float,
        usdt_rate: float,
        error: Exception,
    ) -> None:
        """
        Emergency rollback when the global futures short cannot be opened.

        Without this the local spot coins bought by the maker order stay exposed
        to full directional risk. We market-sell the unhedged spot quantity back
        to quote and surface a CRITICAL event so the operator sees the loss.
        """
        symbol = order_info.get("symbol", "")
        logger.critical(
            f"[Hedge Rollback] Selling {unhedged_delta} {symbol} spot back to quote "
            f"after hedge failure on {pair_tag}"
        )

        # Timeout ambiguity guard: a hedge "failure" may be a timeout AFTER the
        # venue accepted the market sell. Rolling back the spot while the short
        # is actually open would leave the book net-short in the wrong direction.
        # Only proceed with the rollback when the venue confirms no open short.
        global_symbol = str(order_info.get("global_symbol", ""))
        hedge_error_is_ambiguous = await self._hedge_may_be_open(
            pair_tag, global_symbol, error
        )
        if hedge_error_is_ambiguous:
            self._mark_failed(
                pair_tag,
                "HEDGE_STATE_UNKNOWN",
                "hedge error is ambiguous and a futures short may be open; spot rollback blocked pending manual check",
                detail={
                    "unhedged_qty": unhedged_delta,
                    "symbol": symbol,
                    "global_symbol": global_symbol,
                    "hedge_error": str(error),
                },
            )
            event = {
                "event": "HEDGE_FAILED",
                "pair_key": pair_tag,
                "local_exchange": order_info.get("local_exchange"),
                "global_exchange": order_info.get("global_exchange"),
                "symbol": symbol,
                "qty": unhedged_delta,
                "entry_price_idr": price_idr,
                "exit_price_idr": 0.0,
                "rollback_succeeded": False,
                "reason": f"hedge state unknown, spot NOT rolled back: {error}",
                "rollback_error": None,
                "is_mock": getattr(self.config, "mock_mode", False),
                "timestamp_ms": int(time.time() * 1000),
            }
            await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(event))
            self.supabase.event(
                "HEDGE_FAILED",
                pair_key=pair_tag,
                is_mock=bool(event.get("is_mock", False)),
                occurred_at_ms=event.get("timestamp_ms"),
                payload=event,
            )
            await self.publish_status()
            return

        emergency_order, emergency_error = await self._liquidate_unhedged_spot(
            pair_tag,
            symbol,
            unhedged_delta,
            order_info.get("local_amount_spec"),
            float(order_info.get("local_lot_size", 0.0)),
        )

        exit_price_idr = (
            helpers.extract_fill_price(emergency_order)
            if emergency_order is not None
            else 0.0
        )

        # Release reserved capital only after the exchange confirms a full,
        # terminal rollback fill. An ambiguous/partial sell must remain blocked.
        if emergency_error is None:
            await self._release_unhedged_reserve(
                pair_tag, order_info, unhedged_delta, price_idr, usdt_rate
            )

        event = {
            "event": "HEDGE_FAILED",
            "pair_key": pair_tag,
            "local_exchange": order_info.get("local_exchange"),
            "global_exchange": order_info.get("global_exchange"),
            "symbol": symbol,
            "qty": unhedged_delta,
            "entry_price_idr": price_idr,
            "exit_price_idr": exit_price_idr,
            "rollback_succeeded": emergency_error is None,
            "reason": str(error),
            "rollback_error": str(emergency_error) if emergency_error else None,
            "is_mock": getattr(self.config, "mock_mode", False),
            "timestamp_ms": int(time.time() * 1000),
        }
        await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(event))

        self.supabase.event(
            "HEDGE_FAILED",
            pair_key=pair_tag,
            is_mock=bool(event.get("is_mock", False)),
            occurred_at_ms=event.get("timestamp_ms"),
            payload=event,
        )

        if emergency_error is not None:
            self._mark_failed(
                pair_tag,
                "HEDGE_FAILED",
                f"hedge failed and spot rollback was not fully verified: {emergency_error}",
                detail={
                    "unhedged_qty": unhedged_delta,
                    "symbol": symbol,
                    "entry_price_idr": price_idr,
                    "hedge_error": str(error),
                },
            )
        else:
            self.states[pair_tag] = PositionState.CLOSED
        await self.publish_status()
