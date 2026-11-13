"""entry order management methods for entry execution."""

import json
import math
import time
from typing import Any

from executor.models import PositionState
from executor.entry_common import logger



class EntryOrderManagementMixin:
    """Focused entry lifecycle responsibilities."""

    async def _sync_live_price_for_client(
        self, pair_tag: str, local_symbol: str
    ) -> None:
        """Push the cached live snapshot into the client for market touch evaluation."""
        live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if not live_price_raw or not hasattr(self.client, "update_market_price"):
            return
        try:
            live_snapshot = json.loads(live_price_raw)
            self.client.update_market_price(
                symbol=local_symbol,
                best_bid=float(live_snapshot.get("local_best_bid", 0.0)),
                best_ask=float(live_snapshot.get("local_best_ask", 0.0)),
            )
        except Exception:
            pass

    async def _fetch_entry_order_for_cancel_reconciliation(
        self, pair_tag: str, order_info: dict, reason: str,
        cancel_error: str | None,
    ):
        order_id = str(order_info["order_id"])
        local_symbol = str(order_info["symbol"])
        try:
            order = await self.client.fetch_local_order(order_id, local_symbol)
        except Exception as exc:
            self.supabase.event(
                "ENTRY_ORDER_CANCEL_RECONCILIATION_FAILED",
                pair_key=pair_tag,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
                payload={
                    "order_id": order_id,
                    "client_order_id": order_info.get("client_order_id", ""),
                    "symbol": local_symbol,
                    "reason": reason,
                    "cancel_error": cancel_error,
                    "fetch_error": f"{type(exc).__name__}: {exc}",
                },
            )
            logger.exception(
                f"[{reason}] cannot verify order {order_id}; pending state and "
                "reserved funds are retained"
            )
            return None
        if isinstance(order, dict):
            return order
        self.supabase.event(
            "ENTRY_ORDER_CANCEL_RECONCILIATION_FAILED",
            pair_key=pair_tag,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            payload={
                "order_id": order_id,
                "symbol": local_symbol,
                "reason": reason,
                "cancel_error": cancel_error,
                "fetch_error": "invalid exchange order payload",
            },
        )
        logger.critical(
            f"[{reason}] invalid exchange response for {order_id}; "
            "pending state is retained"
        )
        return None

    async def _cancel_entry_order_and_reconcile(
        self, pair_tag: str, order_info: dict, reason: str
    ) -> bool:
        """
        Cancel an entry and verify the authoritative venue state before
        releasing funds or deleting its recovery record.
        """
        order_id = str(order_info["order_id"])
        local_symbol = str(order_info["symbol"])
        requested_at_ms = int(time.time() * 1000)
        order_info["last_cancel_reason"] = reason
        order_info["cancel_requested_at_ms"] = requested_at_ms
        order_info["cancel_attempts"] = int(order_info.get("cancel_attempts", 0) or 0) + 1
        # Persist intent before the network call so a crash/timeout does not
        # erase why the venue order was being canceled.
        await self._persist_pending_entry(pair_tag, order_info)
        self.supabase.event(
            "ENTRY_ORDER_CANCEL_REQUESTED",
            pair_key=pair_tag,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            occurred_at_ms=requested_at_ms,
            payload={
                "order_id": order_id,
                "client_order_id": order_info.get("client_order_id", ""),
                "symbol": local_symbol,
                "exchange": order_info.get("local_exchange", ""),
                "reason": reason,
                "attempt": order_info["cancel_attempts"],
                "order_age_seconds": max(0.0, time.time() - float(order_info.get("placed_time", time.time()) or time.time())),
                "filled_before_cancel": float(order_info.get("filled", 0.0) or 0.0),
                "requested_qty": float(order_info.get("qty", 0.0) or 0.0),
            },
        )
        cancel_error = None
        try:
            await self.client.cancel_local_order(order_id, local_symbol)
        except Exception as exc:
            # A timeout is ambiguous: the venue may have accepted the cancel.
            cancel_error = f"{type(exc).__name__}: {exc}"
            logger.exception(
                f"[{reason}] cancel request failed for {order_id}; "
                "reconciling the order directly with the exchange"
            )

        order = await self._fetch_entry_order_for_cancel_reconciliation(
            pair_tag, order_info, reason, cancel_error
        )
        if order is None:
            return False

        status = str(order.get("status", "unknown")).lower()
        self.supabase.event(
            "ENTRY_ORDER_CANCEL_RESULT",
            pair_key=pair_tag,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            payload={
                "order_id": order_id,
                "client_order_id": order_info.get("client_order_id", ""),
                "symbol": local_symbol,
                "reason": reason,
                "cancel_error": cancel_error,
                "venue_status": status,
                "terminal_confirmed": status in ("closed", "canceled", "cancelled", "rejected", "expired"),
                "filled_qty": float(order.get("filled", 0.0) or 0.0),
                "remaining_qty": float(order.get("remaining", 0.0) or 0.0),
            },
        )
        terminal = ("closed", "canceled", "cancelled", "rejected", "expired")
        if status not in terminal:
            logger.critical(
                f"[{reason}] order {order_id} is not confirmed terminal "
                f"(status={status}); pending state and funds are retained"
            )
            return False

        # A fill can race the cancel. Hedge the final cumulative fill before
        # releasing the unused reservation.
        order_info["filled"] = float(order.get("filled", 0.0) or 0.0)
        self.supabase.order(
            pair_tag,
            "spot_entry",
            order_info.get("local_exchange", ""),
            "spot",
            local_symbol,
            order,
            client_order_id=order_info.get("client_order_id"),
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            submitted_at_ms=int(float(order_info.get("placed_time", 0.0) or 0.0) * 1000) or None,
        )
        await self._handle_entry_fills(pair_tag, order_info, order)
        await self._persist_pending_entry(pair_tag, order_info)
        if self.states.get(pair_tag) in (
            PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
        ):
            logger.critical(
                f"[{reason}] order {order_id} reached terminal state but its "
                "fill could not be safely reconciled; retaining recovery state"
            )
            return False

        self.supabase.event(
            "ENTRY_ORDER_CANCELLED",
            pair_key=pair_tag,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            occurred_at_ms=int(time.time() * 1000),
            payload={
                "order_id": order_id,
                "client_order_id": order_info.get("client_order_id", ""),
                "symbol": local_symbol,
                "reason": reason,
                "terminal_status": status,
                "filled_qty": float(order_info.get("filled", 0.0) or 0.0),
                "order_qty": float(order_info.get("qty", 0.0) or 0.0),
            },
        )
        await self._finalize_entry_order(pair_tag, status)
        return True

    async def _expire_stale_entry_order(
        self, pair_tag: str, order_info: dict, order: dict
    ) -> bool:
        """Cancel an unfilled entry past TTL; resolve only after venue verification."""
        if str(order.get("status", "")).lower() != "open":
            return False

        order_id = order_info["order_id"]
        placed_time = float(order_info.get("placed_time", 0.0))
        max_ttl = getattr(self.config, "max_order_ttl_seconds", 30)
        age = time.time() - placed_time
        if placed_time <= 0 or age < max_ttl:
            return False

        filled = float(order.get("filled", 0.0) or 0.0)
        logger.info(
            f"[State 1 Order Expired] Maker Limit Buy {order_id} for {pair_tag} "
            f"expired after {age:.1f}s (filled={filled}). Cancelling remainder "
            "and reconciling the final cumulative fill."
        )
        return await self._cancel_entry_order_and_reconcile(
            pair_tag, order_info, "Order Expired"
        )

    async def _expire_unfetchable_entry_order(
        self, pair_tag: str, order_info: dict
    ) -> bool:
        """Enforce the entry TTL when the order status cannot be fetched.

        The normal expiry path needs a live order payload. When the venue is
        rate-limiting (HTTP 429 returning HTML, so the client cannot even
        decode a status) that payload never arrives, and without this the
        pending order and its reserved funds stay open forever. Age is taken
        from the local clock instead, and the cancel path re-verifies with the
        venue before anything is declared terminal.
        """
        order_id = order_info.get("order_id")
        placed_time = float(order_info.get("placed_time", 0.0) or 0.0)
        if placed_time <= 0:
            return False

        max_ttl = float(getattr(self.config, "max_order_ttl_seconds", 120))
        max_failures = int(getattr(self.config, "max_order_fetch_failures", 10))
        age = time.time() - placed_time
        failures = int(order_info.get("fetch_failures", 0) or 0)

        if age < max_ttl and failures < max_failures:
            return False

        reason = "Order Expired" if age >= max_ttl else "Order Status Unavailable"
        logger.warning(
            f"[State 1 {reason}] Maker Limit Buy {order_id} for {pair_tag}: "
            f"age={age:.1f}s failures={failures}; "
            "cancelling and reconciling without a fetched status"
        )
        return await self._cancel_entry_order_and_reconcile(pair_tag, order_info, reason)

    @staticmethod
    def _maker_order_is_unfilled(order_info: dict, order: dict) -> bool:
        try:
            quantities = (
                float(order.get("filled", 0.0) or 0.0),
                float(order_info.get("filled", 0.0) or 0.0),
                float(order_info.get("hedged_short_qty", 0.0) or 0.0),
            )
        except (TypeError, ValueError):
            return False
        return all(math.isfinite(value) and value == 0 for value in quantities)

    @staticmethod
    def _maker_quote_snapshot_values(snapshot: dict, order_info: dict):
        timestamp_ms = int(snapshot.get("timestamp_ms", 0) or 0)
        now_ms = int(time.time() * 1000)
        quote_asset = str(order_info.get("quote_asset") or "").strip().upper()
        if not quote_asset:
            sym = str(order_info.get("symbol") or "").upper()
            if "/" in sym:
                quote_asset = sym.rsplit("/", 1)[-1]
            elif "_" in sym:
                quote_asset = sym.rsplit("_", 1)[-1]
            else:
                quote_asset = "IDR"
        fx_rate = float(
            snapshot.get("usdt_idr_rate", 0.0)
            or order_info.get("usdt_idr_rate", 0.0)
        )
        local_bid = float(snapshot.get("local_best_bid", 0.0) or 0.0)
        local_ask = float(snapshot.get("local_best_ask", 0.0) or 0.0)
        if quote_asset in ("USDT", "USDC") and fx_rate > 0:
            local_bid *= fx_rate
            local_ask *= fx_rate
        values = {
            "local_bid": local_bid,
            "local_ask": local_ask,
            "global_bid": float(snapshot.get("global_best_bid_usdt", 0.0) or 0.0),
            "fx_rate": fx_rate,
            "current_price": float(order_info.get("price", 0.0) or 0.0),
            "tick": float(order_info.get("tick_size", 0.0) or 0.0),
            "min_spread": float(
                order_info.get("min_entry_spread_pct", 0.0) or 0.0
            ),
        }
        if timestamp_ms <= 0 or not 0 <= now_ms - timestamp_ms <= 3000:
            return None
        positive_values = (
            values["local_bid"], values["local_ask"], values["global_bid"],
            values["fx_rate"], values["current_price"], values["tick"],
        )
        if not all(math.isfinite(value) and value > 0 for value in positive_values):
            return None
        if not math.isfinite(values["min_spread"]) or values["min_spread"] < 0:
            return None
        values["now_ms"] = now_ms
        return values

    async def _fresh_maker_quote(self, pair_tag: str, order_info: dict):
        raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if not raw:
            return None
        try:
            snapshot = json.loads(raw)
            values = self._maker_quote_snapshot_values(snapshot, order_info)
            if values is None:
                return None
            max_buy_price = float(
                order_info.get("adaptive_max_buy_price_idr", 0.0) or 0.0
            )
            # Re-peg one tick above LocalBestBid, but never cross the ask
            # or exceed the persisted adaptive profit ceiling. This is an
            # evaluation quote only; actual submission still goes through the
            # scanner's fresh payload, sizing, risk gates and durable fence.
            raw_candidate = min(
                values["local_bid"] + values["tick"],
                values["local_ask"] - values["tick"],
            )
            if max_buy_price > 0:
                raw_candidate = min(raw_candidate, max_buy_price)
            candidate = math.floor(
                (raw_candidate + 1e-12) / values["tick"]
            ) * values["tick"]
            if candidate <= 0 or candidate < values["local_bid"]:
                candidate = 0.0

            if candidate > 0 and candidate >= values["local_ask"]:
                candidate = 0.0
            live_global_idr = values["global_bid"] * values["fx_rate"]
            live_spread_pct = (
                (live_global_idr - candidate) / live_global_idr
            ) * 100.0 if candidate > 0 else 0.0
            if candidate > 0 and live_spread_pct < values["min_spread"]:
                # A quote below the required edge is not a safe replacement.
                # Keep the existing order; do not cancel it merely because a
                # fresh replacement would currently be uneconomic.
                return None
            if (
                candidate > 0
                and abs(candidate - values["current_price"]) < values["tick"]
            ):
                return None
            return {
                "local_bid": values["local_bid"],
                "local_ask": values["local_ask"],
                "candidate_price": candidate,
                "current_price": values["current_price"],
                "live_spread_pct": live_spread_pct,
                "min_spread": values["min_spread"],
                "now_ms": values["now_ms"],
            }
        except (TypeError, ValueError, KeyError):
            return None

    async def _requeue_fresh_signal_after_maker_refresh(self, pair_tag: str) -> bool:
        """Re-admit a refreshed pair only through a fresh scanner payload.

        The replacement is deliberately not submitted here: the normal signal
        handler re-runs all risk, spread, fee, sizing, spot-cap and submit-fence
        checks. A fill racing the cancel leaves an active position and blocks
        this requeue.
        """
        try:
            if await self._has_active_or_pending_position(pair_tag):
                return False
            payload = await self._load_signal_payload(pair_tag)
            if payload is None or payload.pricing_snapshot.is_stale(
                getattr(self.config, "max_stale_latency_ms", 500)
            ):
                return False
            added = await self.redis.zadd(
                "arb:signal_priority", {pair_tag: 0.0}, nx=True
            )
            if added:
                self._signal_queue_event.set()
                logger.info(
                    "[Maker Refresh] %s re-queued using fresh scanner signal; "
                    "normal admission and fencing checks will run again",
                    pair_tag,
                )
                return True
        except Exception:
            logger.exception(
                "[Maker Refresh] failed to re-queue fresh scanner signal for %s",
                pair_tag,
            )
        return False

    async def _refresh_stale_maker_quote(
        self, pair_tag: str, order_info: dict, order: dict
    ) -> bool:
        """Cancel a stale, unfilled maker quote only when a fresh quote remains safe."""
        is_indodax_idr = (
            str(order_info.get("local_exchange", "")).lower() == "indodax"
            and str(order_info.get("quote_asset", "")).upper() in ("IDR", "BIDR")
        )
        refresh_after = int(
            getattr(
                self.config,
                "indodax_maker_refresh_after_seconds"
                if is_indodax_idr else "maker_refresh_after_seconds",
                15 if is_indodax_idr else 8,
            ) or 0
        )
        if refresh_after <= 0 or str(order.get("status", "")).lower() != "open":
            return False
        if not self._maker_order_is_unfilled(order_info, order):
            return False

        placed_time = float(order_info.get("placed_time", 0.0) or 0.0)
        age = time.time() - placed_time
        if (
            not math.isfinite(placed_time)
            or placed_time <= 0
            or not math.isfinite(age)
            or age < refresh_after
        ):
            return False

        quote = await self._fresh_maker_quote(pair_tag, order_info)
        if quote is None:
            return False
        cooldown_seconds = int(
            getattr(
                self.config,
                "indodax_maker_refresh_cooldown_seconds"
                if is_indodax_idr else "maker_refresh_cooldown_seconds",
                15 if is_indodax_idr else 12,
            ) or (15 if is_indodax_idr else 12)
        )
        cooldown_key = f"arb:maker_refresh_cooldown:{pair_tag}"
        if await self.redis.exists(cooldown_key):
            return False
        await self.redis.set(cooldown_key, str(quote["now_ms"]), ex=cooldown_seconds)
        logger.info(
            f"[Maker Refresh] {pair_tag}: {refresh_after}s quote evaluation; "
            f"current={quote['current_price']} candidate={quote['candidate_price']} "
            f"best_bid={quote['local_bid']} best_ask={quote['local_ask']} "
            f"edge={quote['live_spread_pct']:.4f}% floor={quote['min_spread']:.4f}%. "
            "Canceling through the existing reconciliation lifecycle; "
            "a fresh scanner signal is required before replacement."
        )
        cancelled = await self._cancel_entry_order_and_reconcile(
            pair_tag, order_info, "Maker Quote Refresh"
        )
        if cancelled:
            await self._requeue_fresh_signal_after_maker_refresh(pair_tag)
        return cancelled

    async def _check_adverse_selection(self, pair_tag: str, order_info: dict) -> bool:
        """Cancel a maker entry on adverse movement, but only resolve after verification."""
        live_price_key = f"arb:live_price:{pair_tag}"
        live_price_raw = await self.redis.get(live_price_key)
        if not live_price_raw:
            return False

        try:
            live_snapshot = json.loads(live_price_raw)
            global_bid = float(live_snapshot.get("global_best_bid_usdt", 0.0))
            threshold = order_info.get("cancel_threshold_usdt", 0.0)
            if threshold > 0 and 0 < global_bid < threshold:
                return await self._cancel_pending_entry_due_to_adverse_selection(
                    pair_tag, order_info, global_bid, threshold
                )
        except Exception as e:
            logger.debug(f"error reading live price for adverse selection: {e}")

        return False

    async def _cancel_pending_entry_due_to_adverse_selection(
        self, pair_tag: str, order_info: dict, global_bid: float, threshold: float
    ) -> bool:
        order_id = order_info["order_id"]
        logger.warning(
            f"[State 1 Adverse Selection] Global Bid {global_bid} dropped below "
            f"threshold {threshold} for {pair_tag}. Cancelling local Limit Buy {order_id}"
        )
        return await self._cancel_entry_order_and_reconcile(
            pair_tag, order_info, "Adverse Selection"
        )
