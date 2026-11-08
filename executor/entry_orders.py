"""Entry monitoring, cancellation, and fill reconciliation."""

"""State 2 entry/monitoring flow for ArbitrageEngine.

Entry order pegging continuation, adverse selection, partial fills, the
global-futures short hedge, and the monitoring loop. Extracted from
executor/engine.py as a mixin.
"""

import asyncio
import json
import logging
import math
import time
from typing import Any, Dict, Optional

from executor.models import ActivePosition, PairInfo, PositionState
from executor import helpers
from executor.helpers import (
    BALANCES_KEY,
    HedgeSlippageExceeded,
    build_client_order_id,
)
from executor.private_stream import fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")


class EntryOrderMixin:

    async def _monitoring_loop(self):
        """Periodic loop monitoring pending entry orders and active positions."""
        while self._running:
            try:
                await self._check_entry_orders()
                await self._check_dynamic_exits()
                await self._check_exit_orders()
                await self._maybe_publish_status()
            except Exception:
                logger.exception("error in monitoring loop")
            await asyncio.sleep(0.05)  # 50ms fast reactive polling

    async def _maybe_publish_status(self) -> None:
        """Throttled status publication (at most once per second)."""
        now = time.time()
        if now - self._last_status_publish < 1.0:
            return
        self._last_status_publish = now
        await self.publish_status()

    async def _check_entry_orders(self):
        """
        Monitors State 1 (Entry Pegging) and State 2 (Instant Short Hedge on Partial Fill):
        - If Global Bid drops below threshold -> cancel order immediately.
        - Checks local order filled qty:
          If newly filled >= $10 -> instantly execute Market Short on Global Futures!
        """
        pair_tags = list(self.pending_entry_orders.keys())
        for pair_tag in pair_tags:
            order_info = self.pending_entry_orders.get(pair_tag)
            if not order_info:
                continue
            await self._process_pending_entry(pair_tag, order_info)

    async def _process_pending_entry(self, pair_tag: str, order_info: dict) -> None:
        if hasattr(self.client, "set_route"):
            self.client.set_route(
                order_info.get("local_exchange", ""),
                order_info.get("global_exchange", ""),
            )
        """Process adverse selection check, fill tracking, and position creation for an entry order.

        Runs under the pair lock so the monitoring loop cannot mutate pending
        order/position state concurrently with the signal entry path."""
        async with self._pair_lock(pair_tag):
            # The entry path may have resolved (or replaced) this order while we
            # waited for the lock; drop the stale snapshot if so.
            if self.pending_entry_orders.get(pair_tag) is not order_info:
                return
            if await self._check_adverse_selection(pair_tag, order_info):
                return

            order_id = order_info["order_id"]
            local_symbol = order_info["symbol"]

            # Sync latest live prices to client for realistic market touch evaluation
            await self._sync_live_price_for_client(pair_tag, local_symbol)

            try:
                order = await fetch_order_preferring_stream(
                    self.private_stream,
                    lambda: self.client.fetch_local_order(order_id, local_symbol),
                    order_id,
                )
            except Exception:
                logger.exception(f"failed to fetch local order {order_id}")
                return

            if await self._expire_stale_entry_order(pair_tag, order_info, order):
                return

            self.supabase.order(
                pair_tag,
                "spot_entry",
                order_info.get("local_exchange", ""),
                "spot",
                local_symbol,
                order,
                client_order_id=order_info.get("client_order_id"),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            await self._handle_entry_fills(pair_tag, order_info, order)
            await self._finalize_entry_order(pair_tag, order.get("status", ""))

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

    async def _cancel_entry_order_and_reconcile(
        self, pair_tag: str, order_info: dict, reason: str
    ) -> bool:
        """
        Cancel an entry and verify the authoritative venue state before
        releasing funds or deleting its recovery record.
        """
        order_id = str(order_info["order_id"])
        local_symbol = str(order_info["symbol"])
        try:
            await self.client.cancel_local_order(order_id, local_symbol)
        except Exception:
            # A timeout is ambiguous: the venue may have accepted the cancel.
            logger.exception(
                f"[{reason}] cancel request failed for {order_id}; "
                "reconciling the order directly with the exchange"
            )

        try:
            order = await self.client.fetch_local_order(order_id, local_symbol)
        except Exception:
            logger.exception(
                f"[{reason}] cannot verify order {order_id}; pending state and "
                "reserved funds are retained"
            )
            return False

        if not isinstance(order, dict):
            logger.critical(
                f"[{reason}] invalid exchange response for {order_id}; "
                "pending state is retained"
            )
            return False

        status = str(order.get("status", "unknown")).lower()
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
        )
        await self._handle_entry_fills(pair_tag, order_info, order)
        if self.states.get(pair_tag) in (
            PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
        ):
            logger.critical(
                f"[{reason}] order {order_id} reached terminal state but its "
                "fill could not be safely reconciled; retaining recovery state"
            )
            return False

        await self._finalize_entry_order(pair_tag, status)
        return True

    async def _expire_stale_entry_order(
        self, pair_tag: str, order_info: dict, order: dict
    ) -> bool:
        """Cancel an unfilled entry past TTL; resolve only after venue verification."""
        if order.get("status") != "open" or not math.isclose(float(order.get("filled", 0.0)), 0.0, abs_tol=1e-12):
            return False

        order_id = order_info["order_id"]
        placed_time = float(order_info.get("placed_time", 0.0))
        max_ttl = getattr(self.config, "max_order_ttl_seconds", 30)
        age = time.time() - placed_time
        if placed_time <= 0 or age < max_ttl:
            return False

        logger.info(
            f"[State 1 Order Expired] Maker Limit Buy {order_id} for {pair_tag} "
            f"expired after {age:.1f}s without market fill. Cancelling."
        )
        return await self._cancel_entry_order_and_reconcile(
            pair_tag, order_info, "Order Expired"
        )

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

    async def _handle_entry_fills(
        self, pair_tag: str, order_info: dict, order: dict
    ) -> None:
        """Check order fill delta and trigger short hedge if threshold is met."""
        status = order.get("status", "")
        current_filled = float(order.get("filled", 0.0))
        hedged_qty = float(order_info.get("hedged_short_qty", 0.0))
        unhedged_delta = current_filled - hedged_qty

        if unhedged_delta <= 0:
            return

        try:
            usdt_rate = float(order_info.get("usdt_idr_rate", 0.0) or 0.0)
        except (TypeError, ValueError):
            usdt_rate = 0.0
        price_idr = float(order_info.get("price", 0.0) or 0.0)
        if not math.isfinite(usdt_rate) or usdt_rate <= 0:
            # A filled IDR order without its persisted live FX snapshot is
            # an unreconciled exposure. Do not fabricate USD value from a
            # hardcoded fallback or record a position on a guessed basis.
            self._mark_failed(
                pair_tag,
                "FILLED_ORDER_FX_RATE_MISSING",
                "Filled IDR order has no valid persisted live USDT/IDR rate; manual reconciliation required.",
                detail={"order_id": order_info.get("order_id"), "unhedged_qty": unhedged_delta},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            await self.publish_status()
            return
        delta_val_usd = (unhedged_delta * price_idr) / usdt_rate

        is_complete = status in ("closed", "canceled", "cancelled", "rejected", "expired")
        if delta_val_usd >= self.config.min_partial_fill_usd or is_complete:
            logger.info(
                f"[State 2 Instant Short Hedge] Fill detected: delta_qty={unhedged_delta} "
                f"(${delta_val_usd:.2f}) on {pair_tag}. Opening Market Short on Global Futures!"
            )
            await self._execute_entry_hedge(
                pair_tag, order_info, unhedged_delta, price_idr, usdt_rate
            )
            # Only clear the partial marker once the hedge actually went through.
            if self.states.get(pair_tag) == PositionState.ENTRY_PARTIAL:
                self.states[pair_tag] = PositionState.HEDGED_ACTIVE
        elif 0 < current_filled < float(order_info.get("qty", 0.0)):
            # Below the hedge threshold with the maker order still resting: the spot
            # leg is partially filled and unhedged. Surface that explicitly rather
            # than leaving the pair looking like a plain pending entry.
            if self.states.get(pair_tag) != PositionState.ENTRY_PARTIAL:
                logger.info(
                    f"[State 1 Partial Fill] {pair_tag} filled {current_filled}"
                    f"/{order_info.get('qty')} below hedge threshold (${delta_val_usd:.2f}); "
                    f"marking ENTRY_PARTIAL"
                )
                self.states[pair_tag] = PositionState.ENTRY_PARTIAL
                await self._maybe_publish_status()
