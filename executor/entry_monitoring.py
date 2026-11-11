"""entry monitoring methods for entry execution."""

import asyncio
import time
from executor.models import PositionState
from executor.private_stream import fetch_order_preferring_stream
from executor.entry_common import logger



class EntryMonitoringMixin:
    """Focused entry lifecycle responsibilities."""

    async def _monitoring_loop(self):
        """Periodic loop monitoring pending entry orders and active positions."""
        interval = float(getattr(self.config, "monitor_interval_seconds", 0.5))
        while self._running:
            try:
                await self._check_entry_orders()
                await self._check_dynamic_exits()
                await self._check_exit_orders()
                await self._maybe_publish_status()
            except Exception:
                logger.exception("error in monitoring loop")
            await asyncio.sleep(interval)

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
        """Route pending entry lifecycle calls through the pair-bound clients."""
        await self._run_pair_bound(
            pair_tag, self._process_pending_entry_bound, pair_tag, order_info
        )

    async def _process_pending_entry_bound(self, pair_tag: str, order_info: dict) -> None:
        """Process adverse selection check, fill tracking, and position creation for an entry order.

        Runs under the pair lock so the monitoring loop cannot mutate pending
        order/position state concurrently with the signal entry path."""
        async with self._pair_lock(pair_tag):
            # The entry path may have resolved (or replaced) this order while we
            # waited for the lock; drop the stale snapshot if so.
            if self.pending_entry_orders.get(pair_tag) is not order_info:
                return
            # A pending entry must not hedge again while recovery has an
            # unresolved order or a non-durable hedge watermark for this
            # pair. Active-position exit monitoring remains independent.
            if (
                self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED
                or pair_tag in (getattr(self, "_recovery_scope_halts", {}) or {})
            ):
                return
            if await self._check_adverse_selection(pair_tag, order_info):
                return

            order_id = order_info["order_id"]
            local_symbol = order_info["symbol"]

            # Backoff gate: a venue answering 429/HTML (or timing out) must not
            # be polled 20x/second. Skip this tick while the per-order backoff
            # window is open; the TTL reconciliation below still runs so a
            # pending order can never be pinned open by a broken status feed.
            now = time.time()
            retry_after = float(order_info.get("fetch_retry_after", 0.0) or 0.0)
            fetch_blocked = now < retry_after

            # Sync latest live prices to client for realistic market touch evaluation
            await self._sync_live_price_for_client(pair_tag, local_symbol)

            order = None
            if not fetch_blocked:
                try:
                    order = await fetch_order_preferring_stream(
                        self.private_stream,
                        lambda: self.client.fetch_local_order(order_id, local_symbol),
                        order_id,
                    )
                except Exception as exc:
                    # Count the failure instead of retrying blindly, then space
                    # out the next attempt so a throttled venue can recover.
                    failures = int(order_info.get("fetch_failures", 0) or 0) + 1
                    order_info["fetch_failures"] = failures
                    order_info["last_fetch_error"] = f"{type(exc).__name__}: {exc}"
                    base = max(0.05, float(getattr(self.config, "monitor_interval_seconds", 0.5)))
                    ceiling = float(getattr(self.config, "order_fetch_backoff_max_seconds", 60.0))
                    delay = min(ceiling, base * (2 ** min(failures, 10)))
                    order_info["fetch_retry_after"] = now + delay
                    logger.error(
                        f"failed to fetch local order {order_id} "
                        f"(attempt {failures}, retry in {delay:.1f}s): {type(exc).__name__}"
                    )
                    await self._expire_unfetchable_entry_order(pair_tag, order_info)
                    return
                order_info["fetch_failures"] = 0
                order_info["fetch_retry_after"] = 0.0
            else:
                # The status feed is down; still enforce the order TTL on the
                # clock so a stuck order is cancelled rather than sitting open.
                await self._expire_unfetchable_entry_order(pair_tag, order_info)
                return

            if await self._expire_stale_entry_order(pair_tag, order_info, order):
                return

            if await self._refresh_stale_maker_quote(pair_tag, order_info, order):
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
                submitted_at_ms=int(float(order_info.get("placed_time", 0.0) or 0.0) * 1000) or None,
            )
            observed_status = str(order.get("status", "")).lower()
            if observed_status in ("closed", "canceled", "cancelled", "rejected", "expired"):
                self.supabase.event(
                    "ENTRY_ORDER_TERMINAL_OBSERVED",
                    pair_key=pair_tag,
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                    payload={
                        "order_id": order_id,
                        "client_order_id": order_info.get("client_order_id", ""),
                        "symbol": local_symbol,
                        "exchange": order_info.get("local_exchange", ""),
                        "venue_status": observed_status,
                        "filled_qty": float(order.get("filled", 0.0) or 0.0),
                        "remaining_qty": float(order.get("remaining", 0.0) or 0.0),
                        "cancel_requested_at_ms": order_info.get("cancel_requested_at_ms"),
                        "cancel_reason": order_info.get("last_cancel_reason"),
                    },
                )
            await self._handle_entry_fills(pair_tag, order_info, order)
            # Persist cumulative fill/hedge progress before the next poll or
            # restart. This prevents an already-hedged partial fill being
            # interpreted as unhedged after recovery.
            await self._persist_pending_entry(pair_tag, order_info)
            await self._finalize_entry_order(pair_tag, order.get("status", ""))
