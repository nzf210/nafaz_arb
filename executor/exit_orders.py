"""exit order management methods for exit lifecycle management."""

import logging
import json
import time

from executor.models import ActivePosition, OrderRole, PositionState
from executor import helpers
from executor.helpers import build_client_order_id
from executor.private_stream import fetch_order_preferring_stream
from executor.fee_engine import rates_for_position, round_trip_fees_usdt

logger = logging.getLogger("executor.engine")



class ExitOrderManagementMixin:
    """Focused exit pricing, order management, or execution responsibilities."""

    async def _fetch_terminal_exit_order(
        self, pair_tag: str, old_order_id: str, symbol: str
    ):
        try:
            final_order = await self.client.fetch_local_order(old_order_id, symbol)
        except Exception:
            logger.exception(
                "[Cancel & Replace] cannot verify terminal status for old exit %s (%s); replacement blocked",
                pair_tag, old_order_id,
            )
            return None
        if not isinstance(final_order, dict):
            logger.error(
                "[Cancel & Replace] malformed status for old exit %s (%s); replacement blocked",
                pair_tag, old_order_id,
            )
            return None
        status = str(final_order.get("status") or "").strip().lower()
        terminal_statuses = {
            "canceled", "cancelled", "closed", "filled", "expired", "rejected",
        }
        if status not in terminal_statuses:
            logger.warning(
                "[Cancel & Replace] old exit %s for %s is still non-terminal (%s); replacement blocked",
                old_order_id, pair_tag, status or "unknown",
            )
            return None
        return final_order

    async def _apply_terminal_exit_order_fills(
        self, pair_tag: str, old_order: dict, pos: ActivePosition,
        symbol: str, final_order: dict,
    ) -> bool:
        self.supabase.order(
            pair_tag,
            "spot_exit",
            pos.local_exchange,
            "spot",
            symbol,
            final_order,
            client_order_id=old_order.get("client_order_id"),
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        try:
            final_filled = max(0.0, float(final_order.get("filled", 0.0) or 0.0))
            already_closed = max(
                0.0, float(old_order.get("closed_short_qty", 0.0) or 0.0)
            )
        except (TypeError, ValueError):
            logger.error(
                "[Cancel & Replace] invalid final fill quantity for %s (%s); replacement blocked",
                pair_tag, old_order.get("order_id"),
            )
            return False
        unclosed_delta = final_filled - already_closed
        if unclosed_delta > 0:
            await self._execute_exit_fill(pair_tag, old_order, pos, unclosed_delta)
            if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
                return False
        return pair_tag in self.active_positions

    async def _cancel_existing_exit_order(self, pair_tag: str, symbol: str) -> bool:
        """Cancel and reconcile the current spot exit before any replacement is submitted."""
        old_order = self.pending_exit_orders.get(pair_tag)
        if old_order is None:
            return True
        old_order_id = str(old_order.get("order_id") or "")
        if not old_order_id:
            logger.critical(
                "[Cancel & Replace] refusing replacement for %s: existing exit has no exchange order id",
                pair_tag,
            )
            return False

        try:
            await self.client.cancel_local_order(old_order_id, symbol)
        except Exception as exc:
            logger.warning(
                "[Cancel & Replace] cancel request for %s (%s) was inconclusive: %s",
                pair_tag, old_order_id, exc,
            )
        final_order = await self._fetch_terminal_exit_order(
            pair_tag, old_order_id, symbol
        )
        if final_order is None:
            return False
        pos = self.active_positions.get(pair_tag)
        if pos is None:
            return False
        if not await self._apply_terminal_exit_order_fills(
            pair_tag, old_order, pos, symbol, final_order
        ):
            return False

        # Remove only this exact pending record; never discard a newer order.
        if self.pending_exit_orders.get(pair_tag) is old_order:
            self.pending_exit_orders.pop(pair_tag, None)
        self.states[pair_tag] = PositionState.HEDGED_ACTIVE
        return True

    async def _place_dynamic_limit_sell(
        self,
        pair_tag: str,
        pos: ActivePosition,
        limit_sell_idr: float,
        tick_size: float,
        global_ask_usdt: float,
    ) -> None:
        try:
            exit_coid = build_client_order_id(
                pair_tag, "exit", self._next_order_seq()
            )
            await self._register_order_intent(
                pair_tag, exit_coid,
                exchange=pos.local_exchange, market_type="spot",
                symbol=pos.symbol, role=OrderRole.EXIT_SPOT.value,
                side="sell", order_type="limit", price=limit_sell_idr,
                quantity=pos.qty, local_amount_spec=pos.local_amount_spec,
            )
            order = await self.client.place_local_limit_sell(
                pos.symbol,
                pos.qty,
                limit_sell_idr,
                client_order_id=exit_coid,
                amount_spec=pos.local_amount_spec,
            )
            order_id = str(order.get("id", ""))
            self.supabase.order(
                pair_tag,
                "spot_exit",
                pos.local_exchange,
                "spot",
                pos.symbol,
                order,
                client_order_id=exit_coid,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            await self._register_order(
                pair_tag,
                exit_coid,
                order,
                exchange=pos.local_exchange,
                market_type="spot",
                symbol=pos.symbol,
                role=OrderRole.EXIT_SPOT.value,
                side="sell",
                order_type="limit",
                price=limit_sell_idr,
                quantity=pos.qty,
                local_amount_spec=pos.local_amount_spec,
            )
            self.pending_exit_orders[pair_tag] = {
                "order_id": order_id,
                "client_order_id": exit_coid,
                "symbol": pos.symbol,
                "price": limit_sell_idr,
                "qty": pos.qty,
                "filled": 0.0,
                "closed_short_qty": 0.0,
                "tick_size": tick_size,
            }
            self.last_pegged_global_ask[pair_tag] = global_ask_usdt
            self.states[pair_tag] = PositionState.EXIT_PENDING
            self.supabase.position_status(pair_tag, "EXIT_PENDING", pos)
        except Exception:
            logger.exception(f"[State 3] failed to place dynamic limit sell for {pair_tag}")

    async def _check_dynamic_exit_pair(self, pair_tag: str) -> None:
        """Run exit evaluation against this position's exact exchange pair."""
        await self._run_pair_bound(
            pair_tag, self._check_dynamic_exit_pair_bound, pair_tag
        )

    async def _exit_live_snapshot(self, pair_tag: str, pos):
        raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        local_bid = pos.entry_price_idr
        local_ask = 0.0
        snapshot = None
        if raw:
            try:
                snapshot = json.loads(raw)
                if not helpers.snapshot_is_stale(snapshot):
                    local_bid = float(
                        snapshot.get("local_best_bid", pos.entry_price_idr)
                    )
                    local_ask = float(snapshot.get("local_best_ask", 0.0))
                if hasattr(self.client, "update_market_price"):
                    self.client.update_market_price(
                        pos.symbol, local_bid, local_ask
                    )
            except Exception:
                pass
        return snapshot, local_bid, local_ask

    async def _persist_exit_unrealized_pnl(
        self, pair_tag: str, pos, local_bid: float,
        global_ask_usdt: float, usdt_idr_rate: float, now_ms: int,
        live_snapshot,
    ) -> None:
        if live_snapshot is None or helpers.snapshot_is_stale(live_snapshot):
            return
        spot_pnl_idr = pos.qty * (local_bid - pos.entry_price_idr)
        spot_pnl_usdt = (
            spot_pnl_idr / usdt_idr_rate if usdt_idr_rate > 0 else 0.0
        )
        futures_pnl_usdt = pos.qty * (
            pos.short_entry_usdt - global_ask_usdt
        )
        rates = rates_for_position(pos)
        est_fees_usdt = round_trip_fees_usdt(
            pos.qty,
            pos.entry_price_idr / usdt_idr_rate,
            local_bid / usdt_idr_rate,
            pos.short_entry_usdt,
            global_ask_usdt,
            rates,
        )
        net_unrealized_usdt = (
            spot_pnl_usdt + futures_pnl_usdt
            + pos.accumulated_funding_usdt - est_fees_usdt
        )
        cap_usdt = pos.qty * pos.short_entry_usdt
        pos.current_spot_bid_idr = local_bid
        pos.current_perp_ask_usdt = global_ask_usdt
        pos.spot_unrealized_pnl_idr = spot_pnl_idr
        pos.futures_unrealized_pnl_usdt = futures_pnl_usdt
        pos.net_unrealized_pnl_usdt = net_unrealized_usdt
        pos.net_unrealized_pnl_idr = net_unrealized_usdt * usdt_idr_rate
        pos.net_unrealized_roi_pct = (
            net_unrealized_usdt / cap_usdt * 100.0 if cap_usdt > 0 else 0.0
        )
        pos.updated_at = now_ms
        await self.redis.set(
            f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
        )

    async def _check_dynamic_exit_pair_bound(self, pair_tag: str) -> None:
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            return
        pos = self.active_positions.get(pair_tag)
        if not pos or pos.qty <= 0:
            return
        price_data = await self._fetch_live_price(pair_tag)
        if not price_data:
            return
        global_ask_usdt, usdt_idr_rate = price_data
        live_snapshot, local_best_bid, _local_best_ask = (
            await self._exit_live_snapshot(pair_tag, pos)
        )
        now_ms = int(time.time() * 1000)
        funding_passed = await self._handle_funding_rollover(
            pair_tag, pos, now_ms
        )
        await self._persist_exit_unrealized_pnl(
            pair_tag, pos, local_best_bid, global_ask_usdt,
            usdt_idr_rate, now_ms, live_snapshot,
        )
        if not self._should_repeg_exit(
            pair_tag, global_ask_usdt, funding_passed
        ):
            return
        limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
            pair_tag, pos, global_ask_usdt, usdt_idr_rate
        )
        if not await self._cancel_existing_exit_order(pair_tag, pos.symbol):
            return
        # Cancellation may race a fill and reduce or close the position.
        pos = self.active_positions.get(pair_tag)
        if not pos or pos.qty <= 0:
            return
        await self._place_dynamic_limit_sell(
            pair_tag, pos, limit_sell_idr, tick_size, global_ask_usdt
        )

    async def _check_dynamic_exits(self):
        """
        State 3: Dynamic Exit Limit Sell Pegging:
        - When position is open, JANGAN menaruh Limit Sell statis.
        - Recalculate Limit Sell IDR whenever Global Ask moves > 0.15% OR funding hour passes.
        - Formula:
          effective_exit_spread_pct = base_target_exit_spread_pct - ((accumulated_funding_usdt / position_value_usdt) * 100)
          limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (1 + effective_exit_spread_pct / 100)
        - Cancel & replace local Limit Sell order.
        """
        pair_tags = list(self.active_positions.keys())
        for pair_tag in pair_tags:
            await self._check_dynamic_exit_pair(pair_tag)

    # -------------------------------------------------------------
    # State 4: Exit Execution (Local Limit Sell Fill -> Global Market Buy reduceOnly)
    # -------------------------------------------------------------

    async def _check_exit_orders(self):
        """
        State 4: Exit Execution:
        - When local Limit Sell is filled (partially or fully):
          Immediately execute Market Buy (reduceOnly=True) on Global Futures to close Short.
        - When position is fully closed:
          DEL arb:active_pos:{local_ex}:{symbol} from Redis.
        """
        pair_tags = list(self.pending_exit_orders.keys())
        for pair_tag in pair_tags:
            await self._process_pending_exit(pair_tag)

    async def _process_pending_exit(
        self, pair_tag: str, force_reconcile: bool = False
    ) -> None:
        """Route pending exit operations through the pair-bound clients."""
        await self._run_pair_bound(
            pair_tag, self._process_pending_exit_bound, pair_tag, force_reconcile
        )

    async def _process_pending_exit_bound(
        self, pair_tag: str, force_reconcile: bool = False
    ) -> None:
        """Fetch local exit order and trigger short closure on fills.

        ``force_reconcile`` is used only by force close while holding the pair lock.
        """
        if (
            self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED
            and not force_reconcile
        ):
            return

        order_info = self.pending_exit_orders.get(pair_tag)
        pos = self.active_positions.get(pair_tag)
        if not order_info or not pos:
            return

        order_id = order_info["order_id"]
        local_symbol = order_info["symbol"]

        # Sync latest live market prices to client for realistic market touch evaluation
        live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if live_price_raw and hasattr(self.client, "update_market_price"):
            try:
                live_snapshot = json.loads(live_price_raw)
                if not helpers.snapshot_is_stale(live_snapshot):
                    self.client.update_market_price(
                        symbol=local_symbol,
                        best_bid=float(live_snapshot.get("local_best_bid", 0.0)),
                        best_ask=float(live_snapshot.get("local_best_ask", 0.0)),
                    )
                    # Paper execution must use the same live futures quote that
                    # triggered the local spot fill, not an unrelated/stale price.
                    global_symbol = self._resolve_global_symbol(pos.symbol)
                    self.client.update_market_price(
                        symbol=global_symbol,
                        best_bid=float(live_snapshot.get("global_best_bid_usdt", 0.0)),
                        best_ask=float(live_snapshot.get("global_best_ask_usdt", 0.0)),
                    )
            except Exception:
                logger.exception(
                    f"failed to synchronize market prices before exit hedge for {pair_tag}"
                )

        try:
            order = await fetch_order_preferring_stream(
                self.private_stream,
                lambda: self.client.fetch_local_order(order_id, local_symbol),
                order_id,
            )
        except Exception:
            logger.exception(f"failed to fetch exit order {order_id}")
            return

        current_filled = float(order.get("filled", 0.0))
        closed_qty = float(order_info.get("closed_short_qty", 0.0))
        unclosed_delta = current_filled - closed_qty

        if isinstance(order, dict):
            self.supabase.order(
                pair_tag,
                "spot_exit",
                pos.local_exchange,
                "spot",
                local_symbol,
                order,
                client_order_id=order_info.get("client_order_id"),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )

        if unclosed_delta > 0:
            await self._execute_exit_fill(pair_tag, order_info, pos, unclosed_delta)
