"""Exit order placement, cancellation, and pending-order reconciliation."""

"""Dynamic exit pegging and exit execution for ArbitrageEngine.

Covers State 3 (dynamic limit sell + funding decay re-pegging) and
State 4 (local limit sell fill -> global market buy reduceOnly).
Extracted from executor/engine.py as a mixin.
"""

import json
import logging
import math
import time
from typing import Optional

from executor.models import ActivePosition, PositionState
from executor import helpers
from executor.helpers import (
    ARB_TRADE_HISTORY_KEY,
    ARB_TRADE_HISTORY_MAX_LEN,
    BALANCES_KEY,
    build_client_order_id,
    ceil_to_tick,
)
from executor.private_stream import fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")


class ExitOrderMixin:
    async def _cancel_existing_exit_order(self, pair_tag: str, symbol: str) -> None:
            if pair_tag not in self.pending_exit_orders:
                return
            old_order = self.pending_exit_orders[pair_tag]
            old_order_id = old_order.get("order_id")
            if old_order_id:
                try:
                    await self.client.cancel_local_order(old_order_id, symbol)
                except Exception as e:
                    logger.debug(f"error cancelling old exit order {old_order_id}: {e}")

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
                order = await self.client.place_local_limit_sell(
                    pos.symbol,
                    pos.qty,
                    limit_sell_idr,
                    client_order_id=exit_coid,
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
            if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
                return

            pos = self.active_positions.get(pair_tag)
            if not pos or pos.qty <= 0:
                return
            if hasattr(self.client, "set_route"):
                self.client.set_route(pos.local_exchange, pos.global_exchange)

            price_data = await self._fetch_live_price(pair_tag, pos)
            if not price_data:
                return
            global_ask_usdt, usdt_idr_rate = price_data

            # Sync latest market prices to client and track floating mark-to-market PnL
            live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
            local_best_bid = pos.entry_price_idr
            local_best_ask = 0.0
            live_snapshot = None
            if live_price_raw:
                try:
                    live_snapshot = json.loads(live_price_raw)
                    if not helpers.snapshot_is_stale(live_snapshot):
                        local_best_bid = float(live_snapshot.get("local_best_bid", pos.entry_price_idr))
                        local_best_ask = float(live_snapshot.get("local_best_ask", 0.0))
                    if hasattr(self.client, "update_market_price"):
                        self.client.update_market_price(pos.symbol, local_best_bid, local_best_ask)
                except Exception:
                    pass

            now_ms = int(time.time() * 1000)
            funding_passed = await self._handle_funding_rollover(
                pair_tag, pos, global_ask_usdt, now_ms
            )

            # Mark-to-market: when no fresh live prices exist (scanner offline,
            # expired snapshot, halted local book), do NOT persist fabricated PnL.
            # Leave the last known values untouched and flag the position stale so
            # the Telegram view can hide the numbers instead of showing fakes.
            stale = live_snapshot is None or helpers.snapshot_is_stale(live_snapshot)
            if not stale:
                spot_pnl_idr = pos.qty * (local_best_bid - pos.entry_price_idr)
                spot_pnl_usdt = spot_pnl_idr / usdt_idr_rate if usdt_idr_rate > 0 else 0.0
                futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - global_ask_usdt)
                capital_usdt = pos.qty * pos.short_entry_usdt
                fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.001) or 0.0)
                est_fees_usdt = capital_usdt * fee_pct
                net_unrealized_usdt = (
                    spot_pnl_usdt + futures_pnl_usdt + pos.accumulated_funding_usdt - est_fees_usdt
                )
                net_unrealized_idr = net_unrealized_usdt * usdt_idr_rate
                cap_usdt = pos.qty * pos.short_entry_usdt
                unrealized_roi = (net_unrealized_usdt / cap_usdt * 100.0) if cap_usdt > 0 else 0.0

                pos.current_spot_bid_idr = local_best_bid
                pos.current_perp_ask_usdt = global_ask_usdt
                pos.spot_unrealized_pnl_idr = spot_pnl_idr
                pos.futures_unrealized_pnl_usdt = futures_pnl_usdt
                pos.net_unrealized_pnl_usdt = net_unrealized_usdt
                pos.net_unrealized_pnl_idr = net_unrealized_idr
                pos.net_unrealized_roi_pct = unrealized_roi
                pos.updated_at = now_ms

                # Persist updated floating PnL to Redis for live position tracking
                await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

            # Observe every fresh market snapshot, even when the resting spot
            # order does not need repricing. Snapshot timestamps deduplicate polls.
            dynamic_slippage_pct = await self._calculate_dynamic_exit_slippage(
                pair_tag,
                live_snapshot if live_snapshot and not stale else {},
                now_ms,
            )
            if not self._should_repeg_exit(pair_tag, global_ask_usdt, funding_passed):
                return

            limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
                pair_tag, pos, global_ask_usdt, usdt_idr_rate,
                exit_slippage_pct=dynamic_slippage_pct,
            )
            await self._cancel_existing_exit_order(pair_tag, pos.symbol)
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
            if hasattr(self.client, "set_route"):
                self.client.set_route(pos.local_exchange, pos.global_exchange)

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
                        global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
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
                await self._execute_exit_fill(pair_tag, order_info, pos, unclosed_delta, order)
