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
        pos = self._get_repriceable_position(pair_tag)
        if pos is None:
            return
        if hasattr(self.client, "set_route"):
            self.client.set_route(pos.local_exchange, pos.global_exchange)
        price_data = await self._fetch_live_price(pair_tag, pos)
        if not price_data:
            return
        global_ask, fx_rate = price_data
        snapshot, local_bid, local_ask = await self._load_exit_market_snapshot(pair_tag, pos)
        now_ms = int(time.time() * 1000)
        funding_passed = await self._handle_funding_rollover(pair_tag, pos, global_ask, now_ms)
        stale = snapshot is None or helpers.snapshot_is_stale(snapshot)
        if not stale:
            await self._update_unrealized_exit_pnl(
                pair_tag, pos, snapshot, local_bid, global_ask, fx_rate, now_ms
            )
        buffer_snapshot = snapshot if snapshot and not stale else {}
        dynamic_slippage = await self._calculate_dynamic_exit_slippage(
            pair_tag, buffer_snapshot, now_ms
        )
        await self._reprice_exit_order(
            pair_tag, pos, global_ask, fx_rate, funding_passed, dynamic_slippage
        )

    def _get_repriceable_position(self, pair_tag: str) -> Optional[ActivePosition]:
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            return None
        pos = self.active_positions.get(pair_tag)
        return pos if pos and pos.qty > 0 else None

    async def _load_exit_market_snapshot(self, pair_tag: str, pos: ActivePosition):
        raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        local_bid = pos.entry_price_idr
        local_ask = 0.0
        if not raw:
            return None, local_bid, local_ask
        snapshot = None
        try:
            snapshot = json.loads(raw)
            if not helpers.snapshot_is_stale(snapshot):
                local_bid = float(snapshot.get("local_best_bid", pos.entry_price_idr))
                local_ask = float(snapshot.get("local_best_ask", 0.0))
            if hasattr(self.client, "update_market_price"):
                self.client.update_market_price(pos.symbol, local_bid, local_ask)
        except Exception:
            pass
        return snapshot, local_bid, local_ask

    async def _update_unrealized_exit_pnl(
        self, pair_tag: str, pos: ActivePosition, snapshot: dict,
        local_bid: float, global_ask: float, fx_rate: float, now_ms: int,
    ) -> None:
        spot_pnl_idr = pos.qty * (local_bid - pos.entry_price_idr)
        spot_pnl_usdt = spot_pnl_idr / fx_rate if fx_rate > 0 else 0.0
        futures_pnl = pos.qty * (pos.short_entry_usdt - global_ask)
        capital = pos.qty * pos.short_entry_usdt
        fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.001) or 0.0)
        fees = capital * fee_pct
        net_usdt = spot_pnl_usdt + futures_pnl + pos.accumulated_funding_usdt - fees
        net_idr = net_usdt * fx_rate
        roi = net_usdt / capital * 100.0 if capital > 0 else 0.0
        pos.current_spot_bid_idr = local_bid
        pos.current_perp_ask_usdt = global_ask
        pos.spot_unrealized_pnl_idr = spot_pnl_idr
        pos.futures_unrealized_pnl_usdt = futures_pnl
        pos.net_unrealized_pnl_usdt = net_usdt
        pos.net_unrealized_pnl_idr = net_idr
        pos.net_unrealized_roi_pct = roi
        pos.updated_at = now_ms
        await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

    async def _reprice_exit_order(
        self, pair_tag: str, pos: ActivePosition, global_ask: float,
        fx_rate: float, funding_passed: bool, dynamic_slippage: float,
    ) -> None:
        if not self._should_repeg_exit(pair_tag, global_ask, funding_passed):
            return
        limit_price, tick_size = self._calculate_limit_sell_idr(
            pair_tag, pos, global_ask, fx_rate, exit_slippage_pct=dynamic_slippage
        )
        await self._cancel_existing_exit_order(pair_tag, pos.symbol)
        await self._place_dynamic_limit_sell(
            pair_tag, pos, limit_price, tick_size, global_ask
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
        if self._exit_reconciliation_blocked(pair_tag, force_reconcile):
            return
        context = self._pending_exit_context(pair_tag)
        if context is None:
            return
        order_info, pos, order_id, local_symbol = context
        if hasattr(self.client, "set_route"):
            self.client.set_route(pos.local_exchange, pos.global_exchange)
        await self._sync_pending_exit_market_prices(pair_tag, pos, local_symbol)
        order = await self._fetch_pending_exit_order(pair_tag, order_id, local_symbol)
        if order is None:
            return
        await self._process_pending_exit_order(pair_tag, order_info, pos, local_symbol, order)

    def _exit_reconciliation_blocked(self, pair_tag: str, force_reconcile: bool) -> bool:
        return self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED and not force_reconcile

    def _pending_exit_context(self, pair_tag: str):
        order_info = self.pending_exit_orders.get(pair_tag)
        pos = self.active_positions.get(pair_tag)
        if not order_info or not pos:
            return None
        return order_info, pos, order_info["order_id"], order_info["symbol"]

    async def _sync_pending_exit_market_prices(self, pair_tag: str, pos: ActivePosition, local_symbol: str) -> None:
        raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if not raw or not hasattr(self.client, "update_market_price"):
            return
        try:
            snapshot = json.loads(raw)
            if helpers.snapshot_is_stale(snapshot):
                return
            self.client.update_market_price(
                symbol=local_symbol,
                best_bid=float(snapshot.get("local_best_bid", 0.0)),
                best_ask=float(snapshot.get("local_best_ask", 0.0)),
            )
            global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            self.client.update_market_price(
                symbol=global_symbol,
                best_bid=float(snapshot.get("global_best_bid_usdt", 0.0)),
                best_ask=float(snapshot.get("global_best_ask_usdt", 0.0)),
            )
        except Exception:
            logger.exception("failed to synchronize market prices before exit hedge for %s", pair_tag)

    async def _fetch_pending_exit_order(self, pair_tag: str, order_id: str, local_symbol: str):
        try:
            return await fetch_order_preferring_stream(
                self.private_stream,
                lambda: self.client.fetch_local_order(order_id, local_symbol),
                order_id,
            )
        except Exception:
            logger.exception("failed to fetch exit order %s", order_id)
            return None

    async def _process_pending_exit_order(
        self, pair_tag: str, order_info: dict, pos: ActivePosition,
        local_symbol: str, order: dict,
    ) -> None:
        current_filled = float(order.get("filled", 0.0))
        closed_qty = float(order_info.get("closed_short_qty", 0.0))
        unclosed_delta = current_filled - closed_qty
        if isinstance(order, dict):
            self.supabase.order(
                pair_tag, "spot_exit", pos.local_exchange, "spot", local_symbol,
                order, client_order_id=order_info.get("client_order_id"),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
        if unclosed_delta > 0:
            await self._execute_exit_fill(pair_tag, order_info, pos, unclosed_delta, order)
