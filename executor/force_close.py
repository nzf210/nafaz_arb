"""Force-close handling for ArbitrageEngine.

Extracted from executor/engine.py; mixed into ArbitrageEngine so the
self.* state and helper methods stay on one object.
"""

import json
import logging
import time
from typing import Tuple

from executor.models import PositionState
from executor import helpers
from executor.helpers import (
    ARB_TRADE_HISTORY_KEY,
    ARB_TRADE_HISTORY_MAX_LEN,
    BALANCES_KEY,
    RetryExhausted,
    build_client_order_id,
)

logger = logging.getLogger("executor.engine")


class ForceCloseMixin:

        @staticmethod
        def _parse_force_close_pair_key(message_data: str):
            """Extracts the pair_key from a FORCE_CLOSE directive (JSON or plain text)."""
            if not message_data:
                return None
            try:
                parsed = json.loads(message_data)
            except Exception:
                return message_data.strip() or None
            if isinstance(parsed, dict):
                return parsed.get("pair_key")
            return None

        async def _cancel_pending_order_for_force_close(
            self, pair_key: str, orders: dict, order_label: str
        ) -> bool:
            """Cancels a pending order (exit/entry) tracked in `orders`. Returns True ONLY if cancellation succeeded on the exchange."""
            order_info = orders.get(pair_key)
            if not order_info:
                return False
            try:
                res = await self.client.cancel_local_order(order_info["order_id"], order_info["symbol"])
                # If exchange client returns status, ensure it's not an explicit failure
                if isinstance(res, dict) and res.get("status") in ("failed", "rejected"):
                    logger.error(
                        f"[Force Close] Exchange rejected cancellation of {order_label} order {order_info['order_id']}: {res}"
                    )
                    return False
                logger.info(
                    f"[Force Close] Canceled pending {order_label} order {order_info['order_id']}"
                )
                return True
            except Exception:
                logger.exception(f"failed to cancel {order_label} order {order_info.get('order_id')} on force close")
                return False

        async def _cancel_pending_exit_for_force_close(self, pair_key: str):
            if await self._cancel_pending_order_for_force_close(
                pair_key, self.pending_exit_orders, "exit"
            ):
                self.pending_exit_orders.pop(pair_key, None)

        async def _cancel_pending_entry_for_force_close(self, pair_key: str):
            order_info = self.pending_entry_orders.get(pair_key)
            if not await self._cancel_pending_order_for_force_close(
                pair_key, self.pending_entry_orders, "entry"
            ):
                return
            if hasattr(self.client, "unlock_funds"):
                self.client.unlock_funds(
                    float(order_info.get("spot_cost_usdt", 0.0)),
                    float(order_info.get("futures_margin_usdt", 0.0)),
                )
                bals = self.client.get_balances()
                await self.redis.set(BALANCES_KEY, json.dumps(bals))
            self.pending_entry_orders.pop(pair_key, None)
            await self._clear_pending_entry(pair_key)

        async def _close_global_short_leg(self, pair_key: str, pos):
            """Closes the global futures short leg at market. Returns True on verified success."""
            global_sym = self._resolve_global_symbol(pos.symbol)
            logger.warning(f"[Force Close] Closing global short {pos.qty} on {global_sym}...")
            try:
                res = await self.client.close_global_short_market(
                    global_sym,
                    pos.qty,
                    client_order_id=build_client_order_id(
                        pair_key, "force-close", self._next_order_seq()
                    ),
                )
                # If the exchange returned an order object, verify status is not rejected/failed.
                # Treat None/non-dict as unverified — log a warning but allow the caller
                # to proceed; the position will be caught by RECONCILIATION_REQUIRED if
                # a later reconcile detects residual exposure.
                if res is None:
                    logger.warning(
                        f"[Force Close] Futures close returned None for {pair_key}; treating as unverified success."
                    )
                elif isinstance(res, dict):
                    status = str(res.get("status", "")).lower()
                    if status in ("rejected", "failed", "canceled"):
                        logger.error(f"[Force Close] Futures close order rejected/failed: {res}")
                        return False
                logger.info(f"[Force Close] Global short closed for {pos.qty} {global_sym}")
                return True
            except Exception:
                logger.exception("failed to close global short on force close")
                return False

        async def _liquidate_spot_leg(self, pair_key: str, pos) -> float:
            """
            Liquidates the local spot leg. Returns the realized exit price in IDR
            only if the order actually filled. Never falls back to entry price.
            """
            logger.warning(f"[Force Close] Selling {pos.qty} {pos.symbol} spot leg...")
            try:
                seq = self._next_order_seq()
                spot_order = await helpers.retry_with_backoff(
                    lambda: self.client.place_local_market_sell(
                        pos.symbol,
                        pos.qty,
                        client_order_id=build_client_order_id(pair_key, "force-liq", seq),
                    ),
                    max_retries=4,  # 5 total attempts
                    base_delay=0.5,
                    label=f"force-close spot liquidation {pos.symbol} for {pair_key}",
                )
                # Verify actual fill and positive exit price. `amount` is the requested
                # quantity (not filled), so only use `filled` for fill verification.
                if isinstance(spot_order, dict):
                    filled_qty = float(spot_order.get("filled", 0.0) or 0.0)
                    status = str(spot_order.get("status", "")).lower()
                    spot_exit_price_idr = helpers.extract_fill_price(spot_order)
                    if (status == "closed" or filled_qty > 0) and spot_exit_price_idr > 0:
                        logger.info(
                            f"[Force Close] Spot leg liquidated: {pos.qty} {pos.symbol} "
                            f"@ {spot_exit_price_idr} (filled={filled_qty})"
                        )
                        return spot_exit_price_idr

                logger.critical(
                    f"[Force Close] FAILED to liquidate spot leg for {pair_key}. "
                    f"MANUAL INTERVENTION REQUIRED: {pos.qty} {pos.symbol} is UNHEDGED. Response: {spot_order}"
                )
            except RetryExhausted:
                logger.exception(
                    f"[Force Close] spot liquidation exhausted retries for {pair_key}"
                )
            return 0.0

        async def _resolve_force_close_prices(self, pair_key: str, pos, spot_exit_price_idr: float):
            """Returns (current_ask_usdt, usdt_rate, current_spot_bid_idr) for PnL accounting."""
            current_ask = pos.short_entry_usdt
            usdt_rate = self.FALLBACK_USDT_IDR_RATE
            current_spot_bid = pos.entry_price_idr

            price_data = await self._fetch_live_price(pair_key, pos)
            if price_data:
                current_ask, usdt_rate = price_data

            live_price_raw = await self.redis.get(f"arb:live_price:{pair_key}")
            if live_price_raw:
                try:
                    s_snap = json.loads(live_price_raw)
                    if s_snap.get("local_best_bid"):
                        current_spot_bid = float(s_snap["local_best_bid"])
                except Exception:
                    pass

            if spot_exit_price_idr > 0:
                current_spot_bid = spot_exit_price_idr
            return current_ask, usdt_rate, current_spot_bid

        @staticmethod
        def _compute_force_close_pnl(pos, current_ask: float, usdt_rate: float, current_spot_bid: float) -> dict:
            """Computes realized PnL components for a force-closed position."""
            spot_pnl_idr = pos.qty * (current_spot_bid - pos.entry_price_idr)
            spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
            futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - current_ask)
            funding_usdt = float(pos.accumulated_funding_usdt)
            fees_usdt = (pos.qty * (pos.short_entry_usdt + current_ask)) * 0.0005
            net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
            capital_usdt = pos.qty * pos.short_entry_usdt
            return {
                "spot_pnl_idr": spot_pnl_idr,
                "spot_pnl_usdt": spot_pnl_usdt,
                "futures_pnl_usdt": futures_pnl_usdt,
                "funding_usdt": funding_usdt,
                "fees_usdt": fees_usdt,
                "net_profit_usdt": net_profit_usdt,
                "net_profit_idr": net_profit_usdt * usdt_rate,
                "roi_pct": (net_profit_usdt / capital_usdt * 100.0) if capital_usdt > 0 else 0.0,
            }

        async def _record_force_close_trade(
            self,
            pair_key: str,
            pos,
            pnl: dict,
            current_ask: float,
            current_spot_bid: float,
            spot_sell_failed: bool,
            usdt_rate: float,
        ):
            """Persists the closed trade, settles balances, and updates stats/events."""
            closed_trade = {
                "trade_id": f"force-{int(time.time()*1000)}",
                "local_exchange": pos.local_exchange,
                "global_exchange": pos.global_exchange,
                "symbol": pos.symbol,
                "qty": pos.qty,
                "entry_price_idr": pos.entry_price_idr,
                "exit_price_idr": current_spot_bid,
                "short_entry_usdt": pos.short_entry_usdt,
                "short_exit_usdt": current_ask,
                "spot_pnl_idr": pnl["spot_pnl_idr"],
                "futures_pnl_usdt": pnl["futures_pnl_usdt"],
                "accumulated_funding_usdt": pnl["funding_usdt"],
                "net_profit_usdt": pnl["net_profit_usdt"],
                "net_profit_idr": pnl["net_profit_idr"],
                "net_roi_pct": pnl["roi_pct"],
                "is_mock": getattr(self.config, "mock_mode", False),
                "spot_liquidation_failed": spot_sell_failed,
                "closed_at_ms": int(time.time() * 1000),
            }
            await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(closed_trade))
            await self.redis.ltrim(
                ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN
            )

            # Update exchange account balances with realized PnL & release locked funds
            spot_cost = getattr(pos, "spot_cost_usdt", pos.qty * pos.entry_price_idr / usdt_rate)
            fut_margin = getattr(pos, "futures_margin_usdt", pos.qty * pos.short_entry_usdt)
            if hasattr(self.client, "apply_realized_trade"):
                self.client.apply_realized_trade(
                    spot_cost_usdt=spot_cost,
                    spot_pnl_usdt=pnl["spot_pnl_usdt"],
                    spot_fees_usdt=0.0,
                    futures_margin_usdt=fut_margin,
                    futures_pnl_usdt=pnl["futures_pnl_usdt"],
                    funding_usdt=pnl["funding_usdt"],
                    futures_fees_usdt=pnl["fees_usdt"],
                )
                bals = self.client.get_balances()
                await self.redis.set(BALANCES_KEY, json.dumps(bals))

            pipe = self.redis.pipeline()
            pipe.hincrby("arb:stats", "total_closed", 1)
            if pnl["net_profit_usdt"] > 0:
                pipe.hincrby("arb:stats", "win_count", 1)
            pipe.hincrbyfloat("arb:stats", "total_pnl_usdt", pnl["net_profit_usdt"])
            pipe.hincrbyfloat("arb:stats", "total_pnl_idr", pnl["net_profit_idr"])
            event_msg = {
                "event": "POSITION_CLOSED",
                **closed_trade,
            }
            pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
            await pipe.execute()

        async def handle_force_close(self, message_data: str):
            """
            Handles FORCE_CLOSE directive from Telegram or API:
            1. Cancels pending exit/entry orders on local exchange.
            2. Closes open Short position on Global Futures via Market Buy (reduceOnly=True).
            3. Liquidates local spot coins if necessary.
            4. Removes active position from Redis.
            """
            logger.warning(f"Received FORCE_CLOSE directive: {message_data}")
            pair_key = self._parse_force_close_pair_key(message_data)
            if not pair_key:
                return

            # Serialize against the entry/exit paths for this pair: cancelling
            # pending orders and liquidating legs must not interleave with a
            # hedge or exit fill processed on another task.
            async with self._pair_lock(pair_key):
                await self._force_close_locked(pair_key)

        async def _cancel_pending_orders_for_force_close(self, pair_key: str) -> None:
            """Cancels pending exit and entry orders before closing position legs."""
            await self._cancel_pending_exit_for_force_close(pair_key)
            await self._cancel_pending_entry_for_force_close(pair_key)

        async def _execute_force_close_legs(self, pair_key: str, pos) -> Tuple[bool, bool, dict]:
            """
            Executes short closure on futures and spot liquidation.
            Returns (futures_closed, spot_sell_failed, pnl).
            """
            if not pos or pos.qty <= 0:
                return True, False, {"net_profit_usdt": 0.0, "net_profit_idr": 0.0}

            futures_closed = await self._close_global_short_leg(pair_key, pos)
            spot_sell_failed = True
            spot_exit_price_idr = 0.0

            if futures_closed:
                spot_exit_price_idr = await self._liquidate_spot_leg(pair_key, pos)
                spot_sell_failed = spot_exit_price_idr <= 0

            current_ask, usdt_rate, current_spot_bid = await self._resolve_force_close_prices(
                pair_key, pos, spot_exit_price_idr if futures_closed else 0.0
            )
            pnl = self._compute_force_close_pnl(pos, current_ask, usdt_rate, current_spot_bid)

            if futures_closed and not spot_sell_failed:
                await self._record_force_close_trade(
                    pair_key,
                    pos,
                    pnl,
                    current_ask,
                    current_spot_bid,
                    spot_sell_failed,
                    usdt_rate,
                )
            elif futures_closed and spot_sell_failed:
                # Futures leg closed but spot liquidation failed — record partial close so
                # the futures PnL is not lost from history, and flag it clearly.
                logger.error(
                    f"[Force Close] Recording partial force-close for {pair_key}: "
                    f"futures closed, spot liquidation FAILED. Manual spot sale required."
                )
                await self._record_force_close_trade(
                    pair_key,
                    pos,
                    pnl,
                    current_ask,
                    current_spot_bid,
                    spot_sell_failed,
                    usdt_rate,
                )

            return futures_closed, spot_sell_failed, pnl

        async def _handle_force_close_failure(
            self, pair_key: str, pos, futures_closed: bool, spot_sell_failed: bool
        ) -> None:
            """P0: Retains persistent state with RECONCILIATION_REQUIRED on partial or failed force close."""
            fail_reason = "FORCE_CLOSE_PARTIAL_OR_FAILED"
            fail_msg = (
                f"futures_closed={futures_closed}, spot_liquidated={not spot_sell_failed}. "
                "Manual reconciliation required; position retained in state."
            )
            if pos:
                pos.state = "RECONCILIATION_REQUIRED"
                pos.updated_at = int(time.time() * 1000)
                await self.redis.set(f"arb:active_pos:{pair_key}", json.dumps(pos.to_dict()))

            self._mark_failed(
                pair_key,
                fail_reason,
                fail_msg,
                detail={
                    "qty": pos.qty if pos else 0.0,
                    "symbol": pos.symbol if pos else "",
                    "futures_closed": futures_closed,
                    "spot_sell_failed": spot_sell_failed,
                },
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            logger.critical(
                f"[Force Close Alert] {pair_key} incomplete! Retaining active position in Redis: {fail_msg}"
            )

        async def _handle_force_close_success(self, pair_key: str, pos, pnl: dict) -> None:
            """Cleans up active state in Redis and memory when force close fully succeeds."""
            await self.redis.delete(f"arb:active_pos:{pair_key}")
            self.active_positions.pop(pair_key, None)
            self.last_pegged_global_ask.pop(pair_key, None)
            if pos and pos.qty > 0:
                self.record_realized_pnl(pnl["net_profit_usdt"])
            self.states[pair_key] = PositionState.CLOSED
            logger.info(f"[Force Close Complete] Position for {pair_key} successfully closed.")

        async def _force_close_locked(self, pair_key: str) -> None:
            # Guard concurrent exit-fill processing for this pair while force close runs.
            self.states[pair_key] = PositionState.RECONCILIATION_REQUIRED
            await self._cancel_pending_orders_for_force_close(pair_key)
            pos = self.active_positions.get(pair_key)
            futures_closed, spot_sell_failed, pnl = await self._execute_force_close_legs(pair_key, pos)

            if not futures_closed or spot_sell_failed:
                await self._handle_force_close_failure(pair_key, pos, futures_closed, spot_sell_failed)
            else:
                await self._handle_force_close_success(pair_key, pos, pnl)

            await self.publish_status()
