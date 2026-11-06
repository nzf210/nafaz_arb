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


        async def _close_global_short_leg(self, pair_key: str, pos):
            """Closes the futures short; returns its actual fill price only after flat verification."""
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
                if not isinstance(res, dict):
                    logger.critical(
                        f"[Force Close] futures close returned no verifiable order for {pair_key}"
                    )
                    return 0.0
                status = str(res.get("status", "")).lower()
                if status not in ("closed", "filled"):
                    logger.error(
                        f"[Force Close] futures close is not terminal (status={status}): {res}"
                    )
                    return 0.0
                try:
                    filled_qty = float(res.get("filled") or 0.0)
                except (TypeError, ValueError):
                    filled_qty = 0.0
                qty_tolerance = max(1e-12, pos.qty * 1e-8)
                if filled_qty + qty_tolerance < pos.qty:
                    logger.critical(
                        f"[Force Close] futures close fill incomplete for {pair_key}: "
                        f"requested={pos.qty}, filled={filled_qty}"
                    )
                    return 0.0

                # Order acknowledgement is not proof that the short is flat.
                # The authoritative position endpoint must confirm zero exposure.
                if not hasattr(self.client, "fetch_positions"):
                    logger.critical(
                        f"[Force Close] cannot verify futures position for {pair_key}: "
                        "client has no fetch_positions method"
                    )
                    return False
                positions = await self.client.fetch_positions(global_sym)
                if positions is None:
                    logger.critical(
                        f"[Force Close] futures position lookup returned None for {pair_key}"
                    )
                    return False
                for position in positions:
                    if not isinstance(position, dict):
                        logger.critical(
                            f"[Force Close] invalid position payload for {pair_key}: {position!r}"
                        )
                        return 0.0
                    info = position.get("info") or {}
                    raw_size = position.get("contracts")
                    if raw_size is None:
                        raw_size = position.get("positionAmt", info.get("positionAmt"))
                    if raw_size is None:
                        logger.critical(
                            f"[Force Close] position size unavailable for {pair_key}: {position}"
                        )
                        return False
                    try:
                        remaining = abs(float(raw_size))
                    except (TypeError, ValueError):
                        return False
                    if remaining > 1e-12:
                        logger.critical(
                            f"[Force Close] residual futures exposure for {pair_key}: "
                            f"{remaining} contracts/units remain"
                        )
                        return False

                fill_price = helpers.extract_fill_price(res)
                if fill_price <= 0:
                    logger.critical(
                        f"[Force Close] futures position is flat but actual close fill price "
                        f"is unavailable for {pair_key}; PnL cannot be safely finalized"
                    )
                    return 0.0
                logger.info(
                    f"[Force Close] Futures position verified flat for {pair_key} "
                    f"after close response {res!r}"
                )
                return fill_price
            except Exception:
                logger.exception(
                    f"failed to close or verify global short on force close for {pair_key}"
                )
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
                # Require the full requested quantity to be filled. A partial
                # market sell leaves residual spot exposure and must not be reported
                # as a successful force close.
                if isinstance(spot_order, dict):
                    filled_qty = float(spot_order.get("filled", 0.0) or 0.0)
                    spot_exit_price_idr = helpers.extract_fill_price(spot_order)
                    if filled_qty + max(1e-12, pos.qty * 1e-8) < pos.qty:
                        order_id = str(spot_order.get("id", "") or "")
                        if order_id:
                            try:
                                verified = await self.client.fetch_local_order(
                                    order_id, pos.symbol
                                )
                                if isinstance(verified, dict):
                                    filled_qty = float(verified.get("filled", 0.0) or 0.0)
                                    spot_exit_price_idr = (
                                        helpers.extract_fill_price(verified)
                                        or spot_exit_price_idr
                                    )
                            except Exception:
                                logger.exception(
                                    f"[Force Close] cannot verify spot sell {order_id} for {pair_key}"
                                )
                    if (
                        filled_qty + max(1e-12, pos.qty * 1e-8) >= pos.qty
                        and spot_exit_price_idr > 0
                    ):
                        logger.info(
                            f"[Force Close] Spot leg fully liquidated: {pos.qty} {pos.symbol} "
                            f"@ {spot_exit_price_idr} (filled={filled_qty})"
                        )
                        return spot_exit_price_idr
                    logger.critical(
                        f"[Force Close] partial/unverified spot liquidation for {pair_key}: "
                        f"requested={pos.qty}, filled={filled_qty}; manual reconciliation required"
                    )

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


        async def _execute_force_close_legs(self, pair_key: str, pos) -> Tuple[bool, bool, dict]:
            """
            Executes short closure on futures and spot liquidation.
            Returns (futures_closed, spot_sell_failed, pnl).
            """
            if not pos or pos.qty <= 0:
                return True, False, {"net_profit_usdt": 0.0, "net_profit_idr": 0.0}

            short_exit_price_usdt = await self._close_global_short_leg(pair_key, pos)
            futures_closed = short_exit_price_usdt > 0
            spot_sell_failed = True
            spot_exit_price_idr = 0.0

            if futures_closed:
                spot_exit_price_idr = await self._liquidate_spot_leg(pair_key, pos)
                spot_sell_failed = spot_exit_price_idr <= 0

            current_ask, usdt_rate, current_spot_bid = await self._resolve_force_close_prices(
                pair_key, pos, spot_exit_price_idr if futures_closed else 0.0
            )
            if futures_closed:
                current_ask = short_exit_price_usdt
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
            orders_reconciled = await self._cancel_pending_orders_for_force_close(pair_key)
            pos = self.active_positions.get(pair_key)
            if not orders_reconciled:
                await self._handle_force_close_failure(
                    pair_key, pos, False, True
                )
                await self.publish_status()
                return

            futures_closed, spot_sell_failed, pnl = await self._execute_force_close_legs(pair_key, pos)

            if not futures_closed or spot_sell_failed:
                await self._handle_force_close_failure(pair_key, pos, futures_closed, spot_sell_failed)
            else:
                await self._handle_force_close_success(pair_key, pos, pnl)

            await self.publish_status()
