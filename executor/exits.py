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

class ExitMixin:

        async def _fetch_live_price(
            self, pair_tag: str, pos: Optional[ActivePosition] = None
        ) -> Optional[tuple[float, float]]:
            """Returns (global_ask_usdt, usdt_idr_rate) from a fresh live-price
            snapshot, or None when no fresh data exists. A stale snapshot is NOT
            substituted with the entry price: doing so fabricates a zero futures
            PnL and makes floating PnL display unreliable."""
            live_price_key = f"arb:live_price:{pair_tag}"
            live_price_raw = await self.redis.get(live_price_key)
            if live_price_raw:
                try:
                    live_snapshot = json.loads(live_price_raw)
                    global_ask_usdt = float(live_snapshot.get("global_best_ask_usdt", 0.0))
                    usdt_idr_rate = float(live_snapshot.get("usdt_idr_rate", 0.0) or 0.0)
                    if global_ask_usdt > 0 and math.isfinite(usdt_idr_rate) and usdt_idr_rate > 0 and not helpers.snapshot_is_stale(live_snapshot):
                        return global_ask_usdt, usdt_idr_rate
                except Exception:
                    pass

            return None

        async def _handle_funding_rollover(
            self, pair_tag: str, pos: ActivePosition, global_ask_usdt: float, now_ms: int
        ) -> bool:
            """
            Applies only funding payments returned by the exchange settlement ledger.
            The market's settlement interval is never assumed to be eight hours.
            """
            if pos.next_funding_time_ms <= 0 or now_ms < pos.next_funding_time_ms:
                return False

            retry_after = getattr(self, "_funding_reconcile_retry_after_ms", {})
            if now_ms < retry_after.get(pair_tag, 0):
                return False
            retry_after[pair_tag] = now_ms + 30_000
            self._funding_reconcile_retry_after_ms = retry_after

            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            since_ms = (
                pos.last_funding_time_ms
                if pos.last_funding_time_ms > 0
                else max(0, pos.next_funding_time_ms - 1)
            )
            try:
                payments = await self.client.fetch_funding_history(
                    symbol, since=since_ms, limit=100
                )
            except Exception:
                logger.exception(
                    f"[Funding Reconciliation] cannot fetch actual settlement history "
                    f"for {pair_tag}; estimated funding will not be credited"
                )
                return False

            if not isinstance(payments, list):
                logger.error(
                    f"[Funding Reconciliation] invalid funding history response for {pair_tag}"
                )
                return False

            settled = []
            for payment in payments:
                if not isinstance(payment, dict):
                    continue
                timestamp = int(payment.get("timestamp") or 0)
                if timestamp <= pos.last_funding_time_ms or timestamp > now_ms:
                    continue
                asset = str(payment.get("code") or payment.get("currency") or "USDT").upper()
                if asset != "USDT":
                    logger.error(
                        f"[Funding Reconciliation] unsupported settlement asset {asset} "
                        f"for {pair_tag}; payment is not added to USDT PnL"
                    )
                    continue
                try:
                    amount = float(payment.get("amount"))
                except (TypeError, ValueError):
                    logger.error(
                        f"[Funding Reconciliation] invalid payment amount for {pair_tag}: {payment}"
                    )
                    continue
                settled.append((timestamp, amount, payment))

            # Refresh the venue's actual next settlement schedule. Different
            # contracts/venues can use 1h, 2h, 4h, 8h or custom intervals.
            next_funding_ms = 0
            current_rate = pos.current_funding_rate
            try:
                rate_data = await self.client.fetch_funding_rate(symbol)
                if isinstance(rate_data, dict):
                    next_funding_ms = int(
                        rate_data.get("nextFundingTimestamp")
                        or rate_data.get("nextFundingTime")
                        or 0
                    )
                    if rate_data.get("fundingRate") is not None:
                        current_rate = float(rate_data["fundingRate"])
            except Exception:
                logger.exception(
                    f"[Funding Reconciliation] cannot refresh next settlement schedule "
                    f"for {pair_tag}; automatic funding rollover is paused"
                )

            if not settled:
                if next_funding_ms > now_ms:
                    pos.next_funding_time_ms = next_funding_ms
                    pos.current_funding_rate = current_rate
                    pos.updated_at = now_ms
                    await self.redis.set(
                        f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                    )
                return False

            settled.sort(key=lambda item: item[0])
            actual_amount = sum(item[1] for item in settled)
            pos.accumulated_funding_usdt += actual_amount
            pos.last_funding_time_ms = settled[-1][0]
            pos.current_funding_rate = current_rate
            pos.next_funding_time_ms = (
                next_funding_ms if next_funding_ms > pos.last_funding_time_ms else 0
            )
            pos.updated_at = now_ms

            await self.redis.set(
                f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
            )
            logger.info(
                f"[Funding Reconciliation] {pair_tag}: applied {len(settled)} actual "
                f"settlement(s), net {actual_amount:+.8f} USDT; "
                f"cumulative={pos.accumulated_funding_usdt:+.8f} USDT"
            )
            if pos.next_funding_time_ms <= 0:
                logger.critical(
                    f"[Funding Reconciliation] next settlement time unavailable for "
                    f"{pair_tag}; funding accrual paused pending a valid exchange schedule"
                )
            return True

        def _should_repeg_exit(
            self, pair_tag: str, global_ask_usdt: float, funding_passed: bool
        ) -> bool:
            if pair_tag not in self.pending_exit_orders:
                return True

            last_ask = self.last_pegged_global_ask.get(pair_tag, 0.0)
            ask_movement = 0.0
            if last_ask > 0:
                ask_movement = abs(global_ask_usdt - last_ask) / last_ask

            return (
                (last_ask <= 0)
                or (ask_movement >= self.config.exit_price_move_threshold)
                or funding_passed
            )

        def _calculate_limit_sell_idr(
            self,
            pair_tag: str,
            pos: ActivePosition,
            global_ask_usdt: float,
            usdt_idr_rate: float,
        ) -> tuple[float, float]:
            # 1. Base pegged exit price using spread & funding decay
            pos_value_usdt = pos.qty * global_ask_usdt
            funding_decay_pct = 0.0
            if pos_value_usdt > 0:
                funding_decay_pct = (pos.accumulated_funding_usdt / pos_value_usdt) * 100.0

            effective_exit_spread_pct = pos.base_target_exit_spread_pct - funding_decay_pct
            pegged_limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (
                1.0 + (effective_exit_spread_pct / 100.0)
            )

            # 2. Combined two-leg profit floor. Use one fee convention everywhere:
            # estimated_round_trip_fee_pct is the fee reserve for both legs and
            # the complete round trip, expressed as a fraction of position notional.
            capital_usdt = pos.qty * pos.short_entry_usdt
            fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.001) or 0.0)
            est_fees_usdt = capital_usdt * fee_pct

            # Closing the short is a market BUY at the futures ask. Add an adverse
            # movement allowance so the spot limit is not priced against a perfect
            # top-of-book fill.
            exit_slippage_pct = float(getattr(self.config, "max_exit_slippage_pct", 0.003) or 0.0)
            risk_adjusted_global_ask = global_ask_usdt * (1.0 + exit_slippage_pct)
            futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - risk_adjusted_global_ask)

            # Buffer min ROI: at least +0.05% net ROI or 25% of base target exit spread
            min_roi_pct = max(0.05, pos.base_target_exit_spread_pct * 0.25)
            min_profit_target_usdt = capital_usdt * (min_roi_pct / 100.0)

            # Required spot PnL covers the conservative futures buyback, fees and funding.
            required_spot_pnl_usdt = (
                min_profit_target_usdt
                + est_fees_usdt
                - futures_pnl_usdt
                - pos.accumulated_funding_usdt
            )

            min_profitable_sell_idr = pos.entry_price_idr
            if pos.qty > 0 and usdt_idr_rate > 0:
                min_profitable_sell_idr += (required_spot_pnl_usdt * usdt_idr_rate) / pos.qty

            limit_sell_idr = max(pegged_limit_sell_idr, min_profitable_sell_idr)

            tick_size = getattr(pos, "local_price_tick", 1.0)
            if tick_size <= 0:
                if pair_tag in self.pending_exit_orders:
                    tick_size = self.pending_exit_orders[pair_tag].get("tick_size", 1.0)
                else:
                    tick_size = 1.0

            # CEIL to tick size so rounding NEVER rounds down below profitable floor
            limit_sell_idr = ceil_to_tick(limit_sell_idr, tick_size)

            logger.info(
                f"[State 3 Dynamic Exit Pegging] Re-pegging Limit Sell for {pair_tag}: "
                f"global_ask={global_ask_usdt}, spread={effective_exit_spread_pct:.3f}%, "
                f"floor_idr={min_profitable_sell_idr:.2f}, limit_sell_idr={limit_sell_idr:.2f}"
            )
            return limit_sell_idr, tick_size

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

            if not self._should_repeg_exit(pair_tag, global_ask_usdt, funding_passed):
                return

            limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
                pair_tag, pos, global_ask_usdt, usdt_idr_rate
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

        async def _execute_exit_fill(
            self,
            pair_tag: str,
            order_info: dict,
            pos: ActivePosition,
            unclosed_delta: float,
            spot_order: Optional[dict] = None,
        ) -> None:
            """Close global short via market buy and update position state.

            Prefer exchange cumulative fill cost/average over the submitted limit
            price. The limit is a conservative fallback when metadata is absent.
            """
            exit_price_idr = float(order_info.get("price", 0.0))
            if isinstance(spot_order, dict):
                prior_cost = float(order_info.get("closed_spot_cost_idr", 0.0) or 0.0)
                cumulative_cost = spot_order.get("cost")
                if cumulative_cost is None and spot_order.get("average") is not None:
                    try:
                        cumulative_cost = float(spot_order["average"]) * float(spot_order.get("filled") or 0.0)
                    except (TypeError, ValueError):
                        cumulative_cost = None
                if cumulative_cost is not None:
                    try:
                        incremental_cost = float(cumulative_cost) - prior_cost
                        if incremental_cost > 0 and unclosed_delta > 0:
                            exit_price_idr = incremental_cost / unclosed_delta
                    except (TypeError, ValueError):
                        pass
                elif spot_order.get("price") is not None:
                    try:
                        candidate = float(spot_order["price"])
                        if candidate > 0 and math.isfinite(candidate):
                            exit_price_idr = candidate
                    except (TypeError, ValueError):
                        pass
            if exit_price_idr <= 0 or not math.isfinite(exit_price_idr):
                raise ValueError("spot exit fill price unavailable; refusing to report fabricated PnL")
            entry_price_idr = float(pos.entry_price_idr)
            short_entry_usdt = float(pos.short_entry_usdt)

            # Fetch FX rate and latest global ask
            usdt_rate = helpers.FALLBACK_USDT_IDR_RATE
            live_global_ask = float(self.last_pegged_global_ask.get(pair_tag, short_entry_usdt))
            live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
            if live_price_raw:
                try:
                    live_snapshot = json.loads(live_price_raw)
                    usdt_rate = float(live_snapshot.get("usdt_idr_rate", helpers.FALLBACK_USDT_IDR_RATE))
                    g_ask = float(live_snapshot.get("global_best_ask_usdt", 0.0))
                    if g_ask > 0:
                        live_global_ask = g_ask
                except Exception:
                    pass

            logger.info(
                f"[State 4 Exit Execution] Local Limit Sell fill detected: sold_delta={unclosed_delta} "
                f"on {pair_tag}. Immediately closing Short via Market Buy (reduceOnly=True)!"
            )
            try:
                global_sym = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
                unwind_coid = build_client_order_id(
                    pair_tag, "unwind", self._next_order_seq()
                )
                close_order = await self.client.close_global_short_market(
                    global_sym,
                    unclosed_delta,
                    client_order_id=unwind_coid,
                )
                if not isinstance(close_order, dict):
                    raise ValueError(
                        "futures close returned no verifiable order payload after spot exit fill"
                    )
                close_status = str(close_order.get("status", "")).lower()
                if close_status not in ("closed", "filled"):
                    raise ValueError(
                        f"futures close is not terminal after spot exit fill: {close_status}"
                    )
                try:
                    actual_closed_qty = float(close_order.get("filled") or 0.0)
                except (TypeError, ValueError):
                    actual_closed_qty = 0.0
                qty_tolerance = max(1e-12, unclosed_delta * 1e-8)
                if actual_closed_qty + qty_tolerance < unclosed_delta:
                    raise ValueError(
                        f"futures close fill incomplete after spot sold: "
                        f"requested={unclosed_delta}, filled={actual_closed_qty}"
                    )
                if actual_closed_qty > unclosed_delta + qty_tolerance:
                    raise ValueError(
                        f"futures close overfilled: requested={unclosed_delta}, "
                        f"filled={actual_closed_qty}"
                    )

                self.supabase.order(
                    pair_tag,
                    "futures_exit",
                    pos.global_exchange,
                    "perpetual",
                    global_sym,
                    close_order,
                    client_order_id=unwind_coid,
                    reduce_only=True,
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )

                # Snapshot qty before mutation so PnL fractions stay consistent.
                total_qty_before = pos.qty
                order_info["closed_short_qty"] += unclosed_delta
                pos.qty -= unclosed_delta

                # Persist updated qty and closed_short_qty immediately so a crash before
                # _finalize_exit_position does not cause the next poll to double-close.
                pos.updated_at = int(time.time() * 1000)
                await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

                # PnL must use the price the short actually closed at, not the last
                # pegged quote: a market buy fills through the book and the gap is
                # real money. Fall back to the live ask only when the venue returns
                # no fill price at all.
                short_exit_usdt = helpers.extract_fill_price(close_order)
                if short_exit_usdt <= 0:
                    raise ValueError(
                        "futures close fill price unavailable after spot sell; "
                        "PnL cannot be safely finalized"
                    )

                spot_pnl_idr = unclosed_delta * (exit_price_idr - entry_price_idr)
                spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
                futures_pnl_usdt = unclosed_delta * (short_entry_usdt - short_exit_usdt)

                # Allocate only the pro-rata share of accumulated funding for this
                # partial fill. Using the full accumulator per fill inflates PnL on
                # multi-fill exits; the remainder stays on the position for later fills.
                fill_ratio = (unclosed_delta / total_qty_before) if total_qty_before > 0 else 1.0
                funding_usdt = float(pos.accumulated_funding_usdt) * fill_ratio
                pos.accumulated_funding_usdt -= funding_usdt

                # Entry commissions are not persisted per fill. Apply the same
                # conservative round-trip reserve used by the profit-floor calculation.
                fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.001) or 0.0)
                capital_usdt = unclosed_delta * short_entry_usdt
                fees_usdt = capital_usdt * fee_pct
                net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
                if isinstance(spot_order, dict):
                    cumulative_cost = spot_order.get("cost")
                    if cumulative_cost is None and spot_order.get("average") is not None:
                        cumulative_cost = float(spot_order["average"]) * float(spot_order.get("filled") or 0.0)
                    if cumulative_cost is not None:
                        order_info["closed_spot_cost_idr"] = float(cumulative_cost)
                net_profit_idr = net_profit_usdt * usdt_rate

                capital_usdt = unclosed_delta * short_entry_usdt
                roi_pct = (net_profit_usdt / capital_usdt * 100.0) if capital_usdt > 0 else 0.0

                is_mock = getattr(self.config, "mock_mode", False)

                if net_profit_usdt < 0:
                    logger.warning(
                        f"[State 4 Realized Loss] Local Limit Sell fill resulted in net negative PnL: "
                        f"{net_profit_usdt:.4f} USDT (ROI: {roi_pct:.3f}%). Spot PnL: {spot_pnl_usdt:+.4f} USDT, "
                        f"Futures Short PnL: {futures_pnl_usdt:+.4f} USDT, Funding: +{funding_usdt:.4f} USDT, Fees: -{fees_usdt:.4f} USDT. "
                        f"Closing Short to maintain delta-neutral position integrity."
                    )
                else:
                    logger.info(
                        f"[State 4 Realized Profit] Trade closed with profit: +{net_profit_usdt:.4f} USDT (ROI: +{roi_pct:.3f}%). "
                        f"Spot PnL: {spot_pnl_usdt:+.4f} USDT, Futures Short PnL: {futures_pnl_usdt:+.4f} USDT."
                    )
                closed_trade = {
                    "trade_id": f"trade-{int(time.time()*1000)}",
                    "local_exchange": pos.local_exchange,
                    "global_exchange": pos.global_exchange,
                    "symbol": pos.symbol,
                    "qty": unclosed_delta,
                    "entry_price_idr": entry_price_idr,
                    "exit_price_idr": exit_price_idr,
                    "short_entry_usdt": short_entry_usdt,
                    "short_exit_usdt": short_exit_usdt,
                    "spot_pnl_idr": spot_pnl_idr,
                    "futures_pnl_usdt": futures_pnl_usdt,
                    "accumulated_funding_usdt": funding_usdt,
                    "net_profit_usdt": net_profit_usdt,
                    "net_profit_idr": net_profit_idr,
                    "net_roi_pct": roi_pct,
                    "is_mock": is_mock,
                    "closed_at_ms": int(time.time() * 1000),
                }

                await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(closed_trade))
                await self.redis.ltrim(ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN)
                self.supabase.trade_closed(pair_tag, closed_trade, pos)

                # Release only the pro-rata share of locked capital for this fill.
                spot_cost = getattr(pos, "spot_cost_usdt", unclosed_delta * entry_price_idr / usdt_rate) * fill_ratio
                fut_margin = getattr(pos, "futures_margin_usdt", unclosed_delta * short_entry_usdt) * fill_ratio
                if hasattr(self.client, "apply_realized_trade"):
                    self.client.apply_realized_trade(
                        spot_cost_usdt=spot_cost,
                        spot_pnl_usdt=spot_pnl_usdt,
                        spot_fees_usdt=0.0,
                        futures_margin_usdt=fut_margin,
                        futures_pnl_usdt=futures_pnl_usdt,
                        funding_usdt=funding_usdt,
                        futures_fees_usdt=fees_usdt,
                    )
                    bals = await self._get_balance_snapshot(
                        pair_tag=pair_tag,
                        symbol=pos.symbol,
                        usdt_idr_rate=usdt_rate,
                    )
                    if bals is not None:
                        await self.redis.set(BALANCES_KEY, json.dumps(bals))

                # Update stats in Redis
                pipe = self.redis.pipeline()
                pipe.hincrby("arb:stats", "total_closed", 1)
                if net_profit_usdt > 0:
                    pipe.hincrby("arb:stats", "win_count", 1)
                pipe.hincrbyfloat("arb:stats", "total_pnl_usdt", net_profit_usdt)
                pipe.hincrbyfloat("arb:stats", "total_pnl_idr", net_profit_idr)

                # Publish event to arb:trade_events for Telegram Bot
                event_msg = {
                    "event": "POSITION_CLOSED",
                    **closed_trade,
                }
                pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
                await pipe.execute()

                self.record_realized_pnl(net_profit_usdt)
                await self.publish_status()
                await self._finalize_exit_position(pair_tag, pos)
            except Exception as ee:
                # The local spot sell has already filled. Do not retry this exit
                # automatically: the futures close may have failed, partially filled,
                # or timed out after acceptance. Preserve state for manual reconciliation.
                pos.state = "RECONCILIATION_REQUIRED"
                pos.updated_at = int(time.time() * 1000)
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                await self.redis.set(
                    f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                )
                self._mark_failed(
                    pair_tag,
                    "EXIT_HEDGE_RECONCILIATION_REQUIRED",
                    "Spot exit filled but futures close could not be verified; automatic exit retries blocked.",
                    detail={
                        "spot_order_id": order_info.get("order_id"),
                        "spot_sold_qty": unclosed_delta,
                        "futures_close_error": str(ee),
                    },
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                logger.critical(
                    f"[State 4 CRITICAL] Futures close not verified after spot exit fill "
                    f"for {pair_tag}: {ee}. Manual reconciliation required."
                )
                await self.publish_status()

        def _resolve_global_symbol(self, symbol: str) -> str:
            """Convert local/pos symbol into global futures symbol format."""
            global_sym = symbol.upper()
            if "/" not in global_sym and "_" in global_sym:
                global_sym = global_sym.replace("_", "/")
            if "IDR" in global_sym:
                base = global_sym.split("/")[0] if "/" in global_sym else global_sym.replace("IDR", "")
                return f"{base}/USDT:USDT"
            return global_sym

        async def _finalize_exit_position(
            self, pair_tag: str, pos: ActivePosition
        ) -> None:
            """Update Redis active position or delete if fully closed."""
            active_key = f"arb:active_pos:{pair_tag}"
            if pos.qty <= 0.00001:  # dust threshold / fully exited
                logger.info(
                    f"[State 4 Complete] Arbitrage position for {pair_tag} fully exited! Deleting active pos."
                )
                await self.redis.delete(active_key)
                del self.active_positions[pair_tag]
                del self.pending_exit_orders[pair_tag]
                if pair_tag in self.last_pegged_global_ask:
                    del self.last_pegged_global_ask[pair_tag]
                self.states[pair_tag] = PositionState.CLOSED
            else:
                pos.updated_at = int(time.time() * 1000)
                await self.redis.set(active_key, json.dumps(pos.to_dict()))
