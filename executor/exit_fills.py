"""Exit fill settlement, paired PnL accounting, and position finalization."""

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


class ExitFillMixin:
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
            cumulative_spot_cost = None
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
                        cumulative_spot_cost = float(cumulative_cost)
                        incremental_cost = cumulative_spot_cost - prior_cost
                        if incremental_cost > 0 and unclosed_delta > 0:
                            exit_price_idr = incremental_cost / unclosed_delta
                    except (TypeError, ValueError):
                        cumulative_spot_cost = None
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
                if cumulative_spot_cost is not None:
                    order_info["closed_spot_cost_idr"] = cumulative_spot_cost
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
                    "estimated_fees_usdt": fees_usdt,
                    "fees_estimated": True,
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
                        # Fee reserve is a combined two-leg estimate; split it
                        # evenly for paper balance accounting while keeping the
                        # authoritative trade PnL deducted exactly once above.
                        spot_fees_usdt=fees_usdt / 2.0,
                        futures_margin_usdt=fut_margin,
                        futures_pnl_usdt=futures_pnl_usdt,
                        funding_usdt=funding_usdt,
                        futures_fees_usdt=fees_usdt / 2.0,
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
