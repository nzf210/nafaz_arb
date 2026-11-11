"""exit execution methods for exit lifecycle management."""

import logging
import json
import time

from executor.models import ActivePosition, OrderRole, PositionState
from executor import helpers
from executor.helpers import ARB_TRADE_HISTORY_KEY, ARB_TRADE_HISTORY_MAX_LEN, BALANCES_KEY, build_client_order_id
from executor.fee_engine import rates_for_position, round_trip_fee_breakdown_usdt

logger = logging.getLogger("executor.engine")



ARB_STATS_KEY = "arb:stats"


class ExitExecutionMixin:
    """Focused exit pricing, order management, or execution responsibilities."""

    def _build_closed_trade_record(
        self, pos: ActivePosition, unclosed_delta: float,
        exit_price_idr: float, entry_price_idr: float, short_entry_usdt: float,
        short_exit_usdt: float, usdt_rate: float, total_qty_before: float,
    ) -> tuple[dict, float, float]:
        spot_pnl_idr = unclosed_delta * (exit_price_idr - entry_price_idr)
        spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
        futures_pnl_usdt = unclosed_delta * (short_entry_usdt - short_exit_usdt)
        fill_ratio = (
            unclosed_delta / total_qty_before if total_qty_before > 0 else 1.0
        )
        funding_usdt = float(pos.accumulated_funding_usdt) * fill_ratio
        pos.accumulated_funding_usdt -= funding_usdt

        rates = rates_for_position(pos)
        fee_breakdown = round_trip_fee_breakdown_usdt(
            unclosed_delta,
            entry_price_idr / usdt_rate,
            exit_price_idr / usdt_rate,
            short_entry_usdt,
            short_exit_usdt,
            rates,
        )
        spot_fees_usdt = fee_breakdown["spot_fees_usdt"]
        futures_fees_usdt = fee_breakdown["futures_fees_usdt"]
        fees_usdt = fee_breakdown["total_fees_usdt"]
        net_profit_usdt = (
            spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
        )
        net_profit_idr = net_profit_usdt * usdt_rate
        capital_usdt = unclosed_delta * short_entry_usdt
        roi_pct = (
            net_profit_usdt / capital_usdt * 100.0
            if capital_usdt > 0 else 0.0
        )
        is_mock = getattr(self.config, "mock_mode", False)
        if net_profit_usdt < 0:
            logger.warning(
                f"[State 4 Realized Loss] Local Limit Sell fill resulted in net negative PnL: "
                f"{net_profit_usdt:.4f} USDT (ROI: {roi_pct:.3f}%). Spot PnL: "
                f"{spot_pnl_usdt:+.4f} USDT, Futures Short PnL: "
                f"{futures_pnl_usdt:+.4f} USDT, Funding: +{funding_usdt:.4f} USDT, "
                f"Fees: -{fees_usdt:.4f} USDT. Closing Short to maintain delta-neutral "
                "position integrity."
            )
        else:
            logger.info(
                f"[State 4 Realized Profit] Trade closed with profit: "
                f"+{net_profit_usdt:.4f} USDT (ROI: +{roi_pct:.3f}%). "
                f"Spot PnL: {spot_pnl_usdt:+.4f} USDT, "
                f"Futures Short PnL: {futures_pnl_usdt:+.4f} USDT."
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
            "spot_fees_usdt": spot_fees_usdt,
            "futures_fees_usdt": futures_fees_usdt,
            "fees_usdt": fees_usdt,
            "fee_source": rates.source,
            "fee_verified": rates.verified,
            "net_profit_usdt": net_profit_usdt,
            "net_profit_idr": net_profit_idr,
            "net_roi_pct": roi_pct,
            "is_mock": is_mock,
            "closed_at_ms": int(time.time() * 1000),
        }
        spot_cost = (
            getattr(
                pos, "spot_cost_usdt",
                unclosed_delta * entry_price_idr / usdt_rate,
            ) * fill_ratio
        )
        futures_margin = (
            getattr(
                pos, "futures_margin_usdt",
                unclosed_delta * short_entry_usdt,
            ) * fill_ratio
        )
        return closed_trade, spot_cost, futures_margin

    async def _persist_closed_trade(
        self, pair_tag: str, pos: ActivePosition, closed_trade: dict,
        spot_cost: float, futures_margin: float, spot_pnl_usdt: float,
        usdt_rate: float,
    ) -> None:
        await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(closed_trade))
        await self.redis.ltrim(
            ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN
        )
        self.supabase.trade_closed(pair_tag, closed_trade, pos)
        if hasattr(self.client, "apply_realized_trade"):
            self.client.apply_realized_trade(
                spot_cost_usdt=spot_cost,
                spot_pnl_usdt=spot_pnl_usdt,
                spot_fees_usdt=closed_trade["spot_fees_usdt"],
                futures_margin_usdt=futures_margin,
                futures_pnl_usdt=closed_trade["futures_pnl_usdt"],
                funding_usdt=closed_trade["accumulated_funding_usdt"],
                futures_fees_usdt=closed_trade["futures_fees_usdt"],
            )
            # Refresh available balances only after applying realized PnL.
            bals = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                symbol=pos.symbol,
                usdt_idr_rate=usdt_rate,
            )
            if bals is not None:
                await self.redis.set(BALANCES_KEY, json.dumps(bals))

        pipe = self.redis.pipeline()
        pipe.hincrby(ARB_STATS_KEY, "total_closed", 1)
        if closed_trade["net_profit_usdt"] > 0:
            pipe.hincrby(ARB_STATS_KEY, "win_count", 1)
        pipe.hincrbyfloat(
            ARB_STATS_KEY, "total_pnl_usdt", closed_trade["net_profit_usdt"]
        )
        pipe.hincrbyfloat(
            ARB_STATS_KEY, "total_pnl_idr", closed_trade["net_profit_idr"]
        )
        event_msg = {"event": "POSITION_CLOSED", **closed_trade}
        pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
        await pipe.execute()
        self.record_realized_pnl(
            closed_trade["net_profit_usdt"],
            event_id=closed_trade["trade_id"],
            pair_key=pair_tag,
        )
        await self.publish_status()
        await self._finalize_exit_position(pair_tag, pos)

    async def _execute_exit_fill(
        self,
        pair_tag: str,
        order_info: dict,
        pos: ActivePosition,
        unclosed_delta: float,
    ) -> None:
        """Close the global short and persist realized exit accounting."""
        exit_price_idr = float(order_info.get("price", 0.0))
        entry_price_idr = float(pos.entry_price_idr)
        short_entry_usdt = float(pos.short_entry_usdt)
        usdt_rate = helpers.FALLBACK_USDT_IDR_RATE
        live_global_ask = float(
            self.last_pegged_global_ask.get(pair_tag, short_entry_usdt)
        )
        live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if live_price_raw:
            try:
                live_snapshot = json.loads(live_price_raw)
                usdt_rate = float(
                    live_snapshot.get(
                        "usdt_idr_rate", helpers.FALLBACK_USDT_IDR_RATE
                    )
                )
                g_ask = float(live_snapshot.get("global_best_ask_usdt", 0.0))
                if g_ask > 0:
                    live_global_ask = g_ask
            except Exception:
                pass

        logger.info(
            f"[State 4 Exit Execution] Local Limit Sell fill detected: "
            f"sold_delta={unclosed_delta} on {pair_tag}. Immediately closing "
            "Short via Market Buy (reduceOnly=True)!"
        )
        try:
            global_sym = self._resolve_global_symbol(pos.symbol)
            unwind_coid = build_client_order_id(
                pair_tag, "unwind", self._next_order_seq()
            )
            await self._register_order_intent(
                pair_tag, unwind_coid,
                exchange=pos.global_exchange, market_type="perpetual",
                symbol=global_sym, role=OrderRole.EXIT_FUTURES.value,
                side="buy", order_type="market", price=live_global_ask,
                quantity=unclosed_delta, reduce_only=True,
                global_amount_spec=pos.global_amount_spec,
                required=False,
            )
            close_order = await self.client.close_global_short_market(
                global_sym,
                unclosed_delta,
                client_order_id=unwind_coid,
                amount_spec=pos.global_amount_spec,
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
            await self._register_order(
                pair_tag,
                unwind_coid,
                close_order,
                exchange=pos.global_exchange,
                market_type="perpetual",
                symbol=global_sym,
                role=OrderRole.EXIT_FUTURES.value,
                side="buy",
                order_type="market",
                price=helpers.extract_fill_price(close_order),
                quantity=unclosed_delta,
                reduce_only=True,
                global_amount_spec=pos.global_amount_spec,
            )

            total_qty_before = pos.qty
            order_info["closed_short_qty"] += unclosed_delta
            pos.qty -= unclosed_delta
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(
                f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
            )
            short_exit_usdt = helpers.extract_fill_price(close_order)
            if short_exit_usdt <= 0:
                raise ValueError(
                    "futures close fill price unavailable after spot sell; "
                    "PnL cannot be safely finalized"
                )
            closed_trade, spot_cost, futures_margin = (
                self._build_closed_trade_record(
                    pos, unclosed_delta, exit_price_idr,
                    entry_price_idr, short_entry_usdt, short_exit_usdt,
                    usdt_rate, total_qty_before,
                )
            )
            await self._persist_closed_trade(
                pair_tag, pos, closed_trade, spot_cost, futures_margin,
                closed_trade["spot_pnl_usdt"], usdt_rate,
            )
        except Exception as ee:
            pos.state = PositionState.RECONCILIATION_REQUIRED
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
