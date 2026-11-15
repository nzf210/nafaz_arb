"""exit order management methods for exit lifecycle management."""

import logging
import json
import time
from types import SimpleNamespace

from executor.models import ActivePosition, OrderRole, PositionState
from executor import helpers
from executor.helpers import build_client_order_id
from executor.private_stream import fetch_order_preferring_stream
from executor.fee_engine import rates_for_position, round_trip_fees_usdt
from executor.exit_decision_engine import ExitAction

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
        usdt_idr_rate: float,
    ) -> None:
        try:
            pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
            if pair_info is None:
                raw_symbol = str(pos.symbol or "").split(":")[-1]
                if "/" in raw_symbol:
                    quote_asset = raw_symbol.rsplit("/", 1)[-1]
                elif "_" in raw_symbol:
                    quote_asset = raw_symbol.rsplit("_", 1)[-1]
                else:
                    raise ValueError(
                        f"cannot determine local quote asset for {pos.local_exchange}:{pos.symbol}"
                    )
                pair_info = SimpleNamespace(quote_asset=quote_asset)
            exchange_limit_sell_price = self._local_order_limit_price(
                pair_info,
                SimpleNamespace(usdt_idr_rate=usdt_idr_rate),
                limit_sell_idr,
            )
            recovery_context = {
                "strategy_price_idr": limit_sell_idr,
                "exchange_order_price": exchange_limit_sell_price,
                "exchange_quote_asset": str(pair_info.quote_asset or "").upper(),
            }
            exit_coid = build_client_order_id(
                pair_tag, "exit", self._next_order_seq()
            )
            await self._register_order_intent(
                pair_tag, exit_coid,
                exchange=pos.local_exchange, market_type="spot",
                symbol=pos.symbol, role=OrderRole.EXIT_SPOT.value,
                side="sell", order_type="limit", price=limit_sell_idr,
                quantity=pos.qty, local_amount_spec=pos.local_amount_spec,
                recovery_context=recovery_context,
                required=False,
            )
            try:
                order = await self.client.place_local_limit_sell(
                    pos.symbol,
                    pos.qty,
                    exchange_limit_sell_price,
                    client_order_id=exit_coid,
                    amount_spec=pos.local_amount_spec,
                )
            except Exception as submit_err:
                rejection_check = getattr(self, "_is_definitive_order_rejection", None)
                if callable(rejection_check) and rejection_check(submit_err):
                    await self._mark_order_submission_rejected(pos.local_exchange, exit_coid)
                    raise
                # An exit SELL is risk-reducing, so it is not gated by Global
                # STOP. But an ambiguous acknowledgement must not cause the
                # next pricing cycle to submit a second SELL with a new id.
                lookup = getattr(
                    self.client, "fetch_local_order_by_client_id", None
                )
                recovered = None
                if callable(lookup):
                    try:
                        recovered = await lookup(
                            exit_coid, pos.symbol, pos.local_amount_spec
                        )
                    except Exception:
                        logger.exception(
                            "[Exit] exact client-id lookup failed after ambiguous submit %s",
                            exit_coid,
                        )
                if not isinstance(recovered, dict) or not recovered.get("id"):
                    await self._mark_order_submission_ambiguous(
                        pair_tag, pos.local_exchange, exit_coid,
                        f"exit limit sell submit outcome ambiguous: "
                        f"{type(submit_err).__name__}: {submit_err}; "
                        "exact client-id lookup did not return a verifiable order",
                    )
                    return
                logger.warning(
                    "[Exit] recovered limit sell %s by exact client id after lost acknowledgement",
                    exit_coid,
                )
                order = recovered
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
                recovery_context=recovery_context,
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
                "usdt_idr_rate": usdt_idr_rate,
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
                raw_bid = float(snapshot.get("local_best_bid", 0.0) or 0.0)
                raw_ask = float(snapshot.get("local_best_ask", 0.0) or 0.0)
                if not helpers.snapshot_is_stale(snapshot):
                    rate = float(snapshot.get("usdt_idr_rate", 0.0) or 0.0)
                    raw_sym = str(pos.symbol or "").upper()
                    is_usdt_quote = raw_sym.endswith(("_USDT", "/USDT", "_USDC", "/USDC"))
                    if is_usdt_quote and rate > 0:
                        local_bid = raw_bid * rate if raw_bid > 0 else pos.entry_price_idr
                        local_ask = raw_ask * rate
                    else:
                        local_bid = raw_bid if raw_bid > 0 else pos.entry_price_idr
                        local_ask = raw_ask
                if hasattr(self.client, "update_market_price"):
                    self.client.update_market_price(
                        pos.symbol,
                        raw_bid if raw_bid > 0 else pos.entry_price_idr,
                        raw_ask,
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

    async def _execute_taker_sniper_exit(
        self,
        pair_tag: str,
        pos: ActivePosition,
        local_best_bid: float,
        global_ask_usdt: float,
        usdt_idr_rate: float,
        reason: str = "TAKER_SNIPER_EXIT",
    ) -> bool:
        """Execute immediate taker market sell and unwind hedge."""
        # ponytail: single-shot market sell; smart chunking deferred to high liquidity
        if not await self._cancel_existing_exit_order(pair_tag, pos.symbol):
            logger.warning(
                f"[Taker Sniper Exit] could not cancel existing exit order for {pair_tag}; aborting"
            )
            return False

        pos = self.active_positions.get(pair_tag)
        if not pos or pos.qty <= 0:
            return True

        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        if pair_info is None:
            raw_symbol = str(pos.symbol or "").split(":")[-1]
            quote_asset = (
                raw_symbol.rsplit("/", 1)[-1]
                if "/" in raw_symbol
                else (raw_symbol.rsplit("_", 1)[-1] if "_" in raw_symbol else "IDR")
            )
            pair_info = SimpleNamespace(quote_asset=quote_asset)

        taker_coid = build_client_order_id(pair_tag, "sniper-exit", self._next_order_seq())
        await self._register_order_intent(
            pair_tag,
            taker_coid,
            exchange=pos.local_exchange,
            market_type="spot",
            symbol=pos.symbol,
            role=OrderRole.EXIT_SPOT.value,
            side="sell",
            order_type="market",
            price=local_best_bid,
            quantity=pos.qty,
            local_amount_spec=pos.local_amount_spec,
            required=False,
        )

        try:
            order = await self.client.place_local_market_sell(
                pos.symbol,
                pos.qty,
                client_order_id=taker_coid,
                amount_spec=pos.local_amount_spec,
            )
        except Exception as exc:
            logger.exception(
                f"[Taker Sniper Exit] place_local_market_sell failed for {pair_tag}: {exc}"
            )
            return False

        order_id = str(order.get("id", ""))
        self.supabase.order(
            pair_tag,
            "spot_exit",
            pos.local_exchange,
            "spot",
            pos.symbol,
            order,
            client_order_id=taker_coid,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        await self._register_order(
            pair_tag,
            taker_coid,
            order,
            exchange=pos.local_exchange,
            market_type="spot",
            symbol=pos.symbol,
            role=OrderRole.EXIT_SPOT.value,
            side="sell",
            order_type="market",
            price=local_best_bid,
            quantity=pos.qty,
            local_amount_spec=pos.local_amount_spec,
        )

        filled_qty = float(order.get("filled", 0.0) or order.get("amount", 0.0) or pos.qty)
        fill_price = helpers.extract_fill_price(order)
        if not fill_price or fill_price <= 0 or (getattr(self.config, "mock_mode", False) and fill_price < local_best_bid * 0.5):
            fill_price = local_best_bid

        order_info = {
            "order_id": order_id,
            "client_order_id": taker_coid,
            "symbol": pos.symbol,
            "price": fill_price,
            "qty": pos.qty,
            "filled": filled_qty,
            "closed_short_qty": 0.0,
            "tick_size": getattr(pos, "local_price_tick", 1.0),
            "usdt_idr_rate": usdt_idr_rate,
        }
        self.pending_exit_orders[pair_tag] = order_info
        self.last_pegged_global_ask[pair_tag] = global_ask_usdt
        logger.info(
            f"[Taker Sniper Exit] {pair_tag}: executed spot market sell for {filled_qty} "
            f"at {fill_price} IDR (reason: {reason}). Unwinding hedge..."
        )
        await self._execute_exit_fill(pair_tag, order_info, pos, filled_qty)
        return True

    async def _check_dynamic_exit_pair_bound(self, pair_tag: str) -> None:
        async with self._pair_lock(pair_tag):
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
            await self._refresh_predicted_funding_rate(pair_tag, pos, now_ms)
            funding_passed = await self._handle_funding_rollover(
                pair_tag, pos, now_ms
            )
            await self._persist_exit_unrealized_pnl(
                pair_tag, pos, local_best_bid, global_ask_usdt,
                usdt_idr_rate, now_ms, live_snapshot,
            )

            target_sell_idr, tick_size = self._calculate_limit_sell_idr(
                pair_tag, pos, global_ask_usdt, usdt_idr_rate, local_best_ask_idr=0.0
            )

            # ExitDecisionEngine & Taker Sniper Exit evaluation
            raw_bid = float((live_snapshot or {}).get("local_best_bid", 0.0) or 0.0)
            if raw_bid > 0 and getattr(self, "exit_decision_engine", None) and getattr(self, "pair_pnl_engine", None):
                rates = rates_for_position(pos)
                fx_spot = 1.0 / usdt_idr_rate if usdt_idr_rate > 0 else 0.0
                spot_exit_fee_rate = rates.spot_exit_pct / 100.0
                spot_entry_fee_quote = pos.qty * pos.entry_price_idr * (rates.spot_entry_pct / 100.0)
                futures_close_fee_rate = rates.perp_exit_pct / 100.0
                pnl_snapshot = self.pair_pnl_engine.compute_pair_pnl(
                    session_id=str(getattr(pos, "session_id", pair_tag) or pair_tag),
                    pair_key=pair_tag,
                    spot_qty=pos.qty,
                    spot_entry_cost_quote=pos.qty * pos.entry_price_idr,
                    spot_executable_bid=local_best_bid,
                    spot_exit_fee_rate=spot_exit_fee_rate,
                    spot_entry_fee_quote=spot_entry_fee_quote,
                    futures_qty=pos.qty,
                    futures_entry_price=pos.short_entry_usdt,
                    futures_executable_ask=global_ask_usdt,
                    futures_close_fee_rate=futures_close_fee_rate,
                    funding_accrued=pos.accumulated_funding_usdt,
                    fx_spot_to_report=fx_spot,
                    fx_futures_to_report=1.0,
                    reporting_currency="USDT",
                )

                position_age_seconds = max(
                    0.0, (now_ms - getattr(pos, "opened_at_ms", 0)) / 1000.0
                ) if getattr(pos, "opened_at_ms", 0) > 0 else 0.0

                min_roi_pct = max(0.05, pos.base_target_exit_spread_pct * 0.25)
                capital_usdt = pos.qty * pos.short_entry_usdt
                dynamic_target_usdt = max(
                    capital_usdt * (min_roi_pct / 100.0),
                    getattr(self.exit_decision_engine, "min_profit_target_usdt", 0.50),
                )

                decision = self.exit_decision_engine.evaluate_exit(
                    pnl_snapshot,
                    position_age_seconds=position_age_seconds,
                    dynamic_target_usdt=dynamic_target_usdt,
                )

                target_profit_reached = local_best_bid >= target_sell_idr
                time_stop_triggered = (
                    (decision.action == ExitAction.CLOSE_BOTH and "TIME_STOP" in decision.reason)
                    or (position_age_seconds >= getattr(self.exit_decision_engine, "max_position_age", 3600.0)
                        and pnl_snapshot.pair_net_liquidation_pnl > 0)
                )

                time_to_funding_ms = (pos.next_funding_time_ms - now_ms) if pos.next_funding_time_ms > 0 else None
                approaching_negative_funding = (
                    time_to_funding_ms is not None
                    and 0 < time_to_funding_ms <= 300_000
                    and getattr(pos, "current_funding_rate", 0.0) < 0
                    and pnl_snapshot.pair_net_liquidation_pnl >= 0
                )

                if target_profit_reached or time_stop_triggered or approaching_negative_funding:
                    if target_profit_reached:
                        sniper_reason = f"TARGET_PROFIT_BID_REACHED (bid={local_best_bid:.2f} >= target={target_sell_idr:.2f})"
                    elif time_stop_triggered:
                        sniper_reason = decision.reason
                    else:
                        sniper_reason = f"NEGATIVE_FUNDING_APPROACHING (rate={pos.current_funding_rate:.6f}, in {time_to_funding_ms/1000:.0f}s)"

                    logger.info(
                        f"[State 3 Taker Sniper Exit] Triggering immediate Taker Sell for {pair_tag}: "
                        f"local_bid={local_best_bid}, target_sell_idr={target_sell_idr:.2f}, "
                        f"pnl_usdt={pnl_snapshot.pair_net_liquidation_pnl:.4f}, reason={sniper_reason}"
                    )
                    if await self._execute_taker_sniper_exit(
                        pair_tag, pos, local_best_bid, global_ask_usdt, usdt_idr_rate, reason=sniper_reason
                    ):
                        return

            if not self._should_repeg_exit(
                pair_tag, global_ask_usdt, funding_passed
            ):
                return
            # Smart Maker Ask: undercut local_best_ask by 1 tick if above target_sell_idr
            limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
                pair_tag, pos, global_ask_usdt, usdt_idr_rate, local_best_ask_idr=_local_best_ask
            )
            if not await self._cancel_existing_exit_order(pair_tag, pos.symbol):
                return
            # Cancellation may race a fill and reduce or close the position.
            pos = self.active_positions.get(pair_tag)
            if not pos or pos.qty <= 0:
                return
            await self._place_dynamic_limit_sell(
                pair_tag, pos, limit_sell_idr, tick_size, global_ask_usdt,
                usdt_idr_rate,
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
        if not force_reconcile:
            async with self._pair_lock(pair_tag):
                return await self._process_pending_exit_bound(
                    pair_tag, force_reconcile=True
                )

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

        current_filled = float(order.get("filled", 0.0) or 0.0)
        closed_qty = float(order_info.get("closed_short_qty", 0.0) or 0.0)
        unclosed_delta = current_filled - closed_qty
        order_info["filled"] = current_filled

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
            status = str(order.get("status") or "").strip().lower()
            is_terminal = status in (
                "closed", "canceled", "cancelled", "rejected", "expired", "filled"
            )
            rate = float(order_info.get("usdt_idr_rate", 0.0) or 0.0)
            if rate <= 0 and live_price_raw:
                try:
                    rate = float(json.loads(live_price_raw).get("usdt_idr_rate", 0.0) or 0.0)
                except Exception:
                    rate = 0.0
            if rate <= 0:
                rate = float(getattr(helpers, "FALLBACK_USDT_IDR_RATE", 16000.0))

            price_idr = float(order_info.get("price", 0.0) or 0.0)
            delta_val_usd = (
                (unclosed_delta * price_idr) / rate
                if (price_idr > 0 and rate > 0)
                else (unclosed_delta * float(pos.short_entry_usdt))
            )
            min_fill_usd = float(getattr(self.config, "min_partial_fill_usd", 1.0) or 1.0)

            # ponytail: accumulator checks value against threshold; adaptive partial sizing deferred
            if delta_val_usd >= min_fill_usd or is_terminal or unclosed_delta >= pos.qty:
                await self._execute_exit_fill(pair_tag, order_info, pos, unclosed_delta)
            else:
                logger.info(
                    f"[State 4 Partial Fill Exit] {pair_tag} unclosed exit fill {unclosed_delta} "
                    f"(${delta_val_usd:.2f}) below threshold (${min_fill_usd:.2f}); accumulating"
                )
