"""exit order management methods for exit lifecycle management."""

import logging
import math
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
        last_tracked = max(already_closed, float(old_order.get("last_spot_fill", already_closed) or 0.0))
        new_fill = max(0.0, final_filled - last_tracked)
        if new_fill > 0:
            pos.unhedged_exit_base_qty = float(getattr(pos, "unhedged_exit_base_qty", 0.0) or 0.0) + new_fill
            old_order["last_spot_fill"] = final_filled
            old_order["filled"] = final_filled
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

        unhedged = float(getattr(pos, "unhedged_exit_base_qty", 0.0) or 0.0)
        if unhedged > 0:
            price_data = await self._fetch_live_price(pair_tag)
            futures_ask_price = price_data[0] if price_data else pos.short_entry_usdt
            min_futures_notional_usd = float(
                getattr(self.config, "min_futures_notional_usd", 0.0)
                or getattr(self.config, "min_partial_fill_usd", 0.0)
                or 5.0
            )
            min_threshold = max(min_futures_notional_usd, 5.50)
            if unhedged * futures_ask_price >= min_threshold or unhedged >= pos.qty:
                qty_to_close = min(unhedged, pos.qty)
                await self._execute_exit_fill(pair_tag, old_order, pos, qty_to_close)
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
            unhedged = float(getattr(pos, "unhedged_exit_base_qty", 0.0) or 0.0)
            qty_to_sell = max(0.0, pos.qty - unhedged)
            if qty_to_sell <= 1e-8:
                logger.info(
                    f"[State 3] Spot qty for {pair_tag} already sold ({unhedged} unhedged); skipping new limit sell"
                )
                return

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
                quantity=qty_to_sell, local_amount_spec=pos.local_amount_spec,
                recovery_context=recovery_context,
                required=False,
            )
            try:
                order = await self.client.place_local_limit_sell(
                    pos.symbol,
                    qty_to_sell,
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
                quantity=qty_to_sell,
                local_amount_spec=pos.local_amount_spec,
                recovery_context=recovery_context,
            )
            now_s = time.time()
            self.pending_exit_orders[pair_tag] = {
                "order_id": order_id,
                "client_order_id": exit_coid,
                "symbol": pos.symbol,
                "price": limit_sell_idr,
                "qty": qty_to_sell,
                "filled": 0.0,
                "closed_short_qty": 0.0,
                "last_spot_fill": 0.0,
                "tick_size": tick_size,
                "usdt_idr_rate": usdt_idr_rate,
                "placed_at_ts": now_s,
                "last_repeg_ts": now_s,
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
        if not raw:
            raw = await self.redis.get(f"arb:market_snapshot:{pair_tag}")
        local_bid = pos.entry_price_idr
        local_ask = 0.0
        snapshot = None
        if raw:
            try:
                snapshot = json.loads(raw)
                raw_bid = float(
                    snapshot.get("local_best_bid")
                    or snapshot.get("spot_best_bid")
                    or 0.0
                )
                raw_ask = float(
                    snapshot.get("local_best_ask")
                    or snapshot.get("spot_best_ask")
                    or 0.0
                )
                if not helpers.snapshot_is_stale(snapshot):
                    rate = float(
                        snapshot.get("usdt_idr_rate")
                        or snapshot.get("usd_idr_rate")
                        or 0.0
                    )
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
            rejection_check = getattr(self, "_is_definitive_order_rejection", None)
            if callable(rejection_check) and rejection_check(exc):
                await self._mark_order_submission_rejected(
                    pos.local_exchange, taker_coid
                )
                logger.warning(
                    "[Taker Sniper Exit] venue definitively rejected %s: %s",
                    taker_coid, exc,
                )
                return False

            # A timeout/reset can happen after the spot SELL reached the venue.
            # Resolve only by the exact client ID; never fall through to a new
            # maker SELL with a different ID while this market SELL is unknown.
            lookup = getattr(
                self.client, "fetch_local_order_by_client_id", None
            )
            recovered = None
            if callable(lookup):
                try:
                    recovered = await lookup(
                        taker_coid, pos.symbol, pos.local_amount_spec
                    )
                except Exception:
                    logger.exception(
                        "[Taker Sniper Exit] exact client-id lookup failed for %s",
                        taker_coid,
                    )
            if not isinstance(recovered, dict) or not recovered.get("id"):
                await self._mark_order_submission_ambiguous(
                    pair_tag, pos.local_exchange, taker_coid,
                    f"taker sniper spot SELL outcome ambiguous: "
                    f"{type(exc).__name__}: {exc}; exact client-id lookup "
                    "did not return a verifiable order",
                )
                return True
            logger.warning(
                "[Taker Sniper Exit] recovered market SELL %s by exact client id",
                taker_coid,
            )
            order = recovered

        if not isinstance(order, dict) or not order.get("id"):
            await self._mark_order_submission_ambiguous(
                pair_tag, pos.local_exchange, taker_coid,
                "taker sniper venue acknowledgement has no verifiable exchange order id",
            )
            return True

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

        now_s = time.time()
        raw_filled = order.get("filled")
        if raw_filled is None:
            await self._mark_order_submission_ambiguous(
                pair_tag, pos.local_exchange, taker_coid,
                "taker sniper market SELL acknowledgement has no cumulative filled quantity",
            )
            return True
        try:
            filled_qty = float(raw_filled or 0.0)
        except (TypeError, ValueError):
            filled_qty = float("nan")
        fill_tolerance = max(1e-12, pos.qty * 1e-8)
        if (
            not math.isfinite(filled_qty)
            or filled_qty < 0
            or filled_qty > pos.qty + fill_tolerance
        ):
            await self._mark_order_submission_ambiguous(
                pair_tag, pos.local_exchange, taker_coid,
                f"taker sniper market SELL returned invalid cumulative fill {raw_filled!r} "
                f"for requested quantity {pos.qty}",
            )
            return True
        filled_qty = min(filled_qty, pos.qty)

        order_info = {
            "order_id": order_id,
            "client_order_id": taker_coid,
            "symbol": pos.symbol,
            "price": fill_price,
            "qty": pos.qty,
            "filled": filled_qty,
            "closed_short_qty": 0.0,
            "last_spot_fill": filled_qty,
            "tick_size": getattr(pos, "local_price_tick", 1.0),
            "usdt_idr_rate": usdt_idr_rate,
            "placed_at_ts": now_s,
            "last_repeg_ts": now_s,
        }
        self.pending_exit_orders[pair_tag] = order_info
        self.last_pegged_global_ask[pair_tag] = global_ask_usdt

        if filled_qty <= fill_tolerance:
            status = str(order.get("status") or "").strip().lower()
            if status in ("rejected", "canceled", "cancelled", "expired"):
                await self._mark_order_submission_rejected(
                    pos.local_exchange, taker_coid
                )
                self.pending_exit_orders.pop(pair_tag, None)
                return False
            # Acknowledged but not yet filled: keep tracking this exact order.
            # Do not close any futures quantity and do not place a fallback SELL.
            self.states[pair_tag] = PositionState.EXIT_PENDING
            await self.redis.set(
                f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
            )
            logger.info(
                "[Taker Sniper Exit] %s market SELL %s is %s with zero fill; "
                "tracking the same order without closing the futures hedge",
                pair_tag, taker_coid, status or "unknown",
            )
            return True

        logger.info(
            f"[Taker Sniper Exit] {pair_tag}: executed spot market sell for {filled_qty} "
            f"at {fill_price} IDR (reason: {reason}). Unwinding hedge..."
        )
        pos.unhedged_exit_base_qty = float(getattr(pos, "unhedged_exit_base_qty", 0.0) or 0.0) + filled_qty
        await self._execute_exit_fill(pair_tag, order_info, pos, filled_qty)
        return True

    async def _check_dynamic_exit_pair_bound(self, pair_tag: str) -> None:
        lock = self._pair_lock(pair_tag)
        if lock.locked():
            # Skip siklus ini jika pair sedang dieksekusi oleh entry/recovery worker
            return
        async with lock:
            await self._check_dynamic_exit_pair_inner(pair_tag)

    async def _check_dynamic_exit_pair_inner(self, pair_tag: str) -> None:
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

        target_sell_idr, _ = self._calculate_limit_sell_idr(
            pair_tag, pos, global_ask_usdt, usdt_idr_rate, local_best_ask_idr=0.0
        )

        tick_size = getattr(pos, "local_price_tick", 1.0)
        if tick_size <= 0:
            tick_size = 1.0

        rates = rates_for_position(pos)
        fx_spot = 1.0 / usdt_idr_rate if usdt_idr_rate > 0 else 0.0
        spot_entry_fee_quote = pos.qty * pos.entry_price_idr * (rates.spot_entry_pct / 100.0)
        futures_entry_fee_rate = (rates.perp_entry_pct / 100.0) if rates.perp_entry_pct > 0 else 0.0005
        futures_taker_fee_rate = (rates.perp_exit_pct + rates.surcharge_pct) / 100.0 if rates.perp_exit_pct > 0 else 0.0005
        spot_taker_fee_rate = (rates.spot_exit_pct + rates.surcharge_pct) / 100.0

        cached_profile = None
        if hasattr(self, "_load_cached_fee_profile"):
            try:
                cached_profile = await self._load_cached_fee_profile(pos.local_exchange, "spot", pos.symbol)
            except Exception:
                pass
        if cached_profile and "maker" in cached_profile:
            spot_maker_fee_rate = float(cached_profile["maker"]) / 100.0
        elif str(pos.local_exchange).lower() == "indodax":
            spot_maker_fee_rate = 0.0
        else:
            spot_maker_fee_rate = (
                float(getattr(self.config, "spot_maker_fee_pct", 0.0)) / 100.0
                if getattr(self.config, "spot_maker_fee_pct", 0.0) > 0
                else spot_taker_fee_rate
            )

        # Smart Maker sell price candidate at top-of-book
        if _local_best_ask > 0:
            maker_candidate_price = max(local_best_bid + tick_size, _local_best_ask - tick_size)
        else:
            maker_candidate_price = local_best_bid + tick_size
        maker_candidate_price = helpers.ceil_to_tick(maker_candidate_price, tick_size)

        # ExitDecisionEngine & Taker Sniper vs Smart Maker evaluation
        raw_bid = float((live_snapshot or {}).get("local_best_bid", 0.0) or (live_snapshot or {}).get("spot_best_bid", 0.0) or 0.0)
        if raw_bid > 0 and getattr(self, "exit_decision_engine", None) and getattr(self, "pair_pnl_engine", None):
            # Scenario 1: Taker Sniper Exit (Spot Taker Fee + Futures Taker Fee at best bid)
            taker_pnl_snapshot = self.pair_pnl_engine.compute_pair_pnl(
                session_id=str(getattr(pos, "session_id", pair_tag) or pair_tag),
                pair_key=pair_tag,
                spot_qty=pos.qty,
                spot_entry_cost_quote=pos.qty * pos.entry_price_idr,
                spot_executable_bid=local_best_bid,
                spot_exit_fee_rate=spot_taker_fee_rate,
                spot_entry_fee_quote=spot_entry_fee_quote,
                futures_qty=pos.qty,
                futures_entry_price=pos.short_entry_usdt,
                futures_executable_ask=global_ask_usdt,
                futures_close_fee_rate=futures_taker_fee_rate,
                futures_entry_fee_rate=futures_entry_fee_rate,
                funding_accrued=pos.accumulated_funding_usdt,
                fx_spot_to_report=fx_spot,
                fx_futures_to_report=1.0,
                reporting_currency="USDT",
                position_state=pos.to_dict() if hasattr(pos, "to_dict") else None,
            )
            taker_exit_net_roi_pct = taker_pnl_snapshot.pair_net_roi_pct

            # Scenario 2: Smart Maker Exit (Spot Maker Fee at top-of-book ask undercut + Futures Taker Fee)
            maker_pnl_snapshot = self.pair_pnl_engine.compute_pair_pnl(
                session_id=str(getattr(pos, "session_id", pair_tag) or pair_tag),
                pair_key=pair_tag,
                spot_qty=pos.qty,
                spot_entry_cost_quote=pos.qty * pos.entry_price_idr,
                spot_executable_bid=maker_candidate_price,
                spot_exit_fee_rate=spot_maker_fee_rate,
                spot_entry_fee_quote=spot_entry_fee_quote,
                futures_qty=pos.qty,
                futures_entry_price=pos.short_entry_usdt,
                futures_executable_ask=global_ask_usdt,
                futures_close_fee_rate=futures_taker_fee_rate,
                futures_entry_fee_rate=futures_entry_fee_rate,
                funding_accrued=pos.accumulated_funding_usdt,
                fx_spot_to_report=fx_spot,
                fx_futures_to_report=1.0,
                reporting_currency="USDT",
                position_state=pos.to_dict() if hasattr(pos, "to_dict") else None,
            )
            maker_exit_net_roi_pct = maker_pnl_snapshot.pair_net_roi_pct

            required_net_roi_pct = float(
                getattr(self.config, "required_net_roi_pct", 0.0)
                or getattr(self.config, "min_net_roi_pct", 0.0)
                or getattr(pos, "required_net_roi_pct", 0.0)
                or pos.base_target_exit_spread_pct
            )

            position_age_seconds = max(
                0.0, (now_ms - getattr(pos, "opened_at_ms", 0)) / 1000.0
            ) if getattr(pos, "opened_at_ms", 0) > 0 else 0.0
            capital_usdt = pos.qty * pos.short_entry_usdt
            dynamic_target_usdt = max(
                capital_usdt * (required_net_roi_pct / 100.0),
                getattr(self.exit_decision_engine, "min_profit_target_usdt", 0.50),
            )
            decision = self.exit_decision_engine.evaluate_exit(
                taker_pnl_snapshot,
                position_age_seconds=position_age_seconds,
                dynamic_target_usdt=dynamic_target_usdt,
            )

            time_stop_triggered = (
                (decision.action == ExitAction.CLOSE_BOTH and "TIME_STOP" in decision.reason)
                or (position_age_seconds >= getattr(self.exit_decision_engine, "max_position_age", 3600.0)
                    and taker_pnl_snapshot.pair_net_liquidation_pnl > 0)
            )

            time_to_funding_ms = (pos.next_funding_time_ms - now_ms) if pos.next_funding_time_ms > 0 else None
            approaching_negative_funding = (
                time_to_funding_ms is not None
                and 0 < time_to_funding_ms <= 300_000
                and getattr(pos, "current_funding_rate", 0.0) < 0
                and taker_pnl_snapshot.pair_net_liquidation_pnl >= 0
            )

            # Volume check on spot_best_bid: minimal 70% kuantitas kita
            spot_bid_qty = None
            if live_snapshot:
                spot_bid_qty = (
                    live_snapshot.get("spot_bid_qty")
                    if "spot_bid_qty" in live_snapshot
                    else (
                        live_snapshot.get("local_bid_qty")
                        if "local_bid_qty" in live_snapshot
                        else live_snapshot.get("bid_qty")
                    )
                )
            has_sufficient_bid_volume = (
                spot_bid_qty is None or float(spot_bid_qty) >= 0.70 * pos.qty
            )

            # Kondisi 1: TAKER_SNIPER_EXIT
            taker_profitable = (taker_exit_net_roi_pct >= required_net_roi_pct) or (local_best_bid >= target_sell_idr)
            if (taker_profitable and has_sufficient_bid_volume) or time_stop_triggered or approaching_negative_funding:
                if taker_profitable:
                    sniper_reason = (
                        f"TAKER_SNIPER_EXIT (taker_roi={taker_exit_net_roi_pct:.3f}% >= req={required_net_roi_pct:.3f}%, "
                        f"bid={local_best_bid:.2f} >= target={target_sell_idr:.2f})"
                    )
                elif time_stop_triggered:
                    sniper_reason = decision.reason
                else:
                    sniper_reason = f"NEGATIVE_FUNDING_APPROACHING (rate={pos.current_funding_rate:.6f}, in {time_to_funding_ms/1000:.0f}s)"

                logger.info(
                    f"[State 3 Taker Sniper Exit] Triggering immediate Taker Sell for {pair_tag}: "
                    f"local_bid={local_best_bid}, target_sell_idr={target_sell_idr:.2f}, "
                    f"pnl_usdt={taker_pnl_snapshot.pair_net_liquidation_pnl:.4f}, reason={sniper_reason}"
                )
                if await self._execute_taker_sniper_exit(
                    pair_tag, pos, local_best_bid, global_ask_usdt, usdt_idr_rate, reason=sniper_reason
                ):
                    return

            # Kondisi 2: SMART_MAKER_EXIT dengan Auto Re-Peg
            if maker_exit_net_roi_pct >= required_net_roi_pct:
                limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
                    pair_tag, pos, global_ask_usdt, usdt_idr_rate,
                    local_best_ask_idr=_local_best_ask,
                    local_best_bid_idr=local_best_bid,
                )

                if not self._should_repeg_exit(
                    pair_tag, global_ask_usdt, funding_passed,
                    local_best_ask_idr=_local_best_ask,
                    maker_roi_viable=(maker_exit_net_roi_pct >= required_net_roi_pct),
                ):
                    return

                if not await self._cancel_existing_exit_order(pair_tag, pos.symbol):
                    return

                pos = self.active_positions.get(pair_tag)
                if not pos or pos.qty <= 0:
                    return

                await self._place_dynamic_limit_sell(
                    pair_tag, pos, limit_sell_idr, tick_size, global_ask_usdt,
                    usdt_idr_rate,
                )
                return

        if not self._should_repeg_exit(
            pair_tag, global_ask_usdt, funding_passed,
            local_best_ask_idr=_local_best_ask,
            maker_roi_viable=True,
        ):
            return

        # Smart Maker Ask: undercut local_best_ask by 1 tick if above target_sell_idr
        limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
            pair_tag, pos, global_ask_usdt, usdt_idr_rate,
            local_best_ask_idr=_local_best_ask,
            local_best_bid_idr=local_best_bid,
        )

        if not await self._cancel_existing_exit_order(pair_tag, pos.symbol):
            return

        # Cancellation may race with fill and reduce or close position.
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
                return await self._process_pending_exit_inner(pair_tag)
        return await self._process_pending_exit_inner(pair_tag)

    async def _process_pending_exit_inner(self, pair_tag: str) -> None:
        if (
            self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED
            and not getattr(self, "_force_reconcile_active", False)
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
        order_info["filled"] = current_filled

        last_spot_fill = float(order_info.get("last_spot_fill", closed_qty) or 0.0)
        new_spot_fill = max(0.0, current_filled - last_spot_fill)
        if new_spot_fill > 0:
            pos.unhedged_exit_base_qty = float(getattr(pos, "unhedged_exit_base_qty", 0.0) or 0.0) + new_spot_fill
            order_info["last_spot_fill"] = current_filled
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

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

        unhedged_qty = float(getattr(pos, "unhedged_exit_base_qty", 0.0) or 0.0)
        if unhedged_qty > 0:
            status = str(order.get("status") or "").strip().lower()
            is_terminal = status in (
                "closed", "canceled", "cancelled", "rejected", "expired", "filled"
            )
            order_qty = float(order_info.get("qty", 0.0) or pos.qty)
            is_100_pct_filled = (
                current_filled >= order_qty - 1e-8
                or pos.qty <= unhedged_qty + 1e-8
                or is_terminal
            )

            futures_ask_price = 0.0
            if live_price_raw:
                try:
                    snap = json.loads(live_price_raw)
                    futures_ask_price = float(
                        snap.get("global_best_ask_usdt")
                        or snap.get("futures_best_ask")
                        or 0.0
                    )
                except Exception:
                    pass
            if futures_ask_price <= 0:
                price_idr = float(order_info.get("price", 0.0) or 0.0)
                rate = float(order_info.get("usdt_idr_rate", 0.0) or 0.0)
                if rate > 0 and price_idr > 0:
                    futures_ask_price = price_idr / rate
                else:
                    futures_ask_price = float(pos.short_entry_usdt or 0.0)

            min_futures_notional_usd = float(
                getattr(self.config, "min_futures_notional_usd", 0.0)
                or getattr(self.config, "min_partial_fill_usd", 0.0)
                or 5.0
            )
            min_threshold = max(min_futures_notional_usd, 5.50)
            notional_usd = unhedged_qty * futures_ask_price

            # ponytail: buffer check against min notional avoids futures exchange rejection; 100% fill drains residual
            if notional_usd >= min_threshold or is_100_pct_filled:
                qty_to_close = min(unhedged_qty, pos.qty)
                await self._execute_exit_fill(pair_tag, order_info, pos, qty_to_close)
            else:
                logger.info(
                    f"[State 4 Partial Fill Exit] {pair_tag} unhedged exit qty {unhedged_qty} "
                    f"(${notional_usd:.2f}) below threshold (${min_threshold:.2f}); accumulating"
                )
