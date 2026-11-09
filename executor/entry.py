"""State 2 entry/monitoring flow for ArbitrageEngine.

Entry order pegging continuation, adverse selection, partial fills, the
global-futures short hedge, and the monitoring loop. Extracted from
executor/engine.py as a mixin.
"""

import asyncio
import json
import logging
import math
import time
from typing import Any, Dict, Optional

from executor.models import ActivePosition, PairInfo, PositionState
from executor import helpers
from executor.quantity import AmountSpec
from executor.helpers import (
    BALANCES_KEY,
    HedgeSlippageExceeded,
    build_client_order_id,
)
from executor.private_stream import fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")


class EntryMixin:

        async def _monitoring_loop(self):
            """Periodic loop monitoring pending entry orders and active positions."""
            while self._running:
                try:
                    await self._check_entry_orders()
                    await self._check_dynamic_exits()
                    await self._check_exit_orders()
                    await self._maybe_publish_status()
                except Exception:
                    logger.exception("error in monitoring loop")
                await asyncio.sleep(0.05)  # 50ms fast reactive polling

        async def _maybe_publish_status(self) -> None:
            """Throttled status publication (at most once per second)."""
            now = time.time()
            if now - self._last_status_publish < 1.0:
                return
            self._last_status_publish = now
            await self.publish_status()

        async def _check_entry_orders(self):
            """
            Monitors State 1 (Entry Pegging) and State 2 (Instant Short Hedge on Partial Fill):
            - If Global Bid drops below threshold -> cancel order immediately.
            - Checks local order filled qty:
              If newly filled >= $10 -> instantly execute Market Short on Global Futures!
            """
            pair_tags = list(self.pending_entry_orders.keys())
            for pair_tag in pair_tags:
                order_info = self.pending_entry_orders.get(pair_tag)
                if not order_info:
                    continue
                await self._process_pending_entry(pair_tag, order_info)

        async def _process_pending_entry(self, pair_tag: str, order_info: dict) -> None:
            """Route pending entry lifecycle calls through the pair-bound clients."""
            await self._run_pair_bound(
                pair_tag, self._process_pending_entry_bound, pair_tag, order_info
            )

        async def _process_pending_entry_bound(self, pair_tag: str, order_info: dict) -> None:
            """Process adverse selection check, fill tracking, and position creation for an entry order.

            Runs under the pair lock so the monitoring loop cannot mutate pending
            order/position state concurrently with the signal entry path."""
            async with self._pair_lock(pair_tag):
                # The entry path may have resolved (or replaced) this order while we
                # waited for the lock; drop the stale snapshot if so.
                if self.pending_entry_orders.get(pair_tag) is not order_info:
                    return
                if await self._check_adverse_selection(pair_tag, order_info):
                    return

                order_id = order_info["order_id"]
                local_symbol = order_info["symbol"]

                # Sync latest live prices to client for realistic market touch evaluation
                await self._sync_live_price_for_client(pair_tag, local_symbol)

                try:
                    order = await fetch_order_preferring_stream(
                        self.private_stream,
                        lambda: self.client.fetch_local_order(order_id, local_symbol),
                        order_id,
                    )
                except Exception:
                    logger.exception(f"failed to fetch local order {order_id}")
                    return

                if await self._expire_stale_entry_order(pair_tag, order_info, order):
                    return

                self.supabase.order(
                    pair_tag,
                    "spot_entry",
                    order_info.get("local_exchange", ""),
                    "spot",
                    local_symbol,
                    order,
                    client_order_id=order_info.get("client_order_id"),
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )
                await self._handle_entry_fills(pair_tag, order_info, order)
                await self._finalize_entry_order(pair_tag, order.get("status", ""))

        async def _sync_live_price_for_client(
            self, pair_tag: str, local_symbol: str
        ) -> None:
            """Push the cached live snapshot into the client for market touch evaluation."""
            live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
            if not live_price_raw or not hasattr(self.client, "update_market_price"):
                return
            try:
                live_snapshot = json.loads(live_price_raw)
                self.client.update_market_price(
                    symbol=local_symbol,
                    best_bid=float(live_snapshot.get("local_best_bid", 0.0)),
                    best_ask=float(live_snapshot.get("local_best_ask", 0.0)),
                )
            except Exception:
                pass

        async def _cancel_entry_order_and_reconcile(
            self, pair_tag: str, order_info: dict, reason: str
        ) -> bool:
            """
            Cancel an entry and verify the authoritative venue state before
            releasing funds or deleting its recovery record.
            """
            order_id = str(order_info["order_id"])
            local_symbol = str(order_info["symbol"])
            try:
                await self.client.cancel_local_order(order_id, local_symbol)
            except Exception:
                # A timeout is ambiguous: the venue may have accepted the cancel.
                logger.exception(
                    f"[{reason}] cancel request failed for {order_id}; "
                    "reconciling the order directly with the exchange"
                )

            try:
                order = await self.client.fetch_local_order(order_id, local_symbol)
            except Exception:
                logger.exception(
                    f"[{reason}] cannot verify order {order_id}; pending state and "
                    "reserved funds are retained"
                )
                return False

            if not isinstance(order, dict):
                logger.critical(
                    f"[{reason}] invalid exchange response for {order_id}; "
                    "pending state is retained"
                )
                return False

            status = str(order.get("status", "unknown")).lower()
            terminal = ("closed", "canceled", "cancelled", "rejected", "expired")
            if status not in terminal:
                logger.critical(
                    f"[{reason}] order {order_id} is not confirmed terminal "
                    f"(status={status}); pending state and funds are retained"
                )
                return False

            # A fill can race the cancel. Hedge the final cumulative fill before
            # releasing the unused reservation.
            order_info["filled"] = float(order.get("filled", 0.0) or 0.0)
            self.supabase.order(
                pair_tag,
                "spot_entry",
                order_info.get("local_exchange", ""),
                "spot",
                local_symbol,
                order,
                client_order_id=order_info.get("client_order_id"),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            await self._handle_entry_fills(pair_tag, order_info, order)
            if self.states.get(pair_tag) in (
                PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
            ):
                logger.critical(
                    f"[{reason}] order {order_id} reached terminal state but its "
                    "fill could not be safely reconciled; retaining recovery state"
                )
                return False

            await self._finalize_entry_order(pair_tag, status)
            return True

        async def _expire_stale_entry_order(
            self, pair_tag: str, order_info: dict, order: dict
        ) -> bool:
            """Cancel an unfilled entry past TTL; resolve only after venue verification."""
            if order.get("status") != "open" or not math.isclose(float(order.get("filled", 0.0)), 0.0, abs_tol=1e-12):
                return False

            order_id = order_info["order_id"]
            placed_time = float(order_info.get("placed_time", 0.0))
            max_ttl = getattr(self.config, "max_order_ttl_seconds", 30)
            age = time.time() - placed_time
            if placed_time <= 0 or age < max_ttl:
                return False

            logger.info(
                f"[State 1 Order Expired] Maker Limit Buy {order_id} for {pair_tag} "
                f"expired after {age:.1f}s without market fill. Cancelling."
            )
            return await self._cancel_entry_order_and_reconcile(
                pair_tag, order_info, "Order Expired"
            )

        async def _check_adverse_selection(self, pair_tag: str, order_info: dict) -> bool:
            """Cancel a maker entry on adverse movement, but only resolve after verification."""
            live_price_key = f"arb:live_price:{pair_tag}"
            live_price_raw = await self.redis.get(live_price_key)
            if not live_price_raw:
                return False

            try:
                live_snapshot = json.loads(live_price_raw)
                global_bid = float(live_snapshot.get("global_best_bid_usdt", 0.0))
                threshold = order_info.get("cancel_threshold_usdt", 0.0)
                if threshold > 0 and 0 < global_bid < threshold:
                    return await self._cancel_pending_entry_due_to_adverse_selection(
                        pair_tag, order_info, global_bid, threshold
                    )
            except Exception as e:
                logger.debug(f"error reading live price for adverse selection: {e}")

            return False

        async def _cancel_pending_entry_due_to_adverse_selection(
            self, pair_tag: str, order_info: dict, global_bid: float, threshold: float
        ) -> bool:
            order_id = order_info["order_id"]
            logger.warning(
                f"[State 1 Adverse Selection] Global Bid {global_bid} dropped below "
                f"threshold {threshold} for {pair_tag}. Cancelling local Limit Buy {order_id}"
            )
            return await self._cancel_entry_order_and_reconcile(
                pair_tag, order_info, "Adverse Selection"
            )

        async def _handle_entry_fills(
            self, pair_tag: str, order_info: dict, order: dict
        ) -> None:
            """Check order fill delta and trigger short hedge if threshold is met."""
            status = order.get("status", "")
            current_filled = float(order.get("filled", 0.0))
            hedged_qty = float(order_info.get("hedged_short_qty", 0.0))
            unhedged_delta = current_filled - hedged_qty

            if unhedged_delta <= 0:
                return

            try:
                usdt_rate = float(order_info.get("usdt_idr_rate", 0.0) or 0.0)
            except (TypeError, ValueError):
                usdt_rate = 0.0
            price_idr = float(order_info.get("price", 0.0) or 0.0)
            if not math.isfinite(usdt_rate) or usdt_rate <= 0:
                # A filled IDR order without its persisted live FX snapshot is
                # an unreconciled exposure. Do not fabricate USD value from a
                # hardcoded fallback or record a position on a guessed basis.
                self._mark_failed(
                    pair_tag,
                    "FILLED_ORDER_FX_RATE_MISSING",
                    "Filled IDR order has no valid persisted live USDT/IDR rate; manual reconciliation required.",
                    detail={"order_id": order_info.get("order_id"), "unhedged_qty": unhedged_delta},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                await self.publish_status()
                return
            delta_val_usd = (unhedged_delta * price_idr) / usdt_rate

            is_complete = status in ("closed", "canceled", "cancelled", "rejected", "expired")
            if delta_val_usd >= self.config.min_partial_fill_usd or is_complete:
                logger.info(
                    f"[State 2 Instant Short Hedge] Fill detected: delta_qty={unhedged_delta} "
                    f"(${delta_val_usd:.2f}) on {pair_tag}. Opening Market Short on Global Futures!"
                )
                await self._execute_entry_hedge(
                    pair_tag, order_info, unhedged_delta, price_idr, usdt_rate
                )
                # Only clear the partial marker once the hedge actually went through.
                if self.states.get(pair_tag) == PositionState.ENTRY_PARTIAL:
                    self.states[pair_tag] = PositionState.HEDGED_ACTIVE
            elif 0 < current_filled < float(order_info.get("qty", 0.0)):
                # Below the hedge threshold with the maker order still resting: the spot
                # leg is partially filled and unhedged. Surface that explicitly rather
                # than leaving the pair looking like a plain pending entry.
                if self.states.get(pair_tag) != PositionState.ENTRY_PARTIAL:
                    logger.info(
                        f"[State 1 Partial Fill] {pair_tag} filled {current_filled}"
                        f"/{order_info.get('qty')} below hedge threshold (${delta_val_usd:.2f}); "
                        f"marking ENTRY_PARTIAL"
                    )
                    self.states[pair_tag] = PositionState.ENTRY_PARTIAL
                    await self._maybe_publish_status()

        async def _execute_entry_hedge(
            self,
            pair_tag: str,
            order_info: dict,
            unhedged_delta: float,
            price_idr: float,
            usdt_rate: float,
        ) -> None:
            """Execute market short on global exchange and persist position state."""
            global_symbol = order_info["global_symbol"]
            try:
                # Use the live futures ask as the expected short fill, never the spot price.
                expected_short_price = float(order_info.get("global_ask_usdt", 0.0) or 0.0)
                if expected_short_price <= 0 and usdt_rate > 0:
                    expected_short_price = price_idr / usdt_rate

                await self._assert_hedge_slippage_within_limit(
                    pair_tag, order_info, unhedged_delta, expected_short_price
                )

                # CORE RULE 2: ONLY Short 1x Futures (Market Sell)
                hedge_coid = build_client_order_id(
                    pair_tag, "hedge", self._next_order_seq()
                )
                short_order = await self.client.open_global_short_market(
                    global_symbol,
                    unhedged_delta,
                    price=expected_short_price,
                    client_order_id=hedge_coid,
                    amount_spec=AmountSpec.from_dict(order_info.get("global_amount_spec")),
                )
                if not isinstance(short_order, dict):
                    raise ValueError(
                        "futures hedge returned no verifiable order payload; "
                        "reconciliation required"
                    )
                hedge_status = str(short_order.get("status", "")).lower()
                if hedge_status not in ("closed", "filled"):
                    raise ValueError(
                        f"futures hedge is not terminal (status={hedge_status})"
                    )
                try:
                    actual_hedged_qty = float(short_order.get("filled") or 0.0)
                except (TypeError, ValueError):
                    actual_hedged_qty = 0.0
                qty_tolerance = max(1e-12, unhedged_delta * 1e-8)
                if actual_hedged_qty + qty_tolerance < unhedged_delta:
                    raise ValueError(
                        f"futures hedge fill incomplete: requested={unhedged_delta}, "
                        f"filled={actual_hedged_qty}; spot rollback will be guarded by "
                        "an authoritative futures-position check"
                    )
                if actual_hedged_qty > unhedged_delta + qty_tolerance:
                    raise ValueError(
                        f"futures hedge overfilled: requested={unhedged_delta}, "
                        f"filled={actual_hedged_qty}; manual reconciliation required"
                    )

                order_info["hedged_short_qty"] += actual_hedged_qty
                self._record_hedged_position(
                    pair_tag, order_info, actual_hedged_qty, short_order, price_idr, usdt_rate
                )
                self.supabase.order(
                    pair_tag,
                    "futures_entry",
                    order_info.get("global_exchange", ""),
                    "perpetual",
                    global_symbol,
                    short_order,
                    client_order_id=hedge_coid,
                    reduce_only=False,
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )
                self.supabase.position_opened(
                    pair_tag,
                    self.active_positions[pair_tag],
                    getattr(self, "_pair_infos", {}).get(pair_tag),
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )

                active_key = f"arb:active_pos:{pair_tag}"
                await self.redis.set(
                    active_key,
                    json.dumps(self.active_positions[pair_tag].to_dict()),
                )
                self.states[pair_tag] = PositionState.HEDGED_ACTIVE

                # Publish event to arb:trade_events for Telegram Bot
                pos = self.active_positions[pair_tag]
                open_msg = {
                    "event": "POSITION_OPENED",
                    "local_exchange": pos.local_exchange,
                    "global_exchange": pos.global_exchange,
                    "symbol": pos.symbol,
                    "qty": pos.qty,
                    "entry_price_idr": pos.entry_price_idr,
                    "short_entry_usdt": pos.short_entry_usdt,
                    "is_mock": getattr(self.config, "mock_mode", False),
                    "timestamp_ms": int(time.time() * 1000),
                }
                await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(open_msg))
                self.supabase.event(
                    "POSITION_OPENED",
                    pair_key=pair_tag,
                    is_mock=open_msg["is_mock"],
                    occurred_at_ms=open_msg["timestamp_ms"],
                    payload=open_msg,
                )
            except Exception as he:
                logger.critical(
                    f"[State 2 CRITICAL] Failed to execute instant short hedge for {pair_tag}: {he}"
                )
                global_exchange = str(order_info.get("global_exchange", ""))
                if global_exchange:
                    self._record_order_failure(
                        global_exchange, "hedge order placement failed"
                    )
                # The spot leg is already filled but unhedged: roll it back immediately.
                await self._rollback_hedge_failure(
                    pair_tag, order_info, unhedged_delta, price_idr, usdt_rate, he
                )
            else:
                global_exchange = str(order_info.get("global_exchange", ""))
                if global_exchange:
                    self._record_order_success(global_exchange)

        @staticmethod
        def _resolve_short_entry_usdt(short_order: Dict[str, Any]) -> float:
            """
            Resolves the actual futures short entry price.

            Never silently falls back to the spot price: if neither the exchange fill
            nor the expected futures ask is known, fail loudly so the position cannot
            be recorded with a wrong basis.
            """
            short_entry_usdt = helpers.extract_fill_price(short_order)
            if short_entry_usdt <= 0:
                raise ValueError(
                    "short entry price unavailable from futures fill; refusing to "
                    "record position using spot price basis"
                )
            return short_entry_usdt

    # Risk controls (exposure caps, venue halts, balance drift) live in
    # executor/risk.py as RiskMixin; hedge-failure rollback lives in
    # executor/rollback.py as RollbackMixin. This module stays focused on entry flow.

        async def _assert_hedge_slippage_within_limit(
            self,
            pair_tag: str,
            order_info: dict,
            unhedged_delta: float,
            expected_short_price: float,
        ) -> None:
            """
            Rejects the hedge when the futures ask already moved past the expected
            short price by more than ``max_entry_slippage_pct``.

            The caller catches the failure and rolls the spot leg back, so a bad
            quote becomes a small, bounded loss instead of an unprofitable position.
            A ticker that cannot be read does not block the hedge: refusing to hedge
            at all is strictly riskier than hedging at an unverified price.
            """
            max_slippage = float(getattr(self.config, "max_entry_slippage_pct", 0.0) or 0.0)
            if max_slippage <= 0 or expected_short_price <= 0:
                return

            global_symbol = order_info["global_symbol"]
            try:
                ticker = await self.client.fetch_global_ticker(global_symbol)
            except Exception as exc:  # nosec BLE001 - fail open, see docstring
                logger.warning(
                    f"[Slippage Guard] could not read {global_symbol} ticker for {pair_tag}: "
                    f"{exc}. Proceeding with the hedge."
                )
                return

            try:
                live_ask = float((ticker or {}).get("ask") or 0.0)
            except (TypeError, ValueError):
                live_ask = 0.0
            if live_ask <= 0:
                logger.info(
                    f"[Slippage Guard] no live ask for {global_symbol}; skipping guard on {pair_tag}"
                )
                return

            limit_price = expected_short_price * (1.0 + max_slippage)
            if live_ask > limit_price:
                deviation_pct = (live_ask / expected_short_price - 1.0) * 100.0
                raise HedgeSlippageExceeded(
                    f"futures ask {live_ask} deviates {deviation_pct:.3f}% from expected "
                    f"{expected_short_price} (limit {max_slippage * 100:.3f}%, "
                    f"qty {unhedged_delta})"
                )


        def _record_hedged_position(
            self,
            pair_tag: str,
            order_info: dict,
            qty: float,
            short_order: dict,
            price_idr: float,
            usdt_rate: float,
        ) -> None:
            """Update or create active position record for the hedged fill."""
            short_entry_usdt = self._resolve_short_entry_usdt(short_order)
            spot_cost = (qty * price_idr) / usdt_rate
            fut_margin = qty * short_entry_usdt

            if pair_tag not in self.active_positions:
                self.active_positions[pair_tag] = ActivePosition(
                    local_exchange=order_info["local_exchange"],
                    global_exchange=order_info["global_exchange"],
                    symbol=order_info["symbol"],
                    base_asset=order_info.get("base_asset", ""),
                    global_symbol=order_info.get("global_symbol", ""),
                    entry_price_idr=price_idr,
                    short_entry_usdt=short_entry_usdt,
                    qty=qty,
                    accumulated_funding_usdt=0.0,
                    next_funding_time_ms=order_info["next_funding_time_ms"],
                    base_target_exit_spread_pct=order_info["base_target_exit_spread_pct"],
                    current_funding_rate=order_info["current_funding_rate"],
                    local_price_tick=float(order_info.get("tick_size", 1.0)),
                    local_lot_size=float(order_info.get("local_lot_size", 0.0)),
                    local_amount_spec=AmountSpec.from_dict(order_info.get("local_amount_spec")),
                    global_amount_spec=AmountSpec.from_dict(order_info.get("global_amount_spec")),
                    spot_cost_usdt=spot_cost,
                    futures_margin_usdt=fut_margin,
                )
            else:
                self.active_positions[pair_tag].qty += qty
                self.active_positions[pair_tag].spot_cost_usdt += spot_cost
                self.active_positions[pair_tag].futures_margin_usdt += fut_margin

        async def _finalize_entry_order(self, pair_tag: str, status: str) -> None:
            """Remove finished entry order from pending list and transition state."""
            if status not in ("closed", "canceled", "cancelled", "rejected", "expired"):
                return

            order_info = self.pending_entry_orders.get(pair_tag, {})
            # If partial fill left unused reserved funds, release the remainder
            if hasattr(self.client, "unlock_funds"):
                pos = self.active_positions.get(pair_tag)
                used_spot = pos.spot_cost_usdt if pos else 0.0
                used_fut = pos.futures_margin_usdt if pos else 0.0
                reserved_spot = float(order_info.get("spot_cost_usdt", 0.0))
                reserved_fut = float(order_info.get("futures_margin_usdt", 0.0))
                unused_spot = max(0.0, reserved_spot - used_spot)
                unused_fut = max(0.0, reserved_fut - used_fut)
                if unused_spot > 0 or unused_fut > 0:
                    self.client.unlock_funds(unused_spot, unused_fut)

            del self.pending_entry_orders[pair_tag]
            await self._clear_pending_entry(pair_tag)
            if pair_tag in self.active_positions and self.active_positions[pair_tag].qty > 0:
                self.states[pair_tag] = PositionState.HEDGED_ACTIVE
            else:
                self.states[pair_tag] = PositionState.IDLE
