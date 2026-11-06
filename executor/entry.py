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
from executor.helpers import (
    BALANCES_KEY,
    HedgeSlippageExceeded,
    RetryExhausted,
    build_client_order_id,
    retry_with_backoff,
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

        async def _expire_stale_entry_order(
            self, pair_tag: str, order_info: dict, order: dict
        ) -> bool:
            """Cancel an unfilled entry order past its TTL. Returns True if cancelled."""
            if order.get("status") != "open" or not math.isclose(float(order.get("filled", 0.0)), 0.0, abs_tol=1e-12):
                return False

            order_id = order_info["order_id"]
            local_symbol = order_info["symbol"]
            placed_time = float(order_info.get("placed_time", 0.0))
            max_ttl = getattr(self.config, "max_order_ttl_seconds", 30)
            age = time.time() - placed_time
            if placed_time <= 0 or age < max_ttl:
                return False

            logger.info(
                f"[State 1 Order Expired] Maker Limit Buy {order_id} for {pair_tag} "
                f"expired after {age:.1f}s without market fill. Cancelling."
            )
            try:
                await self.client.cancel_local_order(order_id, local_symbol)
            except Exception:
                pass
            if hasattr(self.client, "unlock_funds"):
                self.client.unlock_funds(
                    float(order_info.get("spot_cost_usdt", 0.0)),
                    float(order_info.get("futures_margin_usdt", 0.0)),
                )
                bals = self.client.get_balances()
                await self.redis.set(BALANCES_KEY, json.dumps(bals))
            self.pending_entry_orders.pop(pair_tag, None)
            await self._clear_pending_entry(pair_tag)
            self.states[pair_tag] = PositionState.IDLE
            return True

        async def _check_adverse_selection(self, pair_tag: str, order_info: dict) -> bool:
            """Adverse Selection Protection: Check Global Bid Price and cancel if below threshold."""
            live_price_key = f"arb:live_price:{pair_tag}"
            live_price_raw = await self.redis.get(live_price_key)
            if not live_price_raw:
                return False

            try:
                live_snapshot = json.loads(live_price_raw)
                global_bid = float(live_snapshot.get("global_best_bid_usdt", 0.0))
                threshold = order_info.get("cancel_threshold_usdt", 0.0)
                if threshold > 0 and 0 < global_bid < threshold:
                    await self._cancel_pending_entry_due_to_adverse_selection(
                        pair_tag, order_info, global_bid, threshold
                    )
                    return True
            except Exception as e:
                logger.debug(f"error reading live price for adverse selection: {e}")

            return False

        async def _cancel_pending_entry_due_to_adverse_selection(
            self, pair_tag: str, order_info: dict, global_bid: float, threshold: float
        ) -> None:
            order_id = order_info["order_id"]
            local_symbol = order_info["symbol"]
            logger.warning(
                f"[State 1 Adverse Selection] Global Bid {global_bid} dropped below "
                f"threshold {threshold} for {pair_tag}. Cancelling local Limit Buy {order_id}"
            )
            try:
                await self.client.cancel_local_order(order_id, local_symbol)
            except Exception:
                logger.exception(f"failed to cancel order {order_id}")
            if hasattr(self.client, "unlock_funds"):
                self.client.unlock_funds(
                    float(order_info.get("spot_cost_usdt", 0.0)),
                    float(order_info.get("futures_margin_usdt", 0.0)),
                )
                bals = self.client.get_balances()
                await self.redis.set(BALANCES_KEY, json.dumps(bals))
            del self.pending_entry_orders[pair_tag]
            await self._clear_pending_entry(pair_tag)
            self.states[pair_tag] = PositionState.IDLE

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

            usdt_rate = order_info.get("usdt_idr_rate", self.FALLBACK_USDT_IDR_RATE)
            price_idr = order_info.get("price", 1.0)
            delta_val_usd = (unhedged_delta * price_idr) / usdt_rate

            is_complete = status in ("closed", "canceled", "rejected")
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
                short_order = await self.client.open_global_short_market(
                    global_symbol,
                    unhedged_delta,
                    price=expected_short_price,
                    client_order_id=build_client_order_id(pair_tag, "hedge", self._next_order_seq()),
                )
                order_info["hedged_short_qty"] += unhedged_delta

                self._record_hedged_position(
                    pair_tag, order_info, unhedged_delta, short_order, price_idr, usdt_rate
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
            short_entry_usdt = helpers.extract_fill_price(
                short_order, float(short_order.get("expected_price") or 0.0)
            )
            if short_entry_usdt <= 0:
                raise ValueError(
                    "short entry price unavailable from futures fill; refusing to "
                    "record position using spot price basis"
                )
            return short_entry_usdt

    # Risk controls (exposure caps, venue halts, balance drift) live in
    # executor/risk.py as RiskMixin so this module stays focused on entry flow.

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

        async def _liquidate_unhedged_spot(
            self, pair_tag: str, symbol: str, unhedged_delta: float
        ) -> tuple[Optional[Dict[str, Any]], Optional[Exception]]:
            """
            Market-sells the unhedged spot quantity back to quote.

            Returns ``(order, error)``: the order is None when every attempt failed,
            and ``error`` then carries the last failure so the operator event can say
            what went wrong.
            """
            max_retries = 4  # 5 total attempts
            # One id for the whole retry sequence: every attempt is the same logical
            # order, so the venue can reject duplicates rather than sell twice.
            seq = self._next_order_seq()
            try:
                emergency_order = await retry_with_backoff(
                    lambda: self.client.place_local_market_sell(
                        symbol,
                        unhedged_delta,
                        client_order_id=build_client_order_id(pair_tag, "rollback", seq),
                    ),
                    max_retries=max_retries,
                    base_delay=0.5,
                    label=f"hedge rollback sell {symbol} for {pair_tag}",
                )
            except RetryExhausted as exhausted:
                logger.critical(
                    f"[Hedge Rollback] FAILED to liquidate spot leg for {pair_tag} after "
                    f"{exhausted.attempts} attempts: {exhausted.last_error}. "
                    f"MANUAL INTERVENTION REQUIRED: {unhedged_delta} {symbol} is UNHEDGED."
                )
                return None, exhausted.last_error
            return emergency_order, None

        async def _hedge_may_be_open(
            self, pair_tag: str, global_symbol: str, error: Exception
        ) -> bool:
            """
            Returns True when a futures short MIGHT be open after a hedge error.

            Only a venue-confirmed zero position is safe to roll back against. Any
            query failure or unexpected payload blocks the rollback (fail-safe):
            a wrongly-cancelled rollback leaves unhedged spot (recoverable by an
            operator), whereas a wrong rollback on an open short inverts exposure.
            """
            if not global_symbol:
                return True
            try:
                positions = await self.client.fetch_positions(global_symbol)
            except AttributeError:
                # Client without fetch_positions (e.g. some mocks): cannot confirm.
                logger.warning(
                    f"[Hedge Ambiguity] {pair_tag}: client lacks fetch_positions; treating hedge state as unknown"
                )
                return True
            except Exception as exc:
                logger.warning(
                    f"[Hedge Ambiguity] {pair_tag}: position query failed ({exc}); treating hedge state as unknown"
                )
                return True
            if not positions:
                return False
            total = 0.0
            for p in positions:
                side = str(p.get("side", "")).lower()
                qty = abs(float(p.get("contracts", 0.0) or 0.0))
                total += qty if side != "long" else -qty
            if abs(total) > 1e-12:
                logger.critical(
                    f"[Hedge Ambiguity] {pair_tag}: venue reports net short {total} on "
                    f"{global_symbol} despite hedge error ({error}) - spot NOT rolled back"
                )
                return True
            return False

        async def _release_unhedged_reserve(
            self, unhedged_delta: float, price_idr: float, usdt_rate: float
        ) -> None:
            """Releases the capital reserved for the spot leg that never got hedged."""
            if not hasattr(self.client, "unlock_funds"):
                return
            spot_cost = (unhedged_delta * price_idr) / usdt_rate if usdt_rate > 0 else 0.0
            self.client.unlock_funds(spot_cost, 0.0)
            if hasattr(self.client, "get_balances"):
                try:
                    await self.redis.set(BALANCES_KEY, json.dumps(self.client.get_balances()))
                except Exception:
                    logger.exception("failed to persist balances after hedge rollback")

        async def _rollback_hedge_failure(
            self,
            pair_tag: str,
            order_info: dict,
            unhedged_delta: float,
            price_idr: float,
            usdt_rate: float,
            error: Exception,
        ) -> None:
            """
            Emergency rollback when the global futures short cannot be opened.

            Without this the local spot coins bought by the maker order stay exposed
            to full directional risk. We market-sell the unhedged spot quantity back
            to quote and surface a CRITICAL event so the operator sees the loss.
            """
            symbol = order_info.get("symbol", "")
            logger.critical(
                f"[Hedge Rollback] Selling {unhedged_delta} {symbol} spot back to quote "
                f"after hedge failure on {pair_tag}"
            )

            # Timeout ambiguity guard: a hedge "failure" may be a timeout AFTER the
            # venue accepted the market sell. Rolling back the spot while the short
            # is actually open would leave the book net-short in the wrong direction.
            # Only proceed with the rollback when the venue confirms no open short.
            global_symbol = str(order_info.get("global_symbol", ""))
            hedge_error_is_ambiguous = await self._hedge_may_be_open(
                pair_tag, global_symbol, error
            )
            if hedge_error_is_ambiguous:
                self._mark_failed(
                    pair_tag,
                    "HEDGE_STATE_UNKNOWN",
                    "hedge error is ambiguous and a futures short may be open; spot rollback blocked pending manual check",
                    detail={
                        "unhedged_qty": unhedged_delta,
                        "symbol": symbol,
                        "global_symbol": global_symbol,
                        "hedge_error": str(error),
                    },
                )
                event = {
                    "event": "HEDGE_FAILED",
                    "pair_key": pair_tag,
                    "local_exchange": order_info.get("local_exchange"),
                    "global_exchange": order_info.get("global_exchange"),
                    "symbol": symbol,
                    "qty": unhedged_delta,
                    "entry_price_idr": price_idr,
                    "exit_price_idr": 0.0,
                    "rollback_succeeded": False,
                    "reason": f"hedge state unknown, spot NOT rolled back: {error}",
                    "rollback_error": None,
                    "is_mock": getattr(self.config, "mock_mode", False),
                    "timestamp_ms": int(time.time() * 1000),
                }
                await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(event))
                await self.publish_status()
                return

            emergency_order, emergency_error = await self._liquidate_unhedged_spot(
                pair_tag, symbol, unhedged_delta
            )

            exit_price_idr = 0.0
            if emergency_order:
                exit_price_idr = helpers.extract_fill_price(emergency_order, price_idr)
                if exit_price_idr <= 0:
                    exit_price_idr = price_idr

            # Release the reserved capital for the leg that never got hedged.
            await self._release_unhedged_reserve(unhedged_delta, price_idr, usdt_rate)

            event = {
                "event": "HEDGE_FAILED",
                "pair_key": pair_tag,
                "local_exchange": order_info.get("local_exchange"),
                "global_exchange": order_info.get("global_exchange"),
                "symbol": symbol,
                "qty": unhedged_delta,
                "entry_price_idr": price_idr,
                "exit_price_idr": exit_price_idr,
                "rollback_succeeded": emergency_error is None,
                "reason": str(error),
                "rollback_error": str(emergency_error) if emergency_error else None,
                "is_mock": getattr(self.config, "mock_mode", False),
                "timestamp_ms": int(time.time() * 1000),
            }
            await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(event))

            if emergency_order is None:
                self._mark_failed(
                    pair_tag,
                    "HEDGE_FAILED",
                    f"hedge failed and spot rollback failed: {emergency_error}",
                    detail={
                        "unhedged_qty": unhedged_delta,
                        "symbol": symbol,
                        "entry_price_idr": price_idr,
                        "hedge_error": str(error),
                    },
                )
            else:
                self.states[pair_tag] = PositionState.CLOSED
            await self.publish_status()

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
                    entry_price_idr=price_idr,
                    short_entry_usdt=short_entry_usdt,
                    qty=qty,
                    accumulated_funding_usdt=0.0,
                    next_funding_time_ms=order_info["next_funding_time_ms"],
                    base_target_exit_spread_pct=order_info["base_target_exit_spread_pct"],
                    current_funding_rate=order_info["current_funding_rate"],
                    local_price_tick=float(order_info.get("tick_size", 1.0)),
                    spot_cost_usdt=spot_cost,
                    futures_margin_usdt=fut_margin,
                )
            else:
                self.active_positions[pair_tag].qty += qty
                self.active_positions[pair_tag].spot_cost_usdt += spot_cost
                self.active_positions[pair_tag].futures_margin_usdt += fut_margin

        async def _finalize_entry_order(self, pair_tag: str, status: str) -> None:
            """Remove finished entry order from pending list and transition state."""
            if status not in ("closed", "canceled", "rejected"):
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
