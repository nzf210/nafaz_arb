"""entry hedge methods for entry execution."""

import json
import math
import time
from typing import Any, Dict

from executor.models import ActivePosition, OrderRole, PositionState
from executor import helpers
from executor.quantity import AmountSpec
from executor.helpers import HedgeSlippageExceeded, build_client_order_id
from executor.entry_common import logger



class EntryHedgeMixin:
    """Focused entry lifecycle responsibilities."""

        async def _handle_entry_fills(
            self, pair_tag: str, order_info: dict, order: dict
        ) -> None:
            """Check order fill delta and trigger short hedge if threshold is met."""
            status = order.get("status", "")
            current_filled = float(order.get("filled", 0.0) or 0.0)
            order_info["filled"] = max(
                float(order_info.get("filled", 0.0) or 0.0), current_filled
            )
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

        async def _persist_entry_hedge_watermark(
            self, pair_tag: str, order_info: dict, hedged_quantity: float,
            hedge_order_id: str = "",
        ) -> bool:
            """Persist cumulative hedge progress on the originating spot entry."""
            persisted = False
            try:
                quantity = float(hedged_quantity or 0.0)
                if not math.isfinite(quantity) or quantity < 0:
                    raise ValueError("invalid cumulative hedge quantity")
                order_info["hedged_short_qty"] = max(
                    float(order_info.get("hedged_short_qty", 0.0) or 0.0),
                    quantity,
                )
                # Keep per-futures-order idempotency markers on the originating
                # spot record. A restart may replay hedge reconciliation after
                # the parent watermark was written but before the hedge record
                # was retired; the same fill must never be counted twice.
                if hedge_order_id:
                    hedge_fills = order_info.setdefault("hedge_fill_orders", {})
                    if not isinstance(hedge_fills, dict):
                        hedge_fills = {}
                        order_info["hedge_fill_orders"] = hedge_fills
                    hedge_fills.setdefault(str(hedge_order_id), quantity)
                registry = getattr(self, "order_registry", None)
                if registry is not None:
                    exchange = str(order_info.get("local_exchange") or "").lower()
                    order_id = str(order_info.get("order_id") or "")
                    client_id = str(order_info.get("client_order_id") or "")
                    record = None
                    if exchange and order_id:
                        record = await registry.find_by_exchange_id(exchange, order_id)
                    if record is None and exchange and client_id:
                        record = await registry.find_by_client_id(exchange, client_id)
                    if record is not None:
                        record.hedged_quantity = max(
                            float(record.hedged_quantity or 0.0), quantity
                        )
                        if hedge_order_id:
                            context = dict(record.recovery_context or {})
                            hedge_fills = context.get("hedge_fill_orders")
                            if not isinstance(hedge_fills, dict):
                                hedge_fills = {}
                            hedge_fills.setdefault(str(hedge_order_id), quantity)
                            context["hedge_fill_orders"] = hedge_fills
                            record.recovery_context = context
                        await registry.register(record)
                        persisted = True
                    else:
                        logger.critical(
                            "[Recovery] cannot locate originating spot order %s/%s "
                            "to persist hedge watermark for %s",
                            exchange, order_id or client_id, pair_tag,
                        )
                persist = getattr(self, "_persist_pending_entry", None)
                if callable(persist):
                    await persist(pair_tag, order_info)
                return persisted
            except Exception:
                # The hedge has already executed. Do not retry it here; the
                # caller keeps the position managed but blocks new entries.
                logger.exception(
                    "[Recovery] failed to persist hedge watermark for %s", pair_tag
                )
                return False

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
                hedge_recovery_context = {
                    "parent_spot_exchange": str(order_info.get("local_exchange", "")).lower(),
                    "parent_spot_order_id": str(order_info.get("order_id", "")),
                    "parent_spot_client_order_id": str(order_info.get("client_order_id", "")),
                    "hedge_requested_for_spot_fill": float(unhedged_delta),
                }
                await self._register_order_intent(
                    pair_tag,
                    hedge_coid,
                    exchange=order_info.get("global_exchange", ""),
                    market_type="perpetual",
                    symbol=global_symbol,
                    role=OrderRole.HEDGE.value,
                    side="sell",
                    order_type="market",
                    price=expected_short_price,
                    quantity=unhedged_delta,
                    global_amount_spec=order_info.get("global_amount_spec"),
                    recovery_context=hedge_recovery_context,
                    required=True,
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

                # Register the exchange acknowledgement while the hedge is
                # deliberately non-terminal in the recovery journal. If the
                # process dies now, startup can fetch the exact futures order by
                # exchange id and reconcile its fill against the parent spot leg.
                recovery_order = dict(short_order)
                recovery_order["status"] = "open"
                hedge_journal = await self._register_order(
                    pair_tag,
                    hedge_coid,
                    recovery_order,
                    exchange=order_info.get("global_exchange", ""),
                    market_type="perpetual",
                    symbol=global_symbol,
                    role=OrderRole.HEDGE.value,
                    side="sell",
                    order_type="market",
                    price=expected_short_price,
                    quantity=unhedged_delta,
                    global_amount_spec=order_info.get("global_amount_spec"),
                    recovery_context=hedge_recovery_context,
                )
                if hedge_journal is None:
                    raise RuntimeError(
                        "futures hedge was accepted but its fill could not be journaled; "
                        "rollback/reconciliation is required"
                    )
                order_info["hedged_short_qty"] += actual_hedged_qty
                # Persist the entry's cumulative hedge watermark and the unique
                # futures order id together on the originating spot record.
                watermark_persisted = await self._persist_entry_hedge_watermark(
                    pair_tag, order_info, order_info["hedged_short_qty"],
                    hedge_order_id=hedge_coid,
                )
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
                await self._register_order(
                    pair_tag,
                    hedge_coid,
                    short_order,
                    exchange=order_info.get("global_exchange", ""),
                    market_type="perpetual",
                    symbol=global_symbol,
                    role=OrderRole.HEDGE.value,
                    side="sell",
                    order_type="market",
                    price=expected_short_price,
                    quantity=unhedged_delta,
                    global_amount_spec=order_info.get("global_amount_spec"),
                    recovery_context=hedge_recovery_context,
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
                if not watermark_persisted:
                    # The exchange hedge succeeded, so keep managing the live
                    # position, but do not permit another entry against a spot
                    # order whose cumulative hedge watermark is not durable.
                    self._halt_exposure_scope(
                        pair_tag, "entry hedge watermark could not be persisted"
                    )
                    logger.critical(
                        "[Recovery] %s is hedged on the venue but its originating "
                        "spot-order watermark is not durable; pair entry remains halted",
                        pair_tag,
                    )

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
                    spot_entry_fee_pct=float(order_info.get("spot_entry_fee_pct", 0.0)),
                    spot_exit_fee_pct=float(order_info.get("spot_exit_fee_pct", 0.0)),
                    perp_entry_fee_pct=float(order_info.get("perp_entry_fee_pct", 0.0)),
                    perp_exit_fee_pct=float(order_info.get("perp_exit_fee_pct", 0.0)),
                    fee_source=str(order_info.get("fee_source", "LEGACY_UNVERIFIED")),
                    fee_verified=bool(order_info.get("fee_verified", False)),
                    fee_fetched_at_ms=int(order_info.get("fee_fetched_at_ms", 0)),
                    fee_asset_surcharge_pct=float(order_info.get("fee_asset_surcharge_pct", 0.0)),
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
