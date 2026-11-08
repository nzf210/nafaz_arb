"""Hedge execution and hedged-position creation."""

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
    build_client_order_id,
)
from executor.private_stream import fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")


class EntryHedgeMixin:
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
                    global_symbol=order_info.get("global_symbol", ""),
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
                if not self.active_positions[pair_tag].global_symbol:
                    self.active_positions[pair_tag].global_symbol = order_info.get("global_symbol", "")
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
