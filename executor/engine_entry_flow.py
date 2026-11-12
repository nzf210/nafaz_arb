"""Signal-to-entry orchestration facade for the arbitrage engine."""

import asyncio
import math
import time
from typing import Optional

from executor.models import ExecutorPayload
from executor.engine_common import logger
from executor.adaptive_maker import (
    aggressive_maker_price_idr,
    dynamic_max_buy_price_idr,
)


class EngineEntryFlowMixin:
    """Coordinates validation, sizing, and the existing spot entry lifecycle."""

    async def _apply_indodax_adaptive_maker_quote(
        self, pair_tag: str, payload: ExecutorPayload, directives
    ) -> bool:
        """Reprice only live Indodax IDR entries from a fresh local order book."""
        pair_info = payload.pair_info
        if (
            bool(getattr(self.config, "mock_mode", False))
            or str(pair_info.local_exchange or "").lower() != "indodax"
            or str(pair_info.quote_asset or "").upper() not in ("IDR", "BIDR")
        ):
            return True

        try:
            book = await self.client.fetch_local_order_book(
                pair_info.local_symbol, limit=20
            )
            book_timestamp_ms = int((book or {}).get("timestamp") or 0)
            book_age_ms = int(time.time() * 1000) - book_timestamp_ms
            if book_timestamp_ms <= 0 or not 0 <= book_age_ms <= 3000:
                raise ValueError(
                    f"Indodax order book is stale or has no valid timestamp ({book_age_ms}ms)"
                )
            ticker = await self.client.fetch_global_ticker(pair_info.global_symbol)
            global_bid_usdt = float((ticker or {}).get("bid") or 0.0)
            fx_rate = float(payload.pricing_snapshot.usdt_idr_rate or 0.0)
            scanner_price = float(directives.recommended_limit_buy_idr or 0.0)
            quantity = float(directives.quantity or 0.0)
            estimated_profit = float(directives.estimated_net_profit_usdt or 0.0)
            if global_bid_usdt <= 0 or fx_rate <= 0:
                raise ValueError("fresh global bid or USDT/IDR FX is unavailable")

            min_profit_pct = float(
                getattr(self.config, "adaptive_maker_min_net_profit_pct", 0.10)
            )
            max_buy_price = dynamic_max_buy_price_idr(
                scanner_price,
                estimated_profit,
                quantity,
                fx_rate,
                float(directives.spot_entry_fee_pct or 0.0),
                float(directives.fee_asset_surcharge_pct or 0.0),
                min_profit_pct,
            )
            candidate = aggressive_maker_price_idr(
                book.get("bids"), book.get("asks"),
                pair_info.local_price_tick, max_buy_price,
            )
            if candidate is None:
                raise ValueError(
                    "no maker-only price is available inside the dynamic net-profit ceiling"
                )

            min_spread_pct = float(
                getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0
            )
            if min_spread_pct <= 0:
                min_spread_pct = float(directives.min_entry_spread_pct or 0.0)
            global_bid_idr = global_bid_usdt * fx_rate
            live_spread_pct = (
                (global_bid_idr - candidate) / global_bid_idr
            ) * 100.0
            if live_spread_pct < min_spread_pct:
                raise ValueError(
                    f"live executable spread {live_spread_pct:.4f}% is below "
                    f"the required {min_spread_pct:.4f}%"
                )

            entry_fee_multiplier = 1.0 + (
                float(directives.spot_entry_fee_pct or 0.0)
                + float(directives.fee_asset_surcharge_pct or 0.0)
            ) / 100.0
            profit_after_reprice = estimated_profit - (
                quantity * (candidate - scanner_price) / fx_rate
                * entry_fee_multiplier
            )
            target_profit_usdt = (
                quantity * scanner_price / fx_rate * min_profit_pct / 100.0
            )
            if profit_after_reprice < target_profit_usdt:
                raise ValueError(
                    "candidate price no longer retains the dynamic net-profit floor"
                )

            directives.adaptive_max_buy_price_idr = max_buy_price
            directives.recommended_limit_buy_idr = candidate
            directives.estimated_net_profit_usdt = profit_after_reprice
            logger.info(
                "[Adaptive Maker] %s: Indodax IDR aggressive peg %.8f "
                "(best bid %.8f / ask %.8f / cap %.8f), "
                "spread %.4f%%, projected net %.6f USDT",
                pair_tag, candidate, float(book["bids"][0][0]),
                float(book["asks"][0][0]), max_buy_price,
                live_spread_pct, profit_after_reprice,
            )
            return True
        except Exception as exc:
            logger.warning(
                "[Adaptive Maker] rejecting %s: fresh book/profit validation failed: %s",
                pair_tag, exc,
            )
            self._mark_failed(
                pair_tag,
                "ADAPTIVE_MAKER_QUOTE_UNSAFE",
                "Indodax IDR entry rejected because a fresh maker-only quote "
                "could not retain the dynamic net-profit floor.",
                detail={"error": str(exc)[:500]},
            )
            await self.publish_status()
            return False

    def _spot_order_limit(self, exchange: str) -> int:
        limits = getattr(self.config, "max_open_spot_orders_per_exchange", {}) or {}
        try:
            value = int(limits.get(str(exchange).strip().lower(), 2))
        except (TypeError, ValueError):
            value = 2
        return min(100, max(1, value))

    def _spot_open_order_count(self, exchange: str) -> int:
        """Count distinct spot pairs with an open position or pending entry."""
        venue = str(exchange or "").strip().lower()
        def pair_exchange(pair, value=None):
            exchange_name = (
                value.get("local_exchange", "") if isinstance(value, dict)
                else getattr(value, "local_exchange", "")
            )
            return str(exchange_name or str(pair).split(":", 1)[0]).strip().lower()

        active_pairs = {
            pair for pair, position in self.active_positions.items()
            if pair_exchange(pair, position) == venue
            and float(getattr(position, "qty", 0.0) or 0.0) > 0
        }
        pending_pairs = {
            pair for pair, order in self.pending_entry_orders.items()
            if pair_exchange(pair, order) == venue
        }
        # RECONCILIATION_REQUIRED is deliberately absent: a pair in that state
        # with no position and no pending order has nothing outstanding on the
        # venue, and counting it burned a slot forever because the incident can
        # never be cleared without a position. Any real exposure is already
        # covered by ``active_pairs``/``pending_pairs`` above.
        counted_states = {
            "ENTRY_PENDING", "ENTRY_PARTIAL", "HEDGED_ACTIVE", "EXIT_PENDING",
        }
        lifecycle_pairs = {
            pair for pair, state in self.states.items()
            if getattr(state, "value", str(state)) in counted_states
            and pair_exchange(pair, self._pair_infos.get(pair)) == venue
        }
        return len(active_pairs | pending_pairs | lifecycle_pairs)

    def _spot_exchange_entry_lock(self, exchange: str):
        venue = str(exchange or "").strip().lower()
        lock = self._spot_exchange_entry_locks.get(venue)
        if lock is None:
            lock = asyncio.Lock()
            self._spot_exchange_entry_locks[venue] = lock
        return lock

    async def _handle_signal_locked(
        self, pair_tag: str, payload: Optional[ExecutorPayload] = None
    ):
        if payload is None:
            payload = await self._load_signal_payload(pair_tag)
        if payload is None:
            return

        # IDR sizing is only valid with the live FX rate carried by this scan.
        # Never let mock-mode defaults or a static fallback turn an invalid
        # FX snapshot into a tradable order.
        quote_asset = str(payload.pair_info.quote_asset or "").upper()
        if quote_asset in ("IDR", "BIDR"):
            fx_rate = float(payload.pricing_snapshot.usdt_idr_rate or 0.0)
            if not math.isfinite(fx_rate) or fx_rate <= 0:
                logger.error(
                    f"[FX Preflight] rejecting {pair_tag}: live USDT/IDR rate is missing or invalid"
                )
                self._mark_failed(
                    pair_tag,
                    "LIVE_FX_RATE_INVALID",
                    "IDR spot entry requires a finite, positive live USDT/IDR rate.",
                    detail={"quote_asset": quote_asset, "usdt_idr_rate": fx_rate},
                )
                await self.publish_status()
                return

        # Cache the venue names so venue-halt checks work before the next
        # payload read (and during monitoring after the signal key expires).
        self._pair_infos[pair_tag] = payload.pair_info

        if await self._is_stale_or_duplicate_signal(pair_tag, payload):
            return

        if not await self._validate_entry_spread(pair_tag, payload):
            return

        directives = payload.execution_directives
        # Refresh account-specific rates before any new LIVE order. If a venue
        # lacks an authenticated fee endpoint, retain the explicitly supplied
        # scanner profile and keep its source/verification status unchanged.

        await self._refresh_fee_snapshot(pair_tag, payload, directives)
        if not await self._validate_dynamic_fee_edge(pair_tag, directives):
            return

        pair_info = payload.pair_info
        snapshot = payload.pricing_snapshot
        if not await self._validate_entry_market_rules(pair_tag, pair_info):
            return

        if not await self._apply_indodax_adaptive_maker_quote(
            pair_tag, payload, directives
        ):
            return

        sizing = await self._prepare_entry_sizing(
            pair_tag, payload, pair_info, directives
        )
        if sizing is None:
            return
        buy_qty, actual_cost_usdt, buy_price_idr = sizing
        # The exchange-wide lock closes the race where simultaneous signals
        # for different pairs would all pass the same venue cap.
        exchange = str(pair_info.local_exchange or "").strip().lower()
        async with self._spot_exchange_entry_lock(exchange):
            current = self._spot_open_order_count(exchange)
            maximum = self._spot_order_limit(exchange)
            if current >= maximum:
                self._mark_failed(
                    pair_tag,
                    "SPOT_OPEN_ORDER_LIMIT_REACHED",
                    f"{exchange} already has {current}/{maximum} open spot positions or pending entries.",
                    detail={"exchange": exchange, "open_spot_orders": current, "max_open_spot_orders": maximum},
                )
                await self.publish_status()
                return
            await self._submit_entry_order(
                pair_tag, payload, pair_info, snapshot, directives,
                buy_qty, actual_cost_usdt, buy_price_idr,
            )
