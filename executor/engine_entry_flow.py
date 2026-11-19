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
        return min(100, max(0, value))

    def _spot_open_order_count(self, exchange: str) -> int:
        """Count distinct spot pairs with an open position or pending entry."""
        raw_key = str(exchange or "").strip().lower()
        if ":" in raw_key:
            venue, target_quote = raw_key.split(":", 1)
        else:
            venue, target_quote = raw_key, ""

        def pair_exchange(pair, value=None):
            exchange_name = (
                value.get("local_exchange", "") if isinstance(value, dict)
                else getattr(value, "local_exchange", "")
            )
            return str(exchange_name or str(pair).split(":", 1)[0]).strip().lower()

        def pair_matches(pair, value=None) -> bool:
            if pair_exchange(pair, value) != venue:
                return False
            if not target_quote:
                return True
            quote = ""
            if isinstance(value, dict):
                quote = str(value.get("quote_asset") or "").strip().lower()
            elif value is not None:
                quote = str(getattr(value, "quote_asset", "") or "").strip().lower()
            if not quote:
                pinfo = getattr(self, "_pair_infos", {}).get(pair)
                if pinfo is not None:
                    quote = str(getattr(pinfo, "quote_asset", "") or "").strip().lower()
            if not quote:
                from executor.helpers import split_symbol_parts

                sym = (
                    value.get("symbol", "") if isinstance(value, dict)
                    else getattr(value, "symbol", "") or str(pair).split("__", 1)[0]
                )
                _, quote = split_symbol_parts(str(sym))
                quote = quote.lower()
            return quote == target_quote

        active_pairs = {
            pair for pair, position in self.active_positions.items()
            if pair_matches(pair, position)
            and float(getattr(position, "qty", 0.0) or 0.0) > 0
        }
        pending_pairs = {
            pair for pair, order in self.pending_entry_orders.items()
            if pair_matches(pair, order)
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
            and pair_matches(pair, self._pair_infos.get(pair))
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

        exchange = str(payload.pair_info.local_exchange or "").strip().lower()
        quote_asset = str(payload.pair_info.quote_asset or "").strip().lower()
        configured_modules = getattr(self.config, "modules", None) or {}
        module_slot_limit = None
        if configured_modules:
            module = self.config.module_for(exchange, payload.pair_info.quote_asset)
            if module is None:
                self._mark_failed(
                    pair_tag,
                    "EXECUTION_MODULE_NOT_CONFIGURED",
                    "Entry rejected because the spot venue/quote is not in the explicit execution-module allowlist.",
                    detail={"exchange": exchange, "quote_asset": payload.pair_info.quote_asset},
                )
                await self.publish_status()
                return
            if not module.enabled:
                self._mark_failed(
                    pair_tag,
                    "EXECUTION_MODULE_DISABLED",
                    "Entry rejected because its configured execution module is disabled.",
                    detail={"module_id": module.id, "exchange": exchange, "quote_asset": payload.pair_info.quote_asset},
                )
                await self.publish_status()
                return
            # A non-empty hedge_venues list is an explicit allowlist. Enforce it
            # before buying spot so a misrouted/alternative futures venue cannot
            # create an unhedged spot leg outside the module's configured scope.
            allowed_hedge_venues = {
                str(venue or "").strip().lower()
                for venue in (getattr(module, "hedge_venues", None) or [])
                if str(venue or "").strip()
            }
            selected_hedge_venue = str(
                payload.pair_info.global_exchange or ""
            ).strip().lower()
            if not allowed_hedge_venues or selected_hedge_venue not in allowed_hedge_venues:
                self._mark_failed(
                    pair_tag,
                    "EXECUTION_HEDGE_VENUE_NOT_CONFIGURED",
                    "Entry rejected because the execution module has no matching futures venue in its explicit hedge allowlist.",
                    detail={
                        "module_id": module.id,
                        "hedge_exchange": selected_hedge_venue,
                        "allowed_hedge_venues": sorted(allowed_hedge_venues),
                    },
                )
                await self.publish_status()
                return
            try:
                module_slot_limit = int(module.max_slots_per_venue)
            except (TypeError, ValueError):
                module_slot_limit = -1
            if module_slot_limit < 0 or module_slot_limit > 100:
                self._mark_failed(
                    pair_tag,
                    "EXECUTION_MODULE_CONFIG_INVALID",
                    "Entry rejected because max_slots_per_venue must be between 0 and 100.",
                    detail={"module_id": module.id, "max_slots_per_venue": module.max_slots_per_venue},
                )
                await self.publish_status()
                return
        if quote_asset in ("bidr", "idr"):
            quote_key = "idr"
        elif quote_asset in ("usdt", "usdc"):
            quote_key = "usdt"
        else:
            quote_key = quote_asset
        scoped_key = f"{exchange}:{quote_key}" if quote_key else exchange

        async with self._spot_exchange_entry_lock(exchange):
            # Check venue-level limit
            current_open = self._spot_open_order_count(exchange)
            max_open = self._spot_order_limit(exchange)
            if module_slot_limit is not None:
                max_open = min(max_open, module_slot_limit)
            if current_open >= max_open:
                logger.info(
                    "[Entry] skipping %s: %s already has %d/%d open spot positions or pending entries",
                    pair_tag, exchange, current_open, max_open,
                )
                return

            # Check quote-scoped limit (e.g. indodax:idr, indodax:usdt)
            if scoped_key != exchange:
                current_scoped = self._spot_open_order_count(scoped_key)
                max_scoped = self._spot_order_limit(scoped_key)
                if current_scoped >= max_scoped:
                    logger.info(
                        "[Entry] skipping %s: %s already has %d/%d open spot positions or pending entries",
                        pair_tag, scoped_key, current_scoped, max_scoped,
                    )
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

            if not await self._validate_hedge_leg_tradable(pair_tag, pair_info):
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

            current = self._spot_open_order_count(exchange)
            maximum = self._spot_order_limit(exchange)
            if module_slot_limit is not None:
                maximum = min(maximum, module_slot_limit)
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
                module_slot_limit=module_slot_limit,
            )

    async def _get_available_spot_quote_balance(self, spot_venue: str) -> float:
        """Fetch available spot quote balance for a venue."""
        venue = str(spot_venue or "").strip().lower()
        resolver = getattr(getattr(self, "client", None), "for_venue", None)
        venue_client = resolver(venue) if callable(resolver) else getattr(self, "client", None)
        fetch_bal = getattr(venue_client, "get_balances", None)
        if callable(fetch_bal):
            bals = fetch_bal()
            if asyncio.iscoroutine(bals):
                bals = await bals
            return float(bals.get("spot_available", 0.0) or 0.0)
        return 0.0

    async def execute_entry_signal(self, signal: dict) -> dict:
        """Entry execution facade with pair lock and advanced spot venue lock."""
        spot_venue = str(signal.get("spot_venue") or signal.get("local_exchange") or "").lower()
        pair_tag = str(signal.get("pair_tag") or signal.get("symbol") or "")

        async with self._pair_lock(pair_tag):
            # Ambil lock bursa Spot SEBELUM fetch balance dan dynamic sizing
            async with self._spot_exchange_entry_lock(spot_venue):
                free_spot_balance = await self._get_available_spot_quote_balance(spot_venue)
                compute_sizing = getattr(self, "_compute_entry_sizing_from_balance", None)
                if callable(compute_sizing):
                    sized_order = await compute_sizing(signal, free_spot_balance)
                    if hasattr(sized_order, "is_viable") and not sized_order.is_viable:
                        return {"status": "SKIPPED_SIZING", "reason": getattr(sized_order, "reason", "not viable")}
                place_legs = getattr(self, "_place_and_monitor_entry_legs", None)
                if callable(place_legs):
                    return await place_legs(signal, locals().get("sized_order"))
                payload = getattr(self, "_signal_payloads", {}).get(pair_tag)
                if payload is None and hasattr(self, "_load_signal_payload"):
                    payload = await self._load_signal_payload(pair_tag)
                if payload is not None:
                    await self._handle_signal_locked(pair_tag, payload)
                    return {"status": "SUBMITTED", "pair_tag": pair_tag}
                return {"status": "SUCCESS", "pair_tag": pair_tag, "free_spot_balance": free_spot_balance}
