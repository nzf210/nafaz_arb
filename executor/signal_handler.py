"""SignalHandlerMixin responsibilities for ArbitrageEngine."""

import asyncio
import json
import logging
import math
import time
from typing import Any, Dict, Optional

from executor.helpers import BALANCES_KEY, build_client_order_id
from executor.models import ExecutorPayload, PositionState

logger = logging.getLogger("executor.engine")


class SignalHandlerMixin:
    async def handle_signal_notification(self, pair_tag: str):
        """
        Serialize balance checks, exposure reservations and order placement per venue.

        The lock is acquired in sorted venue order to avoid deadlocks. A Redis
        distributed lock is mandatory in LIVE mode; local asyncio locks alone
        cannot protect capital when multiple executor replicas are running.
        """
        payload = await self._load_signal_payload(pair_tag)
        if payload is None:
            return
        venues = sorted({
            str(payload.pair_info.local_exchange or "").strip().lower(),
            str(payload.pair_info.global_exchange or "").strip().lower(),
        } - {""})
        local_locks = []
        distributed_locks = []
        try:
            for venue in venues:
                lock = self._venue_locks.setdefault(venue, asyncio.Lock())
                await lock.acquire()
                local_locks.append(lock)

            if not getattr(self.config, "mock_mode", False):
                lock_factory = getattr(self.redis, "lock", None)
                if not callable(lock_factory):
                    raise RuntimeError(
                        "LIVE entry requires Redis distributed locks; refusing unsafe multi-worker entry"
                    )
                for venue in venues:
                    lock = lock_factory(
                        f"arb:executor:entry_lock:{venue}",
                        timeout=300,
                        blocking_timeout=15,
                    )
                    acquired = await lock.acquire()
                    if not acquired:
                        raise RuntimeError(
                            f"could not acquire distributed entry lock for {venue}"
                        )
                    distributed_locks.append(lock)

            await self._refresh_shared_entry_reservations()
            await self._handle_signal_notification_with_venue_locks(
                pair_tag, expected_venues=tuple(venues)
            )
        except Exception as exc:
            logger.exception("[Risk Control] venue entry lock/preflight failed")
            self._mark_failed(
                pair_tag,
                "VENUE_ENTRY_LOCK_FAILED",
                "Entry dihentikan karena penguncian saldo lintas exchange gagal.",
                detail={"venues": venues, "error": str(exc)},
            )
            await self.publish_status()
        finally:
            for lock in reversed(distributed_locks):
                try:
                    await lock.release()
                except Exception:
                    logger.exception(
                        "[Risk Control] failed to release Redis venue entry lock"
                    )
            for lock in reversed(local_locks):
                lock.release()

    async def _handle_signal_notification_with_venue_locks(
        self, pair_tag: str, expected_venues: tuple[str, ...]
    ):
        """
        State 1 (Entry Order Pegging):
        1. GET arb:signal:{local_ex}:{symbol}
        2. Validate timestamp freshness (<= 500ms).
        3. If no active position, place Maker Limit Buy at recommended_limit_buy_idr.
        """
        async with self._pair_lock(pair_tag):
            self._roll_loss_day_if_needed()
            payload = await self._load_signal_payload(pair_tag)
            if payload is None:
                return
            signal_local = str(payload.pair_info.local_exchange or "").strip().lower()
            signal_global = str(payload.pair_info.global_exchange or "").strip().lower()
            if tuple(sorted({signal_local, signal_global} - {""})) != expected_venues:
                logger.warning(
                    "[Risk Control] signal route changed while waiting for venue locks; "
                    "discarding stale preflight"
                )
                return
            if hasattr(self.client, "set_route"):
                try:
                    self.client.set_route(signal_local, signal_global)
                except Exception as exc:
                    self._pair_infos[pair_tag] = payload.pair_info
                    self._mark_failed(
                        pair_tag,
                        "EXECUTOR_VENUE_NOT_CONFIGURED",
                        f"Signal exchange route is not configured: {exc}",
                        detail={
                            "signal_local_exchange": signal_local,
                            "signal_global_exchange": signal_global,
                        },
                    )
                    await self.publish_status()
                    return
            elif (
                not getattr(self.config, "mock_mode", False)
                and (
                    signal_local != str(self.config.local_exchange_name or "").strip().lower()
                    or signal_global != str(self.config.global_exchange_name or "").strip().lower()
                )
            ):
                self._pair_infos[pair_tag] = payload.pair_info
                self._mark_failed(
                    pair_tag,
                    "EXECUTOR_VENUE_MISMATCH",
                    "Signal exchange pair does not match this single-route executor client.",
                    detail={"signal_local_exchange": signal_local, "signal_global_exchange": signal_global},
                )
                await self.publish_status()
                return
            self._pair_infos[pair_tag] = payload.pair_info
            balances = await self._check_balance_mismatch(
                payload.pair_info,
                payload.pricing_snapshot.usdt_idr_rate,
                pair_tag,
            )
            if balances is None and not getattr(self.config, "mock_mode", False):
                return
            if self._trading_halted:
                logger.warning(
                    f"[Circuit Breaker] rejecting entry for {pair_tag}: "
                    f"trading halted ({self._halt_reason})"
                )
                return
            if venue_reason := self._venue_halted_reason(pair_tag):
                logger.warning(
                    f"[Risk Control] rejecting entry for {pair_tag}: "
                    f"exchange halted ({venue_reason})"
                )
                return
            await self._handle_signal_locked(pair_tag, payload)

    def _next_order_seq(self) -> int:
        """Monotonic per-process sequence used to keep client order ids unique."""
        self._order_seq += 1
        return self._order_seq

    async def _load_signal_payload(self, pair_tag: str) -> Optional[ExecutorPayload]:
        """Reads and decodes the pending signal for a pair. None when absent or unparseable."""
        signal_key = f"arb:signal:{pair_tag}"
        raw_signal = await self.redis.get(signal_key)
        if not raw_signal:
            return None
        try:
            return ExecutorPayload.from_dict(json.loads(raw_signal))
        except Exception:
            logger.exception(f"failed to parse signal payload {signal_key}")
            return None

    async def _is_stale_or_duplicate_signal(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        """True when the signal is too old to trust or the pair is already being worked."""
        if payload.pricing_snapshot.is_stale(self.config.max_stale_latency_ms):
            logger.warning(
                f"stale signal rejected for {pair_tag}: latency > {self.config.max_stale_latency_ms}ms"
            )
            return True
        return await self._has_active_or_pending_position(pair_tag)

    async def _has_active_or_pending_position(self, pair_tag: str) -> bool:
        """True when this pair already has a live position or an order in flight."""
        if await self.redis.exists(f"arb:active_pos:{pair_tag}"):
            return True
        return pair_tag in self.active_positions or pair_tag in self.pending_entry_orders

    async def _validate_entry_spread(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        """
        Pre-entry re-validation: re-quotes the global futures book and rejects
        the signal when the entry spread has collapsed below the floor the
        scanner itself used. Guards the sub-second window between the scanner's
        snapshot and the executor's order placement.

        Floor resolution: explicit config ``entry_revalidation_min_spread_pct``
        overrides; otherwise the payload's ``min_entry_spread_pct``. A ticker
        that cannot be read fails open — the scanner already validated the
        quote one scan ago, and refusing every entry on a ticker blip is worse.
        """
        min_spread = float(
            getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0
        )
        if min_spread <= 0:
            min_spread = float(payload.execution_directives.min_entry_spread_pct or 0.0)
        if min_spread <= 0:
            return True

        global_symbol = payload.pair_info.global_symbol
        try:
            ticker = await self.client.fetch_global_ticker(global_symbol)
        except Exception as exc:  # nosec BLE001 - fail open, see docstring
            logger.warning(
                f"[Entry Revalidation] could not read {global_symbol} ticker for {pair_tag}: "
                f"{exc}. Proceeding with the entry."
            )
            return True

        try:
            live_bid = float((ticker or {}).get("bid") or 0.0)
        except (TypeError, ValueError):
            live_bid = 0.0
        if live_bid <= 0:
            logger.info(
                f"[Entry Revalidation] no live bid for {global_symbol}; "
                f"skipping spread check on {pair_tag}"
            )
            return True

        snapshot = payload.pricing_snapshot
        if snapshot.global_best_bid_usdt <= 0 or snapshot.usdt_idr_rate <= 0:
            return True
        live_bid_idr = live_bid * snapshot.usdt_idr_rate
        buy_price_idr = payload.execution_directives.recommended_limit_buy_idr
        if buy_price_idr <= 0 or live_bid_idr <= 0:
            return True

        live_spread_pct = ((live_bid_idr - buy_price_idr) / live_bid_idr) * 100.0
        if live_spread_pct < min_spread:
            logger.info(
                f"[Entry Revalidation] rejecting {pair_tag}: live spread "
                f"{live_spread_pct:.3f}% < floor {min_spread:.3f}% "
                f"(live bid {live_bid}, snapshot bid {snapshot.global_best_bid_usdt})"
            )
            return False
        return True

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
        pair_info = payload.pair_info
        snapshot = payload.pricing_snapshot

        # Target Limit Buy price in IDR
        buy_price_idr = directives.recommended_limit_buy_idr
        if buy_price_idr <= 0:
            return

        sizing = await self._compute_entry_sizing(pair_tag, payload)
        if sizing is None:
            return
        max_pos_usdt, _ = sizing

        capital_idr = max_pos_usdt * snapshot.usdt_idr_rate
        buy_qty = capital_idr / buy_price_idr
        if pair_info.local_lot_size > 0:
            buy_qty = math.floor(buy_qty / pair_info.local_lot_size) * pair_info.local_lot_size

        if buy_qty <= 0:
            return

        actual_cost_usdt = (buy_qty * buy_price_idr) / snapshot.usdt_idr_rate

        # Lock funds in exchange client
        if hasattr(self.client, "lock_funds"):
            if not self.client.lock_funds(actual_cost_usdt, actual_cost_usdt):
                logger.warning(
                    f"[State 1 Balance Check] Failed to lock funds for {actual_cost_usdt:.2f} USDT on {pair_tag}"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "BALANCE_LOCK_FAILED",
                    "Exchange client gagal mengunci saldo untuk entry.",
                    detail={"required_usdt_per_leg": actual_cost_usdt},
                )
                return
            # Sync balances to Redis
            bals = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                pair_info=pair_info,
                usdt_idr_rate=snapshot.usdt_idr_rate,
            )
            if bals is not None:
                await self.redis.set(BALANCES_KEY, json.dumps(bals))

        try:
            logger.info(
                f"[State 1] Placing Maker Limit Buy on {pair_info.local_exchange} for {pair_info.local_symbol}: "
                f"qty={buy_qty}, price_idr={buy_price_idr}, cost_usd=${actual_cost_usdt:.2f}"
            )
            entry_seq = self._next_order_seq()
            entry_coid = build_client_order_id(pair_tag, "entry", entry_seq)
            order = await self.client.place_local_limit_buy(
                pair_info.local_symbol, buy_qty, buy_price_idr, client_order_id=entry_coid
            )
            order_id = str(order.get("id", ""))
            self._record_order_success(pair_info.local_exchange)
            self.supabase.order(
                pair_tag,
                "spot_entry",
                pair_info.local_exchange,
                "spot",
                pair_info.local_symbol,
                order,
                client_order_id=entry_coid,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            self.pending_entry_orders[pair_tag] = {
                "order_id": order_id,
                "client_order_id": entry_coid,
                "symbol": pair_info.local_symbol,
                "global_symbol": pair_info.global_symbol,
                "local_exchange": pair_info.local_exchange,
                "global_exchange": pair_info.global_exchange,
                "price": buy_price_idr,
                "qty": buy_qty,
                "filled": 0.0,
                "hedged_short_qty": 0.0,
                "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                "tick_size": pair_info.local_price_tick,
                "quote_asset": pair_info.quote_asset,
                "usdt_idr_rate": snapshot.usdt_idr_rate,
                "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                "global_ask_usdt": snapshot.global_best_ask_usdt,
                "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                "current_funding_rate": payload.funding_metrics.current_funding_rate,
                "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                "placed_time": time.time(),
                "spot_cost_usdt": actual_cost_usdt,
                "futures_margin_usdt": actual_cost_usdt,
            }
            await self._persist_pending_entry(pair_tag, self.pending_entry_orders[pair_tag])
        except Exception:
            if hasattr(self.client, "unlock_funds"):
                self.client.unlock_funds(actual_cost_usdt, actual_cost_usdt)
            self._record_order_failure(pair_info.local_exchange, "entry order placement failed")
            logger.exception(f"[State 1] failed to place limit buy for {pair_tag}")
            await self._publish_execution_failure(
                pair_tag,
                "SPOT_ORDER_OPEN_FAILED",
                "Gagal membuka order Spot; periksa error exchange dan saldo.",
                detail={"exchange": pair_info.local_exchange, "symbol": pair_info.local_symbol},
            )
    # -------------------------------------------------------------
    # Failure tracking & status reporting
    # -------------------------------------------------------------

