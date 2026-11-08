"""SignalHandlerMixin responsibilities for ArbitrageEngine."""

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from executor.helpers import BALANCES_KEY, build_client_order_id
from executor.models import ExecutorPayload, PositionState

logger = logging.getLogger("executor.engine")


@dataclass
class EntryOrderPlan:
    pair_info: Any
    directives: Any
    snapshot: Any
    buy_price_idr: float
    buy_qty: float
    actual_cost_usdt: float


class SignalHandlerMixin:
    async def handle_signal_notification(self, pair_tag: str):
        payload = await self._load_signal_payload(pair_tag)
        if payload is None:
            return
        venues = self._signal_venues(payload)
        local_locks = []
        distributed_locks = []
        try:
            await self._acquire_local_venue_locks(venues, local_locks)
            if not getattr(self.config, "mock_mode", False):
                await self._acquire_distributed_venue_locks(venues, distributed_locks)
            await self._refresh_shared_entry_reservations()
            await self._handle_signal_notification_with_venue_locks(
                pair_tag, expected_venues=tuple(venues)
            )
        except Exception as exc:
            await self._handle_venue_lock_failure(pair_tag, venues, exc)
        finally:
            await self._release_venue_locks(distributed_locks, local_locks)

    @staticmethod
    def _signal_venues(payload: ExecutorPayload) -> list[str]:
        return sorted({
            str(payload.pair_info.local_exchange or "").strip().lower(),
            str(payload.pair_info.global_exchange or "").strip().lower(),
        } - {""})

    async def _acquire_local_venue_locks(self, venues: list[str], acquired: list) -> None:
        for venue in venues:
            lock = self._venue_locks.setdefault(venue, asyncio.Lock())
            await lock.acquire()
            acquired.append(lock)

    async def _acquire_distributed_venue_locks(self, venues: list[str], acquired: list) -> None:
        lock_factory = getattr(self.redis, "lock", None)
        if not callable(lock_factory):
            raise RuntimeError("LIVE entry requires Redis distributed locks; refusing unsafe multi-worker entry")
        for venue in venues:
            lock = lock_factory(
                f"arb:executor:entry_lock:{venue}", timeout=300, blocking_timeout=15
            )
            if not await lock.acquire():
                raise RuntimeError(f"could not acquire distributed entry lock for {venue}")
            acquired.append(lock)

    async def _release_venue_locks(self, distributed_locks: list, local_locks: list) -> None:
        for lock in reversed(distributed_locks):
            try:
                await lock.release()
            except Exception:
                logger.exception("[Risk Control] failed to release Redis venue entry lock")
        for lock in reversed(local_locks):
            lock.release()

    async def _handle_venue_lock_failure(self, pair_tag: str, venues: list[str], error: Exception) -> None:
        logger.exception("[Risk Control] venue entry lock/preflight failed")
        self._mark_failed(
            pair_tag, "VENUE_ENTRY_LOCK_FAILED",
            "Entry dihentikan karena penguncian saldo lintas exchange gagal.",
            detail={"venues": venues, "error": str(error)},
        )
        await self.publish_status()

    async def _handle_signal_notification_with_venue_locks(
        self, pair_tag: str, expected_venues: tuple[str, ...]
    ):
        async with self._pair_lock(pair_tag):
            self._roll_loss_day_if_needed()
            payload = await self._load_signal_payload(pair_tag)
            if payload is None:
                return
            if not self._signal_route_matches(payload, expected_venues):
                logger.warning("[Risk Control] signal route changed while waiting for venue locks; discarding stale preflight")
                return
            if not await self._configure_signal_route(pair_tag, payload):
                return
            self._pair_infos[pair_tag] = payload.pair_info
            if not await self._preflight_signal_balance(pair_tag, payload):
                return
            if self._trading_halted:
                logger.warning("[Circuit Breaker] rejecting entry for %s: trading halted (%s)", pair_tag, self._halt_reason)
                return
            if venue_reason := self._venue_halted_reason(pair_tag):
                logger.warning("[Risk Control] rejecting entry for %s: exchange halted (%s)", pair_tag, venue_reason)
                return
            await self._handle_signal_locked(pair_tag, payload)

    def _signal_route_matches(self, payload: ExecutorPayload, expected_venues: tuple[str, ...]) -> bool:
        return tuple(sorted(set(self._signal_venues(payload)))) == expected_venues

    async def _configure_signal_route(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        local = str(payload.pair_info.local_exchange or "").strip().lower()
        global_venue = str(payload.pair_info.global_exchange or "").strip().lower()
        if hasattr(self.client, "set_route"):
            try:
                self.client.set_route(local, global_venue)
                return True
            except Exception as exc:
                self._pair_infos[pair_tag] = payload.pair_info
                self._mark_failed(
                    pair_tag, "EXECUTOR_VENUE_NOT_CONFIGURED",
                    f"Signal exchange route is not configured: {exc}",
                    detail={"signal_local_exchange": local, "signal_global_exchange": global_venue},
                )
                await self.publish_status()
                return False
        if getattr(self.config, "mock_mode", False):
            return True
        expected_local = str(self.config.local_exchange_name or "").strip().lower()
        expected_global = str(self.config.global_exchange_name or "").strip().lower()
        if local == expected_local and global_venue == expected_global:
            return True
        self._pair_infos[pair_tag] = payload.pair_info
        self._mark_failed(
            pair_tag, "EXECUTOR_VENUE_MISMATCH",
            "Signal exchange pair does not match this single-route executor client.",
            detail={"signal_local_exchange": local, "signal_global_exchange": global_venue},
        )
        await self.publish_status()
        return False

    async def _preflight_signal_balance(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        balances = await self._check_balance_mismatch(
            payload.pair_info, payload.pricing_snapshot.usdt_idr_rate, pair_tag
        )
        return balances is not None or getattr(self.config, "mock_mode", False)

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

    def _minimum_entry_spread_pct(self, payload: ExecutorPayload) -> float:
        configured = float(getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0)
        if configured > 0:
            return configured
        return float(payload.execution_directives.min_entry_spread_pct or 0.0)

    async def _fetch_revalidation_bid(self, pair_tag: str, global_symbol: str) -> Optional[float]:
        try:
            ticker = await self.client.fetch_global_ticker(global_symbol)
        except Exception as exc:  # nosec BLE001 - fail open by design
            logger.warning(
                "[Entry Revalidation] could not read %s ticker for %s: %s. Proceeding with entry.",
                global_symbol, pair_tag, exc,
            )
            return None
        try:
            bid = float((ticker or {}).get("bid") or 0.0)
        except (TypeError, ValueError):
            return None
        return bid if bid > 0 else None

    def _calculate_revalidated_spread(self, payload: ExecutorPayload, live_bid: float) -> Optional[float]:
        snapshot = payload.pricing_snapshot
        if snapshot.global_best_bid_usdt <= 0 or snapshot.usdt_idr_rate <= 0:
            return None
        buy_price = payload.execution_directives.recommended_limit_buy_idr
        live_bid_idr = live_bid * snapshot.usdt_idr_rate
        if buy_price <= 0 or live_bid_idr <= 0:
            return None
        return ((live_bid_idr - buy_price) / live_bid_idr) * 100.0

    async def _validate_entry_spread(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        min_spread = self._minimum_entry_spread_pct(payload)
        if min_spread <= 0:
            return True
        live_bid = await self._fetch_revalidation_bid(pair_tag, payload.pair_info.global_symbol)
        if live_bid is None:
            return True
        live_spread = self._calculate_revalidated_spread(payload, live_bid)
        if live_spread is None:
            return True
        if live_spread < min_spread:
            logger.info(
                "[Entry Revalidation] rejecting %s: live spread %.3f%% < floor %.3f%%",
                pair_tag, live_spread, min_spread,
            )
            return False
        return True

    def _validate_signal_fx(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        quote_asset = str(payload.pair_info.quote_asset or "").upper()
        fx_rate = float(payload.pricing_snapshot.usdt_idr_rate or 0.0)
        if math.isfinite(fx_rate) and fx_rate > 0:
            return True
        logger.error("[FX Preflight] rejecting %s: live USDT/IDR rate is missing or invalid", pair_tag)
        self._mark_failed(
            pair_tag,
            "LIVE_FX_RATE_INVALID",
            "Entry pricing and sizing require a finite, positive live USDT/IDR rate.",
            detail={"quote_asset": quote_asset, "usdt_idr_rate": fx_rate},
        )
        return False

    async def _prepare_entry_order_plan(self, pair_tag: str, payload: ExecutorPayload) -> Optional[EntryOrderPlan]:
        if await self._is_stale_or_duplicate_signal(pair_tag, payload):
            return None
        if not await self._validate_entry_spread(pair_tag, payload):
            return None
        pair_info = payload.pair_info
        directives = payload.execution_directives
        snapshot = payload.pricing_snapshot
        buy_price = float(directives.recommended_limit_buy_idr or 0.0)
        if buy_price <= 0:
            return None
        sizing = await self._compute_entry_sizing(pair_tag, payload)
        if sizing is None:
            return None
        max_position_usdt, _ = sizing
        capital_idr = max_position_usdt * snapshot.usdt_idr_rate
        buy_qty = capital_idr / buy_price
        if pair_info.local_lot_size > 0:
            buy_qty = math.floor(buy_qty / pair_info.local_lot_size) * pair_info.local_lot_size
        if buy_qty <= 0:
            return None
        actual_cost_usdt = (buy_qty * buy_price) / snapshot.usdt_idr_rate
        return EntryOrderPlan(pair_info, directives, snapshot, buy_price, buy_qty, actual_cost_usdt)

    async def _lock_entry_funds(self, pair_tag: str, plan: EntryOrderPlan) -> bool:
        if not hasattr(self.client, "lock_funds"):
            return True
        if not self.client.lock_funds(plan.actual_cost_usdt, plan.actual_cost_usdt):
            logger.warning(
                "[State 1 Balance Check] Failed to lock funds for %.2f USDT on %s",
                plan.actual_cost_usdt, pair_tag,
            )
            await self._publish_execution_failure(
                pair_tag, "BALANCE_LOCK_FAILED",
                "Exchange client gagal mengunci saldo untuk entry.",
                detail={"required_usdt_per_leg": plan.actual_cost_usdt},
            )
            return False
        balances = await self._get_balance_snapshot(
            pair_tag=pair_tag, pair_info=plan.pair_info,
            usdt_idr_rate=plan.snapshot.usdt_idr_rate,
        )
        if balances is not None:
            await self.redis.set(BALANCES_KEY, json.dumps(balances))
        return True

    async def _place_entry_order(self, pair_tag: str, payload: ExecutorPayload, plan: EntryOrderPlan) -> None:
        pair_info = plan.pair_info
        try:
            logger.info(
                "[State 1] Placing Maker Limit Buy on %s for %s: qty=%s, price_idr=%s, cost_usd=$%.2f",
                pair_info.local_exchange, pair_info.local_symbol, plan.buy_qty,
                plan.buy_price_idr, plan.actual_cost_usdt,
            )
            client_order_id = build_client_order_id(pair_tag, "entry", self._next_order_seq())
            order = await self.client.place_local_limit_buy(
                pair_info.local_symbol, plan.buy_qty, plan.buy_price_idr,
                client_order_id=client_order_id,
            )
            order_id = str(order.get("id", ""))
            self._record_order_success(pair_info.local_exchange)
            self.supabase.order(
                pair_tag, "spot_entry", pair_info.local_exchange, "spot",
                pair_info.local_symbol, order, client_order_id=client_order_id,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            pending = self._build_pending_entry_record(pair_tag, payload, plan, order_id, client_order_id)
            self.pending_entry_orders[pair_tag] = pending
            await self._persist_pending_entry(pair_tag, pending)
        except Exception:
            if hasattr(self.client, "unlock_funds"):
                self.client.unlock_funds(plan.actual_cost_usdt, plan.actual_cost_usdt)
            self._record_order_failure(pair_info.local_exchange, "entry order placement failed")
            logger.exception("[State 1] failed to place limit buy for %s", pair_tag)
            await self._publish_execution_failure(
                pair_tag, "SPOT_ORDER_OPEN_FAILED",
                "Gagal membuka order Spot; periksa error exchange dan saldo.",
                detail={"exchange": pair_info.local_exchange, "symbol": pair_info.local_symbol},
            )

    def _build_pending_entry_record(self, pair_tag: str, payload: ExecutorPayload, plan: EntryOrderPlan, order_id: str, client_order_id: str) -> dict:
        pair_info = plan.pair_info
        directives = plan.directives
        snapshot = plan.snapshot
        return {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "symbol": pair_info.local_symbol,
            "global_symbol": pair_info.global_symbol,
            "local_exchange": pair_info.local_exchange,
            "global_exchange": pair_info.global_exchange,
            "price": plan.buy_price_idr,
            "qty": plan.buy_qty,
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
            "spot_cost_usdt": plan.actual_cost_usdt,
            "futures_margin_usdt": plan.actual_cost_usdt,
        }

    async def _handle_signal_locked(
        self, pair_tag: str, payload: Optional[ExecutorPayload] = None
    ):
        if payload is None:
            payload = await self._load_signal_payload(pair_tag)
        if payload is None or not self._validate_signal_fx(pair_tag, payload):
            return
        self._pair_infos[pair_tag] = payload.pair_info
        plan = await self._prepare_entry_order_plan(pair_tag, payload)
        if plan is None:
            return
        if not await self._lock_entry_funds(pair_tag, plan):
            return
        await self._place_entry_order(pair_tag, payload, plan)

    # -------------------------------------------------------------
    # Failure tracking & status reporting
    # -------------------------------------------------------------

