import asyncio
import json
import logging
import math
import time
from typing import Optional, Dict, Any

import redis.asyncio as aioredis
from executor.models import (
    PositionState,
    ExecutorPayload,
    ActivePosition,
)
from executor.exchange_client import ExchangeClient
from executor.config import ExecutorConfig
from executor.private_stream import PrivateStreamCache

# Module-level helpers live in executor.helpers; re-exported here so existing
# callers (tests, simulator, main) keep importing them from executor.engine.
from executor.helpers import (  # noqa: F401
    ARB_TRADE_HISTORY_KEY,
    ARB_TRADE_HISTORY_MAX_LEN,
    BALANCES_KEY,
    PENDING_ENTRY_KEY_PREFIX,
    HedgeSlippageExceeded,
    RetryExhausted,
    build_client_order_id,
    ceil_to_tick,
    clean_pair_key,
    floor_to_tick,
    retry_with_backoff,
    round_to_tick,
)
from executor import helpers
from executor.cancel import CancelMixin
from executor.entry import EntryMixin
from executor.exits import ExitMixin
from executor.force_close import ForceCloseMixin
from executor.risk import RiskMixin
from executor.rollback import RollbackMixin

logger = logging.getLogger("executor.engine")


class ArbitrageEngine(
    CancelMixin,
    EntryMixin,
    ExitMixin,
    ForceCloseMixin,
    RollbackMixin,
    RiskMixin,
):
    """
    Hybrid Maker-Taker + Positive Funding Rate Arbitrage Engine.
    Implements 4 States with non-blocking CCXT + redis.asyncio.
    """

    # Shared with helper modules; kept as a class attribute so existing
    # self.<NAME> call sites keep working.
    FALLBACK_USDT_IDR_RATE = helpers.FALLBACK_USDT_IDR_RATE
    LIVE_PRICE_STALE_MS = helpers.LIVE_PRICE_STALE_MS

    def __init__(
        self,
        config: ExecutorConfig,
        client: ExchangeClient,
        redis_client: Optional[aioredis.Redis] = None,
    ):
        self.config = config
        self.client = client
        self.redis = redis_client
        self._running = False
        self.ARB_TRADE_EVENTS = "arb:trade_events"
        # Seeded from wall-clock so client order ids stay unique across restarts;
        # a zero-based counter could regenerate ids used by a previous process.
        self._order_seq = int(time.time()) % 1_000_000_000
        # Populated only when private_stream_enabled is set; None keeps the
        # engine on pure REST polling.
        self.private_stream: Optional[PrivateStreamCache] = None
        if getattr(config, "private_stream_enabled", False):
            self.private_stream = PrivateStreamCache(
                max_age_seconds=getattr(config, "private_stream_max_age_seconds", 5.0)
            )

        # Active state tracking per pair_key
        self.states: Dict[str, PositionState] = {}
        self.active_positions: Dict[str, ActivePosition] = {}
        self.pending_entry_orders: Dict[str, Dict[str, Any]] = {}
        self.pending_exit_orders: Dict[str, Dict[str, Any]] = {}
        self.last_pegged_global_ask: Dict[str, float] = {}

        # Serializes entry/hedge work per pair. Without it two signals arriving
        # together both pass the "no active position" check between the Redis
        # read and the order placement, producing a doubled spot position.
        self._pair_locks: Dict[str, asyncio.Lock] = {}

        # Last failure per pair, surfaced through arb:executor:status so an
        # operator can query why a pair stopped instead of scraping logs.
        self._failure_reasons: Dict[str, Dict[str, Any]] = {}
        self._last_status_publish: float = 0.0
        # Funding settlement reconciliation is rate-limited per pair so a delayed
        # exchange history endpoint is not polled on every 50ms monitoring tick.
        self._funding_reconcile_retry_after_ms: Dict[str, int] = {}

        # Daily loss circuit breaker (UTC day boundary).
        self._daily_realized_loss_usdt: float = 0.0
        self._loss_day: str = self._utc_day()
        self._trading_halted: bool = False
        self._halt_reason: str = ""

        # Risk-control: halts keyed by exchange name (venue-level, not global).
        self._exchange_halted: Dict[str, str] = {}
        self._consecutive_order_failures: Dict[str, int] = {}
        # Last observed available balances per venue leg, for mismatch detection.
        self._last_balances: Dict[str, float] = {}
        # Balance snapshot fetched once per signal; reused by entry sizing so the
        # venue is checked even when the signal is a duplicate and no order follows.
        self._balance_snapshots: Dict[str, Dict[str, Any]] = {}
        # Venue names seen from recent payloads, for pair-tag -> exchange mapping.
        self._pair_infos: Dict[str, Any] = {}

        # Tasks
        self._tasks = []

    async def initialize(self):
        """Initializes Redis connection and restores in-flight state from Redis."""
        if self.redis is None:
            self.redis = aioredis.from_url(
                self.config.redis_url,
                decode_responses=True,
            )
        await self._recover_active_positions()
        await self._recover_pending_entry_orders()
        if not getattr(self.config, "mock_mode", False):
            await self._verify_recovered_active_positions()

    async def _verify_recovered_active_positions(self) -> None:
        """
        Verifies persisted live positions against the exchange after restart.
        Any unknown or mismatched exposure is retained and blocked for manual
        reconciliation; Redis snapshots alone are not proof of an open hedge.
        """
        if not self.active_positions:
            return
        if not hasattr(self.client, "fetch_positions"):
            logger.critical(
                "[Recovery] live client cannot fetch futures positions; "
                "all restored positions require manual reconciliation"
            )
            groups = {}
            for pair_tag, pos in self.active_positions.items():
                groups.setdefault(self._resolve_global_symbol(pos.symbol), []).append((pair_tag, pos))
            for symbol, items in groups.items():
                for pair_tag, pos in items:
                    pos.state = "RECONCILIATION_REQUIRED"
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    await self.redis.set(
                        f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                    )
                    self._mark_failed(
                        pair_tag,
                        "RECOVERY_POSITION_STATE_UNKNOWN",
                        "Live futures position API is unavailable after restart.",
                        detail={"global_symbol": symbol, "expected_qty": pos.qty},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
            return

        groups = {}
        for pair_tag, pos in self.active_positions.items():
            global_symbol = self._resolve_global_symbol(pos.symbol)
            groups.setdefault(global_symbol, []).append((pair_tag, pos))

        for global_symbol, items in groups.items():
            expected_qty = sum(pos.qty for _, pos in items)
            try:
                positions = await self.client.fetch_positions(global_symbol)
                if positions is None:
                    raise ValueError("fetch_positions returned None")
                actual_short_qty = 0.0
                for position in positions:
                    if not isinstance(position, dict):
                        raise ValueError("invalid position payload")
                    info = position.get("info") or {}
                    side = str(
                        position.get("side")
                        or position.get("positionSide")
                        or info.get("posSide")
                        or info.get("positionSide")
                        or ""
                    ).lower()
                    raw_contracts = position.get("contracts")
                    if raw_contracts is not None:
                        size = abs(float(raw_contracts)) * abs(
                            float(position.get("contractSize") or 1.0)
                        )
                        if size <= 1e-12:
                            continue
                        if side != "short":
                            raise ValueError(
                                f"unexpected non-short futures exposure (side={side or 'unknown'})"
                            )
                        actual_short_qty += size
                        continue

                    raw_amount = position.get("positionAmt", info.get("positionAmt"))
                    if raw_amount is None:
                        raise ValueError("position size unavailable")
                    signed_amount = float(raw_amount)
                    if abs(signed_amount) <= 1e-12:
                        continue
                    if signed_amount >= 0:
                        raise ValueError("unexpected long or unsigned futures exposure")
                    actual_short_qty += abs(signed_amount)
            except Exception as exc:
                actual_short_qty = None
                reason = f"cannot verify futures position after restart: {exc}"
            else:
                tolerance = max(1e-10, expected_qty * 1e-4)
                if abs(actual_short_qty - expected_qty) <= tolerance:
                    logger.info(
                        f"[Recovery] verified {global_symbol} short quantity "
                        f"{actual_short_qty} against Redis snapshot {expected_qty}"
                    )
                    continue
                reason = (
                    f"futures quantity mismatch after restart: expected={expected_qty}, "
                    f"exchange={actual_short_qty}"
                )

            logger.critical(
                f"[Recovery] {global_symbol}: {reason}; restored positions blocked"
            )
            for pair_tag, pos in items:
                pos.state = "RECONCILIATION_REQUIRED"
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                await self.redis.set(
                    f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                )
                self._mark_failed(
                    pair_tag,
                    "RECOVERY_POSITION_MISMATCH",
                    reason,
                    detail={
                        "global_symbol": global_symbol,
                        "expected_qty": pos.qty,
                        "exchange_total_short_qty": actual_short_qty,
                    },
                    state=PositionState.RECONCILIATION_REQUIRED,
                )

    async def _persist_pending_entry(self, pair_tag: str, order_info: dict) -> None:
        """Persists a pending entry order so a restart can adopt or cancel it."""
        if not self.redis:
            return
        try:
            await self.redis.set(
                PENDING_ENTRY_KEY_PREFIX + pair_tag, json.dumps(order_info)
            )
        except Exception:
            logger.exception(f"[Recovery] failed to persist pending entry for {pair_tag}")

    async def _clear_pending_entry(self, pair_tag: str) -> None:
        """Removes the persisted pending-entry record once the order is resolved."""
        if not self.redis:
            return
        try:
            await self.redis.delete(PENDING_ENTRY_KEY_PREFIX + pair_tag)
        except Exception:
            logger.exception(f"[Recovery] failed to clear pending entry for {pair_tag}")

    async def _recover_pending_entry_orders(self) -> None:
        """
        Reconciles persisted pending entry orders against the local exchange.

        A crash between placing the limit buy and registering it in memory
        forgets the resting order: it can fill with no hedge ever placed. On
        startup, orders still open on the venue are re-adopted; records whose
        order no longer exists are cleared.
        """
        try:
            keys = await self.redis.keys(PENDING_ENTRY_KEY_PREFIX + "*")
        except Exception:
            logger.exception("[Recovery] failed to scan arb:pending_entry:* keys")
            return
        for key in keys or []:
            pair_tag = key.split(PENDING_ENTRY_KEY_PREFIX, 1)[-1]
            try:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                order_info = json.loads(raw)
                order_id = str(order_info.get("order_id", ""))
                symbol = str(order_info.get("symbol", ""))
                if not order_id or not symbol:
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._mark_failed(
                        pair_tag,
                        "ENTRY_ORDER_RECORD_INVALID",
                        "Persisted pending entry is missing order_id or symbol; record retained.",
                        detail={"order_id": order_id, "symbol": symbol},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
                    logger.critical(
                        f"[Recovery] malformed pending entry record for {pair_tag}; "
                        "Redis record retained for manual reconciliation"
                    )
                    continue
                status, filled = await self._resolve_venue_order_state(order_id, symbol)
                terminal_statuses = ("closed", "canceled", "cancelled", "rejected", "expired")
                if status == "unknown":
                    # Missing/ambiguous venue state is not proof that an order is gone.
                    # Keep both durable and in-memory records so force-close cannot
                    # silently skip an order whose venue state is unknown.
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._mark_failed(
                        pair_tag,
                        "ENTRY_ORDER_STATE_UNKNOWN",
                        f"Cannot verify local order {order_id}; pending record retained.",
                        detail={"order_id": order_id, "symbol": symbol},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
                    logger.critical(
                        f"[Recovery] cannot verify venue state of order {order_id} "
                        f"({pair_tag}) - MANUAL CHECK REQUIRED"
                    )
                    continue

                order_info["filled"] = float(filled or 0.0)
                if status == "open":
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = (
                        PositionState.ENTRY_PARTIAL
                        if order_info["filled"] > 0
                        else PositionState.ENTRY_PENDING
                    )
                    # Keep the Redis key: in-memory adoption alone is not restart-safe.
                    await self._persist_pending_entry(pair_tag, order_info)
                    logger.warning(
                        f"[Recovery] re-adopted pending entry order {order_id} "
                        f"for {pair_tag} (filled={filled})"
                    )
                    continue

                if status in terminal_statuses:
                    # A terminal order can still contain fills that occurred before
                    # cancellation or before the process crashed. Hedge that final
                    # cumulative fill before releasing the unused reservation.
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.ENTRY_PENDING
                    await self._handle_entry_fills(
                        pair_tag,
                        order_info,
                        {"id": order_id, "status": status, "filled": order_info["filled"]},
                    )
                    if self.states.get(pair_tag) in (
                        PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
                    ):
                        logger.critical(
                            f"[Recovery] fill reconciliation failed for {pair_tag}; "
                            "pending record retained"
                        )
                        continue
                    await self._finalize_entry_order(pair_tag, status)
                    logger.info(
                        f"[Recovery] reconciled terminal order {order_id} for "
                        f"{pair_tag} (status={status}, filled={filled})"
                    )
                    continue

                self.pending_entry_orders[pair_tag] = order_info
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                self._mark_failed(
                    pair_tag,
                    "ENTRY_ORDER_STATE_UNSUPPORTED",
                    f"Unsupported exchange order status '{status}' for {order_id}.",
                    detail={"order_id": order_id, "symbol": symbol, "status": status},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                logger.critical(
                    f"[Recovery] unsupported order status '{status}' for {order_id}; "
                    "pending record retained"
                )
            except Exception:
                logger.exception(f"[Recovery] failed to restore pending entry from {key}")

    async def _resolve_venue_order_state(self, order_id: str, symbol: str) -> tuple:
        """Returns (status, filled) for a venue order; ('unknown', None) if it cannot be fetched."""
        try:
            order = await self.client.fetch_local_order(order_id, symbol)
        except Exception as exc:
            logger.warning(f"[Recovery] fetch order {order_id} failed: {exc}")
            return "unknown", None
        if not isinstance(order, dict) or not order.get("id") and order_id not in str(order):
            # A missing response is ambiguous (history retention, API lag, or
            # wrong symbol); never interpret it as a safely canceled order.
            return "unknown", None
        return str(order.get("status", "unknown")), float(order.get("filled", 0.0) or 0.0)

    async def _recover_active_positions(self) -> None:
        """
        Restores open positions from Redis after a restart/redeploy.

        Without this the engine forgets every hedged position it still holds on
        the exchange: no exit pegging, no funding accrual, and no force-close
        target. Recovered positions resume as HEDGED_ACTIVE so the monitoring
        loop immediately resumes managing them.
        """
        recovered = 0
        try:
            keys = await self.redis.keys("arb:active_pos:*")
        except Exception:
            logger.exception("[Recovery] failed to scan arb:active_pos:* keys")
            return

        for key in keys or []:
            pair_tag = key.split("arb:active_pos:", 1)[-1]
            try:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                pos = ActivePosition.from_dict(json.loads(raw))
                if pos.qty <= 0:
                    # A zero/invalid Redis quantity is not proof that the exchange
                    # is flat. Retain the record and require explicit reconciliation.
                    self.active_positions[pair_tag] = pos
                    pos.state = "RECONCILIATION_REQUIRED"
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    await self.redis.set(key, json.dumps(pos.to_dict()))
                    self._mark_failed(
                        pair_tag,
                        "RECOVERED_POSITION_QTY_INVALID",
                        f"Recovered position quantity is invalid ({pos.qty}); exchange exposure must be checked.",
                        detail={"qty": pos.qty, "symbol": pos.symbol},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
                    logger.critical(
                        f"[Recovery] retaining {pair_tag}: recovered qty is {pos.qty}; "
                        "manual reconciliation required"
                    )
                    continue

                self.active_positions[pair_tag] = pos
                pos_state_str = getattr(pos, "state", "HEDGED_ACTIVE")
                if pos_state_str == "RECONCILIATION_REQUIRED":
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._failure_reasons[pair_tag] = {
                        "reason": "RESTORED_RECONCILIATION_REQUIRED",
                        "message": "Position restored from Redis with RECONCILIATION_REQUIRED state. Manual intervention required.",
                        "detail": {"qty": pos.qty, "symbol": pos.symbol},
                        "timestamp_ms": int(time.time() * 1000),
                    }
                    logger.critical(
                        f"[Recovery] Position {pair_tag} has RECONCILIATION_REQUIRED status! "
                        f"Manual intervention required; skipping automatic active exit management."
                    )
                else:
                    self.states[pair_tag] = PositionState.HEDGED_ACTIVE
                recovered += 1
                logger.warning(
                    f"[Recovery] Restored position {pair_tag}: qty={pos.qty}, "
                    f"entry_idr={pos.entry_price_idr}, short_entry_usdt={pos.short_entry_usdt}, "
                    f"state={self.states[pair_tag].value}"
                )
            except Exception as exc:
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                self._mark_failed(
                    pair_tag,
                    "ACTIVE_POSITION_RECORD_INVALID",
                    f"Persisted active position cannot be parsed: {exc}",
                    detail={"redis_key": key},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                logger.exception(
                    f"[Recovery] invalid active position record {key}; Redis key retained"
                )

        if recovered:
            logger.warning(
                f"[Recovery] {recovered} active position(s) restored - resuming management"
            )

    async def start(self):
        """Starts listeners and monitoring loops."""
        await self.initialize()
        self._running = True

        self._tasks.append(asyncio.create_task(self._listen_signals()))
        self._tasks.append(asyncio.create_task(self._listen_emergency_cancel()))
        self._tasks.append(asyncio.create_task(self._listen_force_close()))
        self._tasks.append(asyncio.create_task(self._listen_kill_switch()))
        self._tasks.append(asyncio.create_task(self._monitoring_loop()))
        logger.info("Hybrid Arbitrage Engine started")

    async def stop(self):
        """Stops all running tasks and connections."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.client:
            await self.client.close()
        if self.redis:
            if hasattr(self.redis, "aclose"):
                await self.redis.aclose()
            else:
                await self.redis.close()
        logger.info("Hybrid Arbitrage Engine stopped")

    # -------------------------------------------------------------
    # Redis Pub/Sub Listeners
    # -------------------------------------------------------------

    async def _listen_signals(self):
        """Subscribes to arb:signal_notify channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:signal_notify")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    pair_tag = message.get("data")
                    if isinstance(pair_tag, bytes):
                        pair_tag = pair_tag.decode("utf-8")
                    if pair_tag:
                        await self.handle_signal_notification(pair_tag)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:signal_notify")
            await pubsub.close()

    async def _listen_emergency_cancel(self):
        """Subscribes to arb:emergency_cancel channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:emergency_cancel")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.handle_emergency_cancel(data)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:emergency_cancel")
            await pubsub.close()

    async def _listen_force_close(self):
        """Subscribes to arb:force_close channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:force_close")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.handle_force_close(data)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:force_close")
            await pubsub.close()

    async def _listen_kill_switch(self):
        """Subscribes to arb:kill_switch channel (operator ON/OFF switch)."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:kill_switch")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.handle_kill_switch(data)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:kill_switch")
            await pubsub.close()

    async def handle_kill_switch(self, message_data: str) -> None:
        """
        Handles the operator kill switch on arb:kill_switch.

        ON halts every new entry globally until an explicit OFF is received.
        Unlike the daily-loss breaker the kill switch does NOT auto-reset on a
        UTC rollover - only an operator OFF clears it.
        """
        action = "on"
        reason = "OPERATOR"
        try:
            parsed = json.loads(message_data)
            if isinstance(parsed, dict):
                action = str(parsed.get("action", action)).strip().lower()
                reason = str(parsed.get("reason", reason)) or reason
        except Exception:
            text = (message_data or "").strip().lower()
            if text in ("off", "resume", "clear"):
                action = "off"
            elif text:
                reason = message_data.strip()

        if action == "off":
            if self._trading_halted and self._halt_reason == "KILL_SWITCH":
                self._trading_halted = False
                self._halt_reason = ""
                logger.warning(f"[Kill Switch] Trading resumed ({reason})")
                await self.publish_status()
            return

        if not self._trading_halted:
            self._trading_halted = True
            self._halt_reason = "KILL_SWITCH"
            logger.critical(f"[Kill Switch] ALL new entries halted ({reason})")
            await self.publish_status()

    # -------------------------------------------------------------
    # State 1: Entry Order Pegging
    # -------------------------------------------------------------

    def _pair_lock(self, pair_tag: str) -> asyncio.Lock:
        """Returns the per-pair lock, creating it on first use."""
        lock = self._pair_locks.get(pair_tag)
        if lock is None:
            lock = asyncio.Lock()
            self._pair_locks[pair_tag] = lock
        return lock

    def _pair_venues(self, pair_tag: str) -> tuple[str, str]:
        """Best-effort (local_exchange, global_exchange) names for a pair tag."""
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        if pair_info is not None:
            return pair_info.local_exchange, pair_info.global_exchange
        pending = self.pending_entry_orders.get(pair_tag) or {}
        if pending:
            return pending.get("local_exchange", ""), pending.get("global_exchange", "")
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            return pos.local_exchange, pos.global_exchange
        return "", ""

    async def handle_signal_notification(self, pair_tag: str):
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
            configured_local = str(self.config.local_exchange_name or "").strip().lower()
            configured_global = str(self.config.global_exchange_name or "").strip().lower()
            signal_local = str(payload.pair_info.local_exchange or "").strip().lower()
            signal_global = str(payload.pair_info.global_exchange or "").strip().lower()
            if (
                not getattr(self.config, "mock_mode", False)
                and (signal_local != configured_local or signal_global != configured_global)
            ):
                reason = "EXECUTOR_VENUE_MISMATCH"
                message = (
                    "Signal exchange pair does not match this executor instance; "
                    "entry rejected to prevent orders on the wrong venue."
                )
                self._pair_infos[pair_tag] = payload.pair_info
                self._mark_failed(
                    pair_tag,
                    reason,
                    message,
                    detail={
                        "configured_local_exchange": configured_local,
                        "configured_global_exchange": configured_global,
                        "signal_local_exchange": signal_local,
                        "signal_global_exchange": signal_global,
                    },
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
        Revalidate both local Spot and global Perpetual tickers immediately before
        placing the Spot order. A zero spread floor disables only the profitability
        threshold; it never disables the fresh-price, FX, or maker-price checks.
        """
        min_spread = float(
            getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0
        )
        if min_spread <= 0:
            min_spread = float(payload.execution_directives.min_entry_spread_pct or 0.0)

        local_symbol = payload.pair_info.local_symbol
        global_symbol = payload.pair_info.global_symbol
        local_fetcher = getattr(self.client, "fetch_local_ticker", None)
        if not callable(local_fetcher):
            await self._publish_execution_failure(
                pair_tag,
                "ENTRY_REVALIDATION_UNAVAILABLE",
                "Ticker Spot live tidak didukung; order dibatalkan sebelum membuka posisi.",
                detail={"local_symbol": local_symbol, "global_symbol": global_symbol},
            )
            return False

        try:
            local_ticker, global_ticker = await asyncio.gather(
                local_fetcher(local_symbol),
                self.client.fetch_global_ticker(global_symbol),
            )
        except Exception as exc:  # nosec BLE001 - both legs must be re-quoted
            logger.warning(
                f"[Entry Revalidation] ticker read failed for {pair_tag}: {exc}. Rejecting entry."
            )
            await self._publish_execution_failure(
                pair_tag,
                "ENTRY_REVALIDATION_UNAVAILABLE",
                "Ticker Spot/Futures live tidak dapat diverifikasi; order Spot dibatalkan.",
                detail={"local_symbol": local_symbol, "global_symbol": global_symbol, "error": str(exc)},
            )
            return False

        now_ms = int(time.time() * 1000)
        max_age_ms = int(getattr(self.config, "entry_revalidation_max_age_ms", 2000) or 2000)
        for label, ticker, symbol in (
            ("Spot", local_ticker, local_symbol),
            ("Futures", global_ticker, global_symbol),
        ):
            try:
                ticker_ts = int((ticker or {}).get("timestamp") or 0)
            except (TypeError, ValueError):
                ticker_ts = 0
            age_ms = now_ms - ticker_ts
            if ticker_ts <= 0 or age_ms < -1000 or age_ms > max_age_ms:
                await self._publish_execution_failure(
                    pair_tag,
                    "ENTRY_REVALIDATION_STALE_TICKER",
                    f"Ticker {label} tidak memiliki timestamp valid atau sudah basi; order Spot dibatalkan.",
                    detail={"symbol": symbol, "timestamp_ms": ticker_ts, "age_ms": age_ms, "max_age_ms": max_age_ms},
                )
                return False

        try:
            local_bid = float((local_ticker or {}).get("bid") or 0.0)
            local_ask = float((local_ticker or {}).get("ask") or 0.0)
            live_bid = float((global_ticker or {}).get("bid") or 0.0)
            live_ask = float((global_ticker or {}).get("ask") or 0.0)
        except (TypeError, ValueError):
            local_bid = local_ask = live_bid = live_ask = 0.0

        snapshot = payload.pricing_snapshot
        buy_price_idr = float(payload.execution_directives.recommended_limit_buy_idr or 0.0)
        fx_rate = float(snapshot.usdt_idr_rate or 0.0)
        values = (local_bid, local_ask, live_bid, live_ask, buy_price_idr, fx_rate)
        if any(not math.isfinite(value) or value <= 0 for value in values):
            await self._publish_execution_failure(
                pair_tag,
                "ENTRY_REVALIDATION_PRICE_INVALID",
                "Bid/ask Spot atau Futures, harga order, maupun FX tidak valid; order Spot dibatalkan.",
                detail={
                    "local_bid": local_bid, "local_ask": local_ask,
                    "global_bid": live_bid, "global_ask": live_ask,
                    "buy_price_idr": buy_price_idr, "usdt_idr_rate": fx_rate,
                },
            )
            return False

        quote_asset = str(payload.pair_info.quote_asset or "").upper()
        local_ask_idr = local_ask * fx_rate if quote_asset in ("USDT", "USDC") else local_ask
        live_bid_idr = live_bid * fx_rate
        if buy_price_idr >= local_ask_idr:
            await self._publish_execution_failure(
                pair_tag,
                "ENTRY_REVALIDATION_LIMIT_WOULD_TAKE",
                "Harga limit Buy Spot akan menyentuh/menyeberangi Ask; order dibatalkan agar tidak berubah menjadi taker.",
                detail={"buy_price_idr": buy_price_idr, "live_local_ask_idr": local_ask_idr},
            )
            return False

        live_spread_pct = ((live_bid_idr - buy_price_idr) / live_bid_idr) * 100.0
        if min_spread > 0 and live_spread_pct + 1e-9 < min_spread:
            logger.info(
                f"[Entry Revalidation] rejecting {pair_tag}: live spread "
                f"{live_spread_pct:.3f}% < floor {min_spread:.3f}%"
            )
            await self._publish_execution_failure(
                pair_tag,
                "ENTRY_REVALIDATION_SPREAD_COLLAPSED",
                "Spread live turun di bawah ambang minimum; order Spot dibatalkan.",
                detail={"live_spread_pct": live_spread_pct, "min_spread_pct": min_spread},
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

        live_preflight = not getattr(self.config, "mock_mode", False)
        if live_preflight:
            validator = getattr(self.client, "validate_entry_order", None)
            if not callable(validator):
                await self._publish_execution_failure(
                    pair_tag,
                    "ORDER_CONSTRAINT_PREFLIGHT_UNAVAILABLE",
                    "LIVE entry diblokir: adapter tidak menyediakan validasi market aktif, minimum order, precision, dan contract multiplier.",
                    detail={"local_symbol": pair_info.local_symbol, "global_symbol": pair_info.global_symbol},
                )
                return
            try:
                buy_qty, buy_price_idr = await validator(
                    local_symbol=pair_info.local_symbol,
                    global_symbol=pair_info.global_symbol,
                    amount=buy_qty,
                    price_idr=buy_price_idr,
                    usdt_idr_rate=snapshot.usdt_idr_rate,
                    global_ask_quote=snapshot.global_best_ask_usdt,
                    local_exchange=pair_info.local_exchange,
                    global_exchange=pair_info.global_exchange,
                )
            except Exception as preflight_error:
                logger.warning(
                    f"[Order Constraint Preflight] rejecting {pair_tag}: {preflight_error}"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "ORDER_CONSTRAINT_PREFLIGHT_FAILED",
                    "LIVE entry dibatalkan karena market/precision/minimum order/contract multiplier tidak dapat diverifikasi.",
                    detail={
                        "local_symbol": pair_info.local_symbol,
                        "global_symbol": pair_info.global_symbol,
                        "error": str(preflight_error),
                    },
                )
                return
            if not math.isfinite(float(buy_qty)) or not math.isfinite(float(buy_price_idr)) or buy_qty <= 0 or buy_price_idr <= 0:
                await self._publish_execution_failure(
                    pair_tag,
                    "ORDER_CONSTRAINT_PREFLIGHT_INVALID_RESULT",
                    "LIVE entry dibatalkan karena hasil validasi ukuran/harga tidak valid.",
                    detail={"amount": buy_qty, "price_idr": buy_price_idr},
                )
                return

        actual_cost_usdt = (buy_qty * buy_price_idr) / snapshot.usdt_idr_rate
        if actual_cost_usdt + 1e-9 < float(getattr(self.config, "min_allocated_capital", 0.0) or 0.0):
            await self._publish_execution_failure(
                pair_tag,
                "ORDER_CONSTRAINT_SIZE_BELOW_MIN_CAPITAL",
                "Ukuran setelah pembulatan lot/contract lebih kecil dari minimum modal; entry dibatalkan.",
                detail={"actual_cost_usdt": actual_cost_usdt},
            )
            return

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
            place_kwargs = {"client_order_id": entry_coid}
            if live_preflight:
                place_kwargs["usdt_idr_rate"] = snapshot.usdt_idr_rate
            order = await self.client.place_local_limit_buy(
                pair_info.local_symbol, buy_qty, buy_price_idr, **place_kwargs
            )
            order_id = str(order.get("id", ""))
            self._record_order_success(pair_info.local_exchange)
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

    def _mark_failed(
        self,
        pair_tag: str,
        reason: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        state: PositionState = PositionState.FAILED,
    ) -> None:
        """Puts a pair into a failure state and records why."""
        self.states[pair_tag] = state
        entry = {
            "reason": reason,
            "message": message,
            "detail": detail or {},
            "timestamp_ms": int(time.time() * 1000),
        }
        self._failure_reasons[pair_tag] = entry
        logger.critical(f"[{state.value}] {pair_tag}: {reason} - {message}")
        if reason != "HEDGE_FAILED":
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(
                    self._publish_execution_failure(
                        pair_tag, reason, message, detail=detail, state=state
                    )
                )
            except RuntimeError:
                # A synchronous caller without an active event loop still records
                # the failure locally; the next status poll remains available.
                pass

    async def _publish_execution_failure(
        self,
        pair_tag: str,
        reason: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        state: Optional[PositionState] = None,
    ) -> None:
        """Publish actionable execution failures for the Telegram notifier."""
        if self.redis is None:
            return
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        payload = {
            "event": "EXECUTION_FAILURE",
            "pair_tag": pair_tag,
            "local_exchange": pair_info.local_exchange if pair_info else "",
            "global_exchange": pair_info.global_exchange if pair_info else "",
            "symbol": pair_info.local_symbol if pair_info else pair_tag,
            "reason": reason,
            "message": message,
            "detail": detail or {},
            "state": state.value if state else "",
            "is_mock": bool(getattr(self.config, "mock_mode", False)),
            "timestamp_ms": int(time.time() * 1000),
        }
        try:
            await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(payload, default=str))
        except Exception:
            logger.exception("failed to publish execution failure event for %s", pair_tag)

    @staticmethod
    def _utc_day() -> str:
        return time.strftime("%Y-%m-%d", time.gmtime())

    def _roll_loss_day_if_needed(self) -> None:
        """Resets the daily loss accumulator when the UTC day changes."""
        today = self._utc_day()
        if today == self._loss_day:
            return
        self._loss_day = today
        self._daily_realized_loss_usdt = 0.0
        # The operator kill switch does not auto-reset on a new UTC day.
        if (
            self._trading_halted
            and self._halt_reason == "DAILY_LOSS_LIMIT"
        ):
            self._trading_halted = False
            self._halt_reason = ""
            logger.warning("[Circuit Breaker] New UTC day - trading resumed")

    def record_realized_pnl(self, net_profit_usdt: float) -> None:
        """
        Feeds a closed trade's recorded net PnL estimate into the daily loss breaker; fees are not exchange-confirmed.

        Losses beyond max_daily_loss_usdt halt *new entries* only: open positions
        keep being managed, because abandoning them unhedged would be worse than
        the loss the breaker is trying to stop.
        """
        self._roll_loss_day_if_needed()
        if net_profit_usdt < 0:
            self._daily_realized_loss_usdt += -net_profit_usdt

        limit = getattr(self.config, "max_daily_loss_usdt", 0.0)
        if limit <= 0 or self._trading_halted:
            return
        if self._daily_realized_loss_usdt >= limit:
            self._trading_halted = True
            self._halt_reason = "DAILY_LOSS_LIMIT"
            logger.critical(
                f"[Circuit Breaker] HALTED new entries: daily realized loss "
                f"{self._daily_realized_loss_usdt:.4f} USDT >= limit {limit:.4f} USDT"
            )

    async def resume_trading(self, reason: str = "MANUAL") -> None:
        """Clears the halt flag; used by the operator once a loss is reviewed."""
        self._trading_halted = False
        self._halt_reason = ""
        logger.warning(f"[Circuit Breaker] Trading resumed manually ({reason})")
        await self.publish_status()

    def clear_failure(self, pair_tag: str) -> None:
        """Clears a recorded failure so the pair can trade again."""
        if self._failure_reasons.pop(pair_tag, None) is not None:
            if self.states.get(pair_tag) == PositionState.FAILED:
                self.states[pair_tag] = PositionState.IDLE

    def status_snapshot(self) -> Dict[str, Any]:
        """Point-in-time view of the executor, suitable for a JSON API."""
        return {
            "status": "RUNNING" if self._running else "STOPPED",
            "mock_mode": bool(getattr(self.config, "mock_mode", False)),
            "local_exchange": self.config.local_exchange_name,
            "global_exchange": self.config.global_exchange_name,
            "open_positions": len(self.active_positions),
            "pending_entries": len(self.pending_entry_orders),
            "pending_exits": len(self.pending_exit_orders),
            "failed_pairs": len(self._failure_reasons),
            "trading_halted": self._trading_halted,
            "halt_reason": self._halt_reason,
            "exchange_halted": dict(self._exchange_halted),
            "daily_realized_loss_usdt": round(self._daily_realized_loss_usdt, 6),
            "max_daily_loss_usdt": getattr(self.config, "max_daily_loss_usdt", 0.0),
            "loss_day_utc": self._loss_day,
            "positions": {
                pair: {
                    "state": state.value,
                    "qty": getattr(self.active_positions.get(pair), "qty", 0.0),
                }
                for pair, state in self.states.items()
            },
            "failures": dict(self._failure_reasons),
            "timestamp_ms": int(time.time() * 1000),
        }

    async def publish_status(self) -> None:
        """Makes the latest status readable by the Go API and operators."""
        if self.redis is None:
            return
        payload = json.dumps(self.status_snapshot())
        try:
            await self.redis.set("arb:executor:status", payload, ex=30)
        except Exception:
            logger.exception("failed to publish executor status")

    # -------------------------------------------------------------
    # Emergency Cancel Handling
    # -------------------------------------------------------------

    async def handle_emergency_cancel(self, message_data: str):
        """
        Handles CANCEL_ALL_NEGATIVE_FUNDING from Scanner:
        Cancels any pending local Limit Buy orders in State 1.
        """
        target_pair_key = None
        try:
            parsed = json.loads(message_data)
            target_pair_key = parsed.get("pair_key")
        except Exception:
            target_pair_key = message_data.strip() if message_data else None

        to_cancel = []
        if target_pair_key:
            if target_pair_key in self.pending_entry_orders:
                to_cancel.append(target_pair_key)
            else:
                # Target pair has negative funding but we have no pending order for it; ignore quietly
                logger.debug(f"[Emergency Cancel] Ignored for {target_pair_key}: no pending entry order")
                return
        else:
            # If no specific key or symbol given, cancel all pending entry orders
            to_cancel = list(self.pending_entry_orders.keys())

        for p_key in to_cancel:
            async with self._pair_lock(p_key):
                order_info = self.pending_entry_orders.get(p_key)
                if not order_info:
                    continue
                order_id = order_info["order_id"]
                logger.warning(
                    f"[Emergency Cancel] Cancelling pending entry order {order_id} for {p_key} due to negative funding"
                )
                resolved = await self._cancel_entry_order_and_reconcile(
                    p_key, order_info, "Emergency Cancel"
                )
                if not resolved:
                    logger.critical(
                        f"[Emergency Cancel] order {order_id} remains unresolved; "
                        "pending state and reserved funds are retained"
                    )
