import asyncio
import json
import logging
import math
import time
from typing import Awaitable, Callable, Optional, Dict, Any, TypeVar, Tuple

import redis.asyncio as aioredis
from executor.models import (
    PositionState,
    PairInfo,
    PricingSnapshot,
    FundingMetrics,
    ExecutionDirectives,
    ExecutorPayload,
    ActivePosition,
)
from executor.exchange_client import ExchangeClient
from executor.config import ExecutorConfig
from executor.private_stream import PrivateStreamCache, fetch_order_preferring_stream

logger = logging.getLogger("executor.engine")

T = TypeVar("T")

ARB_TRADE_HISTORY_KEY = "arb:trade_history"
ARB_TRADE_HISTORY_MAX_LEN = 49  # ltrim keeps indices 0..49 → 50 entries

def build_client_order_id(pair_tag: str, leg: str, seq: int) -> str:
    """
    Builds a venue-safe client order id: ``{pair}-{leg}-{seq}``.

    The id is deterministic for a given (pair, leg, sequence) so a retry of the
    same logical order is recognised as a duplicate by the venue instead of
    double-filling. Kept to lowercase alphanumerics and dashes because Binance,
    OKX and Indodax each accept a different, partly overlapping character set.
    """
    clean_pair = "".join(c if c.isalnum() else "-" for c in pair_tag.lower())[:24]
    return f"{clean_pair}-{leg.lower()}-{seq}"[:36]


class HedgeSlippageExceeded(Exception):
    """Raised when the futures ask moved past the allowed entry slippage."""


class RetryExhausted(Exception):
    """Raised by retry_with_backoff when every attempt failed."""

    def __init__(self, attempts: int, last_error: Exception):
        super().__init__(f"all {attempts} attempts failed: {last_error}")
        self.attempts = attempts
        self.last_error = last_error


async def retry_with_backoff(
    fn: Callable[[], Awaitable[T]],
    max_retries: int = 3,
    base_delay: float = 0.5,
    label: str = "operation",
) -> T:
    """
    Calls an async callable, retrying with exponential backoff (base_delay * 2^n).

    ``max_retries`` counts retries *after* the initial attempt, so the callable is
    invoked at most ``max_retries + 1`` times. Raises RetryExhausted carrying the
    last error once every attempt has failed.
    """
    attempts = max(1, max_retries + 1)
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(f"[Retry] {label} attempt {attempt}/{attempts} failed: {exc}")
            if attempt < attempts:
                await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
    raise RetryExhausted(attempts, last_error) from last_error

def clean_pair_key(local_ex: str, symbol: str) -> str:
    """Matches Golang CleanPairKey format: {local_ex}:{clean_symbol}"""
    clean_ex = local_ex.lower().strip()
    clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
    return f"{clean_ex}:{clean_sym}"


def round_to_tick(price: float, tick_size: float) -> float:
    """Rounds price to nearest local tick size."""
    if tick_size <= 0:
        return price
    return round(price / tick_size) * tick_size


def ceil_to_tick(price: float, tick_size: float) -> float:
    """Rounds price UP (ceiling) to local tick size to prevent selling below target/break-even."""
    if tick_size <= 0:
        return price
    return math.ceil(round(price / tick_size, 9)) * tick_size


def floor_to_tick(price: float, tick_size: float) -> float:
    """Rounds price DOWN (floor) to local tick size to prevent buying above target."""
    if tick_size <= 0:
        return price
    return math.floor(round(price / tick_size, 9)) * tick_size

BALANCES_KEY = "arb:balances"
PENDING_ENTRY_KEY_PREFIX = "arb:pending_entry:"

class ArbitrageEngine:
    """
    Hybrid Maker-Taker + Positive Funding Rate Arbitrage Engine.
    Implements 4 States with non-blocking CCXT + redis.asyncio.
    """

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
                    await self.redis.delete(key)
                    continue
                status, filled = await self._resolve_venue_order_state(order_id, symbol)
                if status == "unknown":
                    # Cannot confirm venue state: leave the record for the next
                    # startup and refuse fresh entries for this pair.
                    logger.critical(
                        f"[Recovery] cannot verify venue state of order {order_id} "
                        f"({pair_tag}) - MANUAL CHECK REQUIRED"
                    )
                    continue
                if status == "open":
                    order_info["filled"] = float(filled or 0.0)
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.ENTRY_PENDING
                    logger.warning(
                        f"[Recovery] re-adopted pending entry order {order_id} "
                        f"for {pair_tag} (filled={filled})"
                    )
                else:
                    logger.warning(
                        f"[Recovery] pending entry order {order_id} for {pair_tag} "
                        f"is {status} on the venue - clearing record"
                    )
                await self.redis.delete(key)
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
            # fetch_order on a missing order typically raises; a None-ish reply
            # means the venue does not know the order.
            return "not_found", 0.0
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
            try:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                pos = ActivePosition.from_dict(json.loads(raw))
                pair_tag = key.split("arb:active_pos:", 1)[-1]
                if pos.qty <= 0:
                    logger.warning(
                        f"[Recovery] Dropping {pair_tag}: recovered qty is {pos.qty}"
                    )
                    await self.redis.delete(key)
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
            except Exception:
                logger.exception(f"[Recovery] failed to restore position from {key}")

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

    def _venue_halted_reason(self, pair_tag: str) -> str:
        """Returns why either venue of the pair is halted, or '' when clear."""
        local_ex, global_ex = self._pair_venues(pair_tag)
        if local_ex and local_ex in self._exchange_halted:
            return f"{local_ex}: {self._exchange_halted[local_ex]}"
        if global_ex and global_ex in self._exchange_halted:
            return f"{global_ex}: {self._exchange_halted[global_ex]}"
        return ""

    async def handle_signal_notification(self, pair_tag: str):
        """
        State 1 (Entry Order Pegging):
        1. GET arb:signal:{local_ex}:{symbol}
        2. Validate timestamp freshness (<= 500ms).
        3. If no active position, place Maker Limit Buy at recommended_limit_buy_idr.
        """
        async with self._pair_lock(pair_tag):
            self._roll_loss_day_if_needed()
            self._check_balance_mismatch()
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
            await self._handle_signal_locked(pair_tag)

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

    async def _compute_entry_sizing(
        self, pair_tag: str, payload: ExecutorPayload
    ) -> Optional[tuple[float, float]]:
        """
        Sizes an entry from the signal budget, available balance, exposure cap and
        the fee reserve. Returns ``(max_pos_usdt, min_cap)``, or None when the
        trade cannot be funded.
        """
        directives = payload.execution_directives
        pair_info = payload.pair_info

        max_pos_usdt = directives.max_position_usdt
        if max_pos_usdt <= 0:
            max_pos_usdt = 100.0  # fallback budget

        # Check exchange balance limits ($100 balance per exchange in simulation)
        min_cap = getattr(self.config, "min_allocated_capital", 10.0)

        self._check_balance_mismatch()
        local_ex, global_ex = self._pair_venues(pair_tag)
        for venue in (local_ex, global_ex):
            if venue and venue in self._exchange_halted:
                logger.warning(
                    f"[Risk Control] rejecting entry for {pair_tag}: "
                    f"{venue} halted ({self._exchange_halted[venue]})"
                )
                return None

        if hasattr(self.client, "get_balances"):
            balances = self.client.get_balances()
            avail_spot = float(balances.get("spot_available", 0.0))
            avail_fut = float(balances.get("futures_available", 0.0))
            usable_capital = min(avail_spot, avail_fut)

            if usable_capital < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Insufficient available balance for {pair_tag}: "
                    f"Spot Avail=${avail_spot:.2f}, Futures Avail=${avail_fut:.2f}, Min Required=${min_cap:.2f}. "
                    f"Rejecting entry signal."
                )
                return None

            # Target position size capped by available capital
            max_pos_usdt = min(max_pos_usdt, usable_capital)

            # Respect the per-exchange exposure cap before anything is locked.
            if not self._exposure_allows(pair_tag, pair_info, max_pos_usdt):
                return None

            # Reserve capital for the round-trip fees so the locked budget still
            # covers them; otherwise the exit eats into the hedge and the trade
            # can close nominally profitable but actually net-negative.
            fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.0) or 0.0)
            if fee_pct > 0:
                fee_buffer_usdt = max_pos_usdt * fee_pct
                max_pos_usdt -= fee_buffer_usdt
                logger.debug(
                    f"[State 1 Fee Buffer] {pair_tag} reserved ${fee_buffer_usdt:.4f} "
                    f"({fee_pct * 100:.3f}%) for fees"
                )

            if max_pos_usdt < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Position size ${max_pos_usdt:.2f} < min required ${min_cap:.2f} for {pair_tag}"
                )
                return None

        return max_pos_usdt, min_cap

    async def _handle_signal_locked(self, pair_tag: str):
        payload = await self._load_signal_payload(pair_tag)
        if payload is None:
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
                return
            # Sync balances to Redis
            bals = self.client.get_balances()
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
                "usdt_idr_rate": snapshot.usdt_idr_rate,
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

    # -------------------------------------------------------------
    # State 2 & Monitoring: Adverse Selection, Partial Fills & Short Hedge
    # -------------------------------------------------------------

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
    def _extract_fill_price(order: Dict[str, Any], *fallbacks: float) -> float:
        """Returns the first positive price from the order payload or the given fallbacks."""
        for key in ("average", "price", "fillPrice"):
            try:
                value = float(order.get(key) or 0.0)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0:
                return value
        for fallback in fallbacks:
            if fallback and fallback > 0:
                return float(fallback)
        return 0.0

    @staticmethod
    def _resolve_short_entry_usdt(short_order: Dict[str, Any]) -> float:
        """
        Resolves the actual futures short entry price.

        Never silently falls back to the spot price: if neither the exchange fill
        nor the expected futures ask is known, fail loudly so the position cannot
        be recorded with a wrong basis.
        """
        short_entry_usdt = ArbitrageEngine._extract_fill_price(
            short_order, float(short_order.get("expected_price") or 0.0)
        )
        if short_entry_usdt <= 0:
            raise ValueError(
                "short entry price unavailable from futures fill; refusing to "
                "record position using spot price basis"
            )
        return short_entry_usdt

    def _record_order_success(self, exchange: str) -> None:
        """Clears the consecutive-failure counter for an exchange after a clean order."""
        if exchange:
            self._consecutive_order_failures.pop(exchange, None)

    def _record_order_failure(self, exchange: str, reason: str) -> None:
        """
        Counts a placement failure per exchange and halts the venue once the
        consecutive-failure threshold is crossed. Halting prevents the engine
        from repeatedly hitting a broken venue while open positions stay managed.
        """
        if not exchange:
            return
        threshold = int(getattr(self.config, "max_consecutive_order_failures", 0) or 0)
        if threshold <= 0:
            return
        count = self._consecutive_order_failures.get(exchange, 0) + 1
        self._consecutive_order_failures[exchange] = count
        if count >= threshold:
            self._exchange_halted[exchange] = f"{count} consecutive order failures ({reason})"
            logger.critical(
                f"[Risk Control] Exchange {exchange} HALTED for new entries: "
                f"{count} consecutive order failures ({reason})"
            )
            # Fire-and-forget status refresh; caller context may not be a coroutine.
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._maybe_publish_status())
            except RuntimeError:
                pass

    def resume_exchange(self, exchange: str, reason: str = "MANUAL") -> bool:
        """Clears a venue halt; returns True when a halt was actually removed."""
        if exchange in self._exchange_halted:
            del self._exchange_halted[exchange]
            self._consecutive_order_failures.pop(exchange, None)
            logger.warning(f"[Risk Control] Exchange {exchange} resumed ({reason})")
            return True
        return False

    def _check_balance_mismatch(self) -> None:
        """
        Detects when an exchange's available balance drops far below what the
        engine last observed - e.g. a withdrawal, liquidation, or another
        process consuming the capital. Halts that venue for new entries.

        Fail-open on read errors: a broken balance feed must not be conflated
        with actual missing funds, and refusing to hedge on a stale read leaves
        the spot leg naked.
        """
        fraction = float(getattr(self.config, "balance_mismatch_fraction", 0.0) or 0.0)
        if fraction <= 0 or not hasattr(self.client, "get_balances"):
            return

        try:
            balances = self.client.get_balances()
        except Exception:
            logger.exception("[Risk Control] failed to read balances; skipping mismatch check")
            return

        venue_available = {}
        venue_name = self.config.local_exchange_name
        venue_available[venue_name] = float(balances.get("spot_available", 0.0) or 0.0)
        global_venue = self.config.global_exchange_name
        venue_available[global_venue] = float(balances.get("futures_available", 0.0) or 0.0)

        # A venue with open positions naturally shows less available balance; only
        # flag when the drop cannot be explained by any known committed capital.
        pending_local, pending_global = self._pending_entry_exposure_all()
        committed = self._open_exposure_by_venue()
        committed[venue_name] = committed.get(venue_name, 0.0) + pending_local
        committed[global_venue] = committed.get(global_venue, 0.0) + pending_global

        for venue, avail in venue_available.items():
            last = self._last_balances.get(venue)
            self._last_balances[venue] = avail
            if last is None or last <= 0:
                continue
            drop = last - avail
            if drop <= 0 or drop <= last * fraction:
                continue
            explained = committed.get(venue, 0.0)
            if drop <= explained * (1.0 + 0.01):
                continue
            self._exchange_halted[venue] = (
                f"balance mismatch: available dropped {last:.2f} -> {avail:.2f} "
                f"(-{drop:.2f}) with only {explained:.2f} committed"
            )
            logger.critical(
                f"[Risk Control] Exchange {venue} HALTED: {self._exchange_halted[venue]}"
            )

    def _open_exposure_by_venue(self) -> Dict[str, float]:
        """Capital currently committed to open positions, keyed by exchange name."""
        by_venue: Dict[str, float] = {}
        for pos in self.active_positions.values():
            if pos.local_exchange:
                by_venue[pos.local_exchange] = by_venue.get(pos.local_exchange, 0.0) + pos.spot_cost_usdt
            if pos.global_exchange:
                by_venue[pos.global_exchange] = by_venue.get(pos.global_exchange, 0.0) + pos.futures_margin_usdt
        return by_venue

    def _pending_entry_exposure_all(self) -> tuple[float, float]:
        """
        Capital reserved by resting entry orders across ALL pairs.
        Mirrors _pending_entry_exposure but venue-agnostic, for the global
        balance-mismatch sweep which runs outside any single pair's context.
        """
        total_spot = 0.0
        total_futures = 0.0
        for order_info in self.pending_entry_orders.values():
            remaining_qty = float(order_info.get("qty", 0.0)) - float(order_info.get("filled", 0.0))
            remaining_qty = max(0.0, remaining_qty)
            usdt_rate = float(order_info.get("usdt_idr_rate", self.FALLBACK_USDT_IDR_RATE) or self.FALLBACK_USDT_IDR_RATE)
            value = remaining_qty * float(order_info.get("price", 0.0))
            total_spot += value / usdt_rate if usdt_rate > 0 else 0.0
            total_futures += value / usdt_rate if usdt_rate > 0 else 0.0
        return total_spot, total_futures

    def _exposure_allows(
        self, pair_tag: str, pair_info: PairInfo, planned_usdt: float
    ) -> bool:
        """
        Checks the planned order against ``max_exposure_per_exchange_usdt``.

        Exposure is the capital already committed to open positions plus any
        entry orders still resting, counted separately per exchange so a single
        venue cannot be concentrated beyond the configured cap.
        """
        cap = float(getattr(self.config, "max_exposure_per_exchange_usdt", 0.0) or 0.0)
        if cap <= 0:
            return True

        local_exposure = 0.0
        global_exposure = 0.0
        for tag, pos in self.active_positions.items():
            if tag == pair_tag:
                continue
            if pos.local_exchange == pair_info.local_exchange:
                local_exposure += pos.spot_cost_usdt
            if pos.global_exchange == pair_info.global_exchange:
                global_exposure += pos.futures_margin_usdt

        pending_local, pending_global = self._pending_entry_exposure(
            pair_tag, pair_info
        )
        local_exposure += pending_local
        global_exposure += pending_global
        # The spot leg and the futures margin each consume their own venue's cap.
        blocked = (
            local_exposure + planned_usdt > cap
            or global_exposure + planned_usdt > cap
        )
        if blocked:
            logger.warning(
                f"[State 1 Exposure Cap] Rejecting {pair_tag}: planned ${planned_usdt:.2f} would "
                f"push {pair_info.local_exchange} to ${local_exposure + planned_usdt:.2f} "
                f"and {pair_info.global_exchange} to ${global_exposure + planned_usdt:.2f} "
                f"(cap ${cap:.2f} each)"
            )
            return False
        return True

    def _pending_entry_exposure(
        self, pair_tag: str, pair_info: PairInfo
    ) -> tuple[float, float]:
        """
        Capital already reserved by resting entry orders, per exchange.

        An order that was accepted but has not filled yet still consumes the cap:
        ignoring it lets a burst of signals oversubscribe the same venue before
        any of them turns into a position.
        """
        local_exposure = 0.0
        global_exposure = 0.0
        for tag, info in self.pending_entry_orders.items():
            if tag == pair_tag:
                continue
            if info.get("local_exchange") == pair_info.local_exchange:
                local_exposure += float(info.get("spot_cost_usdt", 0.0))
            if info.get("global_exchange") == pair_info.global_exchange:
                global_exposure += float(info.get("futures_margin_usdt", 0.0))
        return local_exposure, global_exposure

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
            exit_price_idr = self._extract_fill_price(emergency_order, price_idr)
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
        Feeds a closed trade's net PnL into the daily loss breaker.

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
    # State 3: Dynamic Exit Limit Sell + Funding Decay Pegging
    # -------------------------------------------------------------

    # Historically hard-coded; kept only as a last-resort conversion rate for
    # IDR<->USDT bookkeeping when no live rate is available. Real rates can
    # drift far from this, so anything display/exit-critical should avoid
    # relying on it.
    FALLBACK_USDT_IDR_RATE = 16000.0

    # A live price snapshot older than this is considered stale and must not
    # be used to drive floating PnL display or exit pegging decisions.
    LIVE_PRICE_STALE_MS = 120_000

    @staticmethod
    def _snapshot_is_stale(snapshot: dict) -> bool:
        ts = float(snapshot.get("timestamp_ms", 0) or 0)
        if ts <= 0:
            return True
        return (int(time.time() * 1000) - ts) > ArbitrageEngine.LIVE_PRICE_STALE_MS

    async def _fetch_live_price(
        self, pair_tag: str, pos: Optional[ActivePosition] = None
    ) -> Optional[tuple[float, float]]:
        """Returns (global_ask_usdt, usdt_idr_rate) from a fresh live-price
        snapshot, or None when no fresh data exists. A stale snapshot is NOT
        substituted with the entry price: doing so fabricates a zero futures
        PnL and makes floating PnL display unreliable."""
        live_price_key = f"arb:live_price:{pair_tag}"
        live_price_raw = await self.redis.get(live_price_key)
        if live_price_raw:
            try:
                live_snapshot = json.loads(live_price_raw)
                global_ask_usdt = float(live_snapshot.get("global_best_ask_usdt", 0.0))
                usdt_idr_rate = float(live_snapshot.get("usdt_idr_rate", self.FALLBACK_USDT_IDR_RATE))
                if global_ask_usdt > 0 and not self._snapshot_is_stale(live_snapshot):
                    return global_ask_usdt, usdt_idr_rate
            except Exception:
                pass

        return None

    async def _handle_funding_rollover(
        self, pair_tag: str, pos: ActivePosition, global_ask_usdt: float, now_ms: int
    ) -> bool:
        if pos.next_funding_time_ms <= 0 or now_ms < pos.next_funding_time_ms:
            return False

        pos_val_usdt = pos.qty * global_ask_usdt
        accumulated = pos_val_usdt * pos.current_funding_rate
        pos.accumulated_funding_usdt += accumulated
        pos.last_funding_time_ms = pos.next_funding_time_ms
        pos.next_funding_time_ms += 8 * 3600 * 1000  # advance 8h
        pos.updated_at = now_ms

        active_key = f"arb:active_pos:{pair_tag}"
        await self.redis.set(active_key, json.dumps(pos.to_dict()))
        logger.info(
            f"[State 3 Funding Rollover] Funding hour passed for {pair_tag}. "
            f"Added ${accumulated:.4f} funding. Total: ${pos.accumulated_funding_usdt:.4f}"
        )
        return True

    def _should_repeg_exit(
        self, pair_tag: str, global_ask_usdt: float, funding_passed: bool
    ) -> bool:
        if pair_tag not in self.pending_exit_orders:
            return True

        last_ask = self.last_pegged_global_ask.get(pair_tag, 0.0)
        ask_movement = 0.0
        if last_ask > 0:
            ask_movement = abs(global_ask_usdt - last_ask) / last_ask

        return (
            (last_ask <= 0)
            or (ask_movement >= self.config.exit_price_move_threshold)
            or funding_passed
        )

    def _calculate_limit_sell_idr(
        self,
        pair_tag: str,
        pos: ActivePosition,
        global_ask_usdt: float,
        usdt_idr_rate: float,
    ) -> tuple[float, float]:
        # 1. Base pegged exit price using spread & funding decay
        pos_value_usdt = pos.qty * global_ask_usdt
        funding_decay_pct = 0.0
        if pos_value_usdt > 0:
            funding_decay_pct = (pos.accumulated_funding_usdt / pos_value_usdt) * 100.0

        effective_exit_spread_pct = pos.base_target_exit_spread_pct - funding_decay_pct
        pegged_limit_sell_idr = (global_ask_usdt * usdt_idr_rate) * (
            1.0 + (effective_exit_spread_pct / 100.0)
        )

        # 2. Strict Mathematical Break-Even & Profit Floor Protection:
        # We enforce: Realized Net Profit >= min_profit_target (> 0)
        # Net Profit = Spot_PnL + Futures_PnL + Funding - Fees
        # Futures PnL = Qty * (short_entry_usdt - global_ask_usdt)
        # Fees = Qty * (short_entry_usdt + global_ask_usdt) * fee_rate (covers both legs)
        fee_rate = 0.0010
        est_fees_usdt = (pos.qty * (pos.short_entry_usdt + global_ask_usdt)) * fee_rate
        futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - global_ask_usdt)

        # Buffer min ROI: at least +0.05% net ROI or 25% of base target exit spread
        min_roi_pct = max(0.05, pos.base_target_exit_spread_pct * 0.25)
        capital_usdt = pos.qty * pos.short_entry_usdt
        min_profit_target_usdt = capital_usdt * (min_roi_pct / 100.0)

        # Required Spot PnL in USDT to guarantee positive closed PnL
        required_spot_pnl_usdt = (
            min_profit_target_usdt
            + est_fees_usdt
            - futures_pnl_usdt
            - pos.accumulated_funding_usdt
        )

        min_profitable_sell_idr = pos.entry_price_idr
        if pos.qty > 0 and usdt_idr_rate > 0:
            min_profitable_sell_idr += (required_spot_pnl_usdt * usdt_idr_rate) / pos.qty

        limit_sell_idr = max(pegged_limit_sell_idr, min_profitable_sell_idr)

        tick_size = getattr(pos, "local_price_tick", 1.0)
        if tick_size <= 0:
            if pair_tag in self.pending_exit_orders:
                tick_size = self.pending_exit_orders[pair_tag].get("tick_size", 1.0)
            else:
                tick_size = 1.0

        # CEIL to tick size so rounding NEVER rounds down below profitable floor
        limit_sell_idr = ceil_to_tick(limit_sell_idr, tick_size)

        logger.info(
            f"[State 3 Dynamic Exit Pegging] Re-pegging Limit Sell for {pair_tag}: "
            f"global_ask={global_ask_usdt}, spread={effective_exit_spread_pct:.3f}%, "
            f"floor_idr={min_profitable_sell_idr:.2f}, limit_sell_idr={limit_sell_idr:.2f}"
        )
        return limit_sell_idr, tick_size

    async def _cancel_existing_exit_order(self, pair_tag: str, symbol: str) -> None:
        if pair_tag not in self.pending_exit_orders:
            return
        old_order = self.pending_exit_orders[pair_tag]
        old_order_id = old_order.get("order_id")
        if old_order_id:
            try:
                await self.client.cancel_local_order(old_order_id, symbol)
            except Exception as e:
                logger.debug(f"error cancelling old exit order {old_order_id}: {e}")

    async def _place_dynamic_limit_sell(
        self,
        pair_tag: str,
        pos: ActivePosition,
        limit_sell_idr: float,
        tick_size: float,
        global_ask_usdt: float,
    ) -> None:
        try:
            order = await self.client.place_local_limit_sell(
                pos.symbol,
                pos.qty,
                limit_sell_idr,
                client_order_id=build_client_order_id(pair_tag, "exit", self._next_order_seq()),
            )
            order_id = str(order.get("id", ""))
            self.pending_exit_orders[pair_tag] = {
                "order_id": order_id,
                "symbol": pos.symbol,
                "price": limit_sell_idr,
                "qty": pos.qty,
                "filled": 0.0,
                "closed_short_qty": 0.0,
                "tick_size": tick_size,
            }
            self.last_pegged_global_ask[pair_tag] = global_ask_usdt
            self.states[pair_tag] = PositionState.EXIT_PENDING
        except Exception:
            logger.exception(f"[State 3] failed to place dynamic limit sell for {pair_tag}")

    async def _check_dynamic_exit_pair(self, pair_tag: str) -> None:
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            return

        pos = self.active_positions.get(pair_tag)
        if not pos or pos.qty <= 0:
            return

        price_data = await self._fetch_live_price(pair_tag, pos)
        if not price_data:
            return
        global_ask_usdt, usdt_idr_rate = price_data

        # Sync latest market prices to client and track floating mark-to-market PnL
        live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        local_best_bid = pos.entry_price_idr
        local_best_ask = 0.0
        live_snapshot = None
        if live_price_raw:
            try:
                live_snapshot = json.loads(live_price_raw)
                if not self._snapshot_is_stale(live_snapshot):
                    local_best_bid = float(live_snapshot.get("local_best_bid", pos.entry_price_idr))
                    local_best_ask = float(live_snapshot.get("local_best_ask", 0.0))
                if hasattr(self.client, "update_market_price"):
                    self.client.update_market_price(pos.symbol, local_best_bid, local_best_ask)
            except Exception:
                pass

        now_ms = int(time.time() * 1000)
        funding_passed = await self._handle_funding_rollover(
            pair_tag, pos, global_ask_usdt, now_ms
        )

        # Mark-to-market: when no fresh live prices exist (scanner offline,
        # expired snapshot, halted local book), do NOT persist fabricated PnL.
        # Leave the last known values untouched and flag the position stale so
        # the Telegram view can hide the numbers instead of showing fakes.
        stale = live_snapshot is None or self._snapshot_is_stale(live_snapshot)
        if not stale:
            spot_pnl_idr = pos.qty * (local_best_bid - pos.entry_price_idr)
            spot_pnl_usdt = spot_pnl_idr / usdt_idr_rate if usdt_idr_rate > 0 else 0.0
            futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - global_ask_usdt)
            est_fees_usdt = (pos.qty * (pos.short_entry_usdt + global_ask_usdt)) * 0.0005
            net_unrealized_usdt = (
                spot_pnl_usdt + futures_pnl_usdt + pos.accumulated_funding_usdt - est_fees_usdt
            )
            net_unrealized_idr = net_unrealized_usdt * usdt_idr_rate
            cap_usdt = pos.qty * pos.short_entry_usdt
            unrealized_roi = (net_unrealized_usdt / cap_usdt * 100.0) if cap_usdt > 0 else 0.0

            pos.current_spot_bid_idr = local_best_bid
            pos.current_perp_ask_usdt = global_ask_usdt
            pos.spot_unrealized_pnl_idr = spot_pnl_idr
            pos.futures_unrealized_pnl_usdt = futures_pnl_usdt
            pos.net_unrealized_pnl_usdt = net_unrealized_usdt
            pos.net_unrealized_pnl_idr = net_unrealized_idr
            pos.net_unrealized_roi_pct = unrealized_roi
            pos.updated_at = now_ms

            # Persist updated floating PnL to Redis for live position tracking
            await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

        if not self._should_repeg_exit(pair_tag, global_ask_usdt, funding_passed):
            return

        limit_sell_idr, tick_size = self._calculate_limit_sell_idr(
            pair_tag, pos, global_ask_usdt, usdt_idr_rate
        )
        await self._cancel_existing_exit_order(pair_tag, pos.symbol)
        await self._place_dynamic_limit_sell(
            pair_tag, pos, limit_sell_idr, tick_size, global_ask_usdt
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

    async def _process_pending_exit(self, pair_tag: str) -> None:
        """Fetch local exit order and trigger short closure on fills."""
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
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
                self.client.update_market_price(
                    symbol=local_symbol,
                    best_bid=float(live_snapshot.get("local_best_bid", 0.0)),
                    best_ask=float(live_snapshot.get("local_best_ask", 0.0)),
                )
            except Exception:
                pass

        try:
            order = await fetch_order_preferring_stream(
                self.private_stream,
                lambda: self.client.fetch_local_order(order_id, local_symbol),
                order_id,
            )
        except Exception:
            logger.exception(f"failed to fetch exit order {order_id}")
            return

        current_filled = float(order.get("filled", 0.0))
        closed_qty = float(order_info.get("closed_short_qty", 0.0))
        unclosed_delta = current_filled - closed_qty

        if unclosed_delta > 0:
            await self._execute_exit_fill(pair_tag, order_info, pos, unclosed_delta)

    async def _execute_exit_fill(
        self,
        pair_tag: str,
        order_info: dict,
        pos: ActivePosition,
        unclosed_delta: float,
    ) -> None:
        """Close global short via market buy and update position state."""
        exit_price_idr = float(order_info.get("price", 0.0))
        entry_price_idr = float(pos.entry_price_idr)
        short_entry_usdt = float(pos.short_entry_usdt)

        # Fetch FX rate and latest global ask
        usdt_rate = self.FALLBACK_USDT_IDR_RATE
        live_global_ask = float(self.last_pegged_global_ask.get(pair_tag, short_entry_usdt))
        live_price_raw = await self.redis.get(f"arb:live_price:{pair_tag}")
        if live_price_raw:
            try:
                live_snapshot = json.loads(live_price_raw)
                usdt_rate = float(live_snapshot.get("usdt_idr_rate", FALLBACK_USDT_IDR_RATE))
                g_ask = float(live_snapshot.get("global_best_ask_usdt", 0.0))
                if g_ask > 0:
                    live_global_ask = g_ask
            except Exception:
                pass

        logger.info(
            f"[State 4 Exit Execution] Local Limit Sell fill detected: sold_delta={unclosed_delta} "
            f"on {pair_tag}. Immediately closing Short via Market Buy (reduceOnly=True)!"
        )
        try:
            global_sym = self._resolve_global_symbol(pos.symbol)
            close_order = await self.client.close_global_short_market(
                global_sym,
                unclosed_delta,
                client_order_id=build_client_order_id(pair_tag, "unwind", self._next_order_seq()),
            )
            # Snapshot qty before mutation so PnL fractions stay consistent.
            total_qty_before = pos.qty
            order_info["closed_short_qty"] += unclosed_delta
            pos.qty -= unclosed_delta

            # Persist updated qty and closed_short_qty immediately so a crash before
            # _finalize_exit_position does not cause the next poll to double-close.
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict()))

            # PnL must use the price the short actually closed at, not the last
            # pegged quote: a market buy fills through the book and the gap is
            # real money. Fall back to the live ask only when the venue returns
            # no fill price at all.
            short_exit_usdt = self._extract_fill_price(
                close_order or {}, live_global_ask
            )
            if short_exit_usdt <= 0:
                short_exit_usdt = live_global_ask

            spot_pnl_idr = unclosed_delta * (exit_price_idr - entry_price_idr)
            spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
            futures_pnl_usdt = unclosed_delta * (short_entry_usdt - short_exit_usdt)

            # Allocate only the pro-rata share of accumulated funding for this
            # partial fill. Using the full accumulator per fill inflates PnL on
            # multi-fill exits; the remainder stays on the position for later fills.
            fill_ratio = (unclosed_delta / total_qty_before) if total_qty_before > 0 else 1.0
            funding_usdt = float(pos.accumulated_funding_usdt) * fill_ratio
            pos.accumulated_funding_usdt -= funding_usdt

            fees_usdt = (unclosed_delta * (short_entry_usdt + short_exit_usdt)) * 0.0005
            net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
            net_profit_idr = net_profit_usdt * usdt_rate

            capital_usdt = unclosed_delta * short_entry_usdt
            roi_pct = (net_profit_usdt / capital_usdt * 100.0) if capital_usdt > 0 else 0.0

            is_mock = getattr(self.config, "mock_mode", False)

            if net_profit_usdt < 0:
                logger.warning(
                    f"[State 4 Realized Loss] Local Limit Sell fill resulted in net negative PnL: "
                    f"{net_profit_usdt:.4f} USDT (ROI: {roi_pct:.3f}%). Spot PnL: {spot_pnl_usdt:+.4f} USDT, "
                    f"Futures Short PnL: {futures_pnl_usdt:+.4f} USDT, Funding: +{funding_usdt:.4f} USDT, Fees: -{fees_usdt:.4f} USDT. "
                    f"Closing Short to maintain delta-neutral position integrity."
                )
            else:
                logger.info(
                    f"[State 4 Realized Profit] Trade closed with profit: +{net_profit_usdt:.4f} USDT (ROI: +{roi_pct:.3f}%). "
                    f"Spot PnL: {spot_pnl_usdt:+.4f} USDT, Futures Short PnL: {futures_pnl_usdt:+.4f} USDT."
                )
            closed_trade = {
                "trade_id": f"trade-{int(time.time()*1000)}",
                "local_exchange": pos.local_exchange,
                "global_exchange": pos.global_exchange,
                "symbol": pos.symbol,
                "qty": unclosed_delta,
                "entry_price_idr": entry_price_idr,
                "exit_price_idr": exit_price_idr,
                "short_entry_usdt": short_entry_usdt,
                "short_exit_usdt": short_exit_usdt,
                "spot_pnl_idr": spot_pnl_idr,
                "futures_pnl_usdt": futures_pnl_usdt,
                "accumulated_funding_usdt": funding_usdt,
                "net_profit_usdt": net_profit_usdt,
                "net_profit_idr": net_profit_idr,
                "net_roi_pct": roi_pct,
                "is_mock": is_mock,
                "closed_at_ms": int(time.time() * 1000),
            }

            await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(closed_trade))
            await self.redis.ltrim(ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN)

            # Release only the pro-rata share of locked capital for this fill.
            spot_cost = getattr(pos, "spot_cost_usdt", unclosed_delta * entry_price_idr / usdt_rate) * fill_ratio
            fut_margin = getattr(pos, "futures_margin_usdt", unclosed_delta * short_entry_usdt) * fill_ratio
            if hasattr(self.client, "apply_realized_trade"):
                self.client.apply_realized_trade(
                    spot_cost_usdt=spot_cost,
                    spot_pnl_usdt=spot_pnl_usdt,
                    spot_fees_usdt=0.0,
                    futures_margin_usdt=fut_margin,
                    futures_pnl_usdt=futures_pnl_usdt,
                    funding_usdt=funding_usdt,
                    futures_fees_usdt=fees_usdt,
                )
                bals = self.client.get_balances()
                await self.redis.set(BALANCES_KEY, json.dumps(bals))

            # Update stats in Redis
            pipe = self.redis.pipeline()
            pipe.hincrby("arb:stats", "total_closed", 1)
            if net_profit_usdt > 0:
                pipe.hincrby("arb:stats", "win_count", 1)
            pipe.hincrbyfloat("arb:stats", "total_pnl_usdt", net_profit_usdt)
            pipe.hincrbyfloat("arb:stats", "total_pnl_idr", net_profit_idr)

            # Publish event to arb:trade_events for Telegram Bot
            event_msg = {
                "event": "POSITION_CLOSED",
                **closed_trade,
            }
            pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
            await pipe.execute()

            self.record_realized_pnl(net_profit_usdt)
            await self.publish_status()
            await self._finalize_exit_position(pair_tag, pos)
        except Exception as ee:
            logger.critical(
                f"[State 4 CRITICAL] Failed to execute close short market buy for {pair_tag}: {ee}"
            )

    def _resolve_global_symbol(self, symbol: str) -> str:
        """Convert local/pos symbol into global futures symbol format."""
        global_sym = symbol.upper()
        if "/" not in global_sym and "_" in global_sym:
            global_sym = global_sym.replace("_", "/")
        if "IDR" in global_sym:
            base = global_sym.split("/")[0] if "/" in global_sym else global_sym.replace("IDR", "")
            return f"{base}/USDT:USDT"
        return global_sym

    async def _finalize_exit_position(
        self, pair_tag: str, pos: ActivePosition
    ) -> None:
        """Update Redis active position or delete if fully closed."""
        active_key = f"arb:active_pos:{pair_tag}"
        if pos.qty <= 0.00001:  # dust threshold / fully exited
            logger.info(
                f"[State 4 Complete] Arbitrage position for {pair_tag} fully exited! Deleting active pos."
            )
            await self.redis.delete(active_key)
            del self.active_positions[pair_tag]
            del self.pending_exit_orders[pair_tag]
            if pair_tag in self.last_pegged_global_ask:
                del self.last_pegged_global_ask[pair_tag]
            self.states[pair_tag] = PositionState.CLOSED
        else:
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(active_key, json.dumps(pos.to_dict()))

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
                symbol = order_info["symbol"]
                logger.warning(
                    f"[Emergency Cancel] Cancelling pending entry order {order_id} for {p_key} due to negative funding"
                )
                try:
                    await self.client.cancel_local_order(order_id, symbol)
                except Exception:
                    logger.exception(f"emergency cancel failed for {order_id}")
                del self.pending_entry_orders[p_key]
                await self._clear_pending_entry(p_key)
                self.states[p_key] = PositionState.IDLE

    # -------------------------------------------------------------
    # Force Close Handling (from Telegram or API)
    # -------------------------------------------------------------

    @staticmethod
    def _parse_force_close_pair_key(message_data: str):
        """Extracts the pair_key from a FORCE_CLOSE directive (JSON or plain text)."""
        if not message_data:
            return None
        try:
            parsed = json.loads(message_data)
        except Exception:
            return message_data.strip() or None
        if isinstance(parsed, dict):
            return parsed.get("pair_key")
        return None

    async def _cancel_pending_order_for_force_close(
        self, pair_key: str, orders: dict, order_label: str
    ) -> bool:
        """Cancels a pending order (exit/entry) tracked in `orders`. Returns True ONLY if cancellation succeeded on the exchange."""
        order_info = orders.get(pair_key)
        if not order_info:
            return False
        try:
            res = await self.client.cancel_local_order(order_info["order_id"], order_info["symbol"])
            # If exchange client returns status, ensure it's not an explicit failure
            if isinstance(res, dict) and res.get("status") in ("failed", "rejected"):
                logger.error(
                    f"[Force Close] Exchange rejected cancellation of {order_label} order {order_info['order_id']}: {res}"
                )
                return False
            logger.info(
                f"[Force Close] Canceled pending {order_label} order {order_info['order_id']}"
            )
            return True
        except Exception:
            logger.exception(f"failed to cancel {order_label} order {order_info.get('order_id')} on force close")
            return False

    async def _cancel_pending_exit_for_force_close(self, pair_key: str):
        if await self._cancel_pending_order_for_force_close(
            pair_key, self.pending_exit_orders, "exit"
        ):
            self.pending_exit_orders.pop(pair_key, None)

    async def _cancel_pending_entry_for_force_close(self, pair_key: str):
        order_info = self.pending_entry_orders.get(pair_key)
        if not await self._cancel_pending_order_for_force_close(
            pair_key, self.pending_entry_orders, "entry"
        ):
            return
        if hasattr(self.client, "unlock_funds"):
            self.client.unlock_funds(
                float(order_info.get("spot_cost_usdt", 0.0)),
                float(order_info.get("futures_margin_usdt", 0.0)),
            )
            bals = self.client.get_balances()
            await self.redis.set(BALANCES_KEY, json.dumps(bals))
        self.pending_entry_orders.pop(pair_key, None)
        await self._clear_pending_entry(pair_key)

    async def _close_global_short_leg(self, pair_key: str, pos):
        """Closes the global futures short leg at market. Returns True on verified success."""
        global_sym = self._resolve_global_symbol(pos.symbol)
        logger.warning(f"[Force Close] Closing global short {pos.qty} on {global_sym}...")
        try:
            res = await self.client.close_global_short_market(
                global_sym,
                pos.qty,
                client_order_id=build_client_order_id(
                    pair_key, "force-close", self._next_order_seq()
                ),
            )
            # If the exchange returned an order object, verify status is not rejected/failed.
            # Treat None/non-dict as unverified — log a warning but allow the caller
            # to proceed; the position will be caught by RECONCILIATION_REQUIRED if
            # a later reconcile detects residual exposure.
            if res is None:
                logger.warning(
                    f"[Force Close] Futures close returned None for {pair_key}; treating as unverified success."
                )
            elif isinstance(res, dict):
                status = str(res.get("status", "")).lower()
                if status in ("rejected", "failed", "canceled"):
                    logger.error(f"[Force Close] Futures close order rejected/failed: {res}")
                    return False
            logger.info(f"[Force Close] Global short closed for {pos.qty} {global_sym}")
            return True
        except Exception:
            logger.exception("failed to close global short on force close")
            return False

    async def _liquidate_spot_leg(self, pair_key: str, pos) -> float:
        """
        Liquidates the local spot leg. Returns the realized exit price in IDR
        only if the order actually filled. Never falls back to entry price.
        """
        logger.warning(f"[Force Close] Selling {pos.qty} {pos.symbol} spot leg...")
        try:
            seq = self._next_order_seq()
            spot_order = await retry_with_backoff(
                lambda: self.client.place_local_market_sell(
                    pos.symbol,
                    pos.qty,
                    client_order_id=build_client_order_id(pair_key, "force-liq", seq),
                ),
                max_retries=4,  # 5 total attempts
                base_delay=0.5,
                label=f"force-close spot liquidation {pos.symbol} for {pair_key}",
            )
            # Verify actual fill and positive exit price. `amount` is the requested
            # quantity (not filled), so only use `filled` for fill verification.
            if isinstance(spot_order, dict):
                filled_qty = float(spot_order.get("filled", 0.0) or 0.0)
                status = str(spot_order.get("status", "")).lower()
                spot_exit_price_idr = self._extract_fill_price(spot_order)
                if (status == "closed" or filled_qty > 0) and spot_exit_price_idr > 0:
                    logger.info(
                        f"[Force Close] Spot leg liquidated: {pos.qty} {pos.symbol} "
                        f"@ {spot_exit_price_idr} (filled={filled_qty})"
                    )
                    return spot_exit_price_idr

            logger.critical(
                f"[Force Close] FAILED to liquidate spot leg for {pair_key}. "
                f"MANUAL INTERVENTION REQUIRED: {pos.qty} {pos.symbol} is UNHEDGED. Response: {spot_order}"
            )
        except RetryExhausted:
            logger.exception(
                f"[Force Close] spot liquidation exhausted retries for {pair_key}"
            )
        return 0.0

    async def _resolve_force_close_prices(self, pair_key: str, pos, spot_exit_price_idr: float):
        """Returns (current_ask_usdt, usdt_rate, current_spot_bid_idr) for PnL accounting."""
        current_ask = pos.short_entry_usdt
        usdt_rate = self.FALLBACK_USDT_IDR_RATE
        current_spot_bid = pos.entry_price_idr

        price_data = await self._fetch_live_price(pair_key, pos)
        if price_data:
            current_ask, usdt_rate = price_data

        live_price_raw = await self.redis.get(f"arb:live_price:{pair_key}")
        if live_price_raw:
            try:
                s_snap = json.loads(live_price_raw)
                if s_snap.get("local_best_bid"):
                    current_spot_bid = float(s_snap["local_best_bid"])
            except Exception:
                pass

        if spot_exit_price_idr > 0:
            current_spot_bid = spot_exit_price_idr
        return current_ask, usdt_rate, current_spot_bid

    @staticmethod
    def _compute_force_close_pnl(pos, current_ask: float, usdt_rate: float, current_spot_bid: float) -> dict:
        """Computes realized PnL components for a force-closed position."""
        spot_pnl_idr = pos.qty * (current_spot_bid - pos.entry_price_idr)
        spot_pnl_usdt = spot_pnl_idr / usdt_rate if usdt_rate > 0 else 0.0
        futures_pnl_usdt = pos.qty * (pos.short_entry_usdt - current_ask)
        funding_usdt = float(pos.accumulated_funding_usdt)
        fees_usdt = (pos.qty * (pos.short_entry_usdt + current_ask)) * 0.0005
        net_profit_usdt = spot_pnl_usdt + futures_pnl_usdt + funding_usdt - fees_usdt
        capital_usdt = pos.qty * pos.short_entry_usdt
        return {
            "spot_pnl_idr": spot_pnl_idr,
            "spot_pnl_usdt": spot_pnl_usdt,
            "futures_pnl_usdt": futures_pnl_usdt,
            "funding_usdt": funding_usdt,
            "fees_usdt": fees_usdt,
            "net_profit_usdt": net_profit_usdt,
            "net_profit_idr": net_profit_usdt * usdt_rate,
            "roi_pct": (net_profit_usdt / capital_usdt * 100.0) if capital_usdt > 0 else 0.0,
        }

    async def _record_force_close_trade(
        self,
        pair_key: str,
        pos,
        pnl: dict,
        current_ask: float,
        current_spot_bid: float,
        spot_sell_failed: bool,
        usdt_rate: float,
    ):
        """Persists the closed trade, settles balances, and updates stats/events."""
        closed_trade = {
            "trade_id": f"force-{int(time.time()*1000)}",
            "local_exchange": pos.local_exchange,
            "global_exchange": pos.global_exchange,
            "symbol": pos.symbol,
            "qty": pos.qty,
            "entry_price_idr": pos.entry_price_idr,
            "exit_price_idr": current_spot_bid,
            "short_entry_usdt": pos.short_entry_usdt,
            "short_exit_usdt": current_ask,
            "spot_pnl_idr": pnl["spot_pnl_idr"],
            "futures_pnl_usdt": pnl["futures_pnl_usdt"],
            "accumulated_funding_usdt": pnl["funding_usdt"],
            "net_profit_usdt": pnl["net_profit_usdt"],
            "net_profit_idr": pnl["net_profit_idr"],
            "net_roi_pct": pnl["roi_pct"],
            "is_mock": getattr(self.config, "mock_mode", False),
            "spot_liquidation_failed": spot_sell_failed,
            "closed_at_ms": int(time.time() * 1000),
        }
        await self.redis.lpush(ARB_TRADE_HISTORY_KEY, json.dumps(closed_trade))
        await self.redis.ltrim(
            ARB_TRADE_HISTORY_KEY, 0, ARB_TRADE_HISTORY_MAX_LEN
        )

        # Update exchange account balances with realized PnL & release locked funds
        spot_cost = getattr(pos, "spot_cost_usdt", pos.qty * pos.entry_price_idr / usdt_rate)
        fut_margin = getattr(pos, "futures_margin_usdt", pos.qty * pos.short_entry_usdt)
        if hasattr(self.client, "apply_realized_trade"):
            self.client.apply_realized_trade(
                spot_cost_usdt=spot_cost,
                spot_pnl_usdt=pnl["spot_pnl_usdt"],
                spot_fees_usdt=0.0,
                futures_margin_usdt=fut_margin,
                futures_pnl_usdt=pnl["futures_pnl_usdt"],
                funding_usdt=pnl["funding_usdt"],
                futures_fees_usdt=pnl["fees_usdt"],
            )
            bals = self.client.get_balances()
            await self.redis.set(BALANCES_KEY, json.dumps(bals))

        pipe = self.redis.pipeline()
        pipe.hincrby("arb:stats", "total_closed", 1)
        if pnl["net_profit_usdt"] > 0:
            pipe.hincrby("arb:stats", "win_count", 1)
        pipe.hincrbyfloat("arb:stats", "total_pnl_usdt", pnl["net_profit_usdt"])
        pipe.hincrbyfloat("arb:stats", "total_pnl_idr", pnl["net_profit_idr"])
        event_msg = {
            "event": "POSITION_CLOSED",
            **closed_trade,
        }
        pipe.publish(self.ARB_TRADE_EVENTS, json.dumps(event_msg))
        await pipe.execute()

    async def handle_force_close(self, message_data: str):
        """
        Handles FORCE_CLOSE directive from Telegram or API:
        1. Cancels pending exit/entry orders on local exchange.
        2. Closes open Short position on Global Futures via Market Buy (reduceOnly=True).
        3. Liquidates local spot coins if necessary.
        4. Removes active position from Redis.
        """
        logger.warning(f"Received FORCE_CLOSE directive: {message_data}")
        pair_key = self._parse_force_close_pair_key(message_data)
        if not pair_key:
            return

        # Serialize against the entry/exit paths for this pair: cancelling
        # pending orders and liquidating legs must not interleave with a
        # hedge or exit fill processed on another task.
        async with self._pair_lock(pair_key):
            await self._force_close_locked(pair_key)

    async def _cancel_pending_orders_for_force_close(self, pair_key: str) -> None:
        """Cancels pending exit and entry orders before closing position legs."""
        await self._cancel_pending_exit_for_force_close(pair_key)
        await self._cancel_pending_entry_for_force_close(pair_key)

    async def _execute_force_close_legs(self, pair_key: str, pos) -> Tuple[bool, bool, dict]:
        """
        Executes short closure on futures and spot liquidation.
        Returns (futures_closed, spot_sell_failed, pnl).
        """
        if not pos or pos.qty <= 0:
            return True, False, {"net_profit_usdt": 0.0, "net_profit_idr": 0.0}

        futures_closed = await self._close_global_short_leg(pair_key, pos)
        spot_sell_failed = True
        spot_exit_price_idr = 0.0

        if futures_closed:
            spot_exit_price_idr = await self._liquidate_spot_leg(pair_key, pos)
            spot_sell_failed = spot_exit_price_idr <= 0

        current_ask, usdt_rate, current_spot_bid = await self._resolve_force_close_prices(
            pair_key, pos, spot_exit_price_idr if futures_closed else 0.0
        )
        pnl = self._compute_force_close_pnl(pos, current_ask, usdt_rate, current_spot_bid)

        if futures_closed and not spot_sell_failed:
            await self._record_force_close_trade(
                pair_key,
                pos,
                pnl,
                current_ask,
                current_spot_bid,
                spot_sell_failed,
                usdt_rate,
            )
        elif futures_closed and spot_sell_failed:
            # Futures leg closed but spot liquidation failed — record partial close so
            # the futures PnL is not lost from history, and flag it clearly.
            logger.error(
                f"[Force Close] Recording partial force-close for {pair_key}: "
                f"futures closed, spot liquidation FAILED. Manual spot sale required."
            )
            await self._record_force_close_trade(
                pair_key,
                pos,
                pnl,
                current_ask,
                current_spot_bid,
                spot_sell_failed,
                usdt_rate,
            )

        return futures_closed, spot_sell_failed, pnl

    async def _handle_force_close_failure(
        self, pair_key: str, pos, futures_closed: bool, spot_sell_failed: bool
    ) -> None:
        """P0: Retains persistent state with RECONCILIATION_REQUIRED on partial or failed force close."""
        fail_reason = "FORCE_CLOSE_PARTIAL_OR_FAILED"
        fail_msg = (
            f"futures_closed={futures_closed}, spot_liquidated={not spot_sell_failed}. "
            "Manual reconciliation required; position retained in state."
        )
        if pos:
            pos.state = "RECONCILIATION_REQUIRED"
            pos.updated_at = int(time.time() * 1000)
            await self.redis.set(f"arb:active_pos:{pair_key}", json.dumps(pos.to_dict()))

        self._mark_failed(
            pair_key,
            fail_reason,
            fail_msg,
            detail={
                "qty": pos.qty if pos else 0.0,
                "symbol": pos.symbol if pos else "",
                "futures_closed": futures_closed,
                "spot_sell_failed": spot_sell_failed,
            },
            state=PositionState.RECONCILIATION_REQUIRED,
        )
        logger.critical(
            f"[Force Close Alert] {pair_key} incomplete! Retaining active position in Redis: {fail_msg}"
        )

    async def _handle_force_close_success(self, pair_key: str, pos, pnl: dict) -> None:
        """Cleans up active state in Redis and memory when force close fully succeeds."""
        await self.redis.delete(f"arb:active_pos:{pair_key}")
        self.active_positions.pop(pair_key, None)
        self.last_pegged_global_ask.pop(pair_key, None)
        if pos and pos.qty > 0:
            self.record_realized_pnl(pnl["net_profit_usdt"])
        self.states[pair_key] = PositionState.CLOSED
        logger.info(f"[Force Close Complete] Position for {pair_key} successfully closed.")

    async def _force_close_locked(self, pair_key: str) -> None:
        # Guard concurrent exit-fill processing for this pair while force close runs.
        self.states[pair_key] = PositionState.RECONCILIATION_REQUIRED
        await self._cancel_pending_orders_for_force_close(pair_key)
        pos = self.active_positions.get(pair_key)
        futures_closed, spot_sell_failed, pnl = await self._execute_force_close_legs(pair_key, pos)

        if not futures_closed or spot_sell_failed:
            await self._handle_force_close_failure(pair_key, pos, futures_closed, spot_sell_failed)
        else:
            await self._handle_force_close_success(pair_key, pos, pnl)

        await self.publish_status()
