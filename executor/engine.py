import asyncio
from contextvars import ContextVar
from decimal import Decimal, ROUND_DOWN
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
from executor.fee_engine import FeeRates, round_trip_fees_usdt
from executor.pair_registry import ExchangePairKey, PairClientRegistry
from executor.config import ExecutorConfig
from executor.private_stream import PrivateStreamCache
from executor.quantity import quantize_base_quantity

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
from executor.order_recovery import OrderRecoveryMixin
from executor.order_registry import UniversalOrderRegistry
from executor.models import OrderRecord, OrderRole
from executor.risk import RiskMixin
from executor.rollback import RollbackMixin
from executor.supabase import SupabaseWriter
from executor.exposure_reconciliation import ExposureReconciliationWorker
from executor.trade_fill_reconciliation import TradeFillReconciliationWorker

logger = logging.getLogger("executor.engine")

class ArbitrageEngine(
    CancelMixin,
    EntryMixin,
    ExitMixin,
    ForceCloseMixin,
    OrderRecoveryMixin,
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
        self._base_client = client
        self._client_context: ContextVar[Any] = ContextVar(f"executor_client_{id(self)}", default=None)
        self._pair_client_registry = PairClientRegistry()
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
        self._risk_state_restored = False
        self._risk_persistence_failed = False

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
        self._signal_queue_event = asyncio.Event()

        # Best-effort audit persistence. Writes are queued and flushed by a
        # background task, so an unreachable database can never delay a hedge.
        self.supabase = SupabaseWriter(getattr(config, "supabase", None))

        # Universal order registry: durable identity for every order the bot
        # submits, so a restart can re-attach venue orders to their positions.
        self.order_registry = UniversalOrderRegistry(self.redis, self.supabase)
        self._recovered_order_records: Dict[str, Any] = {}
        self._recovered_orphans: Dict[str, Any] = {}
        self._recovery_pending_orders: Dict[str, str] = {}
        # Entry gates installed by recovery. Monitoring, exits and emergency
        # de-risking stay available while these are set.
        self._recovery_scope_halts: Dict[str, str] = {}
        self._entry_blocked = False
        self._recovery_complete = False
        # Serializes periodic exchange reconciliation with itself; the normal
        # order monitor remains independent so risk management is never paused.
        self._exchange_reconciliation_lock = asyncio.Lock()
        self.exposure_reconciliation = ExposureReconciliationWorker(self)
        self.trade_fill_reconciliation = TradeFillReconciliationWorker(self)

    @property
    def client(self):
        """Task-local exchange facade; concurrent pairs never switch shared clients."""
        return self._client_context.get() or self._base_client

    @client.setter
    def client(self, value):
        self._base_client = value

    def _bind_pair_client(self, pair_info):
        key = ExchangePairKey.from_pair_info(pair_info)
        if getattr(self.config, "mock_mode", False):
            local_name = getattr(self._base_client, "local_name", None)
            global_name = getattr(self._base_client, "global_name", None)
            if local_name is not None and global_name is not None:
                configured = (str(local_name).lower(), str(global_name).lower())
                if configured != (key.local_exchange, key.global_exchange):
                    raise LookupError("paper client is configured for a different exchange pair")
            return self._client_context.set(self._base_client)
        resolver = getattr(self._base_client, "for_pair", None)
        if not callable(resolver):
            configured = (
                str(getattr(self.config, "local_exchange_name", "")).lower(),
                str(getattr(self.config, "global_exchange_name", "")).lower(),
            )
            if configured != (key.local_exchange, key.global_exchange):
                raise LookupError("client does not support pair-bound routing for this venue")
            if not self._pair_client_registry.contains(key):
                self._pair_client_registry.register(key, self._base_client)
            return self._client_context.set(self._pair_client_registry.resolve(key))
        if not self._pair_client_registry.contains(key):
            self._pair_client_registry.register(key, resolver(pair_info))
        return self._client_context.set(self._pair_client_registry.resolve(key))

    def _reset_pair_client(self, token) -> None:
        self._client_context.reset(token)

    async def initialize(self):
        """Initializes Redis and restores in-flight state before trading.

        Startup safety sequence:
        1. Redis, 2. Supabase, 3. exchange adapters (already constructed by
        ``main``), 4. durable order registry, 5. active positions, 6. venue open
        orders, 7. reconciliation, 8/9 handled by position verification and the
        exposure worker, 10. ``READY`` only when nothing is left unclassified.
        """
        if self.redis is None:
            self.redis = aioredis.from_url(
                self.config.redis_url,
                decode_responses=True,
            )
        self.order_registry.redis = self.redis
        # Start the writer before recovery so positions and orders restored from
        # Redis are re-registered in Supabase instead of only existing in Redis.
        await self.supabase.start()
        await self._restore_risk_state()
        await self._recover_active_positions()
        await self._recover_pending_entry_orders()
        await self._run_order_recovery()
        if not getattr(self.config, "mock_mode", False):
            await self._verify_recovered_active_positions()
        self._recovery_complete = True
        logger.info(
            "[Recovery] startup reconciliation finished: ready=%s, pending=%d, orphans=%d",
            not self._entry_blocked, len(self._recovery_pending_orders), len(self._recovered_orphans),
        )

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
                symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
                groups.setdefault((pos.global_exchange, symbol), []).append((pair_tag, pos))
            for (_, symbol), items in groups.items():
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
            global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            groups.setdefault((pos.global_exchange, global_symbol), []).append((pair_tag, pos))

        for (global_exchange, global_symbol), items in groups.items():
            expected_qty = sum(pos.qty for _, pos in items)
            token = None
            try:
                token = self._bind_pair_client(self._pair_info_for_tag(items[0][0]))
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
            finally:
                if token is not None:
                    self._reset_pair_client(token)

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

    async def _register_order(
        self,
        pair_tag: str,
        client_order_id: str,
        order: dict,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        role: str,
        side: str,
        order_type: str,
        price: float,
        quantity: float,
        reduce_only: bool = False,
        local_amount_spec: Any = None,
        global_amount_spec: Any = None,
        recovery_context: Any = None,
    ) -> Any:
        """Records one submitted order in the universal registry.

        Registration is best-effort and never raises: an unreachable Redis or
        Supabase must not turn an accepted order into a failed placement (which
        would make the caller retry and double-fill). The order remains visible
        to the next startup sweep through the venue itself.
        """
        if self.order_registry is None:
            return None
        try:
            record = OrderRecord(
                local_order_id=OrderRecord.local_id_for(
                    exchange, client_order_id, str(order.get("id") or "")
                ),
                client_order_id=client_order_id or "",
                exchange_order_id=str(order.get("id") or ""),
                pair_key=pair_tag,
                position_id=f"position:{pair_tag}" if pair_tag else "",
                exchange=str(exchange or "").lower(),
                market_type=market_type,
                symbol=symbol,
                side=side,
                order_type=order_type,
                order_role=role,
                price=float(price or 0.0),
                original_quantity=float(quantity or order.get("amount") or 0.0),
                filled_quantity=float(order.get("filled") or 0.0),
                remaining_quantity=max(
                    0.0,
                    float(quantity or order.get("amount") or 0.0)
                    - float(order.get("filled") or 0.0),
                ),
                average_fill_price=float(order.get("average") or 0.0),
                status=str(order.get("status") or "open"),
                reduce_only=bool(reduce_only),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
                recovery_context=recovery_context,
                local_amount_spec=(
                    local_amount_spec.to_dict()
                    if hasattr(local_amount_spec, "to_dict")
                    else local_amount_spec
                ),
                global_amount_spec=(
                    global_amount_spec.to_dict()
                    if hasattr(global_amount_spec, "to_dict")
                    else global_amount_spec
                ),
            )
            return await self.order_registry.register(record)
        except Exception:
            logger.exception(
                "[OrderRegistry] failed to register %s order %s for %s",
                role, client_order_id or order.get("id"), pair_tag,
            )
            return None

    async def _register_order_intent(
        self,
        pair_tag: str,
        client_order_id: str,
        *,
        exchange: str,
        market_type: str,
        symbol: str,
        role: str,
        side: str,
        order_type: str,
        price: float,
        quantity: float,
        reduce_only: bool = False,
        local_amount_spec: Any = None,
        global_amount_spec: Any = None,
        recovery_context: Any = None,
        required: bool = True,
    ) -> Any:
        """Durably records an order intent before submitting it to a venue.

        The client id is already known at this point. If the process dies after
        the exchange accepts the order but before it returns an exchange id, the
        startup open-order sweep can still bind the venue order to this intent.
        """
        # LIVE trading must never proceed with Redis-only order tracking. The
        # pre-submit intent is the last safe point to reject a configuration
        # that cannot durably journal every exchange order for restart recovery.
        if not bool(getattr(self.config, "mock_mode", False)) and not bool(
            getattr(self.supabase, "enabled", False)
        ):
            raise RuntimeError(
                "refusing LIVE order submission: Supabase durable order persistence is disabled"
            )
        record = await self._register_order(
            pair_tag,
            client_order_id,
            {"status": "submitting"},
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            role=role,
            side=side,
            order_type=order_type,
            price=price,
            quantity=quantity,
            reduce_only=reduce_only,
            local_amount_spec=local_amount_spec,
            global_amount_spec=global_amount_spec,
            recovery_context=recovery_context,
        )
        if required and record is None:
            raise RuntimeError(
                f"refusing to submit {role} order: durable order intent could not be recorded"
            )
        return record

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
                if (
                    "__" not in pair_tag
                    and order_info.get("local_exchange")
                    and order_info.get("global_exchange")
                    and order_info.get("global_symbol")
                ):
                    migrated_tag = ExchangePairKey.from_pair_info({
                        "local_exchange": order_info["local_exchange"],
                        "local_symbol": symbol,
                        "global_exchange": order_info["global_exchange"],
                        "global_symbol": order_info["global_symbol"],
                    }).tag.lower()
                    if migrated_tag != pair_tag:
                        migrated_key = PENDING_ENTRY_KEY_PREFIX + migrated_tag
                        if await self.redis.exists(migrated_key):
                            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                            self._mark_failed(
                                pair_tag,
                                "LEGACY_PENDING_KEY_COLLISION",
                                "Legacy pending entry cannot be migrated because its venue-qualified key already exists.",
                                detail={"legacy_key": key, "target_key": migrated_key},
                                state=PositionState.RECONCILIATION_REQUIRED,
                            )
                            continue
                        await self.redis.set(migrated_key, json.dumps(order_info))
                        await self.redis.delete(key)
                        pair_tag, key = migrated_tag, migrated_key
                self.pending_entry_orders[pair_tag] = order_info
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
                status, filled = await self._resolve_venue_order_state(order_id, symbol, pair_tag)
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
                    # Hedge any final fill on the exact venue before releasing reserves.
                    token = None
                    try:
                        if not getattr(self.config, "mock_mode", False):
                            token = self._bind_pair_client(self._pair_info_for_tag(pair_tag))
                        self.pending_entry_orders[pair_tag] = order_info
                        self.states[pair_tag] = PositionState.ENTRY_PENDING
                        # Resolve any already-submitted futures leg before
                        # the spot fill handler considers placing another hedge.
                        # This prevents a restart between futures fill and spot
                        # watermark persistence from doubling the short position.
                        if not await self._reconcile_pending_entry_hedges(pair_tag, order_info):
                            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                            self._mark_failed(
                                pair_tag,
                                "HEDGE_RECOVERY_UNVERIFIED",
                                "A previously submitted futures hedge could not be verified; "
                                "the spot fill was retained without submitting a duplicate hedge.",
                                detail={"order_id": order_id, "symbol": symbol},
                                state=PositionState.RECONCILIATION_REQUIRED,
                            )
                            continue
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
                    finally:
                        if token is not None:
                            self._reset_pair_client(token)
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

    async def _resolve_venue_order_state(
        self, order_id: str, symbol: str, pair_tag: Optional[str] = None
    ) -> tuple:
        """Returns (status, filled) using the exact local venue when known."""
        token = None
        try:
            if pair_tag and not getattr(self.config, "mock_mode", False):
                token = self._bind_pair_client(self._pair_info_for_tag(pair_tag))
            order = await self.client.fetch_local_order(order_id, symbol)
        except Exception as exc:
            logger.warning(f"[Recovery] fetch order {order_id} failed: {exc}")
            return "unknown", None
        finally:
            if token is not None:
                self._reset_pair_client(token)
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
                if "__" not in pair_tag and pos.global_exchange and pos.global_symbol:
                    migrated_tag = ExchangePairKey.from_pair_info({
                        "local_exchange": pos.local_exchange,
                        "local_symbol": pos.symbol,
                        "global_exchange": pos.global_exchange,
                        "global_symbol": pos.global_symbol,
                    }).tag.lower()
                    if migrated_tag != pair_tag:
                        migrated_key = f"arb:active_pos:{migrated_tag}"
                        existing = await self.redis.get(migrated_key)
                        if existing:
                            pos.state = "RECONCILIATION_REQUIRED"
                            self.active_positions[pair_tag] = pos
                            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                            await self.redis.set(key, json.dumps(pos.to_dict()))
                            self._mark_failed(
                                pair_tag,
                                "LEGACY_POSITION_KEY_COLLISION",
                                "Legacy position cannot be migrated because the venue-qualified key already exists.",
                                detail={"legacy_key": key, "target_key": migrated_key},
                                state=PositionState.RECONCILIATION_REQUIRED,
                            )
                            continue
                        await self.redis.set(migrated_key, json.dumps(pos.to_dict()))
                        await self.redis.delete(key)
                        pair_tag, key = migrated_tag, migrated_key
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
                self.supabase.position_opened(
                    pair_tag,
                    pos,
                    getattr(self, "_pair_infos", {}).get(pair_tag),
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
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

    async def _exchange_reconciliation_loop(self):
        """Periodically reconcile registered venue order states and cumulative fills.

        This complements startup recovery: an order can fill, be canceled, or
        an operator can change exposure while the process is running. The loop
        deliberately reuses the existing recovery and position-verification
        paths rather than introducing a second order lifecycle implementation.
        """
        try:
            interval = max(
                15.0,
                float(getattr(self.config, "exchange_reconciliation_interval_seconds", 60.0) or 60.0),
            )
        except (TypeError, ValueError):
            interval = 60.0
        while self._running:
            try:
                async with self._exchange_reconciliation_lock:
                    await self._run_order_recovery()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("[Reconciliation] periodic exchange reconciliation failed")
                self._halt_exposure_scope(
                    "*", f"exchange reconciliation failed: {type(exc).__name__}: {exc}"
                )
                self._refresh_entry_gate()
            await asyncio.sleep(interval)

    async def start(self):
        """Starts listeners and monitoring loops."""
        await self.initialize()
        # Verify spot inventory coverage and futures short exposure before
        # listeners can accept a new entry signal after startup.
        if not getattr(self.config, "mock_mode", False):
            await self.trade_fill_reconciliation.reconcile_once()
            await self.exposure_reconciliation.reconcile_once()
        self._running = True

        self._tasks.append(asyncio.create_task(self._listen_signals()))
        self._tasks.append(asyncio.create_task(self._process_signal_priority_queue()))
        self._tasks.append(asyncio.create_task(self._listen_emergency_cancel()))
        self._tasks.append(asyncio.create_task(self._listen_force_close()))
        self._tasks.append(asyncio.create_task(self._listen_kill_switch()))
        self._tasks.append(asyncio.create_task(self._listen_balance_requests()))
        self._tasks.append(asyncio.create_task(self._monitoring_loop()))
        self._tasks.append(asyncio.create_task(self._exchange_reconciliation_loop()))
        self._tasks.append(asyncio.create_task(self.exposure_reconciliation.run()))
        self._tasks.append(asyncio.create_task(self.trade_fill_reconciliation.run()))
        logger.info("Hybrid Arbitrage Engine started")

    async def stop(self):
        """Stops all running tasks and connections."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.supabase.enabled:
            await self.supabase.close()
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
                        self._signal_queue_event.set()
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:signal_notify")
            await pubsub.close()

    async def _process_signal_priority_queue(self):
        """Drain eligible signals in descending estimated net-profit order."""
        queue_key = "arb:signal_priority"
        # Coalesce a short burst so ZPOPMAX can rank concurrent signals, but
        # do not spend half the signal-freshness budget waiting for a batch.
        quiet_window = 0.02
        max_batch_window = 0.1
        while self._running:
            await self._signal_queue_event.wait()
            self._signal_queue_event.clear()
            loop = asyncio.get_running_loop()
            deadline = loop.time() + max_batch_window
            quiet_deadline = loop.time() + quiet_window
            while loop.time() < deadline:
                timeout = min(quiet_deadline - loop.time(), deadline - loop.time())
                if timeout <= 0:
                    break
                try:
                    await asyncio.wait_for(self._signal_queue_event.wait(), timeout=timeout)
                    self._signal_queue_event.clear()
                    quiet_deadline = loop.time() + quiet_window
                except asyncio.TimeoutError:
                    break

            while self._running:
                try:
                    ranked = await self.redis.zpopmax(queue_key, count=100)
                except Exception:
                    logger.exception("[Signal Priority] failed to claim ranked signals")
                    break
                if not ranked:
                    break
                for raw_tag, score in ranked:
                    pair_tag = raw_tag.decode("utf-8") if isinstance(raw_tag, bytes) else str(raw_tag)
                    logger.info(
                        "[Signal Priority] executing candidate pair=%s estimated_net_profit=%.8f USDT",
                        pair_tag, float(score),
                    )
                    try:
                        await self.handle_signal_notification(pair_tag)
                    except Exception:
                        logger.exception("[Signal Priority] signal handler failed for %s", pair_tag)

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


    async def _listen_balance_requests(self):
        """Handles read-only Telegram balance requests and stores short-lived replies."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:balance_request")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    try:
                        raw = message.get("data")
                        request = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                        request_id = str(request.get("request_id", "")).strip()
                        if not request_id or len(request_id) > 64 or not request_id.isalnum():
                            continue
                        reports = await self.client.fetch_balance_report()
                        payload = {"request_id": request_id, "timestamp_ms": int(time.time()*1000),
                                   "mock_mode": bool(getattr(self.config, "mock_mode", False)), "exchanges": reports}
                        await self.redis.set(f"arb:balance_reply:{request_id}", json.dumps(payload, allow_nan=False), ex=30)
                    except Exception:
                        logger.exception("failed to process Telegram balance request")
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:balance_request")
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
                self._persist_risk_state("trading_halt", scope="global", resolved=True)
                self._manual_resume_day = self._loss_day
                self._persist_risk_state("trading_resume", scope="global", reason="manual resume", value={"loss_day": self._loss_day})
                logger.warning(f"[Kill Switch] Trading resumed ({reason})")
                await self.publish_status()
            return

        if not self._trading_halted:
            self._trading_halted = True
            self._halt_reason = "KILL_SWITCH"
            self._persist_risk_state("trading_halt", scope="global", reason="KILL_SWITCH", value={"halt_reason": "KILL_SWITCH", "source": "operator"})
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

    def _pair_info_for_tag(self, pair_tag: str):
        info = self._pair_infos.get(pair_tag)
        if info is not None:
            return info
        pending = self.pending_entry_orders.get(pair_tag) or self.pending_exit_orders.get(pair_tag) or {}
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            local_symbol = pos.symbol
            global_symbol = pos.global_symbol
            return type("RecoveredPairInfo", (), {
                "local_exchange": pos.local_exchange,
                "global_exchange": pos.global_exchange,
                "local_symbol": local_symbol,
                "global_symbol": global_symbol or self._resolve_global_symbol(local_symbol),
                "base_asset": pos.base_asset or local_symbol.split("/")[0],
                "quote_asset": local_symbol.split("/")[-1] if "/" in local_symbol else "IDR",
            })()
        if pending.get("local_exchange") and pending.get("global_exchange"):
            return type("PendingPairInfo", (), {
                "local_exchange": pending["local_exchange"],
                "global_exchange": pending["global_exchange"],
                "local_symbol": pending.get("symbol", ""),
                "global_symbol": pending.get("global_symbol", ""),
                "base_asset": pending.get("base_asset", ""),
                "quote_asset": pending.get("quote_asset", "IDR"),
            })()
        raise LookupError(f"exchange pair metadata unavailable for {pair_tag}")

    async def _run_pair_bound(self, pair_tag: str, operation, *args, **kwargs):
        if getattr(self.config, "mock_mode", False):
            return await operation(*args, **kwargs)
        pair_info = self._pair_info_for_tag(pair_tag)
        token = self._bind_pair_client(pair_info)
        try:
            return await operation(*args, **kwargs)
        finally:
            self._reset_pair_client(token)

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
        """Bind the signal to its immutable venue pair before any exchange call."""
        payload = await self._load_signal_payload(pair_tag)
        if payload is None:
            return
        if getattr(self.config, "mock_mode", False):
            await self._handle_signal_notification_bound(pair_tag, payload)
            return
        try:
            token = self._bind_pair_client(payload.pair_info)
        except Exception as exc:
            self._pair_infos[pair_tag] = payload.pair_info
            self._mark_failed(
                pair_tag,
                "EXECUTOR_VENUE_MISMATCH",
                f"Signal rejected: {exc}",
                detail={
                    "local_exchange": payload.pair_info.local_exchange,
                    "global_exchange": payload.pair_info.global_exchange,
                },
            )
            await self.publish_status()
            return
        try:
            await self._handle_signal_notification_bound(pair_tag, payload)
        finally:
            self._reset_pair_client(token)

    async def _handle_signal_notification_bound(
        self, pair_tag: str, payload: Optional[ExecutorPayload] = None
    ):
        """
        State 1 (Entry Order Pegging):
        1. GET arb:signal:{local_ex}:{symbol}
        2. Validate timestamp freshness (<= 500ms).
        3. If no active position, place Maker Limit Buy at recommended_limit_buy_idr.
        """
        async with self._pair_lock(pair_tag):
            if getattr(self, "_risk_persistence_failed", False):
                logger.critical("[Risk] rejecting %s: durable risk-state persistence is not trustworthy", pair_tag)
                return
            self._roll_loss_day_if_needed()
            if payload is None:
                payload = await self._load_signal_payload(pair_tag)
            if payload is None:
                return
            configured_local = str(self.config.local_exchange_name or "").strip().lower()
            configured_global = str(self.config.global_exchange_name or "").strip().lower()
            signal_local = str(payload.pair_info.local_exchange or "").strip().lower()
            signal_global = str(payload.pair_info.global_exchange or "").strip().lower()
            pair_key = ExchangePairKey.from_pair_info(payload.pair_info)
            if (
                not getattr(self.config, "mock_mode", False)
                and not self._pair_client_registry.contains(pair_key)
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
            mock_mode = bool(getattr(self.config, "mock_mode", False))
            if mock_mode:
                await self._check_balance_mismatch(
                    payload.pair_info,
                    payload.pricing_snapshot.usdt_idr_rate,
                    pair_tag,
                )
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
        # Refresh account-specific rates before any new LIVE order. If a venue
        # lacks an authenticated fee endpoint, retain the explicitly supplied
        # scanner profile and keep its source/verification status unchanged.
        if not bool(getattr(self.config, "mock_mode", False)) and hasattr(self.client, "fetch_pair_fee_rates"):
            try:
                cached_profiles = {}
                def fee_cache_key(exchange_name, market_name, symbol):
                    normalized = str(symbol).lower().replace("/", "_").replace("-", "_").replace(":", "_")
                    return f"arb:fee_profile:{exchange_name.lower()}:{market_name}:{normalized}"

                if self.redis is not None:
                    for market_name, exchange_name, symbol in (
                        ("spot", payload.pair_info.local_exchange, payload.pair_info.local_symbol),
                        ("perp", payload.pair_info.global_exchange, payload.pair_info.global_symbol),
                    ):
                        cached_raw = await self.redis.get(
                            fee_cache_key(exchange_name, market_name, symbol)
                        )
                        if not cached_raw:
                            continue
                        try:
                            cached_profile = json.loads(cached_raw)
                            fetched_at = int(cached_profile.get("fetched_at_ms", 0))
                            age_ms = int(time.time() * 1000) - fetched_at
                            if (
                                cached_profile.get("source") == "EXCHANGE_API"
                                and cached_profile.get("verified") is True
                                and 0 <= age_ms <= 300_000
                            ):
                                cached_profiles[market_name] = {
                                    "maker": float(cached_profile["maker_fee_pct"]),
                                    "taker": float(cached_profile["taker_fee_pct"]),
                                    "fetched_at_ms": fetched_at,
                                }
                        except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                            logger.warning("[Fee Engine] ignoring malformed cached %s fee profile", market_name)

                fee_snapshot = await self.client.fetch_pair_fee_rates(
                    payload.pair_info, cached_profiles
                )
                spot_rates = fee_snapshot.get("spot")
                perp_rates = fee_snapshot.get("perp")
                maker_taker = str(getattr(self.config, "execution_mode", "TAKER_TAKER")).upper()
                if spot_rates is not None:
                    directives.spot_entry_fee_pct = (
                        spot_rates["maker"] if maker_taker == "MAKER_TAKER" else spot_rates["taker"]
                    )
                    directives.spot_exit_fee_pct = spot_rates["taker"]
                if perp_rates is not None:
                    directives.perp_entry_fee_pct = perp_rates["taker"]
                    directives.perp_exit_fee_pct = perp_rates["taker"]
                directives.fee_source = fee_snapshot["source"]
                directives.fee_verified = bool(fee_snapshot["verified"])
                directives.fee_fetched_at_ms = int(fee_snapshot["fetched_at_ms"])
                if self.redis is not None:
                    if spot_rates is not None:
                        await self.redis.set(
                            f"arb:fee_profile:{payload.pair_info.local_exchange.lower()}:spot:{payload.pair_info.local_symbol.lower().replace(chr(47), chr(95)).replace(chr(45), chr(95)).replace(chr(58), chr(95))}",
                            json.dumps({
                                "maker_fee_pct": spot_rates["maker"],
                                "taker_fee_pct": spot_rates["taker"],
                                "source": "EXCHANGE_API",
                                "verified": True,
                                "fetched_at_ms": int(spot_rates["fetched_at_ms"]),
                            }),
                            ex=300,
                        )
                    if perp_rates is not None:
                        await self.redis.set(
                            f"arb:fee_profile:{payload.pair_info.global_exchange.lower()}:perp:{payload.pair_info.global_symbol.lower().replace(chr(47), chr(95)).replace(chr(45), chr(95)).replace(chr(58), chr(95))}",
                            json.dumps({
                                "maker_fee_pct": perp_rates["maker"],
                                "taker_fee_pct": perp_rates["taker"],
                                "source": "EXCHANGE_API",
                                "verified": True,
                                "fetched_at_ms": int(perp_rates["fetched_at_ms"]),
                            }),
                            ex=300,
                        )
                logger.info(
                    "[Fee Engine] refreshed account fee tiers for %s: spot=%s perp=%s",
                    pair_tag, spot_rates, perp_rates,
                )
            except Exception as fee_error:
                logger.warning(
                    "[Fee Engine] authenticated fee refresh unavailable for %s; "
                    "using scanner fee snapshot (%s, verified=%s): %s",
                    pair_tag, directives.fee_source, directives.fee_verified, fee_error,
                )
        if directives.fee_source in ("EXCHANGE_API", "PARTIAL_EXCHANGE_API") and directives.quantity > 0:
            try:
                dynamic_fees = round_trip_fees_usdt(
                    directives.quantity,
                    directives.spot_entry_price_usdt,
                    directives.spot_exit_price_usdt,
                    directives.perp_entry_price_usdt,
                    directives.perp_exit_price_usdt,
                    FeeRates(
                        directives.spot_entry_fee_pct,
                        directives.spot_exit_fee_pct,
                        directives.perp_entry_fee_pct,
                        directives.perp_exit_fee_pct,
                        directives.fee_source,
                        directives.fee_verified,
                        directives.fee_fetched_at_ms,
                        directives.fee_asset_surcharge_pct,
                    ),
                )
                directives.estimated_net_profit_usdt += (
                    directives.estimated_fees_usdt - dynamic_fees
                )
                directives.estimated_fees_usdt = dynamic_fees
                if directives.estimated_net_profit_usdt <= 0:
                    self._mark_failed(
                        pair_tag,
                        "NET_EDGE_ERODED_BY_ACTUAL_FEES",
                        "Entry rejected: authenticated account fee tiers reduce estimated net profit to zero or below.",
                        detail={
                            "fee_source": directives.fee_source,
                            "estimated_fees_usdt": dynamic_fees,
                            "adjusted_net_profit_usdt": directives.estimated_net_profit_usdt,
                        },
                    )
                    await self.publish_status()
                    return
            except (TypeError, ValueError) as fee_error:
                self._mark_failed(
                    pair_tag,
                    "FEE_REVALIDATION_FAILED",
                    "Entry rejected because the authenticated fee snapshot could not be applied safely.",
                    detail={"error": str(fee_error), "fee_source": directives.fee_source},
                )
                await self.publish_status()
                return

        pair_info = payload.pair_info
        snapshot = payload.pricing_snapshot
        if pair_info.local_amount_spec is None or pair_info.global_amount_spec is None:
            self._mark_failed(
                pair_tag,
                "MARKET_AMOUNT_RULES_MISSING",
                "Spot and perpetual market amount rules are required; entry was not submitted.",
                detail={
                    "local_symbol": pair_info.local_symbol,
                    "global_symbol": pair_info.global_symbol,
                },
            )
            await self.publish_status()
            return
        if pair_info.local_amount_spec.unit != "BASE":
            self._mark_failed(
                pair_tag,
                "LOCAL_AMOUNT_UNIT_UNSUPPORTED",
                "Spot market amount rules must be expressed in base-asset units.",
                detail={"local_symbol": pair_info.local_symbol},
            )
            await self.publish_status()
            return

        # Target Limit Buy price in IDR
        buy_price_idr = directives.recommended_limit_buy_idr
        if buy_price_idr <= 0:
            return

        sizing = await self._compute_entry_sizing(pair_tag, payload)
        if sizing is None:
            return
        max_pos_usdt, _ = sizing

        capital_idr = Decimal(str(max_pos_usdt)) * Decimal(str(snapshot.usdt_idr_rate))
        desired_qty = float(capital_idr / Decimal(str(buy_price_idr)))
        try:
            buy_qty = quantize_base_quantity(
                desired_qty,
                pair_info.local_amount_spec,
                pair_info.global_amount_spec,
            )
        except ValueError as exc:
            self._mark_failed(
                pair_tag,
                "MARKET_AMOUNT_RULES_REJECT_SIZE",
                "Requested position cannot satisfy both market amount rules.",
                detail={"requested_base_qty": desired_qty, "error": str(exc)},
            )
            await self.publish_status()
            return

        if buy_qty <= 0:
            return

        actual_cost_usdt = (buy_qty * buy_price_idr) / snapshot.usdt_idr_rate

        # Both legs are committed at entry: the spot buy locks the local quote
        # currency and the 1x futures short locks margin on the global venue.
        # Verify BOTH are funded before submitting, so a live run cannot end up
        # with a filled spot leg and an unhedged (or margin-rejected) short leg.
        if not getattr(self.config, "mock_mode", False):
            funded = await self._verify_dual_leg_funding(
                pair_tag, pair_info, actual_cost_usdt, snapshot.usdt_idr_rate
            )
            if not funded:
                return

        # Paper trading reserves simulated funds locally. Live orders are sent
        # directly to the venue so its response is authoritative for availability.
        if getattr(self.config, "mock_mode", False) and hasattr(self.client, "lock_funds"):
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
            placed_time = time.time()
            await self._register_order_intent(
                pair_tag,
                entry_coid,
                exchange=pair_info.local_exchange,
                market_type="spot",
                symbol=pair_info.local_symbol,
                role=OrderRole.ENTRY.value,
                side="buy",
                order_type="limit",
                price=buy_price_idr,
                quantity=buy_qty,
                local_amount_spec=pair_info.local_amount_spec,
                global_amount_spec=pair_info.global_amount_spec,
                recovery_context={
                    "usdt_idr_rate": snapshot.usdt_idr_rate,
                    "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                    "global_ask_usdt": snapshot.global_best_ask_usdt,
                    "global_symbol": pair_info.global_symbol,
                    "base_asset": pair_info.base_asset,
                    "quote_asset": pair_info.quote_asset,
                    "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                    "min_entry_spread_pct": directives.min_entry_spread_pct,
                    "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                    "current_funding_rate": payload.funding_metrics.current_funding_rate,
                    "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                    "spot_entry_fee_pct": directives.spot_entry_fee_pct,
                    "spot_exit_fee_pct": directives.spot_exit_fee_pct,
                    "perp_entry_fee_pct": directives.perp_entry_fee_pct,
                    "perp_exit_fee_pct": directives.perp_exit_fee_pct,
                    "fee_source": directives.fee_source,
                    "fee_verified": directives.fee_verified,
                    "fee_fetched_at_ms": directives.fee_fetched_at_ms,
                    "fee_asset_surcharge_pct": directives.fee_asset_surcharge_pct,
                    "spot_cost_usdt": actual_cost_usdt,
                    "futures_margin_usdt": actual_cost_usdt,
                },
            )
            order = await self.client.place_local_limit_buy(
                pair_info.local_symbol,
                buy_qty,
                buy_price_idr,
                client_order_id=entry_coid,
                amount_spec=pair_info.local_amount_spec,
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
                submitted_at_ms=int(placed_time * 1000),
            )
            await self._register_order(
                pair_tag,
                entry_coid,
                order,
                exchange=pair_info.local_exchange,
                market_type="spot",
                symbol=pair_info.local_symbol,
                role=OrderRole.ENTRY.value,
                side="buy",
                order_type="limit",
                price=buy_price_idr,
                quantity=buy_qty,
                local_amount_spec=pair_info.local_amount_spec,
                global_amount_spec=pair_info.global_amount_spec,
                # A fill must never be priced from a guessed FX rate or fee tier.
                # Persisting the exact snapshot makes a post-restart hedge
                # reproducible instead of requiring manual reconciliation.
                recovery_context={
                    "usdt_idr_rate": snapshot.usdt_idr_rate,
                    "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                    "global_ask_usdt": snapshot.global_best_ask_usdt,
                    "global_symbol": pair_info.global_symbol,
                    "base_asset": pair_info.base_asset,
                    "quote_asset": pair_info.quote_asset,
                    "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                    "min_entry_spread_pct": directives.min_entry_spread_pct,
                    "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                    "current_funding_rate": payload.funding_metrics.current_funding_rate,
                    "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                    "spot_entry_fee_pct": directives.spot_entry_fee_pct,
                    "spot_exit_fee_pct": directives.spot_exit_fee_pct,
                    "perp_entry_fee_pct": directives.perp_entry_fee_pct,
                    "perp_exit_fee_pct": directives.perp_exit_fee_pct,
                    "fee_source": directives.fee_source,
                    "fee_verified": directives.fee_verified,
                    "fee_fetched_at_ms": directives.fee_fetched_at_ms,
                    "fee_asset_surcharge_pct": directives.fee_asset_surcharge_pct,
                    "spot_cost_usdt": actual_cost_usdt,
                    "futures_margin_usdt": actual_cost_usdt,
                },
            )
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            self.pending_entry_orders[pair_tag] = {
                "order_id": order_id,
                "client_order_id": entry_coid,
                "symbol": pair_info.local_symbol,
                "global_symbol": pair_info.global_symbol,
                "base_asset": pair_info.base_asset,
                "local_lot_size": pair_info.local_lot_size,
                "local_amount_spec": pair_info.local_amount_spec.to_dict(),
                "global_amount_spec": pair_info.global_amount_spec.to_dict(),
                "local_exchange": pair_info.local_exchange,
                "global_exchange": pair_info.global_exchange,
                "price": buy_price_idr,
                "qty": buy_qty,
                "filled": 0.0,
                "hedged_short_qty": 0.0,
                "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                "min_entry_spread_pct": (
                    float(self.config.entry_revalidation_min_spread_pct)
                    if float(getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0) > 0
                    else float(directives.min_entry_spread_pct or 0.0)
                ),
                "tick_size": pair_info.local_price_tick,
                "quote_asset": pair_info.quote_asset,
                "usdt_idr_rate": snapshot.usdt_idr_rate,
                "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                "global_ask_usdt": snapshot.global_best_ask_usdt,
                "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                "current_funding_rate": payload.funding_metrics.current_funding_rate,
                "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                "spot_entry_fee_pct": directives.spot_entry_fee_pct,
                "spot_exit_fee_pct": directives.spot_exit_fee_pct,
                "perp_entry_fee_pct": directives.perp_entry_fee_pct,
                "perp_exit_fee_pct": directives.perp_exit_fee_pct,
                "fee_source": directives.fee_source,
                "fee_verified": directives.fee_verified,
                "fee_fetched_at_ms": directives.fee_fetched_at_ms,
                "fee_asset_surcharge_pct": directives.fee_asset_surcharge_pct,
                "placed_time": placed_time,
                "spot_cost_usdt": actual_cost_usdt,
                "futures_margin_usdt": actual_cost_usdt,
            }
            await self._persist_pending_entry(pair_tag, self.pending_entry_orders[pair_tag])
        except Exception as exc:
            if (
                getattr(self.config, "mock_mode", False)
                and hasattr(self.client, "unlock_funds")
            ):
                self.client.unlock_funds(actual_cost_usdt, actual_cost_usdt)
            exchange_error = str(exc).strip() or type(exc).__name__
            normalized_error = f"{type(exc).__name__} {exchange_error}".lower()

            # A venue throttle is not a venue fault and not a business rejection:
            # pause new entries on that venue for a bounded cooldown, keep managing
            # open positions, and let the cooldown lapse on its own.
            is_rate_limited = (
                type(exc).__name__ == "IndodaxRateLimitError"
                or "http_429" in normalized_error
                or "http_503" in normalized_error
                or "rate limit" in normalized_error
                or "too many requests" in normalized_error
            )
            if is_rate_limited:
                cooldown = float(
                    getattr(exc, "retry_after", None)
                    or getattr(self.config, "venue_rate_limit_cooldown_seconds", 30.0)
                )
                self._pause_venue_for_cooldown(pair_info.local_exchange, cooldown)
                logger.warning(
                    f"[Risk Control] {pair_info.local_exchange} rate limited on entry "
                    f"placement; pausing new entries for {cooldown:.1f}s"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "VENUE_RATE_LIMITED",
                    f"Venue {pair_info.local_exchange} membatasi laju request; "
                    f"entry dijeda {cooldown:.1f}s.",
                    detail={
                        "exchange": pair_info.local_exchange,
                        "symbol": pair_info.local_symbol,
                        "cooldown_seconds": cooldown,
                        "error_type": type(exc).__name__,
                        "exchange_error": exchange_error[:500],
                    },
                )
                return

            insufficient_balance = any(
                marker in normalized_error
                for marker in (
                    "insufficientfunds",
                    "insufficient balance",
                    "insufficient funds",
                    "insufficient margin",
                    "not enough balance",
                    "not enough funds",
                    "saldo tidak cukup",
                    "saldo tidak mencukupi",
                )
            )
            min_notional = any(
                marker in normalized_error
                for marker in (
                    "min_notional",
                    "min notional",
                    "minimum notional",
                    "notional too small",
                    "cost.min",
                )
            )
            invalid_symbol = any(
                marker in normalized_error
                for marker in (
                    "invalid_symbol",
                    "invalid symbol",
                    "unknown symbol",
                    "market is not available",
                    "symbol not found",
                )
            )
            if insufficient_balance:
                reason = "INSUFFICIENT_BALANCE"
            elif min_notional:
                reason = "MIN_NOTIONAL"
            elif invalid_symbol:
                reason = "INVALID_SYMBOL"
            else:
                reason = "SPOT_ORDER_OPEN_FAILED"

            # Business rejections reject this opportunity, but do not count as
            # exchange-health failures. Transport/API/internal errors still count.
            failure_category = (
                reason if reason in ("INSUFFICIENT_BALANCE", "MIN_NOTIONAL", "INVALID_SYMBOL")
                else "entry order placement failed"
            )
            self._record_order_failure(pair_info.local_exchange, failure_category)
            logger.exception(f"[State 1] failed to place limit buy for {pair_tag}")
            await self._publish_execution_failure(
                pair_tag,
                reason,
                f"Exchange menolak order Spot: {exchange_error[:500]}",
                detail={
                    "exchange": pair_info.local_exchange,
                    "symbol": pair_info.local_symbol,
                    "error_type": type(exc).__name__,
                    "exchange_error": exchange_error[:1000],
                },
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
        self._persist_risk_state("position_failure", scope="position", pair_key=pair_tag, reason=reason, value={**entry, "state": state.value})
        if state == PositionState.RECONCILIATION_REQUIRED:
            self._persist_risk_state("reconciliation_halt", scope="pair", pair_key=pair_tag, reason=reason, value={"source": "position_failure"})
            self._recovery_scope_halts[pair_tag] = reason
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
        self.supabase.event(
            "EXECUTION_FAILURE",
            pair_key=pair_tag,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
            occurred_at_ms=payload["timestamp_ms"],
            payload=payload,
        )

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
        self._manual_resume_day = None
        self._persist_risk_state("trading_resume", scope="global", resolved=True)
        self._persist_risk_state("daily_loss", scope="global", value={"loss_usdt": 0.0, "loss_day": today})
        # The operator kill switch does not auto-reset on a new UTC day.
        if (
            self._trading_halted
            and self._halt_reason == "DAILY_LOSS_LIMIT"
        ):
            self._trading_halted = False
            self._halt_reason = ""
            self._persist_risk_state("trading_halt", scope="global", resolved=True)
            logger.warning("[Circuit Breaker] New UTC day - trading resumed")

    def record_realized_pnl(self, net_profit_usdt: float, event_id: Optional[str] = None, pair_key: str = "unknown") -> None:
        """
        Feeds a closed trade's net PnL into the daily loss breaker.

        Losses beyond max_daily_loss_usdt halt *new entries* only: open positions
        keep being managed, because abandoning them unhedged would be worse than
        the loss the breaker is trying to stop.
        """
        self._roll_loss_day_if_needed()
        if event_id and getattr(self.supabase, "enabled", False):
            result = self.supabase.risk_pnl_event(
                event_id, pair_key=pair_key, pnl_usdt=float(net_profit_usdt),
                loss_day=self._loss_day,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
            if result == "duplicate":
                logger.info("[Risk] duplicate realized PnL event ignored: %s", event_id)
                return
            if result == "failed":
                self._risk_persistence_failed = True
                self._entry_blocked = True
                logger.critical("[Risk] PnL event durability failed; new entries fail-closed")
        if net_profit_usdt < 0:
            self._daily_realized_loss_usdt += -net_profit_usdt
        self._persist_risk_state("daily_loss", scope="global", value={"loss_usdt": self._daily_realized_loss_usdt, "loss_day": self._loss_day})
        if event_id:
            self._persist_risk_state("trading_resume", scope="global", resolved=True)
            self._manual_resume_day = None

        limit = getattr(self.config, "max_daily_loss_usdt", 0.0)
        if limit <= 0 or self._trading_halted:
            return
        if self._daily_realized_loss_usdt >= limit:
            self._trading_halted = True
            self._halt_reason = "DAILY_LOSS_LIMIT"
            self._persist_risk_state("trading_halt", scope="global", reason="DAILY_LOSS_LIMIT", value={"halt_reason": "DAILY_LOSS_LIMIT", "loss_usdt": self._daily_realized_loss_usdt, "loss_day": self._loss_day})
            logger.critical(
                f"[Circuit Breaker] HALTED new entries: daily realized loss "
                f"{self._daily_realized_loss_usdt:.4f} USDT >= limit {limit:.4f} USDT"
            )

    async def resume_trading(self, reason: str = "MANUAL") -> None:
        """Clears the halt flag; used by the operator once a loss is reviewed."""
        self._trading_halted = False
        self._halt_reason = ""
        self._persist_risk_state("trading_halt", scope="global", resolved=True)
        self._manual_resume_day = self._loss_day
        self._persist_risk_state("trading_resume", scope="global", reason=reason, value={"loss_day": self._loss_day})
        logger.warning(f"[Circuit Breaker] Trading resumed manually ({reason})")
        await self.publish_status()

    def clear_failure(self, pair_tag: str) -> None:
        """Clears a non-reconciliation failure; safety incidents need explicit reconciliation."""
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            logger.warning("[Risk] refusing to clear unresolved reconciliation failure for %s", pair_tag)
            return
        if self._failure_reasons.pop(pair_tag, None) is not None:
            self._persist_risk_state("position_failure", scope="position", pair_key=pair_tag, resolved=True)
            if self.states.get(pair_tag) == PositionState.FAILED:
                self.states[pair_tag] = PositionState.IDLE

    def _pending_order_snapshots(self) -> list:
        """Returns read-only metadata for managed spot orders awaiting resolution."""
        orders = []
        for leg, source in (("SPOT_ENTRY", self.pending_entry_orders), ("SPOT_EXIT", self.pending_exit_orders)):
            for pair_tag, order in source.items():
                if not isinstance(order, dict):
                    continue
                position = self.active_positions.get(pair_tag)
                state = self.states.get(pair_tag)
                if state == PositionState.RECONCILIATION_REQUIRED:
                    status = "RECONCILIATION_REQUIRED"
                else:
                    filled = max(0.0, float(order.get("filled", 0.0) or 0.0))
                    status = "PARTIALLY_FILLED" if filled > 0 else "OPEN"
                quantity = max(0.0, float(order.get("qty", 0.0) or 0.0))
                filled = max(0.0, float(order.get("filled", 0.0) or 0.0))
                placed_at = order.get("placed_time", 0) or order.get("created_at", 0) or 0
                try:
                    created_at_ms = int(float(placed_at) * 1000) if float(placed_at) < 100000000000 else int(float(placed_at))
                except (TypeError, ValueError):
                    created_at_ms = 0
                symbol = str(order.get("symbol", ""))
                inferred_quote = symbol.replace("_", "/").split("/")[-1] if "/" in symbol or "_" in symbol else ""
                orders.append({
                    "pair_tag": pair_tag,
                    "leg": leg,
                    "order_id": str(order.get("order_id", "")),
                    "symbol": symbol,
                    "quote_asset": str(order.get("quote_asset") or inferred_quote),
                    "local_exchange": str(order.get("local_exchange") or getattr(position, "local_exchange", "") or self.config.local_exchange_name),
                    "global_exchange": str(order.get("global_exchange") or getattr(position, "global_exchange", "") or self.config.global_exchange_name),
                    "price": float(order.get("price", 0.0) or 0.0),
                    "qty": quantity,
                    "filled": filled,
                    "remaining": max(0.0, quantity - filled),
                    "status": status,
                    "created_at_ms": created_at_ms,
                })
        return sorted(orders, key=lambda item: (item["created_at_ms"], item["pair_tag"]))

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
            "pending_orders": self._pending_order_snapshots(),
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
            "recovery": {
                "complete": bool(getattr(self, "_recovery_complete", False)),
                "ready": not bool(getattr(self, "_entry_blocked", False)),
                "pending_orders": dict(getattr(self, "_recovery_pending_orders", {}) or {}),
                "scope_halts": dict(getattr(self, "_recovery_scope_halts", {}) or {}),
                "tracked_orders": len(getattr(self, "_recovered_order_records", {}) or {}),
                "orphan_orders": list((getattr(self, "_recovered_orphans", {}) or {}).values()),
            },
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
                token = None
                try:
                    if not getattr(self.config, "mock_mode", False):
                        token = self._bind_pair_client(self._pair_info_for_tag(p_key))
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
                except Exception:
                    logger.exception(f"[Emergency Cancel] pair routing failed for {p_key}")
                finally:
                    if token is not None:
                        self._reset_pair_client(token)
