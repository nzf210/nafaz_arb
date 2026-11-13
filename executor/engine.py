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
from executor.global_stop import GlobalStopService
from executor.exposure_reconciliation import ExposureReconciliationWorker
from executor.trade_fill_reconciliation import TradeFillReconciliationWorker
from executor.engine_order_intents import EngineOrderIntentMixin
from executor.engine_recovery import EngineRecoveryMixin
from executor.engine_runtime import EngineRuntimeMixin
from executor.engine_signal import EngineSignalMixin
from executor.engine_status import EngineStatusMixin
from executor.engine_entry_flow import EngineEntryFlowMixin
from executor.engine_fee_preflight import EngineFeePreflightMixin
from executor.engine_entry_sizing import EngineEntrySizingMixin
from executor.engine_entry_submission import EngineEntrySubmissionMixin

logger = logging.getLogger("executor.engine")

class ArbitrageEngine(
    EngineEntryFlowMixin,
    EngineFeePreflightMixin,
    EngineEntrySizingMixin,
    EngineEntrySubmissionMixin,
    EngineRuntimeMixin,
    EngineSignalMixin,
    EngineRecoveryMixin,
    EngineOrderIntentMixin,
    EngineStatusMixin,
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
        self._startup_time_ms = int(time.time() * 1000)
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
        # Serialize entry admission across different pairs sharing one spot CEX.
        self._spot_exchange_entry_locks: Dict[str, asyncio.Lock] = {}

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
        # Risk-control: consecutive execution failures per pair before 24h blacklist.
        self._consecutive_pair_failures: Dict[str, int] = {}
        self._pair_halted: Dict[str, str] = {}
        self._pair_cooldown_epoch: Dict[str, int] = {}
        self._pair_cooldown_tasks: Dict[str, asyncio.Task] = {}
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
        self.global_stop = (
            GlobalStopService(getattr(config, "postgres_dsn", ""), redis_client)
            if getattr(config, "postgres_dsn", "") else None
        )

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
        self._recovery_complete = await self._run_startup_recovery()
        if not self._recovery_complete:
            self._halt_exposure_scope(
                "*", "startup recovery did not acquire and complete the distributed lease"
            )
            self._refresh_entry_gate()
        logger.info(
            "[Recovery] startup reconciliation finished: ready=%s, pending=%d, orphans=%d",
            not self._entry_blocked, len(self._recovery_pending_orders), len(self._recovered_orphans),
        )


