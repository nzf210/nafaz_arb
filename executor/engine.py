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
from executor.helpers import PENDING_ENTRY_KEY_PREFIX
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
from executor.reconciliation import ReconciliationMixin
from executor.recovery import RecoveryMixin
from executor.lifecycle import LifecycleMixin
from executor.signal_handler import SignalHandlerMixin
from executor.status import StatusMixin
from executor.entry import EntryMixin
from executor.exits import ExitMixin
from executor.force_close import ForceCloseMixin
from executor.risk import RiskMixin
from executor.rollback import RollbackMixin
from executor.supabase import SupabaseWriter

logger = logging.getLogger("executor.engine")


class ArbitrageEngine(
    ReconciliationMixin,
    RecoveryMixin,
    LifecycleMixin,
    SignalHandlerMixin,
    StatusMixin,
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
        # Serialize entry preflight + order placement across every signal sharing
        # either venue. Redis locks extend this protection across executor replicas.
        self._venue_locks: Dict[str, asyncio.Lock] = {}

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

        # Best-effort audit persistence. Writes are queued and flushed by a
        # background task, so an unreachable database can never delay a hedge.
        self.supabase = SupabaseWriter(getattr(config, "supabase", None))

    async def initialize(self):
        """Initializes Redis connection and restores in-flight state from Redis."""
        if self.redis is None:
            self.redis = aioredis.from_url(
                self.config.redis_url,
                decode_responses=True,
            )
        # Start the writer before recovery so positions restored from Redis are
        # re-registered in Supabase instead of only existing in Redis.
        await self.supabase.start()
        await self._recover_active_positions()
        await self._recover_pending_entry_orders()
        if not getattr(self.config, "mock_mode", False):
            await self._verify_recovered_active_positions()
            await self._verify_account_wide_exchange_state()

