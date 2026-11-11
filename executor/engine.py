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
from executor.engine_order_intents import EngineOrderIntentMixin
from executor.engine_recovery import EngineRecoveryMixin
from executor.engine_runtime import EngineRuntimeMixin
from executor.engine_signal import EngineSignalMixin
from executor.engine_status import EngineStatusMixin

logger = logging.getLogger("executor.engine")

class ArbitrageEngine(
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

