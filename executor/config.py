import os
import json
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


@dataclass
class ExecutionModuleConfig:
    """Config for a currency/quote execution module (e.g. spot_idr, spot_usdt)."""
    id: str
    quote: str
    spot_venues: List[str] = field(default_factory=list)
    hedge_venues: List[str] = field(default_factory=list)
    enabled: bool = True
    capital_usdt: float = 0.0
    max_position_usdt: float = 0.0
    max_slots_per_venue: int = 1


@dataclass
class ExchangeCredentials:
    api_key: str = ""
    api_secret: str = ""
    passphrase: Optional[str] = None
    testnet: bool = False
    options: Dict[str, Any] = field(default_factory=dict)
    account_scope_id: str = ""


@dataclass
class SupabaseConfig:
    enabled: bool = False
    url: str = ""
    key: str = ""
    outbox_path: str = "/app/data/executor/supabase-outbox.sqlite3"

@dataclass
class HedgingConfig:
    """Incremental futures-hedging policy (spec: spot fill threshold + incremental hedge).

    The fill threshold is a percentage of the spot target quantity reached by
    cumulative confirmed spot fills; it is a one-time activation condition for
    an entry, after which every partial fill continues to update the hedge.
    """

    enabled: bool = True
    mode: str = "incremental"  # only "incremental" is supported
    spot_fill_trigger_pct: float = 20.0  # cumulative spot fill % that activates hedging
    futures_target_hedge_ratio: float = 1.0  # futures qty per unit of unhedged spot exposure
    futures_min_order_policy: str = "wait_until_covered"  # or "allow_min_notional"
    futures_allow_min_notional_overhedge: bool = False
    futures_max_overhedge_pct: float = 0.0
    execution_hedge_on_partial_fill: bool = True
    execution_use_cumulative_spot_fills: bool = True
    execution_use_confirmed_futures_fills: bool = True
    execution_prevent_duplicate_hedges: bool = True
    execution_reconcile_unknown_orders_before_retry: bool = True
    execution_durable_hedge_intent: bool = True

    _MODES = ("incremental",)
    _MIN_ORDER_POLICIES = ("wait_until_covered", "allow_min_notional")

    def sanitize(self) -> list:
        warnings = []
        if self.mode not in self._MODES:
            warnings.append(
                f"hedging.mode={self.mode!r} unsupported; using 'incremental'"
            )
            self.mode = "incremental"
        for field_name, minimum, maximum, fallback in (
            ("spot_fill_trigger_pct", 0.0, 100.0, 20.0),
            ("futures_target_hedge_ratio", 0.0, 10.0, 1.0),
            ("futures_max_overhedge_pct", 0.0, 100.0, 0.0),
        ):
            raw = getattr(self, field_name)
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = None
            if value is None or value != value:  # unparseable or NaN
                value = fallback
            value = max(minimum, min(maximum, value))
            if value != raw:
                warnings.append(f"hedging.{field_name}={raw!r} out of range, using {value!r}")
            setattr(self, field_name, value)
        if self.futures_min_order_policy not in self._MIN_ORDER_POLICIES:
            warnings.append(
                f"hedging.futures_min_order_policy={self.futures_min_order_policy!r} "
                "unsupported; using 'wait_until_covered'"
            )
            self.futures_min_order_policy = "wait_until_covered"
        if self.futures_allow_min_notional_overhedge:
            warnings.append(
                "hedging.futures_allow_min_notional_overhedge=true permits deliberate "
                "overhedge up to futures_max_overhedge_pct; confirm this is intentional"
            )
        return warnings


@dataclass
class ExecutorConfig:
    redis_url: str = "redis://localhost:6380/0"
    max_stale_latency_ms: int = 2500
    min_partial_fill_usd: float = 10.0
    exit_price_move_threshold: float = 0.0015  # 0.15%
    max_order_ttl_seconds: int = 120  # Max TTL for pending maker entry orders before cancel
    heartbeat_interval_seconds: float = 1.0  # Independent heartbeat loop cadence in seconds
    # Refresh stale, unfilled maker quotes by canceling them and waiting for a fresh scanner signal.
    maker_refresh_after_seconds: int = 8
    maker_refresh_cooldown_seconds: int = 12
    # Minimum projected net return retained by Indodax IDR adaptive maker quotes.
    adaptive_maker_min_net_profit_pct: float = 0.10
    indodax_maker_refresh_after_seconds: int = 15
    indodax_maker_refresh_cooldown_seconds: int = 15
    mock_mode: bool = True  # Paper trading / simulation by default
    initial_exchange_balance_usdt: float = 100.0  # $100 per exchange
    capital_total: float = 0.0  # Total capital budget across pairs
    allocation_per_pair: float = 0.5  # Max 50% capital per pair
    min_allocated_capital: float = 10.0  # Minimum capital to enter position
    max_position_usdt: float = 0.0  # Cap on position size per leg in USDT (0 = use signal directive)
    # Hard stop: halt all new entries once the day's realized loss crosses this.
    # 0 disables the breaker.
    max_daily_loss_usdt: float = 50.0
    max_drawdown_pct: float = 0.0
    max_funding_cost_pct: float = 0.0
    max_unhedged_duration_ms: int = 0
    min_venue_health_score: float = 0.2
    # Abort the hedge if the live futures ask is worse than the expected short
    # price by more than this fraction (0.003 = 0.3%). 0 disables the guard.
    max_entry_slippage_pct: float = 0.003
    # Re-quote the global futures bid just before placing the maker buy and
    # reject the signal when the entry spread has collapsed below the scanner's
    # own minimum. 0 disables the guard (default: on at 0.0 = use payload floor).
    entry_revalidation_min_spread_pct: float = 0.0
    # Cap on total capital committed per exchange, across all active positions.
    # 0 disables the check.
    max_exposure_per_exchange_usdt: float = 0.0
    # Maximum distinct open spot positions plus pending spot-entry orders per CEX.
    # A missing venue override always falls back to the safe default of two.
    max_open_spot_orders_per_exchange: Dict[str, int] = field(default_factory=dict)
    # Balance-mismatch guard for simulated entry snapshots. Live entries do not
    # poll balances before submission; the venue validates available funds.
    balance_mismatch_fraction: float = 0.20
    # Consecutive order-placement failures tolerated per exchange before the
    # venue is halted for new entries. 0 disables the breaker.
    max_consecutive_order_failures: int = 50
    # Round-trip taker/maker fees charged on both legs, used to shrink the
    # position size so the exit can still net a profit. 0 disables the buffer.
    estimated_round_trip_fee_pct: float = 0.001
    # Use a private order/balance feed instead of polling REST for every check.
    # Off by default: the feed is an optimisation, and REST polling is the
    # proven path. When on, stale or missing entries still fall back to REST.
    private_stream_enabled: bool = False
    private_stream_max_age_seconds: float = 5.0
    executor_instance_id: str = ""
    emergency_derisk_owner_id: str = ""
    emergency_derisk_lease_ttl_ms: int = 120000
    # Fraction of accumulated funding retained for realized PnL; only the
    # complementary fraction may decay the target exit spread.
    funding_decay_retention_ratio: float = 0.60
    # Monitoring loop cadence. The venue rate-limits per IP, so a sub-second tick
    # with several pending orders drains the quota and the order-status poll
    # starts answering HTTP 429 with an HTML body. 500ms stays responsive while
    # leaving headroom for the pair fan-out.
    monitor_interval_seconds: float = 0.5
    # Consecutive order-status fetch failures tolerated for one pending entry
    # before it is force-reconciled, so a venue outage cannot pin a pending order
    # (and its reserved funds) open indefinitely.
    max_order_fetch_failures: int = 10
    # Exponential backoff ceiling for order-status polls that keep failing.
    order_fetch_backoff_max_seconds: float = 60.0
    # A pricing HTTP 429 on the local venue (rate limit / throttle) pauses new
    # entry placement for this long instead of hammering the endpoint.
    venue_rate_limit_cooldown_seconds: float = 30.0
    # Cadence for background periodic reconciliation of resting orders with exchange.
    exchange_reconciliation_interval_seconds: float = 60.0
    # Consecutive execution failures on a pair before it is blacklisted
    # for cooldown. 0 disables the auto-blacklist.
    max_pair_failure_threshold: int = 2
    # Duration to blacklist a pair once the failure threshold is reached (default 24h).
    pair_blacklist_cooldown_seconds: float = 86400.0
    local_exchange_name: str = "indodax"
    global_exchange_name: str = "okx"
    execution_mode: str = "TAKER_TAKER"
    exchanges: Dict[str, ExchangeCredentials] = field(default_factory=dict)
    local_credentials: ExchangeCredentials = field(default_factory=ExchangeCredentials)
    global_credentials: ExchangeCredentials = field(default_factory=ExchangeCredentials)
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)
    # Incremental futures-hedging policy for spot entry fills.
    hedging: HedgingConfig = field(default_factory=HedgingConfig)
    # Direct PostgreSQL DSN for transactional global STOP and submit fencing.
    postgres_dsn: str = ""
    # Execution modules keyed by module ID (e.g. "spot_idr", "spot_usdt").
    modules: Dict[str, ExecutionModuleConfig] = field(default_factory=dict)

    def module_for(self, spot_venue: str, quote: str) -> Optional[ExecutionModuleConfig]:
        """Resolves the execution module configured for a (spot_venue, quote) pair."""
        norm_venue = str(spot_venue or "").strip().lower()
        norm_quote = str(quote or "").strip().upper()
        if norm_quote == "BIDR":
            norm_quote = "IDR"
        for mod in self.modules.values():
            mod_quote = str(mod.quote or "").strip().upper()
            if mod_quote == "BIDR":
                mod_quote = "IDR"
            if norm_quote == mod_quote and norm_venue in [v.lower() for v in mod.spot_venues]:
                return mod
        # An explicit module map is an allowlist. Do not silently create an
        # enabled fallback for an unconfigured venue/quote combination.
        if self.modules:
            return None
        # Dynamic fallback is retained only for legacy configs with no modules.
        fallback_id = f"spot_{norm_quote.lower()}"
        return ExecutionModuleConfig(
            id=fallback_id,
            quote=norm_quote,
            spot_venues=[norm_venue] if norm_venue else [],
            enabled=True,
        )

    def _apply_file(self, config_path: str) -> None:
        if not os.path.exists(config_path):
            return

        try:
            with open(config_path, "r") as f:
                data = json.load(f)

            # Keep scanner and executor modes aligned: setting root config mode to LIVE
            # activates real exchange adapters here too. EXECUTOR_MOCK_MODE remains an override.
            scanner_mode = str(data.get("mode", "MOCK")).strip().upper()
            self.mock_mode = scanner_mode != "LIVE"

            redis_cfg = data.get("redis", {})
            if addr := redis_cfg.get("addr"):
                password = redis_cfg.get("password", "")
                auth_part = f":{password}@" if password else ""
                db = redis_cfg.get("db", 0)
                self.redis_url = f"redis://{auth_part}{addr}/{db}"

            self.execution_mode = str(data.get("execution_mode", self.execution_mode)).strip().upper()

            # Check root-level config values as fallbacks
            if "capital_total" in data:
                self.capital_total = float(data["capital_total"])
            if "allocation_per_pair" in data:
                self.allocation_per_pair = float(data["allocation_per_pair"])
            if "min_allocated_capital" in data:
                self.min_allocated_capital = float(data["min_allocated_capital"])

            exec_cfg = data.get("executor", {})
            for key, conv in (
                ("min_partial_fill_usd", float),
                ("exit_price_move_threshold", float),
                ("max_stale_latency_ms", int),
                ("max_order_ttl_seconds", int),
                ("maker_refresh_after_seconds", int),
                ("maker_refresh_cooldown_seconds", int),
                ("adaptive_maker_min_net_profit_pct", float),
                ("indodax_maker_refresh_after_seconds", int),
                ("indodax_maker_refresh_cooldown_seconds", int),
                ("initial_exchange_balance_usdt", float),
                ("allocation_per_pair", float),
                ("min_allocated_capital", float),
                ("max_position_usdt", float),
                ("max_daily_loss_usdt", float),
                ("max_drawdown_pct", float),
                ("max_funding_cost_pct", float),
                ("max_unhedged_duration_ms", int),
                ("min_venue_health_score", float),
                ("max_entry_slippage_pct", float),
                ("entry_revalidation_min_spread_pct", float),
                ("max_exposure_per_exchange_usdt", float),
                ("balance_mismatch_fraction", float),
                ("max_consecutive_order_failures", int),
                ("estimated_round_trip_fee_pct", float),
                ("mock_mode", bool),
                ("private_stream_enabled", bool),
                ("private_stream_max_age_seconds", float),
                ("executor_instance_id", str),
                ("emergency_derisk_owner_id", str),
                ("emergency_derisk_lease_ttl_ms", int),
                ("funding_decay_retention_ratio", float),
                ("monitor_interval_seconds", float),
                ("heartbeat_interval_seconds", float),
                ("max_order_fetch_failures", int),
                ("order_fetch_backoff_max_seconds", float),
                ("venue_rate_limit_cooldown_seconds", float),
                ("exchange_reconciliation_interval_seconds", float),
                ("max_pair_failure_threshold", int),
                ("pair_blacklist_cooldown_seconds", float),
            ):
                if key in exec_cfg:
                    setattr(self, key, conv(exec_cfg[key]))
            limits = exec_cfg.get("max_open_spot_orders_per_exchange", {})
            if isinstance(limits, dict):
                self.max_open_spot_orders_per_exchange = {
                    str(name).strip().lower(): limit
                    for name, limit in limits.items()
                }
            if pg_dsn := data.get("postgres_dsn") or exec_cfg.get("postgres_dsn"):
                if not self.postgres_dsn:
                    self.postgres_dsn = str(pg_dsn).strip()

            # Load modular exchanges dictionary if provided in config
            if "exchanges" in data and isinstance(data["exchanges"], dict):
                for ex_name, ex_data in data["exchanges"].items():
                    if isinstance(ex_data, dict):
                        self.exchanges[ex_name.lower()] = ExchangeCredentials(
                            api_key=str(ex_data.get("api_key", "")),
                            api_secret=str(ex_data.get("api_secret", "")),
                            passphrase=ex_data.get("passphrase"),
                            testnet=bool(ex_data.get("testnet", False)),
                            account_scope_id=str(ex_data.get("account_scope_id", "") or ""),
                            options=dict(ex_data.get("options", {})),
                        )

            # Load Supabase configuration if present
            if "supabase" in data and isinstance(data["supabase"], dict):
                sup_data = data["supabase"]
                self.supabase.enabled = bool(sup_data.get("enabled", False))
                self.supabase.url = str(sup_data.get("url", ""))
                self.supabase.key = str(sup_data.get("key", ""))
                self.supabase.outbox_path = str(
                    sup_data.get("outbox_path", self.supabase.outbox_path)
                )

            # Load incremental-hedging policy if present
            if "hedging" in data and isinstance(data["hedging"], dict):
                hed = data["hedging"]
                hed_spot = hed.get("spot", {}) if isinstance(hed.get("spot"), dict) else {}
                hed_fut = hed.get("futures", {}) if isinstance(hed.get("futures"), dict) else {}
                hed_exec = hed.get("execution", {}) if isinstance(hed.get("execution"), dict) else {}
                cfg_map = {
                    "enabled": ("enabled", bool),
                    "mode": ("mode", str),
                    "spot": {"fill_trigger_pct": ("spot_fill_trigger_pct", float)},
                    "futures": {
                        "target_hedge_ratio": ("futures_target_hedge_ratio", float),
                        "min_order_policy": ("futures_min_order_policy", str),
                        "allow_min_notional_overhedge": ("futures_allow_min_notional_overhedge", bool),
                        "max_overhedge_pct": ("futures_max_overhedge_pct", float),
                    },
                    "execution": {
                        "hedge_on_partial_fill": ("execution_hedge_on_partial_fill", bool),
                        "use_cumulative_spot_fills": ("execution_use_cumulative_spot_fills", bool),
                        "use_confirmed_futures_fills": ("execution_use_confirmed_futures_fills", bool),
                        "prevent_duplicate_hedges": ("execution_prevent_duplicate_hedges", bool),
                        "reconcile_unknown_orders_before_retry": ("execution_reconcile_unknown_orders_before_retry", bool),
                        "durable_hedge_intent": ("execution_durable_hedge_intent", bool),
                    },
                }
                for top_key, spec in cfg_map.items():
                    if top_key in ("spot", "futures", "execution"):
                        section = hed.get(top_key, {})
                        if not isinstance(section, dict):
                            continue
                        for src_key, (attr, conv) in spec.items():
                            if src_key in section:
                                setattr(self.hedging, attr, conv(section[src_key]))
                    else:
                        attr, conv = spec
                        if top_key in hed:
                            setattr(self.hedging, attr, conv(hed[top_key]))

            # Load execution modules dictionary if present
            if "modules" in data and isinstance(data["modules"], dict):
                for mod_id, mod_data in data["modules"].items():
                    if isinstance(mod_data, dict):
                        self.modules[str(mod_id)] = ExecutionModuleConfig(
                            id=str(mod_id),
                            quote=str(mod_data.get("quote", "USDT")).upper(),
                            spot_venues=[str(v).lower() for v in mod_data.get("spot_venues", [])],
                            hedge_venues=[str(v).lower() for v in mod_data.get("hedge_venues", [])],
                            enabled=bool(mod_data.get("enabled", True)),
                            capital_usdt=float(mod_data.get("capital_usdt", 0.0) or 0.0),
                            max_position_usdt=float(mod_data.get("max_position_usdt", 0.0) or 0.0),
                            max_slots_per_venue=int(mod_data.get("max_slots_per_venue", 1)),
                        )
        except Exception as e:
            print(f"warning: error loading config file: {e}")

    def _apply_env(self) -> None:
        if redis_env := os.getenv("REDIS_URL"):
            self.redis_url = redis_env

        if postgres_dsn := os.getenv("POSTGRES_DSN"):
            self.postgres_dsn = postgres_dsn

        if mode := os.getenv("EXECUTION_MODE"):
            self.execution_mode = mode.strip().upper()

        if local_ex := os.getenv("LOCAL_EXCHANGE"):
            self.local_exchange_name = local_ex

        if global_ex := os.getenv("GLOBAL_EXCHANGE"):
            self.global_exchange_name = global_ex

        if mock_env := os.getenv("EXECUTOR_MOCK_MODE"):
            self.mock_mode = mock_env.lower() in ("true", "1", "yes")

        if cap_env := os.getenv("CAPITAL_TOTAL"):
            try:
                self.capital_total = float(cap_env)
            except ValueError:
                pass

        if max_pos_env := os.getenv("MAX_POSITION_USDT"):
            try:
                self.max_position_usdt = float(max_pos_env)
            except ValueError:
                pass

        if ws_env := os.getenv("PRIVATE_STREAM_ENABLED"):
            self.private_stream_enabled = ws_env.lower() in ("true", "1", "yes")

        if ws_age_env := os.getenv("PRIVATE_STREAM_MAX_AGE_SECONDS"):
            try:
                self.private_stream_max_age_seconds = float(ws_age_env)
            except ValueError:
                pass

        if monitor_env := os.getenv("MONITOR_INTERVAL_SECONDS"):
            try:
                self.monitor_interval_seconds = float(monitor_env)
            except ValueError:
                pass

        if fetch_fail_env := os.getenv("MAX_ORDER_FETCH_FAILURES"):
            try:
                self.max_order_fetch_failures = int(fetch_fail_env)
            except ValueError:
                pass

        if fetch_backoff_env := os.getenv("ORDER_FETCH_BACKOFF_MAX_SECONDS"):
            try:
                self.order_fetch_backoff_max_seconds = float(fetch_backoff_env)
            except ValueError:
                pass

        if rl_cooldown_env := os.getenv("VENUE_RATE_LIMIT_COOLDOWN_SECONDS"):
            try:
                self.venue_rate_limit_cooldown_seconds = float(rl_cooldown_env)
            except ValueError:
                pass

        if ttl_env := os.getenv("MAX_ORDER_TTL_SECONDS"):
            try:
                self.max_order_ttl_seconds = int(ttl_env)
            except ValueError:
                pass

        if recon_env := os.getenv("EXCHANGE_RECONCILIATION_INTERVAL_SECONDS"):
            try:
                self.exchange_reconciliation_interval_seconds = float(recon_env)
            except ValueError:
                pass

        if refresh_env := os.getenv("MAKER_REFRESH_AFTER_SECONDS"):
            try:
                self.maker_refresh_after_seconds = int(refresh_env)
            except ValueError:
                pass

        if refresh_cooldown_env := os.getenv("MAKER_REFRESH_COOLDOWN_SECONDS"):
            try:
                self.maker_refresh_cooldown_seconds = int(refresh_cooldown_env)
            except ValueError:
                pass

        if adaptive_profit_env := os.getenv("ADAPTIVE_MAKER_MIN_NET_PROFIT_PCT"):
            try:
                self.adaptive_maker_min_net_profit_pct = float(adaptive_profit_env)
            except ValueError:
                pass

        if indodax_refresh_env := os.getenv("INDODAX_MAKER_REFRESH_AFTER_SECONDS"):
            try:
                self.indodax_maker_refresh_after_seconds = int(indodax_refresh_env)
            except ValueError:
                pass

        if indodax_cooldown_env := os.getenv("INDODAX_MAKER_REFRESH_COOLDOWN_SECONDS"):
            try:
                self.indodax_maker_refresh_cooldown_seconds = int(indodax_cooldown_env)
            except ValueError:
                pass

        if loss_env := os.getenv("MAX_DAILY_LOSS_USDT"):
            try:
                self.max_daily_loss_usdt = float(loss_env)
            except ValueError:
                pass

        for env_name, attr, caster in (
            ("MAX_DRAWDOWN_PCT", "max_drawdown_pct", float),
            ("MAX_FUNDING_COST_PCT", "max_funding_cost_pct", float),
            ("MAX_UNHEDGED_DURATION_MS", "max_unhedged_duration_ms", int),
            ("MIN_VENUE_HEALTH_SCORE", "min_venue_health_score", float),
        ):
            if raw_value := os.getenv(env_name):
                try:
                    setattr(self, attr, caster(raw_value))
                except ValueError:
                    pass

        if instance_id := os.getenv("EXECUTOR_INSTANCE_ID"):
            self.executor_instance_id = instance_id.strip()
        if emergency_owner := os.getenv("EMERGENCY_DERISK_OWNER_ID"):
            self.emergency_derisk_owner_id = emergency_owner.strip()
        if emergency_ttl := os.getenv("EMERGENCY_DERISK_LEASE_TTL_MS"):
            try:
                self.emergency_derisk_lease_ttl_ms = int(emergency_ttl)
            except ValueError:
                pass

        if slippage_env := os.getenv("MAX_ENTRY_SLIPPAGE_PCT"):
            try:
                self.max_entry_slippage_pct = float(slippage_env)
            except ValueError:
                pass

        if rev_env := os.getenv("ENTRY_REVALIDATION_MIN_SPREAD_PCT"):
            try:
                self.entry_revalidation_min_spread_pct = float(rev_env)
            except ValueError:
                pass

        if exposure_env := os.getenv("MAX_EXPOSURE_PER_EXCHANGE_USDT"):
            try:
                self.max_exposure_per_exchange_usdt = float(exposure_env)
            except ValueError:
                pass

        if mismatch_env := os.getenv("BALANCE_MISMATCH_FRACTION"):
            try:
                self.balance_mismatch_fraction = float(mismatch_env)
            except ValueError:
                pass

        if fail_env := os.getenv("MAX_CONSECUTIVE_ORDER_FAILURES"):
            try:
                self.max_consecutive_order_failures = int(fail_env)
            except ValueError:
                pass

        if fee_env := os.getenv("ESTIMATED_ROUND_TRIP_FEE_PCT"):
            try:
                self.estimated_round_trip_fee_pct = float(fee_env)
            except ValueError:
                pass

        self.local_credentials.api_key = os.getenv("LOCAL_API_KEY", self.local_credentials.api_key)
        self.local_credentials.api_secret = os.getenv("LOCAL_API_SECRET", self.local_credentials.api_secret)

        self.global_credentials.api_key = os.getenv("GLOBAL_API_KEY", self.global_credentials.api_key)
        self.global_credentials.api_secret = os.getenv("GLOBAL_API_SECRET", self.global_credentials.api_secret)
        self.global_credentials.passphrase = os.getenv("GLOBAL_PASSPHRASE", self.global_credentials.passphrase)

        if os.getenv("GLOBAL_TESTNET", "").lower() in ("true", "1"):
            self.global_credentials.testnet = True

        # Scan environment variables dynamically for any <EXCHANGE>_API_KEY / <EXCHANGE>_API_SECRET
        # e.g., OKX_API_KEY, BYBIT_API_KEY, MEXC_API_KEY, BINANCE_API_KEY, INDODAX_API_KEY
        for env_k, env_v in os.environ.items():
            if env_k.endswith("_API_KEY") and not env_k.startswith("LOCAL_") and not env_k.startswith("GLOBAL_"):
                prefix = env_k[:-8].lower()
                cred = self.exchanges.setdefault(prefix, ExchangeCredentials())
                cred.api_key = env_v
                if secret := os.getenv(f"{env_k[:-8]}_API_SECRET"):
                    cred.api_secret = secret
                if passp := os.getenv(f"{env_k[:-8]}_PASSPHRASE"):
                    cred.passphrase = passp
                if testnet_val := os.getenv(f"{env_k[:-8]}_TESTNET"):
                    cred.testnet = testnet_val.lower() in ("true", "1", "yes")
                if broker_id := os.getenv(f"{env_k[:-8]}_BROKER_ID"):
                    # Binance Cloud broker id (Tokocrypto); travels into the ccxt
                    # client options so exchange_client can prefix client order ids.
                    cred.options.setdefault("broker_id", broker_id)

        # Sync local and global credentials from modular exchanges if available
        if self.local_exchange_name.lower() in self.exchanges:
            src = self.exchanges[self.local_exchange_name.lower()]
            if src.api_key:
                self.local_credentials.api_key = src.api_key
            if src.api_secret:
                self.local_credentials.api_secret = src.api_secret
            if src.passphrase:
                self.local_credentials.passphrase = src.passphrase
            if src.testnet:
                self.local_credentials.testnet = src.testnet
            if src.options:
                self.local_credentials.options.update(src.options)
        else:
            self.exchanges[self.local_exchange_name.lower()] = self.local_credentials

        if self.global_exchange_name.lower() in self.exchanges:
            src = self.exchanges[self.global_exchange_name.lower()]
            if src.api_key:
                self.global_credentials.api_key = src.api_key
            if src.api_secret:
                self.global_credentials.api_secret = src.api_secret
            if src.passphrase:
                self.global_credentials.passphrase = src.passphrase
            if src.testnet:
                self.global_credentials.testnet = src.testnet
            if src.options:
                self.global_credentials.options.update(src.options)
        else:
            self.exchanges[self.global_exchange_name.lower()] = self.global_credentials

        if sup_url := os.getenv("SUPABASE_URL"):
            self.supabase.url = sup_url.strip()
        if sup_key := os.getenv("SUPABASE_KEY"):
            self.supabase.key = sup_key.strip()
        if sup_enabled := os.getenv("SUPABASE_ENABLED"):
            self.supabase.enabled = sup_enabled.lower() in ("true", "1", "yes")
        elif not self.supabase.enabled:
            # Explicitly keep disabled if config.json set enabled: false
            self.supabase.enabled = False
        else:
            self.supabase.enabled = bool(self.supabase.url and self.supabase.key)
        if outbox_path := os.getenv("SUPABASE_OUTBOX_PATH"):
            self.supabase.outbox_path = outbox_path

    # Bounds applied by sanitize(): (field, minimum, maximum, default when invalid).
    # A bad value is replaced by the default rather than aborting the boot, so a
    # typo in config.json can never leave the executor sizing positions at 0 or
    # accepting unlimited risk.
    _NUMERIC_BOUNDS = {
        "max_stale_latency_ms": (1, 60000, 500),
        "max_order_ttl_seconds": (1, 86400, 120),
        "maker_refresh_after_seconds": (0, 86400, 8),
        "maker_refresh_cooldown_seconds": (1, 86400, 12),
        "indodax_maker_refresh_after_seconds": (0, 86400, 15),
        "indodax_maker_refresh_cooldown_seconds": (1, 86400, 15),
        "adaptive_maker_min_net_profit_pct": (0.0, 10.0, 0.10),
        "min_partial_fill_usd": (0.0, None, 10.0),
        "exit_price_move_threshold": (0.0, 1.0, 0.0015),
        "initial_exchange_balance_usdt": (0.01, None, 100.0),
        "allocation_per_pair": (0.0, 1.0, 0.5),
        "min_allocated_capital": (0.0, None, 10.0),
        "max_daily_loss_usdt": (0.0, None, 50.0),
        "max_drawdown_pct": (0.0, 100.0, 0.0),
        "max_funding_cost_pct": (0.0, 10.0, 0.0),
        "max_unhedged_duration_ms": (0, 86400000, 0),
        "min_venue_health_score": (0.0, 1.0, 0.2),
        "emergency_derisk_lease_ttl_ms": (1000, 300000, 120000),
        "max_entry_slippage_pct": (0.0, 1.0, 0.003),
        "entry_revalidation_min_spread_pct": (0.0, 100.0, 0.0),
        "max_exposure_per_exchange_usdt": (0.0, None, 0.0),
        "balance_mismatch_fraction": (0.0, 1.0, 0.20),
        "max_consecutive_order_failures": (0, 100, 50),
        "estimated_round_trip_fee_pct": (0.0, 1.0, 0.001),
        "private_stream_max_age_seconds": (0.5, 60.0, 5.0),
        "funding_decay_retention_ratio": (0.0, 1.0, 0.60),
        "monitor_interval_seconds": (0.05, 60.0, 0.5),
        "max_order_fetch_failures": (1, 1000, 10),
        "order_fetch_backoff_max_seconds": (0.1, 600.0, 60.0),
        "venue_rate_limit_cooldown_seconds": (1.0, 3600.0, 30.0),
    }

    def sanitize(self) -> list:
        """
        Coerces every numeric knob into its safe range.

        Returns a list of human-readable warnings describing each correction.
        """
        warnings = []
        for name, (minimum, maximum, fallback) in self._NUMERIC_BOUNDS.items():
            raw = getattr(self, name)
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = None
            if value is None or value != value:  # unparseable or NaN
                value = fallback
            if minimum is not None and value < minimum:
                value = minimum
                if minimum <= 0:
                    # Only 0 is a legal "disabled" value, so anything below the
                    # minimum (e.g. a negative size) has to fall back entirely.
                    value = fallback
            if maximum is not None and value > maximum:
                value = maximum
            if value != raw:
                warnings.append(f"{name}={raw!r} out of range, using {value!r}")
            setattr(self, name, value)

        clean_limits = {}
        for exchange, raw_limit in self.max_open_spot_orders_per_exchange.items():
            name = str(exchange).strip().lower()
            try:
                limit = int(raw_limit)
            except (TypeError, ValueError):
                limit = 2
            if not name or limit < 1 or limit > 100:
                warnings.append(f"invalid spot order limit for {exchange!r}; using 2")
                limit = 2
            clean_limits[name] = limit
        self.max_open_spot_orders_per_exchange = clean_limits

        # Hedging policy validation (threshold bounds, policy whitelist).
        warnings.extend(self.hedging.sanitize())

        if self.allocation_per_pair <= 0:
            warnings.append("allocation_per_pair must be > 0, using 0.5")
            self.allocation_per_pair = 0.5

        # A zero fee buffer or zero slippage guard silently disables a safety net;
        # say so out loud so it is a deliberate choice and not a config accident.
        if self.estimated_round_trip_fee_pct == 0:
            warnings.append(
                "estimated_round_trip_fee_pct=0 disables the fee buffer; exits may not net a profit"
            )
        if self.max_entry_slippage_pct == 0:
            warnings.append(
                "max_entry_slippage_pct=0 disables the hedge slippage guard"
            )
        if self.max_consecutive_order_failures == 0:
            warnings.append(
                "max_consecutive_order_failures=0 disables the per-exchange failure breaker"
            )
        return warnings

    def __getitem__(self, key: str) -> Any:
        if key == "executor":
            if not hasattr(self, "_executor_dict") or not isinstance(self._executor_dict, dict):
                self._executor_dict = {}
            self._executor_dict.update({
                "max_position_usdt": getattr(self, "max_position_usdt", 0.0),
                "allocation_per_pair": getattr(self, "allocation_per_pair", 0.0),
                "min_allocated_capital": getattr(self, "min_allocated_capital", 0.0),
                "max_daily_loss_usdt": getattr(self, "max_daily_loss_usdt", 0.0),
                "max_drawdown_pct": getattr(self, "max_drawdown_pct", 0.0),
                "max_funding_cost_pct": getattr(self, "max_funding_cost_pct", 0.0),
                "max_unhedged_duration_ms": getattr(self, "max_unhedged_duration_ms", 0),
                "min_venue_health_score": getattr(self, "min_venue_health_score", 0.2),
                "executor_instance_id": getattr(self, "executor_instance_id", ""),
                "emergency_derisk_owner_id": getattr(self, "emergency_derisk_owner_id", ""),
                "emergency_derisk_lease_ttl_ms": getattr(self, "emergency_derisk_lease_ttl_ms", 120000),
                "mock_mode": getattr(self, "mock_mode", False),
                "execution_mode": getattr(self, "execution_mode", "TAKER_TAKER"),
            })
            return self._executor_dict
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        if key == "executor" and isinstance(value, dict):
            if not hasattr(self, "_executor_dict") or not isinstance(self._executor_dict, dict):
                self._executor_dict = {}
            self._executor_dict.update(value)
            for k, v in value.items():
                if hasattr(self, k):
                    setattr(self, k, v)
        elif hasattr(self, key):
            setattr(self, key, value)
        else:
            setattr(self, key, value)

    def __contains__(self, key: str) -> bool:
        return key == "executor" or hasattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    @classmethod
    def load(cls, config_path: str = "config.json") -> "ExecutorConfig":
        cfg = cls()
        cfg._apply_file(config_path)
        cfg._apply_env()
        for warning in cfg.sanitize():
            print(f"warning: executor config: {warning}")
        cfg.validate_live_credentials()
        return cfg

    def validate_live_credentials(self) -> None:
        """Fail fast instead of starting a live executor with incomplete keys."""
        if self.mock_mode:
            return
        missing = []
        if not self.local_credentials.api_key or not self.local_credentials.api_secret:
            missing.append(f"{self.local_exchange_name} API key/secret")
        if not self.global_credentials.api_key or not self.global_credentials.api_secret:
            missing.append(f"{self.global_exchange_name} API key/secret")
        if self.global_exchange_name.lower() == "okx" and not self.global_credentials.passphrase:
            missing.append("okx API passphrase")
        if missing:
            raise ValueError(
                "LIVE mode requested but exchange credentials are incomplete: "
                + ", ".join(missing)
                + ". Configure exchanges.<name> in config.json or matching environment variables."
            )
