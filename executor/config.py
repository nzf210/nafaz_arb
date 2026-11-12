import os
import json
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class ExchangeCredentials:
    api_key: str = ""
    api_secret: str = ""
    passphrase: Optional[str] = None
    testnet: bool = False
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SupabaseConfig:
    enabled: bool = False
    url: str = ""
    key: str = ""
    outbox_path: str = "/app/data/executor/supabase-outbox.sqlite3"

@dataclass
class ExecutorConfig:
    redis_url: str = "redis://localhost:6380/0"
    max_stale_latency_ms: int = 500
    min_partial_fill_usd: float = 10.0
    exit_price_move_threshold: float = 0.0015  # 0.15%
    max_order_ttl_seconds: int = 120  # Max TTL for pending maker entry orders before cancel
    # Refresh stale, unfilled maker quotes by canceling them and waiting for a fresh scanner signal.
    maker_refresh_after_seconds: int = 45
    maker_refresh_cooldown_seconds: int = 300
    # Minimum projected net return retained by Indodax IDR adaptive maker quotes.
    adaptive_maker_min_net_profit_pct: float = 0.10
    indodax_maker_refresh_after_seconds: int = 15
    indodax_maker_refresh_cooldown_seconds: int = 15
    mock_mode: bool = True  # Paper trading / simulation by default
    initial_exchange_balance_usdt: float = 100.0  # $100 per exchange
    allocation_per_pair: float = 0.5  # Max 50% capital per pair
    min_allocated_capital: float = 10.0  # Minimum capital to enter position
    # Hard stop: halt all new entries once the day's realized loss crosses this.
    # 0 disables the breaker.
    max_daily_loss_usdt: float = 50.0
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
    local_exchange_name: str = "indodax"
    global_exchange_name: str = "okx"
    execution_mode: str = "TAKER_TAKER"
    exchanges: Dict[str, ExchangeCredentials] = field(default_factory=dict)
    local_credentials: ExchangeCredentials = field(default_factory=ExchangeCredentials)
    global_credentials: ExchangeCredentials = field(default_factory=ExchangeCredentials)
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)

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
                ("max_daily_loss_usdt", float),
                ("max_entry_slippage_pct", float),
                ("entry_revalidation_min_spread_pct", float),
                ("max_exposure_per_exchange_usdt", float),
                ("balance_mismatch_fraction", float),
                ("max_consecutive_order_failures", int),
                ("estimated_round_trip_fee_pct", float),
                ("mock_mode", bool),
                ("private_stream_enabled", bool),
                ("private_stream_max_age_seconds", float),
                ("monitor_interval_seconds", float),
                ("max_order_fetch_failures", int),
                ("order_fetch_backoff_max_seconds", float),
                ("venue_rate_limit_cooldown_seconds", float),
            ):
                if key in exec_cfg:
                    setattr(self, key, conv(exec_cfg[key]))
            limits = exec_cfg.get("max_open_spot_orders_per_exchange", {})
            if isinstance(limits, dict):
                self.max_open_spot_orders_per_exchange = {
                    str(name).strip().lower(): limit
                    for name, limit in limits.items()
                }
            # Load modular exchanges dictionary if provided in config
            if "exchanges" in data and isinstance(data["exchanges"], dict):
                for ex_name, ex_data in data["exchanges"].items():
                    if isinstance(ex_data, dict):
                        self.exchanges[ex_name.lower()] = ExchangeCredentials(
                            api_key=str(ex_data.get("api_key", "")),
                            api_secret=str(ex_data.get("api_secret", "")),
                            passphrase=ex_data.get("passphrase"),
                            testnet=bool(ex_data.get("testnet", False)),
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
        except Exception as e:
            print(f"warning: error loading config file: {e}")

    def _apply_env(self) -> None:
        if redis_env := os.getenv("REDIS_URL"):
            self.redis_url = redis_env

        if mode := os.getenv("EXECUTION_MODE"):
            self.execution_mode = mode.strip().upper()

        if local_ex := os.getenv("LOCAL_EXCHANGE"):
            self.local_exchange_name = local_ex

        if global_ex := os.getenv("GLOBAL_EXCHANGE"):
            self.global_exchange_name = global_ex

        if mock_env := os.getenv("EXECUTOR_MOCK_MODE"):
            self.mock_mode = mock_env.lower() in ("true", "1", "yes")

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
            self.supabase.url = sup_url
            self.supabase.enabled = True
        if sup_key := os.getenv("SUPABASE_KEY"):
            self.supabase.key = sup_key
            self.supabase.enabled = True
        if outbox_path := os.getenv("SUPABASE_OUTBOX_PATH"):
            self.supabase.outbox_path = outbox_path

    # Bounds applied by sanitize(): (field, minimum, maximum, default when invalid).
    # A bad value is replaced by the default rather than aborting the boot, so a
    # typo in config.json can never leave the executor sizing positions at 0 or
    # accepting unlimited risk.
    _NUMERIC_BOUNDS = {
        "max_stale_latency_ms": (1, 60000, 500),
        "max_order_ttl_seconds": (1, 86400, 120),
        "maker_refresh_after_seconds": (0, 86400, 45),
        "maker_refresh_cooldown_seconds": (1, 86400, 300),
        "indodax_maker_refresh_after_seconds": (0, 86400, 15),
        "indodax_maker_refresh_cooldown_seconds": (1, 86400, 15),
        "adaptive_maker_min_net_profit_pct": (0.0, 10.0, 0.10),
        "min_partial_fill_usd": (0.0, None, 10.0),
        "exit_price_move_threshold": (0.0, 1.0, 0.0015),
        "initial_exchange_balance_usdt": (0.01, None, 100.0),
        "allocation_per_pair": (0.0, 1.0, 0.5),
        "min_allocated_capital": (0.0, None, 10.0),
        "max_daily_loss_usdt": (0.0, None, 50.0),
        "max_entry_slippage_pct": (0.0, 1.0, 0.003),
        "entry_revalidation_min_spread_pct": (0.0, 100.0, 0.0),
        "max_exposure_per_exchange_usdt": (0.0, None, 0.0),
        "balance_mismatch_fraction": (0.0, 1.0, 0.20),
        "max_consecutive_order_failures": (0, 100, 50),
        "estimated_round_trip_fee_pct": (0.0, 1.0, 0.001),
        "private_stream_max_age_seconds": (0.5, 60.0, 5.0),
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

        # Tokocrypto's CCXT integration uses the account-issued broker
        # marketType as the prefix for clientId. A guessed/empty prefix causes
        # venue error 3703 and, more importantly, prevents reliable recovery
        # of a submit whose acknowledgement is lost.
        toko_credentials = self.exchanges.get("tokocrypto")
        if self.local_exchange_name.lower() == "tokocrypto":
            toko_credentials = self.local_credentials
        if toko_credentials and toko_credentials.api_key and toko_credentials.api_secret:
            options = toko_credentials.options if isinstance(toko_credentials.options, dict) else {}
            broker = options.get("broker") if isinstance(options.get("broker"), dict) else {}
            broker_id = (
                options.get("broker_id")
                or options.get("brokerId")
                or broker.get("marketType")
                or broker.get("market_type")
            )
            if not str(broker_id or "").strip():
                missing.append(
                    "tokocrypto registered broker_id (options.broker_id or options.broker.marketType)"
                )
        if missing:
            raise ValueError(
                "LIVE mode requested but exchange credentials are incomplete: "
                + ", ".join(missing)
                + ". Configure exchanges.<name> in config.json or matching environment variables."
            )
