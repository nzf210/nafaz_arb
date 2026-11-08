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

@dataclass
class ExecutorConfig:
    redis_url: str = "redis://localhost:6380/0"
    max_stale_latency_ms: int = 500
    min_partial_fill_usd: float = 10.0
    exit_price_move_threshold: float = 0.0015  # 0.15%
    max_order_ttl_seconds: int = 30  # Max TTL for pending maker entry orders before cancel
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
    # Adverse futures-ask allowance used by the combined-PnL exit profit floor.
    max_exit_slippage_pct: float = 0.003  # Fallback until enough samples exist.
    min_exit_slippage_pct: float = 0.001
    max_dynamic_exit_slippage_pct: float = 0.02
    exit_volatility_multiplier: float = 2.0
    exit_spread_multiplier: float = 0.5
    # Re-quote the global futures bid just before placing the maker buy and
    # reject the signal when the entry spread has collapsed below the scanner's
    # own minimum. 0 disables the guard (default: on at 0.0 = use payload floor).
    entry_revalidation_min_spread_pct: float = 0.0
    # Cap on total capital committed per exchange, across all active positions.
    # 0 disables the check.
    max_exposure_per_exchange_usdt: float = 0.0
    # Balance-mismatch guard: when an exchange's available balance drops below
    # (last observed available - this fraction of total) between two checks,
    # something took our capital (withdrawal, liquidation, another bot) and the
    # venue is halted for new entries. 0 disables only mismatch comparison;
    # live balance reads remain mandatory for entry sizing.
    balance_mismatch_fraction: float = 0.20
    # Consecutive order-placement failures tolerated per exchange before the
    # venue is halted for new entries. 0 disables the breaker.
    max_consecutive_order_failures: int = 3
    # Round-trip taker/maker fees charged on both legs, used to shrink the
    # position size so the exit can still net a profit. 0 disables the buffer.
    estimated_round_trip_fee_pct: float = 0.001
    # Use a private order/balance feed instead of polling REST for every check.
    # Off by default: the feed is an optimisation, and REST polling is the
    # proven path. When on, stale or missing entries still fall back to REST.
    private_stream_enabled: bool = False
    private_stream_max_age_seconds: float = 5.0
    local_exchange_name: str = "indodax"
    global_exchange_name: str = "okx"
    spot_exchanges: list = field(default_factory=list)
    perpetual_exchanges: list = field(default_factory=list)
    exchanges: Dict[str, ExchangeCredentials] = field(default_factory=dict)
    local_credentials: ExchangeCredentials = field(default_factory=ExchangeCredentials)
    global_credentials: ExchangeCredentials = field(default_factory=ExchangeCredentials)
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)

    def _apply_file(self, config_path: str) -> None:
        if not os.path.exists(config_path):
            return
        try:
            with open(config_path, "r", encoding="utf-8") as stream:
                data = json.load(stream)
            self.mock_mode = str(data.get("mode", "MOCK")).strip().upper() != "LIVE"
            self._apply_file_redis(data.get("redis", {}))
            self._apply_file_root_values(data)
            self._apply_file_executor_values(data.get("executor", {}))
            self._apply_file_exchange_credentials(data.get("exchanges", {}))
            self._apply_file_supabase(data.get("supabase", {}))
            self.spot_exchanges = self._normalize_exchange_list(data.get("spot_exchanges", []))
            self.perpetual_exchanges = self._normalize_exchange_list(data.get("perpetual_exchanges", []))
        except Exception as exc:
            print(f"warning: error loading config file: {exc}")

    def _apply_file_redis(self, redis_cfg: dict) -> None:
        address = redis_cfg.get("addr")
        if not address:
            return
        password = redis_cfg.get("password", "")
        auth_part = f":{password}@" if password else ""
        self.redis_url = f"redis://{auth_part}{address}/{redis_cfg.get('db', 0)}"

    def _apply_file_root_values(self, data: dict) -> None:
        for key in ("allocation_per_pair", "min_allocated_capital"):
            if key in data:
                setattr(self, key, float(data[key]))

    def _apply_file_executor_values(self, executor_cfg: dict) -> None:
        conversions = (
            ("min_partial_fill_usd", float), ("exit_price_move_threshold", float),
            ("max_stale_latency_ms", int), ("max_order_ttl_seconds", int),
            ("initial_exchange_balance_usdt", float), ("allocation_per_pair", float),
            ("min_allocated_capital", float), ("max_daily_loss_usdt", float),
            ("max_entry_slippage_pct", float), ("max_exit_slippage_pct", float),
            ("min_exit_slippage_pct", float), ("max_dynamic_exit_slippage_pct", float),
            ("exit_volatility_multiplier", float), ("exit_spread_multiplier", float),
            ("entry_revalidation_min_spread_pct", float), ("max_exposure_per_exchange_usdt", float),
            ("balance_mismatch_fraction", float), ("max_consecutive_order_failures", int),
            ("estimated_round_trip_fee_pct", float), ("mock_mode", bool),
            ("private_stream_enabled", bool), ("private_stream_max_age_seconds", float),
        )
        for key, converter in conversions:
            if key in executor_cfg:
                setattr(self, key, converter(executor_cfg[key]))

    def _apply_file_exchange_credentials(self, exchange_data: dict) -> None:
        if not isinstance(exchange_data, dict):
            return
        for name, values in exchange_data.items():
            if not isinstance(values, dict):
                continue
            self.exchanges[name.lower()] = ExchangeCredentials(
                api_key=str(values.get("api_key", "")),
                api_secret=str(values.get("api_secret", "")),
                passphrase=values.get("passphrase"),
                testnet=bool(values.get("testnet", False)),
                options=dict(values.get("options", {})),
            )

    def _apply_file_supabase(self, values: dict) -> None:
        if not isinstance(values, dict):
            return
        self.supabase.enabled = bool(values.get("enabled", False))
        self.supabase.url = str(values.get("url", ""))
        self.supabase.key = str(values.get("key", ""))

    @staticmethod
    def _normalize_exchange_list(values: list) -> list:
        return [str(value).strip().lower() for value in values if str(value).strip()]

    def _apply_env(self) -> None:
        self._apply_basic_env()
        self._apply_numeric_env()
        self._apply_legacy_credentials_env()
        self._apply_modular_exchange_env()
        self._merge_legacy_credentials()
        self._apply_supabase_env()

    def _apply_basic_env(self) -> None:
        direct_values = (
            ("REDIS_URL", "redis_url"),
            ("LOCAL_EXCHANGE", "local_exchange_name"),
            ("GLOBAL_EXCHANGE", "global_exchange_name"),
        )
        for env_name, attribute in direct_values:
            if value := os.getenv(env_name):
                setattr(self, attribute, value)
        for env_name, attribute in (
            ("SPOT_EXCHANGES", "spot_exchanges"),
            ("PERPETUAL_EXCHANGES", "perpetual_exchanges"),
        ):
            if values := os.getenv(env_name):
                setattr(self, attribute, self._normalize_exchange_list(values.split(",")))
        if value := os.getenv("EXECUTOR_MOCK_MODE"):
            self.mock_mode = value.lower() in ("true", "1", "yes")
        if value := os.getenv("PRIVATE_STREAM_ENABLED"):
            self.private_stream_enabled = value.lower() in ("true", "1", "yes")

    def _apply_numeric_env(self) -> None:
        conversions = (
            ("PRIVATE_STREAM_MAX_AGE_SECONDS", "private_stream_max_age_seconds", float),
            ("MAX_ORDER_TTL_SECONDS", "max_order_ttl_seconds", int),
            ("MAX_DAILY_LOSS_USDT", "max_daily_loss_usdt", float),
            ("MAX_ENTRY_SLIPPAGE_PCT", "max_entry_slippage_pct", float),
            ("MAX_EXIT_SLIPPAGE_PCT", "max_exit_slippage_pct", float),
            ("MIN_EXIT_SLIPPAGE_PCT", "min_exit_slippage_pct", float),
            ("MAX_DYNAMIC_EXIT_SLIPPAGE_PCT", "max_dynamic_exit_slippage_pct", float),
            ("EXIT_VOLATILITY_MULTIPLIER", "exit_volatility_multiplier", float),
            ("EXIT_SPREAD_MULTIPLIER", "exit_spread_multiplier", float),
            ("ENTRY_REVALIDATION_MIN_SPREAD_PCT", "entry_revalidation_min_spread_pct", float),
            ("MAX_EXPOSURE_PER_EXCHANGE_USDT", "max_exposure_per_exchange_usdt", float),
            ("BALANCE_MISMATCH_FRACTION", "balance_mismatch_fraction", float),
            ("MAX_CONSECUTIVE_ORDER_FAILURES", "max_consecutive_order_failures", int),
            ("ESTIMATED_ROUND_TRIP_FEE_PCT", "estimated_round_trip_fee_pct", float),
        )
        for env_name, attribute, converter in conversions:
            raw = os.getenv(env_name)
            if raw is not None:
                self._set_env_numeric(raw, attribute, converter)

    def _set_env_numeric(self, raw: str, attribute: str, converter) -> None:
        try:
            setattr(self, attribute, converter(raw))
        except ValueError:
            return

    def _apply_legacy_credentials_env(self) -> None:
        self.local_credentials.api_key = os.getenv("LOCAL_API_KEY", self.local_credentials.api_key)
        self.local_credentials.api_secret = os.getenv("LOCAL_API_SECRET", self.local_credentials.api_secret)
        self.global_credentials.api_key = os.getenv("GLOBAL_API_KEY", self.global_credentials.api_key)
        self.global_credentials.api_secret = os.getenv("GLOBAL_API_SECRET", self.global_credentials.api_secret)
        self.global_credentials.passphrase = os.getenv("GLOBAL_PASSPHRASE", self.global_credentials.passphrase)
        if os.getenv("GLOBAL_TESTNET", "").lower() in ("true", "1"):
            self.global_credentials.testnet = True

    def _apply_modular_exchange_env(self) -> None:
        for env_name, value in os.environ.items():
            if not self._is_modular_credential_env(env_name):
                continue
            prefix = env_name[:-8].lower()
            credentials = self.exchanges.setdefault(prefix, ExchangeCredentials())
            credentials.api_key = value
            self._apply_modular_credential_fields(env_name[:-8], credentials)

    @staticmethod
    def _is_modular_credential_env(env_name: str) -> bool:
        return env_name.endswith("_API_KEY") and not env_name.startswith(("LOCAL_", "GLOBAL_"))

    @staticmethod
    def _apply_modular_credential_fields(prefix: str, credentials: ExchangeCredentials) -> None:
        if secret := os.getenv(f"{prefix}_API_SECRET"):
            credentials.api_secret = secret
        if passphrase := os.getenv(f"{prefix}_PASSPHRASE"):
            credentials.passphrase = passphrase
        if testnet := os.getenv(f"{prefix}_TESTNET"):
            credentials.testnet = testnet.lower() in ("true", "1", "yes")

    def _merge_legacy_credentials(self) -> None:
        self._merge_exchange_credentials(self.local_exchange_name, self.local_credentials)
        self._merge_exchange_credentials(self.global_exchange_name, self.global_credentials)

    def _merge_exchange_credentials(self, exchange_name: str, legacy: ExchangeCredentials) -> None:
        modular = self.exchanges.setdefault(exchange_name.lower(), legacy)
        for field_name in ("api_key", "api_secret", "passphrase"):
            modular_value = getattr(modular, field_name)
            legacy_value = getattr(legacy, field_name)
            if modular_value:
                setattr(legacy, field_name, modular_value)
            elif legacy_value:
                setattr(modular, field_name, legacy_value)
        if modular.testnet:
            legacy.testnet = True
        else:
            modular.testnet = legacy.testnet
        if modular.options:
            legacy.options.update(modular.options)
        elif legacy.options:
            modular.options.update(legacy.options)

    def _apply_supabase_env(self) -> None:
        if value := os.getenv("SUPABASE_URL"):
            self.supabase.url = value
            self.supabase.enabled = True
        if value := os.getenv("SUPABASE_KEY"):
            self.supabase.key = value
            self.supabase.enabled = True

    # Bounds applied by sanitize(): (field, minimum, maximum, default when invalid).
    # A bad value is replaced by the default rather than aborting the boot, so a
    # typo in config.json can never leave the executor sizing positions at 0 or
    # accepting unlimited risk.
    _NUMERIC_BOUNDS = {
        "max_stale_latency_ms": (1, 60000, 500),
        "max_order_ttl_seconds": (1, 86400, 30),
        "min_partial_fill_usd": (0.0, None, 10.0),
        "exit_price_move_threshold": (0.0, 1.0, 0.0015),
        "initial_exchange_balance_usdt": (0.01, None, 100.0),
        "allocation_per_pair": (0.0, 1.0, 0.5),
        "min_allocated_capital": (0.0, None, 10.0),
        "max_daily_loss_usdt": (0.0, None, 50.0),
        "max_entry_slippage_pct": (0.0, 1.0, 0.003),
        "max_exit_slippage_pct": (0.0, 1.0, 0.003),
        "min_exit_slippage_pct": (0.0, 1.0, 0.001),
        "max_dynamic_exit_slippage_pct": (0.001, 1.0, 0.02),
        "exit_volatility_multiplier": (0.0, 10.0, 2.0),
        "exit_spread_multiplier": (0.0, 10.0, 0.5),
        "entry_revalidation_min_spread_pct": (0.0, 100.0, 0.0),
        "max_exposure_per_exchange_usdt": (0.0, None, 0.0),
        "balance_mismatch_fraction": (0.0, 1.0, 0.20),
        "max_consecutive_order_failures": (0, 100, 3),
        "estimated_round_trip_fee_pct": (0.0, 1.0, 0.001),
        "private_stream_max_age_seconds": (0.5, 60.0, 5.0),
    }

    def _sanitize_numeric_field(self, name: str, bounds: tuple) -> Optional[str]:
        minimum, maximum, fallback = bounds
        raw = getattr(self, name)
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = None
        if value is None or value != value:
            value = fallback
        if minimum is not None and value < minimum:
            value = fallback if minimum <= 0 else minimum
        if maximum is not None and value > maximum:
            value = maximum
        if value == raw:
            return None
        setattr(self, name, value)
        return f"{name}={raw!r} out of range, using {value!r}"

    def _disabled_safety_warnings(self) -> list[str]:
        disabled = (
            ("estimated_round_trip_fee_pct", "disables the fee buffer; exits may not net a profit"),
            ("max_entry_slippage_pct", "disables the hedge slippage guard"),
            ("max_exit_slippage_pct", "disables the conservative exit profit-floor buffer"),
            ("max_consecutive_order_failures", "disables the per-exchange failure breaker"),
        )
        return [
            f"{name}=0 {description}"
            for name, description in disabled if getattr(self, name) == 0
        ]

    def sanitize(self) -> list:
        warnings = []
        for name, bounds in self._NUMERIC_BOUNDS.items():
            warning = self._sanitize_numeric_field(name, bounds)
            if warning:
                warnings.append(warning)
        if self.allocation_per_pair <= 0:
            warnings.append("allocation_per_pair must be > 0, using 0.5")
            self.allocation_per_pair = 0.5
        warnings.extend(self._disabled_safety_warnings())
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

    def _configured_exchange_names(self) -> list[str]:
        spot_names = self.spot_exchanges or [self.local_exchange_name]
        perp_names = self.perpetual_exchanges or [self.global_exchange_name]
        return sorted(set(name.lower() for name in spot_names + perp_names))

    def _credentials_for_exchange(self, name: str) -> Optional[ExchangeCredentials]:
        credentials = self.exchanges.get(name)
        if credentials is not None:
            return credentials
        if name == self.local_exchange_name.lower():
            return self.local_credentials
        if name == self.global_exchange_name.lower():
            return self.global_credentials
        return None

    def _missing_credentials(self, name: str) -> list[str]:
        credentials = self._credentials_for_exchange(name)
        if credentials is None or not credentials.api_key or not credentials.api_secret:
            return [f"{name} API key/secret"]
        if name == "okx" and not credentials.passphrase:
            return ["okx API passphrase"]
        return []

    def validate_live_credentials(self) -> None:
        """Fail fast instead of starting a live executor with incomplete keys."""
        if self.mock_mode:
            return
        missing = []
        for name in self._configured_exchange_names():
            missing.extend(self._missing_credentials(name))
        if missing:
            raise ValueError(
                "LIVE mode requested but exchange credentials are incomplete: "
                + ", ".join(missing)
                + ". Configure exchanges.<name> in config.json or matching environment variables."
            )
