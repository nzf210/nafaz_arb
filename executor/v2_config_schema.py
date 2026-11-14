"""Strict V2 configuration domain model and fail-closed route validation.

The parser is intentionally separate from the legacy ExecutorConfig loader.
A config can be structurally valid while its routes remain non-executable until
an adapter capability registry has been verified by tests and documentation.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional


class V2ConfigValidationError(ValueError):
    """Raised when a V2 snapshot violates its schema or safety contract."""


_ID_RE = re.compile(r"^[a-z][a-z0-9_]{1,62}$")
_EXCHANGE_RE = re.compile(r"^[a-z][a-z0-9_-]{1,62}$")
_ENV_RE = re.compile(r"^[A-Z_][A-Z0-9_]{1,127}$")
_SECRET_REF_RE = re.compile(r"^secret://[A-Za-z0-9._/-]{1,240}$")
_ASSET_RE = re.compile(r"^[A-Z0-9]{2,24}$")

_ROOT_KEYS = {
    "schema_version", "mode", "execution", "storage", "redis", "venues",
    "accounts", "routes", "risk_profiles", "scanner", "observability",
    "telegram", "fx",
}
_EXECUTION_KEYS = {
    "engine", "execution_mode", "signals_enabled",
    "startup_reconciliation_required",
}
_STORAGE_KEYS = {"driver", "dsn_env", "migrations", "outbox"}
_OUTBOX_KEYS = {"enabled", "delivery"}
_REDIS_KEYS = {"enabled", "url_env", "purpose"}
_VENUE_KEYS = {
    "id", "exchange", "market_type", "account_id", "enabled",
    "market_data", "execution",
}
_MARKET_DATA_KEYS = {"enabled", "adapter"}
_VENUE_EXECUTION_KEYS = {
    "enabled", "adapter", "credentials", "position_mode", "margin_mode",
    "required_leverage",
}
_CREDENTIAL_KEYS = {
    "api_key_env", "api_key_ref", "api_secret_env", "api_secret_ref",
    "passphrase_env", "passphrase_ref",
}
_ACCOUNT_KEYS = {"id", "venue_id", "label", "enabled"}
_ROUTE_KEYS = {
    "id", "spot", "futures", "enabled", "symbols", "risk_profile",
}
_SPOT_LEG_KEYS = {"venue_id", "account_id", "quote_assets"}
_FUTURES_LEG_KEYS = {"venue_id", "account_id", "settle_assets"}
_SYMBOL_KEYS = {"allow", "deny"}
_RISK_KEYS = {
    "max_daily_loss_usdt", "max_exposure_per_account_usdt",
    "max_exposure_per_route_usdt", "max_entry_slippage_pct",
    "max_consecutive_order_failures", "max_open_spot_orders",
    "max_open_futures_orders", "require_fresh_balance",
    "require_position_reconciliation",
}
_SCANNER_KEYS = {
    "enabled", "public_market_data_only", "scan_interval_seconds",
    "quote_assets", "synthetic_fx",
}
_FX_KEYS = {
    "enabled", "source", "max_age_seconds", "max_spread_pct",
    "static_fallback_allowed_in_live",
}
_TELEGRAM_KEYS = {
    "enabled", "bot_token_env", "bot_token_ref", "allowed_user_ids",
    "allowed_chat_ids", "start_confirmation_ttl_seconds",
}
_OBSERVABILITY_KEYS = {"log_level", "metrics_enabled"}
_MARKET_TYPES = {"spot", "linear_perpetual", "inverse_perpetual"}

_REQUIRED_CAPABILITIES = {
    "spot": frozenset({
        "balance_read", "open_orders_read", "limit_order_create",
        "order_cancel", "order_lookup_by_client_id",
    }),
    "linear_perpetual": frozenset({
        "balance_read", "positions_read", "market_order_create",
        "reduce_only_order", "order_cancel", "order_lookup_by_client_id",
    }),
    "inverse_perpetual": frozenset({
        "balance_read", "positions_read", "market_order_create",
        "reduce_only_order", "order_cancel", "order_lookup_by_client_id",
    }),
}


@dataclass(frozen=True)
class V2VenueModel:
    venue_id: str
    exchange: str
    market_type: str
    account_id: str
    enabled: bool
    market_data_enabled: bool
    execution_enabled: bool
    market_data_adapter: Optional[str]
    execution_adapter: Optional[str]


@dataclass(frozen=True)
class V2AccountModel:
    account_id: str
    venue_id: str
    enabled: bool


@dataclass(frozen=True)
class V2RouteModel:
    route_id: str
    spot_venue_id: str
    spot_account_id: str
    futures_venue_id: str
    futures_account_id: str
    risk_profile: str
    enabled: bool
    capability_verified: bool


@dataclass(frozen=True)
class V2ConfigModel:
    schema_version: int
    mode: str
    execution_mode: str
    signals_enabled: bool
    venues: tuple[V2VenueModel, ...]
    accounts: tuple[V2AccountModel, ...]
    routes: tuple[V2RouteModel, ...]
    risk_profile_ids: tuple[str, ...]


def _object(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise V2ConfigValidationError(f"{path} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise V2ConfigValidationError(f"{path} keys must be strings")
    return value


def _keys(value: Mapping[str, Any], allowed: set[str], required: set[str], path: str) -> None:
    missing = sorted(required - set(value))
    unknown = sorted(set(value) - allowed)
    if missing:
        raise V2ConfigValidationError(f"{path} missing required keys: {', '.join(missing)}")
    if unknown:
        raise V2ConfigValidationError(f"{path} has unsupported keys: {', '.join(unknown)}")


def _identifier(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _ID_RE.fullmatch(value):
        raise V2ConfigValidationError(f"{path} must be a lowercase stable identifier")
    return value


def _exchange(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _EXCHANGE_RE.fullmatch(value):
        raise V2ConfigValidationError(f"{path} must be a lowercase adapter/exchange identifier")
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise V2ConfigValidationError(f"{path} must be a boolean")
    return value


def _integer(value: Any, path: str, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise V2ConfigValidationError(f"{path} must be an integer >= {minimum}")
    return value


def _decimal(value: Any, path: str, *, minimum: Decimal, maximum: Optional[Decimal] = None) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise V2ConfigValidationError(f"{path} must be a finite number")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise V2ConfigValidationError(f"{path} must be a finite number") from None
    if not parsed.is_finite() or parsed < minimum:
        raise V2ConfigValidationError(f"{path} must be finite and >= {minimum}")
    if maximum is not None and parsed > maximum:
        raise V2ConfigValidationError(f"{path} must be <= {maximum}")
    return parsed


def _asset_list(value: Any, path: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty):
        raise V2ConfigValidationError(f"{path} must be a non-empty array of asset codes")
    assets = []
    for asset in value:
        if not isinstance(asset, str) or not _ASSET_RE.fullmatch(asset):
            raise V2ConfigValidationError(f"{path} contains an invalid asset code")
        if asset in assets:
            raise V2ConfigValidationError(f"{path} contains duplicate asset {asset}")
        assets.append(asset)
    return tuple(assets)


def _secret_reference(value: Any, key: str, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise V2ConfigValidationError(f"{path}.{key} must contain a secret reference, not a value")
    valid = _ENV_RE.fullmatch(value) if key.endswith("_env") else _SECRET_REF_RE.fullmatch(value)
    if not valid:
        expected = "an environment variable name" if key.endswith("_env") else "a secret:// reference"
        raise V2ConfigValidationError(f"{path}.{key} must be {expected}")
    return value


def _credential_refs(value: Any, path: str, *, required: bool) -> tuple[tuple[str, str], ...]:
    refs = _object(value, path)
    _keys(refs, _CREDENTIAL_KEYS, set(), path)
    resolved = []
    for credential in ("api_key", "api_secret", "passphrase"):
        candidates = [f"{credential}_env", f"{credential}_ref"]
        present = [key for key in candidates if key in refs]
        if len(present) > 1:
            raise V2ConfigValidationError(f"{path} defines multiple references for {credential}")
        if credential in {"api_key", "api_secret"} and required and len(present) != 1:
            raise V2ConfigValidationError(f"{path} requires a reference for {credential}")
        if present:
            key = present[0]
            resolved.append((credential, _secret_reference(refs[key], key, path)))
    return tuple(resolved)


def _unique_index(items: Any, label: str) -> dict[str, Mapping[str, Any]]:
    if not isinstance(items, list):
        raise V2ConfigValidationError(f"{label}s must be an array")
    index: dict[str, Mapping[str, Any]] = {}
    for item in items:
        entry = _object(item, f"{label}s[]")
        identifier = entry.get("id")
        if not isinstance(identifier, str) or not identifier.strip():
            raise V2ConfigValidationError(f"{label} entry has invalid id")
        if identifier in index:
            raise V2ConfigValidationError(f"duplicate {label} id: {identifier}")
        index[identifier] = entry
    return index


def _validate_risk_profile(profile_id: str, value: Any) -> None:
    profile = _object(value, f"risk_profiles.{profile_id}")
    _keys(profile, _RISK_KEYS, _RISK_KEYS, f"risk_profiles.{profile_id}")
    for field in (
        "max_daily_loss_usdt", "max_exposure_per_account_usdt",
        "max_exposure_per_route_usdt",
    ):
        _decimal(profile[field], f"risk_profiles.{profile_id}.{field}", minimum=Decimal("0.000000000000000001"))
    _decimal(
        profile["max_entry_slippage_pct"],
        f"risk_profiles.{profile_id}.max_entry_slippage_pct",
        minimum=Decimal("0.000000000000000001"),
        maximum=Decimal("100"),
    )
    _integer(profile["max_consecutive_order_failures"], f"risk_profiles.{profile_id}.max_consecutive_order_failures")
    _integer(profile["max_open_spot_orders"], f"risk_profiles.{profile_id}.max_open_spot_orders")
    _integer(profile["max_open_futures_orders"], f"risk_profiles.{profile_id}.max_open_futures_orders")
    _boolean(profile["require_fresh_balance"], f"risk_profiles.{profile_id}.require_fresh_balance")
    _boolean(profile["require_position_reconciliation"], f"risk_profiles.{profile_id}.require_position_reconciliation")
    if profile["require_fresh_balance"] is not True or profile["require_position_reconciliation"] is not True:
        raise V2ConfigValidationError(
            f"risk_profiles.{profile_id} must require fresh balances and position reconciliation"
        )


def _validate_fx(value: Any, path: str, *, live: bool) -> None:
    fx = _object(value, path)
    _keys(fx, _FX_KEYS, {"enabled", "source", "max_age_seconds", "static_fallback_allowed_in_live"}, path)
    _boolean(fx["enabled"], f"{path}.enabled")
    if not isinstance(fx["source"], str) or not fx["source"].strip():
        raise V2ConfigValidationError(f"{path}.source must be non-empty")
    _integer(fx["max_age_seconds"], f"{path}.max_age_seconds")
    if "max_spread_pct" in fx:
        _decimal(fx["max_spread_pct"], f"{path}.max_spread_pct", minimum=Decimal("0"), maximum=Decimal("100"))
    _boolean(fx["static_fallback_allowed_in_live"], f"{path}.static_fallback_allowed_in_live")
    if live and fx["static_fallback_allowed_in_live"]:
        raise V2ConfigValidationError(f"{path} cannot allow static FX fallback in LIVE")


def _capability_supported(
    adapter: str,
    market_type: str,
    registry: Optional[Mapping[str, Any]],
    path: str,
) -> bool:
    entry = (registry or {}).get(adapter)
    if not isinstance(entry, Mapping):
        raise V2ConfigValidationError(
            f"{path} adapter {adapter} has no verified capability registration"
        )
    market_types = entry.get("market_types")
    capabilities = entry.get("capabilities")
    if not isinstance(market_types, (list, tuple, set)) or market_type not in market_types:
        raise V2ConfigValidationError(f"{path} adapter {adapter} does not support {market_type}")
    if entry.get("multi_venue") is not True or entry.get("multi_account") is not True:
        raise V2ConfigValidationError(
            f"{path} adapter {adapter} is not verified for multi-venue and multi-account execution"
        )
    if not isinstance(capabilities, (list, tuple, set)):
        raise V2ConfigValidationError(f"{path} adapter {adapter} has no verified capability set")
    missing = sorted(_REQUIRED_CAPABILITIES[market_type] - set(capabilities))
    if missing:
        raise V2ConfigValidationError(
            f"{path} adapter {adapter} is missing capabilities: {', '.join(missing)}"
        )
    return True


def parse_v2_config(
    snapshot: Mapping[str, Any],
    *,
    capability_registry: Optional[Mapping[str, Any]] = None,
) -> V2ConfigModel:
    """Parse and validate a complete V2 snapshot without resolving secret values."""
    root = _object(snapshot, "V2 config")
    required_root = {
        "schema_version", "mode", "execution", "storage", "redis",
        "venues", "accounts", "routes", "risk_profiles", "scanner",
    }
    _keys(root, _ROOT_KEYS, required_root, "V2 config")
    if isinstance(root["schema_version"], bool) or root["schema_version"] != 2:
        raise V2ConfigValidationError("V2 config schema_version must equal 2")
    mode = str(root["mode"]).upper()
    if mode not in {"LIVE", "MOCK"}:
        raise V2ConfigValidationError("V2 config mode must be LIVE or MOCK")
    live = mode == "LIVE"

    execution = _object(root["execution"], "execution")
    _keys(execution, _EXECUTION_KEYS, {"engine", "signals_enabled"}, "execution")
    if execution["engine"] != "python":
        raise V2ConfigValidationError("execution.engine must be python")
    signals_enabled = _boolean(execution["signals_enabled"], "execution.signals_enabled")
    if signals_enabled:
        raise V2ConfigValidationError("execution.signals_enabled must remain false until sole-consumer proof")
    execution_mode = str(execution.get("execution_mode", "MAKER_TAKER")).upper()
    if execution_mode not in {"MAKER_TAKER", "TAKER_TAKER"}:
        raise V2ConfigValidationError("execution.execution_mode is unsupported")
    if live and execution.get("startup_reconciliation_required") is not True:
        raise V2ConfigValidationError("LIVE requires execution.startup_reconciliation_required=true")
    if "startup_reconciliation_required" in execution:
        _boolean(execution["startup_reconciliation_required"], "execution.startup_reconciliation_required")

    storage = _object(root["storage"], "storage")
    _keys(storage, _STORAGE_KEYS, {"driver"}, "storage")
    if storage["driver"] != "postgres":
        raise V2ConfigValidationError("storage.driver must be postgres")
    if "dsn_env" in storage:
        if not isinstance(storage["dsn_env"], str) or not _ENV_RE.fullmatch(storage["dsn_env"]):
            raise V2ConfigValidationError("storage.dsn_env must be an environment variable name")
    if live and (storage.get("migrations") != "required" or not storage.get("dsn_env")):
        raise V2ConfigValidationError("LIVE requires storage.dsn_env and migrations=required")
    if "migrations" in storage and storage["migrations"] not in {"required", "managed"}:
        raise V2ConfigValidationError("storage.migrations must be required or managed")
    if "outbox" in storage:
        outbox = _object(storage["outbox"], "storage.outbox")
        _keys(outbox, _OUTBOX_KEYS, {"enabled", "delivery"}, "storage.outbox")
        _boolean(outbox["enabled"], "storage.outbox.enabled")
        if outbox["delivery"] not in {"redis", "database"}:
            raise V2ConfigValidationError("storage.outbox.delivery is unsupported")
    elif live:
        raise V2ConfigValidationError("LIVE requires a configured transactional outbox")

    redis = _object(root["redis"], "redis")
    _keys(redis, _REDIS_KEYS, {"enabled"}, "redis")
    redis_enabled = _boolean(redis["enabled"], "redis.enabled")
    if live and not redis_enabled:
        raise V2ConfigValidationError("LIVE requires Redis for transient coordination")
    if "url_env" in redis:
        if not isinstance(redis["url_env"], str) or not _ENV_RE.fullmatch(redis["url_env"]):
            raise V2ConfigValidationError("redis.url_env must be an environment variable name")
    if live and not redis.get("url_env"):
        raise V2ConfigValidationError("LIVE requires redis.url_env")
    if "purpose" in redis:
        purpose = redis["purpose"]
        allowed_purpose = {"candidate_queue", "transient_cache", "coordination"}
        if not isinstance(purpose, list) or not set(purpose) <= allowed_purpose:
            raise V2ConfigValidationError("redis.purpose contains unsupported responsibilities")

    scanner = _object(root["scanner"], "scanner")
    _keys(scanner, _SCANNER_KEYS, {"public_market_data_only"}, "scanner")
    if _boolean(scanner["public_market_data_only"], "scanner.public_market_data_only") is not True:
        raise V2ConfigValidationError("scanner must be public-market-data-only")
    if "enabled" in scanner:
        _boolean(scanner["enabled"], "scanner.enabled")
    if "scan_interval_seconds" in scanner:
        _integer(scanner["scan_interval_seconds"], "scanner.scan_interval_seconds")
    if "quote_assets" in scanner:
        _asset_list(scanner["quote_assets"], "scanner.quote_assets")
    if "synthetic_fx" in scanner:
        _validate_fx(scanner["synthetic_fx"], "scanner.synthetic_fx", live=live)
    if "fx" in root:
        _validate_fx(root["fx"], "fx", live=live)
    elif live and "synthetic_fx" not in scanner:
        raise V2ConfigValidationError("LIVE requires an explicit fresh FX source policy")

    if "observability" in root:
        observability = _object(root["observability"], "observability")
        _keys(observability, _OBSERVABILITY_KEYS, set(), "observability")
        if "metrics_enabled" in observability:
            _boolean(observability["metrics_enabled"], "observability.metrics_enabled")
        if "log_level" in observability and observability["log_level"] not in {
            "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL",
        }:
            raise V2ConfigValidationError("observability.log_level is unsupported")

    if "telegram" in root:
        telegram = _object(root["telegram"], "telegram")
        _keys(telegram, _TELEGRAM_KEYS, set(), "telegram")
        enabled = _boolean(telegram.get("enabled", False), "telegram.enabled")
        for key in ("bot_token_env", "bot_token_ref"):
            if key in telegram:
                _secret_reference(telegram[key], key, "telegram")
        if "bot_token_env" in telegram and "bot_token_ref" in telegram:
            raise V2ConfigValidationError("telegram must use one bot-token reference")
        if enabled and not ({"bot_token_env", "bot_token_ref"} & set(telegram)):
            raise V2ConfigValidationError("enabled Telegram control requires a bot-token reference")
        if enabled:
            for key in ("allowed_user_ids", "allowed_chat_ids"):
                values = telegram.get(key)
                if not isinstance(values, list) or not values or any(
                    isinstance(item, bool) or not isinstance(item, int) or item <= 0
                    for item in values
                ):
                    raise V2ConfigValidationError(
                        f"enabled Telegram control requires non-empty positive {key}"
                    )
        if "start_confirmation_ttl_seconds" in telegram:
            _integer(telegram["start_confirmation_ttl_seconds"], "telegram.start_confirmation_ttl_seconds")

    venue_index = _unique_index(root["venues"], "venue")
    account_index = _unique_index(root["accounts"], "account")
    route_index = _unique_index(root["routes"], "route")
    risk_profiles = _object(root["risk_profiles"], "risk_profiles")

    for risk_id, profile in risk_profiles.items():
        _identifier(risk_id, f"risk_profiles key {risk_id!r}")
        _validate_risk_profile(risk_id, profile)

    venues: list[V2VenueModel] = []
    for venue_id, venue in venue_index.items():
        _identifier(venue_id, f"venue {venue_id}.id")
        _keys(venue, _VENUE_KEYS, _VENUE_KEYS, f"venue {venue_id}")
        exchange = _exchange(venue["exchange"], f"venue {venue_id}.exchange")
        market_type = venue["market_type"]
        if market_type not in _MARKET_TYPES:
            raise V2ConfigValidationError(f"venue {venue_id} has unsupported market_type")
        account_id = _identifier(venue["account_id"], f"venue {venue_id}.account_id")
        if account_id not in account_index:
            raise V2ConfigValidationError(f"venue {venue_id} references unknown account {account_id!r}")
        enabled = _boolean(venue["enabled"], f"venue {venue_id}.enabled")

        market_data = _object(venue["market_data"], f"venue {venue_id}.market_data")
        _keys(market_data, _MARKET_DATA_KEYS, {"enabled"}, f"venue {venue_id}.market_data")
        market_data_enabled = _boolean(market_data["enabled"], f"venue {venue_id}.market_data.enabled")
        market_data_adapter = None
        if market_data_enabled:
            market_data_adapter = _exchange(
                market_data.get("adapter"), f"venue {venue_id}.market_data.adapter"
            )

        venue_execution = _object(venue["execution"], f"venue {venue_id}.execution")
        _keys(venue_execution, _VENUE_EXECUTION_KEYS, {"enabled"}, f"venue {venue_id}.execution")
        execution_enabled = _boolean(venue_execution["enabled"], f"venue {venue_id}.execution.enabled")
        execution_adapter = None
        if execution_enabled:
            execution_adapter = _exchange(
                venue_execution.get("adapter"), f"venue {venue_id}.execution.adapter"
            )
            credentials = venue_execution.get("credentials")
            _credential_refs(credentials, f"venue {venue_id}.execution.credentials", required=True)
            if "required_leverage" in venue_execution:
                _decimal(
                    venue_execution["required_leverage"],
                    f"venue {venue_id}.execution.required_leverage",
                    minimum=Decimal("1"),
                    maximum=Decimal("100"),
                )
            if "position_mode" in venue_execution and venue_execution["position_mode"] not in {"one_way", "hedge"}:
                raise V2ConfigValidationError(f"venue {venue_id}.execution.position_mode is unsupported")
            if "margin_mode" in venue_execution and venue_execution["margin_mode"] not in {"cross", "isolated"}:
                raise V2ConfigValidationError(f"venue {venue_id}.execution.margin_mode is unsupported")
        elif "credentials" in venue_execution:
            raise V2ConfigValidationError(
                f"venue {venue_id} must not attach credentials while execution is disabled"
            )

        venues.append(V2VenueModel(
            venue_id=venue_id,
            exchange=exchange,
            market_type=market_type,
            account_id=account_id,
            enabled=enabled,
            market_data_enabled=market_data_enabled,
            execution_enabled=execution_enabled,
            market_data_adapter=market_data_adapter,
            execution_adapter=execution_adapter,
        ))

    accounts: list[V2AccountModel] = []
    for account_id, account in account_index.items():
        _identifier(account_id, f"account {account_id}.id")
        _keys(account, _ACCOUNT_KEYS, _ACCOUNT_KEYS, f"account {account_id}")
        venue_id = _identifier(account["venue_id"], f"account {account_id}.venue_id")
        if venue_id not in venue_index:
            raise V2ConfigValidationError(f"account {account_id} references unknown venue {venue_id}")
        if venue_index[venue_id].get("account_id") != account_id:
            raise V2ConfigValidationError(f"account {account_id} does not match venue {venue_id}.account_id")
        accounts.append(V2AccountModel(
            account_id=account_id,
            venue_id=venue_id,
            enabled=_boolean(account["enabled"], f"account {account_id}.enabled"),
        ))

    routes: list[V2RouteModel] = []
    for route_id, route in route_index.items():
        _identifier(route_id, f"route {route_id}.id")
        _keys(route, _ROUTE_KEYS, _ROUTE_KEYS, f"route {route_id}")
        enabled = _boolean(route["enabled"], f"route {route_id}.enabled")
        spot = _object(route["spot"], f"route {route_id}.spot")
        futures = _object(route["futures"], f"route {route_id}.futures")
        _keys(spot, _SPOT_LEG_KEYS, _SPOT_LEG_KEYS, f"route {route_id}.spot")
        _keys(futures, _FUTURES_LEG_KEYS, _FUTURES_LEG_KEYS, f"route {route_id}.futures")
        spot_venue_id = _identifier(spot["venue_id"], f"route {route_id}.spot.venue_id")
        futures_venue_id = _identifier(futures["venue_id"], f"route {route_id}.futures.venue_id")
        spot_account_id = _identifier(spot["account_id"], f"route {route_id}.spot.account_id")
        futures_account_id = _identifier(futures["account_id"], f"route {route_id}.futures.account_id")
        if spot_venue_id not in venue_index or spot_account_id not in account_index:
            raise V2ConfigValidationError(f"route {route_id} spot references unknown venue/account")
        if futures_venue_id not in venue_index or futures_account_id not in account_index:
            raise V2ConfigValidationError(f"route {route_id} futures references unknown venue/account")
        if account_index[spot_account_id].get("venue_id") != spot_venue_id:
            raise V2ConfigValidationError(f"route {route_id} spot account/venue mismatch")
        if account_index[futures_account_id].get("venue_id") != futures_venue_id:
            raise V2ConfigValidationError(f"route {route_id} futures account/venue mismatch")
        if venue_index[spot_venue_id].get("market_type") != "spot":
            raise V2ConfigValidationError(f"route {route_id} spot leg references a non-spot venue")
        if venue_index[futures_venue_id].get("market_type") not in {
            "linear_perpetual", "inverse_perpetual",
        }:
            raise V2ConfigValidationError(f"route {route_id} futures leg references a non-perpetual venue")
        _asset_list(spot["quote_assets"], f"route {route_id}.spot.quote_assets")
        _asset_list(futures["settle_assets"], f"route {route_id}.futures.settle_assets")

        symbols = _object(route["symbols"], f"route {route_id}.symbols")
        _keys(symbols, _SYMBOL_KEYS, {"allow", "deny"}, f"route {route_id}.symbols")
        allowed_symbols = _asset_list(symbols["allow"], f"route {route_id}.symbols.allow")
        denied_symbols = _asset_list(symbols["deny"], f"route {route_id}.symbols.deny", allow_empty=True)
        if set(allowed_symbols) & set(denied_symbols):
            raise V2ConfigValidationError(f"route {route_id} symbol allow/deny lists overlap")

        risk_id = _identifier(route["risk_profile"], f"route {route_id}.risk_profile")
        if risk_id not in risk_profiles:
            raise V2ConfigValidationError(f"route {route_id} references unknown risk_profile {risk_id}")

        capability_verified = False
        if enabled:
            for leg_name, venue_id, account_id in (
                ("spot", spot_venue_id, spot_account_id),
                ("futures", futures_venue_id, futures_account_id),
            ):
                venue = venue_index[venue_id]
                account = account_index[account_id]
                if venue.get("enabled") is not True or account.get("enabled") is not True:
                    raise V2ConfigValidationError(
                        f"route {route_id} {leg_name} references a disabled venue/account"
                    )
                venue_execution = venue.get("execution")
                if not isinstance(venue_execution, Mapping) or venue_execution.get("enabled") is not True:
                    raise V2ConfigValidationError(
                        f"route {route_id} {leg_name} execution is disabled"
                    )
                adapter = venue_execution.get("adapter")
                _capability_supported(
                    str(adapter),
                    str(venue["market_type"]),
                    capability_registry,
                    f"route {route_id} {leg_name}",
                )
            capability_verified = True

        routes.append(V2RouteModel(
            route_id=route_id,
            spot_venue_id=spot_venue_id,
            spot_account_id=spot_account_id,
            futures_venue_id=futures_venue_id,
            futures_account_id=futures_account_id,
            risk_profile=risk_id,
            enabled=enabled,
            capability_verified=capability_verified,
        ))

    return V2ConfigModel(
        schema_version=2,
        mode=mode,
        execution_mode=execution_mode,
        signals_enabled=signals_enabled,
        venues=tuple(venues),
        accounts=tuple(accounts),
        routes=tuple(routes),
        risk_profile_ids=tuple(sorted(risk_profiles)),
    )
