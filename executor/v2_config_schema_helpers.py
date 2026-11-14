"""Strict low-level validators shared by the V2 config parser."""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional

from executor.v2_config_model import V2ConfigValidationError, V2RiskProfileModel


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


def _validate_risk_profile(profile_id: str, value: Any) -> V2RiskProfileModel:
    profile = _object(value, f"risk_profiles.{profile_id}")
    _keys(profile, _RISK_KEYS, _RISK_KEYS, f"risk_profiles.{profile_id}")
    max_daily_loss = _decimal(
        profile["max_daily_loss_usdt"],
        f"risk_profiles.{profile_id}.max_daily_loss_usdt",
        minimum=Decimal("0.000000000000000001"),
    )
    max_account_exposure = _decimal(
        profile["max_exposure_per_account_usdt"],
        f"risk_profiles.{profile_id}.max_exposure_per_account_usdt",
        minimum=Decimal("0.000000000000000001"),
    )
    max_route_exposure = _decimal(
        profile["max_exposure_per_route_usdt"],
        f"risk_profiles.{profile_id}.max_exposure_per_route_usdt",
        minimum=Decimal("0.000000000000000001"),
    )
    max_slippage = _decimal(
        profile["max_entry_slippage_pct"],
        f"risk_profiles.{profile_id}.max_entry_slippage_pct",
        minimum=Decimal("0.000000000000000001"),
        maximum=Decimal("100"),
    )
    max_failures = _integer(profile["max_consecutive_order_failures"], f"risk_profiles.{profile_id}.max_consecutive_order_failures")
    max_spot_orders = _integer(profile["max_open_spot_orders"], f"risk_profiles.{profile_id}.max_open_spot_orders")
    max_futures_orders = _integer(profile["max_open_futures_orders"], f"risk_profiles.{profile_id}.max_open_futures_orders")
    fresh_balance = _boolean(profile["require_fresh_balance"], f"risk_profiles.{profile_id}.require_fresh_balance")
    reconcile_positions = _boolean(profile["require_position_reconciliation"], f"risk_profiles.{profile_id}.require_position_reconciliation")
    if fresh_balance is not True or reconcile_positions is not True:
        raise V2ConfigValidationError(
            f"risk_profiles.{profile_id} must require fresh balances and position reconciliation"
        )
    return V2RiskProfileModel(
        profile_id=profile_id,
        max_daily_loss_usdt=max_daily_loss,
        max_exposure_per_account_usdt=max_account_exposure,
        max_exposure_per_route_usdt=max_route_exposure,
        max_entry_slippage_pct=max_slippage,
        max_consecutive_order_failures=max_failures,
        max_open_spot_orders=max_spot_orders,
        max_open_futures_orders=max_futures_orders,
        require_fresh_balance=fresh_balance,
        require_position_reconciliation=reconcile_positions,
    )


def _validate_fx(value: Any, path: str, *, live: bool) -> None:
    fx = _object(value, path)
    _keys(fx, _FX_KEYS, {"enabled", "source", "max_age_seconds", "static_fallback_allowed_in_live"}, path)
    _boolean(fx["enabled"], f"{path}.enabled")
    if not isinstance(fx["source"], str) or not fx["source"].strip():
        raise V2ConfigValidationError(f"{path}.source must be non-empty")
    _integer(fx["max_age_seconds"], f"{path}.max_age_seconds")
    if live and "max_spread_pct" not in fx:
        raise V2ConfigValidationError(f"{path}.max_spread_pct is required in LIVE")
    if "max_spread_pct" in fx:
        _decimal(fx["max_spread_pct"], f"{path}.max_spread_pct", minimum=Decimal("0"), maximum=Decimal("100"))
    _boolean(fx["static_fallback_allowed_in_live"], f"{path}.static_fallback_allowed_in_live")
    if live and not fx["enabled"]:
        raise V2ConfigValidationError(f"{path}.enabled must be true in LIVE")
    if live and fx["static_fallback_allowed_in_live"]:
        raise V2ConfigValidationError(f"{path} cannot allow static FX fallback in LIVE")
    if live and any(marker in fx["source"].lower() for marker in ("static", "synthetic", "fixed")):
        raise V2ConfigValidationError(f"{path}.source must identify a live FX feed in LIVE")


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
    if not isinstance(capabilities, (list, tuple, set)) or any(
        not isinstance(capability, str) for capability in capabilities
    ):
        raise V2ConfigValidationError(f"{path} adapter {adapter} has no verified capability set")
    missing = sorted(_REQUIRED_CAPABILITIES[market_type] - set(capabilities))
    if missing:
        raise V2ConfigValidationError(
            f"{path} adapter {adapter} is missing capabilities: {', '.join(missing)}"
        )
    return True
