"""Strict V2 configuration parser and fail-closed route capability validation."""
from __future__ import annotations

from typing import Any, Mapping, Optional

from executor.v2_config_model import V2ConfigModel, V2ConfigValidationError
from executor.v2_config_schema_helpers import _ROOT_KEYS, _keys, _object
from executor.v2_config_schema_sections import (
    parse_risk_profiles,
    parse_routes,
    parse_runtime_settings,
    parse_venues_accounts,
)


def parse_v2_config(
    snapshot: Mapping[str, Any],
    *,
    capability_registry: Optional[Mapping[str, Any]] = None,
) -> V2ConfigModel:
    """Parse and validate a complete V2 snapshot without resolving secret values."""
    root = _object(snapshot, "V2 config")
    required = {
        "schema_version", "mode", "execution", "storage", "redis",
        "venues", "accounts", "routes", "risk_profiles", "scanner",
    }
    _keys(root, _ROOT_KEYS, required, "V2 config")
    version = root["schema_version"]
    if not isinstance(version, int) or isinstance(version, bool) or version != 2:
        raise V2ConfigValidationError("V2 config schema_version must equal 2")
    if not isinstance(root["mode"], str) or root["mode"].upper() not in {"LIVE", "MOCK"}:
        raise V2ConfigValidationError("V2 config mode must be LIVE or MOCK")
    mode = root["mode"].upper()
    live = mode == "LIVE"

    runtime = parse_runtime_settings(root, live)
    risk_profiles = parse_risk_profiles(root)
    venue_index, account_index, venues, accounts = parse_venues_accounts(root)
    routes = parse_routes(
        root,
        venue_index,
        account_index,
        {profile.profile_id for profile in risk_profiles},
        capability_registry,
    )

    return V2ConfigModel(
        schema_version=version,
        mode=mode,
        execution_mode=runtime.execution_mode,
        signals_enabled=runtime.signals_enabled,
        startup_reconciliation_required=runtime.startup_reconciliation_required,
        storage_driver=runtime.storage_driver,
        storage_dsn_env=runtime.storage_dsn_env,
        redis_enabled=runtime.redis_enabled,
        redis_url_env=runtime.redis_url_env,
        scanner_enabled=runtime.scanner_enabled,
        scanner_quote_assets=runtime.scanner_quote_assets,
        venues=venues,
        accounts=accounts,
        routes=routes,
        risk_profiles=risk_profiles,
    )
