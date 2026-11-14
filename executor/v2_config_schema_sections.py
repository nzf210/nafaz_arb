"""Section parsers for strict V2 config validation."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping, Optional

from executor.v2_config_model import (
    V2AccountModel,
    V2ConfigValidationError,
    V2RouteModel,
    V2RiskProfileModel,
    V2RuntimeSettingsModel,
    V2VenueModel,
)
from executor.v2_config_schema_helpers import (
    _ACCOUNT_KEYS,
    _ENV_RE,
    _EXECUTION_KEYS,
    _FUTURES_LEG_KEYS,
    _MARKET_DATA_KEYS,
    _MARKET_TYPES,
    _OBSERVABILITY_KEYS,
    _OUTBOX_KEYS,
    _REDIS_KEYS,
    _ROUTE_KEYS,
    _SCANNER_KEYS,
    _SPOT_LEG_KEYS,
    _STORAGE_KEYS,
    _SYMBOL_KEYS,
    _TELEGRAM_KEYS,
    _VENUE_EXECUTION_KEYS,
    _VENUE_KEYS,
    _asset_list,
    _boolean,
    _capability_supported,
    _credential_refs,
    _decimal,
    _exchange,
    _identifier,
    _integer,
    _keys,
    _object,
    _unique_index,
    _validate_fx,
    _validate_risk_profile,
)


def _parse_execution(root: Mapping[str, Any], live: bool) -> tuple[str, bool, bool]:
    execution = _object(root["execution"], "execution")
    _keys(execution, _EXECUTION_KEYS, {"engine", "signals_enabled"}, "execution")
    if execution["engine"] != "python":
        raise V2ConfigValidationError("execution.engine must be python")
    signals_enabled = _boolean(execution["signals_enabled"], "execution.signals_enabled")
    if signals_enabled:
        raise V2ConfigValidationError("execution.signals_enabled must remain false until sole-consumer proof")
    raw_mode = execution.get("execution_mode", "MAKER_TAKER")
    if not isinstance(raw_mode, str) or raw_mode.upper() not in {"MAKER_TAKER", "TAKER_TAKER"}:
        raise V2ConfigValidationError("execution.execution_mode is unsupported")
    startup_required = _boolean(
        execution.get("startup_reconciliation_required", False),
        "execution.startup_reconciliation_required",
    )
    if live and not startup_required:
        raise V2ConfigValidationError("LIVE requires execution.startup_reconciliation_required=true")
    return raw_mode.upper(), signals_enabled, startup_required


def _parse_storage(root: Mapping[str, Any], live: bool) -> tuple[str, Optional[str]]:
    storage = _object(root["storage"], "storage")
    _keys(storage, _STORAGE_KEYS, {"driver"}, "storage")
    if storage["driver"] != "postgres":
        raise V2ConfigValidationError("storage.driver must be postgres")
    dsn_env = storage.get("dsn_env")
    if dsn_env is not None and (
        not isinstance(dsn_env, str) or not _ENV_RE.fullmatch(dsn_env)
    ):
        raise V2ConfigValidationError("storage.dsn_env must be an environment variable name")
    migrations = storage.get("migrations")
    if migrations is not None and (
        not isinstance(migrations, str) or migrations not in {"required", "managed"}
    ):
        raise V2ConfigValidationError("storage.migrations must be required or managed")
    if live and (migrations != "required" or not dsn_env):
        raise V2ConfigValidationError("LIVE requires storage.dsn_env and migrations=required")
    if "outbox" not in storage:
        if live:
            raise V2ConfigValidationError("LIVE requires a configured transactional outbox")
    else:
        outbox = _object(storage["outbox"], "storage.outbox")
        _keys(outbox, _OUTBOX_KEYS, {"enabled", "delivery"}, "storage.outbox")
        outbox_enabled = _boolean(outbox["enabled"], "storage.outbox.enabled")
        if not isinstance(outbox["delivery"], str) or outbox["delivery"] not in {"redis", "database"}:
            raise V2ConfigValidationError("storage.outbox.delivery is unsupported")
        if live and not outbox_enabled:
            raise V2ConfigValidationError("LIVE requires storage.outbox.enabled=true")
    return str(storage["driver"]), dsn_env


def _parse_redis(root: Mapping[str, Any], live: bool) -> tuple[bool, Optional[str]]:
    redis = _object(root["redis"], "redis")
    _keys(redis, _REDIS_KEYS, {"enabled"}, "redis")
    enabled = _boolean(redis["enabled"], "redis.enabled")
    if live and not enabled:
        raise V2ConfigValidationError("LIVE requires Redis for transient coordination")
    url_env = redis.get("url_env")
    if url_env is not None and (
        not isinstance(url_env, str) or not _ENV_RE.fullmatch(url_env)
    ):
        raise V2ConfigValidationError("redis.url_env must be an environment variable name")
    if live and not url_env:
        raise V2ConfigValidationError("LIVE requires redis.url_env")
    purpose = redis.get("purpose")
    allowed = {"candidate_queue", "transient_cache", "coordination"}
    if purpose is None:
        if live:
            raise V2ConfigValidationError("LIVE requires explicit Redis responsibilities")
    elif (
        not isinstance(purpose, list)
        or any(not isinstance(item, str) for item in purpose)
        or not set(purpose) <= allowed
    ):
        raise V2ConfigValidationError("redis.purpose contains unsupported responsibilities")
    return enabled, url_env


def _parse_scanner(root: Mapping[str, Any], live: bool) -> tuple[bool, tuple[str, ...]]:
    scanner = _object(root["scanner"], "scanner")
    _keys(
        scanner,
        _SCANNER_KEYS,
        {"enabled", "public_market_data_only", "scan_interval_seconds", "quote_assets"},
        "scanner",
    )
    if _boolean(scanner["public_market_data_only"], "scanner.public_market_data_only") is not True:
        raise V2ConfigValidationError("scanner must be public-market-data-only")
    enabled = _boolean(scanner["enabled"], "scanner.enabled")
    if live and not enabled:
        raise V2ConfigValidationError("LIVE requires scanner.enabled=true")
    _integer(scanner["scan_interval_seconds"], "scanner.scan_interval_seconds")
    quote_assets = _asset_list(scanner["quote_assets"], "scanner.quote_assets")
    if "synthetic_fx" in scanner:
        _validate_fx(scanner["synthetic_fx"], "scanner.synthetic_fx", live=live)
    if "fx" in root:
        if "synthetic_fx" in scanner:
            raise V2ConfigValidationError("define FX policy in one location only")
        _validate_fx(root["fx"], "fx", live=live)
    elif live and "synthetic_fx" not in scanner:
        raise V2ConfigValidationError("LIVE requires an explicit fresh FX source policy")
    return enabled, quote_assets


def _validate_optional_sections(root: Mapping[str, Any], live: bool) -> None:
    if "observability" in root:
        observability = _object(root["observability"], "observability")
        _keys(observability, _OBSERVABILITY_KEYS, set(), "observability")
        if "metrics_enabled" in observability:
            _boolean(observability["metrics_enabled"], "observability.metrics_enabled")
        level = observability.get("log_level")
        if level is not None and (
            not isinstance(level, str)
            or level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        ):
            raise V2ConfigValidationError("observability.log_level is unsupported")
    if "telegram" in root:
        telegram = _object(root["telegram"], "telegram")
        _keys(telegram, _TELEGRAM_KEYS, set(), "telegram")
        enabled = _boolean(telegram.get("enabled", False), "telegram.enabled")
        for key in ("bot_token_env", "bot_token_ref"):
            if key in telegram:
                _credential_refs({key: telegram[key]}, "telegram", required=False)
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


def parse_runtime_settings(root: Mapping[str, Any], live: bool) -> V2RuntimeSettingsModel:
    execution_mode, signals_enabled, startup_required = _parse_execution(root, live)
    storage_driver, dsn_env = _parse_storage(root, live)
    redis_enabled, redis_url_env = _parse_redis(root, live)
    scanner_enabled, quote_assets = _parse_scanner(root, live)
    _validate_optional_sections(root, live)
    return V2RuntimeSettingsModel(
        execution_mode=execution_mode,
        signals_enabled=signals_enabled,
        startup_reconciliation_required=startup_required,
        storage_driver=storage_driver,
        storage_dsn_env=dsn_env,
        redis_enabled=redis_enabled,
        redis_url_env=redis_url_env,
        scanner_enabled=scanner_enabled,
        scanner_quote_assets=quote_assets,
    )


def parse_risk_profiles(root: Mapping[str, Any]) -> tuple[V2RiskProfileModel, ...]:
    risk_profiles = _object(root["risk_profiles"], "risk_profiles")
    profiles = []
    for risk_id, profile in risk_profiles.items():
        _identifier(risk_id, f"risk_profiles key {risk_id!r}")
        profiles.append(_validate_risk_profile(risk_id, profile))
    return tuple(profiles)


def _parse_venue(
    venue_id: str,
    venue: Mapping[str, Any],
    account_index: Mapping[str, Mapping[str, Any]],
) -> V2VenueModel:
    _identifier(venue_id, f"venue {venue_id}.id")
    _keys(venue, _VENUE_KEYS, _VENUE_KEYS, f"venue {venue_id}")
    exchange = _exchange(venue["exchange"], f"venue {venue_id}.exchange")
    market_type = venue["market_type"]
    if not isinstance(market_type, str) or market_type not in _MARKET_TYPES:
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
    credential_refs: tuple[tuple[str, str], ...] = ()
    required_leverage = None
    if execution_enabled:
        execution_adapter = _exchange(
            venue_execution.get("adapter"), f"venue {venue_id}.execution.adapter"
        )
        credential_refs = _credential_refs(
            venue_execution.get("credentials"),
            f"venue {venue_id}.execution.credentials",
            required=True,
        )
        if "required_leverage" in venue_execution:
            required_leverage = _decimal(
                venue_execution["required_leverage"],
                f"venue {venue_id}.execution.required_leverage",
                minimum=Decimal("1"),
                maximum=Decimal("100"),
            )
        if "position_mode" in venue_execution and (
            not isinstance(venue_execution["position_mode"], str)
            or venue_execution["position_mode"] not in {"one_way", "hedge"}
        ):
            raise V2ConfigValidationError(f"venue {venue_id}.execution.position_mode is unsupported")
        if "margin_mode" in venue_execution and (
            not isinstance(venue_execution["margin_mode"], str)
            or venue_execution["margin_mode"] not in {"cross", "isolated"}
        ):
            raise V2ConfigValidationError(f"venue {venue_id}.execution.margin_mode is unsupported")
    elif "credentials" in venue_execution:
        raise V2ConfigValidationError(
            f"venue {venue_id} must not attach credentials while execution is disabled"
        )

    return V2VenueModel(
        venue_id=venue_id,
        exchange=exchange,
        market_type=market_type,
        account_id=account_id,
        enabled=enabled,
        market_data_enabled=market_data_enabled,
        execution_enabled=execution_enabled,
        market_data_adapter=market_data_adapter,
        execution_adapter=execution_adapter,
        credential_refs=credential_refs,
        position_mode=venue_execution.get("position_mode"),
        margin_mode=venue_execution.get("margin_mode"),
        required_leverage=required_leverage,
    )


def _parse_account(
    account_id: str,
    account: Mapping[str, Any],
    venue_index: Mapping[str, Mapping[str, Any]],
) -> V2AccountModel:
    _identifier(account_id, f"account {account_id}.id")
    _keys(account, _ACCOUNT_KEYS, _ACCOUNT_KEYS, f"account {account_id}")
    venue_id = _identifier(account["venue_id"], f"account {account_id}.venue_id")
    if venue_id not in venue_index:
        raise V2ConfigValidationError(f"account {account_id} references unknown venue {venue_id}")
    if venue_index[venue_id].get("account_id") != account_id:
        raise V2ConfigValidationError(f"account {account_id} does not match venue {venue_id}.account_id")
    label = account["label"]
    if not isinstance(label, str) or not label.strip():
        raise V2ConfigValidationError(f"account {account_id}.label must be non-empty")
    return V2AccountModel(
        account_id=account_id,
        venue_id=venue_id,
        label=label.strip(),
        enabled=_boolean(account["enabled"], f"account {account_id}.enabled"),
    )


def parse_venues_accounts(
    root: Mapping[str, Any],
) -> tuple[
    dict[str, Mapping[str, Any]],
    dict[str, Mapping[str, Any]],
    tuple[V2VenueModel, ...],
    tuple[V2AccountModel, ...],
]:
    venue_index = _unique_index(root["venues"], "venue")
    account_index = _unique_index(root["accounts"], "account")
    venues = tuple(
        _parse_venue(venue_id, venue, account_index)
        for venue_id, venue in venue_index.items()
    )
    accounts = tuple(
        _parse_account(account_id, account, venue_index)
        for account_id, account in account_index.items()
    )
    return venue_index, account_index, venues, accounts


def _validate_route_refs(
    route_id: str,
    spot: Mapping[str, Any],
    futures: Mapping[str, Any],
    venue_index: Mapping[str, Mapping[str, Any]],
    account_index: Mapping[str, Mapping[str, Any]],
) -> tuple[str, str, str, str, tuple[str, ...], tuple[str, ...]]:
    _keys(spot, _SPOT_LEG_KEYS, _SPOT_LEG_KEYS, f"route {route_id}.spot")
    _keys(futures, _FUTURES_LEG_KEYS, _FUTURES_LEG_KEYS, f"route {route_id}.futures")
    spot_venue = _identifier(spot["venue_id"], f"route {route_id}.spot.venue_id")
    futures_venue = _identifier(futures["venue_id"], f"route {route_id}.futures.venue_id")
    spot_account = _identifier(spot["account_id"], f"route {route_id}.spot.account_id")
    futures_account = _identifier(futures["account_id"], f"route {route_id}.futures.account_id")
    if spot_venue not in venue_index or spot_account not in account_index:
        raise V2ConfigValidationError(f"route {route_id} spot references unknown venue/account")
    if futures_venue not in venue_index or futures_account not in account_index:
        raise V2ConfigValidationError(f"route {route_id} futures references unknown venue/account")
    if account_index[spot_account].get("venue_id") != spot_venue:
        raise V2ConfigValidationError(f"route {route_id} spot account/venue mismatch")
    if account_index[futures_account].get("venue_id") != futures_venue:
        raise V2ConfigValidationError(f"route {route_id} futures account/venue mismatch")
    if venue_index[spot_venue].get("market_type") != "spot":
        raise V2ConfigValidationError(f"route {route_id} spot leg references a non-spot venue")
    if venue_index[futures_venue].get("market_type") not in {
        "linear_perpetual", "inverse_perpetual",
    }:
        raise V2ConfigValidationError(f"route {route_id} futures leg references a non-perpetual venue")
    spot_quotes = _asset_list(spot["quote_assets"], f"route {route_id}.spot.quote_assets")
    futures_settles = _asset_list(futures["settle_assets"], f"route {route_id}.futures.settle_assets")
    return spot_venue, spot_account, futures_venue, futures_account, spot_quotes, futures_settles


def _validate_route_symbols(route_id: str, value: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    symbols = _object(value, f"route {route_id}.symbols")
    _keys(symbols, _SYMBOL_KEYS, {"allow", "deny"}, f"route {route_id}.symbols")
    allowed = _asset_list(symbols["allow"], f"route {route_id}.symbols.allow")
    denied = _asset_list(symbols["deny"], f"route {route_id}.symbols.deny", allow_empty=True)
    if set(allowed) & set(denied):
        raise V2ConfigValidationError(f"route {route_id} symbol allow/deny lists overlap")
    return allowed, denied


def _validate_enabled_route(
    route_id: str,
    legs: tuple[tuple[str, str, str], ...],
    venue_index: Mapping[str, Mapping[str, Any]],
    account_index: Mapping[str, Mapping[str, Any]],
    capability_registry: Optional[Mapping[str, Any]],
) -> None:
    for leg_name, venue_id, account_id in legs:
        venue = venue_index[venue_id]
        account = account_index[account_id]
        if venue.get("enabled") is not True or account.get("enabled") is not True:
            raise V2ConfigValidationError(
                f"route {route_id} {leg_name} references a disabled venue/account"
            )
        market_data = venue.get("market_data")
        if not isinstance(market_data, Mapping) or market_data.get("enabled") is not True:
            raise V2ConfigValidationError(
                f"route {route_id} {leg_name} market-data adapter is disabled"
            )
        execution = venue.get("execution")
        if not isinstance(execution, Mapping) or execution.get("enabled") is not True:
            raise V2ConfigValidationError(f"route {route_id} {leg_name} execution is disabled")
        _capability_supported(
            str(execution.get("adapter")),
            str(venue["market_type"]),
            capability_registry,
            f"route {route_id} {leg_name}",
        )


def _parse_route(
    route_id: str,
    route: Mapping[str, Any],
    venue_index: Mapping[str, Mapping[str, Any]],
    account_index: Mapping[str, Mapping[str, Any]],
    risk_profile_ids: set[str],
    capability_registry: Optional[Mapping[str, Any]],
) -> V2RouteModel:
    _identifier(route_id, f"route {route_id}.id")
    _keys(route, _ROUTE_KEYS, _ROUTE_KEYS, f"route {route_id}")
    enabled = _boolean(route["enabled"], f"route {route_id}.enabled")
    spot = _object(route["spot"], f"route {route_id}.spot")
    futures = _object(route["futures"], f"route {route_id}.futures")
    legs = _validate_route_refs(route_id, spot, futures, venue_index, account_index)
    spot_venue, spot_account, futures_venue, futures_account, quotes, settles = legs
    allowed, denied = _validate_route_symbols(route_id, route["symbols"])
    risk_id = _identifier(route["risk_profile"], f"route {route_id}.risk_profile")
    if risk_id not in risk_profile_ids:
        raise V2ConfigValidationError(f"route {route_id} references unknown risk_profile {risk_id}")
    if enabled:
        _validate_enabled_route(
            route_id,
            (("spot", spot_venue, spot_account), ("futures", futures_venue, futures_account)),
            venue_index,
            account_index,
            capability_registry,
        )
    return V2RouteModel(
        route_id=route_id,
        spot_venue_id=spot_venue,
        spot_account_id=spot_account,
        spot_quote_assets=quotes,
        futures_venue_id=futures_venue,
        futures_account_id=futures_account,
        futures_settle_assets=settles,
        allowed_symbols=allowed,
        denied_symbols=denied,
        risk_profile=risk_id,
        enabled=enabled,
        capability_verified=enabled,
    )


def parse_routes(
    root: Mapping[str, Any],
    venue_index: Mapping[str, Mapping[str, Any]],
    account_index: Mapping[str, Mapping[str, Any]],
    risk_profile_ids: set[str],
    capability_registry: Optional[Mapping[str, Any]],
) -> tuple[V2RouteModel, ...]:
    route_index = _unique_index(root["routes"], "route")
    return tuple(
        _parse_route(route_id, route, venue_index, account_index, risk_profile_ids, capability_registry)
        for route_id, route in route_index.items()
    )
