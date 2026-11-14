import copy

import pytest

from executor.v2_config import ConfigSnapshotError, validate_v2_snapshot


def _base():
    return {
        "schema_version": 2,
        "mode": "LIVE",
        "execution": {
            "engine": "python",
            "execution_mode": "MAKER_TAKER",
            "signals_enabled": False,
            "startup_reconciliation_required": True,
        },
        "storage": {
            "driver": "postgres",
            "dsn_env": "NAFAZ_DATABASE_URL",
            "migrations": "required",
            "outbox": {"enabled": True, "delivery": "redis"},
        },
        "redis": {
            "enabled": True,
            "url_env": "NAFAZ_REDIS_URL",
            "purpose": ["candidate_queue", "transient_cache", "coordination"],
        },
        "venues": [],
        "accounts": [],
        "routes": [],
        "risk_profiles": {
            "default": {
                "max_daily_loss_usdt": 5,
                "max_exposure_per_account_usdt": 100,
                "max_exposure_per_route_usdt": 50,
                "max_entry_slippage_pct": 0.2,
                "max_consecutive_order_failures": 5,
                "max_open_spot_orders": 2,
                "max_open_futures_orders": 2,
                "require_fresh_balance": True,
                "require_position_reconciliation": True,
            }
        },
        "scanner": {
            "enabled": True,
            "public_market_data_only": True,
            "scan_interval_seconds": 30,
            "quote_assets": ["USDT", "USDC"],
            "synthetic_fx": {
                "enabled": True,
                "source": "configured_live_fx_market",
                "max_age_seconds": 5,
                "static_fallback_allowed_in_live": False,
            },
        },
    }


def _venue(venue_id, exchange, market_type, account_id, *, execution_enabled=False):
    execution = {"enabled": execution_enabled}
    if execution_enabled:
        execution.update({
            "adapter": exchange,
            "credentials": {
                "api_key_env": f"{exchange.upper()}_API_KEY",
                "api_secret_env": f"{exchange.upper()}_API_SECRET",
            },
        })
    return {
        "id": venue_id,
        "exchange": exchange,
        "market_type": market_type,
        "account_id": account_id,
        "enabled": True,
        "market_data": {"enabled": True, "adapter": exchange},
        "execution": execution,
    }


def _account(account_id, venue_id, *, enabled=True):
    return {"id": account_id, "venue_id": venue_id, "label": account_id, "enabled": enabled}


def _route(*, enabled=False, spot_venue="spot_a", spot_account="account_a",
           futures_venue="perp_b", futures_account="account_b"):
    return {
        "id": "route_a",
        "spot": {
            "venue_id": spot_venue,
            "account_id": spot_account,
            "quote_assets": ["IDR", "USDT"],
        },
        "futures": {
            "venue_id": futures_venue,
            "account_id": futures_account,
            "settle_assets": ["USDT"],
        },
        "enabled": enabled,
        "symbols": {"allow": ["BTC", "ETH"], "deny": []},
        "risk_profile": "default",
    }


def _enabled_route_snapshot():
    snapshot = _base()
    snapshot["venues"] = [
        _venue("spot_a", "indodax", "spot", "account_a", execution_enabled=True),
        _venue("perp_b", "okx", "linear_perpetual", "account_b", execution_enabled=True),
    ]
    snapshot["accounts"] = [
        _account("account_a", "spot_a"),
        _account("account_b", "perp_b"),
    ]
    snapshot["routes"] = [_route(enabled=True)]
    return snapshot


_CAPABILITIES = {
    "indodax": {
        "market_types": ["spot"],
        "multi_venue": True,
        "multi_account": True,
        "capabilities": [
            "balance_read", "open_orders_read", "limit_order_create",
            "order_cancel", "order_lookup_by_client_id",
        ],
    },
    "okx": {
        "market_types": ["linear_perpetual"],
        "multi_venue": True,
        "multi_account": True,
        "capabilities": [
            "balance_read", "positions_read", "market_order_create",
            "reduce_only_order", "order_cancel", "order_lookup_by_client_id",
        ],
    },
}


def test_parses_two_spot_and_two_perpetual_venues_and_separate_accounts():
    snapshot = _base()
    snapshot["venues"] = [
        _venue("indodax_spot_primary", "indodax", "spot", "indodax_account_1"),
        _venue("tokocrypto_spot_primary", "tokocrypto", "spot", "toko_account_1"),
        _venue("okx_swap_primary", "okx", "linear_perpetual", "okx_account_1"),
        _venue("okx_swap_secondary", "okx", "linear_perpetual", "okx_account_2"),
    ]
    snapshot["accounts"] = [
        _account("indodax_account_1", "indodax_spot_primary"),
        _account("toko_account_1", "tokocrypto_spot_primary"),
        _account("okx_account_1", "okx_swap_primary"),
        _account("okx_account_2", "okx_swap_secondary"),
    ]

    model = validate_v2_snapshot(snapshot)

    assert len(model.venues) == 4
    assert len(model.accounts) == 4
    assert model.mode == "LIVE"
    assert model.signals_enabled is False


def test_rejects_market_type_mismatch_between_route_leg_and_venue():
    snapshot = _enabled_route_snapshot()
    snapshot["routes"][0]["spot"]["venue_id"] = "perp_b"
    snapshot["routes"][0]["spot"]["account_id"] = "account_b"

    with pytest.raises(ConfigSnapshotError, match="spot leg references a non-spot"):
        validate_v2_snapshot(snapshot)


def test_enabled_route_fails_closed_without_verified_capability_registry():
    with pytest.raises(ConfigSnapshotError, match="no verified capability registration"):
        validate_v2_snapshot(_enabled_route_snapshot())


def test_enabled_route_requires_all_documented_capabilities():
    incomplete = copy.deepcopy(_CAPABILITIES)
    incomplete["okx"]["capabilities"].remove("reduce_only_order")

    with pytest.raises(ConfigSnapshotError, match="missing capabilities"):
        validate_v2_snapshot(
            _enabled_route_snapshot(),
            capability_registry=incomplete,
        )


def test_enabled_route_passes_only_with_multi_venue_multi_account_capabilities():
    model = validate_v2_snapshot(
        _enabled_route_snapshot(),
        capability_registry=_CAPABILITIES,
    )

    assert model.routes[0].enabled is True
    assert model.routes[0].capability_verified is True


def test_enabled_route_rejects_disabled_account_or_venue():
    snapshot = _enabled_route_snapshot()
    snapshot["accounts"][0]["enabled"] = False

    with pytest.raises(ConfigSnapshotError, match="disabled venue/account"):
        validate_v2_snapshot(snapshot, capability_registry=_CAPABILITIES)


def test_rejects_unknown_nested_keys_and_invalid_secret_references():
    snapshot = _enabled_route_snapshot()
    snapshot["venues"][0]["execution"]["unreviewed_option"] = True
    with pytest.raises(ConfigSnapshotError, match="unsupported keys"):
        validate_v2_snapshot(snapshot)

    snapshot = _enabled_route_snapshot()
    snapshot["venues"][0]["execution"]["credentials"]["api_key_env"] = "actual-secret-value"
    with pytest.raises(ConfigSnapshotError, match="environment variable name"):
        validate_v2_snapshot(snapshot)


def test_rejects_non_positive_risk_limits_and_symbol_overlap():
    snapshot = _base()
    snapshot["risk_profiles"]["default"]["max_daily_loss_usdt"] = 0
    with pytest.raises(ConfigSnapshotError, match="finite and >="):
        validate_v2_snapshot(snapshot)

    snapshot = _base()
    snapshot["venues"] = [
        _venue("spot_a", "indodax", "spot", "account_a"),
        _venue("perp_b", "okx", "linear_perpetual", "account_b"),
    ]
    snapshot["accounts"] = [_account("account_a", "spot_a"), _account("account_b", "perp_b")]
    snapshot["routes"] = [_route()]
    snapshot["routes"][0]["symbols"] = {"allow": ["BTC"], "deny": ["BTC"]}
    with pytest.raises(ConfigSnapshotError, match="overlap"):
        validate_v2_snapshot(snapshot)


def test_live_snapshot_cannot_enable_scanner_private_data_or_disable_reconciliation():
    snapshot = _base()
    snapshot["scanner"]["public_market_data_only"] = False
    with pytest.raises(ConfigSnapshotError, match="public-market-data-only"):
        validate_v2_snapshot(snapshot)

    snapshot = _base()
    snapshot["execution"]["startup_reconciliation_required"] = False
    with pytest.raises(ConfigSnapshotError, match="startup_reconciliation_required"):
        validate_v2_snapshot(snapshot)


def test_active_snapshot_validation_uses_injected_production_capability_registry():
    from executor.v2_config import calculate_snapshot_sha256, validate_snapshot_row

    snapshot = _enabled_route_snapshot()
    row = {
        "version": 2,
        "schema_version": 2,
        "snapshot": snapshot,
        "content_sha256": calculate_snapshot_sha256(snapshot),
    }

    with pytest.raises(ConfigSnapshotError, match="no verified capability registration"):
        validate_snapshot_row(row)

    parsed = validate_snapshot_row(row, capability_registry=_CAPABILITIES)
    assert parsed.version == 2
