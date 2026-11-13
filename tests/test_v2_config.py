import json

import pytest

from executor.v2_config import (
    ConfigSnapshotError,
    PostgresConfigRepository,
    RedisSnapshotCache,
    calculate_snapshot_sha256,
    load_verified_active_snapshot,
    validate_snapshot_row,
)


def _valid_snapshot():
    return {
        "schema_version": 2,
        "mode": "LIVE",
        "execution": {"engine": "python", "signals_enabled": False},
        "storage": {"driver": "postgres"},
        "redis": {"enabled": True},
        "venues": [],
        "accounts": [],
        "routes": [],
        "risk_profiles": {},
        "scanner": {"public_market_data_only": True, "synthetic_fx": {"static_fallback_allowed_in_live": False}},
    }


def _row(snapshot=None):
    snapshot = snapshot or _valid_snapshot()
    return {
        "version": 7,
        "schema_version": 2,
        "snapshot": snapshot,
        "content_sha256": calculate_snapshot_sha256(snapshot),
    }


def test_snapshot_hash_is_canonical_across_key_order():
    left = {"schema_version": 2, "mode": "LIVE", "nested": {"b": 2, "a": 1}}
    right = {"nested": {"a": 1, "b": 2}, "mode": "LIVE", "schema_version": 2}
    assert calculate_snapshot_sha256(left) == calculate_snapshot_sha256(right)


def test_validates_supported_schema_and_hash():
    parsed = validate_snapshot_row(_row())
    assert parsed.version == 7
    assert parsed.schema_version == 2
    assert parsed.snapshot["mode"] == "LIVE"


def test_rejects_hash_mismatch():
    row = _row()
    row["content_sha256"] = "0" * 64
    with pytest.raises(ConfigSnapshotError, match="SHA-256"):
        validate_snapshot_row(row)


def test_rejects_unsupported_schema():
    snapshot = {"schema_version": 3}
    row = {
        "version": 1,
        "schema_version": 3,
        "snapshot": snapshot,
        "content_sha256": calculate_snapshot_sha256(snapshot),
    }
    with pytest.raises(ConfigSnapshotError, match="unsupported"):
        validate_snapshot_row(row)


def test_rejects_non_finite_json_values():
    with pytest.raises(ConfigSnapshotError, match="canonical JSON"):
        calculate_snapshot_sha256({"schema_version": 2, "bad": float("nan")})


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return False


class _Connection:
    def __init__(self, row):
        self.row = row

    async def fetchrow(self, _query):
        return self.row


class _Pool:
    def __init__(self, row):
        self.row = row

    def acquire(self):
        return _Acquire(_Connection(self.row))


class _Redis:
    def __init__(self):
        self.values = {}
        self.fail_get = False

    async def set(self, key, value):
        self.values[key] = value

    async def get(self, key):
        if self.fail_get:
            raise OSError("redis unavailable")
        return self.values.get(key)


@pytest.mark.asyncio
async def test_loads_postgres_snapshot_then_publishes_and_verifies_redis():
    redis = _Redis()
    config = await load_verified_active_snapshot(
        PostgresConfigRepository(_Pool(_row())),
        RedisSnapshotCache(redis),
    )
    cached = json.loads(redis.values["nafaz:v2:config:active"])
    assert config.version == 7
    assert cached["version"] == 7
    assert cached["content_sha256"] == config.content_sha256


@pytest.mark.asyncio
async def test_missing_active_config_fails_closed():
    with pytest.raises(ConfigSnapshotError, match="no active"):
        await PostgresConfigRepository(_Pool(None)).fetch_active()


@pytest.mark.asyncio
async def test_redis_failure_fails_closed_after_database_read():
    redis = _Redis()
    redis.fail_get = True
    with pytest.raises(ConfigSnapshotError, match="Redis"):
        await load_verified_active_snapshot(
            PostgresConfigRepository(_Pool(_row())),
            RedisSnapshotCache(redis),
        )


def test_rejects_inline_secret_values_but_allows_secret_references():
    referenced = _valid_snapshot()
    referenced["credentials"] = {"api_key_env": "OKX_API_KEY", "api_secret_env": "OKX_API_SECRET"}
    assert validate_snapshot_row({
        "version": 8,
        "schema_version": 2,
        "snapshot": referenced,
        "content_sha256": calculate_snapshot_sha256(referenced),
    }).version == 8

    inline = _valid_snapshot()
    inline["credentials"] = {"api_key": "must-not-be-stored"}
    with pytest.raises(ConfigSnapshotError, match="inline secret"):
        validate_snapshot_row({
            "version": 9,
            "schema_version": 2,
            "snapshot": inline,
            "content_sha256": calculate_snapshot_sha256(inline),
        })


def test_v2_contract_rejects_unknown_root_keys():
    snapshot = _valid_snapshot()
    snapshot["mystery_setting"] = True
    from executor.v2_config import validate_v2_snapshot
    with pytest.raises(ConfigSnapshotError, match="unsupported root keys"):
        validate_v2_snapshot(snapshot)


def test_v2_contract_rejects_signals_and_non_public_scanner():
    from executor.v2_config import validate_v2_snapshot
    snapshot = _valid_snapshot()
    snapshot["execution"]["signals_enabled"] = True
    with pytest.raises(ConfigSnapshotError, match="signals"):
        validate_v2_snapshot(snapshot)

    snapshot = _valid_snapshot()
    snapshot["scanner"]["public_market_data_only"] = False
    with pytest.raises(ConfigSnapshotError, match="public-market-data-only"):
        validate_v2_snapshot(snapshot)


def test_v2_contract_rejects_duplicate_and_dangling_references():
    from executor.v2_config import validate_v2_snapshot
    snapshot = _valid_snapshot()
    snapshot["venues"] = [
        {"id": "spot_a", "market_type": "spot", "account_id": "account_a"},
        {"id": "spot_a", "market_type": "spot", "account_id": "account_a"},
    ]
    snapshot["accounts"] = [{"id": "account_a", "venue_id": "spot_a"}]
    with pytest.raises(ConfigSnapshotError, match="duplicate venue"):
        validate_v2_snapshot(snapshot)

    snapshot = _valid_snapshot()
    snapshot["venues"] = [{"id": "spot_a", "market_type": "spot", "account_id": "missing"}]
    snapshot["accounts"] = [{"id": "account_a", "venue_id": "spot_a"}]
    with pytest.raises(ConfigSnapshotError, match="unknown account"):
        validate_v2_snapshot(snapshot)


def test_v2_contract_rejects_route_account_venue_mismatch():
    from executor.v2_config import validate_v2_snapshot
    snapshot = _valid_snapshot()
    snapshot["venues"] = [
        {"id": "spot_a", "market_type": "spot", "account_id": "account_a"},
        {"id": "perp_b", "market_type": "linear_perpetual", "account_id": "account_b"},
    ]
    snapshot["accounts"] = [
        {"id": "account_a", "venue_id": "spot_a"},
        {"id": "account_b", "venue_id": "perp_b"},
        {"id": "account_wrong", "venue_id": "spot_a"},
    ]
    snapshot["risk_profiles"] = {"default": {}}
    snapshot["routes"] = [{
        "id": "route_a",
        "spot": {"venue_id": "spot_a", "account_id": "account_wrong"},
        "futures": {"venue_id": "perp_b", "account_id": "account_b"},
        "risk_profile": "default",
    }]
    with pytest.raises(ConfigSnapshotError, match="account/venue mismatch"):
        validate_v2_snapshot(snapshot)


def test_rejects_telegram_token_value_but_allows_env_reference():
    from executor.v2_config import _reject_inline_secrets
    _reject_inline_secrets({"telegram": {"bot_token_env": "NAFAZ_TELEGRAM_TOKEN"}})
    with pytest.raises(ConfigSnapshotError, match="inline secret"):
        _reject_inline_secrets({"telegram": {"bot_token": "not-allowed"}})
