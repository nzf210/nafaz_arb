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


def _row(snapshot=None):
    snapshot = snapshot or {"schema_version": 2, "mode": "LIVE", "execution": {"signals_enabled": False}}
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
