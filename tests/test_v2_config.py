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
    referenced["telegram"] = {"bot_token_env": "NAFAZ_TELEGRAM_TOKEN"}
    assert validate_snapshot_row({
        "version": 8,
        "schema_version": 2,
        "snapshot": referenced,
        "content_sha256": calculate_snapshot_sha256(referenced),
    }).version == 8

    inline = _valid_snapshot()
    inline["telegram"] = {"bot_token": "must-not-be-stored"}
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
        {"id": "account_wrong", "venue_id": "perp_b"},
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



class _OutboxConnection:
    def __init__(self, events):
        self.events = events
        self.acked = []
        self.retried = []

    async def fetch(self, _query, _batch_size, _worker_id, _lease_seconds):
        return self.events

    async def execute(self, query, *args):
        if "published_at = now()" in query:
            self.acked.append(args)
        elif "next_attempt_at = now()" in query:
            self.retried.append(args)
        return "UPDATE 1"


class _OutboxPool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


class _PublishRedis:
    def __init__(self, fail=False):
        self.messages = []
        self.fail = fail

    async def publish(self, channel, message):
        if self.fail:
            raise OSError("redis connection detail must not be persisted")
        self.messages.append((channel, json.loads(message)))
        return 1


@pytest.mark.asyncio
async def test_outbox_publisher_delivers_and_acknowledges_event():
    from executor.v2_config import PostgresOutboxPublisher

    event = {
        "event_id": 41,
        "event_key": "config:8:activated",
        "aggregate_type": "config",
        "aggregate_version": 8,
        "event_type": "config.activated",
        "payload": {"version": 8, "content_sha256": "a" * 64},
        "attempts": 1,
    }
    connection = _OutboxConnection([event])
    redis = _PublishRedis()
    publisher = PostgresOutboxPublisher(
        _OutboxPool(connection), redis, worker_id="outbox-test-worker"
    )

    result = await publisher.publish_pending()

    assert result == {"claimed": 1, "published": 1, "failed": 0, "ack_lost": 0}
    assert redis.messages[0][0] == "nafaz:v2:events"
    assert redis.messages[0][1]["event_key"] == "config:8:activated"
    assert len(connection.acked) == 1


@pytest.mark.asyncio
async def test_outbox_publisher_retries_and_does_not_persist_exception_details():
    from executor.v2_config import PostgresOutboxPublisher

    event = {
        "event_id": 42,
        "event_key": "config:9:activated",
        "aggregate_type": "config",
        "aggregate_version": 9,
        "event_type": "config.activated",
        "payload": {"version": 9},
        "attempts": 2,
    }
    connection = _OutboxConnection([event])
    publisher = PostgresOutboxPublisher(
        _OutboxPool(connection), _PublishRedis(fail=True), worker_id="outbox-test-worker"
    )

    result = await publisher.publish_pending()

    assert result == {"claimed": 1, "published": 0, "failed": 1, "ack_lost": 0}
    assert len(connection.retried) == 1
    assert connection.retried[0][2] == 4
    assert connection.retried[0][3] == "OSError"


class _RuntimeRepository:
    def __init__(self, config):
        self.config = config
        self.reads = 0

    async def fetch_active(self):
        self.reads += 1
        return self.config


class _RuntimeCache:
    def __init__(self):
        self.published = []

    async def publish_and_verify(self, config):
        self.published.append(config)


class _RuntimeRegistry:
    def __init__(self):
        self.acks = []
        self.degraded = []

    async def acknowledge(self, config):
        self.acks.append(config)

    async def mark_degraded(self, error):
        self.degraded.append(type(error).__name__)


@pytest.mark.asyncio
async def test_config_invalidation_refetches_postgres_then_swaps_and_acknowledges():
    from executor.v2_config import ConfigSnapshot
    from executor.v2_config_runtime import (
        AtomicConfigSnapshot,
        ConfigInvalidationConsumer,
    )

    config = ConfigSnapshot(
        version=12,
        schema_version=2,
        content_sha256="b" * 64,
        snapshot=_valid_snapshot(),
    )
    repository = _RuntimeRepository(config)
    cache = _RuntimeCache()
    registry = _RuntimeRegistry()
    store = AtomicConfigSnapshot()
    consumer = ConfigInvalidationConsumer(repository, cache, registry, store)

    result = await consumer.handle_event({
        "event_key": "config:12:activated",
        "aggregate_type": "config",
        "payload": {"version": 999, "content_sha256": "untrusted"},
    })

    assert result == config
    assert repository.reads == 1
    assert cache.published == [config]
    assert store.current == config
    assert registry.acks == [config]


@pytest.mark.asyncio
async def test_config_invalidation_rejects_non_config_event():
    from executor.v2_config_runtime import (
        AtomicConfigSnapshot,
        ConfigInvalidationConsumer,
    )

    registry = _RuntimeRegistry()
    consumer = ConfigInvalidationConsumer(
        _RuntimeRepository(None), _RuntimeCache(), registry, AtomicConfigSnapshot()
    )
    with pytest.raises(ConfigSnapshotError, match="not a config"):
        await consumer.handle_event({
            "event_key": "operator:4:stop",
            "aggregate_type": "operator_control",
        })
    assert registry.acks == []


class _ConvergenceConnection:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, _query, _lease_seconds):
        return self.rows


class _ConvergencePool:
    def __init__(self, rows):
        self.connection = _ConvergenceConnection(rows)

    def acquire(self):
        return _Acquire(self.connection)


@pytest.mark.asyncio
async def test_config_convergence_requires_every_live_instance_to_ack_exact_version_hash():
    from executor.v2_config import ConfigSnapshot
    from executor.v2_config_runtime import ConfigConsumerRegistry

    target = ConfigSnapshot(
        version=13,
        schema_version=2,
        content_sha256="c" * 64,
        snapshot=_valid_snapshot(),
    )
    rows = [
        {"instance_id": "executor-a", "status": "ACTIVE",
         "acknowledged_version": 13, "acknowledged_sha256": "c" * 64},
        {"instance_id": "executor-b", "status": "ACTIVE",
         "acknowledged_version": 12, "acknowledged_sha256": "d" * 64},
    ]
    registry = ConfigConsumerRegistry(
        _ConvergencePool(rows), instance_id="executor-a", process_id="pid-a"
    )
    report = await registry.convergence(target)
    assert report.converged is False
    assert report.active_instances == 2
    assert report.missing_or_stale == ("executor-b",)
    with pytest.raises(ConfigSnapshotError, match="convergence gate closed"):
        await registry.require_converged(target)


@pytest.mark.asyncio
async def test_config_convergence_fails_closed_when_no_executor_has_live_lease():
    from executor.v2_config import ConfigSnapshot
    from executor.v2_config_runtime import ConfigConsumerRegistry

    target = ConfigSnapshot(
        version=14,
        schema_version=2,
        content_sha256="e" * 64,
        snapshot=_valid_snapshot(),
    )
    registry = ConfigConsumerRegistry(
        _ConvergencePool([]), instance_id="executor-a", process_id="pid-a"
    )
    report = await registry.convergence(target)
    assert report.converged is False
    assert report.missing_or_stale == ("no_live_executor_instances",)
