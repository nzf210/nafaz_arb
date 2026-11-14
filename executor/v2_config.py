"""PostgreSQL-authoritative V2 configuration snapshot loading.

This module is deliberately not wired into the legacy executor startup yet.
Callers must opt into it explicitly during the controlled runtime cutover.
Redis is populated only after a snapshot has been read and verified against
PostgreSQL; a cache hit is never treated as authority.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from executor.v2_config_schema import (
    V2ConfigValidationError,
    V2ConfigModel,
    parse_v2_config,
)

logger = logging.getLogger("executor.v2_config")

SUPPORTED_SCHEMA_VERSION = 2
REDIS_ACTIVE_SNAPSHOT_KEY = "nafaz:v2:config:active"


class ConfigSnapshotError(RuntimeError):
    """Raised when the authoritative configuration cannot be verified."""


@dataclass(frozen=True)
class ConfigSnapshot:
    version: int
    schema_version: int
    content_sha256: str
    snapshot: dict[str, Any]


def canonical_snapshot_bytes(snapshot: Mapping[str, Any]) -> bytes:
    """Canonical JSON representation used for content-address verification."""
    try:
        encoded = json.dumps(
            snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ConfigSnapshotError(f"snapshot is not valid canonical JSON: {exc}") from exc
    return encoded.encode("utf-8")


def calculate_snapshot_sha256(snapshot: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_snapshot_bytes(snapshot)).hexdigest()


def _reject_inline_secrets(value: Any, path: str = "snapshot") -> None:
    """Reject raw credential material while allowing *_env and *_ref pointers."""
    forbidden = {
        "api_key", "api_secret", "secret", "password", "passphrase",
        "token", "private_key", "access_key", "refresh_token",
    }
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            is_reference = normalized.endswith("_env") or normalized.endswith("_ref")
            is_secret_name = normalized in forbidden or normalized.endswith((
                "_api_key", "_api_secret", "_secret", "_password", "_passphrase",
                "_token", "_private_key", "_access_key",
            ))
            if is_secret_name and not is_reference:
                raise ConfigSnapshotError(
                    f"inline secret field is forbidden in V2 config: {path}.{normalized}"
                )
            _reject_inline_secrets(child, f"{path}.{normalized}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_inline_secrets(child, f"{path}[{index}]")




def validate_v2_snapshot(
    snapshot: Mapping[str, Any],
    *,
    capability_registry: Optional[Mapping[str, Any]] = None,
) -> V2ConfigModel:
    """Return the typed V2 model after strict schema and route-capability checks."""
    try:
        return parse_v2_config(snapshot, capability_registry=capability_registry)
    except V2ConfigValidationError as exc:
        raise ConfigSnapshotError(str(exc)) from exc

def validate_snapshot_row(row: Mapping[str, Any]) -> ConfigSnapshot:
    try:
        version = int(row["version"])
        schema_version = int(row["schema_version"])
        snapshot = row["snapshot"]
        expected_hash = str(row["content_sha256"]).strip().lower()
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigSnapshotError(f"active configuration row is incomplete: {exc}") from exc

    if version <= 0:
        raise ConfigSnapshotError("active configuration version must be positive")
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        raise ConfigSnapshotError(
            f"unsupported config schema_version={schema_version}; "
            f"binary supports {SUPPORTED_SCHEMA_VERSION}"
        )
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError as exc:
            raise ConfigSnapshotError("active configuration snapshot is invalid JSON") from exc
    if not isinstance(snapshot, dict):
        raise ConfigSnapshotError("active configuration snapshot must be a JSON object")

    _reject_inline_secrets(snapshot)
    validate_v2_snapshot(snapshot)
    actual_hash = calculate_snapshot_sha256(snapshot)
    if len(expected_hash) != 64 or actual_hash != expected_hash:
        raise ConfigSnapshotError("active configuration SHA-256 does not match stored hash")
    if int(snapshot.get("schema_version", -1)) != schema_version:
        raise ConfigSnapshotError("snapshot schema_version does not match database metadata")

    return ConfigSnapshot(
        version=version,
        schema_version=schema_version,
        content_sha256=actual_hash,
        snapshot=snapshot,
    )


class PostgresConfigRepository:
    """Read the active immutable snapshot from PostgreSQL."""

    _ACTIVE_SQL = """
        select v.version, v.schema_version, v.snapshot, v.content_sha256, v.status
          from public.active_config_version a
          join public.config_versions v on v.version = a.version
         where a.singleton = true and v.status = 'active'
    """

    def __init__(self, pool: Any):
        self._pool = pool

    async def fetch_active(self) -> ConfigSnapshot:
        try:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(self._ACTIVE_SQL)
        except Exception as exc:
            raise ConfigSnapshotError(f"cannot read active config from PostgreSQL: {exc}") from exc
        if row is None:
            raise ConfigSnapshotError("PostgreSQL has no active configuration version")
        return validate_snapshot_row(row)


class RedisSnapshotCache:
    """Versioned Redis mirror; it is a verified transport/cache, never authority."""

    def __init__(self, redis_client: Any, key: str = REDIS_ACTIVE_SNAPSHOT_KEY):
        self._redis = redis_client
        self._key = key

    async def publish_and_verify(self, config: ConfigSnapshot) -> None:
        payload = json.dumps(
            {
                "version": config.version,
                "schema_version": config.schema_version,
                "content_sha256": config.content_sha256,
                "snapshot": config.snapshot,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        try:
            await self._redis.set(self._key, payload)
            stored = await self._redis.get(self._key)
        except Exception as exc:
            raise ConfigSnapshotError(f"cannot publish/verify Redis config snapshot: {exc}") from exc
        if stored is None:
            raise ConfigSnapshotError("Redis config snapshot read-back returned empty")
        if isinstance(stored, bytes):
            stored = stored.decode("utf-8")
        try:
            cached = json.loads(stored)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ConfigSnapshotError("Redis config snapshot read-back is invalid JSON") from exc
        if (
            cached.get("version") != config.version
            or cached.get("schema_version") != config.schema_version
            or cached.get("content_sha256") != config.content_sha256
            or calculate_snapshot_sha256(cached.get("snapshot", {})) != config.content_sha256
        ):
            raise ConfigSnapshotError("Redis config snapshot read-back does not match PostgreSQL")


async def load_verified_active_snapshot(
    repository: PostgresConfigRepository,
    cache: Optional[RedisSnapshotCache] = None,
) -> ConfigSnapshot:
    """Fetch from PostgreSQL first; optionally mirror and verify Redis."""
    config = await repository.fetch_active()
    if cache is not None:
        await cache.publish_and_verify(config)
    return config


async def create_postgres_pool(database_url: str, *, min_size: int = 1, max_size: int = 5):
    """Create a small asyncpg pool without logging or exposing the DSN."""
    if not database_url or not database_url.strip():
        raise ConfigSnapshotError("NAFAZ_DATABASE_URL is required for PostgreSQL config")
    try:
        import asyncpg
        return await asyncpg.create_pool(
            dsn=database_url,
            min_size=min_size,
            max_size=max_size,
            command_timeout=10,
        )
    except Exception as exc:
        raise ConfigSnapshotError(f"cannot initialize PostgreSQL config pool: {exc}") from exc


class PostgresConfigWriter:
    """Transactional draft/validate/activate API with CAS, audit and outbox."""

    def __init__(self, pool: Any):
        self._pool = pool

    @staticmethod
    def _require_actor_reason(actor: str, reason: str) -> None:
        if not actor or not actor.strip():
            raise ConfigSnapshotError("config change actor is required")
        if not reason or not reason.strip():
            raise ConfigSnapshotError("config change reason is required")

    async def create_draft(
        self,
        snapshot: Mapping[str, Any],
        *,
        actor: str,
        reason: str,
        change_diff: Optional[Mapping[str, Any]] = None,
        source_version: Optional[int] = None,
    ) -> int:
        self._require_actor_reason(actor, reason)
        normalized = dict(snapshot)
        _reject_inline_secrets(normalized)
        validate_v2_snapshot(normalized)
        digest = calculate_snapshot_sha256(normalized)
        payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "select pg_advisory_xact_lock(hashtextextended('nafaz:v2:config_versions', 0))"
                )
                version = await connection.fetchval(
                    "select coalesce(max(version), 0) + 1 from public.config_versions"
                )
                await connection.execute(
                    """
                    insert into public.config_versions
                        (version, schema_version, snapshot, content_sha256, status,
                         created_by, change_reason, change_diff, source_version)
                    values ($1, $2, $3::jsonb, $4, 'draft', $5, $6, $7::jsonb, $8)
                    """,
                    version, SUPPORTED_SCHEMA_VERSION, payload, digest, actor, reason,
                    json.dumps(dict(change_diff or {}), sort_keys=True), source_version,
                )
                await connection.execute(
                    """
                    insert into public.config_audit_log
                        (version, action, actor, reason, diff)
                    values ($1, 'created', $2, $3, $4::jsonb)
                    """,
                    version, actor, reason, json.dumps(dict(change_diff or {}), sort_keys=True),
                )
                await connection.execute(
                    """
                    insert into public.outbox_events
                        (event_key, aggregate_type, aggregate_version, event_type, payload)
                    values ($1, 'config', $2, 'config.draft_created', $3::jsonb)
                    """,
                    f"config:{version}:draft_created", version,
                    json.dumps({"version": version, "content_sha256": digest}, sort_keys=True),
                )
        return int(version)

    async def validate_version(self, version: int, *, actor: str, report: Optional[Mapping[str, Any]] = None) -> ConfigSnapshot:
        self._require_actor_reason(actor, "validation")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    select version, schema_version, snapshot, content_sha256, status
                      from public.config_versions where version = $1 for update
                    """,
                    version,
                )
                if row is None:
                    raise ConfigSnapshotError(f"config version {version} does not exist")
                if row["status"] not in {"draft", "validated"}:
                    raise ConfigSnapshotError(f"config version {version} cannot be validated from status {row['status']}")
                config = validate_snapshot_row(row)
                await connection.execute(
                    """
                    update public.config_versions
                       set status = 'validated', validated_by = $2,
                           validated_at = now(), validation_report = $3::jsonb
                     where version = $1
                    """,
                    version, actor, json.dumps(dict(report or {"result": "passed"}), sort_keys=True),
                )
                await connection.execute(
                    """
                    insert into public.config_audit_log(version, action, actor, reason, diff)
                    values ($1, 'validated', $2, 'snapshot contract validation passed', $3::jsonb)
                    """,
                    version, actor, json.dumps(dict(report or {"result": "passed"}), sort_keys=True),
                )
        return config

    async def activate_version(
        self,
        version: int,
        *,
        expected_current_version: Optional[int],
        actor: str,
        reason: str,
    ) -> ConfigSnapshot:
        self._require_actor_reason(actor, reason)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "select pg_advisory_xact_lock(hashtextextended('nafaz:v2:active_config', 0))"
                )
                current = await connection.fetchval(
                    "select version from public.active_config_version where singleton = true for update"
                )
                if current != expected_current_version:
                    raise ConfigSnapshotError(
                        f"stale config activation: expected current {expected_current_version}, found {current}"
                    )
                row = await connection.fetchrow(
                    """
                    select version, schema_version, snapshot, content_sha256, status
                      from public.config_versions where version = $1 for update
                    """,
                    version,
                )
                if row is None or row["status"] != "validated":
                    raise ConfigSnapshotError("only an existing validated config version can be activated")
                config = validate_snapshot_row(row)
                if current is not None:
                    await connection.execute(
                        "update public.config_versions set status = 'superseded' where version = $1",
                        current,
                    )
                await connection.execute(
                    """
                    insert into public.active_config_version(singleton, version, updated_by, reason)
                    values (true, $1, $2, $3)
                    on conflict (singleton) do update
                       set version = excluded.version, updated_at = now(),
                           updated_by = excluded.updated_by, reason = excluded.reason
                    """,
                    version, actor, reason,
                )
                await connection.execute(
                    """
                    update public.config_versions set status = 'active'
                     where version = $1
                    """,
                    version,
                )
                await connection.execute(
                    """
                    insert into public.config_audit_log
                        (version, previous_version, action, actor, reason, diff)
                    values ($1, $2, 'activated', $3, $4, $5::jsonb)
                    """,
                    version, current, actor, reason,
                    json.dumps({"expected_previous_version": expected_current_version}, sort_keys=True),
                )
                await connection.execute(
                    """
                    insert into public.outbox_events
                        (event_key, aggregate_type, aggregate_version, event_type, payload)
                    values ($1, 'config', $2, 'config.activated', $3::jsonb)
                    """,
                    f"config:{version}:activated", version,
                    json.dumps({
                        "version": version,
                        "previous_version": current,
                        "content_sha256": config.content_sha256,
                    }, sort_keys=True),
                )
        return config

    async def create_rollback_draft(
        self,
        source_version: int,
        *,
        actor: str,
        reason: str,
    ) -> int:
        self._require_actor_reason(actor, reason)
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                select version, schema_version, snapshot, content_sha256, status
                  from public.config_versions where version = $1
                """,
                source_version,
            )
        if row is None:
            raise ConfigSnapshotError(f"rollback source version {source_version} does not exist")
        source = validate_snapshot_row(row)
        return await self.create_draft(
            source.snapshot,
            actor=actor,
            reason=reason,
            change_diff={"rollback_from_version": source_version},
            source_version=source_version,
        )


class PostgresOutboxPublisher:
    """At-least-once PostgreSQL outbox delivery to Redis.

    Consumers must deduplicate by event_key. A crash after Redis publish but
    before the PostgreSQL acknowledgement can intentionally cause redelivery.
    """

    CLAIM_SQL = """
        with candidates as (
            select event_id
              from public.outbox_events
             where published_at is null
               and next_attempt_at <= now()
               and (locked_at is null or locked_at < now() - make_interval(secs => $3))
             order by event_id
             for update skip locked
             limit $1
        )
        update public.outbox_events as event
           set locked_at = now(),
               locked_by = $2,
               attempts = event.attempts + 1
          from candidates
         where event.event_id = candidates.event_id
        returning event.event_id, event.event_key, event.aggregate_type,
                  event.aggregate_version, event.event_type, event.payload,
                  event.attempts
    """

    def __init__(
        self,
        pool: Any,
        redis: Any,
        *,
        worker_id: str,
        channel: str = "nafaz:v2:events",
        execution_channel: str = "nafaz:v2:execution:events",
        batch_size: int = 50,
        lease_seconds: int = 60,
        max_backoff_seconds: int = 300,
    ):
        if not worker_id or not worker_id.strip():
            raise ValueError("outbox worker_id is required")
        if batch_size < 1 or lease_seconds < 1 or max_backoff_seconds < 1:
            raise ValueError("outbox batch, lease and backoff settings must be positive")
        self._pool = pool
        self._redis = redis
        self._worker_id = worker_id
        self._channel = channel
        self._execution_channel = execution_channel
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_backoff_seconds = max_backoff_seconds

    async def _claim(self) -> list[Mapping[str, Any]]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                self.CLAIM_SQL,
                self._batch_size,
                self._worker_id,
                self._lease_seconds,
            )
        return list(rows or [])

    async def _acknowledge(self, event_id: int) -> bool:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                update public.outbox_events
                   set published_at = now(), locked_at = null, locked_by = null,
                       last_error = null
                 where event_id = $1 and locked_by = $2 and published_at is null
                """,
                event_id,
                self._worker_id,
            )
        return str(result).endswith(" 1")

    async def _retry(self, event_id: int, attempts: int, error: Exception) -> None:
        delay = min(self._max_backoff_seconds, 2 ** min(max(attempts, 1), 8))
        # Do not persist arbitrary exception strings: they can contain connection
        # details. Store only the exception class as a bounded diagnostic.
        safe_error = type(error).__name__[:80]
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                update public.outbox_events
                   set next_attempt_at = now() + make_interval(secs => $3),
                       locked_at = null, locked_by = null, last_error = $4
                 where event_id = $1 and locked_by = $2 and published_at is null
                """,
                event_id,
                self._worker_id,
                delay,
                safe_error,
            )

    async def publish_pending(self) -> dict[str, int]:
        """Publish one bounded batch; return counts without hiding failures."""
        events = await self._claim()
        result = {"claimed": len(events), "published": 0, "failed": 0, "ack_lost": 0}
        for event in events:
            event_id = int(event["event_id"])
            attempts = int(event["attempts"])
            envelope = {
                "event_id": event_id,
                "event_key": str(event["event_key"]),
                "aggregate_type": str(event["aggregate_type"]),
                "aggregate_version": int(event["aggregate_version"]),
                "event_type": str(event["event_type"]),
                "payload": event["payload"],
            }
            try:
                channel = (
                    self._execution_channel
                    if envelope["aggregate_type"] == "execution"
                    else self._channel
                )
                await self._redis.publish(
                    channel,
                    json.dumps(envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False),
                )
            except Exception as exc:
                result["failed"] += 1
                await self._retry(event_id, attempts, exc)
                logger.warning(
                    "v2 outbox publish failed event_id=%s error_type=%s",
                    event_id,
                    type(exc).__name__,
                )
                continue
            if await self._acknowledge(event_id):
                result["published"] += 1
            else:
                # Redis may already have received the event. Report the lost
                # acknowledgement so monitoring can detect likely redelivery.
                result["ack_lost"] += 1
        return result
