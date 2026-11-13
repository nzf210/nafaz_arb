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
            if normalized in forbidden:
                raise ConfigSnapshotError(
                    f"inline secret field is forbidden in V2 config: {path}.{normalized}"
                )
            _reject_inline_secrets(child, f"{path}.{normalized}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_inline_secrets(child, f"{path}[{index}]")


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
