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




_ALLOWED_ROOT_KEYS = {
    "schema_version", "mode", "execution", "storage", "redis",
    "venues", "accounts", "routes", "risk_profiles", "scanner",
    "observability", "telegram", "fx",
}
_REQUIRED_ROOT_KEYS = {
    "schema_version", "mode", "execution", "storage", "redis",
    "venues", "accounts", "routes", "risk_profiles", "scanner",
}


def validate_v2_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Validate the V2 root contract and all stable cross-object references."""
    if not isinstance(snapshot, dict):
        raise ConfigSnapshotError("V2 config must be a JSON object")
    missing = sorted(_REQUIRED_ROOT_KEYS - set(snapshot))
    unknown = sorted(set(snapshot) - _ALLOWED_ROOT_KEYS)
    if missing:
        raise ConfigSnapshotError(f"V2 config missing required root keys: {', '.join(missing)}")
    if unknown:
        raise ConfigSnapshotError(f"V2 config has unsupported root keys: {', '.join(unknown)}")
    if snapshot.get("schema_version") != SUPPORTED_SCHEMA_VERSION:
        raise ConfigSnapshotError("V2 config schema_version is not supported")
    if str(snapshot.get("mode", "")).upper() not in {"LIVE", "MOCK"}:
        raise ConfigSnapshotError("V2 config mode must be LIVE or MOCK")

    execution = snapshot.get("execution")
    scanner = snapshot.get("scanner")
    if not isinstance(execution, dict) or execution.get("engine") != "python":
        raise ConfigSnapshotError("V2 execution.engine must be python")
    if execution.get("signals_enabled") is not False:
        raise ConfigSnapshotError("V2 signals must remain disabled until sole-consumer proof")
    if not isinstance(scanner, dict) or scanner.get("public_market_data_only") is not True:
        raise ConfigSnapshotError("V2 scanner must be public-market-data-only")
    if str(snapshot["mode"]).upper() == "LIVE":
        synthetic_fx = scanner.get("synthetic_fx", {})
        if isinstance(synthetic_fx, dict) and synthetic_fx.get("static_fallback_allowed_in_live") is True:
            raise ConfigSnapshotError("static FX fallback is forbidden for LIVE")

    venues = snapshot.get("venues")
    accounts = snapshot.get("accounts")
    routes = snapshot.get("routes")
    risk_profiles = snapshot.get("risk_profiles")
    if not all(isinstance(value, list) for value in (venues, accounts, routes)):
        raise ConfigSnapshotError("venues, accounts and routes must be arrays")
    if not isinstance(risk_profiles, dict):
        raise ConfigSnapshotError("risk_profiles must be an object")

    def unique_index(items: list, label: str) -> dict:
        index = {}
        for item in items:
            if not isinstance(item, dict):
                raise ConfigSnapshotError(f"{label} entries must be objects")
            identifier = item.get("id")
            if not isinstance(identifier, str) or not identifier or identifier.strip() != identifier:
                raise ConfigSnapshotError(f"{label} entry has invalid id")
            if identifier in index:
                raise ConfigSnapshotError(f"duplicate {label} id: {identifier}")
            index[identifier] = item
        return index

    venue_index = unique_index(venues, "venue")
    account_index = unique_index(accounts, "account")
    route_index = unique_index(routes, "route")
    del route_index

    for venue_id, venue in venue_index.items():
        if venue.get("market_type") not in {"spot", "linear_perpetual", "inverse_perpetual"}:
            raise ConfigSnapshotError(f"venue {venue_id} has unsupported market_type")
        account_id = venue.get("account_id")
        if account_id not in account_index:
            raise ConfigSnapshotError(f"venue {venue_id} references unknown account {account_id!r}")
    for account_id, account in account_index.items():
        if account.get("venue_id") not in venue_index:
            raise ConfigSnapshotError(f"account {account_id} references unknown venue")
    for route in routes:
        route_id = route["id"]
        spot = route.get("spot")
        futures = route.get("futures")
        if not isinstance(spot, dict) or not isinstance(futures, dict):
            raise ConfigSnapshotError(f"route {route_id} must define spot and futures objects")
        for leg_name, leg in (("spot", spot), ("futures", futures)):
            venue_id = leg.get("venue_id")
            account_id = leg.get("account_id")
            if venue_id not in venue_index or account_id not in account_index:
                raise ConfigSnapshotError(f"route {route_id} {leg_name} references unknown venue/account")
            if account_index[account_id].get("venue_id") != venue_id:
                raise ConfigSnapshotError(f"route {route_id} {leg_name} account/venue mismatch")
        if not route.get("risk_profile") or route["risk_profile"] not in risk_profiles:
            raise ConfigSnapshotError(f"route {route_id} references unknown risk_profile")


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
