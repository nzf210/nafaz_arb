"""Runtime config invalidation, atomic snapshot swap and multi-instance convergence.

Redis messages are notifications only. Every accepted invalidation causes a
fresh PostgreSQL read, contract/hash validation, Redis mirror verification,
then an atomic in-process snapshot replacement and durable DB acknowledgement.
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from executor.v2_config import (
    ConfigSnapshot,
    ConfigSnapshotError,
    PostgresConfigRepository,
    RedisSnapshotCache,
    load_verified_active_snapshot,
)

logger = logging.getLogger("executor.v2_config_runtime")
CONFIG_EVENTS_CHANNEL = "nafaz:v2:events"


@dataclass(frozen=True)
class ConvergenceReport:
    converged: bool
    active_instances: int
    missing_or_stale: tuple[str, ...]
    target_version: Optional[int]
    target_sha256: Optional[str]


class ConfigConsumerRegistry:
    """PostgreSQL-backed executor membership, leases and config acknowledgements."""

    def __init__(self, pool: Any, *, instance_id: str, process_id: str):
        if not instance_id or not instance_id.strip():
            raise ValueError("instance_id is required")
        if not process_id or not process_id.strip():
            raise ValueError("process_id is required")
        self._pool = pool
        self.instance_id = instance_id
        self.process_id = process_id

    async def heartbeat(self) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                insert into public.config_consumers
                    (instance_id, process_id, status, last_seen_at, started_at)
                values ($1, $2, 'STARTING', now(), now())
                on conflict (instance_id) do update
                   set process_id = excluded.process_id,
                       last_seen_at = now(),
                       status = case
                           when public.config_consumers.process_id is distinct from excluded.process_id
                             or public.config_consumers.status = 'STOPPED' then 'STARTING'
                           else public.config_consumers.status
                       end,
                       stopped_at = null
                """,
                self.instance_id,
                self.process_id,
            )

    async def acknowledge(self, config: ConfigSnapshot) -> None:
        async with self._pool.acquire() as connection:
            result = await connection.execute(
                """
                update public.config_consumers
                   set acknowledged_version = $2,
                       acknowledged_sha256 = $3,
                       last_ack_at = now(),
                       last_seen_at = now(),
                       status = 'ACTIVE',
                       last_error_code = null
                 where instance_id = $1 and process_id = $4
                """,
                self.instance_id,
                config.version,
                config.content_sha256,
                self.process_id,
            )
        if not str(result).endswith(" 1"):
            raise ConfigSnapshotError("executor instance lease disappeared before config acknowledgement")

    async def mark_degraded(self, error: Exception) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                update public.config_consumers
                   set status = 'DEGRADED', last_seen_at = now(),
                       last_error_code = $2
                 where instance_id = $1 and process_id = $3
                """,
                self.instance_id,
                type(error).__name__[:80],
                self.process_id,
            )

    async def stop(self) -> None:
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                update public.config_consumers
                   set status = 'STOPPED', stopped_at = now(), last_seen_at = now()
                 where instance_id = $1 and process_id = $2
                """,
                self.instance_id,
                self.process_id,
            )

    async def convergence(
        self,
        target: ConfigSnapshot,
        *,
        lease_seconds: int = 30,
        minimum_instances: int = 1,
    ) -> ConvergenceReport:
        if lease_seconds < 1 or minimum_instances < 1:
            raise ValueError("lease_seconds and minimum_instances must be positive")
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                select instance_id, status, acknowledged_version, acknowledged_sha256
                  from public.config_consumers
                 where status in ('STARTING','ACTIVE','DEGRADED')
                   and last_seen_at >= now() - make_interval(secs => $1)
                 order by instance_id
                """,
                lease_seconds,
            )
        active = list(rows or [])
        lagging_items = [
            str(row["instance_id"])
            for row in active
            if row["status"] != "ACTIVE"
            or row["acknowledged_version"] != target.version
            or str(row["acknowledged_sha256"] or "").strip() != target.content_sha256
        ]
        if len(active) < minimum_instances:
            lagging_items.append(f"insufficient_instances:{len(active)}/{minimum_instances}")
        lagging = tuple(lagging_items)
        converged = len(active) >= minimum_instances and not lagging
        return ConvergenceReport(
            converged=converged,
            active_instances=len(active),
            missing_or_stale=lagging if active else ("no_live_executor_instances",),
            target_version=target.version,
            target_sha256=target.content_sha256,
        )

    async def require_converged(
        self,
        target: ConfigSnapshot,
        *,
        lease_seconds: int = 30,
        minimum_instances: int = 1,
    ) -> ConvergenceReport:
        report = await self.convergence(
            target, lease_seconds=lease_seconds, minimum_instances=minimum_instances
        )
        if not report.converged:
            raise ConfigSnapshotError(
                "config convergence gate closed: "
                f"active_instances={report.active_instances}, "
                f"lagging={','.join(report.missing_or_stale)}"
            )
        return report


class AtomicConfigSnapshot:
    """Single-reference snapshot store; readers never see a partially applied dict."""

    def __init__(self, initial: Optional[ConfigSnapshot] = None):
        self._current = copy.deepcopy(initial)
        self._lock = asyncio.Lock()

    @property
    def current(self) -> Optional[ConfigSnapshot]:
        # ConfigSnapshot is a frozen dataclass, but its nested JSON dict is
        # mutable. Return a defensive copy so one consumer cannot mutate the
        # process-wide version seen by other admission checks.
        return copy.deepcopy(self._current)

    async def swap(self, config: ConfigSnapshot) -> None:
        snapshot = copy.deepcopy(config)
        async with self._lock:
            self._current = snapshot


class ConfigInvalidationConsumer:
    """Handle config events by refetching PostgreSQL, not trusting Redis payload."""

    def __init__(
        self,
        repository: PostgresConfigRepository,
        cache: RedisSnapshotCache,
        registry: ConfigConsumerRegistry,
        store: AtomicConfigSnapshot,
    ):
        self._repository = repository
        self._cache = cache
        self._registry = registry
        self._store = store
        self._reload_lock = asyncio.Lock()

    @staticmethod
    def _parse_event(message: Any) -> Mapping[str, Any]:
        if isinstance(message, dict):
            data = message
        else:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            if isinstance(message, str):
                data = json.loads(message)
            elif isinstance(message, Mapping):
                data = message
            else:
                raise ConfigSnapshotError("unsupported config invalidation message")
        if not isinstance(data, Mapping):
            raise ConfigSnapshotError("config invalidation message must be an object")
        if data.get("aggregate_type") != "config":
            raise ConfigSnapshotError("message is not a config invalidation event")
        if not data.get("event_key"):
            raise ConfigSnapshotError("config invalidation event_key is required")
        return data

    async def reload_authoritative(self) -> ConfigSnapshot:
        # Serialize reloads within this process. Duplicate/out-of-order events are
        # harmless because the authoritative active pointer is read each time.
        async with self._reload_lock:
            try:
                config = await load_verified_active_snapshot(self._repository, self._cache)
                await self._store.swap(config)
                await self._registry.acknowledge(config)
                return config
            except Exception as exc:
                try:
                    await self._registry.mark_degraded(exc)
                except Exception:
                    logger.error("could not persist degraded consumer state")
                raise

    async def handle_event(self, message: Any) -> ConfigSnapshot:
        self._parse_event(message)
        # Event contents are deliberately not used to construct a config snapshot.
        return await self.reload_authoritative()

    async def run(
        self,
        redis_client: Any,
        *,
        stop_event: Optional[asyncio.Event] = None,
        heartbeat_interval: float = 5.0,
        reconcile_interval: float = 10.0,
    ) -> None:
        """Consume hints and periodically reconcile to recover missed Pub/Sub events."""
        if heartbeat_interval <= 0 or reconcile_interval <= 0:
            raise ValueError("heartbeat_interval and reconcile_interval must be positive")
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CONFIG_EVENTS_CHANNEL)
        loop = asyncio.get_running_loop()
        next_heartbeat = loop.time() + heartbeat_interval
        next_reconcile = loop.time() + reconcile_interval
        try:
            await self._registry.heartbeat()
            # A fresh process must load and acknowledge before it can count as converged.
            await self.reload_authoritative()
            while stop_event is None or not stop_event.is_set():
                now = loop.time()
                if now >= next_heartbeat:
                    try:
                        await self._registry.heartbeat()
                    except Exception as exc:
                        logger.error("config consumer heartbeat failed error_type=%s", type(exc).__name__)
                    next_heartbeat = now + heartbeat_interval
                if now >= next_reconcile:
                    try:
                        await self.reload_authoritative()
                    except Exception as exc:
                        logger.error("periodic config reconciliation failed error_type=%s", type(exc).__name__)
                    next_reconcile = now + reconcile_interval
                item = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if item:
                    try:
                        await self.handle_event(item.get("data"))
                    except Exception as exc:
                        logger.error("config invalidation reload failed error_type=%s", type(exc).__name__)
        finally:
            await pubsub.unsubscribe(CONFIG_EVENTS_CHANNEL)
            close = getattr(pubsub, "aclose", None) or getattr(pubsub, "close", None)
            if close:
                result = close()
                if hasattr(result, "__await__"):
                    await result


class PostgresEntryAdmissionGate:
    """Fail-closed final admission check against PostgreSQL control state.

    The gate deliberately refuses snapshots whose scanner signals remain
    disabled. It also verifies the DB active pointer and RUNNING operator state
    on every check, then requires every live executor lease to acknowledge the
    same version/hash. A caller must invoke it immediately before each entry
    submission; it is not a substitute for the future transactional STOP fence.
    """

    def __init__(
        self,
        pool: Any,
        store: AtomicConfigSnapshot,
        registry: ConfigConsumerRegistry,
        cache: Optional[RedisSnapshotCache] = None,
    ):
        self._pool = pool
        self._store = store
        self._registry = registry
        self._cache = cache

    async def _verify_redis_snapshot(self, config: ConfigSnapshot) -> None:
        if self._cache is None:
            raise ConfigSnapshotError("Redis snapshot verification is not configured")
        await self._cache.verify(config)

    async def require_entry_allowed(self) -> ConvergenceReport:
        config = self._store.current
        if config is None:
            raise ConfigSnapshotError("no verified V2 config snapshot is loaded")
        execution = config.snapshot.get("execution")
        if not isinstance(execution, dict) or execution.get("signals_enabled") is not True:
            raise ConfigSnapshotError("V2 execution signals are disabled; entry admission is closed")
        await self._verify_redis_snapshot(config)

        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                select s.state, s.trading_enabled, s.config_version,
                       v.version as active_version, v.content_sha256 as active_sha256
                  from public.operator_control_state s
                  join public.active_config_version p on p.singleton = true
                  join public.config_versions v on v.version = p.version
                 where s.singleton = true and v.status = 'active'
                """
            )
        if not row:
            raise ConfigSnapshotError("durable operator state or active config pointer is missing")
        if row["state"] != "RUNNING" or row["trading_enabled"] is not True:
            raise ConfigSnapshotError("durable operator state is not RUNNING")
        if row["config_version"] != config.version or row["active_version"] != config.version:
            raise ConfigSnapshotError("executor snapshot is not the active operator config version")
        if str(row["active_sha256"]).strip() != config.content_sha256:
            raise ConfigSnapshotError("active PostgreSQL config hash differs from executor snapshot")
        return await self._registry.require_converged(config)

    @asynccontextmanager
    async def entry_submission_fence(self):
        """Serialize the final exchange entry call against durable STOP/config activation.

        The shared transaction-scoped advisory lock is held until the caller's
        exchange submission returns. STOP and config activation take the same
        lock exclusively, so once STOP commits no later entry can pass this
        fence. Existing position-management and exit paths do not use it.
        """
        config = self._store.current
        if config is None:
            raise ConfigSnapshotError("no verified V2 config snapshot is loaded")
        execution = config.snapshot.get("execution")
        if not isinstance(execution, dict) or execution.get("signals_enabled") is not True:
            raise ConfigSnapshotError("V2 execution signals are disabled; entry admission is closed")

        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "select pg_advisory_xact_lock_shared(hashtextextended($1, 0))",
                    "nafaz:v2:operator_entry_fence",
                )
                await self._verify_redis_snapshot(config)
                row = await connection.fetchrow(
                    """
                    select s.state, s.trading_enabled, s.config_version,
                           v.version as active_version, v.content_sha256 as active_sha256
                      from public.operator_control_state s
                      join public.active_config_version p on p.singleton = true
                      join public.config_versions v on v.version = p.version
                     where s.singleton = true and v.status = 'active'
                    """
                )
                if not row:
                    raise ConfigSnapshotError("durable operator state or active config pointer is missing")
                if row["state"] != "RUNNING" or row["trading_enabled"] is not True:
                    raise ConfigSnapshotError("durable operator state is not RUNNING")
                if row["config_version"] != config.version or row["active_version"] != config.version:
                    raise ConfigSnapshotError("executor snapshot is not the active operator config version")
                if str(row["active_sha256"]).strip() != config.content_sha256:
                    raise ConfigSnapshotError("active PostgreSQL config hash differs from executor snapshot")

                consumers = await connection.fetch(
                    """
                    select instance_id, status, acknowledged_version, acknowledged_sha256
                      from public.config_consumers
                     where status in ('STARTING','ACTIVE','DEGRADED')
                       and last_seen_at >= now() - make_interval(secs => 30)
                     order by instance_id
                    """
                )
                active = len(consumers)
                stale = tuple(
                    str(item["instance_id"])
                    for item in consumers
                    if item["status"] != "ACTIVE"
                    or item["acknowledged_version"] != config.version
                    or str(item["acknowledged_sha256"] or "").strip() != config.content_sha256
                )
                if active < 1:
                    raise ConfigSnapshotError("no live executor consumer has acknowledged the active config")
                if stale:
                    raise ConfigSnapshotError(
                        "executor config convergence is incomplete: " + ", ".join(stale)
                    )
                yield ConvergenceReport(
                    converged=True,
                    active_instances=active,
                    missing_or_stale=(),
                    target_version=config.version,
                    target_sha256=config.content_sha256,
                )
