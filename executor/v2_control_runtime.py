"""Opt-in V2 PostgreSQL control-plane bootstrap for the Python executor.

The legacy executor configuration remains untouched during the migration.
When enabled, this module loads and verifies the V2 snapshot, starts the
PostgreSQL-backed consumer/outbox loops, and exposes the durable admission gate.
A missing readiness provider intentionally makes Telegram START fail closed.
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import uuid
from typing import Any, Optional

import redis.asyncio as aioredis

from executor.v2_config import (
    ConfigSnapshotError,
    PostgresConfigRepository,
    RedisSnapshotCache,
    create_postgres_pool,
    load_verified_active_snapshot,
    PostgresOutboxPublisher,
)
from executor.v2_config_runtime import (
    AtomicConfigSnapshot,
    ConfigConsumerRegistry,
    ConfigInvalidationConsumer,
    PostgresEntryAdmissionGate,
)
from executor.v2_operator_control import (
    OperatorControlError,
    PostgresOperatorControlRepository,
    TelegramPolicy,
)
from executor.v2_telegram_bot import TelegramControlBot, TelegramControlService

logger = logging.getLogger("executor.v2_control_runtime")


def _enabled(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


class V2ControlPlaneRuntime:
    """Owns V2 config convergence, durable operator state and Telegram polling."""

    def __init__(
        self,
        *,
        pool: Any,
        redis: Any,
        repository: PostgresConfigRepository,
        registry: ConfigConsumerRegistry,
        operator_repository: PostgresOperatorControlRepository,
        admission_gate: PostgresEntryAdmissionGate,
        invalidation_consumer: ConfigInvalidationConsumer,
        outbox_publisher: PostgresOutboxPublisher,
        telegram_bot: Optional[TelegramControlBot],
        instance_id: str,
        process_id: str,
    ):
        self.pool = pool
        self.redis = redis
        self.repository = repository
        self.registry = registry
        self.operator_repository = operator_repository
        self.admission_gate = admission_gate
        self._invalidation_consumer = invalidation_consumer
        self._outbox_publisher = outbox_publisher
        self._telegram_bot = telegram_bot
        self.instance_id = instance_id
        self.process_id = process_id
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._started = False

    @classmethod
    async def create(
        cls,
        executor_config: Any,
        *,
        readiness_provider=None,
    ) -> "V2ControlPlaneRuntime":
        """Bootstrap only from an explicit V2 opt-in and verified DB snapshot."""
        if not _enabled(os.environ.get("NAFAZ_V2_CONTROL_ENABLED")):
            raise ConfigSnapshotError("NAFAZ_V2_CONTROL_ENABLED is not enabled")

        dsn_env = os.environ.get("NAFAZ_V2_DATABASE_DSN_ENV", "NAFAZ_DATABASE_URL").strip()
        if not dsn_env or not dsn_env.replace("_", "").isalnum() or not dsn_env[0].isalpha():
            raise ConfigSnapshotError("V2 bootstrap database environment name is invalid")
        dsn = os.environ.get(dsn_env, "")
        pool = await create_postgres_pool(dsn)
        redis = None
        try:
            repository = PostgresConfigRepository(pool)
            bootstrap_config = await repository.fetch_active()
            storage = bootstrap_config.snapshot.get("storage", {})
            if storage.get("dsn_env") != dsn_env:
                raise ConfigSnapshotError(
                    "bootstrap database environment does not match active V2 config"
                )

            redis_settings = bootstrap_config.snapshot.get("redis", {})
            redis_env = redis_settings.get("url_env")
            if not isinstance(redis_env, str) or not redis_env.strip():
                raise ConfigSnapshotError("active V2 config has no Redis URL environment reference")
            redis_url = os.environ.get(redis_env, "")
            if not redis_url.strip():
                raise ConfigSnapshotError("active V2 Redis URL secret reference is unresolved")
            legacy_redis_url = str(getattr(executor_config, "redis_url", "") or "")
            if not legacy_redis_url or legacy_redis_url != redis_url:
                raise ConfigSnapshotError(
                    "legacy executor Redis endpoint differs from active V2 Redis reference"
                )

            redis = aioredis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            cache = RedisSnapshotCache(redis)
            config = await load_verified_active_snapshot(repository, cache)
            store = AtomicConfigSnapshot(config)

            suffix = uuid.uuid4().hex[:12]
            instance_id = (
                os.environ.get("NAFAZ_EXECUTOR_INSTANCE_ID", "").strip()
                or f"{socket.gethostname()}-{suffix}"
            )
            process_id = f"{os.getpid()}-{suffix}"
            registry = ConfigConsumerRegistry(
                pool, instance_id=instance_id, process_id=process_id
            )
            operator_repository = PostgresOperatorControlRepository(pool)

            # A process restart is not an operator START. Reset the durable
            # state before this instance can acknowledge config convergence.
            await operator_repository.enforce_stopped_on_startup(instance_id)
            await registry.heartbeat()
            await registry.acknowledge(config)

            admission_gate = PostgresEntryAdmissionGate(pool, store, registry, cache=cache)
            invalidation_consumer = ConfigInvalidationConsumer(
                repository, cache, registry, store
            )
            outbox_publisher = PostgresOutboxPublisher(
                pool, redis, worker_id=process_id
            )

            telegram_bot = None
            telegram = config.snapshot.get("telegram")
            if isinstance(telegram, dict) and telegram.get("enabled") is True:
                policy = TelegramPolicy.from_snapshot(config.snapshot)
                secret_resolver = lambda _reference: os.environ.get(
                    "NAFAZ_TELEGRAM_BOT_TOKEN", ""
                )
                try:
                    service = TelegramControlService(
                        operator_repository,
                        repository,
                        readiness_provider=readiness_provider,
                    )
                    telegram_bot = TelegramControlBot(
                        policy, service, secret_resolver=secret_resolver
                    )
                except OperatorControlError as exc:
                    # Keep the durable entry gate active, but do not pretend
                    # Telegram is available without a resolvable bot secret.
                    logger.error(
                        "V2 Telegram bot disabled because its secret is unresolved error_type=%s",
                        type(exc).__name__,
                    )

            return cls(
                pool=pool,
                redis=redis,
                repository=repository,
                registry=registry,
                operator_repository=operator_repository,
                admission_gate=admission_gate,
                invalidation_consumer=invalidation_consumer,
                outbox_publisher=outbox_publisher,
                telegram_bot=telegram_bot,
                instance_id=instance_id,
                process_id=process_id,
            )
        except Exception:
            if redis is not None:
                close = getattr(redis, "aclose", None) or getattr(redis, "close", None)
                if close:
                    result = close()
                    if hasattr(result, "__await__"):
                        await result
            await pool.close()
            raise

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._tasks.append(
            asyncio.create_task(
                self._invalidation_consumer.run(
                    self.redis, stop_event=self._stop_event
                ),
                name="v2-config-invalidation",
            )
        )
        self._tasks.append(
            asyncio.create_task(self._publish_outbox(), name="v2-outbox-publisher")
        )
        if self._telegram_bot is not None:
            self._tasks.append(
                asyncio.create_task(self._telegram_bot.run(), name="v2-telegram-control")
            )
        logger.info(
            "V2 control plane started instance=%s telegram=%s trading_state=STOPPED",
            self.instance_id,
            self._telegram_bot is not None,
        )

    async def _publish_outbox(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._outbox_publisher.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "V2 outbox publisher failed error_type=%s", type(exc).__name__
                )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop_event.set()
        if self._telegram_bot is not None:
            self._telegram_bot.stop()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await self.registry.stop()
        except Exception as exc:
            logger.warning(
                "V2 executor lease stop acknowledgement failed error_type=%s",
                type(exc).__name__,
            )
        close = getattr(self.redis, "aclose", None) or getattr(self.redis, "close", None)
        if close:
            result = close()
            if hasattr(result, "__await__"):
                await result
        await self.pool.close()
        self._tasks.clear()
        self._started = False
