"""PostgreSQL-backed global STOP state and cross-instance order-submit fencing.

The transaction-scoped advisory lock is deliberately held across the venue
create-order request. This makes STOP and a new-order request linearizable:
whichever obtains the lock first completes its critical section first. The
exchange side effect is still not atomic with PostgreSQL; callers must use a
stable client order id and reconcile ambiguous acknowledgements.
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

logger = logging.getLogger("executor.global_stop")

# Stable, application-wide PostgreSQL advisory-lock key. Do not change without
# coordinating every executor instance using this schema.
FENCE_LOCK_KEY = 731_904_221
STOP_CHANNEL = "arb:global_stop"


class GlobalStopError(RuntimeError):
    """Raised when global admission cannot be proven safe."""


class GlobalStopService:
    def __init__(self, dsn: str, redis: Any = None, *, min_size: int = 1, max_size: int = 5):
        self.dsn = str(dsn or "").strip()
        self.redis = redis
        self.min_size = min_size
        self.max_size = max_size
        self.pool = None

    @property
    def enabled(self) -> bool:
        return self.pool is not None

    async def start(self) -> None:
        if self.pool is not None:
            return
        if not self.dsn:
            raise GlobalStopError("POSTGRES_DSN is required for PostgreSQL global STOP fencing")
        try:
            import asyncpg
        except ImportError as exc:
            raise GlobalStopError("asyncpg is required for PostgreSQL fencing") from exc
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn, min_size=self.min_size, max_size=self.max_size,
            command_timeout=10,
        )
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select is_stopped, epoch, reason from public.executor_global_control where singleton_id = 1"
            )
            if row is None:
                raise GlobalStopError("global control row is missing; apply PostgreSQL fencing migration")
        logger.info("PostgreSQL global STOP/fencing service connected (epoch=%s stopped=%s)", row["epoch"], row["is_stopped"])

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    def _require_pool(self):
        if self.pool is None:
            raise GlobalStopError("PostgreSQL global STOP service is not initialized")

    async def read_state(self) -> dict:
        self._require_pool()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select is_stopped, epoch, reason, source, metadata, updated_at "
                "from public.executor_global_control where singleton_id = 1"
            )
        if row is None:
            raise GlobalStopError("global control row is missing")
        return dict(row)

    @asynccontextmanager
    async def order_submit_fence(self, *, client_order_id: str) -> AsyncIterator[int]:
        """Serialize one create-order HTTP request against every global STOP.

        The yielded epoch is the generation observed while holding the lock.
        Keep the scope limited to a single venue submit; do not do recovery or
        unrelated work while holding this transaction.
        """
        self._require_pool()
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required before submit")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await conn.fetchrow(
                    "select is_stopped, epoch, reason from public.executor_global_control "
                    "where singleton_id = 1 for update"
                )
                if row is None:
                    raise GlobalStopError("global control row is missing")
                if row["is_stopped"]:
                    raise GlobalStopError(
                        f"global STOP active (epoch={row['epoch']} reason={row['reason'] or 'unspecified'})"
                    )
                yield int(row["epoch"])

    async def request_stop(
        self, reason: str, *, source: str, metadata: Optional[dict] = None
    ) -> dict:
        """Persist STOP under the same lock used by all create-order submits."""
        self._require_pool()
        reason = str(reason or "").strip().upper()
        source = str(source or "").strip()[:100]
        if not reason or not source:
            raise ValueError("reason and source are required")
        payload = json.dumps(metadata or {}, separators=(",", ":"), default=str)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await conn.fetchrow(
                    "update public.executor_global_control "
                    "set is_stopped = true, epoch = epoch + 1, reason = $1, source = $2, "
                    "metadata = $3::jsonb, updated_at = now() "
                    "where singleton_id = 1 "
                    "returning is_stopped, epoch, reason, source, metadata, updated_at",
                    reason, source, payload,
                )
                if row is None:
                    raise GlobalStopError("global control row is missing; STOP was not persisted")
        result = dict(row)
        # Redis is an acceleration/notification channel only. PostgreSQL is canonical.
        if self.redis is not None:
            try:
                await self.redis.publish(STOP_CHANNEL, json.dumps({
                    "is_stopped": True, "epoch": int(result["epoch"]),
                    "reason": reason, "source": source,
                }))
            except Exception:
                logger.exception("STOP committed to PostgreSQL but Redis notification failed")
        return result

    async def acknowledge_resume(self, *, operator: str, expected_epoch: int) -> dict:
        """Clear STOP only by explicit operator acknowledgement of current epoch."""
        self._require_pool()
        if not operator:
            raise ValueError("operator acknowledgement is required")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await conn.fetchrow(
                    "update public.executor_global_control "
                    "set is_stopped = false, epoch = epoch + 1, reason = null, "
                    "source = $1, metadata = jsonb_build_object('resumed_from_epoch', $2), updated_at = now() "
                    "where singleton_id = 1 and is_stopped = true and epoch = $2 "
                    "returning is_stopped, epoch, reason, source, metadata, updated_at",
                    operator[:100], expected_epoch,
                )
                if row is None:
                    raise GlobalStopError("STOP state changed or is not active; refusing stale resume")
        result = dict(row)
        if self.redis is not None:
            try:
                await self.redis.publish(STOP_CHANNEL, json.dumps({
                    "is_stopped": False, "epoch": int(result["epoch"]),
                    "source": operator,
                }))
            except Exception:
                logger.exception("Resume committed to PostgreSQL but Redis notification failed")
        return result
