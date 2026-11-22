from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Optional

from executor.global_stop_common import (
    DERISK_LOCK_KEY, FENCE_LOCK_KEY, STOP_CHANNEL, GlobalStopError,
    _json_mapping, logger,
)
from executor.global_stop_submit import GlobalStopSubmitMixin
from executor.global_stop_attempts import GlobalStopAttemptMixin
from executor.ownership import OwnershipLeaseService
from executor.risk_control import RiskControlPlane

class GlobalStopService(GlobalStopSubmitMixin, GlobalStopAttemptMixin):
    def __init__(self, dsn: str, redis: Any = None, *, min_size: int = 1, max_size: int = 5):
        self.dsn = str(dsn or "").strip()
        self.redis = redis
        self.min_size = min_size
        self.max_size = max_size
        self.pool = None
        # Set only when the driver is missing or the DSN is unusable: retrying
        # such a configuration on every order would add latency for nothing.
        self._pool_unusable = False
        self.ownership = OwnershipLeaseService(owner_id=uuid.uuid4().hex)
        self.risk_control_plane = RiskControlPlane()

    @property
    def enabled(self) -> bool:
        return self.pool is not None

    @staticmethod
    def _is_connection_error(exc: BaseException) -> bool:
        """True for errors that mean the pool itself is dead, not the statement."""
        name = type(exc).__name__
        return name in (
            "ConnectionDoesNotExistError", "ConnectionFailureError",
            "InterfaceError", "TooManyConnectionsError", "PostgresConnectionError",
            "OSError", "ConnectionResetError", "TimeoutError",
        )

    async def ensure_pool(self) -> bool:
        """Return a usable pool, transparently reconnecting after a drop.

        PostgreSQL is the canonical fencing authority, so an instance that has
        lost its connection must prove the authority is reachable again rather
        than silently keep trading. A failed reconnect leaves the pool empty and
        the caller fails closed.
        """
        if self.pool is not None:
            return True
        if self._pool_unusable or not self.dsn:
            return False
        try:
            import asyncpg
        except ImportError:
            self._pool_unusable = True
            logger.critical("asyncpg missing; PostgreSQL STOP fencing is unavailable")
            return False
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn, min_size=self.min_size, max_size=self.max_size,
                command_timeout=10,
            )
            logger.warning("PostgreSQL STOP/fencing pool re-established")
            return True
        except Exception:
            logger.exception("PostgreSQL STOP/fencing pool reconnect failed")
            self.pool = None
            return False

    def _demote_broken_pool(self, exc: BaseException) -> None:
        if not self._is_connection_error(exc):
            return
        pool = self.pool
        self.pool = None
        if pool is not None:
            try:
                asyncio.create_task(pool.close())
            except Exception:
                pass
        logger.critical(
            "PostgreSQL STOP/fencing connection lost (%s); the next order will re-prove "
            "the authority before submitting", type(exc).__name__,
        )

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
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "select is_stopped, epoch, reason from public.executor_global_control where singleton_id = 1"
                )
                if row is None:
                    raise GlobalStopError("global control row is missing; apply PostgreSQL fencing migration")
                schema = await conn.fetchrow(
                    """
                    SELECT
                        to_regclass('public.executor_ownership_leases') AS ownership_table,
                        to_regclass('public.executor_ownership_events') AS ownership_events,
                        to_regclass('public.executor_risk_controls') AS risk_controls,
                        to_regclass('public.executor_risk_control_events') AS risk_events
                    """
                )
                if not schema or any(
                    schema[name] is None
                    for name in ("ownership_table", "ownership_events", "risk_controls", "risk_events")
                ):
                    raise GlobalStopError(
                        "Executor V3 ownership/risk schema is missing; apply the V3 ownership migration"
                    )
                owner_columns = await conn.fetchval(
                    """
                    SELECT count(*) FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='executor_submit_attempts'
                      AND column_name IN ('account_scope_id','owner_scope_key','owner_generation')
                    """
                )
                if int(owner_columns or 0) != 3:
                    raise GlobalStopError(
                        "Executor V3 submit ownership columns are missing; apply the V3 ownership migration"
                    )
        except Exception:
            await self.close()
            raise
        logger.info("PostgreSQL global STOP/fencing service connected (epoch=%s stopped=%s)", row["epoch"], row["is_stopped"])

    async def close(self) -> None:
        if self.pool is not None:
            pool = self.pool
            try:
                async with pool.acquire() as conn:
                    released = await self.ownership.release_all(conn)
                    if released:
                        logger.info("Released %d scoped ownership leases on shutdown", released)
            except Exception:
                logger.exception("Could not release scoped ownership leases during shutdown")
            await pool.close()
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
                row = await self.request_stop_in_transaction(
                    conn, reason, source=source, metadata=metadata
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


    async def request_stop_in_transaction(
        self, conn: Any, reason: str, *, source: str, metadata: Optional[dict] = None
    ) -> dict:
        """Commit STOP while the caller holds the shared session advisory lock."""
        reason = str(reason or "").strip().upper()
        source = str(source or "").strip()[:100]
        payload = json.dumps(metadata or {}, separators=(",", ":"), default=str)
        async with conn.transaction():
            row = await conn.fetchrow(
                "update public.executor_global_control "
                "set is_stopped = true, epoch = epoch + case when is_stopped then 0 else 1 end, "
                "reason = case when is_stopped then reason else $1 end, "
                "source = case when is_stopped then source else $2 end, "
                "metadata = case when is_stopped then metadata else $3::jsonb end, updated_at = now() "
                "where singleton_id = 1 "
                "returning is_stopped, epoch, reason, source, metadata, updated_at",
                reason, source, payload,
            )
        if row is None:
            raise GlobalStopError("global control row is missing; STOP was not persisted")
        return dict(row)

    async def notify_state(self, state: dict) -> None:
        """Best-effort Redis notification after PostgreSQL transaction commits."""
        if self.redis is None:
            return
        try:
            await self.redis.publish(STOP_CHANNEL, json.dumps({
                "is_stopped": bool(state.get("is_stopped")),
                "epoch": int(state.get("epoch", 0)),
                "reason": state.get("reason"),
                "source": state.get("source"),
            }))
        except Exception:
            logger.exception("Global STOP committed to PostgreSQL but Redis notification failed")

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
