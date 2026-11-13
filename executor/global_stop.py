"""PostgreSQL-backed global STOP state and cross-instance order-submit fencing.

The transaction-scoped advisory lock is deliberately held across the venue
create-order request. This makes STOP and a new-order request linearizable:
whichever obtains the lock first completes its critical section first. The
exchange side effect is still not atomic with PostgreSQL; callers must use a
stable client order id and reconcile ambiguous acknowledgements.
"""
from __future__ import annotations

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
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "select is_stopped, epoch, reason from public.executor_global_control where singleton_id = 1"
                )
                if row is None:
                    raise GlobalStopError("global control row is missing; apply PostgreSQL fencing migration")
        except Exception:
            await self.close()
            raise
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

    async def prepare_submit_attempt(
        self, *, client_order_id: str, exchange: str, market_type: str,
        symbol: str, side: str, order_type: str, quantity: float,
        price: float, pair_key: str, recovery_context: Optional[dict] = None,
        amount_spec: Optional[dict] = None,
    ) -> dict:
        """Commit a durable PREPARED intent before any venue HTTP request."""
        self._require_pool()
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required")
        values = (
            client_order_id, str(exchange or "").lower()[:100],
            str(market_type or "").lower()[:30], str(symbol or "")[:160],
            str(side or "").lower()[:16], str(order_type or "").lower()[:24],
            float(quantity), float(price), str(pair_key or "")[:200],
            json.dumps(recovery_context or {}, separators=(",", ":"), default=str),
            json.dumps(amount_spec or {}, separators=(",", ":"), sort_keys=True, default=str),
        )
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "insert into public.executor_submit_attempts "
                "(client_order_id, exchange, market_type, symbol, side, order_type, "
                "quantity, price, pair_key, recovery_context, amount_spec, status) "
                "values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,'PREPARED') "
                "on conflict (client_order_id) do nothing "
                "returning client_order_id, status, created_at",
                *values,
            )
            if row is None:
                existing = await conn.fetchrow(
                    "select client_order_id, status, created_at, exchange, market_type, symbol, "
                    "side, order_type, quantity, price, pair_key, amount_spec from public.executor_submit_attempts "
                    "where client_order_id = $1",
                    client_order_id,
                )
                if existing is None or existing["status"] != "PREPARED":
                    raise GlobalStopError(
                        f"client order id already has a non-reusable submit attempt: {client_order_id}"
                    )
                expected = {
                    "exchange": values[1], "market_type": values[2], "symbol": values[3],
                    "side": values[4], "order_type": values[5], "quantity": values[6],
                    "price": values[7], "pair_key": values[8],
                }
                stored_amount_spec = existing.get("amount_spec")
                if isinstance(stored_amount_spec, str):
                    try:
                        stored_amount_spec = json.loads(stored_amount_spec)
                    except (TypeError, ValueError):
                        stored_amount_spec = None
                expected_amount_spec = json.loads(values[10])
                if (
                    any(existing[key] != value for key, value in expected.items())
                    or stored_amount_spec != expected_amount_spec
                ):
                    raise GlobalStopError(
                        f"client order id collision with a different order intent: {client_order_id}"
                    )
                row = existing
        return dict(row)

    @asynccontextmanager
    async def order_submit_fence(self, *, client_order_id: str) -> AsyncIterator[Any]:
        """Hold a session advisory lock while a committed SUBMITTING row protects the HTTP side effect.

        Session locks are released by PostgreSQL if the connection/process dies.
        The separately committed SUBMITTING ledger row survives that disconnect
        and blocks every later entry until venue reconciliation resolves it.
        """
        self._require_pool()
        if not client_order_id or len(client_order_id) > 128:
            raise GlobalStopError("valid stable client_order_id is required before submit")
        async with self.pool.acquire() as conn:
            locked = False
            try:
                await conn.execute("select pg_advisory_lock($1)", FENCE_LOCK_KEY)
                locked = True
                async with conn.transaction():
                    control = await conn.fetchrow(
                        "select is_stopped, epoch, reason from public.executor_global_control "
                        "where singleton_id = 1 for update"
                    )
                    if control is None:
                        raise GlobalStopError("global control row is missing")
                    if control["is_stopped"]:
                        raise GlobalStopError(
                            f"global STOP active (epoch={control['epoch']} reason={control['reason'] or 'unspecified'})"
                        )
                    unresolved = await conn.fetchrow(
                        "select client_order_id, exchange, symbol, status from public.executor_submit_attempts "
                        "where status in ('SUBMITTING','UNKNOWN') and client_order_id <> $1 "
                        "order by created_at limit 1",
                        client_order_id,
                    )
                    if unresolved is not None:
                        raise GlobalStopError(
                            "unresolved in-flight submit blocks new entries: "
                            f"{unresolved['exchange']}:{unresolved['symbol']} "
                            f"client_order_id={unresolved['client_order_id']} status={unresolved['status']}"
                        )
                    attempt = await conn.fetchrow(
                        "update public.executor_submit_attempts "
                        "set status='SUBMITTING', submit_started_at=now(), "
                        "submit_count=submit_count+1, fence_epoch=$2, updated_at=now() "
                        "where client_order_id=$1 and status='PREPARED' "
                        "returning client_order_id",
                        client_order_id, int(control["epoch"]),
                    )
                    if attempt is None:
                        raise GlobalStopError(
                            f"durable PREPARED submit attempt missing or already used: {client_order_id}"
                        )
                # The SUBMITTING state is committed while the session lock is
                # still held. A connection loss now releases the lock but not
                # this durable row.
                yield conn
            finally:
                if locked:
                    try:
                        await conn.execute("select pg_advisory_unlock($1)", FENCE_LOCK_KEY)
                    except Exception:
                        # A broken connection has already caused PostgreSQL to
                        # release its session-level lock; the ledger is the guard.
                        logger.warning(
                            "could not explicitly release PostgreSQL submit session lock; "
                            "connection loss must be treated as ambiguous"
                        )

    async def mark_submit_acknowledged(
        self, client_order_id: str, order: dict
    ) -> None:
        """Persist a positive venue acknowledgement; failure leaves SUBMITTING durable."""
        self._require_pool()
        order_id = str((order or {}).get("id") or "")
        if not order_id:
            raise GlobalStopError("cannot acknowledge submit without a venue order id")
        await self._update_submit_attempt(
            client_order_id, "ACKNOWLEDGED", exchange_order_id=order_id,
            venue_response=order, error=None,
        )

    async def mark_submit_rejected(self, client_order_id: str, error: str) -> None:
        """Resolve only a definitive venue rejection, never a transport timeout."""
        await self._update_submit_attempt(
            client_order_id, "REJECTED", exchange_order_id=None,
            venue_response=None, error=str(error)[:2000],
        )

    async def mark_submit_aborted(self, client_order_id: str, reason: str) -> None:
        """Retire an intent proven not to have crossed the venue HTTP boundary."""
        await self._update_submit_attempt(
            client_order_id, "ABORTED", exchange_order_id=None,
            venue_response=None, error=str(reason)[:2000],
        )

    async def mark_submit_unknown(self, client_order_id: str, error: str) -> None:
        """Annotate an ambiguous attempt; if DB is down SUBMITTING remains blocking."""
        await self._update_submit_attempt(
            client_order_id, "UNKNOWN", exchange_order_id=None,
            venue_response=None, error=str(error)[:2000],
        )

    async def _update_submit_attempt(
        self, client_order_id: str, status: str, *, exchange_order_id: Optional[str],
        venue_response: Optional[dict], error: Optional[str],
    ) -> None:
        self._require_pool()
        if status not in {"ACKNOWLEDGED", "REJECTED", "UNKNOWN", "ABORTED"}:
            raise ValueError("invalid submit-attempt transition")
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "update public.executor_submit_attempts set status=$2, "
                "exchange_order_id=coalesce($3, exchange_order_id), "
                "venue_response=case when $4::jsonb is null then venue_response else $4::jsonb end, "
                "last_error=$5, resolved_at=case when $2 in ('ACKNOWLEDGED','REJECTED','ABORTED') then now() else null end, "
                "updated_at=now() where client_order_id=$1 and status in ('PREPARED','SUBMITTING','UNKNOWN')",
                client_order_id, status, exchange_order_id,
                json.dumps(venue_response, separators=(",", ":"), default=str) if venue_response is not None else None,
                error,
            )
            if result.endswith(" 0"):
                raise GlobalStopError(
                    f"submit attempt transition rejected for {client_order_id} (current state may have changed)"
                )

    async def list_unresolved_submit_attempts(self, *, limit: int = 100) -> list:
        """Return attempts whose venue outcome is not yet proven."""
        self._require_pool()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "select client_order_id, exchange, market_type, symbol, side, order_type, "
                "quantity, price, pair_key, amount_spec, recovery_context, status, "
                "submit_started_at, last_error from public.executor_submit_attempts "
                "where status in ('SUBMITTING','UNKNOWN') order by submit_started_at limit $1",
                max(1, min(int(limit), 1000)),
            )
        return [dict(row) for row in rows]

    async def reconcile_submit_attempt_found(
        self, client_order_id: str, order: dict
    ) -> None:
        """Adopt an order found by exact venue client-order-id lookup."""
        await self.mark_submit_acknowledged(client_order_id, order)

    async def reconcile_submit_attempt_not_found(
        self, client_order_id: str, *, evidence: str
    ) -> None:
        """Record a negative lookup but deliberately keep the attempt UNKNOWN.

        Absence from an eventually consistent lookup endpoint is not proof that
        a create-order request was rejected. Only an authoritative venue
        rejection or an operator-approved resolution may release the global gate.
        """
        self._require_pool()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "update public.executor_submit_attempts set last_lookup_at=now(), "
                "last_lookup_result='NOT_FOUND', last_error=$2, updated_at=now() "
                "where client_order_id=$1 and status in ('SUBMITTING','UNKNOWN')",
                client_order_id, str(evidence)[:2000],
            )


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
