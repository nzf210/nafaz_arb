"""Durable cross-instance hedge-intent claims.

A claim row in ``public.executor_hedge_intent_claims`` is the only
authorization to submit a hedge for a given (parent spot order, cumulative
spot fill) generation. The claim is acquired with an atomic
``INSERT .. ON CONFLICT DO NOTHING RETURNING`` — a database-level CAS — so
two executor instances sharing the same PostgreSQL database can never both
hold the same generation. The claim identity also derives the stable hedge
client order id, so retries and restarts reuse the same venue order id.

This module does NOT replace the Global STOP submission fence: the fence
still guards the venue create-order boundary. The claim guards the
compute-residual-then-intent step that the fence does not cover.
"""

import time
from typing import Any, Optional

from executor.entry_common import logger


class HedgeClaimUnavailableError(RuntimeError):
    """Raised when PostgreSQL cannot prove ownership of a hedge generation."""

    def __init__(self, message: str, *, claim_key: str = ""):
        super().__init__(message)
        self.claim_key = str(claim_key or "")


def hedge_claim_key(
    parent_spot_exchange: str, parent_spot_order_id: str, fill_version: int
) -> str:
    """Stable claim identity for one hedge generation."""
    return (
        f"{str(parent_spot_exchange or '').lower()}:"
        f"{str(parent_order_id_or_empty(parent_spot_order_id))}:{int(fill_version)}"
    )


def parent_order_id_or_empty(order_id: Any) -> str:
    return str(order_id or "")


def hedge_client_order_id_for_claim(claim_key: str) -> str:
    """Deterministic client order id derived from the claim identity.

    Same claim -> same client id across retries, restarts and instances, so a
    duplicate submission is impossible at the venue id level as well.
    """
    import hashlib

    digest = hashlib.sha256(claim_key.encode("utf-8")).hexdigest()[:24]
    return f"hdg{digest}"


class HedgeIntentClaimService:
    """PostgreSQL-backed atomic hedge generation claims."""

    def __init__(self, global_stop: Any = None):
        # Reuses the GlobalStopService pool: one connection authority, one
        # failure domain. global_stop may be None (mock mode).
        self.global_stop = global_stop

    @property
    def enabled(self) -> bool:
        return (
            self.global_stop is not None
            and getattr(self.global_stop, "enabled", False)
        )

    async def _conn(self):
        stop = self.global_stop
        if stop is None or not getattr(stop, "enabled", False):
            return None
        if not await stop.ensure_pool():
            return None
        return stop.pool

    async def claim_generation(
        self, *, parent_spot_exchange: str, parent_spot_order_id: str,
        parent_spot_client_order_id: str, cumulative_spot_filled: float,
        target_hedge_qty: float, residual_qty: float, hedge_ratio: float,
        futures_exchange: str, futures_symbol: str, claimed_by: str,
    ) -> Optional[dict]:
        """Atomically claim one hedge generation.

        Returns None only when another unresolved worker already owns this
        parent Spot order generation. PostgreSQL unavailability or an
        indeterminate claim transaction raises HedgeClaimUnavailableError
        carrying the deterministic claim key for later exact claim/order recovery.
        """
        if residual_qty <= 0:
            return None
        # Fill version: monotonic micro-units of the cumulative spot fill so
        # every new cumulative level is a distinct, replay-safe generation.
        fill_version = int(round(float(cumulative_spot_filled) * 1e9))
        key = hedge_claim_key(
            parent_spot_exchange, parent_order_id_or_empty(parent_spot_order_id),
            fill_version,
        )
        coid = hedge_client_order_id_for_claim(key)
        try:
            pool = await self._conn()
            if pool is None:
                raise HedgeClaimUnavailableError(
                    "PostgreSQL hedge-claim authority is unavailable",
                    claim_key=key,
                )
            async with pool.acquire() as conn:
                parent_exchange = str(parent_spot_exchange or "").lower()
                parent_order_id = str(parent_spot_order_id or "")
                # Serialize all generations for one parent Spot order. A later
                # cumulative fill must not create a second hedge while an older
                # generation is still CLAIMED/SUBMITTED and its venue outcome
                # has not been confirmed. The lock and lookup share one DB
                # transaction, so independent executor processes observe one
                # linearizable claim lifecycle.
                async with conn.transaction():
                    await conn.execute(
                        "select pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"{parent_exchange}:{parent_order_id}",
                    )
                    unresolved = await conn.fetchrow(
                        """
                        select claim_key, status
                        from public.executor_hedge_intent_claims
                        where parent_spot_exchange = $1
                          and parent_spot_order_id = $2
                          and status in ('CLAIMED', 'SUBMITTED')
                        order by created_at desc
                        limit 1
                        """,
                        parent_exchange, parent_order_id,
                    )
                    if unresolved is not None:
                        # A peer owns this generation or an earlier fill
                        # generation. Do not submit a duplicate and do not
                        # trigger rollback while that peer may be hedging.
                        return None
                    row = await conn.fetchrow(
                        """
                        insert into public.executor_hedge_intent_claims (
                            claim_key, parent_spot_exchange, parent_spot_order_id,
                            parent_spot_client_order_id, cumulative_spot_filled,
                            target_hedge_qty, residual_qty, hedge_ratio,
                            futures_exchange, futures_symbol, hedge_client_order_id,
                            status, claimed_by
                        ) values (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                            'CLAIMED', $12
                        )
                        on conflict (claim_key) do update
                        set status = 'CLAIMED',
                            target_hedge_qty = excluded.target_hedge_qty,
                            residual_qty = excluded.residual_qty,
                            hedge_ratio = excluded.hedge_ratio,
                            claimed_by = excluded.claimed_by,
                            version = executor_hedge_intent_claims.version + 1,
                            updated_at = now()
                        where executor_hedge_intent_claims.status = 'ABANDONED'
                        returning claim_key, hedge_client_order_id, version
                        """,
                        key,
                        parent_exchange,
                        parent_order_id,
                        str(parent_spot_client_order_id or "") or None,
                        float(cumulative_spot_filled),
                        float(target_hedge_qty),
                        float(residual_qty),
                        float(hedge_ratio),
                        str(futures_exchange or ""),
                        str(futures_symbol or ""),
                        coid,
                        str(claimed_by or ""),
                    )
                    if row is None:
                        # This exact generation was already claimed.
                        return None
                    return {
                        "claim_key": row["claim_key"],
                        "hedge_client_order_id": row["hedge_client_order_id"],
                        "version": int(row["version"]),
                    }
        except HedgeClaimUnavailableError:
            raise
        except Exception as exc:
            logger.exception(
                "[Hedge Claim] claim transaction failed for %s; no futures submit is permitted",
                key,
            )
            raise HedgeClaimUnavailableError(
                f"PostgreSQL hedge-claim transaction is unavailable for {key}: "
                f"{type(exc).__name__}",
                claim_key=key,
            ) from exc

    async def mark_submitted(
        self, claim_key: str, hedge_client_order_id: str
    ) -> bool:
        return await self._advance(
            claim_key, "SUBMITTED", hedge_client_order_id
        )

    async def mark_confirmed(
        self, claim_key: str, confirmed_qty: float, hedge_client_order_id: str
    ) -> bool:
        pool = await self._conn()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    update public.executor_hedge_intent_claims
                    set status = 'CONFIRMED',
                        confirmed_qty = greatest(confirmed_qty, $2),
                        version = version + 1,
                        updated_at = now()
                    where claim_key = $1
                      and hedge_client_order_id = $3
                      and status in ('CLAIMED', 'SUBMITTED')
                    """,
                    claim_key, float(confirmed_qty), hedge_client_order_id,
                )
                if result.endswith("1"):
                    return True
                # Recovery is replayable: if a previous pass already confirmed
                # this exact (or larger) fill, treat the persisted state as success.
                row = await conn.fetchrow(
                    """
                    select status, confirmed_qty
                    from public.executor_hedge_intent_claims
                    where claim_key = $1 and hedge_client_order_id = $2
                    """,
                    claim_key, hedge_client_order_id,
                )
                return bool(
                    row
                    and str(row["status"]) == "CONFIRMED"
                    and float(row["confirmed_qty"] or 0.0) >= float(confirmed_qty)
                )
        except Exception:
            logger.exception(
                "[Hedge Claim] confirm update failed for %s", claim_key
            )
            return False

    async def mark_abandoned(
        self, claim_key: str, hedge_client_order_id: str, reason: str
    ) -> bool:
        """Release a claim only when no venue submission was started."""
        pool = await self._conn()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    update public.executor_hedge_intent_claims
                    set status = 'ABANDONED',
                        version = version + 1,
                        updated_at = now()
                    where claim_key = $1
                      and hedge_client_order_id = $2
                      and status = 'CLAIMED'
                    """,
                    claim_key, hedge_client_order_id,
                )
                if not result.endswith("1"):
                    logger.error(
                        "[Hedge Claim] could not abandon unsubmitted claim %s (%s)",
                        claim_key, str(reason)[:300],
                    )
                    return False
                return True
        except Exception:
            logger.exception(
                "[Hedge Claim] abandon update failed for %s", claim_key
            )
            return False

    async def mark_zero_fill_terminal(
        self, claim_key: str, hedge_client_order_id: str, reason: str
    ) -> bool:
        """Release a submitted claim only after the venue confirms terminal zero fill."""
        pool = await self._conn()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        select status, version, hedge_client_order_id
                        from public.executor_hedge_intent_claims
                        where claim_key = $1
                        for update
                        """,
                        claim_key,
                    )
                    if row is None:
                        return False
                    status = str(row["status"])
                    current_client_order_id = str(row["hedge_client_order_id"] or "")
                    if current_client_order_id != str(hedge_client_order_id or ""):
                        # The old venue attempt was already finalized and its
                        # claim ID rotated. A replay must not abandon the newer
                        # attempt that now owns this cumulative Spot generation.
                        return True
                    if status == "ABANDONED":
                        return True
                    if status not in ("CLAIMED", "SUBMITTED"):
                        return False
                    # A terminal zero-fill order has consumed its venue client
                    # order ID. Rotate the ID before allowing this fill
                    # generation to be claimed again; the old ACKNOWLEDGED
                    # submit-attempt row must never be reused for a new order.
                    next_version = int(row["version"] or 0) + 1
                    retry_coid = hedge_client_order_id_for_claim(
                        f"{claim_key}:retry:{next_version}"
                    )
                    result = await conn.execute(
                        """
                        update public.executor_hedge_intent_claims
                        set status = 'ABANDONED',
                            hedge_client_order_id = $3,
                            version = version + 1,
                            updated_at = now()
                        where claim_key = $1
                          and hedge_client_order_id = $2
                          and status in ('CLAIMED', 'SUBMITTED')
                        """,
                        claim_key, hedge_client_order_id, retry_coid,
                    )
                    return result.endswith("1")
        except Exception:
            logger.exception(
                "[Hedge Claim] terminal zero-fill transition failed for %s", claim_key
            )
            return False

    async def _advance(
        self, claim_key: str, status: str, hedge_client_order_id: str
    ) -> bool:
        pool = await self._conn()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    update public.executor_hedge_intent_claims
                    set status = $2, version = version + 1, updated_at = now()
                    where claim_key = $1
                      and hedge_client_order_id = $3
                      and status = 'CLAIMED'
                    """,
                    claim_key, status, hedge_client_order_id,
                )
                return result.endswith("1")
        except Exception:
            logger.exception(
                "[Hedge Claim] advance to %s failed for %s", status, claim_key
            )
            return False
