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

        Returns the claim row (dict) when this worker won the generation, or
        None when another worker already holds it (or the DB is unavailable —
        callers must then fail closed for LIVE submissions).
        """
        if residual_qty <= 0:
            return None
        pool = await self._conn()
        if pool is None:
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
            async with pool.acquire() as conn:
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
                    on conflict (claim_key) do nothing
                    returning claim_key, hedge_client_order_id, version
                    """,
                    key,
                    str(parent_spot_exchange or "").lower(),
                    str(parent_spot_order_id or ""),
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
                    # Another worker holds this generation.
                    return None
                return {
                    "claim_key": row["claim_key"],
                    "hedge_client_order_id": row["hedge_client_order_id"],
                    "version": int(row["version"]),
                }
        except Exception:
            logger.exception(
                "[Hedge Claim] claim insert failed for %s; failing closed", key
            )
            return None

    async def mark_submitted(self, claim_key: str) -> bool:
        return await self._advance(claim_key, "SUBMITTED")

    async def mark_confirmed(self, claim_key: str, confirmed_qty: float) -> bool:
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
                    where claim_key = $1 and status in ('CLAIMED', 'SUBMITTED')
                    """,
                    claim_key, float(confirmed_qty),
                )
                return result.endswith("1")
        except Exception:
            logger.exception(
                "[Hedge Claim] confirm update failed for %s", claim_key
            )
            return False

    async def _advance(self, claim_key: str, status: str) -> bool:
        pool = await self._conn()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    """
                    update public.executor_hedge_intent_claims
                    set status = $2, version = version + 1, updated_at = now()
                    where claim_key = $1 and status = 'CLAIMED'
                    """,
                    claim_key, status,
                )
                return result.endswith("1")
        except Exception:
            logger.exception(
                "[Hedge Claim] advance to %s failed for %s", status, claim_key
            )
            return False
