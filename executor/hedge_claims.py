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
        """Claim the current residual using durable parent-level fill totals.

        Caller snapshots are advisory only. Under the per-parent transaction
        lock, the database is authoritative for maximum Spot cumulative fill
        and the sum of confirmed Futures base-equivalent fills. This prevents
        a worker with stale local hedge state from submitting a duplicate.
        """
        import math

        try:
            requested_spot_filled = float(cumulative_spot_filled)
            ratio = float(hedge_ratio)
            requested_target = float(target_hedge_qty)
            requested_residual = float(residual_qty)
        except (TypeError, ValueError):
            return None
        if (
            not all(math.isfinite(value) for value in (
                requested_spot_filled, ratio, requested_target, requested_residual
            ))
            or requested_spot_filled <= 0
            or ratio <= 0
            or requested_target < 0
            or requested_residual < 0
        ):
            return None

        pool = await self._conn()
        if pool is None:
            return None

        parent_exchange = str(parent_spot_exchange or "").lower()
        parent_order_id = str(parent_spot_order_id or "")
        if not parent_exchange or not parent_order_id:
            return None

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Serialize all generations for one Spot parent across
                    # processes. The lock covers aggregate reads and claim insert.
                    await conn.execute(
                        "select pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"{parent_exchange}:{parent_order_id}",
                    )
                    unresolved = await conn.fetchrow(
                        """
                        select claim_key, status, hedge_client_order_id, version,
                               cumulative_spot_filled, target_hedge_qty, hedge_ratio,
                               futures_exchange, futures_symbol
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
                        # CLAIMED means another worker is still preparing its
                        # first attempt. SUBMITTED may be retried only after all
                        # prior attempts are terminal and their trade history
                        # has been reconciled into the append-only attempt ledger.
                        if str(unresolved["status"]) != "SUBMITTED":
                            return None
                        active_attempt = await conn.fetchrow(
                            """
                            select attempt_id
                            from public.executor_hedge_attempts
                            where claim_key=$1
                              and status in (
                                'PREPARED','SUBMITTING','UNKNOWN','ACKNOWLEDGED',
                                'OPEN','PARTIALLY_FILLED','RECONCILIATION_REQUIRED'
                              )
                            limit 1
                            """,
                            str(unresolved["claim_key"]),
                        )
                        if active_attempt is not None:
                            return None
                        attempt_totals = await conn.fetchrow(
                            """
                            select coalesce(sum(f.fill_qty_base),0) as confirmed,
                                   count(distinct a.attempt_id) as attempt_count
                            from public.executor_hedge_attempts a
                            left join public.executor_hedge_attempt_fills f
                              on f.attempt_id=a.attempt_id
                            where a.claim_key=$1
                            """,
                            str(unresolved["claim_key"]),
                        )
                        if (
                            attempt_totals is None
                            or int(attempt_totals["attempt_count"] or 0) <= 0
                        ):
                            # A legacy SUBMITTED claim with no attempt-ledger
                            # row cannot be assumed terminal or safe to replay.
                            return None
                        verified_confirmed = float(
                            attempt_totals["confirmed"] or 0.0
                        )
                        effective_spot_filled = max(
                            requested_spot_filled,
                            float(unresolved["cumulative_spot_filled"] or 0.0),
                        )
                        claim_target = max(
                            float(unresolved["target_hedge_qty"] or 0.0),
                            effective_spot_filled * ratio,
                        )
                        tolerance = max(1e-12, abs(claim_target) * 1e-8)
                        if verified_confirmed + tolerance >= claim_target:
                            await conn.execute(
                                """
                                update public.executor_hedge_intent_claims
                                set status='CONFIRMED', confirmed_qty=$2,
                                    cumulative_spot_filled=$3, target_hedge_qty=$4,
                                    residual_qty=0, hedge_ratio=$5,
                                    version=version+1, updated_at=now()
                                where claim_key=$1 and status='SUBMITTED'
                                """,
                                str(unresolved["claim_key"]), verified_confirmed,
                                effective_spot_filled, claim_target, ratio,
                            )
                            return None
                        retry_residual = max(0.0, claim_target - verified_confirmed)
                        if retry_residual <= 0:
                            return None
                        updated = await conn.fetchrow(
                            """
                            update public.executor_hedge_intent_claims
                            set cumulative_spot_filled=$2, target_hedge_qty=$3,
                                residual_qty=$4, hedge_ratio=$5,
                                version=version+1, updated_at=now()
                            where claim_key=$1 and status='SUBMITTED'
                            returning version
                            """,
                            str(unresolved["claim_key"]), effective_spot_filled,
                            claim_target, retry_residual, ratio,
                        )
                        if updated is None:
                            return None
                        return {
                            "claim_key": str(unresolved["claim_key"]),
                            "hedge_client_order_id": str(unresolved["hedge_client_order_id"]),
                            "version": int(updated["version"]),
                            "cumulative_spot_filled": effective_spot_filled,
                            "target_hedge_qty": claim_target,
                            "confirmed_qty": verified_confirmed,
                            "residual_qty": retry_residual,
                            "retry_after_terminal_attempt": True,
                        }

                    totals = await conn.fetchrow(
                        """
                        select
                            coalesce(max(cumulative_spot_filled), 0) as max_spot_filled,
                            coalesce(
                                sum(confirmed_qty) filter (where status = 'CONFIRMED'),
                                0
                            ) as confirmed_futures_qty
                        from public.executor_hedge_intent_claims
                        where parent_spot_exchange = $1
                          and parent_spot_order_id = $2
                        """,
                        parent_exchange, parent_order_id,
                    )
                    durable_spot_filled = float(totals["max_spot_filled"] or 0.0)
                    durable_confirmed = float(totals["confirmed_futures_qty"] or 0.0)

                    effective_spot_filled = max(
                        requested_spot_filled, durable_spot_filled
                    )
                    effective_target = max(
                        requested_target, effective_spot_filled * ratio
                    )
                    # Infer the caller's confirmed watermark from its target
                    # and residual. A higher durable DB aggregate always wins.
                    caller_confirmed = max(
                        0.0, requested_target - requested_residual
                    )
                    effective_confirmed = max(
                        durable_confirmed, caller_confirmed
                    )
                    effective_residual = max(
                        0.0, effective_target - effective_confirmed
                    )
                    if effective_residual <= 0:
                        return None

                    fill_version = int(round(effective_spot_filled * 1e9))
                    key = hedge_claim_key(
                        parent_exchange, parent_order_id, fill_version
                    )
                    coid = hedge_client_order_id_for_claim(key)
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
                        effective_spot_filled,
                        effective_target,
                        effective_residual,
                        ratio,
                        str(futures_exchange or ""),
                        str(futures_symbol or ""),
                        coid,
                        str(claimed_by or ""),
                    )
                    if row is None:
                        return None
                    return {
                        "claim_key": row["claim_key"],
                        "hedge_client_order_id": row["hedge_client_order_id"],
                        "version": int(row["version"]),
                        "cumulative_spot_filled": effective_spot_filled,
                        "target_hedge_qty": effective_target,
                        "confirmed_qty": effective_confirmed,
                        "residual_qty": effective_residual,
                    }
        except Exception:
            logger.exception(
                "[Hedge Claim] claim failed for parent %s:%s; failing closed",
                parent_exchange, parent_order_id,
            )
            return None

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
