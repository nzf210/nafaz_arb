"""Durable per-order-attempt ledger for Executor V3 Futures hedges.

The claim row serializes one cumulative Spot-fill generation. This service
serializes distinct venue orders within that generation and records verified
trade facts idempotently. It deliberately does not submit exchange orders:
callers must keep the existing Global STOP/mutation fence around that boundary.
"""
from __future__ import annotations

import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


_ACTIVE = (
    "PREPARED", "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED", "OPEN",
    "PARTIALLY_FILLED", "RECONCILIATION_REQUIRED",
)
_TERMINAL = ("FILLED", "CANCELED", "REJECTED")


def _decimal(value: Any, field: str, *, allow_zero: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise ValueError(f"{field} must be {'non-negative' if allow_zero else 'positive'}")
    return result


def _client_order_id(claim_key: str, attempt_no: int) -> str:
    digest = hashlib.sha256(
        f"{claim_key}:attempt:{attempt_no}".encode("utf-8")
    ).hexdigest()[:24]
    return f"hda{digest}"


class HedgeAttemptLedger:
    """PostgreSQL-backed attempt, fill, and audit repository."""

    def __init__(self, global_stop: Any = None):
        self.global_stop = global_stop

    @property
    def enabled(self) -> bool:
        return bool(
            self.global_stop is not None
            and getattr(self.global_stop, "enabled", False)
        )

    async def _pool(self):
        stop = self.global_stop
        if stop is None or not getattr(stop, "enabled", False):
            return None
        if not await stop.ensure_pool():
            return None
        return stop.pool

    async def prepare_attempt(
        self,
        *,
        claim_key: str,
        futures_account_scope_id: str,
        futures_exchange: str,
        futures_symbol: str,
        actor_id: str,
    ) -> Optional[dict]:
        """Reserve the next residual attempt before crossing the venue boundary.

        An unresolved attempt blocks creation of another attempt. Residual is
        derived only from append-only verified fills and unresolved reservations;
        caller-provided local quantities are intentionally not accepted.
        """
        pool = await self._pool()
        if pool is None:
            return None
        claim_key = str(claim_key or "").strip()
        scope = str(futures_account_scope_id or "").strip()
        exchange = str(futures_exchange or "").strip().lower()
        symbol = str(futures_symbol or "").strip()
        if not all((claim_key, scope, exchange, symbol)):
            raise ValueError("claim, account scope, exchange and symbol are required")

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "select pg_advisory_xact_lock(hashtextextended($1, 0))",
                        "hedge-attempt:" + claim_key,
                    )
                    claim = await conn.fetchrow(
                        """
                        select target_hedge_qty, futures_exchange, futures_symbol,
                               status
                        from public.executor_hedge_intent_claims
                        where claim_key=$1
                        for update
                        """,
                        claim_key,
                    )
                    if claim is None:
                        return None
                    if str(claim["status"]) not in ("CLAIMED", "SUBMITTED"):
                        return None
                    if (
                        str(claim["futures_exchange"] or "").lower() != exchange
                        or str(claim["futures_symbol"] or "") != symbol
                    ):
                        raise ValueError("attempt venue/instrument differs from its hedge claim")

                    unresolved = await conn.fetchrow(
                        """
                        select attempt_id, attempt_no, hedge_client_order_id, status,
                               requested_qty_base, confirmed_qty_base
                        from public.executor_hedge_attempts
                        where claim_key=$1 and status = any($2::text[])
                        order by attempt_no desc limit 1
                        """,
                        claim_key, list(_ACTIVE),
                    )
                    if unresolved is not None:
                        return {
                            "blocked": True,
                            "attempt_id": str(unresolved["attempt_id"]),
                            "attempt_no": int(unresolved["attempt_no"]),
                            "client_order_id": str(unresolved["hedge_client_order_id"]),
                            "status": str(unresolved["status"]),
                        }

                    totals = await conn.fetchrow(
                        """
                        select
                          coalesce((
                            select sum(f.fill_qty_base)
                            from public.executor_hedge_attempt_fills f
                            join public.executor_hedge_attempts a
                              on a.attempt_id=f.attempt_id
                            where a.claim_key=$1
                          ), 0) as confirmed,
                          coalesce((
                            select sum(greatest(a.requested_qty_base-a.confirmed_qty_base, 0))
                            from public.executor_hedge_attempts a
                            where a.claim_key=$1 and a.status=any($2::text[])
                          ), 0) as reserved,
                          coalesce((
                            select max(attempt_no)
                            from public.executor_hedge_attempts
                            where claim_key=$1
                          ), 0) as max_attempt
                        """,
                        claim_key, list(_ACTIVE),
                    )
                    target = _decimal(claim["target_hedge_qty"], "claim target")
                    confirmed = _decimal(totals["confirmed"], "confirmed fills", allow_zero=True)
                    reserved = _decimal(totals["reserved"], "reserved quantity", allow_zero=True)
                    residual = target - confirmed - reserved
                    if residual <= Decimal("0"):
                        return None

                    attempt_no = int(totals["max_attempt"]) + 1
                    coid = _client_order_id(claim_key, attempt_no)
                    row = await conn.fetchrow(
                        """
                        insert into public.executor_hedge_attempts (
                          claim_key, attempt_no, futures_account_scope_id,
                          futures_exchange, futures_symbol, hedge_client_order_id,
                          requested_qty_base, status
                        ) values ($1,$2,$3,$4,$5,$6,$7,'PREPARED')
                        returning attempt_id, attempt_no, hedge_client_order_id,
                                  requested_qty_base, status, version
                        """,
                        claim_key, attempt_no, scope, exchange, symbol, coid, residual,
                    )
                    await self._event(
                        conn, row["attempt_id"], "prepared",
                        "ATTEMPT_PREPARED",
                        {
                            "claim_key": claim_key,
                            "attempt_no": attempt_no,
                            "requested_qty_base": str(residual),
                            "actor_id": str(actor_id or ""),
                        },
                    )
                    return {
                        "blocked": False,
                        "attempt_id": str(row["attempt_id"]),
                        "attempt_no": int(row["attempt_no"]),
                        "client_order_id": str(row["hedge_client_order_id"]),
                        "requested_qty_base": str(row["requested_qty_base"]),
                        "status": str(row["status"]),
                        "version": int(row["version"]),
                        "confirmed_qty_base": str(confirmed),
                        "residual_qty_base": str(residual),
                    }
        except Exception:
            # Fail closed: the caller must not fall back to a non-durable ID.
            import logging
            logging.getLogger(__name__).exception(
                "failed to prepare durable hedge attempt for claim %s", claim_key
            )
            return None

    async def mark_submitting(self, attempt_id: str, *, expected_version: int) -> bool:
        return await self._transition(
            attempt_id, expected_version, "PREPARED", "SUBMITTING",
            "ATTEMPT_SUBMITTING",
        )

    async def mark_unknown(self, attempt_id: str, *, expected_version: int, reason: str) -> bool:
        return await self._transition(
            attempt_id, expected_version, "SUBMITTING", "UNKNOWN",
            "ATTEMPT_UNKNOWN", {"reason": str(reason or "")[:1000]},
        )

    async def mark_acknowledged(
        self, attempt_id: str, *, expected_version: int, venue_order_id: str
    ) -> bool:
        venue_order_id = str(venue_order_id or "").strip()
        if not venue_order_id:
            return False
        pool = await self._pool()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        update public.executor_hedge_attempts
                        set status='ACKNOWLEDGED', venue_order_id=$3,
                            submitted_at=coalesce(submitted_at, now()),
                            version=version+1, updated_at=now()
                        where attempt_id=$1::uuid and version=$2
                          and status in ('SUBMITTING','UNKNOWN')
                        returning version
                        """,
                        attempt_id, int(expected_version), venue_order_id,
                    )
                    if row is None:
                        return False
                    await self._event(
                        conn, attempt_id, f"ack:{venue_order_id}",
                        "ATTEMPT_ACKNOWLEDGED", {"venue_order_id": venue_order_id},
                    )
                    return True
        except Exception:
            return False

    async def record_fill(
        self,
        *,
        attempt_id: str,
        futures_account_scope_id: str,
        futures_exchange: str,
        futures_symbol: str,
        venue_trade_id: str,
        source_event_id: str = "",
        fill_qty_base: Any,
        fill_price: Any,
        fee_amount: Any = None,
        fee_currency: str = "",
        venue_filled_at: Any = None,
    ) -> dict:
        """Append one verified trade and atomically refresh attempt fill projection."""
        pool = await self._pool()
        if pool is None:
            raise RuntimeError("durable hedge attempt ledger is unavailable")
        qty = _decimal(fill_qty_base, "fill quantity")
        price = _decimal(fill_price, "fill price")
        fee = None if fee_amount is None else _decimal(fee_amount, "fee", allow_zero=True)
        trade_id = str(venue_trade_id or "").strip()
        if not trade_id:
            raise ValueError("venue trade ID is required")
        scope = str(futures_account_scope_id or "").strip()
        exchange = str(futures_exchange or "").strip().lower()
        symbol = str(futures_symbol or "").strip()
        if not all((scope, exchange, symbol)):
            raise ValueError("fill account scope, exchange and symbol are required")

        async with pool.acquire() as conn:
            async with conn.transaction():
                attempt = await conn.fetchrow(
                    """
                    select futures_account_scope_id, futures_exchange, futures_symbol,
                           status, requested_qty_base
                    from public.executor_hedge_attempts
                    where attempt_id=$1::uuid for update
                    """,
                    attempt_id,
                )
                if attempt is None:
                    raise ValueError("unknown hedge attempt")
                identity = (str(attempt["futures_account_scope_id"]),
                            str(attempt["futures_exchange"]).lower(),
                            str(attempt["futures_symbol"]))
                if identity != (scope, exchange, symbol):
                    raise ValueError("fill identity does not match hedge attempt")
                if str(attempt["status"]) not in (
                    "ACKNOWLEDGED", "OPEN", "PARTIALLY_FILLED", "RECONCILIATION_REQUIRED"
                ):
                    raise ValueError("attempt is not in a fill-accepting state")

                inserted = await conn.fetchrow(
                    """
                    insert into public.executor_hedge_attempt_fills (
                      attempt_id, futures_account_scope_id, futures_exchange,
                      futures_symbol, venue_trade_id, source_event_id,
                      fill_qty_base, fill_price, fee_amount, fee_currency,
                      venue_filled_at
                    ) values ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    on conflict do nothing
                    returning fill_id
                    """,
                    attempt_id, scope, exchange, symbol, trade_id,
                    str(source_event_id or "") or None, qty, price, fee,
                    str(fee_currency or "") or None, venue_filled_at,
                )
                if inserted is None:
                    existing = await conn.fetchrow(
                        """
                        select fill_qty_base, fill_price, fee_amount, fee_currency
                        from public.executor_hedge_attempt_fills
                        where attempt_id=$1::uuid and venue_trade_id=$2
                        """,
                        attempt_id, trade_id,
                    )
                    if existing is None:
                        raise ValueError("fill idempotency conflict belongs to another attempt")
                    if (
                        _decimal(existing["fill_qty_base"], "stored fill") != qty
                        or _decimal(existing["fill_price"], "stored price") != price
                        or (None if existing["fee_amount"] is None else _decimal(existing["fee_amount"], "stored fee", allow_zero=True)) != fee
                        or (str(existing["fee_currency"] or "")) != str(fee_currency or "")
                    ):
                        raise ValueError("immutable venue trade facts changed on replay")

                total = await conn.fetchval(
                    """
                    select coalesce(sum(fill_qty_base),0)
                    from public.executor_hedge_attempt_fills where attempt_id=$1::uuid
                    """,
                    attempt_id,
                )
                total = _decimal(total, "attempt fill total", allow_zero=True)
                requested = _decimal(attempt["requested_qty_base"], "requested quantity")
                if total > requested:
                    raise ValueError("verified fills exceed attempt requested quantity")
                await conn.execute(
                    """
                    update public.executor_hedge_attempts
                    set confirmed_qty_base=$2,
                        status=case when status='ACKNOWLEDGED' then 'PARTIALLY_FILLED'
                                    else status end,
                        version=version+1, updated_at=now()
                    where attempt_id=$1::uuid
                    """,
                    attempt_id, total,
                )
                await self._event(
                    conn, attempt_id, f"fill:{trade_id}", "ATTEMPT_FILL_RECORDED",
                    {"venue_trade_id": trade_id, "fill_qty_base": str(qty),
                     "aggregate_confirmed_qty_base": str(total)},
                )
                return {"inserted": inserted is not None, "confirmed_qty_base": str(total)}

    async def finalize_attempt(
        self,
        attempt_id: str,
        *,
        expected_version: int,
        terminal_status: str,
        order_reconciled: bool,
        fills_reconciled: bool,
        actor_id: str,
    ) -> bool:
        """Close an attempt only after both order and trade history are reconciled."""
        status = str(terminal_status or "").upper()
        if status not in _TERMINAL or not order_reconciled or not fills_reconciled:
            return False
        pool = await self._pool()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        update public.executor_hedge_attempts
                        set status=$3, terminal_at=now(), last_reconciled_at=now(),
                            version=version+1, updated_at=now()
                        where attempt_id=$1::uuid and version=$2
                          and status in ('ACKNOWLEDGED','OPEN','PARTIALLY_FILLED','RECONCILIATION_REQUIRED')
                        returning confirmed_qty_base, requested_qty_base
                        """,
                        attempt_id, int(expected_version), status,
                    )
                    if row is None:
                        return False
                    await self._event(
                        conn, attempt_id, f"terminal:{status}:{expected_version}",
                        "ATTEMPT_TERMINAL_RECONCILED",
                        {"status": status, "actor_id": str(actor_id or ""),
                         "confirmed_qty_base": str(row["confirmed_qty_base"]),
                         "requested_qty_base": str(row["requested_qty_base"])},
                    )
                    return True
        except Exception:
            return False

    async def _transition(
        self, attempt_id: str, expected_version: int, from_status: str,
        to_status: str, event_type: str, payload: Optional[dict] = None,
    ) -> bool:
        pool = await self._pool()
        if pool is None:
            return False
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        """
                        update public.executor_hedge_attempts
                        set status=$4, submitted_at=case when $4='SUBMITTING'
                              then coalesce(submitted_at,now()) else submitted_at end,
                            version=version+1, updated_at=now()
                        where attempt_id=$1::uuid and version=$2 and status=$3
                        returning version
                        """,
                        attempt_id, int(expected_version), from_status, to_status,
                    )
                    if row is None:
                        return False
                    await self._event(
                        conn, attempt_id, f"{to_status.lower()}:{expected_version}",
                        event_type, payload or {},
                    )
                    return True
        except Exception:
            return False

    @staticmethod
    async def _event(conn, attempt_id: Any, event_key: str,
                     event_type: str, payload: dict) -> None:
        await conn.execute(
            """
            insert into public.executor_hedge_attempt_events
              (attempt_id,event_key,event_type,payload)
            values ($1::uuid,$2,$3,$4::jsonb)
            on conflict (attempt_id,event_key) do nothing
            """,
            str(attempt_id), event_key, event_type,
            __import__("json").dumps(payload, sort_keys=True, default=str),
        )
