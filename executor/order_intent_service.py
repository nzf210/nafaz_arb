"""Durable logical order/mutation intents for Executor V3.

This service has no exchange client. It persists immutable intent identity,
reservations, and lifecycle events before a mutation gateway can perform I/O.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from executor.helpers import build_client_order_id
from executor.ownership import build_scope_key
from executor.order_intent_common import (
    OrderIntentError, OrderIntentConflict, OrderIntentReservationConflict,
    required,
)
from executor.order_intent_lifecycle import OrderIntentLifecycleMixin


def _canonical(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"),
            ensure_ascii=True, allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OrderIntentError("mutation intent payload is not canonical JSON") from exc


def _decimal(value: Any, field: str, *, allow_zero: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not number.is_finite() or (number < 0 if allow_zero else number <= 0):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {qualifier}")
    return number


class OrderIntentService(OrderIntentLifecycleMixin):
    """PostgreSQL logical intent repository with deterministic idempotency."""

    ACTIVE_RESERVATION_STATES = ("PREPARED", "SUBMITTING", "UNKNOWN", "ACKNOWLEDGED")

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL pool is required")
        self.pool = pool

    @staticmethod
    def _identity(account_scope_id: str, idempotency_key: str) -> tuple[str, str]:
        digest = hashlib.sha256(
            ("nafaz:mutation-intent:v1:" + account_scope_id + ":" + idempotency_key)
            .encode("utf-8")
        ).hexdigest()
        return "v3-mi-" + digest[:40], digest

    async def prepare_create(
        self, *, account_scope_id: str, idempotency_key: str,
        venue: str, market_type: str, instrument: str,
        side: str, order_type: str, quantity: Any, price: Any,
        actor_id: str, session_id: str | None = None, leg_id: str | None = None,
        reserved_base_qty: Any = 0, reserved_notional: Any = 0,
        reservation_asset: str | None = None,
        max_reserved_base_qty: Any | None = None,
        max_reserved_notional: Any | None = None,
        request_metadata: Mapping[str, Any] | None = None,
    ) -> dict:
        account = required(account_scope_id, "account_scope_id")
        idem = required(idempotency_key, "idempotency_key")
        exchange = required(venue, "venue").lower()
        market = required(market_type, "market_type").lower()
        symbol = required(instrument, "instrument").upper()
        order_side = required(side, "side").lower()
        kind = required(order_type, "order_type").lower()
        actor = required(actor_id, "actor_id")
        qty = _decimal(quantity, "quantity")
        limit_price = _decimal(price, "price", allow_zero=True)
        if kind == "limit" and limit_price <= 0:
            raise ValueError("limit orders require a positive price")
        if order_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        base_reservation = _decimal(reserved_base_qty, "reserved_base_qty", allow_zero=True)
        notional_reservation = _decimal(reserved_notional, "reserved_notional", allow_zero=True)
        asset = str(reservation_asset or "").strip().upper() or None
        if (notional_reservation > 0 or max_reserved_notional is not None) and not asset:
            raise ValueError("reservation_asset is required for notional reservation or limits")
        session = str(session_id or "").strip() or None
        leg = str(leg_id or "").strip() or None
        if leg and not session:
            raise ValueError("leg_id requires session_id")
        intent_id, digest = self._identity(account, idem)
        client_order_id = build_client_order_id(
            f"{exchange}-{digest[:10]}", "create", 1
        )
        payload = {
            "operation": "CREATE", "account_scope_id": account,
            "session_id": session, "leg_id": leg, "venue": exchange,
            "market_type": market, "instrument": symbol, "side": order_side,
            "order_type": kind, "quantity": format(qty, "f"),
            "price": format(limit_price, "f"),
            "reserved_base_qty": format(base_reservation, "f"),
            "reserved_notional": format(notional_reservation, "f"),
            "reservation_asset": asset,
            "request_metadata": dict(request_metadata or {}),
        }
        request_json = _canonical(payload)
        request_hash = hashlib.sha256(request_json.encode("utf-8")).hexdigest()
        return await self._prepare(
            intent_id=intent_id, idempotency_key=idem, account_scope_id=account,
            operation="CREATE", venue=exchange, market_type=market,
            instrument=symbol, client_order_id=client_order_id,
            target_order_id=None, target_client_order_id=None,
            side=order_side, order_type=kind, quantity=qty, price=limit_price,
            session_id=session, leg_id=leg, reserved_base_qty=base_reservation,
            reserved_notional=notional_reservation, reservation_asset=asset,
            request_hash=request_hash, request_payload=request_json,
            actor_id=actor, max_reserved_base_qty=max_reserved_base_qty,
            max_reserved_notional=max_reserved_notional,
        )

    async def prepare_cancel(
        self, *, account_scope_id: str, idempotency_key: str,
        venue: str, market_type: str, instrument: str,
        actor_id: str, target_order_id: str | None = None,
        target_client_order_id: str | None = None,
        session_id: str | None = None, leg_id: str | None = None,
    ) -> dict:
        account = required(account_scope_id, "account_scope_id")
        idem = required(idempotency_key, "idempotency_key")
        exchange = required(venue, "venue").lower()
        market = required(market_type, "market_type").lower()
        symbol = required(instrument, "instrument").upper()
        actor = required(actor_id, "actor_id")
        order_id = str(target_order_id or "").strip() or None
        target_cid = str(target_client_order_id or "").strip() or None
        if not order_id and not target_cid:
            raise ValueError("target_order_id or target_client_order_id is required")
        session = str(session_id or "").strip() or None
        leg = str(leg_id or "").strip() or None
        if leg and not session:
            raise ValueError("leg_id requires session_id")
        intent_id, _ = self._identity(account, idem)
        payload = {
            "operation": "CANCEL", "account_scope_id": account,
            "session_id": session, "leg_id": leg, "venue": exchange,
            "market_type": market, "instrument": symbol,
            "target_order_id": order_id, "target_client_order_id": target_cid,
        }
        request_json = _canonical(payload)
        request_hash = hashlib.sha256(request_json.encode("utf-8")).hexdigest()
        return await self._prepare(
            intent_id=intent_id, idempotency_key=idem, account_scope_id=account,
            operation="CANCEL", venue=exchange, market_type=market,
            instrument=symbol, client_order_id=None, target_order_id=order_id,
            target_client_order_id=target_cid, side=None, order_type=None,
            quantity=None, price=None, session_id=session, leg_id=leg,
            reserved_base_qty=Decimal("0"), reserved_notional=Decimal("0"),
            reservation_asset=None, request_hash=request_hash,
            request_payload=request_json, actor_id=actor,
        )

    async def _prepare(
        self, *, intent_id: str, idempotency_key: str, account_scope_id: str,
        operation: str, venue: str, market_type: str, instrument: str,
        client_order_id: str | None, target_order_id: str | None,
        target_client_order_id: str | None, side: str | None,
        order_type: str | None, quantity: Decimal | None, price: Decimal | None,
        session_id: str | None, leg_id: str | None,
        reserved_base_qty: Decimal, reserved_notional: Decimal,
        reservation_asset: str | None, request_hash: str,
        request_payload: str, actor_id: str,
        max_reserved_base_qty: Any | None = None,
        max_reserved_notional: Any | None = None,
    ) -> dict:
        scope_key = build_scope_key(account_scope_id, venue, market_type, instrument)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "select pg_advisory_xact_lock(hashtextextended('nafaz:mutation-intent:' || $1, 0))",
                    scope_key,
                )
                if session_id:
                    session = await conn.fetchrow(
                        "SELECT account_scope_id FROM public.execution_sessions WHERE session_id=$1 FOR KEY SHARE",
                        session_id,
                    )
                    if session is None or str(session["account_scope_id"] or "") != account_scope_id:
                        raise OrderIntentError("session is missing or belongs to a different account scope")
                if leg_id:
                    leg = await conn.fetchrow(
                        "SELECT session_id, account_scope_id FROM public.execution_legs WHERE leg_id=$1 FOR KEY SHARE",
                        leg_id,
                    )
                    if (
                        leg is None or str(leg["session_id"]) != str(session_id)
                        or str(leg["account_scope_id"] or "") != account_scope_id
                    ):
                        raise OrderIntentError("leg identity does not belong to the requested session/account")

                existing = await conn.fetchrow(
                    """
                    SELECT * FROM public.executor_mutation_intents
                    WHERE account_scope_id=$1 AND idempotency_key=$2
                    FOR UPDATE
                    """,
                    account_scope_id, idempotency_key,
                )
                if existing is not None:
                    if str(existing["request_hash"]) != request_hash:
                        raise OrderIntentConflict(
                            "idempotency key is already bound to a different mutation request"
                        )
                    return dict(existing)

                if operation == "CREATE":
                    await self._assert_reservation_budget(
                        conn, account_scope_id=account_scope_id, venue=venue,
                        market_type=market_type, instrument=instrument,
                        reserved_base_qty=reserved_base_qty,
                        reserved_notional=reserved_notional,
                        max_reserved_base_qty=max_reserved_base_qty,
                        max_reserved_notional=max_reserved_notional,
                    )
                row = await conn.fetchrow(
                    """
                    INSERT INTO public.executor_mutation_intents (
                        intent_id, idempotency_key, operation, account_scope_id,
                        session_id, leg_id, venue, market_type, instrument,
                        client_order_id, target_order_id, target_client_order_id,
                        side, order_type, quantity, price, reserved_base_qty,
                        reserved_notional, reservation_asset, request_hash,
                        request_payload, status, state_version
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                        $13,$14,$15,$16,$17,$18,$19,$20,$21::jsonb,'PREPARED',1
                    )
                    RETURNING *
                    """,
                    intent_id, idempotency_key, operation, account_scope_id,
                    session_id, leg_id, venue, market_type, instrument,
                    client_order_id, target_order_id, target_client_order_id,
                    side, order_type, quantity, price, reserved_base_qty,
                    reserved_notional, reservation_asset, request_hash, request_payload,
                )
                await conn.execute(
                    """
                    INSERT INTO public.executor_mutation_intent_events (
                        intent_id, event_version, event_type, event_idempotency_key,
                        actor_id, payload
                    ) VALUES ($1,1,$2,$3,$4,$5::jsonb)
                    """,
                    intent_id, f"{operation}_INTENT_PREPARED",
                    f"{intent_id}:prepared", actor_id,
                    json.dumps({"request_hash": request_hash, "scope_key": scope_key}),
                )
        return dict(row)

    @staticmethod
    async def _assert_reservation_budget(
        conn: Any, *, account_scope_id: str, venue: str,
        market_type: str, instrument: str,
        reserved_base_qty: Decimal, reserved_notional: Decimal,
        max_reserved_base_qty: Any | None,
        max_reserved_notional: Any | None,
    ) -> None:
        if max_reserved_base_qty is None and max_reserved_notional is None:
            return
        current = await conn.fetchrow(
            """
            SELECT COALESCE(sum(reserved_base_qty),0) AS base_qty,
                   COALESCE(sum(reserved_notional),0) AS notional
            FROM public.executor_mutation_intents
            WHERE account_scope_id=$1 AND venue=$2 AND market_type=$3
              AND instrument=$4 AND reservation_asset IS NOT DISTINCT FROM $5
              AND status IN ('PREPARED','SUBMITTING','UNKNOWN','ACKNOWLEDGED')
            """,
            account_scope_id, venue, market_type, instrument,
            reservation_asset,
        )
        base_total = Decimal(str(current["base_qty"])) + reserved_base_qty
        notional_total = Decimal(str(current["notional"])) + reserved_notional
        if max_reserved_base_qty is not None and base_total > _decimal(
            max_reserved_base_qty, "max_reserved_base_qty", allow_zero=True
        ):
            raise OrderIntentReservationConflict("base-equivalent in-flight reservation budget exceeded")
        if max_reserved_notional is not None and notional_total > _decimal(
            max_reserved_notional, "max_reserved_notional", allow_zero=True
        ):
            raise OrderIntentReservationConflict("notional in-flight reservation budget exceeded")

    async def list_unresolved(
        self, *, account_scope_id: str | None = None, limit: int = 100,
    ) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM public.executor_mutation_intents
                WHERE status IN ('PREPARED','SUBMITTING','UNKNOWN','ACKNOWLEDGED')
                  AND ($1::text IS NULL OR account_scope_id=$1)
                ORDER BY created_at LIMIT $2
                """,
                str(account_scope_id or "").strip() or None,
                max(1, min(int(limit), 1000)),
            )
        return [dict(row) for row in rows]
