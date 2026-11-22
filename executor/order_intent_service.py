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


class OrderIntentError(RuntimeError):
    """Base error for durable mutation intent operations."""


class OrderIntentConflict(OrderIntentError):
    """An idempotency key or client ID was reused for different request data."""


class OrderIntentReservationConflict(OrderIntentError):
    """The new intent would exceed a caller-supplied reservation budget."""


class OrderIntentStaleOwner(OrderIntentError):
    """The mutation lease is missing, expired or superseded."""


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


def _required(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


class OrderIntentService:
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
        account = _required(account_scope_id, "account_scope_id")
        idem = _required(idempotency_key, "idempotency_key")
        exchange = _required(venue, "venue").lower()
        market = _required(market_type, "market_type").lower()
        symbol = _required(instrument, "instrument").upper()
        order_side = _required(side, "side").lower()
        kind = _required(order_type, "order_type").lower()
        actor = _required(actor_id, "actor_id")
        qty = _decimal(quantity, "quantity")
        limit_price = _decimal(price, "price", allow_zero=True)
        if kind == "limit" and limit_price <= 0:
            raise ValueError("limit orders require a positive price")
        if order_side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        base_reservation = _decimal(reserved_base_qty, "reserved_base_qty", allow_zero=True)
        notional_reservation = _decimal(reserved_notional, "reserved_notional", allow_zero=True)
        asset = str(reservation_asset or "").strip().upper() or None
        if notional_reservation > 0 and not asset:
            raise ValueError("reservation_asset is required for a positive reserved_notional")
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
        account = _required(account_scope_id, "account_scope_id")
        idem = _required(idempotency_key, "idempotency_key")
        exchange = _required(venue, "venue").lower()
        market = _required(market_type, "market_type").lower()
        symbol = _required(instrument, "instrument").upper()
        actor = _required(actor_id, "actor_id")
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
              AND instrument=$4
              AND status IN ('PREPARED','SUBMITTING','UNKNOWN','ACKNOWLEDGED')
            """,
            account_scope_id, venue, market_type, instrument,
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

    async def _validate_mutation_lease(
        self, conn: Any, row: Any, lease: Any, *, require_persisted_owner: bool,
    ) -> tuple[str, int]:
        account = str(row["account_scope_id"])
        venue = str(row["venue"])
        market = str(row["market_type"])
        instrument = str(row["instrument"])
        expected_scope = build_scope_key(account, venue, market, instrument)
        scope_key = str(getattr(lease, "scope_key", "") or "")
        owner_id = str(getattr(lease, "owner_id", "") or "")
        generation = int(getattr(lease, "generation", 0) or 0)
        if not owner_id or scope_key != expected_scope or generation <= 0:
            raise OrderIntentStaleOwner("mutation lease does not match the intent scope")
        if require_persisted_owner and (
            str(row["owner_scope_key"] or "") != scope_key
            or int(row["owner_generation"] or 0) != generation
        ):
            raise OrderIntentStaleOwner("mutation intent is fenced by a newer owner generation")
        active = await conn.fetchrow(
            """
            SELECT 1 FROM public.executor_ownership_leases
            WHERE scope_key=$1 AND account_scope_id=$2 AND venue=$3
              AND market_type=$4 AND instrument=$5 AND owner_id=$6
              AND generation=$7 AND lease_expires_at > clock_timestamp()
            FOR UPDATE
            """,
            scope_key, account, venue, market, instrument, owner_id, generation,
        )
        if active is None:
            raise OrderIntentStaleOwner("mutation lease is expired, missing or superseded")
        return scope_key, generation

    async def _transition(
        self, *, intent_id: str, target_status: str, allowed_from: tuple[str, ...],
        event_type: str, actor_id: str, payload: Mapping[str, Any],
        lease: Any = None, require_lease: bool = True,
        exchange_order_id: str | None = None, venue_response: Mapping[str, Any] | None = None,
        error: str | None = None, release_reservation: bool = False,
    ) -> dict:
        actor = _required(actor_id, "actor_id")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT * FROM public.executor_mutation_intents WHERE intent_id=$1 FOR UPDATE",
                    intent_id,
                )
                if row is None:
                    raise OrderIntentError("mutation intent does not exist")
                scope_key = str(row["owner_scope_key"] or "")
                generation = int(row["owner_generation"] or 0)
                if require_lease:
                    scope_key, generation = await self._validate_mutation_lease(
                        conn, row, lease,
                        require_persisted_owner=target_status != "SUBMITTING",
                    )
                event_key = str(
                    payload.get("event_idempotency_key")
                    or f"{intent_id}:{target_status.lower()}:{int(row['attempt_count'])}"
                )
                event = await conn.fetchrow(
                    """
                    SELECT event_type, payload FROM public.executor_mutation_intent_events
                    WHERE intent_id=$1 AND event_idempotency_key=$2
                    """,
                    intent_id, event_key,
                )
                if event is not None:
                    if str(event["event_type"]) != event_type:
                        raise OrderIntentConflict("event idempotency key was reused for another transition")
                    return dict(row)
                current = str(row["status"]).upper()
                if current not in allowed_from:
                    raise OrderIntentConflict(
                        f"invalid intent transition {current} -> {target_status}"
                    )
                attempt_count = int(row["attempt_count"])
                if target_status == "SUBMITTING":
                    attempt_count += 1
                next_version = int(row["state_version"]) + 1
                reservation_base = Decimal("0") if release_reservation else Decimal(str(row["reserved_base_qty"]))
                reservation_notional = Decimal("0") if release_reservation else Decimal(str(row["reserved_notional"]))
                body = {
                    **dict(payload),
                    "previous_status": current,
                    "target_status": target_status,
                    "state_version": next_version,
                    "attempt_count": attempt_count,
                }
                updated = await conn.fetchrow(
                    """
                    UPDATE public.executor_mutation_intents
                    SET status=$2, state_version=$3, attempt_count=$4,
                        owner_scope_key=COALESCE($5, owner_scope_key),
                        owner_generation=COALESCE($6, owner_generation),
                        exchange_order_id=COALESCE($7, exchange_order_id),
                        venue_response=COALESCE($8::jsonb, venue_response),
                        last_error=$9, reserved_base_qty=$10, reserved_notional=$11,
                        resolved_at=CASE WHEN $2 IN (
                            'REJECTED','CANCEL_CONFIRMED','ABORTED','COMPLETED'
                        ) THEN clock_timestamp() ELSE NULL END,
                        updated_at=clock_timestamp()
                    WHERE intent_id=$1 AND state_version=$12
                    RETURNING *
                    """,
                    intent_id, target_status, next_version, attempt_count,
                    scope_key or None, generation or None, exchange_order_id,
                    json.dumps(dict(venue_response), separators=(",", ":"), default=str)
                    if venue_response is not None else None,
                    str(error)[:2000] if error else None,
                    reservation_base, reservation_notional, int(row["state_version"]),
                )
                if updated is None:
                    raise OrderIntentStaleOwner("mutation intent compare-and-swap lost")
                await conn.execute(
                    """
                    INSERT INTO public.executor_mutation_intent_events (
                        intent_id, event_version, event_type, event_idempotency_key,
                        actor_id, owner_scope_key, owner_generation, payload
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    """,
                    intent_id, next_version, event_type, event_key, actor,
                    scope_key or None, generation or None,
                    json.dumps(body, sort_keys=True, separators=(",", ":"), default=str),
                )
        return dict(updated)

    async def mark_submitting(
        self, intent_id: str, *, lease: Any, actor_id: str,
    ) -> dict:
        return await self._transition(
            intent_id=intent_id, target_status="SUBMITTING",
            allowed_from=("PREPARED",), event_type="INTENT_SUBMITTING",
            actor_id=actor_id, payload={}, lease=lease,
        )

    async def mark_acknowledged(
        self, intent_id: str, *, lease: Any, actor_id: str,
        exchange_order_id: str, venue_response: Mapping[str, Any],
    ) -> dict:
        order_id = _required(exchange_order_id, "exchange_order_id")
        return await self._transition(
            intent_id=intent_id, target_status="ACKNOWLEDGED",
            allowed_from=("SUBMITTING", "UNKNOWN"), event_type="INTENT_ACKNOWLEDGED",
            actor_id=actor_id, payload={"exchange_order_id": order_id},
            lease=lease, exchange_order_id=order_id, venue_response=venue_response,
        )

    async def mark_unknown(
        self, intent_id: str, *, lease: Any, actor_id: str, error: str,
    ) -> dict:
        return await self._transition(
            intent_id=intent_id, target_status="UNKNOWN",
            allowed_from=("SUBMITTING",), event_type="INTENT_SUBMISSION_UNKNOWN",
            actor_id=actor_id, payload={}, lease=lease, error=error,
        )

    async def mark_rejected(
        self, intent_id: str, *, lease: Any, actor_id: str,
        error: str, definitive: bool,
    ) -> dict:
        if not definitive:
            raise OrderIntentError("only a definitive venue rejection may resolve an intent")
        return await self._transition(
            intent_id=intent_id, target_status="REJECTED",
            allowed_from=("SUBMITTING", "UNKNOWN"), event_type="INTENT_REJECTED",
            actor_id=actor_id, payload={}, lease=lease, error=error,
            release_reservation=True,
        )

    async def mark_aborted(
        self, intent_id: str, *, actor_id: str, reason: str,
    ) -> dict:
        return await self._transition(
            intent_id=intent_id, target_status="ABORTED",
            allowed_from=("PREPARED",), event_type="INTENT_ABORTED",
            actor_id=actor_id, payload={}, require_lease=False, error=reason,
            release_reservation=True,
        )

    async def mark_cancel_confirmed(
        self, intent_id: str, *, lease: Any, actor_id: str,
        final_status: str, fresh_fetch: bool, fills_reconciled: bool,
    ) -> dict:
        status = str(final_status or "").strip().lower()
        if status not in {"canceled", "cancelled", "closed", "filled", "rejected", "expired"}:
            raise OrderIntentError("cancel confirmation requires a verified terminal venue status")
        if not fresh_fetch or not fills_reconciled:
            raise OrderIntentError("cancel confirmation requires fresh order state and reconciled racing fills")
        return await self._transition(
            intent_id=intent_id, target_status="CANCEL_CONFIRMED",
            allowed_from=("SUBMITTING", "UNKNOWN", "ACKNOWLEDGED"),
            event_type="INTENT_CANCEL_CONFIRMED", actor_id=actor_id,
            payload={"final_status": status, "fresh_fetch": True, "fills_reconciled": True},
            lease=lease, release_reservation=True,
        )

    async def mark_completed(
        self, intent_id: str, *, lease: Any, actor_id: str,
        terminal_status: str, fills_reconciled: bool,
    ) -> dict:
        status = str(terminal_status or "").strip().lower()
        if status not in {"canceled", "cancelled", "closed", "filled", "rejected", "expired"}:
            raise OrderIntentError("completion requires a verified terminal order status")
        if not fills_reconciled:
            raise OrderIntentError("completion requires reconciled fills")
        return await self._transition(
            intent_id=intent_id, target_status="COMPLETED",
            allowed_from=("ACKNOWLEDGED",), event_type="INTENT_COMPLETED",
            actor_id=actor_id, payload={"terminal_status": status, "fills_reconciled": True},
            lease=lease, release_reservation=True,
        )

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
