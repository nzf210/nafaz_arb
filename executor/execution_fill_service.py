"""Durable V3 fill facts and private-event inbox.

This service is intentionally isolated from V2 order submission. It accepts
already-confirmed venue trades, persists each trade exactly once, and advances
the owning leg/session aggregate in the same PostgreSQL transaction.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from executor.execution_repository import (
    ExecutionRepositoryError,
)
from executor.execution_repository_fencing import (
    insert_session_event, validate_coordinator_lease,
)


class FillConflict(ExecutionRepositoryError):
    """A venue trade identity was replayed with different immutable facts."""


class FillValidationError(ValueError):
    """A venue fill is incomplete or inconsistent with its durable leg."""


def _decimal(value: Any, name: str, *, positive: bool = True) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise FillValidationError(f"{name} must be a finite decimal") from exc
    if not number.is_finite():
        raise FillValidationError(f"{name} must be a finite decimal")
    if number <= 0 if positive else number < 0:
        condition = "positive" if positive else "non-negative"
        raise FillValidationError(f"{name} must be {condition}")
    return number


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FillValidationError("filled_at must be an ISO timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise FillValidationError("filled_at must be a timezone-aware timestamp")
    return value.astimezone(timezone.utc)


def _same_fill(row: Any, facts: Mapping[str, Any]) -> bool:
    return (
        str(row["session_id"]) == facts["session_id"]
        and str(row["leg_id"]) == facts["leg_id"]
        and str(row["account_scope_id"]) == facts["account_scope_id"]
        and str(row["venue"]).lower() == facts["venue"]
        and str(row["market_type"]).lower() == facts["market_type"]
        and str(row["symbol"]) == facts["symbol"]
        and str(row["side"]).lower() == facts["side"]
        and Decimal(str(row["quantity"])) == facts["quantity"]
        and Decimal(str(row["price"])) == facts["price"]
        and Decimal(str(row["quote_quantity"])) == facts["quote_quantity"]
        and str(row["quote_asset"]).upper() == facts["quote_asset"]
        and Decimal(str(row["fee_amount"])) == facts["fee_amount"]
        and str(row["fee_asset"] or "").upper() == facts["fee_asset"]
        and bool(row["fee_verified"]) == facts["fee_verified"]
        and str(row["fee_source"] or "") == facts["fee_source"]
        and str(row["exchange_order_id"] or "") == facts["exchange_order_id"]
        and str(row["client_order_id"] or "") == facts["client_order_id"]
        and str(row["source_event_id"] or "") == facts["source_event_id"]
        and row["filled_at"] == facts["filled_at"]
    )


class ExecutionFillService:
    """PostgreSQL-backed, exactly-once V3 fill persistence."""

    def __init__(self, pool: Any):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        self.pool = pool

    async def persist_inbox_event(
        self, *, account_scope_id: str, venue: str, event_type: str,
        source_event_id: str, payload: Mapping[str, Any],
        session_id: str | None = None, leg_id: str | None = None,
    ) -> dict:
        """Durably accept a normalized private/REST event before processing it."""
        values = {
            "account_scope_id": str(account_scope_id or "").strip(),
            "venue": str(venue or "").strip().lower(),
            "event_type": str(event_type or "").strip().upper(),
            "source_event_id": str(source_event_id or "").strip(),
        }
        if not all(values.values()) or not isinstance(payload, Mapping):
            raise FillValidationError("inbox identity and mapping payload are required")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO public.execution_event_inbox (
                    account_scope_id, venue, event_type, source_event_id,
                    session_id, leg_id, payload
                ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
                ON CONFLICT (account_scope_id, venue, event_type, source_event_id)
                DO NOTHING
                RETURNING inbox_id, processing_status
                """,
                values["account_scope_id"], values["venue"], values["event_type"],
                values["source_event_id"], session_id, leg_id, encoded,
            )
            if row is not None:
                return {"accepted": True, "inbox_id": str(row["inbox_id"])}
            existing = await conn.fetchrow(
                """
                SELECT inbox_id, session_id, leg_id, payload
                FROM public.execution_event_inbox
                WHERE account_scope_id=$1 AND venue=$2 AND event_type=$3
                  AND source_event_id=$4
                """,
                values["account_scope_id"], values["venue"], values["event_type"],
                values["source_event_id"],
            )
            if (
                existing is None
                or str(existing["session_id"] or "") != str(session_id or "")
                or str(existing["leg_id"] or "") != str(leg_id or "")
                or (json.loads(existing["payload"]) if isinstance(existing["payload"], str)
                    else existing["payload"]) != json.loads(encoded)
            ):
                raise FillConflict("inbox source event identity was reused with different payload")
            return {"accepted": False, "duplicate": True, "inbox_id": str(existing["inbox_id"])}

    async def mark_inbox_event(
        self, *, inbox_id: str, status: str, error: str | None = None,
    ) -> bool:
        """Move an inbox event through its durable processing states."""
        target = str(status or "").upper()
        allowed = {"PROCESSING", "PROCESSED", "RETRY", "QUARANTINED"}
        if target not in allowed:
            raise FillValidationError("unsupported inbox processing status")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE public.execution_event_inbox
                SET processing_status=$2,
                    attempts=attempts + CASE WHEN $2='PROCESSING' THEN 1 ELSE 0 END,
                    last_error=$3,
                    processed_at=CASE WHEN $2='PROCESSED' THEN clock_timestamp() ELSE NULL END
                WHERE inbox_id=$1::uuid
                  AND (
                    (processing_status='RECEIVED' AND $2 IN ('PROCESSING','QUARANTINED'))
                    OR (processing_status='PROCESSING' AND $2 IN ('PROCESSED','RETRY','QUARANTINED'))
                    OR (processing_status='RETRY' AND $2 IN ('PROCESSING','QUARANTINED'))
                  )
                RETURNING inbox_id
                """,
                inbox_id, target, str(error or "")[:2000] or None,
            )
            return row is not None

    async def record_fill(
        self, *, session_id: str, leg_id: str, account_scope_id: str,
        venue: str, market_type: str, symbol: str, exchange_trade_id: str,
        side: str, quantity: Any, price: Any, quote_asset: str,
        filled_at: Any, actor_id: str, event_idempotency_key: str,
        expected_leg_state_version: int, expected_aggregate_version: int,
        fencing_generation: int, coordinator_lease: Any,
        quote_quantity: Any | None = None, exchange_order_id: str | None = None,
        client_order_id: str | None = None, fee_amount: Any = "0",
        fee_asset: str | None = None, fee_verified: bool = False,
        fee_source: str = "UNVERIFIED", source_event_id: str | None = None,
        raw_payload: Mapping[str, Any] | None = None,
    ) -> dict:
        """Append one confirmed trade and update leg totals/event cursor atomically."""
        facts = {
            "session_id": str(session_id or "").strip(),
            "leg_id": str(leg_id or "").strip(),
            "account_scope_id": str(account_scope_id or "").strip(),
            "venue": str(venue or "").strip().lower(),
            "market_type": str(market_type or "").strip().lower(),
            "symbol": str(symbol or "").strip(),
            "exchange_trade_id": str(exchange_trade_id or "").strip(),
            "side": str(side or "").strip().lower(),
            "quantity": _decimal(quantity, "quantity"),
            "price": _decimal(price, "price"),
            "quote_asset": str(quote_asset or "").strip().upper(),
            "filled_at": _timestamp(filled_at),
            "fee_amount": _decimal(fee_amount, "fee_amount", positive=False),
            "fee_asset": str(fee_asset or "").strip().upper(),
        }
        if not all(facts[k] for k in (
            "session_id", "leg_id", "account_scope_id", "venue",
            "market_type", "symbol", "exchange_trade_id", "quote_asset",
        )):
            raise FillValidationError("session, leg, account, venue, market, symbol, trade and quote identity are required")
        if facts["side"] not in {"buy", "sell"}:
            raise FillValidationError("side must be buy or sell")
        facts["quote_quantity"] = (
            _decimal(quote_quantity, "quote_quantity")
            if quote_quantity is not None
            else facts["quantity"] * facts["price"]
        )
        actor = str(actor_id or "").strip()
        idem = str(event_idempotency_key or "").strip()
        if not actor or not idem or coordinator_lease is None:
            raise FillValidationError("actor, event idempotency key and coordinator lease are required")
        raw = json.dumps(raw_payload or {}, sort_keys=True, separators=(",", ":"), default=str)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await conn.fetchrow(
                    """
                    SELECT status, state_version, aggregate_version, fencing_generation,
                           account_scope_id, coordinator_scope_key, coordinator_owner_id
                    FROM public.execution_sessions
                    WHERE session_id=$1 FOR UPDATE
                    """,
                    facts["session_id"],
                )
                if session is None:
                    raise ExecutionRepositoryError("fill session does not exist")
                if str(session["account_scope_id"]) != facts["account_scope_id"]:
                    raise FillValidationError("fill account scope does not match session")
                await validate_coordinator_lease(
                    conn, coordinator_lease, session_id=facts["session_id"],
                    account_scope_id=facts["account_scope_id"],
                    expected_generation=fencing_generation,
                    session_scope_key=session["coordinator_scope_key"],
                    session_owner_id=session["coordinator_owner_id"],
                )
                leg = await conn.fetchrow(
                    """
                    SELECT status, state_version, venue, market_type, symbol,
                           quote_asset, account_scope_id, base_per_unit,
                           confirmed_filled_quantity, confirmed_quote_quantity,
                           confirmed_price_quantity, confirmed_exposure_quantity
                    FROM public.execution_legs
                    WHERE session_id=$1 AND leg_id=$2 FOR UPDATE
                    """,
                    facts["session_id"], facts["leg_id"],
                )
                if leg is None:
                    raise ExecutionRepositoryError("fill leg does not belong to session")
                if (
                    str(leg["account_scope_id"]) != facts["account_scope_id"]
                    or str(leg["venue"]).lower() != facts["venue"]
                    or str(leg["market_type"]).lower() != facts["market_type"]
                    or str(leg["symbol"]) != facts["symbol"]
                    or str(leg["quote_asset"]).upper() != facts["quote_asset"]
                ):
                    raise FillValidationError("venue-bound fill identity does not match its leg")

                prior_event = await conn.fetchrow(
                    """
                    SELECT event_type, payload
                    FROM public.execution_session_events
                    WHERE session_id=$1 AND event_idempotency_key=$2
                    """,
                    facts["session_id"], idem,
                )
                if prior_event is not None:
                    prior_payload = (
                        json.loads(prior_event["payload"])
                        if isinstance(prior_event["payload"], str)
                        else prior_event["payload"]
                    )
                    if (
                        str(prior_event["event_type"]) not in {
                            "LEG_FILL_RECORDED", "LEG_LATE_FILL_REQUIRES_RECONCILIATION"
                        }
                        or str(prior_payload.get("exchange_trade_id") or "")
                        != facts["exchange_trade_id"]
                    ):
                        raise FillConflict("fill event idempotency key was reused for another trade")
                    prior_fill = await conn.fetchrow(
                        """
                        SELECT * FROM public.execution_fills
                        WHERE account_scope_id=$1 AND venue=$2 AND market_type=$3
                          AND symbol=$4 AND exchange_trade_id=$5
                        """,
                        facts["account_scope_id"], facts["venue"], facts["market_type"],
                        facts["symbol"], facts["exchange_trade_id"],
                    )
                    if prior_fill is None:
                        raise FillConflict("fill event exists without its immutable fill record")
                    if not _same_fill(prior_fill, facts):
                        raise FillConflict("fill event idempotency key was replayed with changed immutable facts")
                    return {
                        "duplicate": True, "fill_id": str(prior_fill["fill_id"]),
                        "aggregate_version": int(session["aggregate_version"]),
                    }

                inserted = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_fills (
                        session_id, leg_id, account_scope_id, venue, market_type,
                        symbol, exchange_order_id, client_order_id, exchange_trade_id,
                        side, quantity, price, quote_quantity, quote_asset,
                        fee_amount, fee_asset, fee_verified, fee_source, filled_at,
                        source_event_id, raw_payload
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                        $15,$16,$17,$18,$19,$20,$21::jsonb
                    )
                    ON CONFLICT (account_scope_id, venue, market_type, symbol, exchange_trade_id)
                    DO NOTHING
                    RETURNING fill_id
                    """,
                    facts["session_id"], facts["leg_id"], facts["account_scope_id"],
                    facts["venue"], facts["market_type"], facts["symbol"],
                    exchange_order_id, client_order_id, facts["exchange_trade_id"],
                    facts["side"], facts["quantity"], facts["price"],
                    facts["quote_quantity"], facts["quote_asset"], facts["fee_amount"],
                    facts["fee_asset"] or None, bool(fee_verified),
                    str(fee_source or "UNVERIFIED")[:60], facts["filled_at"],
                    source_event_id, raw,
                )
                if inserted is None:
                    existing = await conn.fetchrow(
                        """
                        SELECT * FROM public.execution_fills
                        WHERE account_scope_id=$1 AND venue=$2 AND market_type=$3
                          AND symbol=$4 AND exchange_trade_id=$5
                        """,
                        facts["account_scope_id"], facts["venue"], facts["market_type"],
                        facts["symbol"], facts["exchange_trade_id"],
                    )
                    if existing is None or not _same_fill(existing, facts):
                        raise FillConflict("venue trade identity was replayed with different immutable facts")
                    return {"duplicate": True, "fill_id": str(existing["fill_id"]),
                            "aggregate_version": int(session["aggregate_version"])}

                old_leg_state = str(leg["status"]).upper()
                old_session_state = str(session["status"]).upper()
                leg_target = old_leg_state
                if old_leg_state in {"SUBMITTING", "SUBMISSION_UNKNOWN", "OPEN"}:
                    leg_target = "PARTIALLY_FILLED"
                elif old_leg_state in {"CANCELED", "CLOSED", "UNKNOWN", "RECONCILIATION_REQUIRED"}:
                    leg_target = "RECONCILIATION_REQUIRED"
                elif old_leg_state not in {"PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING"}:
                    raise FillValidationError(f"fill is not valid for leg state {old_leg_state}")

                session_target = old_session_state
                late_fill = old_session_state in {"COMPLETED", "FAILED", "ABORTED"}
                if late_fill:
                    leg_target = "RECONCILIATION_REQUIRED"
                if late_fill or leg_target == "RECONCILIATION_REQUIRED":
                    session_target = "RECONCILIATION_REQUIRED"
                elif old_session_state in {"CREATED", "VALIDATING", "REJECTED"}:
                    raise FillValidationError(f"fill is not valid for session state {old_session_state}")

                base_per_unit = _decimal(leg["base_per_unit"], "leg.base_per_unit")
                new_leg_qty = Decimal(str(leg["confirmed_filled_quantity"])) + facts["quantity"]
                new_leg_quote = Decimal(str(leg["confirmed_quote_quantity"])) + facts["quote_quantity"]
                new_price_quantity = Decimal(str(leg["confirmed_price_quantity"])) + facts["quantity"] * facts["price"]
                new_exposure_quantity = Decimal(str(leg["confirmed_exposure_quantity"])) + facts["quantity"] * base_per_unit
                new_average = new_price_quantity / new_leg_qty
                new_leg_version = int(leg["state_version"]) + 1
                new_aggregate = int(session["aggregate_version"]) + 1
                new_session_version = int(session["state_version"]) + (
                    1 if session_target != old_session_state else 0
                )
                await conn.execute(
                    """
                    UPDATE public.execution_legs
                    SET status=$3, state_version=$4,
                        confirmed_filled_quantity=$5, confirmed_quote_quantity=$6,
                        confirmed_price_quantity=$7, confirmed_exposure_quantity=$8,
                        average_fill_price=$9, last_fill_at=$10, updated_at=clock_timestamp()
                    WHERE session_id=$1 AND leg_id=$2 AND state_version=$11
                    """,
                    facts["session_id"], facts["leg_id"], leg_target, new_leg_version,
                    new_leg_qty, new_leg_quote, new_price_quantity,
                    new_exposure_quantity, new_average, facts["filled_at"],
                    int(leg["state_version"]),
                )
                await conn.execute(
                    """
                    UPDATE public.execution_sessions
                    SET status=$2, state_version=$3, aggregate_version=$4,
                        updated_at=clock_timestamp()
                    WHERE session_id=$1 AND aggregate_version=$5
                      AND state_version=$6 AND fencing_generation=$7
                    """,
                    facts["session_id"], session_target, new_session_version, new_aggregate,
                    int(session["aggregate_version"]), int(session["state_version"]),
                    int(fencing_generation),
                )
                await insert_session_event(
                    conn, session_id=facts["session_id"], aggregate_version=new_aggregate,
                    entity_type="LEG", entity_id=facts["leg_id"],
                    entity_version=new_leg_version,
                    event_type=("LEG_LATE_FILL_REQUIRES_RECONCILIATION" if late_fill
                                else "LEG_FILL_RECORDED"),
                    idempotency_key=idem, actor_id=actor,
                    fencing_generation=int(fencing_generation),
                    reason=("late venue fill after terminal session state" if late_fill else ""),
                    correlation_id=source_event_id or facts["exchange_trade_id"],
                    payload={
                        "fill_id": str(inserted["fill_id"]),
                        "exchange_trade_id": facts["exchange_trade_id"],
                        "quantity": str(facts["quantity"]),
                        "price": str(facts["price"]),
                        "quote_quantity": str(facts["quote_quantity"]),
                        "leg_status": leg_target,
                        "session_status": session_target,
                    },
                )
        return {
            "duplicate": False, "fill_id": str(inserted["fill_id"]),
            "leg_state": leg_target, "leg_state_version": new_leg_version,
            "session_state": session_target, "session_state_version": new_session_version,
            "aggregate_version": new_aggregate,
            "confirmed_filled_quantity": str(new_leg_qty),
            "confirmed_quote_quantity": str(new_leg_quote),
            "confirmed_exposure_quantity": str(new_exposure_quantity),
            "average_fill_price": str(new_average),
        }
