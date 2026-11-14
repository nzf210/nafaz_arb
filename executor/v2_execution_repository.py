"""PostgreSQL persistence boundary for V2 execution intents and journal events.

This module is not wired into the legacy exchange submit path yet. Callers must
persist an intent before any external order request; journal and outbox rows
commit in the same PostgreSQL transaction. Redis is only a delivery transport.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Optional


class ExecutionRepositoryError(RuntimeError):
    """A durable execution write could not be completed safely."""


class ExecutionIdempotencyConflict(ExecutionRepositoryError):
    """An idempotency key or event key was reused for different content."""


@dataclass(frozen=True)
class ExecutionIntent:
    idempotency_key: str
    route_id: str
    venue_id: str
    account_id: str
    market_id: str
    canonical_symbol: str
    order_role: str
    side: str
    order_type: str
    client_order_id: str
    requested_qty: Decimal
    limit_price: Optional[Decimal] = None
    is_mock: bool = True


_ALLOWED_ROLES = {
    "ENTRY", "HEDGE", "EXIT_SPOT", "EXIT_FUTURES",
    "EMERGENCY", "FORCE_CLOSE", "ROLLBACK",
}
_ALLOWED_STATES = {
    "RECEIVED", "VALIDATING", "AUTHORIZED", "SUBMITTING", "ACKNOWLEDGED",
    "PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "CANCELED", "REJECTED",
    "UNKNOWN", "RECOVERY_REQUIRED", "CLOSED",
}
_ALLOWED_TRANSITIONS = {
    "RECEIVED": {"VALIDATING", "REJECTED"},
    "VALIDATING": {"AUTHORIZED", "REJECTED", "RECOVERY_REQUIRED"},
    "AUTHORIZED": {"SUBMITTING", "REJECTED"},
    "SUBMITTING": {"ACKNOWLEDGED", "UNKNOWN", "REJECTED", "RECOVERY_REQUIRED"},
    "ACKNOWLEDGED": {"ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "RECOVERY_REQUIRED"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "RECOVERY_REQUIRED"},
    "CANCEL_PENDING": {"CANCELED", "PARTIALLY_FILLED", "FILLED", "UNKNOWN", "RECOVERY_REQUIRED"},
    "UNKNOWN": {"ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "RECOVERY_REQUIRED"},
    "RECOVERY_REQUIRED": {"ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "CLOSED"},
    "FILLED": {"CLOSED"},
    "CANCELED": {"CLOSED"},
    "REJECTED": {"CLOSED"},
    "CLOSED": set(),
}
_SECRET_KEY = re.compile(
    r"(?:^|_)(?:api_key|api_secret|secret|password|passphrase|token|private_key|access_key)(?:$|_)",
    re.IGNORECASE,
)


def _required_text(value: Any, field: str, max_length: int = 160) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ValueError(f"{field} must be a non-empty string <= {max_length} characters")
    return value.strip()


def _financial_decimal(value: Any, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, (bool, float)) or value is None:
        raise ValueError(f"{field} must be Decimal, integer, or decimal string (not float)")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} must be a finite decimal") from None
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"{field} must be finite and {'positive' if positive else 'valid'}")
    return parsed


def _reject_secret_fields(value: Any, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip()
            if _SECRET_KEY.search(normalized):
                raise ValueError(f"{path} contains a forbidden secret-like field")
            _reject_secret_fields(child, f"{path}.{normalized}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secret_fields(child, f"{path}[{index}]")


def _json_payload(payload: Optional[Mapping[str, Any]]) -> str:
    data = dict(payload or {})
    _reject_secret_fields(data)
    def encode_value(value: Any) -> str:
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ValueError("non-finite decimal")
            return str(value)
        raise TypeError("unsupported journal payload value")

    try:
        encoded = json.dumps(
            data,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            default=encode_value,
        )
    except (TypeError, ValueError):
        raise ValueError("journal payload must be finite JSON data") from None
    if len(encoded.encode("utf-8")) > 16_384:
        raise ValueError("journal payload exceeds 16 KiB")
    return encoded


def _validate_intent(intent: ExecutionIntent) -> dict[str, Any]:
    if not isinstance(intent, ExecutionIntent):
        raise TypeError("intent must be an ExecutionIntent")
    idempotency_key = _required_text(intent.idempotency_key, "idempotency_key")
    route_id = _required_text(intent.route_id, "route_id", 64)
    venue_id = _required_text(intent.venue_id, "venue_id", 64)
    account_id = _required_text(intent.account_id, "account_id", 64)
    market_id = _required_text(intent.market_id, "market_id", 128)
    canonical_symbol = _required_text(intent.canonical_symbol, "canonical_symbol", 64)
    client_order_id = _required_text(intent.client_order_id, "client_order_id", 128)
    if intent.order_role not in _ALLOWED_ROLES:
        raise ValueError("order_role is unsupported")
    if intent.side not in {"buy", "sell"}:
        raise ValueError("side must be buy or sell")
    if intent.order_type not in {"limit", "market"}:
        raise ValueError("order_type must be limit or market")
    if not isinstance(intent.is_mock, bool):
        raise ValueError("is_mock must be a boolean")
    quantity = _financial_decimal(intent.requested_qty, "requested_qty", positive=True)
    limit_price = None
    if intent.limit_price is not None:
        limit_price = _financial_decimal(intent.limit_price, "limit_price", positive=True)
    if intent.order_type == "limit" and limit_price is None:
        raise ValueError("limit orders require a positive limit_price")
    return {
        "idempotency_key": idempotency_key,
        "route_id": route_id,
        "venue_id": venue_id,
        "account_id": account_id,
        "market_id": market_id,
        "canonical_symbol": canonical_symbol,
        "order_role": intent.order_role,
        "side": intent.side,
        "order_type": intent.order_type,
        "client_order_id": client_order_id,
        "requested_qty": quantity,
        "limit_price": limit_price,
        "is_mock": intent.is_mock,
    }


class PostgresExecutionRepository:
    """Atomic intent + journal + outbox repository using an asyncpg pool."""

    def __init__(self, pool: Any):
        self._pool = pool

    @staticmethod
    def _event_type(value: str) -> str:
        event_type = _required_text(value, "event_type", 100)
        if not re.fullmatch(r"[a-z][a-z0-9_.-]{1,99}", event_type):
            raise ValueError("event_type must be a lowercase event identifier")
        return event_type

    @staticmethod
    def _same_journal_event(
        existing: Mapping[str, Any],
        *,
        intent_id: str,
        event_type: str,
        from_state: Optional[str],
        to_state: Optional[str],
        payload: str,
        quantity: Optional[Decimal],
        price: Optional[Decimal],
        quote_amount: Optional[Decimal],
        fee_amount: Optional[Decimal],
        realized_pnl: Optional[Decimal],
        asset: Optional[str],
        exchange_event_at: Any,
    ) -> bool:
        """Require an event-key replay to match the complete persisted event."""
        if (
            str(existing["intent_id"]) != str(intent_id)
            or str(existing["event_type"]) != event_type
            or existing["from_state"] != from_state
            or existing["to_state"] != to_state
            or existing["asset"] != asset
            or existing["exchange_event_at"] != exchange_event_at
        ):
            return False
        try:
            stored_payload = existing["payload"]
            if isinstance(stored_payload, str):
                stored_payload = json.loads(stored_payload)
            if _json_payload(stored_payload) != payload:
                return False
            for field, expected in (
                ("quantity", quantity),
                ("price", price),
                ("quote_amount", quote_amount),
                ("fee_amount", fee_amount),
                ("realized_pnl", realized_pnl),
            ):
                actual = existing[field]
                if expected is None:
                    if actual is not None:
                        return False
                elif actual is None or Decimal(str(actual)) != expected:
                    return False
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return False
        return True

    async def _append_journal_and_outbox(
        self,
        connection: Any,
        *,
        event_key: str,
        intent_id: str,
        state_version: int,
        event_type: str,
        from_state: Optional[str],
        to_state: Optional[str],
        payload: str,
        quantity: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
        quote_amount: Optional[Decimal] = None,
        fee_amount: Optional[Decimal] = None,
        realized_pnl: Optional[Decimal] = None,
        asset: Optional[str] = None,
        exchange_event_at: Any = None,
    ) -> int:
        event_key = _required_text(event_key, "event_key", 200)
        journal = await connection.fetchrow(
            """
            insert into public.v2_execution_journal
                (event_key, intent_id, event_type, from_state, to_state,
                 quantity, price, quote_amount, fee_amount, realized_pnl,
                 asset, exchange_event_at, payload)
            values ($1, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
            on conflict (event_key) do nothing
            returning journal_id
            """,
            event_key, intent_id, event_type, from_state, to_state,
            quantity, price, quote_amount, fee_amount, realized_pnl,
            asset, exchange_event_at, payload,
        )
        if journal is None:
            existing = await connection.fetchrow(
                """
                select journal_id, intent_id::text, event_type, from_state, to_state,
                       quantity, price, quote_amount, fee_amount, realized_pnl,
                       asset, exchange_event_at, payload
                  from public.v2_execution_journal where event_key = $1
                """,
                event_key,
            )
            if existing is None or not self._same_journal_event(
                existing,
                intent_id=intent_id,
                event_type=event_type,
                from_state=from_state,
                to_state=to_state,
                payload=payload,
                quantity=quantity,
                price=price,
                quote_amount=quote_amount,
                fee_amount=fee_amount,
                realized_pnl=realized_pnl,
                asset=asset,
                exchange_event_at=exchange_event_at,
            ):
                raise ExecutionIdempotencyConflict("execution journal event key was reused for different event data")
            return int(existing["journal_id"])

        journal_id = int(journal["journal_id"])
        outbox_payload = json.dumps({
            "journal_id": journal_id,
            "intent_id": str(intent_id),
            "event_key": event_key,
            "event_type": event_type,
            "state_version": state_version,
        }, sort_keys=True, separators=(",", ":"))
        await connection.execute(
            """
            insert into public.outbox_events
                (event_key, aggregate_type, aggregate_version, event_type, payload)
            values ($1, 'execution', $2, $3, $4::jsonb)
            on conflict (event_key) do nothing
            """,
            f"execution:{event_key}", state_version, f"execution.{event_type}", outbox_payload,
        )
        return journal_id

    async def create_intent(
        self,
        intent: ExecutionIntent,
        *,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        """Persist an idempotent RECEIVED intent before an external submit."""
        values = _validate_intent(intent)
        encoded_payload = _json_payload(payload)
        event_key = f"intent:{values['idempotency_key']}:received"
        event_type = "intent.received"
        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    row = await connection.fetchrow(
                        """
                        insert into public.v2_execution_intents
                            (idempotency_key, route_id, venue_id, account_id, market_id,
                             canonical_symbol, order_role, side, order_type, client_order_id,
                             requested_qty, limit_price, is_mock)
                        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        on conflict (idempotency_key) do nothing
                        returning intent_id::text, state_version
                        """,
                        values["idempotency_key"], values["route_id"], values["venue_id"],
                        values["account_id"], values["market_id"], values["canonical_symbol"],
                        values["order_role"], values["side"], values["order_type"],
                        values["client_order_id"], values["requested_qty"], values["limit_price"],
                        values["is_mock"],
                    )
                    if row is None:
                        existing = await connection.fetchrow(
                            """
                            select intent_id::text, route_id, venue_id, account_id, market_id,
                                   canonical_symbol, order_role, side, order_type, client_order_id,
                                   requested_qty, limit_price, is_mock, state_version
                              from public.v2_execution_intents
                             where idempotency_key = $1
                            """,
                            values["idempotency_key"],
                        )
                        if existing is None or not self._same_intent(existing, values):
                            raise ExecutionIdempotencyConflict(
                                "execution idempotency key was reused for different intent data"
                            )
                        return {
                            "intent_id": str(existing["intent_id"]),
                            "created": False,
                            "state_version": int(existing["state_version"]),
                        }

                    intent_id = str(row["intent_id"])
                    state_version = int(row["state_version"])
                    journal_id = await self._append_journal_and_outbox(
                        connection,
                        event_key=event_key,
                        intent_id=intent_id,
                        state_version=state_version,
                        event_type=event_type,
                        from_state=None,
                        to_state="RECEIVED",
                        payload=encoded_payload,
                    )
                    return {
                        "intent_id": intent_id,
                        "created": True,
                        "state_version": state_version,
                        "journal_id": journal_id,
                    }
        except (ExecutionRepositoryError, ValueError, TypeError):
            raise
        except Exception:
            raise ExecutionRepositoryError(
                "PostgreSQL could not commit the execution intent, journal and outbox transaction"
            ) from None

    @staticmethod
    def _same_intent(existing: Mapping[str, Any], values: Mapping[str, Any]) -> bool:
        fields = (
            "route_id", "venue_id", "account_id", "market_id", "canonical_symbol",
            "order_role", "side", "order_type", "client_order_id", "is_mock",
        )
        if any(existing[field] != values[field] for field in fields):
            return False
        try:
            return (
                Decimal(str(existing["requested_qty"])) == values["requested_qty"]
                and (
                    None if existing["limit_price"] is None
                    else Decimal(str(existing["limit_price"]))
                ) == values["limit_price"]
            )
        except (InvalidOperation, TypeError):
            return False

    async def transition_intent(
        self,
        intent_id: str,
        *,
        expected_state: str,
        next_state: str,
        event_key: str,
        event_type: str,
        payload: Optional[Mapping[str, Any]] = None,
        quantity: Any = None,
        price: Any = None,
        quote_amount: Any = None,
        fee_amount: Any = None,
        realized_pnl: Any = None,
        asset: Optional[str] = None,
        exchange_event_at: Any = None,
    ) -> dict[str, Any]:
        """Compare-and-swap one state transition with journal/outbox in one transaction."""
        intent_id = _required_text(intent_id, "intent_id", 64)
        event_key = _required_text(event_key, "event_key", 200)
        event_type = self._event_type(event_type)
        if expected_state not in _ALLOWED_STATES or next_state not in _ALLOWED_STATES:
            raise ValueError("intent state is unsupported")
        if next_state not in _ALLOWED_TRANSITIONS.get(expected_state, set()):
            raise ValueError(f"invalid execution state transition: {expected_state} -> {next_state}")
        encoded_payload = _json_payload(payload)

        def optional_decimal(value: Any, field: str, *, non_negative: bool = True):
            if value is None:
                return None
            parsed = _financial_decimal(value, field)
            if non_negative and parsed < 0:
                raise ValueError(f"{field} cannot be negative")
            return parsed

        financials = {
            "quantity": optional_decimal(quantity, "quantity"),
            "price": optional_decimal(price, "price"),
            "quote_amount": optional_decimal(quote_amount, "quote_amount"),
            "fee_amount": optional_decimal(fee_amount, "fee_amount"),
            "realized_pnl": optional_decimal(realized_pnl, "realized_pnl", non_negative=False),
        }
        if asset is not None:
            asset = _required_text(asset, "asset", 24)
            if not re.fullmatch(r"[A-Z0-9]{2,24}", asset):
                raise ValueError("asset must be an uppercase asset code")

        try:
            async with self._pool.acquire() as connection:
                async with connection.transaction():
                    prior = await connection.fetchrow(
                        """
                        select journal_id, intent_id::text, event_type, from_state, to_state,
                               quantity, price, quote_amount, fee_amount, realized_pnl,
                               asset, exchange_event_at, payload
                          from public.v2_execution_journal where event_key = $1
                        """,
                        event_key,
                    )
                    if prior is not None:
                        if not self._same_journal_event(
                            prior,
                            intent_id=intent_id,
                            event_type=event_type,
                            from_state=expected_state,
                            to_state=next_state,
                            payload=encoded_payload,
                            quantity=financials["quantity"],
                            price=financials["price"],
                            quote_amount=financials["quote_amount"],
                            fee_amount=financials["fee_amount"],
                            realized_pnl=financials["realized_pnl"],
                            asset=asset,
                            exchange_event_at=exchange_event_at,
                        ):
                            raise ExecutionIdempotencyConflict(
                                "execution event key was reused for different event data"
                            )
                        return {
                            "intent_id": intent_id,
                            "duplicate": True,
                            "journal_id": int(prior["journal_id"]),
                        }

                    current = await connection.fetchrow(
                        """
                        select state, state_version
                          from public.v2_execution_intents
                         where intent_id = $1::uuid
                         for update
                        """,
                        intent_id,
                    )
                    if current is None:
                        raise ExecutionRepositoryError("execution intent does not exist")
                    if current["state"] != expected_state:
                        raise ExecutionRepositoryError(
                            f"execution state conflict: expected {expected_state}, found {current['state']}"
                        )
                    next_version = int(current["state_version"]) + 1
                    updated = await connection.fetchrow(
                        """
                        update public.v2_execution_intents
                           set state = $2, state_version = $3, updated_at = now()
                         where intent_id = $1::uuid
                           and state = $4 and state_version = $5
                        returning state_version
                        """,
                        intent_id, next_state, next_version, expected_state,
                        int(current["state_version"]),
                    )
                    if updated is None:
                        raise ExecutionRepositoryError("execution state changed concurrently")
                    journal_id = await self._append_journal_and_outbox(
                        connection,
                        event_key=event_key,
                        intent_id=intent_id,
                        state_version=next_version,
                        event_type=event_type,
                        from_state=expected_state,
                        to_state=next_state,
                        payload=encoded_payload,
                        quantity=financials["quantity"],
                        price=financials["price"],
                        quote_amount=financials["quote_amount"],
                        fee_amount=financials["fee_amount"],
                        realized_pnl=financials["realized_pnl"],
                        asset=asset,
                        exchange_event_at=exchange_event_at,
                    )
                    return {
                        "intent_id": intent_id,
                        "duplicate": False,
                        "state_version": next_version,
                        "journal_id": journal_id,
                    }
        except (ExecutionRepositoryError, ValueError, TypeError):
            raise
        except Exception:
            raise ExecutionRepositoryError(
                "PostgreSQL could not commit the execution transition, journal and outbox transaction"
            ) from None
