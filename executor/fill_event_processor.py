"""Idempotent V3 fill event processing and hedge accounting orchestration."""
from __future__ import annotations

import inspect
import json
from typing import Any, Callable, Mapping

from executor.execution_fill_service import ExecutionFillService, FillValidationError
from executor.execution_hedge_common import HedgeObligationError
from executor.execution_hedge_obligations import ExecutionHedgeObligationService


class FillEventProcessor:
    """Turn a durable normalized fill inbox event into fill and hedge facts.

    The processor receives an already acquired session coordinator lease. It
    never resolves exchange clients and never submits/cancels orders.
    """

    def __init__(
        self, pool: Any, *,
        fill_service: ExecutionFillService | None = None,
        hedge_service: ExecutionHedgeObligationService | None = None,
        max_attempts: int = 12,
        processing_lease_seconds: int = 120,
    ) -> None:
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        if int(max_attempts) <= 0 or int(processing_lease_seconds) <= 0:
            raise ValueError("max_attempts and processing_lease_seconds must be positive")
        self.pool = pool
        self.fills = fill_service or ExecutionFillService(pool)
        self.hedges = hedge_service or ExecutionHedgeObligationService(pool)
        self.max_attempts = int(max_attempts)
        self.processing_lease_seconds = int(processing_lease_seconds)

    async def process_inbox_event(
        self, *, inbox_id: str, coordinator_lease: Any,
        fencing_generation: int, actor_id: str, hedge_ratio: Any = "1",
    ) -> dict:
        if coordinator_lease is None:
            raise FillValidationError("a current session coordinator lease is required")
        actor = str(actor_id or "").strip()
        if not actor:
            raise FillValidationError("actor_id is required")
        event = await self.fills.claim_inbox_event(
            inbox_id=inbox_id,
            processing_lease_seconds=self.processing_lease_seconds,
            max_attempts=self.max_attempts,
        )
        if event is None:
            return {"claimed": False, "inbox_id": inbox_id}

        try:
            result = await self._process_claimed(
                event=event, coordinator_lease=coordinator_lease,
                fencing_generation=fencing_generation, actor_id=actor,
                hedge_ratio=hedge_ratio,
            )
            await self.fills.mark_inbox_event(inbox_id=inbox_id, status="PROCESSED")
            return {"claimed": True, "processed": True, "inbox_id": inbox_id, **result}
        except Exception as exc:
            attempts = int(event["attempts"])
            terminal = attempts >= self.max_attempts
            status = "QUARANTINED" if terminal else "RETRY"
            delay = 0 if terminal else min(300, 2 ** max(0, attempts - 1))
            await self.fills.mark_inbox_event(
                inbox_id=inbox_id, status=status,
                error=f"{type(exc).__name__}: {exc}",
                retry_delay_seconds=delay,
            )
            return {
                "claimed": True, "processed": False,
                "quarantined": terminal, "retry_delay_seconds": delay,
                "inbox_id": inbox_id, "error": f"{type(exc).__name__}: {exc}",
            }

    async def process_batch(
        self, *, coordinator_lease_resolver: Callable[[str], Any],
        actor_id: str, limit: int = 100,
        hedge_ratio_resolver: Callable[[str], Any] | None = None,
    ) -> list[dict]:
        """Process a bounded due batch using caller-owned session lease resolution.

        Resolver result must be a mapping with \`lease\` and
        \`fencing_generation\`; a missing lease leaves the event untouched for
        another worker/owner to claim later.
        """
        if not callable(coordinator_lease_resolver):
            raise ValueError("coordinator_lease_resolver callback is required")
        due = await self.fills.list_due_inbox_events(
            limit=limit, processing_lease_seconds=self.processing_lease_seconds,
        )
        results: list[dict] = []
        for item in due:
            session_id = str(item.get("session_id") or "").strip()
            leg_id = str(item.get("leg_id") or "").strip()
            if not session_id or not leg_id:
                changed = await self.fills.mark_inbox_event(
                    inbox_id=str(item["inbox_id"]), status="QUARANTINED",
                    error="fill inbox event is missing session_id or leg_id",
                )
                results.append({
                    "inbox_id": str(item["inbox_id"]),
                    "quarantined": bool(changed),
                    "error": "fill inbox event is missing session_id or leg_id",
                })
                continue
            lease_context = coordinator_lease_resolver(session_id)
            if inspect.isawaitable(lease_context):
                lease_context = await lease_context
            if not isinstance(lease_context, Mapping):
                continue
            lease = lease_context.get("lease")
            generation = lease_context.get("fencing_generation")
            if lease is None or generation is None:
                continue
            ratio = "1"
            if hedge_ratio_resolver is not None:
                ratio = hedge_ratio_resolver(session_id)
                if inspect.isawaitable(ratio):
                    ratio = await ratio
            result = await self.process_inbox_event(
                inbox_id=str(item["inbox_id"]),
                coordinator_lease=lease,
                fencing_generation=int(generation),
                actor_id=actor_id,
                hedge_ratio=ratio,
            )
            results.append(result)
        return results

    async def _process_claimed(
        self, *, event: Mapping[str, Any], coordinator_lease: Any,
        fencing_generation: int, actor_id: str, hedge_ratio: Any,
    ) -> dict:
        event_type = str(event["event_type"] or "").upper()
        if event_type not in {"FILL", "TRADE", "MY_TRADE"}:
            raise FillValidationError(f"unsupported inbox event type {event_type}")
        session_id = str(event["session_id"] or "").strip()
        leg_id = str(event["leg_id"] or "").strip()
        if not session_id or not leg_id:
            raise FillValidationError("fill inbox event must be bound to a session and leg")
        payload = event["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, Mapping):
            raise FillValidationError("normalized fill payload must be a mapping")

        async with self.pool.acquire() as conn:
            route = await conn.fetchrow(
                """
                SELECT s.account_scope_id, s.aggregate_version,
                       s.fencing_generation, l.state_version,
                       l.leg_type, l.venue, l.market_type, l.symbol,
                       l.status AS leg_status
                FROM public.execution_sessions s
                JOIN public.execution_legs l
                  ON l.session_id=s.session_id AND l.leg_id=$2
                WHERE s.session_id=$1
                """,
                session_id, leg_id,
            )
        if route is None:
            raise FillValidationError("fill inbox session/leg route does not exist")
        if int(route["fencing_generation"]) != int(fencing_generation):
            raise FillValidationError("fill processor fencing generation is stale")
        if (
            str(route["account_scope_id"]) != str(event["account_scope_id"])
            or str(route["venue"]).lower() != str(event["venue"]).lower()
        ):
            raise FillValidationError("fill inbox account or venue does not match the durable session leg")

        trade_id = str(payload.get("exchange_trade_id") or payload.get("trade_id") or "").strip()
        if not trade_id:
            raise FillValidationError("normalized fill requires exchange_trade_id")
        fill = await self.fills.record_fill(
            session_id=session_id, leg_id=leg_id,
            account_scope_id=str(route["account_scope_id"]),
            venue=str(route["venue"]), market_type=str(route["market_type"]),
            symbol=str(route["symbol"]),
            exchange_trade_id=trade_id,
            side=payload.get("side"),
            quantity=payload.get("quantity"),
            price=payload.get("price"),
            quote_asset=payload.get("quote_asset"),
            quote_quantity=payload.get("quote_quantity"),
            filled_at=payload.get("filled_at"),
            exchange_order_id=payload.get("exchange_order_id"),
            client_order_id=payload.get("client_order_id"),
            fee_amount=payload.get("fee_amount", "0"),
            fee_asset=payload.get("fee_asset"),
            fee_verified=payload.get("fee_verified", False),
            fee_source=payload.get("fee_source", "UNVERIFIED"),
            source_event_id=str(event["source_event_id"]),
            raw_payload=payload,
            actor_id=actor_id,
            event_idempotency_key=f"fill-inbox:{event['inbox_id']}",
            expected_leg_state_version=int(route["state_version"]),
            expected_aggregate_version=int(route["aggregate_version"]),
            fencing_generation=int(fencing_generation),
            coordinator_lease=coordinator_lease,
        )
        fill_id = str(fill["fill_id"])
        leg_type = str(route["leg_type"]).upper()
        side = str(payload.get("side") or "").lower()

        if leg_type == "SPOT" and side == "buy":
            obligation = await self.hedges.create_from_spot_fill(
                session_id=session_id, spot_fill_id=fill_id,
                hedge_ratio=hedge_ratio, actor_id=actor_id,
                coordinator_lease=coordinator_lease,
                fencing_generation=fencing_generation,
            )
            return {"fill_id": fill_id, "duplicate_fill": bool(fill.get("duplicate")), "obligation": obligation}

        if leg_type == "FUTURES" and side == "sell":
            client_order_id = str(payload.get("client_order_id") or "").strip()
            if not client_order_id:
                raise FillValidationError("Futures hedge fill requires client_order_id for reservation matching")
            async with self.pool.acquire() as conn:
                reservation = await conn.fetchrow(
                    """
                    SELECT reservation_id
                    FROM public.execution_hedge_reservations
                    WHERE session_id=$1 AND client_order_id=$2
                    """,
                    session_id, client_order_id,
                )
            if reservation is None:
                raise HedgeObligationError("confirmed Futures fill has no matching durable hedge reservation")
            allocation = await self.hedges.allocate_futures_fill(
                session_id=session_id,
                reservation_id=str(reservation["reservation_id"]),
                futures_fill_id=fill_id,
                actor_id=actor_id,
                event_idempotency_key=f"hedge-fill-allocation:{fill_id}",
                coordinator_lease=coordinator_lease,
                fencing_generation=fencing_generation,
            )
            return {"fill_id": fill_id, "duplicate_fill": bool(fill.get("duplicate")), "allocation": allocation}

        raise FillValidationError(
            f"entry hedge processor does not handle {leg_type} {side}; exit fills require the exit lifecycle"
        )
