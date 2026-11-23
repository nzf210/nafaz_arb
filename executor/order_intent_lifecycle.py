"""Fenced lifecycle transitions for durable mutation intents."""
from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, Mapping

from executor.ownership import build_scope_key
from executor.order_intent_common import (
    OrderIntentError, OrderIntentConflict, OrderIntentStaleOwner, required,
    decode_intent,
)


class OrderIntentLifecycleMixin:
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
            or str(row["owner_id"] or "") != owner_id
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
        risk_snapshot: Mapping[str, Any] | None = None,
    ) -> dict:
        actor = required(actor_id, "actor_id")
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
                owner_id = str(row["owner_id"] or "")
                current = str(row["status"]).upper()
                attempt_count = int(row["attempt_count"])
                if target_status == "SUBMITTING":
                    attempt_count += 1
                if require_lease:
                    scope_key, generation = await self._validate_mutation_lease(
                        conn, row, lease,
                        require_persisted_owner=(target_status != "SUBMITTING" or current != "PREPARED"),
                    )
                event_key = str(
                    payload.get("event_idempotency_key")
                    or f"{intent_id}:{target_status.lower()}:{attempt_count}"
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
                    stored_payload = event["payload"]
                    if isinstance(stored_payload, str):
                        stored_payload = json.loads(stored_payload)
                    if any(stored_payload.get(key) != value for key, value in payload.items()):
                        raise OrderIntentConflict("event idempotency key was replayed with different evidence")
                    return decode_intent(row)
                if current not in allowed_from:
                    raise OrderIntentConflict(
                        f"invalid intent transition {current} -> {target_status}"
                    )
                required_operation = (
                    "CANCEL" if target_status == "CANCEL_CONFIRMED"
                    else "CREATE" if target_status == "COMPLETED" else None
                )
                if required_operation and str(row["operation"]) != required_operation:
                    raise OrderIntentConflict(
                        f"{target_status} is invalid for {row['operation']} intent"
                    )
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
                        owner_id=COALESCE($6, owner_id),
                        owner_generation=COALESCE($7, owner_generation),
                        exchange_order_id=COALESCE($8, exchange_order_id),
                        venue_response=COALESCE($9::jsonb, venue_response),
                        last_error=$10, reserved_base_qty=$11, reserved_notional=$12,
                        risk_control_snapshot=CASE WHEN $13::jsonb IS NULL
                            THEN risk_control_snapshot ELSE $13::jsonb END,
                        resolved_at=CASE WHEN $2 IN (
                            'REJECTED','CANCEL_CONFIRMED','ABORTED','COMPLETED'
                        ) THEN clock_timestamp() ELSE NULL END,
                        updated_at=clock_timestamp()
                    WHERE intent_id=$1 AND state_version=$14
                    RETURNING *
                    """,
                    intent_id, target_status, next_version, attempt_count,
                    scope_key or None, str(getattr(lease, "owner_id", "") or "") or None,
                    generation or None, exchange_order_id,
                    json.dumps(dict(venue_response), separators=(",", ":"), default=str)
                    if venue_response is not None else None,
                    str(error)[:2000] if error else None,
                    reservation_base, reservation_notional,
                    json.dumps(dict(risk_snapshot), sort_keys=True, separators=(",", ":"), default=str)
                    if risk_snapshot is not None else None,
                    int(row["state_version"]),
                )
                if updated is None:
                    raise OrderIntentStaleOwner("mutation intent compare-and-swap lost")
                await conn.execute(
                    """
                    INSERT INTO public.executor_mutation_intent_events (
                        intent_id, event_version, event_type, event_idempotency_key,
                        actor_id, owner_scope_key, owner_id, owner_generation, payload
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                    """,
                    intent_id, next_version, event_type, event_key, actor,
                    scope_key or None, owner_id or str(getattr(lease, "owner_id", "") or "") or None,
                    generation or None,
                    json.dumps(body, sort_keys=True, separators=(",", ":"), default=str),
                )
        return decode_intent(updated)

    async def reclaim_for_reconciliation_in_transaction(
        self, conn: Any, intent_id: str, *, lease: Any, actor_id: str,
    ) -> dict:
        actor = required(actor_id, "actor_id")
        row = await conn.fetchrow(
            "SELECT * FROM public.executor_mutation_intents WHERE intent_id=$1 FOR UPDATE",
            intent_id,
        )
        if row is None:
            raise OrderIntentError("mutation intent does not exist")
        status = str(row["status"]).upper()
        if status not in {"SUBMITTING", "UNKNOWN", "ACKNOWLEDGED"}:
            raise OrderIntentConflict("only unresolved or acknowledged intents may be reclaimed")
        scope_key, generation = await self._validate_mutation_lease(
            conn, row, lease, require_persisted_owner=False,
        )
        previous_generation = int(row["owner_generation"] or 0)
        previous_owner = str(row["owner_id"] or "")
        owner_id = str(getattr(lease, "owner_id", "") or "")
        if not previous_generation or not previous_owner:
            raise OrderIntentStaleOwner("intent has no prior owner provenance")
        if generation < previous_generation:
            raise OrderIntentStaleOwner("recovery lease generation is stale")
        if generation == previous_generation:
            if owner_id != previous_owner:
                raise OrderIntentStaleOwner("owner changed without a generation increment")
            return decode_intent(row)
        event_key = f"{intent_id}:owner-reclaimed:{generation}"
        existing = await conn.fetchrow(
            """
            SELECT event_type, payload FROM public.executor_mutation_intent_events
            WHERE intent_id=$1 AND event_idempotency_key=$2
            """,
            intent_id, event_key,
        )
        if existing is not None:
            if str(existing["event_type"]) != "INTENT_OWNER_RECLAIMED":
                raise OrderIntentConflict("recovery event key was reused")
            return dict(row)
        next_version = int(row["state_version"]) + 1
        updated = await conn.fetchrow(
            """
            UPDATE public.executor_mutation_intents
            SET owner_scope_key=$2, owner_id=$3, owner_generation=$4,
                state_version=$5, updated_at=clock_timestamp()
            WHERE intent_id=$1 AND state_version=$6
            RETURNING *
            """,
            intent_id, scope_key, owner_id, generation,
            next_version, int(row["state_version"]),
        )
        if updated is None:
            raise OrderIntentStaleOwner("intent owner reclaim compare-and-swap lost")
        await conn.execute(
            """
            INSERT INTO public.executor_mutation_intent_events (
                intent_id, event_version, event_type, event_idempotency_key,
                actor_id, owner_scope_key, owner_id, owner_generation, payload
            ) VALUES ($1,$2,'INTENT_OWNER_RECLAIMED',$3,$4,$5,$6,$7,$8::jsonb)
            """,
            intent_id, next_version, event_key, actor, scope_key,
            owner_id, generation,
            json.dumps({
                "previous_owner_id": previous_owner,
                "previous_generation": previous_generation,
                "current_owner_id": owner_id,
                "current_generation": generation,
                "status": status,
            }, sort_keys=True, separators=(",", ":")),
        )
        return decode_intent(updated)

    async def reclaim_for_reconciliation(
        self, intent_id: str, *, lease: Any, actor_id: str,
    ) -> dict:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                return await self.reclaim_for_reconciliation_in_transaction(
                    conn, intent_id, lease=lease, actor_id=actor_id,
                )

    async def mark_submitting_in_transaction(
        self, conn: Any, intent_id: str, *, lease: Any,
        actor_id: str, risk_snapshot: Mapping[str, Any],
    ) -> dict:
        actor = required(actor_id, "actor_id")
        if not isinstance(risk_snapshot, Mapping) or not risk_snapshot:
            raise OrderIntentError("SUBMITTING requires a durable risk-control snapshot")
        row = await conn.fetchrow(
            "SELECT * FROM public.executor_mutation_intents WHERE intent_id=$1 FOR UPDATE",
            intent_id,
        )
        if row is None:
            raise OrderIntentError("mutation intent does not exist")
        current = str(row["status"]).upper()
        if current not in {"PREPARED", "SUBMITTING"}:
            raise OrderIntentConflict(f"invalid intent transition {current} -> SUBMITTING")
        scope_key, generation = await self._validate_mutation_lease(
            conn, row, lease, require_persisted_owner=current != "PREPARED",
        )
        attempt_count = int(row["attempt_count"])
        if current == "PREPARED":
            attempt_count += 1
        event_key = f"{intent_id}:submitting:{attempt_count}"
        event = await conn.fetchrow(
            """
            SELECT event_type, payload FROM public.executor_mutation_intent_events
            WHERE intent_id=$1 AND event_idempotency_key=$2
            """,
            intent_id, event_key,
        )
        event_payload = {"risk_control_snapshot": dict(risk_snapshot)}
        if event is not None:
            stored = event["payload"]
            if isinstance(stored, str):
                stored = json.loads(stored)
            if (
                str(event["event_type"]) != "INTENT_SUBMITTING"
                or stored.get("risk_control_snapshot") != dict(risk_snapshot)
            ):
                raise OrderIntentConflict("submit intent replay changed risk evidence")
            return dict(row)
        if current != "PREPARED":
            raise OrderIntentConflict("SUBMITTING intent has no matching durable submit event")
        next_version = int(row["state_version"]) + 1
        updated = await conn.fetchrow(
            """
            UPDATE public.executor_mutation_intents
            SET status='SUBMITTING', state_version=$2, attempt_count=$3,
                owner_scope_key=$4, owner_id=$5, owner_generation=$6,
                risk_control_snapshot=$7::jsonb, updated_at=clock_timestamp()
            WHERE intent_id=$1 AND state_version=$8
            RETURNING *
            """,
            intent_id, next_version, attempt_count, scope_key,
            lease.owner_id, generation,
            json.dumps(dict(risk_snapshot), sort_keys=True, separators=(",", ":"), default=str),
            int(row["state_version"]),
        )
        if updated is None:
            raise OrderIntentStaleOwner("mutation intent submit compare-and-swap lost")
        await conn.execute(
            """
            INSERT INTO public.executor_mutation_intent_events (
                intent_id, event_version, event_type, event_idempotency_key,
                actor_id, owner_scope_key, owner_id, owner_generation, payload
            ) VALUES ($1,$2,'INTENT_SUBMITTING',$3,$4,$5,$6,$7,$8::jsonb)
            """,
            intent_id, next_version, event_key, actor, scope_key,
            lease.owner_id, generation,
            json.dumps({
                **event_payload, "previous_status": current,
                "target_status": "SUBMITTING", "attempt_count": attempt_count,
            }, sort_keys=True, separators=(",", ":"), default=str),
        )
        return dict(updated)

    async def mark_submitting(
        self, intent_id: str, *, lease: Any, actor_id: str,
        risk_snapshot: Mapping[str, Any],
    ) -> dict:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                return await self.mark_submitting_in_transaction(
                    conn, intent_id, lease=lease, actor_id=actor_id,
                    risk_snapshot=risk_snapshot,
                )

    async def mark_acknowledged(
        self, intent_id: str, *, lease: Any, actor_id: str,
        exchange_order_id: str, venue_response: Mapping[str, Any],
        event_idempotency_key: str | None = None,
    ) -> dict:
        order_id = required(exchange_order_id, "exchange_order_id")
        return await self._transition(
            intent_id=intent_id, target_status="ACKNOWLEDGED",
            allowed_from=("SUBMITTING", "UNKNOWN"), event_type="INTENT_ACKNOWLEDGED",
            actor_id=actor_id,
            payload={
                "exchange_order_id": order_id,
                **({"event_idempotency_key": event_idempotency_key} if event_idempotency_key else {}),
            },
            lease=lease, exchange_order_id=order_id, venue_response=venue_response,
        )

    async def mark_unknown(
        self, intent_id: str, *, lease: Any, actor_id: str, error: str,
        event_idempotency_key: str | None = None,
    ) -> dict:
        return await self._transition(
            intent_id=intent_id, target_status="UNKNOWN",
            allowed_from=("SUBMITTING", "ACKNOWLEDGED"), event_type="INTENT_SUBMISSION_UNKNOWN",
            actor_id=actor_id,
            payload={
                "error": str(error)[:2000],
                **({"event_idempotency_key": event_idempotency_key} if event_idempotency_key else {}),
            },
            lease=lease, error=error,
        )

    async def mark_rejected(
        self, intent_id: str, *, lease: Any, actor_id: str,
        error: str, definitive: bool,
        event_idempotency_key: str | None = None,
    ) -> dict:
        if not definitive:
            raise OrderIntentError("only a definitive venue rejection may resolve an intent")
        return await self._transition(
            intent_id=intent_id, target_status="REJECTED",
            allowed_from=("SUBMITTING", "UNKNOWN", "ACKNOWLEDGED"), event_type="INTENT_REJECTED",
            actor_id=actor_id,
            payload={
                "error": str(error)[:2000],
                **({"event_idempotency_key": event_idempotency_key} if event_idempotency_key else {}),
            },
            lease=lease, error=error,
            release_reservation=True,
        )

    async def mark_aborted(
        self, intent_id: str, *, actor_id: str, reason: str,
    ) -> dict:
        return await self._transition(
            intent_id=intent_id, target_status="ABORTED",
            allowed_from=("PREPARED",), event_type="INTENT_ABORTED",
            actor_id=actor_id, payload={"reason": str(reason)[:2000]}, require_lease=False, error=reason,
            release_reservation=True,
        )

    async def mark_cancel_confirmed(
        self, intent_id: str, *, lease: Any, actor_id: str,
        final_status: str, fresh_fetch: bool, fills_reconciled: bool,
        exchange_order_id: str | None = None,
        venue_response: Mapping[str, Any] | None = None,
        event_idempotency_key: str | None = None,
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
            payload={
                "final_status": status, "fresh_fetch": True, "fills_reconciled": True,
                **({"event_idempotency_key": event_idempotency_key} if event_idempotency_key else {}),
            },
            lease=lease, release_reservation=True,
            exchange_order_id=exchange_order_id, venue_response=venue_response,
        )

    async def mark_completed(
        self, intent_id: str, *, lease: Any, actor_id: str,
        terminal_status: str, fills_reconciled: bool,
        event_idempotency_key: str | None = None,
    ) -> dict:
        status = str(terminal_status or "").strip().lower()
        if status not in {"canceled", "cancelled", "closed", "filled", "rejected", "expired"}:
            raise OrderIntentError("completion requires a verified terminal order status")
        if not fills_reconciled:
            raise OrderIntentError("completion requires reconciled fills")
        return await self._transition(
            intent_id=intent_id, target_status="COMPLETED",
            allowed_from=("ACKNOWLEDGED",), event_type="INTENT_COMPLETED",
            actor_id=actor_id,
            payload={
                "terminal_status": status, "fills_reconciled": True,
                **({"event_idempotency_key": event_idempotency_key} if event_idempotency_key else {}),
            },
            lease=lease, release_reservation=True,
        )


