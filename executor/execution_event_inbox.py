"""Durable private-stream/REST event inbox for Executor V3."""
from __future__ import annotations

import json
from typing import Any, Mapping

from executor.execution_fill_service import FillConflict, FillValidationError


class ExecutionEventInboxService:
    """Persist normalized private events before their fill/order processors run."""

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

