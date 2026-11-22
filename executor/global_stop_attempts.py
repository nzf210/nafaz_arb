"""Durable submit-attempt lifecycle and operator reconciliation methods."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Optional

from executor.global_stop_common import GlobalStopError, logger, _json_mapping


class GlobalStopAttemptMixin:
    async def mark_submit_acknowledged(
        self, client_order_id: str, order: dict
    ) -> None:
        """Persist a positive venue acknowledgement; failure leaves SUBMITTING durable."""
        self._require_pool()
        order_id = str((order or {}).get("id") or "")
        if not order_id:
            raise GlobalStopError("cannot acknowledge submit without a venue order id")
        await self._update_submit_attempt(
            client_order_id, "ACKNOWLEDGED", exchange_order_id=order_id,
            venue_response=order, error=None,
        )

    async def mark_submit_rejected(self, client_order_id: str, error: str) -> None:
        """Resolve only a definitive venue rejection, never a transport timeout."""
        await self._update_submit_attempt(
            client_order_id, "REJECTED", exchange_order_id=None,
            venue_response=None, error=str(error)[:2000],
        )

    async def mark_submit_aborted(self, client_order_id: str, reason: str) -> None:
        """Retire an intent proven not to have crossed the venue HTTP boundary."""
        await self._update_submit_attempt(
            client_order_id, "ABORTED", exchange_order_id=None,
            venue_response=None, error=str(reason)[:2000],
        )

    async def mark_submit_unknown(self, client_order_id: str, error: str) -> None:
        """Annotate an ambiguous attempt; if DB is down SUBMITTING remains blocking."""
        await self._update_submit_attempt(
            client_order_id, "UNKNOWN", exchange_order_id=None,
            venue_response=None, error=str(error)[:2000],
        )

    async def _update_submit_attempt(
        self, client_order_id: str, status: str, *, exchange_order_id: Optional[str],
        venue_response: Optional[dict], error: Optional[str],
    ) -> None:
        self._require_pool()
        if status not in {"ACKNOWLEDGED", "REJECTED", "UNKNOWN", "ABORTED"}:
            raise ValueError("invalid submit-attempt transition")
        # ABORTED is proof that the venue boundary was never crossed. It may
        # retire PREPARED only; it must never clear a SUBMITTING/UNKNOWN attempt.
        allowed_from = (
            "status = 'PREPARED'"
            if status == "ABORTED"
            else "status in ('PREPARED','SUBMITTING','UNKNOWN')"
        )
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "update public.executor_submit_attempts set status=$2, "
                "exchange_order_id=coalesce($3, exchange_order_id), "
                "venue_response=case when $4::jsonb is null then venue_response else $4::jsonb end, "
                "last_error=$5, resolved_at=case when $2 in ('ACKNOWLEDGED','REJECTED','ABORTED') then now() else null end, "
                f"updated_at=now() where client_order_id=$1 and {allowed_from}",
                client_order_id, status, exchange_order_id,
                json.dumps(venue_response, separators=(",", ":"), default=str) if venue_response is not None else None,
                error,
            )
            if result.endswith(" 0"):
                raise GlobalStopError(
                    f"submit attempt transition rejected for {client_order_id} (current state may have changed)"
                )

    async def list_unresolved_submit_attempts(self, *, limit: int = 100) -> list:
        """Return attempts whose venue outcome is not yet proven."""
        self._require_pool()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "select client_order_id, exchange, market_type, symbol, side, order_type, "
                "quantity, price, pair_key, amount_spec, recovery_context, status, "
                "submit_started_at, last_error from public.executor_submit_attempts "
                "where status in ('SUBMITTING','UNKNOWN') order by submit_started_at limit $1",
                max(1, min(int(limit), 1000)),
            )
        return [dict(row) for row in rows]

    async def reconcile_submit_attempt_found(
        self, client_order_id: str, order: dict
    ) -> None:
        """Adopt an order found by exact venue client-order-id lookup."""
        await self.mark_submit_acknowledged(client_order_id, order)

    async def reconcile_submit_attempt_not_found(
        self, client_order_id: str, *, evidence: str
    ) -> None:
        """Record a negative lookup but deliberately keep the attempt UNKNOWN.

        Absence from an eventually consistent lookup endpoint is not proof that
        a create-order request was rejected. Only an authoritative venue
        rejection or an operator-approved resolution may release the global gate.
        """
        self._require_pool()
        async with self.pool.acquire() as conn:
            await conn.execute(
                "update public.executor_submit_attempts set last_lookup_at=now(), "
                "last_lookup_result='NOT_FOUND', last_error=$2, updated_at=now() "
                "where client_order_id=$1 and status in ('SUBMITTING','UNKNOWN')",
                client_order_id, str(evidence)[:2000],
            )


    async def resolve_submit_attempt_signed(
        self, envelope: dict, *, secret: str, max_age_seconds: int = 300
    ) -> dict:
        """Apply an operator-authenticated resolution with replay protection.

        The operator signs the canonical JSON payload with the out-of-band
        HMAC key. A resolution always requires two independent evidence
        references; a venue lookup returning NOT_FOUND is never sufficient.
        """
        self._require_pool()
        if not isinstance(envelope, dict) or not isinstance(envelope.get("payload"), dict):
            raise GlobalStopError("signed operator resolution payload is required")
        if not secret or len(secret.encode("utf-8")) < 32:
            raise GlobalStopError("OPERATOR_RESOLUTION_HMAC_SECRET must be at least 32 bytes")
        payload = envelope["payload"]
        signature = str(envelope.get("signature") or "").lower()
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        expected = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise GlobalStopError("operator resolution signature is invalid")
        try:
            issued_at = int(payload["issued_at"])
        except (KeyError, TypeError, ValueError) as exc:
            raise GlobalStopError("operator resolution issued_at is invalid") from exc
        if abs(int(time.time()) - issued_at) > max(1, min(int(max_age_seconds), 600)):
            raise GlobalStopError("operator resolution signature is expired or not yet valid")

        client_id = str(payload.get("client_order_id") or "").strip()
        operator = str(payload.get("operator_id") or "").strip()
        nonce = str(payload.get("nonce") or "").strip()
        decision = str(payload.get("decision") or "").strip().upper()
        evidence_a = str(payload.get("venue_evidence_ref") or "").strip()
        evidence_b = str(payload.get("independent_evidence_ref") or "").strip()
        order_id = str(payload.get("exchange_order_id") or "").strip()
        rationale = str(payload.get("rationale") or "").strip()
        if not all((client_id, operator, nonce, evidence_a, evidence_b, rationale)):
            raise GlobalStopError("operator, attempt, nonce, two evidence references and rationale are required")
        if evidence_a == evidence_b:
            raise GlobalStopError("venue and independent evidence must be distinct")
        if len(nonce) > 128 or len(operator) > 160 or len(evidence_a) > 1000 or len(evidence_b) > 1000:
            raise GlobalStopError("operator resolution fields exceed their limits")
        decisions = {
            "FOUND": ("ACKNOWLEDGED", order_id),
            "DEFINITIVE_REJECTION": ("REJECTED", ""),
            "PROVEN_NOT_SUBMITTED": ("ABORTED", ""),
        }
        if decision not in decisions:
            raise GlobalStopError("decision must be FOUND, DEFINITIVE_REJECTION, or PROVEN_NOT_SUBMITTED")
        target_status, _ = decisions[decision]
        if decision == "FOUND" and not order_id:
            raise GlobalStopError("FOUND resolution requires the exact venue order id")
        payload_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        venue_response = None
        if decision == "FOUND":
            venue_response = {
                "id": order_id, "manual_resolution": True,
                "venue_evidence_ref": evidence_a,
                "independent_evidence_ref": evidence_b,
            }

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("select pg_advisory_xact_lock($1)", FENCE_LOCK_KEY)
                row = await conn.fetchrow(
                    "select status from public.executor_submit_attempts "
                    "where client_order_id=$1 for update", client_id,
                )
                if row is None or row["status"] not in ("SUBMITTING", "UNKNOWN"):
                    raise GlobalStopError("attempt is missing or no longer unresolved")
                await conn.execute(
                    "insert into public.executor_submit_resolution_audit "
                    "(client_order_id, operator_id, decision, venue_evidence_ref, "
                    "independent_evidence_ref, rationale, nonce, signed_payload_sha256) "
                    "values ($1,$2,$3,$4,$5,$6,$7,$8)",
                    client_id, operator, decision, evidence_a, evidence_b,
                    rationale[:2000], nonce, payload_hash,
                )
                result = await conn.execute(
                    "update public.executor_submit_attempts set status=$2, "
                    "exchange_order_id=case when $3='' then exchange_order_id else $3 end, "
                    "venue_response=case when $4::jsonb is null then venue_response else $4::jsonb end, "
                    "last_error=$5, resolved_at=now(), updated_at=now() "
                    "where client_order_id=$1 and status in ('SUBMITTING','UNKNOWN')",
                    client_id, target_status, order_id,
                    json.dumps(venue_response, separators=(",", ":")) if venue_response else None,
                    ("operator resolution: " + rationale)[:2000],
                )
                if result.endswith(" 0"):
                    raise GlobalStopError("attempt changed during operator resolution")
        return {
            "client_order_id": client_id, "status": target_status,
            "operator_id": operator, "decision": decision,
            "signed_payload_sha256": payload_hash,
        }

