"""Durable, fenced persistence for pure hedge-liquidity decisions."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from executor.execution_fill_service import FillConflict
from executor.execution_hedge_common import HedgeObligationError, HedgeServiceBase, _text
from executor.hedge_liquidity_evaluator import HedgeLiquidityDecision


class HedgeScheduler(HedgeServiceBase):
    """Persists defer/eligible/de-risk decisions; never submits exchange orders."""

    async def record_decision(
        self, *, session_id: str, obligation_id: str,
        decision: HedgeLiquidityDecision, actor_id: str,
        idempotency_key: str, coordinator_lease: Any,
        fencing_generation: int,
    ) -> dict:
        actor = _text(actor_id, "actor_id")
        key = _text(idempotency_key, "idempotency_key")
        action = str(decision.action or "").upper()
        if action not in {"SUBMIT", "PARTIAL_SUBMIT", "DEFER", "DE_RISK_REQUIRED"}:
            raise ValueError("unsupported hedge liquidity action")
        payload = {
            "action": action,
            "reason": decision.reason,
            "executable_base_quantity": str(decision.executable_base_quantity),
            "executable_native_quantity": str(decision.executable_native_quantity),
            "executable_vwap": str(decision.executable_vwap) if decision.executable_vwap is not None else None,
            "executable_notional_quote": str(decision.executable_notional_quote),
            "executable_notional_usd": str(decision.executable_notional_usd),
            "unhedged_notional_usd": str(decision.unhedged_notional_usd),
            "slippage_bps": str(decision.slippage_bps) if decision.slippage_bps is not None else None,
            "estimated_fee_bps": str(decision.estimated_fee_bps),
            "estimated_total_cost_bps": str(decision.estimated_total_cost_bps) if decision.estimated_total_cost_bps is not None else None,
            "book_age_ms": decision.book_age_ms,
            "book_sequence": decision.book_sequence,
            "wait_elapsed_ms": decision.wait_elapsed_ms,
            "wait_remaining_ms": decision.wait_remaining_ms,
            "next_evaluate_at_ms": decision.next_evaluate_at_ms,
        }
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        book_at = datetime.fromtimestamp(
            decision.book_observed_at_ms / 1000, timezone.utc
        )
        next_at = (
            datetime.fromtimestamp(decision.next_evaluate_at_ms / 1000, timezone.utc)
            if action == "DEFER" and decision.next_evaluate_at_ms > 0
            else None
        )
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await self._lock_session(
                    conn, session_id=session_id, lease=coordinator_lease,
                    generation=fencing_generation,
                )
                obligation = await conn.fetchrow(
                    """
                    SELECT status, account_scope_id, target_base_quantity,
                           confirmed_base_quantity, reserved_base_quantity,
                           deferred_since, deferred_count, de_risk_required, created_at
                    FROM public.execution_hedge_obligations
                    WHERE obligation_id=$1::uuid AND session_id=$2
                    FOR UPDATE
                    """,
                    obligation_id, session_id,
                )
                if obligation is None:
                    raise HedgeObligationError("hedge obligation does not belong to session")
                if str(obligation["account_scope_id"]) != str(session["account_scope_id"]):
                    raise HedgeObligationError("hedge obligation account scope mismatch")
                prior = await conn.fetchrow(
                    """
                    SELECT action, reason, book_sequence, decision_payload,
                           scheduler_event_id
                    FROM public.execution_hedge_scheduler_events
                    WHERE obligation_id=$1::uuid AND idempotency_key=$2
                    """,
                    obligation_id, key,
                )
                if prior is not None:
                    prior_payload = (
                        json.loads(prior["decision_payload"])
                        if isinstance(prior["decision_payload"], str)
                        else prior["decision_payload"]
                    )
                    if prior_payload != payload:
                        raise FillConflict("hedge scheduler idempotency key was reused with changed decision")
                    return {
                        "duplicate": True,
                        "scheduler_event_id": str(prior["scheduler_event_id"]),
                        "action": str(prior["action"]),
                        "aggregate_version": int(session["aggregate_version"]),
                    }

                if bool(obligation["de_risk_required"]) or str(obligation["status"]) == "BLOCKED":
                    raise HedgeObligationError("blocked hedge obligation requires recovery/de-risk resolution")
                deferred_since = obligation["deferred_since"]
                max_wait_until = None
                if action == "DEFER":
                    if deferred_since is None:
                        deferred_since = obligation["created_at"]
                    max_wait_until = datetime.fromtimestamp(
                        decision.evaluated_at_ms / 1000 + decision.wait_remaining_ms / 1000,
                        timezone.utc,
                    )
                elif action in {"SUBMIT", "PARTIAL_SUBMIT"}:
                    deferred_since = None

                if action == "DE_RISK_REQUIRED":
                    new_status = "BLOCKED"
                    next_at = None
                else:
                    new_status = str(obligation["status"])

                updated = await conn.execute(
                    """
                    UPDATE public.execution_hedge_obligations
                    SET status=$2, deferred_reason=$3, deferred_since=$4,
                        next_evaluate_at=$5, max_wait_until=COALESCE(max_wait_until,$6),
                        last_evaluated_at=clock_timestamp(), last_book_at=$7,
                        last_book_sequence=$8,
                        deferred_count=deferred_count + CASE WHEN $3::text IS NOT NULL OR $9=TRUE THEN 1 ELSE 0 END,
                        de_risk_required=de_risk_required OR $9, updated_at=clock_timestamp()
                    WHERE obligation_id=$1::uuid
                    """,
                    obligation_id, new_status,
                    decision.reason if action in {"DEFER", "DE_RISK_REQUIRED"} else None,
                    deferred_since, next_at, max_wait_until, book_at,
                    decision.book_sequence or None,
                    action == "DE_RISK_REQUIRED",
                )
                if not updated.endswith(" 1"):
                    raise HedgeObligationError("hedge scheduler obligation update failed")

                event = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_hedge_scheduler_events (
                        session_id, obligation_id, action, reason, idempotency_key,
                        book_sequence, book_observed_at, book_age_ms,
                        executable_base_quantity, executable_vwap,
                        executable_notional_quote, executable_notional_usd,
                        slippage_bps, estimated_fee_bps, estimated_total_cost_bps,
                        unhedged_notional_usd, wait_elapsed_ms, wait_remaining_ms,
                        next_evaluate_at, decision_payload
                    ) VALUES (
                        $1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                        $15,$16,$17,$18,$19,$20::jsonb
                    )
                    RETURNING scheduler_event_id
                    """,
                    session_id, obligation_id, action, decision.reason, key,
                    decision.book_sequence or None, book_at, decision.book_age_ms,
                    decision.executable_base_quantity, decision.executable_vwap,
                    decision.executable_notional_quote, decision.executable_notional_usd,
                    decision.slippage_bps, decision.estimated_fee_bps,
                    decision.estimated_total_cost_bps,
                    decision.unhedged_notional_usd,
                    decision.wait_elapsed_ms, decision.wait_remaining_ms,
                    next_at, payload_json,
                )
                current_session_state = str(session["status"]).upper()
                if action == "DE_RISK_REQUIRED" and current_session_state in {
                    "ENTRY_IN_PROGRESS", "HEDGE_PENDING", "PARTIALLY_HEDGED",
                    "HEDGED", "BLOCKED",
                }:
                    target_session_state = "BLOCKED"
                else:
                    target_session_state = current_session_state
                event_type = {
                    "SUBMIT": "HEDGE_LIQUIDITY_ELIGIBLE",
                    "PARTIAL_SUBMIT": "HEDGE_LIQUIDITY_PARTIAL_ELIGIBLE",
                    "DEFER": "HEDGE_LIQUIDITY_DEFERRED",
                    "DE_RISK_REQUIRED": "HEDGE_WAIT_BUDGET_EXHAUSTED",
                }[action]
                version = await self._append_event(
                    conn, session_id=session_id, session=session,
                    event_type=event_type, idempotency_key=key,
                    actor_id=actor, generation=fencing_generation,
                    reason=decision.reason,
                    payload={
                        "scheduler_event_id": str(event["scheduler_event_id"]),
                        "obligation_id": obligation_id,
                        "session_state": target_session_state,
                        **payload,
                    },
                    target_state=target_session_state,
                )
                return {
                    "duplicate": False,
                    "scheduler_event_id": str(event["scheduler_event_id"]),
                    "action": action, "obligation_status": new_status,
                    "next_evaluate_at": next_at.isoformat() if next_at else None,
                    "aggregate_version": version,
                }
