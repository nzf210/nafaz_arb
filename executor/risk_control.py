"""Durable hierarchical risk controls for Executor V3.

ENTRY_BLOCKED and DE_RISK_ALLOWED are separate permissions. A risk halt may
reject new exposure while still allowing a verified protective hedge or exit.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from executor.risk_budget import RiskBudgetEvaluator, RiskBudgetViolation


class RiskControlError(RuntimeError):
    """Base risk-control error."""


class RiskPermissionDenied(RiskControlError):
    """A requested mutation is not permitted by durable risk controls."""


class RiskControlConflict(RiskControlError):
    """A stale version or reused idempotency key conflicts with stored state."""


def risk_scope_keys(
    *, account_scope_id: str, venue: str, market_type: str,
    instrument: str, pair_key: str = "", session_id: str = "",
) -> list[str]:
    account = str(account_scope_id or "").strip()
    exchange = str(venue or "").strip().lower()
    market = str(market_type or "").strip().lower()
    symbol = str(instrument or "").strip()
    keys = ["global"]
    if account:
        keys.append("account:" + account)
    if account and exchange:
        keys.append(f"venue:{account}:{exchange}")
    if account and exchange and market and symbol:
        keys.append(f"instrument:{account}:{exchange}:{market}:{symbol}")
    if pair_key:
        keys.append("pair:" + str(pair_key).strip())
    if session_id:
        keys.append("session:" + str(session_id).strip())
    return keys


class RiskControlPlane:
    """PostgreSQL control projection and append-only control event journal."""

    @staticmethod
    async def assert_allowed(
        conn: Any, *, scope_keys: list[str], action_class: str,
        risk_metrics: Optional[Mapping[str, Any]] = None,
        risk_limits: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        action = str(action_class or "ENTRY").strip().upper()
        if action not in {"ENTRY", "HEDGE", "DERISK"}:
            raise ValueError("action_class must be ENTRY, HEDGE or DERISK")
        rows = await conn.fetch(
            """
            SELECT scope_key, entry_blocked, derisk_allowed, version, limits
            FROM public.executor_risk_controls
            WHERE scope_key = ANY($1::text[])
            """,
            list(dict.fromkeys(scope_keys)),
        )
        blocked = []
        derisk_denied = []
        snapshot = []
        for row in rows:
            entry_blocked = bool(row["entry_blocked"])
            derisk_allowed = bool(row["derisk_allowed"])
            scope_key = str(row["scope_key"])
            raw_limits = row["limits"]
            if isinstance(raw_limits, str):
                try:
                    raw_limits = json.loads(raw_limits)
                except (TypeError, ValueError):
                    raw_limits = {}
            scope_limits = dict(risk_limits or {})
            if isinstance(raw_limits, Mapping):
                scope_limits.update(dict(raw_limits))
            try:
                budget = RiskBudgetEvaluator.evaluate(
                    metrics=risk_metrics, limits=scope_limits, action_class=action
                )
            except RiskBudgetViolation as budget_error:
                raise RiskPermissionDenied(
                    f"RISK_BUDGET_BLOCKED at {scope_key}: {budget_error}"
                ) from budget_error
            snapshot.append({
                "scope_key": scope_key,
                "version": int(row["version"]),
                "entry_blocked": entry_blocked,
                "derisk_allowed": derisk_allowed,
                "risk_budget": budget,
            })
            if action == "ENTRY" and entry_blocked:
                blocked.append(scope_key)
            if action in {"HEDGE", "DERISK"} and not derisk_allowed:
                derisk_denied.append(scope_key)
        if not rows:
            try:
                budget = RiskBudgetEvaluator.evaluate(
                    metrics=risk_metrics, limits=risk_limits, action_class=action
                )
            except RiskBudgetViolation as budget_error:
                raise RiskPermissionDenied(f"RISK_BUDGET_BLOCKED: {budget_error}") from budget_error
            snapshot.append({
                "scope_key": "engine-default",
                "version": 0,
                "entry_blocked": False,
                "derisk_allowed": True,
                "risk_budget": budget,
            })
        if blocked:
            raise RiskPermissionDenied(
                "ENTRY_BLOCKED by durable risk scopes: " + ", ".join(sorted(blocked))
            )
        if derisk_denied:
            raise RiskPermissionDenied(
                "DE_RISK_NOT_ALLOWED by durable risk scopes: " + ", ".join(sorted(derisk_denied))
            )
        return {
            "action_class": action,
            "controls": sorted(snapshot, key=lambda item: item["scope_key"]),
        }

    async def set_control(
        self, pool: Any, *, scope_key: str, scope_type: str,
        entry_blocked: bool, derisk_allowed: bool,
        actor_id: str, event_type: str, event_idempotency_key: str,
        expected_version: Optional[int] = None,
        account_scope_id: Optional[str] = None, venue: Optional[str] = None,
        market_type: Optional[str] = None, instrument: Optional[str] = None,
        pair_key: Optional[str] = None, session_id: Optional[str] = None,
        limits: Optional[Mapping[str, Any]] = None,
        state: Optional[Mapping[str, Any]] = None,
    ) -> dict:
        key = str(scope_key or "").strip()
        actor = str(actor_id or "").strip()
        event_key = str(event_idempotency_key or "").strip()
        kind = str(scope_type or "").strip().upper()
        event_name = str(event_type or "").strip()
        if not key or not actor or not event_key or not kind or not event_name:
            raise ValueError(
                "scope_key, scope_type, event_type, actor_id and event_idempotency_key are required"
            )
        payload = {
            "entry_blocked": bool(entry_blocked),
            "derisk_allowed": bool(derisk_allowed),
            "limits": dict(limits or {}),
            "state": dict(state or {}),
        }
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "select pg_advisory_xact_lock(hashtextextended('nafaz:risk-control:' || $1, 0))",
                    key,
                )
                prior_event = await conn.fetchrow(
                    """
                    SELECT version, event_type, entry_blocked, derisk_allowed, payload
                    FROM public.executor_risk_control_events
                    WHERE scope_key=$1 AND event_idempotency_key=$2
                    """,
                    key, event_key,
                )
                if prior_event is not None:
                    prior_payload = prior_event["payload"]
                    if isinstance(prior_payload, str):
                        prior_payload = json.loads(prior_payload)
                    if (
                        str(prior_event["event_type"]) != event_name
                        or bool(prior_event["entry_blocked"]) != bool(entry_blocked)
                        or bool(prior_event["derisk_allowed"]) != bool(derisk_allowed)
                        or prior_payload != payload
                    ):
                        raise RiskControlConflict("risk control idempotency key was reused with different content")
                    return {
                        "scope_key": key, "version": int(prior_event["version"]),
                        "duplicate": True,
                    }

                current = await conn.fetchrow(
                    "SELECT version FROM public.executor_risk_controls WHERE scope_key=$1 FOR UPDATE",
                    key,
                )
                current_version = int(current["version"]) if current else 0
                if expected_version is not None and current_version != int(expected_version):
                    raise RiskControlConflict(
                        f"risk control version changed: expected {expected_version}, actual {current_version}"
                    )
                next_version = current_version + 1
                await conn.execute(
                    """
                    INSERT INTO public.executor_risk_controls (
                        scope_key, scope_type, account_scope_id, venue, market_type,
                        instrument, pair_key, session_id, entry_blocked,
                        derisk_allowed, limits, state, version, updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12::jsonb,$13,now()
                    )
                    ON CONFLICT (scope_key) DO UPDATE SET
                        scope_type=EXCLUDED.scope_type,
                        account_scope_id=EXCLUDED.account_scope_id,
                        venue=EXCLUDED.venue,
                        market_type=EXCLUDED.market_type,
                        instrument=EXCLUDED.instrument,
                        pair_key=EXCLUDED.pair_key,
                        session_id=EXCLUDED.session_id,
                        entry_blocked=EXCLUDED.entry_blocked,
                        derisk_allowed=EXCLUDED.derisk_allowed,
                        limits=EXCLUDED.limits,
                        state=EXCLUDED.state,
                        version=EXCLUDED.version,
                        updated_at=now()
                    """,
                    key, kind, account_scope_id, venue, market_type, instrument,
                    pair_key, session_id, bool(entry_blocked), bool(derisk_allowed),
                    json.dumps(dict(limits or {}), separators=(",", ":"), default=str),
                    json.dumps(dict(state or {}), separators=(",", ":"), default=str),
                    next_version,
                )
                await conn.execute(
                    """
                    INSERT INTO public.executor_risk_control_events (
                        scope_key, version, event_type, event_idempotency_key,
                        actor_id, entry_blocked, derisk_allowed, payload
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    """,
                    key, next_version, event_name, event_key, actor,
                    bool(entry_blocked), bool(derisk_allowed), payload_json,
                )
        return {"scope_key": key, "version": next_version, "duplicate": False}
