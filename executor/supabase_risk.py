"""supabase risk operations for the durable Supabase writer."""

import json
import uuid
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

from executor.supabase_common import (
    _AUTHORIZATION_BEARER_PREFIX, _JSON_CONTENT_TYPE, _POSITION_NS,
    _TIMEOUT, _ms_to_iso, _num, logger,
)



class SupabaseRiskMixin:
    """Focused writer methods grouped by persistence responsibility."""

    def risk_state(
        self, *, state_type: str, scope: str, value: Any, reason: str = "",
        exchange: Optional[str] = None, pair_key: Optional[str] = None,
        resolved: bool = False, state_id: Optional[str] = None,
        is_mock: bool = True,
    ) -> bool:
        """Journal a mutable risk-state snapshot by stable identity.

        The SQLite outbox is the synchronous durability boundary; Supabase is
        the cross-process source of truth. Repeated writes upsert one state row.
        """
        identity = state_id or str(uuid.uuid5(
            _POSITION_NS,
            f"risk:{state_type}:{scope}:{(exchange or '').lower()}:{pair_key or ''}",
        ))
        now = datetime.now(timezone.utc).isoformat()
        return self._emit(
            "risk_state",
            {
                "state_id": identity,
                "state_type": str(state_type),
                "scope": str(scope),
                "exchange": str(exchange).lower() if exchange else None,
                "pair_key": pair_key,
                "reason": str(reason or ""),
                "value": value if isinstance(value, (dict, list, str, int, float, bool)) or value is None else str(value),
                "created_at": now,
                "updated_at": now,
                "resolved_at": now if resolved else None,
                "is_mock": bool(is_mock),
            },
            on_conflict="state_id",
        )

    def _merge_active_risk_state_rows(self, rows: list, is_mock: bool) -> list:
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise RuntimeError("risk state read returned malformed payload")
        merged = {
            str(row.get("state_id")): dict(row)
            for row in rows if row.get("state_id")
        }
        if self._outbox is not None:
            for _item_id, _table, _conflict, pending_row in self._outbox.pending_table(
                "risk_state"
            ):
                if bool(pending_row.get("is_mock", True)) != bool(is_mock):
                    continue
                state_id = str(pending_row.get("state_id") or "")
                if state_id:
                    merged[state_id] = dict(pending_row)
        return [
            row for row in merged.values()
            if row.get("resolved_at") is None
        ]

    def _merge_pending_realized_pnl_events(
        self, events: dict, loss_day: str, is_mock: bool
    ) -> dict:
        if self._outbox is None:
            return events
        for event_id in events:
            self._outbox.set_numeric_state(f"risk-pnl:{event_id}", 1.0)
        for _item_id, _table, _conflict, row in self._outbox.pending_table(
            "risk_pnl_events"
        ):
            if bool(row.get("is_mock", True)) != bool(is_mock):
                continue
            if (
                str(row.get("loss_day_utc") or "") == str(loss_day)
                and row.get("event_id")
            ):
                events[str(row["event_id"])] = float(row.get("pnl_usdt") or 0.0)
        return events

    async def fetch_active_risk_states(self, *, is_mock: bool = True) -> list:
        """Reads unresolved state from Supabase; errors are not treated as empty."""
        if not self.enabled or not self.url or not self.key:
            return []
        params = {
            "select": "state_id,state_type,scope,exchange,pair_key,reason,value,created_at,updated_at,resolved_at,is_mock",
            "resolved_at": "is.null",
            "is_mock": f"eq.{str(bool(is_mock)).lower()}",
            "order": "updated_at.asc",
            "limit": "5000",
        }
        session = self._session
        created = False
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=_TIMEOUT)
                created = True
            headers = {
                "apikey": self.key,
                "Authorization": _AUTHORIZATION_BEARER_PREFIX + self.key,
                "Accept": _JSON_CONTENT_TYPE,
                "Accept-Encoding": "identity",
            }
            async with session.get(
                f"{self.url}/rest/v1/risk_state", params=params, headers=headers
            ) as resp:
                if resp.status >= 300:
                    body = (await resp.text())[:500]
                    raise RuntimeError(f"risk state read failed with HTTP {resp.status}: {body}")
                rows = await resp.json()
            # Overlay the SQLite journal: queued halt/resume events must survive
            # a crash before their asynchronous Supabase upsert is delivered.
            return self._merge_active_risk_state_rows(rows, is_mock)
        except Exception as exc:
            logger.error("durable risk-state restoration failed: %s", exc)
            raise RuntimeError("durable risk state could not be restored") from exc
        finally:
            if created and session is not None:
                await session.close()


    def risk_pnl_event(self, event_id: str, *, pair_key: str, pnl_usdt: float, loss_day: str, is_mock: bool = True) -> str:
        """Idempotently journal a verified realized-PnL event.

        Returns inserted, duplicate, or failed. A duplicate event must never
        increment the daily loss accumulator a second time.
        """
        if not self.enabled or self._outbox is None:
            return "failed"
        try:
            inserted = self._outbox.enqueue_once(
                "risk_pnl_events", "event_id",
                {
                    "event_id": str(event_id),
                    "pair_key": str(pair_key),
                    "pnl_usdt": float(pnl_usdt),
                    "loss_day_utc": str(loss_day),
                    "is_mock": bool(is_mock),
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                },
                f"risk-pnl:{event_id}",
            )
            return "inserted" if inserted else "duplicate"
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: failed to journal realized PnL event %s", event_id)
            return "failed"


    async def fetch_daily_realized_loss_usdt(self, loss_day: str, *, is_mock: bool = True) -> float:
        """Rebuild the UTC loss accumulator from immutable, deduplicated PnL events."""
        if not self.enabled or not self.url or not self.key:
            raise RuntimeError("durable realized-PnL ledger is unavailable")
        params = {
            "select": "event_id,pnl_usdt",
            "loss_day_utc": f"eq.{loss_day}",
            "is_mock": f"eq.{str(bool(is_mock)).lower()}",
            "limit": "10000",
        }
        session = self._session
        created = False
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=_TIMEOUT)
                created = True
            headers = {
                "apikey": self.key,
                "Authorization": _AUTHORIZATION_BEARER_PREFIX + self.key,
                "Accept": _JSON_CONTENT_TYPE,
                "Accept-Encoding": "identity",
            }
            async with session.get(
                f"{self.url}/rest/v1/risk_pnl_events", params=params, headers=headers
            ) as resp:
                if resp.status >= 300:
                    body = (await resp.text())[:500]
                    raise RuntimeError(f"realized PnL read failed with HTTP {resp.status}: {body}")
                rows = await resp.json()
            if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                raise RuntimeError("realized PnL ledger returned malformed payload")
            events = {
                str(row.get("event_id")): float(row.get("pnl_usdt") or 0.0)
                for row in rows if row.get("event_id")
            }
            events = self._merge_pending_realized_pnl_events(
                events, loss_day, is_mock
            )
            return sum(-pnl for pnl in events.values() if pnl < 0)
        except Exception as exc:
            logger.error("daily realized PnL restoration failed: %s", exc)
            raise RuntimeError("verified daily realized PnL could not be restored") from exc
        finally:
            if created and session is not None:
                await session.close()


    def mark_risk_persistence_failure(self) -> None:
        if self._outbox is not None:
            self._outbox.set_numeric_state("risk-persistence-failed", 1.0)

    def risk_persistence_failed(self) -> bool:
        if self._outbox is None:
            return False
        return (self._outbox.get_numeric_state("risk-persistence-failed") or 0.0) > 0

    def clear_risk_persistence_failure(self) -> None:
        if self._outbox is not None:
            self._outbox.set_numeric_state("risk-persistence-failed", 0.0)
