"""Best-effort durable operational persistence for Supabase Postgres.

Redis remains authoritative for real-time coordination and emergency workflows.
This adapter is deliberately non-blocking from the trading-safety perspective:
callers catch persistence errors and continue managing exchange exposure.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Optional
import uuid

import httpx

class SupabasePersistenceError(RuntimeError):
    pass


def _iso_from_ms(value: Any) -> Optional[str]:
    try:
        millis = int(value or 0)
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()


def _event_key(event: dict[str, Any]) -> str:
    explicit = str(event.get("event_id") or "").strip()
    if explicit:
        return explicit
    trade_id = str(event.get("trade_id") or "").strip()
    if trade_id:
        return "trade:" + trade_id
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SupabaseRepository:
    def __init__(
        self,
        url: str,
        service_role_key: str,
        *,
        client: Optional[httpx.AsyncClient] = None,
        retries: int = 2,
    ):
        self.url = url.rstrip("/")
        self.service_role_key = service_role_key
        if not self.url.startswith(("https://", "http://")):
            raise ValueError("SUPABASE_URL must be an absolute http(s) URL")
        if not service_role_key.strip():
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY is required")
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(timeout=httpx.Timeout(3.0, connect=2.0))
        self.retries = max(0, min(int(retries), 5))

    @classmethod
    def from_config(cls, config):
        if not getattr(config, "enabled", False):
            return None
        return cls(getattr(config, "url", ""), getattr(config, "key", ""))

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def _request(
        self,
        method: str,
        table: str,
        *,
        params: Optional[dict[str, str]] = None,
        payload: Any = None,
        prefer: str = "return=minimal",
    ) -> None:
        headers = {
            "apikey": self.service_role_key,
            "Authorization": "Bearer " + self.service_role_key,
            "Content-Type": "application/json",
            "Prefer": prefer,
        }
        url = f"{self.url}/rest/v1/{table}"
        last_error: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            try:
                response = await self.client.request(
                    method, url, params=params, json=payload, headers=headers
                )
                if 200 <= response.status_code < 300:
                    return
                error = SupabasePersistenceError(
                    f"Supabase {method} {table} returned HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )
                if response.status_code < 500 and response.status_code != 429:
                    raise error
                last_error = error
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = exc
            if attempt < self.retries:
                await asyncio.sleep(0.1 * (2 ** attempt))
        raise SupabasePersistenceError(
            f"Supabase persistence failed for {table}: {last_error}"
        ) from last_error

    async def _upsert(self, table: str, rows: Any, conflict: str, *, ignore: bool = False):
        prefer = "resolution=ignore-duplicates,return=minimal" if ignore else "resolution=merge-duplicates,return=minimal"
        await self._request(
            "POST",
            table,
            params={"on_conflict": conflict},
            payload=rows,
            prefer=prefer,
        )

    async def persist_event(self, event: dict[str, Any]) -> None:
        """Persist the event journal and the normalized operational records."""
        if not isinstance(event, dict) or not event.get("event"):
            raise ValueError("event payload must include event")
        event_type = str(event["event"])
        pair_key = str(event.get("pair_key") or event.get("pair_tag") or "")
        event_id = _event_key(event)
        occurred_at = (
            _iso_from_ms(event.get("timestamp_ms") or event.get("closed_at_ms"))
            or datetime.now(timezone.utc).isoformat()
        )
        is_mock = bool(event.get("is_mock", True))

        if event_type == "POSITION_OPENED":
            await self._upsert_position(event, status="HEDGED_ACTIVE", occurred_at=occurred_at)
        elif event_type == "POSITION_CLOSED":
            remaining = float(event.get("remaining_qty", 0.0) or 0.0)
            partial_force = bool(event.get("spot_liquidation_failed", False))
            status = "RECONCILIATION_REQUIRED" if partial_force else ("CLOSED" if remaining <= 1e-8 else "HEDGED_ACTIVE")
            # Upsert the parent position first so the closed_trades/executor_events
            # foreign keys remain valid even when the original open write was missed.
            await self._upsert_position(event, status=status, occurred_at=occurred_at)
            await self._persist_closed_trade(event, pair_key, is_mock, occurred_at)
        elif event_type == "EXECUTION_FAILURE":
            await self._persist_risk_event(event, event_id, pair_key, is_mock, occurred_at)
            if str(event.get("state") or "") == "RECONCILIATION_REQUIRED":
                await self._mark_reconciliation_required(event, pair_key, is_mock, occurred_at)

        event_row = {
            "event_id": event_id,
            "event_type": event_type,
            "pair_key": pair_key or None,
            "position_id": event.get("position_id") or None,
            "is_mock": is_mock,
            "occurred_at": occurred_at,
            "payload": event,
        }
        await self._upsert("executor_events", event_row, "event_id", ignore=True)

    async def _upsert_position(self, event: dict[str, Any], *, status: str, occurred_at: str):
        position_id = str(event.get("position_id") or "").strip()
        if not position_id:
            logger.warning("Supabase position persistence skipped: event has no position_id")
            return
        try:
            uuid.UUID(position_id)
        except ValueError:
            raise ValueError("position_id must be a UUID")

        pair_key = str(event.get("pair_key") or event.get("pair_tag") or "")
        symbol = str(event.get("local_symbol") or event.get("symbol") or "")
        global_symbol = str(event.get("global_symbol") or symbol)
        base_asset = str(event.get("base_asset") or symbol.replace("/", "_").split("_")[0]).upper()
        quote_asset = str(event.get("quote_asset") or ("IDR" if "IDR" in symbol.upper() else "USDT")).upper()
        qty = float(event.get("remaining_qty", event.get("qty", 0.0)) or 0.0)
        row = {
            "id": position_id,
            "pair_key": pair_key,
            "local_exchange": event.get("local_exchange") or "",
            "global_exchange": event.get("global_exchange") or "",
            "local_symbol": symbol,
            "global_symbol": global_symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "futures_quote_asset": event.get("futures_quote_asset") or ("USDC" if "USDC" in global_symbol.upper() else "USDT"),
            "entry_usdt_idr_rate": event.get("entry_usdt_idr_rate"),
            "status": status,
            "execution_mode": event.get("execution_mode") or "MAKER_TAKER",
            "is_mock": bool(event.get("is_mock", True)),
            "qty": qty,
            "initial_qty": float(event.get("initial_qty", event.get("qty", 0.0)) or 0.0),
            "spot_entry_avg_price": event.get("entry_price_idr"),
            "futures_short_entry_avg_price": event.get("short_entry_usdt"),
            "spot_cost_quote": event.get("spot_cost_quote"),
            "futures_margin_quote": event.get("futures_margin_quote"),
            "accumulated_funding": event.get(
                "position_accumulated_funding_usdt",
                event.get("accumulated_funding_usdt", 0.0),
            ),
            "funding_asset": "USDT",
            "opened_at": _iso_from_ms(event.get("opened_at_ms")) or occurred_at,
            "state_snapshot": event,
        }
        await self._upsert("positions", row, "id")

        remaining = max(0.0, qty)
        spot_status = "OPEN" if remaining > 0 else "CLOSED"
        perp_status = "CLOSED" if event.get("spot_liquidation_failed") else ("OPEN" if remaining > 0 else "CLOSED")
        legs = [
            {
                "position_id": position_id, "leg": "spot",
                "exchange_name": row["local_exchange"], "symbol": symbol,
                "side": "long", "status": spot_status, "qty": remaining,
                "average_entry_price": event.get("entry_price_idr"),
            },
            {
                "position_id": position_id, "leg": "perpetual",
                "exchange_name": row["global_exchange"], "symbol": global_symbol,
                "side": "short", "status": perp_status, "qty": remaining,
                "average_entry_price": event.get("short_entry_usdt"),
            },
        ]
        await self._upsert("position_legs", legs, "position_id,leg")

    async def _persist_closed_trade(self, event, pair_key, is_mock, occurred_at):
        trade_id = str(event.get("trade_id") or "").strip()
        if not trade_id:
            raise ValueError("closed trade event requires trade_id")
        if event.get("pnl_complete") is False or event.get("pnl_unverified") is True:
            # Keep the event for audit, but never put incomplete PnL in realized-trade totals.
            return
        row = {
            "trade_key": trade_id,
            "pair_key": pair_key,
            "position_id": event.get("position_id") or None,
            "local_exchange": event.get("local_exchange") or "",
            "global_exchange": event.get("global_exchange") or "",
            "symbol": event.get("symbol") or "",
            "qty": event.get("qty") or 0,
            "entry_price_spot": event.get("entry_price_idr"),
            "exit_price_spot": event.get("exit_price_idr"),
            "entry_price_perp": event.get("short_entry_usdt"),
            "exit_price_perp": event.get("short_exit_usdt"),
            "spot_pnl": event.get("spot_pnl_idr") or 0,
            "spot_pnl_asset": "IDR",
            "futures_pnl": event.get("futures_pnl_usdt") or 0,
            "futures_pnl_asset": "USDT",
            "funding_pnl": event.get("accumulated_funding_usdt") or 0,
            "funding_asset": "USDT",
            "fees": event.get("total_fees_usdt", event.get("fees_usdt", 0)) or 0,
            "fees_asset": "USDT",
            "net_pnl": event.get("net_profit_usdt") or 0,
            "pnl_asset": "USDT",
            "net_pnl_idr": event.get("net_profit_idr"),
            "entry_usdt_idr_rate": event.get("entry_usdt_idr_rate"),
            "exit_usdt_idr_rate": event.get("exit_usdt_idr_rate"),
            "net_roi_pct": event.get("net_roi_pct"),
            "is_mock": is_mock,
            "close_reason": (
                "FORCE_CLOSE_PARTIAL" if event.get("spot_liquidation_failed")
                else ("FORCE_CLOSE" if str(trade_id).startswith("force-") else "EXIT")
            ),
            "opened_at": _iso_from_ms(event.get("opened_at_ms")),
            "closed_at": _iso_from_ms(event.get("closed_at_ms")) or occurred_at,
            "metadata": event,
        }
        await self._upsert("closed_trades", row, "trade_key")

    async def _update_position_after_close(self, event, pair_key, is_mock, occurred_at):
        position_id = str(event.get("position_id") or "").strip()
        if not position_id:
            return
        remaining = float(event.get("remaining_qty", 0.0) or 0.0)
        partial_force = bool(event.get("spot_liquidation_failed", False))
        status = "RECONCILIATION_REQUIRED" if partial_force else ("CLOSED" if remaining <= 1e-8 else "HEDGED_ACTIVE")
        patch = {
            "status": status,
            "qty": remaining,
            "accumulated_funding": 0,
            "closed_at": occurred_at if status == "CLOSED" else None,
            "state_snapshot": event,
        }
        await self._request("PATCH", "positions", params={"id": f"eq.{position_id}"}, payload=patch)
        leg_status = "OPEN" if remaining > 1e-8 or partial_force else "CLOSED"
        legs = [
            {"position_id": position_id, "leg": "spot", "exchange_name": event.get("local_exchange") or "", "symbol": event.get("symbol") or "", "side": "long", "status": "OPEN" if partial_force else leg_status, "qty": remaining},
            {"position_id": position_id, "leg": "perpetual", "exchange_name": event.get("global_exchange") or "", "symbol": event.get("global_symbol") or event.get("symbol") or "", "side": "short", "status": "CLOSED" if not partial_force else "CLOSED", "qty": 0 if partial_force else remaining},
        ]
        await self._upsert("position_legs", legs, "position_id,leg")

    async def _persist_risk_event(self, event, event_id, pair_key, is_mock, occurred_at):
        reason = str(event.get("reason") or event.get("event") or "EXECUTION_FAILURE")
        state = str(event.get("state") or "")
        severity = "critical" if state == "RECONCILIATION_REQUIRED" else "error"
        row = {
            "event_key": event_id,
            "severity": severity,
            "event_type": reason,
            "pair_key": pair_key or None,
            "exchange_name": event.get("global_exchange") or event.get("local_exchange") or None,
            "message": str(event.get("message") or reason),
            "action_taken": state or None,
            "is_mock": is_mock,
            "details": event.get("detail") or {},
            "occurred_at": occurred_at,
        }
        await self._upsert("risk_events", row, "event_key", ignore=True)

    async def _mark_reconciliation_required(self, event, pair_key, is_mock, occurred_at):
        # Existing partial unique index protects one active position per pair/mode.
        filters = {
            "pair_key": f"eq.{pair_key}",
            "is_mock": f"eq.{str(is_mock).lower()}",
            "status": "in.(ENTRY_PENDING,ENTRY_PARTIAL,HEDGED_ACTIVE,EXIT_PENDING,RECONCILIATION_REQUIRED)",
        }
        await self._request(
            "PATCH", "positions", params=filters,
            payload={"status": "RECONCILIATION_REQUIRED", "state_snapshot": event},
        )
