"""Durable, asynchronous Supabase audit persistence for the executor.

Callbacks commit rows to a local SQLite journal; a background worker performs
all network delivery. A database outage therefore cannot hold a hedge,
rollback, or force-close operation on a remote request.
"""

import asyncio
import json
import logging
import math
import os
import socket
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

from executor.supabase_outbox import DurableOutbox
from executor.supabase_registry import SupabaseRegistry

logger = logging.getLogger("executor.supabase")

_MAX_ATTEMPTS = 3
_BATCH_MAX = 50
_STATUS_HEARTBEAT_SECONDS = 60.0
_TIMEOUT = aiohttp.ClientTimeout(total=8, connect=3)
_SIDE_BY_LEG = {
    "spot_entry": "buy",
    "spot_exit": "sell",
    "futures_entry": "sell",
    "futures_exit": "buy",
    "rollback": "sell",
    "force_close": "sell",
}
# Positions are keyed deterministically by pair so a restart reuses the same row.
# The schema allows only one active position per (pair_key, is_mock); a random id
# per process would collide with the previous process's row instead of updating it.
_POSITION_NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


def _position_uuid(pair_key: str, opened_at_ms: Any = 0) -> str:
    """Deterministic position id, stable across retries and restarts.

    Including the open timestamp keeps consecutive trades on the same pair as
    separate rows instead of overwriting each other.
    """
    try:
        stamp = int(opened_at_ms or 0)
    except (TypeError, ValueError):
        stamp = 0
    return str(uuid.uuid5(_POSITION_NS, f"{pair_key}:{stamp}"))


def _ms_to_iso(value: Any) -> Optional[str]:
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _optional_num(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


class SupabaseWriter:
    """Journaled writer for the nafaz core schema.

    Callbacks perform a local durable SQLite commit only; Supabase HTTP delivery
    runs in the background. A disabled writer remains a no-op for compatibility.
    """

    def __init__(self, config: Any):
        self.url = str(getattr(config, "url", "") or "").rstrip("/")
        self.key = str(getattr(config, "key", "") or "")
        self.enabled = bool(getattr(config, "enabled", False)) and bool(self.url) and bool(self.key)

        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._dropped = 0
        self._outbox: Optional[DurableOutbox] = None
        if self.enabled:
            outbox_path = str(
                getattr(config, "outbox_path", "")
                or os.getenv("SUPABASE_OUTBOX_PATH", "/app/data/executor/supabase-outbox.sqlite3")
            )
            try:
                self._outbox = DurableOutbox(outbox_path)
            except Exception:
                self.enabled = False
                logger.exception("Supabase persistence disabled: durable outbox could not be opened")

        # Client-generated ids keep upserts idempotent across retries and
        # restarts, and remove the need to read an id back from Supabase.
        self._position_ids: Dict[str, str] = {}
        self._position_rows: Dict[str, Dict[str, Any]] = {}
        self._position_pair_infos: Dict[str, Any] = {}
        self._order_ids: Dict[str, str] = {}
        self._last_status_signature: Optional[str] = None
        self._last_status_at = 0.0
        self._registry = SupabaseRegistry(self._emit)

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        if self._outbox is None:
            logger.error("Supabase persistence is enabled but durable outbox is unavailable")
            return
        self._task = asyncio.create_task(self._run())
        logger.info("Supabase persistence enabled -> %s (durable outbox: %s)", self.url, self._outbox.path)

    async def close(self) -> None:
        if self._task is None:
            return
        deadline = asyncio.get_running_loop().time() + 2.0
        while self._outbox is not None and self._outbox.pending_count() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        if self._outbox is not None and self._outbox.pending_count():
            logger.warning("Supabase durable outbox retained %d pending rows for restart", self._outbox.pending_count())
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
        if self._dropped:
            logger.error("Supabase durable outbox enqueue failures: %d rows were not journaled", self._dropped)
        if self._outbox is not None:
            self._outbox.close()
            self._outbox = None

    # ------------------------------------------------------------------- emit

    def _emit(self, table: str, row: Dict[str, Any], on_conflict: Optional[str] = None) -> None:
        """Queues one row. Never blocks, never raises."""
        if not self.enabled or self._outbox is None:
            return
        try:
            self._outbox.enqueue(table, on_conflict, row)
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: durable Supabase outbox enqueue failed for table=%s", table)
            return
        # The worker polls the durable journal. No in-memory queue is needed,
        # so process restarts and backpressure cannot discard accepted rows.

    def runtime_configuration(self, config: Any, instance_id: Optional[str] = None) -> None:
        """Journal sanitized active venue and risk metadata; never stores secrets."""
        self._registry.runtime_configuration(config, instance_id)

    def event(
        self,
        event_type: str,
        *,
        pair_key: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        is_mock: bool = True,
        occurred_at_ms: Optional[int] = None,
        event_id: Optional[str] = None,
    ) -> None:
        """Appends to the executor event journal. `event_id` makes it idempotent."""
        stamp = int(occurred_at_ms or time.time() * 1000)
        resolved_event_id = event_id or f"{event_type}:{pair_key or '-'}:{stamp}"
        event_payload = payload or {}
        occurred_at = _ms_to_iso(stamp) or datetime.now(timezone.utc).isoformat()
        self._emit(
            "executor_events",
            {
                "event_id": resolved_event_id,
                "event_type": event_type,
                "pair_key": pair_key,
                "is_mock": bool(is_mock),
                "occurred_at": occurred_at,
                "payload": event_payload,
            },
            on_conflict="event_id",
        )
        if event_type == "EXECUTION_FAILURE":
            state = str(event_payload.get("state") or "").upper()
            self._emit(
                "risk_events",
                {
                    "event_key": resolved_event_id,
                    "severity": "critical" if state == "RECONCILIATION_REQUIRED" else "error",
                    "event_type": event_type,
                    "pair_key": pair_key,
                    "exchange_name": event_payload.get("exchange_name") or event_payload.get("exchange"),
                    "message": str(
                        event_payload.get("message") or event_payload.get("reason") or "Execution failure"
                    ),
                    "action_taken": event_payload.get("action_taken") or state or None,
                    "is_mock": bool(is_mock),
                    "details": event_payload,
                    "occurred_at": occurred_at,
                },
                on_conflict="event_key",
            )

    # --------------------------------------------------------------- positions

    def position_opened(
        self, pair_key: str, pos: Any, pair_info: Any, is_mock: bool = True
    ) -> None:
        position_id = self._position_ids.get(pair_key)
        if position_id is None:
            position_id = _position_uuid(pair_key, getattr(pos, "opened_at_ms", 0) if pos else 0)
            self._position_ids[pair_key] = position_id
        row = {
            "id": position_id,
            "pair_key": pair_key,
            "local_exchange": getattr(pos, "local_exchange", "") or "",
            "global_exchange": getattr(pos, "global_exchange", "") or "",
            "local_symbol": getattr(pair_info, "local_symbol", "") or getattr(pos, "symbol", ""),
            "global_symbol": getattr(pair_info, "global_symbol", "") or getattr(pos, "symbol", ""),
            "base_asset": getattr(pair_info, "base_asset", "") or "",
            "quote_asset": getattr(pair_info, "quote_asset", "") or "",
            "status": getattr(pos, "state", "HEDGED_ACTIVE") or "HEDGED_ACTIVE",
            "execution_mode": "MAKER_TAKER",
            "is_mock": bool(is_mock),
            "qty": _num(getattr(pos, "qty", 0.0)),
            "initial_qty": _num(getattr(pos, "initial_qty", 0.0) or getattr(pos, "qty", 0.0)),
            "spot_entry_avg_price": _num(getattr(pos, "entry_price_idr", 0.0)),
            "futures_short_entry_avg_price": _num(getattr(pos, "short_entry_usdt", 0.0)),
            "spot_cost_quote": _num(getattr(pos, "spot_cost_usdt", 0.0)),
            "futures_margin_quote": _num(getattr(pos, "futures_margin_usdt", 0.0)),
            "accumulated_funding": _num(getattr(pos, "accumulated_funding_usdt", 0.0)),
            "current_funding_rate": _num(getattr(pos, "current_funding_rate", 0.0)),
            "opened_at": _ms_to_iso(getattr(pos, "opened_at_ms", 0)),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "state_snapshot": pos.to_dict() if hasattr(pos, "to_dict") else {},
        }
        self._position_rows[pair_key] = row
        self._position_pair_infos[pair_key] = pair_info
        self._emit("positions", row, on_conflict="id")
        self._emit_position_legs(pair_key, position_id, pos, row["status"])
        self._registry.market_pair(pair_key, pair_info, is_mock=is_mock)

    def position_status(self, pair_key: str, status: str, pos: Any = None) -> None:
        position_id = self._position_ids.get(pair_key)
        if position_id is None:
            return
        # Merge into the last known full row: a partial payload cannot be sent,
        # because PostgREST requires every object in a batch to share one key set.
        row = dict(self._position_rows.get(pair_key) or {"id": position_id})
        row["status"] = status
        row["updated_at"] = datetime.now(timezone.utc).isoformat()
        if pos is not None:
            row["qty"] = _num(getattr(pos, "qty", 0.0))
            row["accumulated_funding"] = _num(getattr(pos, "accumulated_funding_usdt", 0.0))
            row["state_snapshot"] = pos.to_dict() if hasattr(pos, "to_dict") else {}
            if status == "CLOSED":
                row["closed_at"] = datetime.now(timezone.utc).isoformat()
        self._position_rows[pair_key] = row
        self._emit("positions", row, on_conflict="id")
        self._emit_position_legs(pair_key, position_id, pos, status)

    def _emit_position_legs(
        self, pair_key: str, position_id: str, pos: Any, status: str
    ) -> None:
        """Upserts the local long and perpetual short legs for an active position."""
        position_row = self._position_rows.get(pair_key) or {}
        pair_info = self._position_pair_infos.get(pair_key)
        fallback_symbol = getattr(pos, "symbol", "") or position_row.get("local_symbol", "")
        qty = _num(getattr(pos, "qty", position_row.get("qty", 0.0)))
        spot_entry = _num(
            getattr(pos, "entry_price_idr", position_row.get("spot_entry_avg_price", 0.0))
        )
        perp_entry = _num(
            getattr(pos, "short_entry_usdt", position_row.get("futures_short_entry_avg_price", 0.0))
        )
        legs = (
            (
                "spot", "long",
                str(getattr(pos, "local_exchange", "") or position_row.get("local_exchange", "")),
                str(getattr(pair_info, "local_symbol", "") or fallback_symbol),
                spot_entry,
            ),
            (
                "perpetual", "short",
                str(getattr(pos, "global_exchange", "") or position_row.get("global_exchange", "")),
                str(getattr(pair_info, "global_symbol", "") or fallback_symbol),
                perp_entry,
            ),
        )
        for leg, side, exchange, symbol, entry_price in legs:
            if not exchange or not symbol:
                continue
            row = {
                "id": str(uuid.uuid5(_POSITION_NS, f"position-leg:{position_id}:{leg}")),
                "position_id": position_id,
                "leg": leg,
                "exchange_name": exchange,
                "symbol": symbol,
                "side": side,
                "status": status,
                "qty": qty,
                "average_entry_price": entry_price or None,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            self._emit("position_legs", row, on_conflict="position_id,leg")

    def funding_payment(
        self,
        pair_key: str,
        pos: Any,
        symbol: str,
        payment: Dict[str, Any],
        *,
        is_mock: bool = True,
    ) -> None:
        """Journals a confirmed exchange settlement with a deterministic primary key."""
        if not isinstance(payment, dict):
            return
        timestamp = _optional_num(payment.get("timestamp"))
        amount = _optional_num(payment.get("amount"))
        exchange = str(getattr(pos, "global_exchange", "") or payment.get("exchange") or "").lower()
        resolved_symbol = str(symbol or payment.get("symbol") or "").strip()
        if timestamp is None or timestamp <= 0 or amount is None or not exchange or not resolved_symbol:
            return

        info = payment.get("info") if isinstance(payment.get("info"), dict) else {}
        external_id = (
            payment.get("id") or info.get("id") or info.get("billId")
            or info.get("tranId") or info.get("paymentId")
        )
        asset = str(payment.get("code") or payment.get("currency") or "USDT").upper()
        if external_id in (None, ""):
            external_id = f"derived:{resolved_symbol}:{int(timestamp)}:{asset}:{amount:.18g}"
        external_id = str(external_id)
        position_qty = _optional_num(payment.get("contracts"))
        if position_qty is None:
            position_qty = _optional_num(getattr(pos, "qty", None))
        row = {
            "id": str(uuid.uuid5(_POSITION_NS, f"funding:{exchange}:{external_id}")),
            "position_id": self._position_ids.get(pair_key),
            "exchange_name": exchange,
            "symbol": resolved_symbol,
            "exchange_payment_id": external_id,
            "funding_rate": _optional_num(payment.get("rate")),
            "position_qty": position_qty,
            "mark_price": _optional_num(payment.get("price")),
            "amount": amount,
            "asset": asset,
            "payment_status": "confirmed",
            "settled_at": _ms_to_iso(int(timestamp)),
            "raw_payload": {"payment": payment, "is_mock": bool(is_mock)},
        }
        self._emit("funding_payments", row, on_conflict="id")

    def balance_snapshot(
        self,
        reports: Any,
        *,
        is_mock: bool = True,
        observed_at_ms: Optional[int] = None,
    ) -> int:
        """Persists successful per-asset balance observations; never stores failed reads as zero."""
        if not isinstance(reports, (list, tuple)):
            return 0
        stamp = int(observed_at_ms or time.time() * 1000)
        observed_at = _ms_to_iso(stamp)
        written = 0
        for report in reports:
            if not isinstance(report, dict) or str(report.get("status", "")).lower() != "ok":
                continue
            exchange = str(report.get("name") or "").strip().lower()
            market = str(report.get("market") or "").strip().lower()
            if market == "spot":
                market_type = "spot"
            elif market in ("future", "futures", "swap", "perpetual"):
                market_type = "perpetual"
            else:
                continue
            if not exchange:
                continue
            assets = report.get("assets")
            if not isinstance(assets, list):
                continue
            for asset_row in assets:
                if not isinstance(asset_row, dict):
                    continue
                asset = str(asset_row.get("asset") or "").strip().upper()
                total = _optional_num(asset_row.get("total"))
                free = _optional_num(asset_row.get("free"))
                used = _optional_num(asset_row.get("used"))
                if not asset or total is None or free is None or used is None:
                    continue
                if min(total, free, used) < 0:
                    continue
                row_id = str(uuid.uuid5(
                    _POSITION_NS,
                    f"balance:{exchange}:{market_type}:{asset}:{stamp}:{bool(is_mock)}",
                ))
                self._emit(
                    "balance_snapshots",
                    {
                        "id": row_id,
                        "exchange_name": exchange,
                        "market_type": market_type,
                        "asset": asset,
                        "total": total,
                        "free": free,
                        "used": used,
                        "is_mock": bool(is_mock),
                        "observed_at": observed_at,
                        "raw_payload": {"report": report, "asset": asset_row},
                    },
                    on_conflict="id",
                )
                written += 1
        return written

    def executor_status(
        self, snapshot: Dict[str, Any], *, instance_id: Optional[str] = None
    ) -> bool:
        """Stores status changes immediately and an unchanged-state heartbeat every minute."""
        if not isinstance(snapshot, dict):
            return False
        stamp = int(snapshot.get("timestamp_ms") or time.time() * 1000)
        stable_snapshot = dict(snapshot)
        stable_snapshot.pop("timestamp_ms", None)
        signature = json.dumps(stable_snapshot, sort_keys=True, separators=(",", ":"), default=str)
        now = time.monotonic()
        if signature == self._last_status_signature and now - self._last_status_at < _STATUS_HEARTBEAT_SECONDS:
            return False

        resolved_instance = str(
            instance_id or os.getenv("EXECUTOR_INSTANCE_ID") or socket.gethostname() or "executor"
        ).strip()[:128] or "executor"
        is_mock = bool(snapshot.get("mock_mode", True))
        row = {
            "id": str(uuid.uuid5(_POSITION_NS, f"executor-status:{resolved_instance}:{stamp}")),
            "instance_id": resolved_instance,
            "status": str(snapshot.get("status") or "UNKNOWN"),
            "mode": "MOCK" if is_mock else "LIVE",
            "is_mock": is_mock,
            "snapshot": snapshot,
            "observed_at": _ms_to_iso(stamp),
        }
        self._emit("executor_status_snapshots", row, on_conflict="id")
        self._last_status_signature = signature
        self._last_status_at = now
        return True

    # ------------------------------------------------------------------ orders

    def order(
        self,
        pair_key: str,
        leg: str,
        exchange: str,
        market_type: str,
        symbol: str,
        order: Dict[str, Any],
        *,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        is_mock: bool = True,
    ) -> None:
        """Upserts an order and appends any newly observed fill quantity."""
        if not isinstance(order, dict):
            return
        exchange_order_id = str(order.get("id") or "") or None
        key = client_order_id or exchange_order_id
        if not key:
            return
        order_state_key = f"{exchange}:{pair_key}:{key}"
        order_id = self._order_ids.setdefault(
            order_state_key, str(uuid.uuid5(_POSITION_NS, f"order:{order_state_key}"))
        )

        requested = _num(order.get("amount"), _num(order.get("filled")))
        filled = _num(order.get("filled"))
        self._emit(
            "orders",
            {
                "id": order_id,
                "position_id": self._position_ids.get(pair_key),
                "pair_key": pair_key,
                "exchange_name": exchange,
                "market_type": market_type,
                "leg": leg,
                "exchange_order_id": exchange_order_id,
                "client_order_id": client_order_id,
                "symbol": symbol,
                "side": _SIDE_BY_LEG.get(leg, "sell"),
                "order_type": str(order.get("type") or "limit"),
                "status": str(order.get("status") or "open"),
                "requested_qty": requested,
                "filled_qty": filled,
                "remaining_qty": max(0.0, requested - filled),
                "limit_price": _num(order.get("price")) or None,
                "average_fill_price": _num(order.get("average")) or None,
                "reduce_only": bool(reduce_only),
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": (
                    datetime.now(timezone.utc).isoformat()
                    if str(order.get("status") or "").lower() in ("closed", "canceled", "cancelled", "rejected", "expired")
                    else None
                ),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "raw_payload": order,
            },
            on_conflict="id",
        )

        if filled > 1e-12 and self._outbox is not None:
            price = _num(order.get("average"), _num(order.get("price")))
            cumulative_quote = _num(order.get("cost"))
            if cumulative_quote <= 0 and price > 0:
                cumulative_quote = filled * price
            order_state_key = f"{exchange}:{pair_key}:{key}"
            fill_id = f"{exchange}:{pair_key}:{exchange_order_id or key}:{filled:.18f}"
            fill_row = {
                # The database's account-scoped expression index cannot be used
                # as an ON CONFLICT target. Include pair identity in our stable
                # primary key to avoid collisions if a venue reuses an order id.
                "id": str(uuid.uuid5(_POSITION_NS, f"fill:{order_state_key}:{filled:.18f}")),
                "order_id": order_id,
                "exchange_name": exchange,
                "exchange_fill_id": fill_id,
                "price": price,
                "filled_at": datetime.now(timezone.utc).isoformat(),
                "raw_payload": {"derived_from": "cumulative_filled"},
            }
            try:
                self._outbox.enqueue_cumulative_fill(
                    "order_fills",
                    "id",
                    fill_row,
                    order_state_key,
                    filled,
                    cumulative_quote,
                )
            except Exception:
                self._dropped += 1
                logger.exception("CRITICAL: failed to journal cumulative fill for %s", key)

    # ------------------------------------------------------------ closed trades

    def trade_closed(self, pair_key: str, trade: Dict[str, Any], pos: Any = None) -> None:
        if not isinstance(trade, dict):
            return
        trade_key = str(trade.get("trade_id") or "")
        if not trade_key:
            return
        self._emit(
            "closed_trades",
            {
                "position_id": self._position_ids.get(pair_key),
                "trade_key": trade_key,
                "pair_key": pair_key,
                "local_exchange": str(trade.get("local_exchange") or ""),
                "global_exchange": str(trade.get("global_exchange") or ""),
                "symbol": str(trade.get("symbol") or ""),
                "qty": _num(trade.get("qty")),
                "entry_price_spot": _num(trade.get("entry_price_idr")),
                "exit_price_spot": _num(trade.get("exit_price_idr")),
                "entry_price_perp": _num(trade.get("short_entry_usdt")),
                "exit_price_perp": _num(trade.get("short_exit_usdt")),
                "spot_pnl": _num(trade.get("spot_pnl_idr")),
                "futures_pnl": _num(trade.get("futures_pnl_usdt")),
                "funding_pnl": _num(trade.get("accumulated_funding_usdt")),
                "fees": _num(trade.get("fees_usdt")),
                "net_pnl": _num(trade.get("net_profit_usdt")),
                "net_pnl_idr": _num(trade.get("net_profit_idr")),
                "net_roi_pct": _num(trade.get("net_roi_pct")),
                "is_mock": bool(trade.get("is_mock", True)),
                "close_reason": trade.get("close_reason"),
                "opened_at": _ms_to_iso(getattr(pos, "opened_at_ms", 0)) if pos else None,
                "closed_at": _ms_to_iso(trade.get("closed_at_ms")) or datetime.now(timezone.utc).isoformat(),
                "metadata": {
                    "pnl_complete": trade.get("pnl_complete"),
                    "spot_liquidation_failed": trade.get("spot_liquidation_failed"),
                },
            },
            on_conflict="trade_key",
        )
        self.position_status(pair_key, "CLOSED", pos)
        self.event(
            "POSITION_CLOSED",
            pair_key=pair_key,
            is_mock=bool(trade.get("is_mock", True)),
            occurred_at_ms=trade.get("closed_at_ms"),
            payload=trade,
        )

    # ------------------------------------------------------------------- flush

    async def _run(self) -> None:
        while True:
            try:
                if self._outbox is None:
                    await asyncio.sleep(1.0)
                    continue
                batch = await asyncio.to_thread(self._outbox.pending, _BATCH_MAX)
                if not batch:
                    await asyncio.sleep(0.25)
                    continue
                await self._flush(batch)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Supabase durable outbox worker error")
                await asyncio.sleep(1.0)

    async def _flush(self, batch: list) -> None:
        grouped: Dict[tuple, list] = {}
        for item_id, table, on_conflict, row in batch:
            grouped.setdefault((table, on_conflict, frozenset(row)), []).append(
                (item_id, row)
            )

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_TIMEOUT,
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                    "Accept-Encoding": "identity",
                },
            )

        for (table, on_conflict, _keys), items in grouped.items():
            # PostgREST rejects duplicate conflict keys in one upsert statement.
            # Keep the newest row, but acknowledge all superseded journal entries
            # only after that newest state has been accepted.
            conflict_cols = [c.strip() for c in (on_conflict or "id").split(",")]
            latest: Dict[tuple, Dict[str, Any]] = {}
            for _item_id, row in items:
                latest[tuple(str(row.get(col)) for col in conflict_cols)] = row
            rows = list(latest.values())
            url = f"{self.url}/rest/v1/{table}"
            if on_conflict:
                url = f"{url}?on_conflict={on_conflict}"
            last_error = "Supabase request failed"
            delivered = False
            for attempt in range(1, _MAX_ATTEMPTS + 1):
                try:
                    async with self._session.post(url, json=rows) as resp:
                        if resp.status < 300:
                            delivered = True
                            break
                        body = (await resp.text())[:300]
                        last_error = f"HTTP {resp.status}: {body}"
                        # Keep permanent 4xx errors in the outbox as well: a
                        # schema/permission repair must be able to replay them.
                        if 400 <= resp.status < 500 and resp.status not in (408, 429):
                            break
                except Exception as exc:
                    last_error = str(exc)[:500]
                if attempt < _MAX_ATTEMPTS:
                    await asyncio.sleep(0.5 * attempt)

            item_ids = [item_id for item_id, _row in items]
            if delivered:
                await asyncio.to_thread(self._outbox.ack, item_ids)
            else:
                await asyncio.to_thread(self._outbox.retry, item_ids, last_error)
                logger.error(
                    "Supabase delivery deferred table=%s rows=%d: %s",
                    table, len(rows), last_error,
                )
