"""Best-effort Supabase persistence for the executor.

Every write is queued and flushed by a single background task, so a database
outage can never delay a hedge, rollback, or force close. Rows are upserted on
client-generated ids, which makes retries idempotent: replaying a queued write
cannot duplicate a position, an order, or a fill.
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger("executor.supabase")

_QUEUE_MAX = 512
_MAX_ATTEMPTS = 3
_BATCH_MAX = 50
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
    return result if result == result else default  # drop NaN


class SupabaseWriter:
    """Queued, non-blocking writer for the nafaz core schema.

    A disabled writer is a no-op object with the same interface, so call sites
    never branch on configuration.
    """

    def __init__(self, config: Any):
        self.url = str(getattr(config, "url", "") or "").rstrip("/")
        self.key = str(getattr(config, "key", "") or "")
        self.enabled = bool(getattr(config, "enabled", False)) and bool(self.url) and bool(self.key)

        self._queue: Optional[asyncio.Queue] = None
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._dropped = 0

        # Client-generated ids keep upserts idempotent across retries and
        # restarts, and remove the need to read an id back from Supabase.
        self._position_ids: Dict[str, str] = {}
        self._position_rows: Dict[str, Dict[str, Any]] = {}
        self._order_ids: Dict[str, str] = {}
        self._last_status_snapshot = 0.0

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        self._queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._task = asyncio.create_task(self._run())
        logger.info("Supabase persistence enabled -> %s", self.url)

    async def close(self) -> None:
        if self._task is None:
            return
        if self._queue is not None:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "Supabase queue not drained on shutdown (%d pending)", self._queue.qsize()
                )
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
            logger.warning("Supabase writer dropped %d writes under backpressure", self._dropped)

    # ------------------------------------------------------------------- emit

    def _emit(self, table: str, row: Dict[str, Any], on_conflict: Optional[str] = None) -> None:
        """Queues one row. Never blocks, never raises."""
        if not self.enabled or self._queue is None:
            return
        item = (table, on_conflict, row)
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._dropped += 1

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
        self._emit(
            "executor_events",
            {
                "event_id": event_id or f"{event_type}:{pair_key or '-'}:{stamp}",
                "event_type": event_type,
                "pair_key": pair_key,
                "is_mock": bool(is_mock),
                "occurred_at": _ms_to_iso(stamp),
                "payload": payload or {},
            },
            on_conflict="event_id",
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
        self._emit("positions", row, on_conflict="id")
        self._emit_position_legs(pair_key, pos, pair_info, row["status"], is_mock)

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
        self._emit_position_legs(pair_key, pos, None, status, row.get("is_mock", True))

    def _emit_position_legs(self, pair_key: str, pos: Any, pair_info: Any, status: str, is_mock: bool) -> None:
        if pos is None:
            return
        position_id = self._position_ids.get(pair_key) or _position_uuid(
            pair_key, getattr(pos, "opened_at_ms", 0)
        )
        local_symbol = getattr(pair_info, "local_symbol", "") or getattr(pos, "symbol", "")
        global_symbol = getattr(pair_info, "global_symbol", "") or getattr(pos, "symbol", "")
        common = {
            "position_id": position_id,
            "status": status,
            "qty": _num(getattr(pos, "qty", 0.0)),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._emit("position_legs", {
            **common, "leg": "spot", "exchange_name": getattr(pos, "local_exchange", "") or "",
            "symbol": local_symbol, "side": "long",
            "average_entry_price": _num(getattr(pos, "entry_price_idr", 0.0)) or None,
        }, on_conflict="position_id,leg")
        self._emit("position_legs", {
            **common, "leg": "perpetual", "exchange_name": getattr(pos, "global_exchange", "") or "",
            "symbol": global_symbol, "side": "short",
            "average_entry_price": _num(getattr(pos, "short_entry_usdt", 0.0)) or None,
        }, on_conflict="position_id,leg")

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
        order_identity = f"{exchange}:{key}"
        order_id = self._order_ids.setdefault(order_identity, str(uuid.uuid5(_POSITION_NS, "order:" + order_identity)))

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

        # Only persist exchange-reported trade fills. Cumulative order.filled is
        # persisted on orders, but must not be converted into synthetic fills:
        # after a restart the in-memory delta cursor is gone and would double count.
        for trade in order.get("trades") or []:
            if not isinstance(trade, dict):
                continue
            info = trade.get("info") if isinstance(trade.get("info"), dict) else {}
            exchange_fill_id = str(
                trade.get("id") or info.get("tradeId") or info.get("trade_id") or ""
            ).strip()
            qty = _num(trade.get("amount"), _num(trade.get("qty")))
            price = _num(trade.get("price"))
            if not exchange_fill_id or qty <= 0 or price < 0:
                continue
            timestamp = trade.get("timestamp") or order.get("lastTradeTimestamp")
            filled_at = _ms_to_iso(timestamp) or datetime.now(timezone.utc).isoformat()
            fee = trade.get("fee") if isinstance(trade.get("fee"), dict) else {}
            self._emit(
                "order_fills",
                {
                    "id": str(uuid.uuid5(_POSITION_NS, f"fill:{exchange}:{exchange_fill_id}")),
                    "order_id": order_id,
                    "exchange_name": exchange,
                    "exchange_fill_id": exchange_fill_id,
                    "qty": qty,
                    "price": price,
                    "quote_qty": _num(trade.get("cost"), qty * price),
                    "fee_amount": _num(fee.get("cost")) or None,
                    "fee_asset": fee.get("currency"),
                    "filled_at": filled_at,
                    "raw_payload": trade,
                },
                on_conflict="id",
            )

    # ------------------------------------------------------------ closed trades

    def trade_closed(self, pair_key: str, trade: Dict[str, Any], pos: Any = None) -> None:
        if not isinstance(trade, dict):
            return
        trade_key = str(trade.get("trade_id") or "")
        if not trade_key:
            return
        position_id = self._position_ids.get(pair_key) or (
            _position_uuid(pair_key, getattr(pos, "opened_at_ms", 0)) if pos is not None else None
        )
        self._emit(
            "closed_trades",
            {
                "position_id": position_id,
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

    # ---------------------------------------------------------- other snapshots

    def balance_snapshot(
        self, exchange: str, market_type: str, asset: str, total: Any, free: Any,
        used: Any, *, is_mock: bool = False, observed_at_ms: Optional[int] = None,
    ) -> None:
        stamp = int(observed_at_ms or time.time() * 1000)
        identity = f"balance:{exchange}:{market_type}:{asset}:{stamp}"
        self._emit("balance_snapshots", {
            "id": str(uuid.uuid5(_POSITION_NS, identity)),
            "exchange_name": exchange,
            "market_type": market_type,
            "asset": asset,
            "total": _num(total),
            "free": _num(free),
            "used": _num(used),
            "is_mock": bool(is_mock),
            "observed_at": _ms_to_iso(stamp),
        }, on_conflict="id")

    def risk_event(
        self, event_type: str, message: str, *, pair_key: Optional[str] = None,
        exchange_name: Optional[str] = None, severity: str = "warning",
        action_taken: Optional[str] = None, details: Optional[Dict[str, Any]] = None,
        is_mock: bool = False, occurred_at_ms: Optional[int] = None,
    ) -> None:
        stamp = int(occurred_at_ms or time.time() * 1000)
        event_key = str(uuid.uuid5(
            _POSITION_NS, f"risk:{event_type}:{pair_key or '-'}:{stamp}"
        ))
        self._emit("risk_events", {
            "event_key": event_key,
            "severity": severity if severity in ("info", "warning", "error", "critical") else "warning",
            "event_type": event_type,
            "pair_key": pair_key,
            "exchange_name": exchange_name,
            "message": str(message)[:1000],
            "action_taken": action_taken,
            "is_mock": bool(is_mock),
            "details": details or {},
            "occurred_at": _ms_to_iso(stamp),
        }, on_conflict="event_key")

    def funding_payment(
        self, pair_key: str, exchange: str, symbol: str, payment: Dict[str, Any],
        *, amount: float, asset: str = "USDT", is_mock: bool = False,
    ) -> None:
        if not isinstance(payment, dict):
            return
        info = payment.get("info") if isinstance(payment.get("info"), dict) else {}
        timestamp = int(payment.get("timestamp") or info.get("timestamp") or 0)
        raw_id = str(payment.get("id") or info.get("tranId") or info.get("id") or "").strip()
        identity = f"{exchange}:{symbol}:{raw_id}" if raw_id else f"{exchange}:{symbol}:{timestamp}:{amount:.18f}:{asset}"
        payment_id = str(uuid.uuid5(_POSITION_NS, f"funding:{identity}"))
        position_id = self._position_ids.get(pair_key)
        self._emit("funding_payments", {
            "id": payment_id,
            "position_id": position_id,
            "exchange_name": exchange,
            "symbol": symbol,
            "exchange_payment_id": raw_id or None,
            "funding_rate": _num(payment.get("rate")) or None,
            "position_qty": _num(payment.get("contracts")) or None,
            "mark_price": _num(payment.get("price")) or None,
            "amount": _num(amount),
            "asset": asset,
            "payment_status": "confirmed",
            "settled_at": _ms_to_iso(timestamp) or datetime.now(timezone.utc).isoformat(),
            "raw_payload": payment,
        }, on_conflict="id")

    def status_snapshot(self, snapshot: Dict[str, Any], *, is_mock: bool = False) -> None:
        now = time.monotonic()
        if now - self._last_status_snapshot < 10.0:
            return
        self._last_status_snapshot = now
        instance_id = str(uuid.uuid5(_POSITION_NS, f"executor-instance:{id(self)}"))
        self._emit("executor_status_snapshots", {
            "id": str(uuid.uuid4()),
            "instance_id": instance_id,
            "status": str(snapshot.get("status") or "UNKNOWN"),
            "mode": "MOCK" if is_mock else "LIVE",
            "is_mock": bool(is_mock),
            "snapshot": snapshot,
            "observed_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="id")

    # ------------------------------------------------------------------- flush

    async def _run(self) -> None:
        while True:
            batch = []
            try:
                batch = [await self._queue.get()]
                while len(batch) < _BATCH_MAX:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                await self._flush(batch)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("supabase writer loop error")
            finally:
                for _ in batch:
                    self._queue.task_done()

    async def _flush(self, batch: list) -> None:
        grouped: Dict[tuple, list] = {}
        for table, on_conflict, row in batch:
            # PostgREST rejects a batch whose objects have different key sets, so
            # the key signature is part of the grouping key.
            grouped.setdefault((table, on_conflict, frozenset(row)), []).append(row)

        for key, rows in list(grouped.items()):
            # An upsert cannot touch the same row twice in one statement, and a
            # newer queued write always supersedes an older one for the same row.
            table, on_conflict, _keys = key
            conflict_cols = [c.strip() for c in (on_conflict or "id").split(",")]
            by_row: Dict[tuple, Dict[str, Any]] = {}
            for row in rows:
                by_row[tuple(str(row.get(c)) for c in conflict_cols)] = row
            grouped[key] = list(by_row.values())

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_TIMEOUT,
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                    # This host's aiohttp cannot decode brotli; ask for identity.
                    "Accept-Encoding": "identity",
                },
            )

        for (table, on_conflict, _keys), rows in grouped.items():
            url = f"{self.url}/rest/v1/{table}"
            if on_conflict:
                url = f"{url}?on_conflict={on_conflict}"
            for attempt in range(1, _MAX_ATTEMPTS + 1):
                try:
                    async with self._session.post(url, json=rows) as resp:
                        if resp.status < 300:
                            break
                        body = (await resp.text())[:300]
                        raise RuntimeError(f"HTTP {resp.status}: {body}")
                except Exception as exc:
                    if attempt == _MAX_ATTEMPTS:
                        logger.warning(
                            "supabase write failed table=%s rows=%d: %s", table, len(rows), exc
                        )
                    else:
                        await asyncio.sleep(0.5 * attempt)
