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
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

from executor.supabase_outbox import DurableOutbox

logger = logging.getLogger("executor.supabase")

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

# Order role -> (leg per market, side, market) for the universal order registry.
# Sides are canonical because the schema stores `side` in ('buy','sell') while
# the bot describes the futures leg as "short". A role maps to different legs per
# market: FORCE_CLOSE buys on perpetual (close the short) but sells on spot, and
# the schema's leg vocabulary distinguishes them.
_ROLE_CATALOGUE = {
    "ENTRY": ({"spot": "spot_entry"}, "buy", "spot"),
    "HEDGE": ({"perpetual": "futures_entry"}, "sell", "perpetual"),
    "EXIT_SPOT": ({"spot": "spot_exit"}, "sell", "spot"),
    "EXIT_FUTURES": ({"perpetual": "futures_exit"}, "buy", "perpetual"),
    "EMERGENCY": ({"spot": "rollback"}, "sell", "spot"),
    "ROLLBACK": ({"spot": "rollback"}, "sell", "spot"),
    "FORCE_CLOSE": ({"spot": "force_close", "perpetual": "futures_exit"}, "sell", "spot"),
}

_LEG_TO_ROLE = {
    "spot_entry": "ENTRY",
    "futures_entry": "HEDGE",
    "spot_exit": "EXIT_SPOT",
    "futures_exit": "EXIT_FUTURES",
    "rollback": "ROLLBACK",
    "force_close": "FORCE_CLOSE",
}

def _role_catalogue(role: Any, leg: Optional[str], market: Any = "") -> tuple:
    """Returns (side, leg, market_type) for a registry row, filling gaps only."""
    key = str(role or "").upper()
    legs, side, default_market = _ROLE_CATALOGUE.get(
        key, ({"spot": "spot_entry"}, "sell", "spot")
    )
    resolved_market = "perpetual" if str(market or "").lower() in ("perpetual", "futures", "swap") else default_market
    if leg:
        resolved_leg = str(leg)
        resolved_market = "perpetual" if resolved_leg in ("futures_entry", "futures_exit") else "spot"
    else:
        resolved_leg = legs.get(resolved_market) or next(iter(legs.values()))
    return side, resolved_leg, resolved_market

def _canonical_side(value: Any) -> Optional[str]:
    """Maps a direction onto the schema's buy/sell vocabulary, or None.

    A FORCE_CLOSE role covers both a spot sell and a futures buy, so the role
    alone cannot state the side; the caller's own direction is authoritative.
    """
    side = str(value or "").strip().lower()
    if side in ("buy", "long"):
        return "buy"
    if side in ("sell", "short"):
        return "sell"
    return None
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
        self._order_ids: Dict[str, str] = {}
        self._order_submitted_at: Dict[str, str] = {}
        self._order_completed_at: Dict[str, str] = {}

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

    def _emit(self, table: str, row: Dict[str, Any], on_conflict: Optional[str] = None) -> bool:
        """Queues one row. Never blocks, never raises; reports journal success."""
        if not self.enabled or self._outbox is None:
            return False
        try:
            self._outbox.enqueue(table, on_conflict, row)
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: durable Supabase outbox enqueue failed for table=%s", table)
            return False
        # The worker polls the durable journal. No in-memory queue is needed,
        # so process restarts and backpressure cannot discard accepted rows.
        return True

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
        self._emit("positions", row, on_conflict="id")

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

    def balance_snapshot(
        self,
        *,
        exchange: str,
        market_type: str,
        asset: str,
        total: float,
        free: float,
        used: float,
        is_mock: bool = True,
        raw_payload: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Append one validated exchange balance observation.

        Rows are intentionally append-only: each poll is a point-in-time
        snapshot, not an upsert that could erase evidence of prior drift.
        """
        exchange_name = str(exchange or "").strip().lower()
        market = str(market_type or "").strip().lower()
        currency = str(asset or "").strip().upper()
        if not exchange_name or market not in ("spot", "perpetual") or not currency:
            return False
        try:
            values = [float(total), float(free), float(used)]
        except (TypeError, ValueError):
            return False
        if any(not math.isfinite(value) or value < 0 for value in values):
            return False
        return self._emit(
            "balance_snapshots",
            {
                "exchange_name": exchange_name,
                "market_type": market,
                "asset": currency,
                "total": values[0],
                "free": values[1],
                "used": values[2],
                "is_mock": bool(is_mock),
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "raw_payload": raw_payload or {},
            },
        )

    # ------------------------------------------------------------------ orders

    def order_record(
        self,
        row: Dict[str, Any],
        leg: Optional[str] = None,
        market_type: Optional[str] = None,
        limit_price: Optional[float] = None,
    ) -> bool:
        """Upserts one universal order-registry row.

        ``local_order_id`` is the primary key and is derived from the client
        order id, so a re-submitted or re-adopted order updates its own row
        instead of inserting a duplicate. ``pair_key`` and ``position_key`` are
        only sent when they are non-null, keeping the newest known linkage
        instead of erasing it with a null.
        """
        local_order_id = str(row.get("local_order_id") or "")
        if not local_order_id:
            return
        role = str(row.get("order_role") or "ENTRY")
        fallback_side, resolved_leg, resolved_market = _role_catalogue(
            role, leg, market_type or row.get("market_type")
        )
        payload = {
            "local_order_id": local_order_id,
            "exchange_name": str(row.get("exchange_name") or ""),
            "market_type": resolved_market,
            "leg": resolved_leg,
            "order_role": role,
            "symbol": str(row.get("symbol") or ""),
            "side": _canonical_side(row.get("side")) or fallback_side,
            "order_type": str(row.get("order_type") or "limit"),
            "status": str(row.get("status") or "open"),
            "requested_qty": _num(row.get("original_quantity")),
            "filled_qty": _num(row.get("filled_quantity")),
            "remaining_qty": max(
                0.0,
                _num(row.get("remaining_quantity"), _num(row.get("original_quantity")) - _num(row.get("filled_quantity"))),
            ),
            "average_fill_price": _num(row.get("average_fill_price")) or None,
            "limit_price": limit_price if limit_price is not None else (_num(row.get("price")) or None),
            "reduce_only": bool(row.get("reduce_only", False)),
            "recovery_status": str(row.get("recovery_status") or "MANAGED"),
            "is_mock": bool(row.get("is_mock", True)),
            "last_reconciled_at": datetime.now(timezone.utc).isoformat(),
            # Carries the entry lifecycle context (FX snapshot, fee profile,
            # amount rules) that a post-restart hedge cannot re-derive.
            "raw_payload": json.dumps(
                {
                    key: row.get(key)
                    for key in (
                        "local_amount_spec", "global_amount_spec", "recovery_context",
                        "hedged_quantity", "orphan_id", "scope", "reason", "detected_at",
                    )
                    if row.get(key) is not None
                },
                default=str,
            ),
        }
        exchange_order_id = str(row.get("exchange_order_id") or "")
        if exchange_order_id:
            payload["exchange_order_id"] = exchange_order_id
        client_order_id = str(row.get("client_order_id") or "")
        if client_order_id:
            payload["client_order_id"] = client_order_id
        # Kept distinct: a position key ("position:<pair>") is not a pair key, and
        # writing one into the other would corrupt every recovery lookup.
        pair_key = str(row.get("pair_key") or "")
        if pair_key:
            payload["pair_key"] = pair_key
        position_key = str(row.get("position_key") or "")
        if position_key:
            payload["position_key"] = position_key
        if isinstance(row.get("raw_payload"), str):
            payload["raw_payload"] = row["raw_payload"]
        registry_ok = self._emit("order_registry", payload, on_conflict="local_order_id")
        # Every bot-owned order also needs an analytics parent row because
        # order_fills.order_id references public.orders, not order_registry.
        # This covers futures hedges/rollbacks that do not call the legacy
        # pair-scoped order() callback directly.
        analytics_ok = True
        if pair_key and (client_order_id or exchange_order_id):
            order_key = client_order_id or exchange_order_id
            order_row_id = self.order_id_for(pair_key, payload["exchange_name"], order_key)
            submitted_at = _ms_to_iso(row.get("created_at")) or datetime.now(timezone.utc).isoformat()
            analytics_ok = self._emit(
                "orders",
                {
                    "id": order_row_id,
                    "position_id": self._position_ids.get(pair_key),
                    "pair_key": pair_key,
                    "exchange_name": payload["exchange_name"],
                    "market_type": resolved_market,
                    "leg": resolved_leg,
                    "exchange_order_id": exchange_order_id or None,
                    "client_order_id": client_order_id or None,
                    "symbol": payload["symbol"],
                    "side": payload["side"],
                    "order_type": payload["order_type"],
                    "status": payload["status"],
                    "requested_qty": payload["requested_qty"],
                    "filled_qty": payload["filled_qty"],
                    "remaining_qty": payload["remaining_qty"],
                    "limit_price": payload["limit_price"],
                    "average_fill_price": payload["average_fill_price"],
                    "reduce_only": payload["reduce_only"],
                    "is_mock": payload["is_mock"],
                    "submitted_at": submitted_at,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "raw_payload": {
                        "source": "universal_order_registry",
                        "local_order_id": local_order_id,
                        "local_amount_spec": row.get("local_amount_spec"),
                        "global_amount_spec": row.get("global_amount_spec"),
                        "recovery_context": row.get("recovery_context"),
                    },
                },
                on_conflict="id",
            )
        return bool(registry_ok and analytics_ok)

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
        submitted_at_ms: Optional[int] = None,
    ) -> None:
        """Upserts the order lifecycle row; individual fills come from venue trade history."""
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
        now_iso = datetime.now(timezone.utc).isoformat()
        submitted_at = (
            _ms_to_iso(submitted_at_ms)
            or _ms_to_iso(order.get("timestamp"))
            or self._order_submitted_at.setdefault(order_state_key, now_iso)
        )
        terminal_status = str(order.get("status") or "").lower() in (
            "closed", "filled", "canceled", "cancelled", "rejected", "expired"
        )
        completed_at = (
            self._order_completed_at.setdefault(order_state_key, now_iso)
            if terminal_status else self._order_completed_at.get(order_state_key)
        )
        return self._emit(
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
                "is_mock": bool(is_mock),
                "submitted_at": submitted_at,
                "completed_at": completed_at,
                "updated_at": now_iso,
                "raw_payload": order,
            },
            on_conflict="id",
        )

    def order_id_for(
        self, pair_key: str, exchange: str, key: str
    ) -> str:
        """Returns the same stable order UUID used by the order lifecycle writer."""
        order_state_key = f"{exchange}:{pair_key}:{key}"
        return self._order_ids.setdefault(
            order_state_key, str(uuid.uuid5(_POSITION_NS, f"order:{order_state_key}"))
        )

    def order_fill(
        self,
        *,
        order_id: str,
        exchange: str,
        fill: Dict[str, Any],
        is_mock: bool = False,
    ) -> bool:
        """Journals one exchange execution by its immutable venue trade id."""
        if not isinstance(fill, dict) or not order_id:
            return False
        fill_id = str(fill.get("id") or fill.get("trade_id") or "").strip()
        if not fill_id:
            return False
        try:
            qty = float(fill.get("amount"))
            price = float(fill.get("price"))
            quote_qty = float(fill.get("cost") if fill.get("cost") is not None else qty * price)
            timestamp = int(fill.get("timestamp") or 0)
        except (TypeError, ValueError):
            return False
        if (
            not all(math.isfinite(value) for value in (qty, price, quote_qty))
            or qty <= 0 or price <= 0 or quote_qty < 0 or timestamp <= 0
        ):
            return False
        fee = fill.get("fee") if isinstance(fill.get("fee"), dict) else {}
        fee_cost = fee.get("cost")
        try:
            fee_amount = float(fee_cost) if fee_cost is not None else None
        except (TypeError, ValueError):
            return False
        if fee_amount is not None and not math.isfinite(fee_amount):
            return False
        fee_asset = str(fee.get("currency") or "").strip().upper() or None
        row = {
            "id": str(uuid.uuid5(_POSITION_NS, f"fill:{exchange}:{fill_id}")),
            "order_id": order_id,
            "exchange_name": str(exchange).strip().lower(),
            "exchange_fill_id": fill_id,
            "qty": qty,
            "price": price,
            "quote_qty": quote_qty,
            "fee_amount": fee_amount,
            "fee_asset": fee_asset,
            "filled_at": datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat(),
            "raw_payload": fill.get("info") if isinstance(fill.get("info"), dict) else fill,
        }
        if not self.enabled or self._outbox is None:
            return False
        try:
            self._outbox.enqueue_once(
                "order_fills", "id", row, f"exchange-fill:{exchange}:{fill_id}"
            )
            # False from enqueue_once means this immutable fill was already
            # journaled (or delivered) by an earlier reconciliation pass.
            return True
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: failed to journal exchange fill %s", fill_id)
            return False

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
                    "spot_fees_usdt": _num(trade.get("spot_fees_usdt")),
                    "futures_fees_usdt": _num(trade.get("futures_fees_usdt")),
                    "fee_source": str(trade.get("fee_source") or "LEGACY_UNVERIFIED"),
                    "fee_verified": bool(trade.get("fee_verified", False)),
                    "fee_estimation": "four_fill_round_trip",
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

    async def fetch_recent_order_fill_candidates(self, days: int = 90, limit: int = 2000) -> list:
        """Read recent live order rows to replay immutable fills after a restart."""
        if not self.enabled or not self.url or not self.key:
            return []
        cutoff = datetime.fromtimestamp(
            time.time() - max(1, int(days)) * 86400, tz=timezone.utc
        ).isoformat()
        params = {
            "select": (
                "id,pair_key,exchange_name,market_type,leg,exchange_order_id,"
                "client_order_id,symbol,side,filled_qty,submitted_at,updated_at,is_mock"
            ),
            "is_mock": "eq.false",
            "updated_at": f"gte.{cutoff}",
            "order": "updated_at.desc",
            "limit": str(max(1, min(int(limit), 10000))),
        }
        session = self._session
        created = False
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=_TIMEOUT)
                created = True
            headers = {
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            }
            async with session.get(
                f"{self.url}/rest/v1/orders", params=params, headers=headers
            ) as resp:
                if resp.status >= 300:
                    body = (await resp.text())[:500]
                    raise RuntimeError(
                        f"recent order fill candidate read failed with HTTP {resp.status}: {body}"
                    )
                rows = await resp.json()
        except Exception as exc:
            logger.error("recent order fill candidate read failed: %s", exc)
            raise RuntimeError("recent order fill candidates could not be read") from exc
        finally:
            if created and session is not None:
                try:
                    await session.close()
                except Exception:
                    pass
        if not isinstance(rows, list):
            raise RuntimeError("recent order fill candidates returned a malformed payload")
        return [row for row in rows if isinstance(row, dict)]

    async def fetch_open_order_records(self, limit: int = 500) -> list:
        """Reads non-terminal order-registry rows for startup recovery.

        The outbox is write-only, so a flushed or partial Redis cache needs a
        read path back into the durable record. A disabled writer returns an
        empty list; an enabled-but-unreadable registry raises so startup can
        fail closed instead of mistaking a database outage for zero orders.
        """
        if not self.enabled or not self.url or not self.key:
            return []
        params = {
            "select": (
                "local_order_id,client_order_id,exchange_order_id,pair_key,position_key,"
                "exchange_name,market_type,leg,order_role,symbol,side,order_type,status,"
                "requested_qty,filled_qty,remaining_qty,limit_price,average_fill_price,"
                "reduce_only,recovery_status,is_mock,raw_payload"
            ),
            "recovery_status": "in.(MANAGED,ADOPTED,UNVERIFIED)",
            "status": "not.in.(closed,filled,canceled,cancelled,rejected,expired)",
            "order": "updated_at.desc",
            "limit": str(max(1, int(limit))),
        }
        session = self._session
        created = False
        try:
            if session is None or session.closed:
                session = aiohttp.ClientSession(timeout=_TIMEOUT)
                created = True
            headers = {
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            }
            async with session.get(
                f"{self.url}/rest/v1/order_registry", params=params, headers=headers
            ) as resp:
                if resp.status >= 300:
                    body = (await resp.text())[:500]
                    raise RuntimeError(
                        f"order registry read failed with HTTP {resp.status}: {body}"
                    )
                rows = await resp.json()
        except Exception as exc:
            logger.error("order registry read failed: %s", exc)
            raise RuntimeError("durable order registry could not be read") from exc
        finally:
            if created and session is not None:
                try:
                    await session.close()
                except Exception:
                    pass
        if not isinstance(rows, list):
            raise RuntimeError("order registry read returned a malformed payload")
        return [row for row in rows if isinstance(row, dict)]

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
