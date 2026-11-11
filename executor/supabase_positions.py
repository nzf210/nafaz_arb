"""supabase position operations for the durable Supabase writer."""

import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from executor.supabase_common import _ms_to_iso, _num, _position_uuid, logger



class SupabasePositionMixin:
    """Focused writer methods grouped by persistence responsibility."""

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
        self._position_legs(pair_key, position_id, pos, pair_info, status=row["status"], is_mock=is_mock)

    def _position_legs(
        self, pair_key: str, position_id: str, pos: Any, pair_info: Any,
        *, status: str, is_mock: bool,
    ) -> None:
        """Upsert spot and perpetual legs independently from the parent position."""
        if pos is None:
            return
        now = datetime.now(timezone.utc).isoformat()
        parent = self._position_rows.get(pair_key, {})
        local_exchange = str(getattr(pos, "local_exchange", "") or parent.get("local_exchange") or "")
        global_exchange = str(getattr(pos, "global_exchange", "") or parent.get("global_exchange") or "")
        local_symbol = str(
            getattr(pair_info, "local_symbol", "") or parent.get("local_symbol")
            or getattr(pos, "symbol", "") or ""
        )
        global_symbol = str(
            getattr(pair_info, "global_symbol", "") or parent.get("global_symbol")
            or getattr(pos, "symbol", "") or ""
        )
        qty = max(0.0, _num(getattr(pos, "qty", 0.0)))
        spot_price = _num(getattr(pos, "entry_price_idr", 0.0)) or None
        perp_price = _num(getattr(pos, "short_entry_usdt", 0.0)) or None
        for leg, exchange, symbol, side, price in (
            ("spot", local_exchange, local_symbol, "long", spot_price),
            ("perpetual", global_exchange, global_symbol, "short", perp_price),
        ):
            if not exchange or not symbol:
                continue
            leg_id = str(uuid.uuid5(_POSITION_NS, f"{position_id}:{leg}"))
            self._emit(
                "position_legs",
                {
                    "id": leg_id,
                    "position_id": position_id,
                    "leg": leg,
                    "exchange_name": exchange,
                    "symbol": symbol,
                    "side": side,
                    "status": status,
                    "qty": qty,
                    "average_entry_price": price,
                    "updated_at": now,
                },
                on_conflict="position_id,leg",
            )

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
        self._position_legs(
            pair_key, position_id, pos, None, status=status,
            is_mock=bool(row.get("is_mock", True)),
        )

    def funding_payment(
        self, pair_key: str, pos: Any, payment: Dict[str, Any], *, is_mock: bool = False
    ) -> bool:
        """Journal one confirmed exchange funding settlement idempotently."""
        position_id = self._position_ids.get(pair_key)
        if not position_id or not isinstance(payment, dict):
            return False
        info = payment.get("info") if isinstance(payment.get("info"), dict) else {}
        raw_id = (
            payment.get("id") or payment.get("tranId") or payment.get("tradeId")
            or info.get("id") or info.get("tranId") or info.get("billId")
        )
        try:
            timestamp = int(payment.get("timestamp") or info.get("timestamp") or 0)
            amount = float(payment.get("amount"))
        except (TypeError, ValueError):
            return False
        asset = str(payment.get("code") or payment.get("currency") or info.get("currency") or "USDT").upper()
        symbol = str(payment.get("symbol") or getattr(pos, "symbol", "") or "")
        exchange = str(getattr(pos, "global_exchange", "") or "").lower()
        if timestamp <= 0 or not math.isfinite(amount) or not asset or not symbol or not exchange:
            return False
        # Some venues omit a payment ID. A deterministic composite prevents
        # duplicate replay of the same ledger row without guessing its value.
        payment_id = str(raw_id or uuid.uuid5(
            _POSITION_NS, f"{exchange}:{symbol}:{timestamp}:{asset}:{amount:.18g}"
        ))
        return self._emit_once(
            "funding_payments",
            {
                "id": str(uuid.uuid5(_POSITION_NS, f"funding:{exchange}:{payment_id}")),
                "position_id": position_id,
                "exchange_name": exchange,
                "symbol": symbol,
                "exchange_payment_id": payment_id,
                "funding_rate": _num(payment.get("rate", info.get("fundingRate")), 0.0) or None,
                "position_qty": (
                    _num(payment.get("contracts", info.get("positionAmt")))
                    if payment.get("contracts", info.get("positionAmt")) is not None else None
                ),
                "mark_price": _num(payment.get("price", info.get("markPrice"))) or None,
                "amount": amount,
                "asset": asset,
                "payment_status": "confirmed",
                "settled_at": _ms_to_iso(timestamp),
                "raw_payload": payment,
            },
            state_key=f"funding:{exchange}:{payment_id}",
            on_conflict="id",
        )

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
