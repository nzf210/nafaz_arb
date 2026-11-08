"""Order and fill persistence mixin."""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from executor.supabase_helpers import _num, _POSITION_NS, _SIDE_BY_LEG


class SupabaseOrderMixin:
    def order(
        self, pair_key: str, leg: str, exchange: str, market_type: str,
        symbol: str, order: Dict[str, Any], *, client_order_id: Optional[str] = None,
        reduce_only: bool = False, is_mock: bool = True,
    ) -> None:
        if not isinstance(order, dict):
            return
        exchange_order_id = str(order.get("id") or "") or None
        key = client_order_id or exchange_order_id
        if not key:
            return
        order_id = self._order_ids.setdefault(key, str(uuid.uuid4()))
        requested = _num(order.get("amount"), _num(order.get("filled")))
        filled = _num(order.get("filled"))
        self._emit(
            "orders",
            self._build_order_row(
                pair_key, leg, exchange, market_type, symbol, order,
                order_id, exchange_order_id, client_order_id, requested, filled,
                reduce_only, is_mock,
            ),
            on_conflict="id",
        )
        self._emit_new_order_fill(key, pair_key, exchange, order_id, order, filled)

    def _build_order_row(
        self, pair_key, leg, exchange, market_type, symbol, order,
        order_id, exchange_order_id, client_order_id, requested, filled,
        reduce_only, is_mock,
    ) -> dict:
        status = str(order.get("status") or "open")
        return {
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
            "status": status,
            "requested_qty": requested,
            "filled_qty": filled,
            "remaining_qty": max(0.0, requested - filled),
            "limit_price": _num(order.get("price")) or None,
            "average_fill_price": _num(order.get("average")) or None,
            "reduce_only": bool(reduce_only),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": self._completed_order_timestamp(status),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "raw_payload": order,
            "is_mock": bool(is_mock),
        }

    @staticmethod
    def _completed_order_timestamp(status: str):
        if status.lower() in ("closed", "canceled", "cancelled", "rejected", "expired"):
            return datetime.now(timezone.utc).isoformat()
        return None

    def _emit_new_order_fill(self, key: str, pair_key: str, exchange: str, order_id: str, order: dict, filled: float) -> None:
        previous = self._order_filled.get(key, 0.0)
        delta = filled - previous
        if delta <= 1e-12:
            return
        self._order_filled[key] = filled
        price = _num(order.get("average"), _num(order.get("price")))
        exchange_order_id = str(order.get("id") or "") or key
        fill_id = f"{exchange_order_id}:{filled:.18f}"
        row = self._build_order_fill_row(order_id, exchange, fill_id, delta, price)
        self._emit("order_fills", row, on_conflict="id")

    @staticmethod
    def _build_order_fill_row(order_id: str, exchange: str, fill_id: str, qty: float, price: float) -> dict:
        return {
            "id": str(uuid.uuid5(_POSITION_NS, f"fill:{exchange}:{fill_id}")),
            "order_id": order_id,
            "exchange_name": exchange,
            "exchange_fill_id": fill_id,
            "qty": qty,
            "price": price,
            "quote_qty": qty * price,
            "filled_at": datetime.now(timezone.utc).isoformat(),
            "raw_payload": {"derived_from": "cumulative_filled"},
        }
