"""supabase order operations for the durable Supabase writer."""

import json
import uuid
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from executor.supabase_common import _POSITION_NS, _SIDE_BY_LEG, _canonical_side, _ms_to_iso, _num, _role_catalogue, logger



class SupabaseOrderMixin:
    """Focused writer methods grouped by persistence responsibility."""

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
            return False
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
                    "completed_at": (
                        datetime.now(timezone.utc).isoformat()
                        if str(payload["status"]).lower() in (
                            "closed", "filled", "canceled", "cancelled", "rejected", "expired"
                        ) else None
                    ),
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
    ) -> bool:
        """Upserts the order lifecycle row; individual fills come from venue trade history."""
        if not isinstance(order, dict):
            return False
        exchange_order_id = str(order.get("id") or "") or None
        key = client_order_id or exchange_order_id
        if not key:
            return False
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

    def fill_sync_watermark(self, identity: str) -> Optional[float]:
        if self._outbox is None:
            return None
        return self._outbox.get_numeric_state(f"fill-sync:{identity}")

    def mark_fill_sync(self, identity: str, cumulative_filled: float) -> bool:
        if self._outbox is None or not self.enabled:
            return False
        try:
            self._outbox.set_numeric_state(
                f"fill-sync:{identity}", float(cumulative_filled)
            )
            return True
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: failed to persist fill reconciliation watermark for %s", identity)
            return False

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
        fee_rows = fill.get("fees") if isinstance(fill.get("fees"), list) else []
        row = {
            "id": str(uuid.uuid5(_POSITION_NS, f"fill:{exchange}:{fill_id}")),
            "order_id": order_id,
            "exchange_name": str(exchange).strip().lower(),
            "exchange_fill_id": fill_id,
            "fill_source": "exchange_trade",
            "qty": qty,
            "price": price,
            "quote_qty": quote_qty,
            "fee_amount": fee_amount,
            "fee_asset": fee_asset,
            "fee_verified": (
                fee_amount is not None and bool(fee_asset) and len(fee_rows) <= 1
            ),
            "filled_at": datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).isoformat(),
            "raw_payload": (
                fill.get("raw_payload") if isinstance(fill.get("raw_payload"), dict)
                else fill.get("info") if isinstance(fill.get("info"), dict)
                else fill
            ),
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
