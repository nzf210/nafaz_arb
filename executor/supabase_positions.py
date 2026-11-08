"""Position and closed-trade row persistence mixin."""

from datetime import datetime, timezone
from typing import Any, Dict

from executor.supabase_helpers import _ms_to_iso, _num, _position_uuid


class SupabasePositionMixin:
    def position_opened(self, pair_key: str, pos: Any, pair_info: Any, is_mock: bool = True) -> None:
        position_id = self._position_ids.get(pair_key)
        if position_id is None:
            opened_at = getattr(pos, "opened_at_ms", 0) if pos else 0
            position_id = _position_uuid(pair_key, opened_at)
            self._position_ids[pair_key] = position_id
        row = self._build_position_row(position_id, pair_key, pos, pair_info, is_mock)
        self._position_rows[pair_key] = row
        self._emit("positions", row, on_conflict="id")

    def _build_position_row(self, position_id: str, pair_key: str, pos: Any, pair_info: Any, is_mock: bool) -> dict:
        return {
            **self._position_identity_fields(position_id, pair_key, pos, pair_info),
            **self._position_market_fields(pos, is_mock),
            **self._position_accounting_fields(pos),
            **self._position_timestamp_fields(pos),
        }

    @staticmethod
    def _position_identity_fields(position_id: str, pair_key: str, pos: Any, pair_info: Any) -> dict:
        return {
            "id": position_id,
            "pair_key": pair_key,
            "local_exchange": getattr(pos, "local_exchange", "") or "",
            "global_exchange": getattr(pos, "global_exchange", "") or "",
            "local_symbol": getattr(pair_info, "local_symbol", "") or getattr(pos, "symbol", ""),
            "global_symbol": getattr(pos, "global_symbol", "") or getattr(pair_info, "global_symbol", "") or getattr(pos, "symbol", ""),
            "base_asset": getattr(pair_info, "base_asset", "") or "",
            "quote_asset": getattr(pair_info, "quote_asset", "") or "",
        }

    @staticmethod
    def _position_market_fields(pos: Any, is_mock: bool) -> dict:
        return {
            "status": getattr(pos, "state", "HEDGED_ACTIVE") or "HEDGED_ACTIVE",
            "execution_mode": "MAKER_TAKER",
            "is_mock": bool(is_mock),
        }

    @staticmethod
    def _position_accounting_fields(pos: Any) -> dict:
        return {
            "qty": _num(getattr(pos, "qty", 0.0)),
            "initial_qty": _num(getattr(pos, "initial_qty", 0.0) or getattr(pos, "qty", 0.0)),
            "spot_entry_avg_price": _num(getattr(pos, "entry_price_idr", 0.0)),
            "futures_short_entry_avg_price": _num(getattr(pos, "short_entry_usdt", 0.0)),
            "spot_cost_quote": _num(getattr(pos, "spot_cost_usdt", 0.0)),
            "futures_margin_quote": _num(getattr(pos, "futures_margin_usdt", 0.0)),
            "accumulated_funding": _num(getattr(pos, "accumulated_funding_usdt", 0.0)),
            "current_funding_rate": _num(getattr(pos, "current_funding_rate", 0.0)),
        }

    @staticmethod
    def _position_timestamp_fields(pos: Any) -> dict:
        return {
            "opened_at": _ms_to_iso(getattr(pos, "opened_at_ms", 0)),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "state_snapshot": pos.to_dict() if hasattr(pos, "to_dict") else {},
        }

    def position_status(self, pair_key: str, status: str, pos: Any = None) -> None:
        position_id = self._position_ids.get(pair_key)
        if position_id is None:
            return
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

    def trade_closed(self, pair_key: str, trade: Dict[str, Any], pos: Any = None) -> None:
        if not isinstance(trade, dict):
            return
        trade_key = str(trade.get("trade_id") or "")
        if not trade_key:
            return
        row = self._build_closed_trade_row(pair_key, trade, pos, trade_key)
        self._emit("closed_trades", row, on_conflict="trade_key")
        self.position_status(pair_key, "CLOSED", pos)
        self.event(
            "POSITION_CLOSED", pair_key=pair_key,
            is_mock=bool(trade.get("is_mock", True)),
            occurred_at_ms=trade.get("closed_at_ms"), payload=trade,
        )

    def _build_closed_trade_row(self, pair_key: str, trade: dict, pos: Any, trade_key: str) -> dict:
        return {
            "position_id": self._position_ids.get(pair_key),
            "trade_key": trade_key,
            "pair_key": pair_key,
            **self._closed_trade_identity(trade),
            **self._closed_trade_pnl(trade),
            **self._closed_trade_timestamps(trade, pos),
            "metadata": {
                "pnl_complete": trade.get("pnl_complete"),
                "spot_liquidation_failed": trade.get("spot_liquidation_failed"),
            },
        }

    @staticmethod
    def _closed_trade_identity(trade: dict) -> dict:
        return {
            "local_exchange": str(trade.get("local_exchange") or ""),
            "global_exchange": str(trade.get("global_exchange") or ""),
            "symbol": str(trade.get("symbol") or ""),
            "qty": _num(trade.get("qty")),
            "is_mock": bool(trade.get("is_mock", True)),
            "close_reason": trade.get("close_reason"),
        }

    @staticmethod
    def _closed_trade_pnl(trade: dict) -> dict:
        return {
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
        }

    @staticmethod
    def _closed_trade_timestamps(trade: dict, pos: Any) -> dict:
        return {
            "opened_at": _ms_to_iso(getattr(pos, "opened_at_ms", 0)) if pos else None,
            "closed_at": _ms_to_iso(trade.get("closed_at_ms")) or datetime.now(timezone.utc).isoformat(),
        }
