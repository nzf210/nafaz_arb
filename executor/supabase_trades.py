"""supabase trade operations for the durable Supabase writer."""

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

from executor.supabase_common import _ms_to_iso, _num



class SupabaseTradeMixin:
    """Focused writer methods grouped by persistence responsibility."""

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
