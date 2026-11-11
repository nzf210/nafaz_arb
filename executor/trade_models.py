"""trade models definitions."""

from dataclasses import dataclass, field
from typing import Dict, Any
import time


class ClosedTrade:
    trade_id: str
    local_exchange: str
    global_exchange: str
    symbol: str
    qty: float
    entry_price_idr: float
    exit_price_idr: float
    short_entry_usdt: float
    short_exit_usdt: float
    spot_pnl_idr: float
    futures_pnl_usdt: float
    accumulated_funding_usdt: float
    total_fees_usdt: float
    net_profit_usdt: float
    net_profit_idr: float
    net_roi_pct: float
    is_mock: bool = True
    opened_at_ms: int = 0
    closed_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "local_exchange": self.local_exchange,
            "global_exchange": self.global_exchange,
            "symbol": self.symbol,
            "qty": self.qty,
            "entry_price_idr": self.entry_price_idr,
            "exit_price_idr": self.exit_price_idr,
            "short_entry_usdt": self.short_entry_usdt,
            "short_exit_usdt": self.short_exit_usdt,
            "spot_pnl_idr": self.spot_pnl_idr,
            "futures_pnl_usdt": self.futures_pnl_usdt,
            "accumulated_funding_usdt": self.accumulated_funding_usdt,
            "total_fees_usdt": self.total_fees_usdt,
            "net_profit_usdt": self.net_profit_usdt,
            "net_profit_idr": self.net_profit_idr,
            "net_roi_pct": self.net_roi_pct,
            "is_mock": self.is_mock,
            "opened_at_ms": self.opened_at_ms,
            "closed_at_ms": self.closed_at_ms,
        }
