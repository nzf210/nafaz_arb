"""Authenticated INDODAX order-history and individual-fill methods."""

import time
from typing import Any, Dict, List, Optional


def _history_symbol(symbol: str) -> str:
    return "".join(ch for ch in str(symbol or "") if ch.isalnum()).upper()


def _history_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


class IndodaxHistoryMixin:
    """Owns terminal-order and authenticated fill-history operations."""

    async def _fetch_order_histories(
        self, symbol: str, since: Optional[int] = None, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch terminal-order history within the venue's seven-day query window."""
        if not symbol:
            raise ValueError("fetch_order_histories requires a symbol")
        now_ms = int(time.time() * 1000)
        start_ms = int(since) if since is not None else now_ms - 24 * 60 * 60 * 1000
        max_window_ms = 7 * 24 * 60 * 60 * 1000
        if start_ms < now_ms - max_window_ms:
            raise ValueError(
                "Indodax order history supports at most a seven-day window; "
                "use exact origClientOrderId lookup for older orders"
            )
        query = {
            "symbol": _history_symbol(symbol),
            "startTime": start_ms,
            "endTime": now_ms,
            "limit": min(1000, max(1, int(limit or 500))),
        }
        payload = await self._request("GET", "/api/v2/order/histories", query)
        rows = payload.get("data") if isinstance(payload, dict) else payload
        if rows is None and isinstance(payload, dict):
            rows = payload.get("orders")
        if not isinstance(rows, list):
            raise ValueError("indodax v2 returned a malformed order histories payload")
        return [self._order(row, symbol) for row in rows]
    
    async def fetch_closed_orders(
        self, symbol: Optional[str] = None, since: Optional[int] = None,
        limit: Optional[int] = None, params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch filled orders from authenticated terminal-order history."""
        rows = await self._fetch_order_histories(symbol, since, limit)
        return [row for row in rows if row.get("status") == "closed"]
    
    async def fetch_canceled_orders(
        self, symbol: Optional[str] = None, since: Optional[int] = None,
        limit: Optional[int] = None, params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch canceled/rejected/expired orders from authenticated history."""
        rows = await self._fetch_order_histories(symbol, since, limit)
        return [
            row for row in rows
            if row.get("status") in {"canceled", "rejected", "expired"}
        ]
    
    @staticmethod
    def _normalize_my_trade(raw: dict, default_symbol: str) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError("indodax v2 returned a malformed trade row")
        fee_cost = raw.get("commission")
        fee_asset = raw.get("commissionAsset")
        side = None
        if raw.get("isBuyer") is True:
            side = "buy"
        elif raw.get("isBuyer") is False:
            side = "sell"
        maker_taker = None
        if raw.get("isMaker") is True:
            maker_taker = "maker"
        elif raw.get("isMaker") is False:
            maker_taker = "taker"
        return {
            "id": str(raw.get("tradeId") or ""),
            "order": str(raw.get("orderId") or ""),
            "clientOrderId": str(raw.get("clientOrderId") or ""),
            "symbol": str(raw.get("symbol") or default_symbol),
            "side": side,
            "amount": _history_float(raw.get("qty")),
            "price": _history_float(raw.get("price")),
            "cost": _history_float(raw.get("quoteQty")),
            "fee": (
                {
                    "cost": _history_float(fee_cost),
                    "currency": str(fee_asset or "").upper(),
                }
                if fee_cost is not None else None
            ),
            "timestamp": int(raw.get("time") or 0) or None,
            "takerOrMaker": maker_taker,
            "info": raw,
        }
    
    async def fetch_my_trades(
        self,
        symbol: str,
        since: Optional[int] = None,
        limit: Optional[int] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch authenticated individual fills from the TAPI v2 myTrades API."""
        if not symbol:
            raise ValueError("fetch_my_trades requires a symbol")
        query = dict(params or {})
        query["symbol"] = _history_symbol(symbol)
        if since is not None:
            query.setdefault("startTime", int(since))
        query.setdefault("endTime", int(time.time() * 1000))
        query["limit"] = min(1000, max(10, int(limit or 500)))
        query.setdefault("sort", "asc")
        payload = await self._request("GET", "/api/v2/myTrades", query)
        rows = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            raise ValueError("indodax v2 returned a malformed myTrades payload")
        return [
            self._normalize_my_trade(raw, query["symbol"])
            for raw in rows
        ]
