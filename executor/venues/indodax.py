"""Indodax exchange venue module."""
import asyncio
import time
from typing import Any, Dict, List, Optional
from executor.venues.base import BaseExchangeVenue


class IndodaxVenue(BaseExchangeVenue):
    """Indodax spot exchange venue implementation."""

    @property
    def venue_name(self) -> str:
        return "indodax"

    @property
    def default_market_type(self) -> str:
        return "spot"

    async def fetch_trade_history(
        self,
        fetcher_or_exchange: Any,
        symbol: str,
        limit: int = 50,
        since: Optional[int] = None,
        until: Optional[int] = None,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Paginated Indodax trade history fetch across 7-day windows."""
        if callable(fetcher_or_exchange):
            fetcher = fetcher_or_exchange
        else:
            fetcher = getattr(fetcher_or_exchange, "fetch_my_trades", None)
        if not callable(fetcher):
            return []

        end_ms = min(
            int(time.time() * 1000),
            int(until) if until is not None else int(time.time() * 1000),
        )
        cursor = int(
            since or max(0, end_ms - 7 * 24 * 60 * 60 * 1000)
        )
        raw_trades: List[Dict[str, Any]] = []
        first_window = True
        while cursor <= end_ms:
            window_end = min(
                end_ms, cursor + 7 * 24 * 60 * 60 * 1000 - 1
            )
            params: Dict[str, Any] = {"startTime": cursor, "endTime": window_end, "sort": "asc"}
            if client_order_id:
                params["clientOrderId"] = str(client_order_id)
            if order_id:
                params["orderId"] = str(order_id)
            if not first_window:
                await asyncio.sleep(0.25)
            first_window = False
            chunk = await fetcher(symbol, cursor, 1000, params)
            if not isinstance(chunk, list):
                raise ValueError("exchange returned malformed trade history")
            if len(chunk) >= 1000:
                raise RuntimeError(
                    "INDODAX trade-history window reached 1000-fill limit; "
                    "cannot prove complete order fill history"
                )
            raw_trades.extend(chunk)
            cursor = window_end + 1

        return raw_trades
