"""Tokocrypto REST compatibility fixes for the pinned CCXT adapter.

CCXT 4.5.92 declares Tokocrypto's documented GET /open/v1/orders/detail
endpoint, but its generic fetch_order implementation still calls
GET /open/v1/orders with only an orderId. The latter is the paginated
symbol-scoped order-list endpoint. This adapter uses the documented detail
endpoint and keeps CCXT's normalized order shape for the executor.
"""

from typing import Any, Dict, Optional

import ccxt.async_support as ccxt


class TokocryptoAdapter(ccxt.tokocrypto):
    """CCXT Tokocrypto client with a corrected single-order lookup."""

    async def fetch_order(
        self,
        id: str,
        symbol: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if id in (None, ""):
            raise ValueError("tokocrypto fetch_order requires an order id")

        request: Dict[str, Any] = {"orderId": str(id)}
        response = await self.privateGetOpenV1OrdersDetail(
            self.extend(request, params or {})
        )
        raw_order = self.safe_dict(response, "data", None)
        if not isinstance(raw_order, dict) or not raw_order:
            raise ValueError(
                "tokocrypto order-detail endpoint returned a malformed or empty data object"
            )

        market = self.market(symbol) if symbol else None
        parsed = self.parse_order(raw_order, market)
        if not isinstance(parsed, dict) or not parsed.get("id"):
            raise ValueError(
                "tokocrypto order-detail response did not contain a valid order id"
            )
        if str(parsed["id"]) != str(id):
            raise ValueError(
                "tokocrypto order-detail response returned a different order id"
            )
        return parsed
