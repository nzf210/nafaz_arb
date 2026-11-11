"""
INDODAX Trade API 2.0 adapter (https://api.indodax.com/api/v2).

ccxt's indodax client still signs the legacy TAPI v1 endpoint
(``https://indodax.com/tapi``, HMAC-SHA512). Keys created on
``https://indodax.com/trade_api`` are TAPIv2-only and the legacy endpoint answers
``{"error_code":"invalid_version_key"}`` for them, so every private call (balance,
order create/cancel/query) fails. This module implements the subset of the ccxt
exchange surface the executor actually uses, on top of the v2 REST API
(``X-APIKEY`` + ``Sign`` = HMAC-SHA256 over the request query string).

Return shapes deliberately mirror ccxt so ``ExchangeClient`` and the engine need
no changes.
"""

import hashlib
import hmac
import json
import logging
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger("executor.indodax_v2")

BASE_URL = "https://api.indodax.com"

# v2 status names -> ccxt status names. Partially filled orders stay "open":
# the engine derives the filled delta from `filled`, not from the status.
_STATUS_MAP = {
    "NEW": "open",
    "PARTIALLY_FILLED": "open",
    "FILLED": "closed",
    "CANCELLED": "canceled",
    "REJECTED": "rejected",
    "EXPIRED": "expired",
}

class IndodaxAPIError(Exception):
    """Raised for any v2 error payload; carries the venue error code."""

    def __init__(self, code: Any, message: str):
        super().__init__(f"indodax v2 error {code}: {message}")
        self.code = code
        self.message = message

class IndodaxRateLimitError(IndodaxAPIError):
    """Raised when the venue throttles the caller (HTTP 429/503).

    Subclasses IndodaxAPIError so existing handlers keep working, while the
    engine can catch this specific type to back off and pause new entries.
    """

    def __init__(self, code: Any, message: str, retry_after: Optional[float] = None):
        super().__init__(code, message)
        self.retry_after = retry_after

def _parse_retry_after(raw: Any) -> Optional[float]:
    """Parses a Retry-After header (delta-seconds form) into float seconds."""
    if raw is None:
        return None
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None

def normalize_symbol(symbol: str) -> str:
    """'btc_idr' / 'btc/idr' -> 'BTCIDR' (v2 expects the compact upper form)."""
    cleaned = "".join(ch for ch in str(symbol or "") if ch.isalnum())
    return cleaned.upper()

def _fmt(value: Any) -> str:
    """Formats a numeric value without exponent and without float noise.

    The venue rejects quantities/prices carrying more precision than the pair
    allows. A value like ``0.00012300000000000001`` (float artefact from the
    engine's lot-size flooring) is quantized before it reaches the wire, and
    rounding is downward so a buy never asks for more than the balance covers.
    """
    dec = Decimal(str(value)).quantize(Decimal("1e-10"), rounding=ROUND_DOWN)
    text = format(dec, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"

def _fmt_to_increment(value: Any, increment: Any) -> str:
    """Rounds a quantity down to the exact market increment without float noise."""
    quantity = Decimal(str(value))
    step = Decimal(str(increment))
    if not quantity.is_finite() or not step.is_finite() or step <= 0:
        raise ValueError(f"invalid quantity or increment: quantity={value}, increment={increment}")
    rounded = (quantity / step).to_integral_value(rounding=ROUND_DOWN) * step
    text = format(rounded, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"

class IndodaxTAPIv2:
    """Minimal ccxt-compatible async client for INDODAX Trade API 2.0."""

    id = "indodax"
    # v2 rejects requests whose timestamp is older than recvWindow. The default
    # 5000ms leaves no room for a small VPS clock drift, so ask for more.
    recv_window = 10000

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = BASE_URL,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        if not api_key or not api_secret:
            raise ValueError("indodax v2 requires both api_key and api_secret")
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self._session = session
        self._owns_session = session is None

    # ------------------------------------------------------------------ http

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20)
            )
            self._owns_session = True
        return self._session

    def sign(self, payload: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = True,
    ) -> Any:
        params = dict(params or {})
        query = ""
        if signed:
            params.setdefault("timestamp", int(time.time() * 1000))
            params.setdefault("recvWindow", self.recv_window)
            query = urlencode(
                {k: v for k, v in params.items() if v is not None}
            )
        elif params:
            query = urlencode(params)

        headers = {"Accept": "application/json", "Accept-Encoding": "identity"}
        url = f"{self.base_url}{path}"
        body = None
        if signed:
            headers["X-APIKEY"] = self.api_key
            headers["Sign"] = self.sign(query)

        if method in ("POST", "PUT"):
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            body = query
        elif query:
            # GET and DELETE carry their parameters in the query string; the
            # signature covers exactly that string.
            url = f"{url}?{query}"

        session = await self._get_session()
        async with session.request(method, url, data=body, headers=headers) as resp:
            text = await resp.text()
            # A throttle or WAF page answers with HTML, not the JSON envelope, so
            # it must be classified as a rate limit before the decode attempt --
            # otherwise it surfaces as a generic non-JSON error and callers retry
            # straight into the same wall.
            if resp.status == 429 or resp.status == 503:
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                raise IndodaxRateLimitError(
                    f"http_{resp.status}", f"rate limited: {text[:200]}", retry_after
                )
            try:
                payload = json.loads(text)
            except ValueError:
                raise IndodaxAPIError(
                    f"http_{resp.status}", f"non-JSON response: {text[:200]}"
                )
            if isinstance(payload, dict) and payload.get("code") not in (None, 0):
                raise IndodaxAPIError(payload.get("code"), str(payload.get("msg") or payload))
            if resp.status >= 400:
                raise IndodaxAPIError(f"http_{resp.status}", text[:200])
            return payload

    # --------------------------------------------------------------- mapping

    @staticmethod
    def _order(order: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        amount = _to_float(order.get("origQty"))
        filled = _to_float(order.get("executedQty"))
        status_raw = str(order.get("status") or "").upper()
        return {
            "id": str(order.get("orderId") or ""),
            "clientOrderId": order.get("clientOrderId"),
            "symbol": symbol,
            "type": str(order.get("type") or "").lower(),
            "side": str(order.get("side") or "").lower(),
            "price": _to_float(order.get("price")),
            "amount": amount,
            "filled": filled,
            "remaining": max(0.0, amount - filled),
            "status": _STATUS_MAP.get(status_raw, status_raw.lower()),
            "timestamp": order.get("time"),
            "info": order,
        }

    # ---------------------------------------------------------- ccxt surface

    async def create_limit_buy_order(
        self, symbol: str, amount: float, price: float, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        return await self._create_order(symbol, "BUY", "LIMIT", amount, price, params)

    async def create_limit_sell_order(
        self, symbol: str, amount: float, price: float, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        return await self._create_order(symbol, "SELL", "LIMIT", amount, price, params)

    async def create_market_sell_order(
        self, symbol: str, amount: float, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        return await self._create_order(symbol, "SELL", "MARKET", amount, None, params)

    async def _create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        amount: float,
        price: Optional[float],
        params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if amount <= 0:
            raise ValueError(f"invalid order amount: {amount}")
        if order_type == "LIMIT" and (price is None or price <= 0):
            raise ValueError(f"invalid limit price: {price}")

        payload: Dict[str, Any] = {
            "symbol": normalize_symbol(symbol),
            "side": side,
            "type": order_type,
        }
        quantity_increment = (params or {}).get("quantityIncrement")
        payload["quantity"] = (
            _fmt_to_increment(amount, quantity_increment)
            if quantity_increment is not None
            else _fmt(amount)
        )
        if Decimal(payload["quantity"]) <= 0:
            raise ValueError(
                f"order amount {amount} rounds to zero for quantity increment {quantity_increment}"
            )
        if price is not None:
            payload["price"] = _fmt(price)
        client_order_id = (params or {}).get("clientOrderId")
        if client_order_id:
            payload["newClientOrderId"] = str(client_order_id)[:36]

        created = await self._request("POST", "/api/v2/order", payload)
        # The create payload omits `status` entirely; a successful POST means the
        # venue accepted the order and it is now live. Defaulting to "open" keeps
        # the engine's expiry/adverse-selection polling working on the first tick.
        created["status"] = created.get("status") or "NEW"
        return self._order(created, symbol)

    async def cancel_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        if not order_id:
            raise ValueError("cancel_order requires an order id")
        if not symbol:
            raise ValueError("indodax v2 cancel_order requires the order symbol")
        cancelled = await self._request(
            "DELETE", "/api/v2/order", {"symbol": normalize_symbol(symbol), "orderId": str(order_id)}
        )
        # The DELETE payload carries no `status` field: the venue answering with
        # a cancelled order *is* the confirmation. Without this the engine would
        # see an empty status and refuse to treat the order as terminal.
        cancelled = dict(cancelled)
        cancelled.setdefault("clientOrderId", cancelled.get("origClientOrderId"))
        cancelled["status"] = cancelled.get("status") or "CANCELLED"
        return self._order(cancelled, symbol)

    async def fetch_order(self, order_id: str, symbol: Optional[str] = None) -> Dict[str, Any]:
        if not order_id:
            raise ValueError("fetch_order requires an order id")
        if not symbol:
            raise ValueError("indodax v2 fetch_order requires the order symbol")
        order = await self._request(
            "GET", "/api/v2/order", {"symbol": normalize_symbol(symbol), "orderId": str(order_id)}
        )
        return self._order(order, symbol)

    async def fetch_order_by_client_id(
        self, client_order_id: str, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch one order by the venue's origClientOrderId lookup parameter."""
        if not client_order_id:
            raise ValueError("fetch_order_by_client_id requires a client order id")
        if not symbol:
            raise ValueError("fetch_order_by_client_id requires a symbol")
        order = await self._request(
            "GET", "/api/v2/order",
            {"symbol": normalize_symbol(symbol), "origClientOrderId": str(client_order_id)},
        )
        if not isinstance(order, dict):
            raise ValueError("indodax v2 returned a malformed client-id order payload")
        mapped = self._order(order, symbol)
        if str(mapped.get("clientOrderId") or "") != str(client_order_id):
            raise ValueError("indodax v2 client-id lookup returned a different client id")
        return mapped

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
            "symbol": normalize_symbol(symbol),
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
            "amount": _to_float(raw.get("qty")),
            "price": _to_float(raw.get("price")),
            "cost": _to_float(raw.get("quoteQty")),
            "fee": (
                {
                    "cost": _to_float(fee_cost),
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
        query["symbol"] = normalize_symbol(symbol)
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

    async def fetch_balance(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        account = await self._request("GET", "/api/v2/account")
        free: Dict[str, float] = {}
        used: Dict[str, float] = {}
        total: Dict[str, float] = {}
        per_asset: Dict[str, Dict[str, float]] = {}
        for row in account.get("balances") or []:
            asset = str(row.get("asset") or "")
            if not asset:
                continue
            if not isinstance(row.get("free"), (str, int, float)) or not isinstance(
                row.get("locked"), (str, int, float)
            ):
                raise ValueError(f"indodax v2 returned a malformed balance row for {asset}")
            available = Decimal(str(row["free"]))
            locked = Decimal(str(row["locked"]))
            if available < 0 or locked < 0:
                raise ValueError(f"indodax v2 returned a negative balance for {asset}")
            free[asset] = float(available)
            used[asset] = float(locked)
            total[asset] = float(available + locked)
            per_asset[asset] = {
                "free": free[asset],
                "used": used[asset],
                "total": total[asset],
            }
        result: Dict[str, Any] = {
            "free": free,
            "used": used,
            "total": total,
            "info": account,
        }
        result.update(per_asset)
        return result

    async def fetch_open_orders(
        self, symbol: Optional[str] = None, since: Optional[int] = None, limit: Optional[int] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = normalize_symbol(symbol)
        orders = await self._request("GET", "/api/v2/openOrders", query)
        if not isinstance(orders, list):
            raise ValueError("indodax v2 returned a malformed open orders payload")
        # Without a symbol filter the venue returns every resting order on the
        # account, and each row carries its own symbol. Mapping them all to the
        # requested symbol (empty) would make the payload unmatchable at recovery.
        requested = normalize_symbol(symbol) if symbol else ""
        mapped = []
        for order in orders:
            row_symbol = requested or str(order.get("symbol") or "")
            mapped.append(self._order(order, row_symbol))
        return mapped

    async def close(self) -> None:
        if self._session is not None and self._owns_session and not self._session.closed:
            await self._session.close()

def _to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)
