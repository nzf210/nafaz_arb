"""exchange trade methods for the exchange facade."""

import asyncio
import math
import time
from typing import Optional, Any

import ccxt.async_support as ccxt

from executor.indodax_v2 import IndodaxTAPIv2
from executor.quantity import AmountSpec, order_amount_to_base
from executor.exchange_common import venue_client_order_id
from executor.venues import get_venue



class ExchangeTradeMixin:
    """Focused asynchronous exchange adapter operations."""

    async def _fetch_indodax_trade_history(
        self, fetcher, resolved: str, since: Optional[int],
        until: Optional[int], client_order_id: Optional[str],
        order_id: Optional[str],
    ) -> list:
        return await get_venue("indodax").fetch_trade_history(
            fetcher, resolved, limit=1000, since=since, until=until,
            client_order_id=client_order_id, order_id=order_id,
        )

    @staticmethod
    async def _request_ccxt_trade_history(
        exchange, fetcher, resolved: str, since: Optional[int],
        order_id: Optional[str],
    ) -> list:
        order_trade_fetcher = getattr(exchange, "fetch_order_trades", None)
        if not callable(order_trade_fetcher) or not order_id:
            return await fetcher(resolved, int(since or 0) or None, 1000)
        try:
            return await order_trade_fetcher(
                str(order_id), resolved, int(since or 0) or None, 1000
            )
        except (NotImplementedError, ccxt.NotSupported):
            return await fetcher(resolved, int(since or 0) or None, 1000)

    async def _fetch_ccxt_trade_history(
        self, exchange, fetcher, resolved: str, since: Optional[int],
        until: Optional[int], order_id: Optional[str],
    ) -> list:
        raw_trades = await self._request_ccxt_trade_history(
            exchange, fetcher, resolved, since, order_id
        )
        if not isinstance(raw_trades, list):
            raise ValueError("exchange returned malformed trade history")
        if len(raw_trades) >= 1000:
            raise RuntimeError(
                "exchange trade-history response reached its 1000-fill limit; "
                "cannot prove complete order fill history"
            )
        if until is not None:
            raw_trades = [
                item for item in raw_trades
                if isinstance(item, dict)
                and int(item.get("timestamp") or 0) <= int(until)
            ]
        return raw_trades

    @staticmethod
    def _trade_matches_requested_order(
        actual_order: str, actual_client: str,
        expected_order: str, expected_client: str,
    ) -> bool:
        if expected_client and actual_client:
            return actual_client == expected_client
        if expected_order and actual_order:
            return actual_order == expected_order
        return False

    @staticmethod
    def _trade_fee_payload(trade: dict, info: dict):
        fee = trade.get("fee")
        if (
            not isinstance(fee, dict)
            or fee.get("cost") is None
            or not fee.get("currency")
        ):
            fee_rows = trade.get("fees") if isinstance(trade.get("fees"), list) else []
            fee = fee_rows[0] if fee_rows and isinstance(fee_rows[0], dict) else {
                "cost": trade.get(
                    "commission", info.get("commission", info.get("execFee"))
                ),
                "currency": trade.get(
                    "commissionAsset",
                    info.get("commissionAsset", info.get("feeCurrency")),
                ),
            }
        return fee if isinstance(fee, dict) else None

    @staticmethod
    def _trade_order_identity(trade: dict, info: dict) -> tuple:
        actual_order = str(
            trade.get("order") or trade.get("orderId") or info.get("orderId")
            or info.get("ordId") or info.get("order_id") or ""
        )
        actual_client = str(
            trade.get("clientOrderId") or trade.get("client_order_id")
            or info.get("clientOrderId") or info.get("clOrdId") or ""
        )
        fill_id = str(
            trade.get("id") or trade.get("tradeId") or info.get("tradeId")
            or info.get("execId") or info.get("matchId") or info.get("fillId") or ""
        )
        return actual_order, actual_client, fill_id

    @staticmethod
    def _trade_quantity_and_price(trade: dict, spec: Optional[AmountSpec]):
        try:
            qty = float(
                trade.get("amount")
                if trade.get("amount") is not None else trade.get("qty")
            )
            price = float(trade.get("price"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "exchange trade has invalid quantity or price"
            ) from exc
        if (
            not math.isfinite(qty) or qty <= 0
            or not math.isfinite(price) or price <= 0
        ):
            raise ValueError(
                "exchange trade has non-positive or non-finite quantity/price"
            )
        if spec is not None and spec.unit == "CONTRACT":
            qty = order_amount_to_base(qty, spec)
        if spec is not None and spec.unit == "CONTRACT":
            cost = qty * price
        else:
            quote_cost = trade.get("cost")
            if quote_cost is None:
                quote_cost = trade.get("quoteQty")
            cost = float(quote_cost if quote_cost is not None else qty * price)
        return qty, price, cost

    def _normalize_order_trade(
        self, trade: dict, spec: Optional[AmountSpec],
        expected_order: str, expected_client: str,
    ):
        if not isinstance(trade, dict):
            raise ValueError("exchange returned a malformed trade row")
        info = trade.get("info") if isinstance(trade.get("info"), dict) else {}
        actual_order, actual_client, fill_id = self._trade_order_identity(
            trade, info
        )
        if not self._trade_matches_requested_order(
            actual_order, actual_client, expected_order, expected_client
        ):
            return None

        qty, price, cost = self._trade_quantity_and_price(trade, spec)
        fill = dict(trade)
        fill["id"] = fill_id
        fill["order"] = actual_order
        fill["clientOrderId"] = actual_client
        fill["amount"] = qty
        fill["price"] = price
        fill["cost"] = cost
        fill["fee"] = self._trade_fee_payload(trade, info)
        fill["raw_payload"] = {
            "info": info,
            "fee": fill["fee"],
            "fees": trade.get("fees") if isinstance(trade.get("fees"), list) else [],
        }
        fill["timestamp"] = int(
            trade.get("timestamp") or trade.get("time") or info.get("time") or 0
        ) or None
        if not fill["id"]:
            raise ValueError("exchange trade is missing its immutable trade id")
        return fill

    @staticmethod
    def _deduplicate_order_trades(normalized: list) -> list:
        unique = {}
        for fill in normalized:
            fill_id = str(fill.get("id") or "")
            previous = unique.get(fill_id)
            if previous is not None:
                if (
                    float(previous.get("amount", 0.0)) != float(fill.get("amount", 0.0))
                    or float(previous.get("price", 0.0)) != float(fill.get("price", 0.0))
                ):
                    raise ValueError(
                        f"exchange returned conflicting payloads for trade id {fill_id}"
                    )
                continue
            unique[fill_id] = fill
        return list(unique.values())

    async def fetch_order_trades(
        self,
        market_type: str,
        symbol: str,
        *,
        since: Optional[int] = None,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
        until: Optional[int] = None,
        amount_spec: Any = None,
    ) -> list:
        """Read immutable executions for exactly one bot order."""
        market = str(market_type or "").lower()
        if market not in ("spot", "perpetual"):
            raise ValueError(f"unsupported fill market type: {market_type}")
        exchange = self.global_fut if market == "perpetual" else self.local
        resolved = await self._resolve_symbol(
            exchange, symbol, "futures" if market == "perpetual" else "spot"
        )
        fetcher = getattr(exchange, "fetch_my_trades", None)
        if not callable(fetcher):
            raise NotImplementedError(
                f"{getattr(exchange, 'id', 'exchange')} does not support authenticated trade history"
            )

        if isinstance(exchange, IndodaxTAPIv2):
            raw_trades = await self._fetch_indodax_trade_history(
                fetcher, resolved, since, until, client_order_id, order_id
            )
        else:
            raw_trades = await self._fetch_ccxt_trade_history(
                exchange, fetcher, resolved, since, until, order_id
            )

        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        expected_order = str(order_id or "")
        expected_client = str(client_order_id or "")
        normalized = []
        for trade in raw_trades:
            fill = self._normalize_order_trade(
                trade, spec, expected_order, expected_client
            )
            if fill is not None:
                normalized.append(fill)
        return self._deduplicate_order_trades(normalized)
