"""Deterministic paper-trading implementation of the exchange client contract."""

import asyncio
import time
from typing import Optional, Dict, Any

from executor.quantity import AmountSpec, base_to_order_amount
from executor.exchange_common import _require_amount_spec

class PaperTradingExchangeClient:
    """
    Simulated Paper Trading Client for local and global exchanges.
    Executes mock orders against real market data without spending real funds.
    Strictly follows:
    1. Local Spot: Maker Buy for entry, Maker Sell for exit.
    2. Global Futures: Market Sell to Open Short 1x, Market Buy reduceOnly=True to Close Short.
    """

    def __init__(
        self,
        local_name: str = "indodax",
        global_name: str = "okx",
        initial_balance_usdt: float = 100.0,
    ):
        self.local_name = local_name
        self.global_name = global_name
        self.initial_balance = float(initial_balance_usdt)
        self.balance_spot = float(initial_balance_usdt)
        self.balance_futures = float(initial_balance_usdt)
        self.locked_spot = 0.0
        self.locked_futures = 0.0
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.order_counter = 1000
        self.trades = []
        self.market_prices: Dict[str, Dict[str, float]] = {}


    async def fetch_balance_report(self) -> list:
        """Returns simulated balances with an explicit simulation label."""
        return [
            {"name": self.local_name, "market": "spot", "status": "ok", "assets": [
                {"asset": "USDT", "free": max(0.0, self.balance_spot-self.locked_spot), "used": self.locked_spot, "total": self.balance_spot}]},
            {"name": self.global_name, "market": "futures", "status": "ok", "assets": [
                {"asset": "USDT", "free": max(0.0, self.balance_futures-self.locked_futures), "used": self.locked_futures, "total": self.balance_futures}]},
        ]

    def get_balances(
        self,
        local_quote_asset: str = "IDR",
        global_quote_asset: str = "USDT",
        usdt_idr_rate: float = 0.0,
    ) -> Dict[str, float]:
        """Returns simulated balance breakdown; quote arguments mirror the live client."""
        return {
            "spot_total": round(self.balance_spot, 4),
            "spot_available": round(max(0.0, self.balance_spot - self.locked_spot), 4),
            "spot_locked": round(self.locked_spot, 4),
            "futures_total": round(self.balance_futures, 4),
            "futures_available": round(max(0.0, self.balance_futures - self.locked_futures), 4),
            "futures_locked": round(self.locked_futures, 4),
            "total_equity": round(self.balance_spot + self.balance_futures, 4),
        }

    def lock_funds(self, spot_usdt: float, futures_usdt: float) -> bool:
        """Locks funds for pending/active position if available balance permits."""
        avail_spot = self.balance_spot - self.locked_spot
        avail_futures = self.balance_futures - self.locked_futures
        if avail_spot < spot_usdt or avail_futures < futures_usdt:
            return False
        self.locked_spot += spot_usdt
        self.locked_futures += futures_usdt
        return True

    @staticmethod
    def _validate_base_amount(amount: float, amount_spec: Any) -> None:
        spec = _require_amount_spec(amount_spec)
        base_to_order_amount(amount, spec)

    def unlock_funds(self, spot_usdt: float, futures_usdt: float) -> None:
        """Unlocks reserved funds when order is cancelled or position exited."""
        self.locked_spot = max(0.0, self.locked_spot - spot_usdt)
        self.locked_futures = max(0.0, self.locked_futures - futures_usdt)

    def apply_realized_trade(
        self,
        spot_cost_usdt: float,
        spot_pnl_usdt: float,
        spot_fees_usdt: float,
        futures_margin_usdt: float,
        futures_pnl_usdt: float,
        funding_usdt: float,
        futures_fees_usdt: float,
    ) -> None:
        """
        Updates actual balances when a position is closed:
        - Releases locked capital for both legs
        - Spot balance credited with net spot PnL
        - Futures balance credited with net futures PnL + funding
        """
        self.unlock_funds(spot_cost_usdt, futures_margin_usdt)
        self.balance_spot += (spot_pnl_usdt - spot_fees_usdt)
        self.balance_futures += (futures_pnl_usdt + funding_usdt - futures_fees_usdt)

    def update_market_price(
        self, symbol: str, best_bid: float, best_ask: float, last_price: float = 0.0
    ) -> None:
        """Updates live market price for a symbol to evaluate realistic limit order fills."""
        clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
        self.market_prices[clean_sym] = {
            "bid": float(best_bid),
            "ask": float(best_ask),
            "last": float(last_price),
            "updated_at": time.time(),
        }

    async def place_local_limit_buy(
        self, symbol: str, amount: float, price: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        self._validate_base_amount(amount, amount_spec)
        if client_order_id:
            existing = self._find_by_client_id(client_order_id)
            if existing is not None:
                # Idempotent replay: return the original order instead of opening
                # a second one, exactly like a venue rejecting a duplicate id.
                return existing
        self.order_counter += 1
        oid = f"mock-buy-{self.order_counter}"
        order = {
            "id": oid,
            "client_order_id": client_order_id,
            "symbol": symbol,
            "side": "buy",
            "type": "limit",
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "status": "open",
            "_created_time": time.time(),
            "timestamp": int(time.time() * 1000),
        }
        self.orders[oid] = order
        return order

    async def place_local_limit_sell(
        self, symbol: str, amount: float, price: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        self._validate_base_amount(amount, amount_spec)
        if client_order_id:
            existing = self._find_by_client_id(client_order_id)
            if existing is not None:
                return existing
        self.order_counter += 1
        oid = f"mock-sell-{self.order_counter}"
        order = {
            "id": oid,
            "client_order_id": client_order_id,
            "symbol": symbol,
            "side": "sell",
            "type": "limit",
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "status": "open",
            "_created_time": time.time(),
            "timestamp": int(time.time() * 1000),
        }
        self.orders[oid] = order
        return order

    async def place_local_market_sell(
        self, symbol: str, amount: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """Taker market sell at the live best bid. Used for emergency hedge rollback."""
        self._validate_base_amount(amount, amount_spec)
        if amount <= 0:
            raise ValueError(f"invalid market sell amount: {amount}")
        self.order_counter += 1
        oid = f"mock-market-sell-{self.order_counter}"
        clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
        mkt = self.market_prices.get(clean_sym) or {}
        fill_price = float(mkt.get("bid", 0.0))
        order = {
            "id": oid,
            "symbol": symbol,
            "side": "sell",
            "type": "market",
            "amount": amount,
            "price": fill_price,
            "client_order_id": client_order_id,
            "average": fill_price,
            "filled": amount,
            "status": "closed",
            "_filled_time": time.time(),
            "timestamp": int(time.time() * 1000),
        }
        self.orders[oid] = order
        return order

    async def cancel_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        if order_id in self.orders:
            self.orders[order_id]["status"] = "canceled"
        return {"id": order_id, "status": "canceled"}

    async def fetch_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        order = self.orders.get(order_id)
        if not order:
            return {"id": order_id, "status": "closed", "filled": 0.0}

        if order.get("status") != "open":
            return order

        clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
        mkt = self.market_prices.get(clean_sym)
        order_price = float(order.get("price", 0.0))
        side = order.get("side")

        # REALISTIC MARKET FILL EVALUATION:
        # Limit orders do NOT auto-fill on timer. They only fill if real market touches/crosses the price level.
        if mkt:
            mkt_bid = mkt.get("bid", 0.0)
            mkt_ask = mkt.get("ask", 0.0)

            # Limit Buy (Maker): Fills if sellers hit or drop to our bid (market ask <= our limit buy price)
            if side == "buy":
                if mkt_ask > 0 and mkt_ask <= order_price:
                    order["filled"] = order["amount"]
                    order["status"] = "closed"
                    order["_filled_time"] = time.time()

            # Limit Sell (Maker): Fills if buyers bid up to or cross our ask (market bid >= our limit sell price)
            elif side == "sell":
                if mkt_bid > 0 and mkt_bid >= order_price:
                    order["filled"] = order["amount"]
                    order["status"] = "closed"
                    order["_filled_time"] = time.time()

        return order

    def _find_by_client_id(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        for order in self.orders.values():
            if order.get("client_order_id") == client_order_id:
                return order
        return None

    async def fetch_local_open_orders(
        self, symbol: Optional[str] = None, amount_spec: Any = None
    ) -> list:
        """Resting spot orders in the simulated book, in venue payload shape.

        The paper venue applies the same open-order completion rules as
        ``fetch_local_order`` so a simulated restart sees exactly the orders the
        venue would still be holding.
        """
        open_orders = []
        for order_id, order in list(self.orders.items()):
            if str(order.get("status", "")).lower() != "open":
                continue
            if symbol and str(order.get("symbol", "")) != symbol:
                continue
            current = await self.fetch_local_order(order_id, str(order.get("symbol", symbol or "")))
            if str(current.get("status", "")).lower() == "open":
                open_orders.append(dict(current))
        return open_orders

    async def fetch_global_open_orders(
        self, symbol: Optional[str] = None, amount_spec: Any = None
    ) -> list:
        """Resting futures orders in the simulated book.

        Hedge and unwound legs are market orders and therefore terminal, so a
        correct simulation returns nothing resting here.
        """
        open_orders = []
        for order in self.orders.values():
            if str(order.get("status", "")).lower() != "open":
                continue
            if str(order.get("market", "spot")) != "futures":
                continue
            if symbol and str(order.get("symbol", "")) != symbol:
                continue
            open_orders.append(dict(order))
        return open_orders

    def _global_fill_price(self, symbol: str, price: float = 0.0) -> float:
        """Best available futures price for a market fill in paper mode."""
        if price and price > 0:
            return float(price)
        for key in (symbol, symbol.lower().replace("/", "_").replace("-", "_")):
            mkt = self.market_prices.get(key)
            if mkt:
                mid = float(mkt.get("ask") or mkt.get("bid") or 0.0)
                if mid > 0:
                    return mid
        return 0.0

    async def open_global_short_market(
        self, symbol: str, amount: float, price: float = 0.0,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        self._validate_base_amount(amount, amount_spec)
        if amount <= 0:
            raise ValueError(f"invalid short amount: {amount}")
        self.order_counter += 1
        oid = f"mock-short-{self.order_counter}"
        fill_price = self._global_fill_price(symbol, price)
        trade = {
            "id": oid,
            "symbol": symbol,
            "side": "sell",
            "type": "market",
            "amount": amount,
            "filled": amount,
            "status": "closed",
            "price": fill_price,
            "average": fill_price,
            "expected_price": fill_price,
            "client_order_id": client_order_id,
        }
        self.trades.append(trade)
        return trade

    async def close_global_short_market(
        self, symbol: str, amount: float, client_order_id: Optional[str] = None,
        amount_spec: Any = None,
    ) -> Dict[str, Any]:
        self._validate_base_amount(amount, amount_spec)
        self.order_counter += 1
        oid = f"mock-close-{self.order_counter}"
        # A buy-to-close pays the ask; reporting a fill price keeps paper-mode PnL
        # accounting honest instead of silently falling back to the live quote.
        fill_price = self._global_fill_price(symbol)
        trade = {
            "id": oid,
            "symbol": symbol,
            "side": "buy",
            "type": "market",
            "amount": amount,
            "filled": amount,
            "status": "closed",
            "price": fill_price,
            "average": fill_price,
            "cost": fill_price * amount,
            "params": {"reduceOnly": True},
            "client_order_id": client_order_id,
        }
        self.trades.append(trade)
        return trade

    async def fetch_positions(self, symbol: str) -> list:
        """Paper-mode position: net of tracked open-short and close trades."""
        shorts = sum(
            float(t["amount"])
            for t in self.trades
            if t.get("symbol") == symbol and t.get("side") == "sell"
        )
        closes = sum(
            float(t["amount"])
            for t in self.trades
            if t.get("symbol") == symbol and t.get("side") == "buy"
        )
        qty = shorts - closes
        if qty <= 0:
            return []
        return [{"symbol": symbol, "contracts": qty, "side": "short"}]

    async def fetch_funding_history(
        self, symbol: str, since: Optional[int] = None, limit: int = 100
    ) -> list:
        """Paper mode has no exchange settlement ledger; never invent funding credits."""
        return []

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Paper mode does not fabricate a future settlement schedule."""
        return {}

    async def fetch_global_ticker(self, symbol: str) -> Dict[str, Any]:
        """Mirrors the live client: returns the last known ask for this symbol."""
        clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
        price = self.market_prices.get(clean_sym) or {}
        return {
            "symbol": symbol,
            "bid": float(price.get("bid", 0.0)),
            "ask": float(price.get("ask", 0.0)),
        }

    async def close(self):
        # Preserve the async exchange-client contract during paper-mode shutdown.
        await asyncio.sleep(0)

