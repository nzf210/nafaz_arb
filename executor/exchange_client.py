import asyncio
import time
from typing import Optional, Dict, Any
import ccxt.async_support as ccxt


def _client_order_params(client_order_id: Optional[str]) -> tuple:
    """
    Builds the positional params argument for a ccxt create_*_order call.

    Passing a client order id makes retries idempotent at the venue: a duplicate
    submit is rejected instead of double-filling. When no id is supplied the
    argument is omitted entirely so ccxt does not see an empty params dict.
    """
    return ({"clientOrderId": client_order_id},) if client_order_id else ()


class ExchangeClient:
    """
    Unified client enforcing Core Rules:
    1. Local CEX: ONLY Spot one-way (Buy Spot to enter, Sell Spot to exit).
    2. Global CEX Futures: ONLY Short 1x (Market Sell to Open Short, Market Buy reduceOnly to Close Short).
       NEVER Open Long.
    3. No transfer/deposit/withdraw features.
    """

    def __init__(self, local_exchange: Any, global_exchange: Any):
        self.local = local_exchange
        self.global_fut = global_exchange

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Places a Maker Limit Buy on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid buy params: amount={amount}, price={price}")
        return await self.local.create_limit_buy_order(symbol, amount, price, *_client_order_params(client_order_id))

    async def place_local_limit_sell(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Places a Maker Limit Sell on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid sell params: amount={amount}, price={price}")
        return await self.local.create_limit_sell_order(symbol, amount, price, *_client_order_params(client_order_id))

    async def cancel_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancels an existing order on local spot exchange."""
        return await self.local.cancel_order(order_id, symbol)

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Taker market sell on local spot. Used only for emergency de-risking."""
        if amount <= 0:
            raise ValueError(f"invalid market sell amount: {amount}")
        return await self.local.create_market_sell_order(symbol, amount, *_client_order_params(client_order_id))

    async def fetch_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Fetches status of an order on local spot exchange."""
        return await self.local.fetch_order(order_id, symbol)

    async def fetch_positions(self, symbol: str) -> list:
        """Fetches open futures positions for a symbol (global exchange).

        A malformed position response must never be converted into an empty list:
        callers use an empty list as proof that the futures leg is flat.
        """
        try:
            positions = await self.global_fut.fetch_positions([symbol])
        except (TypeError, AttributeError):
            positions = await self.global_fut.fetch_positions()

        if positions is None or not isinstance(positions, (list, tuple)):
            raise ValueError("exchange returned an invalid futures positions payload")

        matched = []
        for position in positions:
            if not isinstance(position, dict):
                raise ValueError("exchange returned a malformed futures position")
            position_symbol = position.get("symbol")
            if not isinstance(position_symbol, str) or not position_symbol:
                raise ValueError(
                    "exchange futures position is missing its symbol; flat state cannot be verified"
                )
            if position_symbol == symbol:
                matched.append(position)
        return matched

    async def open_global_short_market(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Executes an instant Short 1x Market Hedge on Global Futures.
        STRICT: Only executes Market SELL. Never buy/long to open.
        """
        if amount <= 0:
            raise ValueError(f"invalid short amount: {amount}")
        # Market Sell on Futures opens a Short position
        return await self.global_fut.create_market_sell_order(symbol, amount, *_client_order_params(client_order_id))

    async def close_global_short_market(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Closes Global Short position by buying back sold coins with reduceOnly=True.
        STRICT: Must have reduceOnly=True so it can never inadvertently open a Long.
        """
        if amount <= 0:
            raise ValueError(f"invalid close amount: {amount}")
        params = {"reduceOnly": True}
        if client_order_id:
            params["clientOrderId"] = client_order_id
        return await self.global_fut.create_market_buy_order(symbol, amount, params)

    async def fetch_funding_history(
        self, symbol: str, since: Optional[int] = None, limit: int = 100
    ) -> list:
        """Fetches actual settled funding payments for a perpetual contract."""
        fetcher = getattr(self.global_fut, "fetch_funding_history", None)
        if fetcher is None:
            raise NotImplementedError(
                f"{getattr(self.global_fut, 'id', 'global futures')} adapter does not support fetch_funding_history"
            )
        return await fetcher(symbol, since, limit)

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Fetches current funding rate and the venue's next settlement timestamp."""
        fetcher = getattr(self.global_fut, "fetch_funding_rate", None)
        if fetcher is None:
            raise NotImplementedError(
                f"{getattr(self.global_fut, 'id', 'global futures')} adapter does not support fetch_funding_rate"
            )
        return await fetcher(symbol)

    async def fetch_global_ticker(self, symbol: str) -> Dict[str, Any]:
        """Fetches ticker from global futures exchange."""
        return await self.global_fut.fetch_ticker(symbol)

    async def close(self):
        """Closes exchange connections."""
        if self.local and hasattr(self.local, "close"):
            await self.local.close()
        if self.global_fut and hasattr(self.global_fut, "close"):
            await self.global_fut.close()


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

    def get_balances(self) -> Dict[str, float]:
        """Returns balance breakdown per exchange, including available and locked amounts."""
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

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
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

    async def place_local_limit_sell(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
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

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Taker market sell at the live best bid. Used for emergency hedge rollback."""
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

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id: Optional[str] = None) -> Dict[str, Any]:
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

    async def close_global_short_market(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
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
        pass


def create_exchange_client(config: Any) -> Any:
    """Factory creating appropriate client (CCXT live or Paper Trading mock)."""
    if getattr(config, "mock_mode", False):
        return PaperTradingExchangeClient(
            local_name=config.local_exchange_name,
            global_name=config.global_exchange_name,
            initial_balance_usdt=getattr(config, "initial_exchange_balance_usdt", 100.0),
        )
    return create_ccxt_client(
        local_name=config.local_exchange_name,
        global_name=config.global_exchange_name,
        local_creds=config.local_credentials,
        global_creds=config.global_credentials,
    )


def init_ccxt_exchange(exchange_name: str, creds: Any, default_type: str = "spot") -> Any:
    """Helper to initialize a CCXT async exchange with proper credentials and settings."""
    ex_class = getattr(ccxt, exchange_name.lower())
    cfg: Dict[str, Any] = {
        "apiKey": getattr(creds, "api_key", ""),
        "secret": getattr(creds, "api_secret", ""),
        "enableRateLimit": True,
        "options": {"defaultType": default_type},
    }
    if hasattr(creds, "options") and isinstance(creds.options, dict):
        cfg["options"].update(creds.options)

    if getattr(creds, "passphrase", None):
        cfg["password"] = creds.passphrase

    instance = ex_class(cfg)
    if getattr(creds, "testnet", False) and hasattr(instance, "set_sandbox_mode"):
        instance.set_sandbox_mode(True)
    return instance

def create_ccxt_client(local_name: str, global_name: str, local_creds, global_creds) -> ExchangeClient:
    """Factory creating CCXT async instances."""
    local_ex = init_ccxt_exchange(local_name, local_creds, default_type="spot")
    global_ex = init_ccxt_exchange(global_name, global_creds, default_type="future")
    return ExchangeClient(local_ex, global_ex)
