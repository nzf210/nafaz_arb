import asyncio
import math
import time
from contextvars import ContextVar
from typing import Optional, Dict, Any
import ccxt.async_support as ccxt

from executor.indodax_v2 import IndodaxTAPIv2


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

    def __init__(
        self,
        local_exchange: Any = None,
        global_exchange: Any = None,
        spot_clients: Optional[Dict[str, Any]] = None,
        perpetual_clients: Optional[Dict[str, Any]] = None,
        default_spot: str = "indodax",
        default_perpetual: str = "okx",
        strict_routing: bool = False,
    ):
        # Context-local routing prevents concurrent signals from cross-wiring venues.
        self.spot_clients = {str(k).lower(): v for k, v in (spot_clients or {}).items()}
        self.perpetual_clients = {str(k).lower(): v for k, v in (perpetual_clients or {}).items()}
        if local_exchange is not None:
            self.spot_clients.setdefault(default_spot.lower(), local_exchange)
        if global_exchange is not None:
            self.perpetual_clients.setdefault(default_perpetual.lower(), global_exchange)
        self._default_spot = default_spot.lower()
        self._default_perpetual = default_perpetual.lower()
        # Live factory clients require every trading coroutine to select its
        # signal/position route explicitly. Legacy single-client constructions
        # keep their historical defaults for backwards compatibility.
        self._strict_routing = bool(strict_routing)
        self._route = ContextVar(f"exchange_route_{id(self)}", default=None)

    def set_route(self, spot_name: str, perpetual_name: str):
        spot = str(spot_name or "").strip().lower()
        perp = str(perpetual_name or "").strip().lower()
        if spot not in self.spot_clients:
            raise RuntimeError(f"spot exchange route is not configured: {spot or '<empty>'}")
        if perp not in self.perpetual_clients:
            raise RuntimeError(f"perpetual exchange route is not configured: {perp or '<empty>'}")
        return self._route.set((spot, perp))

    def reset_route(self, token) -> None:
        self._route.reset(token)

    def _route_names(self):
        route = self._route.get()
        if route is None:
            if self._strict_routing:
                raise RuntimeError(
                    "exchange route must be selected before a live trading operation"
                )
            return self._default_spot, self._default_perpetual
        return route

    @property
    def local(self):
        name, _ = self._route_names()
        if name not in self.spot_clients:
            raise RuntimeError(f"spot exchange route is not configured: {name}")
        return self.spot_clients[name]

    @property
    def global_fut(self):
        _, name = self._route_names()
        if name not in self.perpetual_clients:
            raise RuntimeError(f"perpetual exchange route is not configured: {name}")
        return self.perpetual_clients[name]

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


    async def fetch_balance_report(self) -> list:
        """Fetch complete free/used/total balances from both live venues."""
        reports = []
        exchanges = [
            (name, "spot", exchange) for name, exchange in self.spot_clients.items()
        ] + [
            (name, "futures", exchange) for name, exchange in self.perpetual_clients.items()
        ]
        for name, market, exchange in exchanges:
            try:
                raw = await exchange.fetch_balance()
                free_map, used_map, total_map = raw.get("free") or {}, raw.get("used") or {}, raw.get("total") or {}
                assets = []
                for asset in sorted(set(free_map) | set(used_map) | set(total_map)):
                    free_raw = free_map.get(asset)
                    used_raw = used_map.get(asset)
                    total_raw = total_map.get(asset)
                    free = float(free_raw or 0.0)
                    total = float(total_raw) if total_raw is not None else 0.0
                    used = (
                        float(used_raw)
                        if used_raw is not None
                        else max(0.0, total - free)
                    )
                    if total_raw is None:
                        total = free + used
                    if not all(math.isfinite(v) and v >= 0 for v in (free, used, total)):
                        raise ValueError(f"invalid balance value for {asset}")
                    if free or used or total:
                        assets.append({"asset": str(asset), "free": free, "used": used, "total": total})
                reports.append({"name": name, "market": market, "status": "ok", "assets": assets})
            except Exception as exc:
                reports.append({"name": name, "market": market, "status": "error", "error": str(exc), "assets": []})
        return reports

    async def get_balances(
        self,
        local_quote_asset: str = "IDR",
        global_quote_asset: str = "USDT",
        usdt_idr_rate: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Fetches normalized available balances for entry sizing and risk checks.

        Local IDR is converted using the live USDT/IDR rate supplied by the
        scanner snapshot. USDT and USDC are treated as stable quote units for
        sizing; unsupported quote assets fail closed rather than guessing.
        """
        local_quote = str(local_quote_asset or "").upper()
        global_quote = str(global_quote_asset or "").upper()
        supported = ("IDR", "USDT", "USDC")
        if local_quote not in supported or global_quote not in ("USDT", "USDC"):
            raise ValueError(
                f"unsupported balance quote conversion: local={local_quote}, global={global_quote}"
            )

        spot_raw = await self.local.fetch_balance()
        futures_raw = await self.global_fut.fetch_balance()

        def amount(balance: Dict[str, Any], asset: str, bucket: str) -> float:
            section = balance.get(bucket)
            if isinstance(section, dict) and asset in section:
                value = float(section.get(asset) or 0.0)
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"exchange returned invalid {asset} {bucket} balance")
                return value
            per_asset = balance.get(asset)
            if isinstance(per_asset, dict):
                value = float(per_asset.get(bucket) or 0.0)
                if not math.isfinite(value) or value < 0:
                    raise ValueError(f"exchange returned invalid {asset} {bucket} balance")
                return value
            value = float(section.get(asset) or 0.0) if isinstance(section, dict) else 0.0
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"exchange returned invalid {asset} {bucket} balance")
            return value

        spot_free = amount(spot_raw, local_quote, "free")
        spot_total = amount(spot_raw, local_quote, "total")
        if local_quote == "IDR":
            if not math.isfinite(float(usdt_idr_rate)) or usdt_idr_rate <= 0:
                raise ValueError("live USDT/IDR rate is required to normalize IDR spot balance")
            spot_free /= usdt_idr_rate
            spot_total /= usdt_idr_rate

        futures_free = amount(futures_raw, global_quote, "free")
        futures_total = amount(futures_raw, global_quote, "total")

        return {
            "spot_available": spot_free,
            "futures_available": futures_free,
            "spot_total": spot_total,
            "futures_total": futures_total,
            "spot_quote_asset": local_quote,
            "futures_quote_asset": global_quote,
            "stablecoin_peg_assumed": local_quote == "USDC" or global_quote == "USDC",
        }

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

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("requested futures symbol is missing; flat state cannot be verified")

        def normalize_symbol(value: str) -> str:
            # CCXT may represent the same linear contract as BTCUSDT or
            # BTC/USDT:USDT depending on venue/version.
            return value.split(":")[0].replace("/", "").replace("-", "").replace("_", "").upper()

        target_symbol = normalize_symbol(symbol)
        matched = []
        for position in positions:
            if not isinstance(position, dict):
                raise ValueError("exchange returned a malformed futures position")
            position_symbol = position.get("symbol")
            if not isinstance(position_symbol, str) or not position_symbol:
                raise ValueError(
                    "exchange futures position is missing its symbol; flat state cannot be verified"
                )
            if normalize_symbol(position_symbol) == target_symbol:
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
        seen = set()
        for exchange in list(self.spot_clients.values()) + list(self.perpetual_clients.values()):
            if id(exchange) in seen:
                continue
            seen.add(id(exchange))
            if exchange and hasattr(exchange, "close"):
                await exchange.close()


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
    spot_names = list(getattr(config, "spot_exchanges", []) or [config.local_exchange_name])
    perp_names = list(getattr(config, "perpetual_exchanges", []) or [config.global_exchange_name])
    credentials = getattr(config, "exchanges", {}) or {}
    spot_clients = {}
    perpetual_clients = {}
    for name in sorted(set(str(x).lower() for x in spot_names)):
        creds = credentials.get(name)
        if creds is None and name == str(config.local_exchange_name).lower():
            creds = config.local_credentials
        if creds is None or not creds.api_key or not creds.api_secret:
            raise ValueError(f"LIVE spot route {name} has no complete API credentials")
        spot_clients[name] = init_local_exchange(name, creds, default_type="spot")
    for name in sorted(set(str(x).lower() for x in perp_names)):
        creds = credentials.get(name)
        if creds is None and name == str(config.global_exchange_name).lower():
            creds = config.global_credentials
        if creds is None or not creds.api_key or not creds.api_secret:
            raise ValueError(f"LIVE perpetual route {name} has no complete API credentials")
        if name == "okx" and not creds.passphrase:
            raise ValueError("LIVE perpetual route okx requires API passphrase")
        # CCXT exposes perpetual markets under different default types:
        # Binance USD-M uses "future"; OKX, MEXC and Bybit use "swap".
        default_type = "future" if name == "binance" else "swap"
        perpetual_clients[name] = init_ccxt_exchange(name, creds, default_type=default_type)
    return ExchangeClient(
        spot_clients=spot_clients,
        perpetual_clients=perpetual_clients,
        default_spot=config.local_exchange_name,
        default_perpetual=config.global_exchange_name,
        strict_routing=True,
    )


# Venues whose TAPI v1 support was retired. ccxt still signs their legacy
# endpoint, which rejects TAPIv2 keys with `invalid_version_key`, so these
# need the v2 adapter instead of a ccxt instance.
V2_ADAPTERS = {"indodax": IndodaxTAPIv2}

def init_local_exchange(exchange_name: str, creds: Any, default_type: str = "spot") -> Any:
    """Builds the local spot client: v2 adapter when required, else ccxt."""
    adapter = V2_ADAPTERS.get(exchange_name.lower())
    if adapter is not None:
        return adapter(
            api_key=getattr(creds, "api_key", ""),
            api_secret=getattr(creds, "api_secret", ""),
        )
    return init_ccxt_exchange(exchange_name, creds, default_type=default_type)

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
    local_ex = init_local_exchange(local_name, local_creds, default_type="spot")
    global_ex = init_ccxt_exchange(global_name, global_creds, default_type="future")
    return ExchangeClient(
        local_ex,
        global_ex,
        default_spot=local_name,
        default_perpetual=global_name,
    )
