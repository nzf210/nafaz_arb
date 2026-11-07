import asyncio
import math
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

    LOCAL_VENUES = {"indodax", "tokocrypto"}
    GLOBAL_VENUES = {"mexc", "okx", "binance", "bybit"}

    def __init__(self, local_exchange: Any, global_exchange: Any,
                 local_exchanges: Optional[Dict[str, Any]] = None,
                 global_exchanges: Optional[Dict[str, Any]] = None,
                 local_default_name: str = "indodax",
                 global_default_name: str = "okx"):
        self.local = local_exchange
        self.global_fut = global_exchange
        self.local_default_name = local_default_name.lower()
        self.global_default_name = global_default_name.lower()
        self.local_exchanges = dict(local_exchanges or {})
        self.global_exchanges = dict(global_exchanges or {})
        self.local_exchanges.setdefault(self.local_default_name, local_exchange)
        self.global_exchanges.setdefault(self.global_default_name, global_exchange)

    def _local_for(self, symbol: str, exchange_name: Optional[str] = None) -> Any:
        if any(str(symbol).startswith(prefix) for prefix in ("mexc:", "mexc_spot:", "okx:", "binance:", "bybit:")):
            raise ValueError("foreign Futures symbol cannot be routed to a local Spot client")
        name = (exchange_name or ("tokocrypto" if str(symbol).startswith("toko:") else self.local_default_name)).lower()
        if name not in self.LOCAL_VENUES:
            raise ValueError(f"{name} is not an allowed local Spot venue")
        client = self.local_exchanges.get(name)
        if client is None:
            raise RuntimeError(f"local Spot exchange {name} is not configured/enabled")
        return client

    def _global_for(self, symbol: str, exchange_name: Optional[str] = None) -> Any:
        raw = str(symbol)
        if raw.startswith("toko:"):
            raise ValueError("local Spot symbol cannot be routed to a foreign Futures client")
        inferred = next((name for name in ("mexc", "binance", "bybit", "okx") if raw.startswith(name + ":")), None)
        name = (exchange_name or inferred or self.global_default_name).lower()
        if name not in self.GLOBAL_VENUES:
            raise ValueError(f"{name} is not an allowed foreign Futures venue")
        client = self.global_exchanges.get(name)
        if client is None:
            raise RuntimeError(f"foreign Futures exchange {name} is not configured/enabled")
        return client

    async def _resolve_symbol(self, client: Any, symbol: str) -> str:
        raw = str(symbol)
        for prefix in ("toko:", "mexc:", "binance:", "bybit:", "okx:"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                break
        loader = getattr(client, "load_markets", None)
        if callable(loader):
            await loader()
            by_id = getattr(client, "markets_by_id", {}) or {}
            market = by_id.get(raw) or by_id.get(raw.upper()) or by_id.get(raw.lower())
            if market is None:
                compact = raw.replace("/", "").replace("-", "").replace("_", "").upper()
                for market_id, candidate in by_id.items():
                    if str(market_id).replace("/", "").replace("-", "").replace("_", "").upper() == compact:
                        market = candidate
                        break
            if isinstance(market, list):
                market = market[0] if market else None
            if isinstance(market, dict) and market.get("symbol"):
                return str(market["symbol"])
            markets = getattr(client, "markets", {}) or {}
            if raw in markets:
                return raw
            raise ValueError(f"exchange {getattr(client, 'id', 'unknown')} has no CCXT market for exchange symbol {raw}")
        return raw

    def _contract_size(self, client: Any, market_symbol: str) -> float:
        """Return CCXT contractSize; all engine quantities are expressed in base units."""
        market = None
        getter = getattr(client, "market", None)
        if callable(getter):
            try:
                market = getter(market_symbol)
            except Exception:
                market = None
        if not isinstance(market, dict):
            market = (getattr(client, "markets", {}) or {}).get(market_symbol)
        if not isinstance(market, dict):
            raise ValueError(f"missing futures market metadata for {market_symbol}")
        if market.get("contract") is False:
            raise ValueError(f"market {market_symbol} is not a contract market")
        size = float(market.get("contractSize") or 0.0)
        if not math.isfinite(size) or size <= 0:
            raise ValueError(f"invalid contractSize for {market_symbol}: {size}")
        return size

    async def _to_contract_amount(self, client: Any, market_symbol: str, base_amount: float) -> tuple[float, float]:
        if not math.isfinite(float(base_amount)) or base_amount <= 0:
            raise ValueError(f"invalid base quantity: {base_amount}")
        contract_size = self._contract_size(client, market_symbol)
        contracts = float(base_amount) / contract_size
        precision = getattr(client, "amount_to_precision", None)
        if callable(precision):
            contracts = float(precision(market_symbol, contracts))
        if not math.isfinite(contracts) or contracts <= 0:
            raise ValueError(f"base quantity {base_amount} rounds below minimum contract quantity")
        return contracts, contract_size

    @staticmethod
    def _normalize_contract_order(order: Dict[str, Any], contract_size: float) -> Dict[str, Any]:
        """Expose CCXT contract fills to the engine in base-asset units."""
        if not isinstance(order, dict):
            raise ValueError("futures order response is not an object")
        normalized = dict(order)
        for field in ("amount", "filled", "remaining"):
            value = normalized.get(field)
            if value is not None:
                normalized[field + "_contracts"] = value
                normalized[field] = float(value or 0.0) * contract_size
        normalized["contract_size"] = contract_size
        return normalized

    def _local_price_from_idr(self, client: Any, market_symbol: str, price_idr: float, usdt_idr_rate: float) -> float:
        """Convert the engine's IDR-normalized price into the venue's quote currency."""
        market = None
        getter = getattr(client, "market", None)
        if callable(getter):
            try:
                market = getter(market_symbol)
            except Exception:
                market = None
        if not isinstance(market, dict):
            market = (getattr(client, "markets", {}) or {}).get(market_symbol)
        if not isinstance(market, dict):
            raise ValueError(f"missing local market metadata for {market_symbol}")
        quote = str(market.get("quote") or "").upper()
        if quote in ("IDR", "BIDR"):
            return float(price_idr)
        if quote in ("USDT", "USDC"):
            if not math.isfinite(float(usdt_idr_rate)) or usdt_idr_rate <= 0:
                raise ValueError("live USDT/IDR rate is required to price a stablecoin-quoted local Spot order")
            # USDC is valued at the USDT peg for conversion; scanner exposes this assumption.
            return float(price_idr) / float(usdt_idr_rate)
        raise ValueError(f"unsupported local Spot quote currency {quote!r} for {market_symbol}")

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None, usdt_idr_rate: float = 0.0) -> Dict[str, Any]:
        """Places a Maker Limit Buy on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid buy params: amount={amount}, price={price}")
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        local_price = self._local_price_from_idr(client, market_symbol, price, usdt_idr_rate)
        return await client.create_limit_buy_order(market_symbol, amount, local_price, *_client_order_params(client_order_id))

    async def place_local_limit_sell(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None, usdt_idr_rate: float = 0.0) -> Dict[str, Any]:
        """Places a Maker Limit Sell on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid sell params: amount={amount}, price={price}")
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        local_price = self._local_price_from_idr(client, market_symbol, price, usdt_idr_rate)
        return await client.create_limit_sell_order(market_symbol, amount, local_price, *_client_order_params(client_order_id))

    async def cancel_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancels an existing order on local spot exchange."""
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        return await client.cancel_order(order_id, market_symbol)

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """Taker market sell on local spot. Used only for emergency de-risking."""
        if amount <= 0:
            raise ValueError(f"invalid market sell amount: {amount}")
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        return await client.create_market_sell_order(market_symbol, amount, *_client_order_params(client_order_id))

    async def fetch_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Fetches status of an order on local spot exchange."""
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        return await client.fetch_order(order_id, market_symbol)

    async def get_balances(
        self,
        local_quote_asset: str = "IDR",
        global_quote_asset: str = "USDT",
        usdt_idr_rate: float = 0.0,
        local_exchange: Optional[str] = None,
        global_exchange: Optional[str] = None,
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

        local_client = self._local_for("", local_exchange)
        global_client = self._global_for("", global_exchange)
        spot_raw = await local_client.fetch_balance()
        futures_raw = await global_client.fetch_balance()

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
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        try:
            positions = await client.fetch_positions([market_symbol])
        except (TypeError, AttributeError):
            # Some CCXT adapters do not accept a symbols argument. Fetch all,
            # then filter below; routing and symbol resolution still must succeed.
            positions = await client.fetch_positions()

        if positions is None or not isinstance(positions, (list, tuple)):
            raise ValueError("exchange returned an invalid futures positions payload")

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("requested futures symbol is missing; flat state cannot be verified")

        def normalize_symbol(value: str) -> str:
            # CCXT may represent the same linear contract as BTCUSDT or
            # BTC/USDT:USDT depending on venue/version.
            return value.split(":")[0].replace("/", "").replace("-", "").replace("_", "").upper()

        target_symbol = normalize_symbol(market_symbol)
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
        # Scanner and executor quantities are base-asset units; CCXT derivatives
        # generally expect a number of contracts. Convert and normalize fills.
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        contracts, contract_size = await self._to_contract_amount(client, market_symbol, amount)
        order = await client.create_market_sell_order(market_symbol, contracts, *_client_order_params(client_order_id))
        return self._normalize_contract_order(order, contract_size)

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
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        contracts, contract_size = await self._to_contract_amount(client, market_symbol, amount)
        order = await client.create_market_buy_order(market_symbol, contracts, params)
        return self._normalize_contract_order(order, contract_size)

    async def fetch_funding_history(
        self, symbol: str, since: Optional[int] = None, limit: int = 100
    ) -> list:
        """Fetches actual settled funding payments for a perpetual contract."""
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        fetcher = getattr(client, "fetch_funding_history", None)
        if fetcher is None:
            raise NotImplementedError(
                f"{getattr(self.global_fut, 'id', 'global futures')} adapter does not support fetch_funding_history"
            )
        return await fetcher(market_symbol, since, limit)

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Fetches current funding rate and the venue's next settlement timestamp."""
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        fetcher = getattr(client, "fetch_funding_rate", None)
        if fetcher is None:
            raise NotImplementedError(
                f"{getattr(self.global_fut, 'id', 'global futures')} adapter does not support fetch_funding_rate"
            )
        return await fetcher(market_symbol)

    async def fetch_global_ticker(self, symbol: str) -> Dict[str, Any]:
        """Fetches ticker from global futures exchange."""
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        return await client.fetch_ticker(market_symbol)

    async def close(self):
        """Closes exchange connections."""
        closed = set()
        for client in list(self.local_exchanges.values()) + list(self.global_exchanges.values()):
            if client is not None and id(client) not in closed and hasattr(client, "close"):
                closed.add(id(client))
                await client.close()


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
    return create_ccxt_client(
        local_name=config.local_exchange_name,
        global_name=config.global_exchange_name,
        local_creds=config.local_credentials,
        global_creds=config.global_credentials,
        exchanges=getattr(config, "exchanges", {}),
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

def create_ccxt_client(local_name: str, global_name: str, local_creds, global_creds, exchanges=None) -> ExchangeClient:
    """Build role-separated CCXT clients; local venues can only be Spot and foreign venues only Perpetual."""
    local_name = str(local_name or "indodax").lower()
    global_name = str(global_name or "okx").lower()
    local_roles = {"indodax", "tokocrypto"}
    global_roles = {"mexc", "okx", "binance", "bybit"}
    if local_name not in local_roles:
        raise ValueError(f"{local_name} is not an allowed local Spot exchange")
    if global_name not in global_roles:
        raise ValueError(f"{global_name} is not an allowed foreign Futures exchange")

    credentials = exchanges if isinstance(exchanges, dict) else {}
    local_clients = {}
    global_clients = {}

    # Instantiate only additional venues with complete credentials. The selected
    # defaults are always constructed so missing credentials fail at the venue,
    # while symbols from an unconfigured secondary venue fail closed in routing.
    for name, creds in credentials.items():
        name = str(name).lower()
        if name not in local_roles and name not in global_roles:
            continue
        if not getattr(creds, "api_key", "") or not getattr(creds, "api_secret", ""):
            continue
        if name in local_roles:
            local_clients[name] = init_ccxt_exchange(name, creds, default_type="spot")
        else:
            market_type = "future" if name == "binance" else "swap"
            global_clients[name] = init_ccxt_exchange(name, creds, default_type=market_type)

    if local_name not in local_clients:
        local_clients[local_name] = init_ccxt_exchange(local_name, local_creds, default_type="spot")
    if global_name not in global_clients:
        market_type = "future" if global_name == "binance" else "swap"
        global_clients[global_name] = init_ccxt_exchange(global_name, global_creds, default_type=market_type)

    return ExchangeClient(
        local_clients[local_name],
        global_clients[global_name],
        local_exchanges=local_clients,
        global_exchanges=global_clients,
        local_default_name=local_name,
        global_default_name=global_name,
    )
