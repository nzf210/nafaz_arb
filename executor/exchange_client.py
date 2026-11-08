import asyncio
import math
import time
from contextvars import ContextVar
from typing import Optional, Dict, Any


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

    async def _perpetual_contract_amount(self, symbol: str, base_amount: float) -> tuple[float, float]:
        """Convert base units to venue contract units and verify exact hedge coverage."""
        base = self._validate_perpetual_base_amount(base_amount)
        market, exchange = await self._load_perpetual_market(symbol)
        if market is None:
            return base, 1.0
        contract_size = self._resolve_contract_size(symbol, market)
        contracts = self._convert_contract_amount(symbol, base, contract_size, exchange)
        self._validate_exact_contract_coverage(symbol, base, contracts, contract_size)
        return contracts, contract_size

    @staticmethod
    def _validate_perpetual_base_amount(base_amount: float) -> float:
        amount = float(base_amount)
        if not math.isfinite(amount) or amount <= 0:
            raise ValueError(f"invalid perpetual base amount: {base_amount}")
        return amount

    async def _load_perpetual_market(self, symbol: str):
        exchange = self.global_fut
        market_fn = getattr(exchange, "market", None)
        if not callable(market_fn):
            return None, exchange
        if not getattr(exchange, "markets", None):
            loader = getattr(exchange, "load_markets", None)
            if callable(loader):
                await loader()
        market = market_fn(symbol)
        if not isinstance(market, dict):
            raise ValueError(f"perpetual market metadata unavailable for {symbol}")
        if not market.get("contract"):
            raise ValueError(
                f"{symbol} is not a CCXT contract market; refusing unverified hedge sizing"
            )
        return market, exchange

    @staticmethod
    def _resolve_contract_size(symbol: str, market: dict) -> float:
        try:
            contract_size = float(market.get("contractSize") or 0.0)
        except (TypeError, ValueError):
            contract_size = 0.0
        if not math.isfinite(contract_size) or contract_size <= 0:
            raise ValueError(f"invalid contractSize for {symbol}: {market.get('contractSize')!r}")
        return contract_size

    @staticmethod
    def _convert_contract_amount(symbol: str, base_amount: float, contract_size: float, exchange) -> float:
        contracts = base_amount / contract_size
        precision_fn = getattr(exchange, "amount_to_precision", None)
        if callable(precision_fn):
            contracts = float(precision_fn(symbol, contracts))
        if not math.isfinite(contracts) or contracts <= 0:
            raise ValueError(f"perpetual amount rounds to zero for {symbol}")
        return contracts

    @staticmethod
    def _validate_exact_contract_coverage(symbol: str, base_amount: float, contracts: float, contract_size: float) -> None:
        normalized_base = contracts * contract_size
        tolerance = max(1e-12, abs(base_amount) * 1e-8)
        if abs(normalized_base - base_amount) > tolerance:
            raise ValueError(
                f"spot quantity {base_amount} cannot be exactly hedged on {symbol}: "
                f"{contracts} contracts x {contract_size} = {normalized_base} base units"
            )

    @staticmethod
    def _normalize_perpetual_order(order: Dict[str, Any], contract_size: float) -> Dict[str, Any]:
        """Expose CCXT contract fills to the engine in base-asset units."""
        if contract_size == 1.0:
            return order
        normalized = dict(order)
        normalized["_nafaz_contract_size"] = contract_size
        for field in ("amount", "filled", "remaining"):
            raw = normalized.get(field)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                raise ValueError(f"invalid CCXT {field} in perpetual order: {raw!r}")
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"invalid CCXT {field} in perpetual order: {raw!r}")
            normalized[f"_nafaz_contract_{field}"] = value
            normalized[field] = value * contract_size
        return normalized

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
        """Fetch complete free/used/total balances from every configured venue."""
        reports = []
        for name, market, exchange in self._configured_balance_exchanges():
            reports.append(await self._fetch_venue_balance_report(name, market, exchange))
        return reports

    def _configured_balance_exchanges(self) -> list:
        spot = [(name, "spot", exchange) for name, exchange in self.spot_clients.items()]
        futures = [(name, "futures", exchange) for name, exchange in self.perpetual_clients.items()]
        return spot + futures

    async def _fetch_venue_balance_report(self, name: str, market: str, exchange) -> dict:
        try:
            raw = await exchange.fetch_balance()
            assets = self._normalize_balance_assets(raw)
            return {"name": name, "market": market, "status": "ok", "assets": assets}
        except Exception as exc:
            return {"name": name, "market": market, "status": "error", "error": str(exc), "assets": []}

    @staticmethod
    def _normalize_balance_assets(raw: dict) -> list:
        free_map = raw.get("free") or {}
        used_map = raw.get("used") or {}
        total_map = raw.get("total") or {}
        assets = []
        for asset in sorted(set(free_map) | set(used_map) | set(total_map)):
            free = float(free_map.get(asset) or 0.0)
            total_raw = total_map.get(asset)
            used_raw = used_map.get(asset)
            total = float(total_raw) if total_raw is not None else 0.0
            used = float(used_raw) if used_raw is not None else max(0.0, total - free)
            if total_raw is None:
                total = free + used
            if not all(math.isfinite(value) and value >= 0 for value in (free, used, total)):
                raise ValueError(f"invalid balance value for {asset}")
            if free or used or total:
                assets.append({"asset": str(asset), "free": free, "used": used, "total": total})
        return assets

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

    async def fetch_account_positions_report(self) -> list:
        """Fetch every configured perpetual account's non-zero position snapshot."""
        reports = []
        for name, exchange in self.perpetual_clients.items():
            try:
                fetcher = getattr(exchange, "fetch_positions", None)
                if not callable(fetcher):
                    raise NotImplementedError("fetch_positions is unavailable")
                positions = await fetcher()
                if not isinstance(positions, (list, tuple)):
                    raise ValueError("invalid account positions payload")
                reports.append({"name": name, "status": "ok", "positions": list(positions)})
            except Exception as exc:
                reports.append({"name": name, "status": "error", "error": str(exc), "positions": []})
        return reports

    async def fetch_account_open_orders_report(self) -> list:
        """Fetch every configured spot/perpetual venue's open orders."""
        reports = []
        exchanges = [(name, "spot", ex) for name, ex in self.spot_clients.items()]
        exchanges += [(name, "perpetual", ex) for name, ex in self.perpetual_clients.items()]
        for name, market, exchange in exchanges:
            try:
                fetcher = getattr(exchange, "fetch_open_orders", None)
                if not callable(fetcher):
                    raise NotImplementedError("fetch_open_orders is unavailable")
                orders = await fetcher()
                if not isinstance(orders, (list, tuple)):
                    raise ValueError("invalid open orders payload")
                if any(not isinstance(order, dict) or not order.get("id") for order in orders):
                    raise ValueError("open orders contain malformed order identity")
                reports.append({"name": name, "market": market, "status": "ok", "orders": list(orders)})
            except Exception as exc:
                reports.append({"name": name, "market": market, "status": "error", "error": str(exc), "orders": []})
        return reports

    async def fetch_positions(self, symbol: str) -> list:
        """Fetch positions and fail closed unless the response proves symbol state."""
        try:
            positions = await self.global_fut.fetch_positions([symbol])
        except (TypeError, AttributeError):
            positions = await self.global_fut.fetch_positions()
        self._validate_positions_payload(positions, symbol)
        return self._filter_positions_for_symbol(positions, symbol)

    @staticmethod
    def _validate_positions_payload(positions, symbol: str) -> None:
        if positions is None or not isinstance(positions, (list, tuple)):
            raise ValueError("exchange returned an invalid futures positions payload")
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("requested futures symbol is missing; flat state cannot be verified")

    def _filter_positions_for_symbol(self, positions, symbol: str) -> list:
        target = self._normalize_position_symbol(symbol)
        matched = []
        for position in positions:
            normalized = self._position_symbol(position)
            if normalized == target:
                matched.append(position)
        return matched

    @staticmethod
    def _normalize_position_symbol(value: str) -> str:
        return value.split(":")[0].replace("/", "").replace("-", "").replace("_", "").upper()

    def _position_symbol(self, position: dict) -> str:
        if not isinstance(position, dict):
            raise ValueError("exchange returned a malformed futures position")
        symbol = position.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ValueError(
                "exchange futures position is missing its symbol; flat state cannot be verified"
            )
        return self._normalize_position_symbol(symbol)

    async def open_global_short_market(
        self,
        symbol: str,
        amount: float,
        client_order_id: Optional[str] = None,
        *,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Executes an instant Short 1x Market Hedge on Global Futures.
        STRICT: Only executes Market SELL. Never buy/long to open.

        price is accepted for compatibility with the engine's slippage
        preflight/audit context. The order remains MARKET; this value is never
        passed as a limit price to the exchange.
        """
        if amount <= 0:
            raise ValueError(f"invalid short amount: {amount}")
        # Convert scanner/spot base quantity into venue contract units.
        contracts, contract_size = await self._perpetual_contract_amount(symbol, amount)
        # Market Sell on Futures opens a Short position.
        order = await self.global_fut.create_market_sell_order(
            symbol, contracts, *_client_order_params(client_order_id)
        )
        if not isinstance(order, dict):
            raise ValueError("perpetual short returned an invalid order payload")
        return self._normalize_perpetual_order(order, contract_size)

    async def close_global_short_market(self, symbol: str, amount: float, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Closes Global Short position by buying back sold coins with reduceOnly=True.
        STRICT: Must have reduceOnly=True so it can never inadvertently open a Long.
        """
        if amount <= 0:
            raise ValueError(f"invalid close amount: {amount}")
        contracts, contract_size = await self._perpetual_contract_amount(symbol, amount)
        params = {"reduceOnly": True}
        if client_order_id:
            params["clientOrderId"] = client_order_id
        order = await self.global_fut.create_market_buy_order(symbol, contracts, params)
        if not isinstance(order, dict):
            raise ValueError("perpetual close returned an invalid order payload")
        return self._normalize_perpetual_order(order, contract_size)

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
                f"{gedef create_exchange_client(config: Any) -> Any:
    """Compatibility wrapper retaining injectable exchange factories for tests."""
    from executor.exchange_client_factory import create_exchange_client as factory
    return factory(
        config,
        local_factory=init_local_exchange,
        perpetual_factory=init_ccxt_exchange,
    )
