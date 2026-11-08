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
        if inferred is None and ":" in raw:
            prefix = raw.split(":", 1)[0].lower()
            # Preserve legacy CCXT unified symbols such as BTC/USDT:USDT, but
            # never let a local/unknown exchange-prefixed symbol reach Futures.
            if prefix in self.LOCAL_VENUES or ("/" not in prefix and prefix not in self.GLOBAL_VENUES):
                raise ValueError(f"unknown or local symbol cannot be routed to foreign Futures: {raw!r}")
        name = (exchange_name or inferred or self.global_default_name).lower()
        if name not in self.GLOBAL_VENUES:
            raise ValueError(f"{name} is not an allowed foreign Futures venue")
        client = self.global_exchanges.get(name)
        if client is None:
            raise RuntimeError(f"foreign Futures exchange {name} is not configured/enabled")
        return client


    async def _refresh_markets(self, client: Any) -> None:
        """Refreshes exchange metadata immediately before an entry is allowed."""
        loader = getattr(client, "load_markets", None)
        if not callable(loader):
            raise ValueError("CCXT market metadata loader is unavailable")
        try:
            await loader(reload=True)
        except TypeError:
            await loader()
        if not isinstance(getattr(client, "markets", None), dict) or not client.markets:
            raise ValueError("exchange returned no market metadata")

    def _market(self, client: Any, market_symbol: str) -> Dict[str, Any]:
        getter = getattr(client, "market", None)
        market = getter(market_symbol) if callable(getter) else None
        if not isinstance(market, dict):
            market = (getattr(client, "markets", {}) or {}).get(market_symbol)
        if not isinstance(market, dict):
            raise ValueError(f"missing market metadata for {market_symbol}")
        return market

    @staticmethod
    def _require_market_active(market: Dict[str, Any], market_symbol: str) -> None:
        active = market.get("active")
        if active is None:
            info = market.get("info") or {}
            status = str(
                info.get("state") or info.get("status") or info.get("contractStatus") or ""
            ).strip().lower()
            active = status in ("live", "trading", "enabled", "online", "1", "true")
        if active is not True:
            raise ValueError(f"market {market_symbol} is not confirmed active")

    @staticmethod
    def _required_market_minimum(market: Dict[str, Any], group: str, market_symbol: str) -> float:
        limits = market.get("limits")
        section = limits.get(group) if isinstance(limits, dict) else None
        raw = section.get("min") if isinstance(section, dict) else None
        try:
            value = float(raw)
        except (TypeError, ValueError):
            raise ValueError(f"market {market_symbol} has no verified {group}.min constraint")
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"market {market_symbol} has invalid {group}.min constraint")
        return value

    async def _to_spot_amount(self, client: Any, market_symbol: str, base_amount: float) -> float:
        if not math.isfinite(float(base_amount)) or base_amount <= 0:
            raise ValueError(f"invalid Spot base quantity: {base_amount}")
        precision = getattr(client, "amount_to_precision", None)
        if not callable(precision):
            raise ValueError(f"Spot amount precision is unavailable for {market_symbol}")
        normalized = float(precision(market_symbol, base_amount))
        tolerance = max(1e-12, abs(float(base_amount)) * 1e-8)
        if not math.isfinite(normalized) or normalized <= 0 or abs(normalized - base_amount) > tolerance:
            raise ValueError(
                f"Spot quantity {base_amount} is not exactly representable for {market_symbol}; refusing order"
            )
        return normalized

    async def validate_entry_order(
        self,
        local_symbol: str,
        global_symbol: str,
        amount: float,
        price_idr: float,
        usdt_idr_rate: float,
        global_ask_quote: float,
        local_exchange: Optional[str] = None,
        global_exchange: Optional[str] = None,
        max_ticker_age_ms: int = 2000,
        max_price_deviation_pct: float = 0.003,
    ) -> tuple[float, float]:
        """Fail-closed preflight for active markets, min-notional, precision and contract size."""
        values = (amount, price_idr, usdt_idr_rate)
        if any(not math.isfinite(float(value)) or float(value) <= 0 for value in values):
            raise ValueError("entry preflight received a non-positive or non-finite price, quantity, or FX value")

        local_client = self._local_for(local_symbol, local_exchange)
        global_client = self._global_for(global_symbol, global_exchange)
        await self._refresh_markets(local_client)
        await self._refresh_markets(global_client)
        spot_symbol = await self._resolve_symbol(local_client, local_symbol)
        perp_symbol = await self._resolve_symbol(global_client, global_symbol)
        spot_market = self._market(local_client, spot_symbol)
        perp_market = self._market(global_client, perp_symbol)

        self._require_market_active(spot_market, spot_symbol)
        self._require_market_active(perp_market, perp_symbol)
        if not (spot_market.get("spot") is True or str(spot_market.get("type", "")).lower() == "spot"):
            raise ValueError(f"local market {spot_symbol} is not confirmed Spot")
        if perp_market.get("contract") is not True or perp_market.get("linear") is not True:
            raise ValueError(f"perpetual market {perp_symbol} is not confirmed linear contract")
        if not (perp_market.get("swap") is True or perp_market.get("future") is True
                or str(perp_market.get("type", "")).lower() in ("swap", "future")):
            raise ValueError(f"market {perp_symbol} is not a perpetual/futures contract")

        local_quote = str(spot_market.get("quote") or "").upper()
        global_quote = str(perp_market.get("settle") or perp_market.get("quote") or "").upper()
        if local_quote not in ("IDR", "BIDR", "USDT", "USDC"):
            raise ValueError(f"unsupported local Spot quote asset {local_quote!r}")
        if global_quote not in ("USDT", "USDC"):
            raise ValueError(f"unsupported Futures quote/settle asset {global_quote!r}")

        spot_amount_min = self._required_market_minimum(spot_market, "amount", spot_symbol)
        spot_cost_min = self._required_market_minimum(spot_market, "cost", spot_symbol)
        perp_contract_min = self._required_market_minimum(perp_market, "amount", perp_symbol)
        perp_cost_min = self._required_market_minimum(perp_market, "cost", perp_symbol)

        spot_price_quote = self._local_price_from_idr(
            local_client, spot_symbol, price_idr, usdt_idr_rate
        )
        spot_price_precision = getattr(local_client, "price_to_precision", None)
        if not callable(spot_price_precision):
            raise ValueError(f"Spot price tick/precision is unavailable for {spot_symbol}")
        precise_spot_price = float(spot_price_precision(spot_symbol, spot_price_quote))
        tolerance = max(1e-12, abs(spot_price_quote) * 1e-10)
        if not math.isfinite(precise_spot_price) or precise_spot_price <= 0:
            raise ValueError(f"invalid precision-adjusted Spot price for {spot_symbol}")
        if precise_spot_price > spot_price_quote + tolerance:
            raise ValueError("Spot buy price precision would round the limit upward")

        async def fresh_ask(client: Any, symbol: str, venue_label: str) -> float:
            fetcher = getattr(client, "fetch_ticker", None)
            if not callable(fetcher):
                raise ValueError(f"{venue_label} live ticker reader is unavailable for {symbol}")
            ticker = await fetcher(symbol)
            if not isinstance(ticker, dict):
                raise ValueError(f"{venue_label} returned an invalid ticker for {symbol}")
            try:
                ask = float(ticker.get("ask") or 0.0)
                timestamp_ms = int(ticker.get("timestamp") or 0)
            except (TypeError, ValueError):
                raise ValueError(f"{venue_label} ticker has invalid ask/timestamp for {symbol}")
            if not math.isfinite(ask) or ask <= 0 or timestamp_ms <= 0:
                raise ValueError(f"{venue_label} ticker is missing a valid ask/timestamp for {symbol}")
            age_ms = int(time.time() * 1000) - timestamp_ms
            if age_ms < -1000 or age_ms > max(1, int(max_ticker_age_ms)):
                raise ValueError(f"{venue_label} ticker age {age_ms}ms is outside the allowed window")
            return ask

        local_live_ask = await fresh_ask(local_client, spot_symbol, "Spot")
        global_live_ask = await fresh_ask(global_client, perp_symbol, "Futures")
        if global_ask_quote > 0 and max_price_deviation_pct > 0:
            deviation = abs(global_live_ask - global_ask_quote) / global_ask_quote
            if deviation > max_price_deviation_pct:
                raise ValueError(
                    f"live Futures ask moved {deviation:.3%} from scan snapshot "
                    f"(max {max_price_deviation_pct:.3%})"
                )
        if precise_spot_price >= local_live_ask:
            raise ValueError("precision-adjusted Spot buy limit would take the live ask")

        spot_amount_precision = getattr(local_client, "amount_to_precision", None)
        if not callable(spot_amount_precision):
            raise ValueError(f"Spot lot-size precision is unavailable for {spot_symbol}")
        spot_amount = float(spot_amount_precision(spot_symbol, amount))
        if not math.isfinite(spot_amount) or spot_amount <= 0 or spot_amount > amount + max(1e-12, amount * 1e-8):
            raise ValueError(f"invalid precision-adjusted Spot quantity for {spot_symbol}")

        contract_size = self._contract_size(global_client, perp_symbol)
        perp_amount_precision = getattr(global_client, "amount_to_precision", None)
        if not callable(perp_amount_precision):
            raise ValueError(f"Futures contract-lot precision is unavailable for {perp_symbol}")
        contracts = float(perp_amount_precision(perp_symbol, spot_amount / contract_size))
        if not math.isfinite(contracts) or contracts <= 0:
            raise ValueError(f"Spot quantity is below minimum Futures contract size for {perp_symbol}")
        common_amount = contracts * contract_size
        common_amount = float(spot_amount_precision(spot_symbol, common_amount))
        tolerance = max(1e-12, abs(common_amount) * 1e-8)
        if common_amount <= 0 or abs(common_amount - contracts * contract_size) > tolerance:
            raise ValueError("Spot lot size and Futures contract multiplier cannot represent the same base quantity")
        if common_amount > spot_amount + tolerance or common_amount > amount + tolerance:
            raise ValueError("Futures contract rounding would over-hedge the Spot quantity")
        contracts_final = float(perp_amount_precision(perp_symbol, common_amount / contract_size))
        if abs(contracts_final * contract_size - common_amount) > tolerance:
            raise ValueError("Futures contract precision cannot exactly hedge the Spot quantity")

        if common_amount + tolerance < spot_amount_min:
            raise ValueError(f"Spot quantity {common_amount} is below min amount {spot_amount_min}")
        spot_notional = common_amount * precise_spot_price
        if spot_notional + tolerance < spot_cost_min:
            raise ValueError(f"Spot notional {spot_notional} is below min notional {spot_cost_min}")
        if contracts_final + tolerance < perp_contract_min:
            raise ValueError(f"Futures contracts {contracts_final} are below min amount {perp_contract_min}")
        perp_notional = common_amount * global_live_ask
        if perp_notional + tolerance < perp_cost_min:
            raise ValueError(f"Futures notional {perp_notional} is below min notional {perp_cost_min}")

        if local_quote in ("USDT", "USDC"):
            precise_price_idr = precise_spot_price * usdt_idr_rate
        else:
            precise_price_idr = precise_spot_price
        return common_amount, precise_price_idr

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
        if not callable(precision):
            raise ValueError(f"futures amount precision is unavailable for {market_symbol}")
        contracts = float(precision(market_symbol, contracts))
        normalized_base = contracts * contract_size
        tolerance = max(1e-12, abs(float(base_amount)) * 1e-8)
        if not math.isfinite(contracts) or contracts <= 0:
            raise ValueError(f"base quantity {base_amount} rounds below minimum contract quantity")
        if abs(normalized_base - float(base_amount)) > tolerance:
            raise ValueError(
                f"base quantity {base_amount} is not exactly representable in {market_symbol} "
                f"contracts (contractSize={contract_size}, normalized={normalized_base}); refusing hedge"
            )
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
        local_amount = await self._to_spot_amount(client, market_symbol, amount)
        local_price = self._local_price_from_idr(client, market_symbol, price, usdt_idr_rate)
        precision = getattr(client, "price_to_precision", None)
        if not callable(precision):
            raise ValueError(f"Spot price precision is unavailable for {market_symbol}")
        precise_price = float(precision(market_symbol, local_price))
        tolerance = max(1e-12, abs(local_price) * 1e-10)
        if not math.isfinite(precise_price) or precise_price <= 0 or precise_price > local_price + tolerance:
            raise ValueError("Spot buy price precision is invalid or would round the limit upward")
        return await client.create_limit_buy_order(market_symbol, local_amount, precise_price, *_client_order_params(client_order_id))

    async def place_local_limit_sell(self, symbol: str, amount: float, price: float, client_order_id: Optional[str] = None, usdt_idr_rate: float = 0.0) -> Dict[str, Any]:
        """Places a Maker Limit Sell on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid sell params: amount={amount}, price={price}")
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        local_amount = await self._to_spot_amount(client, market_symbol, amount)
        local_price = self._local_price_from_idr(client, market_symbol, price, usdt_idr_rate)
        precision = getattr(client, "price_to_precision", None)
        if not callable(precision):
            raise ValueError(f"Spot price precision is unavailable for {market_symbol}")
        precise_price = float(precision(market_symbol, local_price))
        tolerance = max(1e-12, abs(local_price) * 1e-10)
        if precise_price + tolerance < local_price:
            raise ValueError("Spot sell price precision would round the limit downward")
        return await client.create_limit_sell_order(market_symbol, local_amount, precise_price, *_client_order_params(client_order_id))

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
        local_amount = await self._to_spot_amount(client, market_symbol, amount)
        return await client.create_market_sell_order(market_symbol, local_amount, *_client_order_params(client_order_id))

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
        if not matched:
            return []

        normalized = []
        for position in matched:
            item = dict(position)
            try:
                raw_contracts = float(item.get("contracts"))
            except (TypeError, ValueError):
                raise ValueError("exchange futures position is missing a valid contracts quantity")
            if not math.isfinite(raw_contracts) or raw_contracts < 0:
                raise ValueError("exchange futures position contains invalid contracts quantity")
            try:
                contract_size = float(item.get("contractSize") or self._contract_size(client, market_symbol))
            except (TypeError, ValueError):
                raise ValueError("exchange futures position has no valid contractSize")
            if not math.isfinite(contract_size) or contract_size <= 0:
                raise ValueError("exchange futures position has invalid contractSize")
            item["contracts_contracts"] = raw_contracts
            item["contracts"] = raw_contracts * contract_size
            item["contractSize"] = contract_size
            normalized.append(item)
        return normalized

    async def open_global_short_market(self, symbol: str, amount: float, client_order_id: Optional[str] = None, price: Optional[float] = None) -> Dict[str, Any]:
        """
        Executes an instant Short 1x Market Hedge on Global Futures.
        ``price`` is an optional reference quote supplied by the engine; the
        order remains a market order and the venue-reported fill is authoritative.
        STRICT: Only executes Market SELL. Never buy/long to open.
        """
        if amount <= 0:
            raise ValueError(f"invalid short amount: {amount}")
        # Scanner and executor quantities are base-asset units; CCXT derivatives
        # generally expect a number of contracts. Convert and normalize fills.
        client = self._global_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        leverage_setter = getattr(client, "set_leverage", None)
        if not callable(leverage_setter):
            raise RuntimeError(f"{getattr(client, 'id', 'futures venue')} cannot set/verify 1x leverage")
        # Keep the futures leg at 1x; if the exchange rejects this setting, do not open the hedge.
        await leverage_setter(1, market_symbol)
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

    async def fetch_local_ticker(self, symbol: str) -> Dict[str, Any]:
        """Fetches the current Spot ticker for final entry revalidation."""
        client = self._local_for(symbol)
        market_symbol = await self._resolve_symbol(client, symbol)
        return await client.fetch_ticker(market_symbol)

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

    async def fetch_local_ticker(self, symbol: str) -> Dict[str, Any]:
        """Returns the latest paper Spot ticker with a receive timestamp."""
        clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
        price = self.market_prices.get(clean_sym) or {}
        return {
            "symbol": symbol,
            "bid": float(price.get("bid", 0.0)),
            "ask": float(price.get("ask", 0.0)),
            "timestamp": int(float(price.get("updated_at", time.time())) * 1000),
        }

    async def fetch_global_ticker(self, symbol: str) -> Dict[str, Any]:
        """Mirrors the live client: returns the last known Futures ticker."""
        clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
        price = self.market_prices.get(clean_sym) or {}
        return {
            "symbol": symbol,
            "bid": float(price.get("bid", 0.0)),
            "ask": float(price.get("ask", 0.0)),
            "timestamp": int(float(price.get("updated_at", time.time())) * 1000),
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
    global_type = "future" if str(global_name).lower() == "binance" else "swap"
    global_ex = init_ccxt_exchange(global_name, global_creds, default_type=global_type)
    return ExchangeClient(
        local_ex,
        global_ex,
        local_exchanges={str(local_name).lower(): local_ex},
        global_exchanges={str(global_name).lower(): global_ex},
        local_default_name=str(local_name).lower(),
        global_default_name=str(global_name).lower(),
    )
