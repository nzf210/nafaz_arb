import asyncio
import math
import time
from typing import Optional, Dict, Any
import ccxt.async_support as ccxt

from executor.indodax_v2 import IndodaxTAPIv2
from executor.quantity import AmountSpec, base_to_order_amount, order_amount_to_base
from executor.helpers import _broker_prefix_for


def _client_order_params(client_order_id: Optional[str]) -> tuple:
    """
    Builds the positional params argument for a ccxt create_*_order call.

    Passing a client order id makes retries idempotent at the venue: a duplicate
    submit is rejected instead of double-filling. When no id is supplied the
    argument is omitted entirely so ccxt does not see an empty params dict.
    """
    return ({"clientOrderId": client_order_id},) if client_order_id else ()

def _local_order_params(
    exchange: Any,
    client_order_id: Optional[str],
    amount_spec: AmountSpec,
    require_amount_step: bool = False,
    broker_prefix: str = "",
) -> tuple:
    venue_id = venue_client_order_id(exchange, client_order_id, broker_prefix)
    params = {"clientOrderId": venue_id} if venue_id else {}
    if isinstance(exchange, IndodaxTAPIv2):
        if amount_spec.unit != "BASE":
            raise ValueError("Indodax spot amount rules must use BASE units")
        if amount_spec.step <= 0 and require_amount_step:
            raise ValueError("Indodax orders require the market quantity increment")
        params["quantityIncrement"] = amount_spec.step
    return (params,) if params else ()

VENUE_CLIENT_ORDER_ID_MAX_LEN = 36

def venue_client_order_id(
    exchange: Any, client_order_id: Optional[str], broker_prefix: str = ""
) -> Optional[str]:
    """Prefixes a broker id for Binance Cloud venues when the id has none.

    ``broker_prefix`` is the account's registered broker id. With no prefix the
    id is sent unchanged; ccxt then mints its own venue-native client id, which
    the venue accepts, rather than rejecting a guessed prefix with error 3703.

    The prefix is prepended here, so the pair slug -- the only free-form part --
    is trimmed to keep the final id within the venue's 36-character limit.
    Callers therefore need not know the prefix when building the id.
    """
    if not client_order_id:
        return client_order_id
    exchange_id = str(getattr(exchange, "id", "") or "").lower()
    if exchange_id != "tokocrypto" or not broker_prefix:
        return client_order_id
    if client_order_id.startswith(broker_prefix):
        return client_order_id[:VENUE_CLIENT_ORDER_ID_MAX_LEN]
    # Trim the free-form pair slug, never the trailing "-{leg}-{seq}" that keeps
    # a retried submit idempotent at the venue.
    room = VENUE_CLIENT_ORDER_ID_MAX_LEN - len(broker_prefix)
    if room < 1:
        raise ValueError("broker prefix leaves no room for a client order id")
    if len(client_order_id) > room:
        leg_tail = ""
        parts = client_order_id.rsplit("-", 2)
        if len(parts) == 3:
            leg_tail = f"-{parts[1]}-{parts[2]}"
        keep = room - len(leg_tail)
        if keep < 1:
            raise ValueError("client order ID leaves no room for the pair slug")
        client_order_id = f"{client_order_id[:keep]}{leg_tail}"
    return f"{broker_prefix}{client_order_id}"


def _require_amount_spec(value: Any) -> AmountSpec:
    spec = value if isinstance(value, AmountSpec) else AmountSpec.from_dict(value)
    if spec is None:
        raise ValueError("market amount rules are required before submitting an order")
    spec.validate()
    return spec


def _base_order_result(order: Dict[str, Any], spec: AmountSpec) -> Dict[str, Any]:
    """Keep ExchangeClient responses in base-asset units, regardless of venue API units."""
    normalized = dict(order)
    for key in ("amount", "filled", "remaining"):
        if normalized.get(key) is not None:
            normalized[key] = order_amount_to_base(float(normalized[key]), spec)
    return normalized

def _expose_client_order_id(order: Dict[str, Any]) -> Dict[str, Any]:
    """Adds a snake_case ``client_order_id`` alias.

    Every venue except indodax answers with ccxt's ``clientOrderId``; normalizing
    here means the recovery matcher never has to know which venue produced the
    payload.
    """
    if isinstance(order, dict) and order.get("client_order_id") is None:
        alias = order.get("clientOrderId")
        if alias:
            order = dict(order)
            order["client_order_id"] = alias
    return order

def _normalize_open_order(order: Dict[str, Any], symbol: str, spec: Optional[AmountSpec]) -> Dict[str, Any]:
    """Canonical shape for an open order discovered directly on the venue.

    ``symbol`` is the bot's own symbol when the caller asked for one, because the
    whole point of reading the venue is to match against bot state.
    """
    normalized = dict(order)
    if spec is not None:
        normalized = _base_order_result(normalized, spec)
    normalized.setdefault("symbol", symbol)
    if not normalized.get("symbol"):
        normalized["symbol"] = symbol
    normalized.pop("info", None)
    return _expose_client_order_id(normalized)


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
        local_exchange: Any,
        global_exchange: Any,
        balance_exchanges: Optional[Dict[str, Dict[str, Any]]] = None,
        pair_info: Any = None,
    ):
        self.local = local_exchange
        self.global_fut = global_exchange
        self._pair_info = pair_info
        self._fee_cache: Dict[tuple, tuple] = {}
        # Registered broker id for Binance Cloud venues (Tokocrypto). Read from
        # the local client's own options, so it travels with the account;
        # empty means "let the venue mint the client id".
        self._local_broker_prefix = _broker_prefix_for(
            getattr(local_exchange, "id", ""), local_exchange
        )
        self.balance_exchanges = balance_exchanges or {
            getattr(local_exchange, "id", "local"): {"market": "spot", "client": local_exchange},
            getattr(global_exchange, "id", "global"): {"market": "futures", "client": global_exchange},
        }

    async def _resolve_symbol(self, exchange: Any, symbol: str, market_kind: str) -> str:
        """Resolve scanner market IDs to the venue's CCXT unified symbol."""
        if isinstance(exchange, IndodaxTAPIv2):
            return symbol
        markets = getattr(exchange, "markets", None)
        if not markets:
            loader = getattr(exchange, "load_markets", None)
            if callable(loader):
                markets = await loader()
        if not isinstance(markets, dict) or not markets:
            return symbol

        exchange_id = str(getattr(exchange, "id", "") or "").lower()
        raw_id = str(symbol or "").strip()
        if ":" in raw_id and raw_id.split(":", 1)[0].lower() in {
            exchange_id, "toko", "okx", "mexc",
        }:
            raw_id = raw_id.split(":", 1)[1]
        normalized_id = "".join(char for char in raw_id.upper() if char.isalnum())
        pair_base = str(getattr(self._pair_info, "base_asset", "") or "").upper()
        pair_quote = str(
            getattr(self._pair_info, "global_quote_asset", "")
            if market_kind == "futures"
            else getattr(self._pair_info, "quote_asset", "")
        ).upper()
        candidates = []
        for market in markets.values():
            if not isinstance(market, dict):
                continue
            market_id = "".join(
                char for char in str(market.get("id", "")).upper() if char.isalnum()
            )
            if market_id != normalized_id:
                continue
            if pair_base and str(market.get("base", "")).upper() != pair_base:
                continue
            if pair_quote and str(market.get("quote", "")).upper() != pair_quote:
                # Synthetic IDR pairs may map BIDR scanner markets to an IDR
                # CCXT alias; exact venue market ID remains authoritative.
                quote_alias = {pair_quote, "BIDR" if pair_quote == "IDR" else pair_quote}
                if str(market.get("quote", "")).upper() not in quote_alias:
                    continue
            if market_kind == "spot":
                if market.get("swap") or market.get("future") or market.get("contract"):
                    continue
            elif not (market.get("swap") or market.get("future") or market.get("contract")):
                continue
            candidates.append(market)

        if len(candidates) == 1 and candidates[0].get("symbol"):
            return str(candidates[0]["symbol"])
        if len(candidates) > 1:
            raise ValueError(f"ambiguous {market_kind} market symbol {symbol}")
        raise ValueError(
            f"scanner symbol {symbol} did not match a {market_kind} market "
            f"on {exchange_id or 'exchange'}"
        )

    async def place_local_limit_buy(
        self, symbol: str, amount: float, price: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """Places a Maker Limit Buy on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid buy params: amount={amount}, price={price}")
        spec = _require_amount_spec(amount_spec)
        if spec.unit != "BASE":
            raise ValueError("spot order amount rules must use BASE units")
        order_amount = base_to_order_amount(amount, spec)
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.create_limit_buy_order(
            order_symbol,
            order_amount,
            price,
            *_local_order_params(
                self.local, client_order_id, spec, require_amount_step=True,
                broker_prefix=self._local_broker_prefix,
            ),
        )
        return _base_order_result(order, spec)

    async def place_local_limit_sell(
        self, symbol: str, amount: float, price: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """Places a Maker Limit Sell on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid sell params: amount={amount}, price={price}")
        spec = _require_amount_spec(amount_spec)
        if spec.unit != "BASE":
            raise ValueError("spot order amount rules must use BASE units")
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.create_limit_sell_order(
            order_symbol,
            base_to_order_amount(amount, spec),
            price,
            *_local_order_params(
                self.local, client_order_id, spec,
                broker_prefix=self._local_broker_prefix,
            ),
        )
        return _base_order_result(order, spec)

    async def cancel_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancels an existing order on local spot exchange."""
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        return await self.local.cancel_order(order_id, order_symbol)

    async def place_local_market_sell(
        self, symbol: str, amount: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """Taker market sell on local spot. Used only for emergency de-risking."""
        if amount <= 0:
            raise ValueError(f"invalid market sell amount: {amount}")
        spec = _require_amount_spec(amount_spec)
        if spec.unit != "BASE":
            raise ValueError("spot order amount rules must use BASE units")
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.create_market_sell_order(
            order_symbol, base_to_order_amount(amount, spec),
            *_local_order_params(
                self.local, client_order_id, spec,
                broker_prefix=self._local_broker_prefix,
            ),
        )
        return _base_order_result(order, spec)

    async def fetch_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Fetches status of an order on local spot exchange."""
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.fetch_order(order_id, order_symbol)
        return _expose_client_order_id(order)

    async def fetch_local_open_orders(
        self, symbol: Optional[str] = None, amount_spec: Any = None
    ) -> list:
        """Opens orders resting on the local spot venue.

        Raises when the venue cannot answer, so a caller can never mistake
        "unknown" for "nothing is resting" and release funds that are still
        committed. ``amount_spec`` normalizes venue units back to base units.
        """
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        resolved_symbol = await self._resolve_symbol(self.local, symbol, "spot") if symbol else None
        raw = await self.local.fetch_open_orders(resolved_symbol)
        if raw is None or not isinstance(raw, list):
            raise ValueError("local venue returned a malformed open orders payload")
        return [_normalize_open_order(order, symbol or "", spec) for order in raw]


    async def fetch_pair_fee_rates(self, pair_info: Any, cached_profiles: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
        """Fetch account-specific rates where the venue exposes a supported endpoint.

        CCXT rates are fractions and are converted to percentage points. Indodax
        currently has no CCXT fetchTradingFee implementation; its explicit
        scanner fee profile is retained while the perpetual venue can still
        refresh independently. Tokocrypto's account endpoint reports bips.
        """
        async def read(exchange: Any, symbol: str, market_kind: str):
            exchange_id = str(getattr(exchange, "id", "") or "").lower()
            resolved = await self._resolve_symbol(exchange, symbol, market_kind)
            cache_key = (exchange_id, market_kind, resolved)
            cached = (cached_profiles or {}).get("spot" if market_kind == "spot" else "perp")
            if cached and cached.get("fetched_at_ms", 0) > 0:
                return cached
            cached = self._fee_cache.get(cache_key)
            if cached and time.time() - cached[0] < 300:
                return cached[1]
            fetcher = getattr(exchange, "fetch_trading_fee", None)
            if exchange_id == "tokocrypto" and market_kind == "spot":
                endpoint = (
                    getattr(exchange, "privateGetOpenV1AccountSpot", None)
                    or getattr(exchange, "private_get_open_v1_account_spot", None)
                )
                if callable(endpoint):
                    raw = await endpoint()
                    account = raw.get("data", {}) if isinstance(raw, dict) else {}
                    if not isinstance(account, dict):
                        raise ValueError("invalid Tokocrypto account fee response")
                    maker_bips = account.get("makerCommission")
                    taker_bips = account.get("takerCommission")
                    if maker_bips is None or taker_bips is None:
                        raise ValueError("Tokocrypto account response omitted commission rates")
                    # Tokocrypto account commission values are basis points (1 bp = 0.01%).
                    maker_pct, taker_pct = float(maker_bips) / 100.0, float(taker_bips) / 100.0
                elif callable(fetcher):
                    raw = await fetcher(resolved)
                    maker, taker = raw.get("maker"), raw.get("taker")
                    if maker is None or taker is None:
                        raise ValueError("incomplete Tokocrypto spot trading fee response")
                    maker_pct, taker_pct = float(maker) * 100.0, float(taker) * 100.0
                else:
                    return None
            elif callable(fetcher):
                raw = await fetcher(resolved)
                if not isinstance(raw, dict):
                    raise ValueError(f"invalid {market_kind} trading fee response")
                maker, taker = raw.get("maker"), raw.get("taker")
                if maker is None or taker is None:
                    raise ValueError(f"incomplete {market_kind} trading fee response")
                maker_pct, taker_pct = float(maker) * 100.0, float(taker) * 100.0
            else:
                return None
            if not all(math.isfinite(v) and 0 <= v <= 100 for v in (maker_pct, taker_pct)):
                raise ValueError(f"invalid {market_kind} fee rates")
            rates = {
                "maker": maker_pct,
                "taker": taker_pct,
                "fetched_at_ms": int(time.time() * 1000),
            }
            self._fee_cache[cache_key] = (time.time(), rates)
            return rates

        errors = {}
        profiles = {}
        for key, exchange, symbol, market_kind in (
            ("spot", self.local, pair_info.local_symbol, "spot"),
            ("perp", self.global_fut, pair_info.global_symbol, "futures"),
        ):
            try:
                profiles[key] = await read(exchange, symbol, market_kind)
            except Exception as exc:
                profiles[key] = None
                errors[key] = f"{type(exc).__name__}: {exc}"

        available = [profile for profile in profiles.values() if profile is not None]
        return {
            "spot": profiles["spot"],
            "perp": profiles["perp"],
            "source": "EXCHANGE_API" if len(available) == 2 else ("PARTIAL_EXCHANGE_API" if available else "CONFIG"),
            "verified": len(available) == 2,
            "fetched_at_ms": max((int(profile.get("fetched_at_ms", 0)) for profile in available), default=0),
            "errors": errors,
        }

    async def fetch_balance_report(self) -> list:
        """Fetch read-only balances for every configured venue independently."""
        reports = []
        for name, entry in sorted(self.balance_exchanges.items()):
            market = str(entry.get("market") or "unknown")
            exchange = entry.get("client")
            init_error = str(entry.get("error") or "")
            if exchange is None:
                reports.append({
                    "name": name, "market": market,
                    "status": "not_configured" if "not configured" in init_error.lower() else "error",
                    "error": init_error or "exchange client is unavailable", "assets": [],
                })
                continue
            try:
                raw = await _fetch_exchange_balance(exchange, market)
                free_map, used_map, total_map = raw.get("free") or {}, raw.get("used") or {}, raw.get("total") or {}
                assets = []
                for asset in sorted(set(free_map) | set(used_map) | set(total_map)):
                    free_raw, used_raw, total_raw = free_map.get(asset), used_map.get(asset), total_map.get(asset)
                    free = float(free_raw or 0.0)
                    total = float(total_raw) if total_raw is not None else 0.0
                    used = float(used_raw) if used_raw is not None else max(0.0, total - free)
                    if total_raw is None: total = free + used
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
        futures_raw = await _fetch_exchange_balance(self.global_fut, "futures")

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

    def for_pair(self, pair_info: Any) -> "ExchangeClient":
        """Return an immutable facade bound to the exact spot/futures venue pair."""
        local_name = str(getattr(pair_info, "local_exchange", "") or "").strip().lower()
        global_name = str(getattr(pair_info, "global_exchange", "") or "").strip().lower()
        local_entry = self.balance_exchanges.get(local_name)
        global_entry = self.balance_exchanges.get(global_name)
        if not local_entry or local_entry.get("market") != "spot" or local_entry.get("client") is None:
            raise LookupError(f"spot client unavailable for venue: {local_name or 'unknown'}")
        if not global_entry or global_entry.get("market") != "futures" or global_entry.get("client") is None:
            raise LookupError(f"futures client unavailable for venue: {global_name or 'unknown'}")
        return ExchangeClient(
            local_entry["client"],
            global_entry["client"],
            self.balance_exchanges,
            pair_info=pair_info,
        )

    async def fetch_order_trades(
        self,
        market_type: str,
        symbol: str,
        *,
        since: Optional[int] = None,
        client_order_id: Optional[str] = None,
        order_id: Optional[str] = None,
        amount_spec: Any = None,
    ) -> list:
        """Read the venue's individual executions for exactly one bot order.

        Spot and perpetual clients are selected independently. INDODAX TAPI v2
        is queried in <=7-day windows using its clientOrderId filter; CCXT
        venues are queried through fetch_my_trades and filtered by order id.
        """
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

        raw_trades = []
        if isinstance(exchange, IndodaxTAPIv2):
            end_ms = int(time.time() * 1000)
            cursor = int(since or max(0, end_ms - 7 * 24 * 60 * 60 * 1000))
            while cursor <= end_ms:
                window_end = min(end_ms, cursor + 7 * 24 * 60 * 60 * 1000 - 1)
                params = {"startTime": cursor, "endTime": window_end, "sort": "asc"}
                if client_order_id:
                    params["clientOrderId"] = str(client_order_id)
                elif order_id:
                    params["orderId"] = str(order_id)
                chunk = await fetcher(resolved, cursor, 1000, params)
                if not isinstance(chunk, list):
                    raise ValueError("exchange returned malformed trade history")
                if len(chunk) >= 1000:
                    raise RuntimeError(
                        "INDODAX trade-history window reached its 1000-fill limit; "
                        "cannot prove complete order fill history"
                    )
                raw_trades.extend(chunk)
                cursor = window_end + 1
        else:
            raw_trades = await fetcher(resolved, int(since or 0) or None, 1000)
            if not isinstance(raw_trades, list):
                raise ValueError("exchange returned malformed trade history")
            if len(raw_trades) >= 1000 and since:
                oldest = min(
                    (int(item.get("timestamp") or 0) for item in raw_trades if isinstance(item, dict)),
                    default=0,
                )
                if oldest > int(since):
                    raise RuntimeError(
                        "exchange trade-history response reached its 1000-fill limit; "
                        "cannot prove complete order fill history"
                    )

        expected_order = str(order_id or "")
        expected_client = str(client_order_id or "")
        normalized = []
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        for trade in raw_trades:
            if not isinstance(trade, dict):
                raise ValueError("exchange returned a malformed trade row")
            info = trade.get("info") if isinstance(trade.get("info"), dict) else {}
            actual_order = str(trade.get("order") or trade.get("orderId") or info.get("orderId") or "")
            actual_client = str(
                trade.get("clientOrderId") or trade.get("client_order_id")
                or info.get("clientOrderId") or ""
            )
            if expected_order and actual_order and actual_order != expected_order:
                continue
            if expected_client and actual_client and actual_client != expected_client:
                continue
            if (expected_order or expected_client) and not (actual_order or actual_client):
                continue
            fill = dict(trade)
            fill["id"] = str(trade.get("id") or trade.get("tradeId") or info.get("tradeId") or "")
            fill["order"] = actual_order
            fill["clientOrderId"] = actual_client
            try:
                qty = float(trade.get("amount") if trade.get("amount") is not None else trade.get("qty"))
                price = float(trade.get("price"))
            except (TypeError, ValueError) as exc:
                raise ValueError("exchange trade has invalid quantity or price") from exc
            if not math.isfinite(qty) or qty <= 0 or not math.isfinite(price) or price <= 0:
                raise ValueError("exchange trade has non-positive or non-finite quantity/price")
            if spec is not None and spec.unit == "CONTRACT":
                qty = order_amount_to_base(qty, spec)
            fill["amount"] = qty
            fill["price"] = price
            fill["cost"] = float(
                trade.get("cost") if trade.get("cost") is not None
                else trade.get("quoteQty") if trade.get("quoteQty") is not None
                else qty * price
            )
            fee = trade.get("fee")
            if not isinstance(fee, dict):
                fee_rows = trade.get("fees") if isinstance(trade.get("fees"), list) else []
                fee = fee_rows[0] if fee_rows and isinstance(fee_rows[0], dict) else {
                    "cost": trade.get("commission", info.get("commission")),
                    "currency": trade.get("commissionAsset", info.get("commissionAsset")),
                }
            fill["fee"] = fee if isinstance(fee, dict) else None
            fill["timestamp"] = int(
                trade.get("timestamp") or trade.get("time") or info.get("time") or 0
            ) or None
            if not fill["id"]:
                raise ValueError("exchange trade is missing its immutable trade id")
            normalized.append(fill)
        return normalized

    async def fetch_positions(self, symbol: str) -> list:
        """Fetches open futures positions for a symbol (global exchange).

        A malformed position response must never be converted into an empty list:
        callers use an empty list as proof that the futures leg is flat.
        """
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("requested futures symbol is missing; flat state cannot be verified")
        symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        try:
            positions = await self.global_fut.fetch_positions([symbol])
        except (TypeError, AttributeError):
            positions = await self.global_fut.fetch_positions()

        if positions is None or not isinstance(positions, (list, tuple)):
            raise ValueError("exchange returned an invalid futures positions payload")

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

    async def open_global_short_market(
        self, symbol: str, amount: float, client_order_id: Optional[str] = None,
        amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """
        Executes an instant Short 1x Market Hedge on Global Futures.
        STRICT: Only executes Market SELL. Never buy/long to open.
        """
        if amount <= 0:
            raise ValueError(f"invalid short amount: {amount}")
        spec = _require_amount_spec(amount_spec)
        order_amount = base_to_order_amount(amount, spec)
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        # Market Sell on Futures opens a Short position
        order = await self.global_fut.create_market_sell_order(
            order_symbol, order_amount, *_client_order_params(client_order_id),
            {"reduceOnly": False},
        )
        return _base_order_result(order, spec)

    async def close_global_short_market(
        self, symbol: str, amount: float, client_order_id: Optional[str] = None,
        amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """
        Closes Global Short position by buying back sold coins with reduceOnly=True.
        STRICT: Must have reduceOnly=True so it can never inadvertently open a Long.
        """
        if amount <= 0:
            raise ValueError(f"invalid close amount: {amount}")
        spec = _require_amount_spec(amount_spec)
        order_amount = base_to_order_amount(amount, spec)
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        params = {"reduceOnly": True}
        if client_order_id:
            params["clientOrderId"] = client_order_id
        order = await self.global_fut.create_market_buy_order(order_symbol, order_amount, params)
        return _base_order_result(order, spec)

    async def fetch_global_order(
        self, order_id: str, symbol: str, amount_spec: Any = None
    ) -> Dict[str, Any]:
        """Fetches a futures order by id (hedge or close leg)."""
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        order = await self.global_fut.fetch_order(order_id, order_symbol)
        if not isinstance(order, dict):
            raise ValueError("global futures venue returned a malformed order payload")
        normalized = _base_order_result(order, spec) if spec is not None else dict(order)
        return _expose_client_order_id(normalized)

    async def fetch_global_open_orders(
        self, symbol: Optional[str] = None, amount_spec: Any = None
    ) -> list:
        """Open orders on the global futures venue (short hedge legs).

        The open global short legs are the highest-risk orders to lose across a
        restart: they carry margin and are invisible in the local spot book.
        """
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        resolved_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures") if symbol else None
        raw = await self.global_fut.fetch_open_orders(resolved_symbol)
        if raw is None or not isinstance(raw, list):
            raise ValueError("global futures venue returned a malformed open orders payload")
        return [_normalize_open_order(order, symbol or "", spec) for order in raw]

    async def fetch_funding_history(
        self, symbol: str, since: Optional[int] = None, limit: int = 100
    ) -> list:
        """Fetches actual settled funding payments for a perpetual contract."""
        fetcher = getattr(self.global_fut, "fetch_funding_history", None)
        if fetcher is None:
            raise NotImplementedError(
                f"{getattr(self.global_fut, 'id', 'global futures')} adapter does not support fetch_funding_history"
            )
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        return await fetcher(order_symbol, since, limit)

    async def fetch_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Fetches current funding rate and the venue's next settlement timestamp."""
        fetcher = getattr(self.global_fut, "fetch_funding_rate", None)
        if fetcher is None:
            raise NotImplementedError(
                f"{getattr(self.global_fut, 'id', 'global futures')} adapter does not support fetch_funding_rate"
            )
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        return await fetcher(order_symbol)

    async def fetch_global_ticker(self, symbol: str) -> Dict[str, Any]:
        """Fetches ticker from global futures exchange."""
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        return await self.global_fut.fetch_ticker(order_symbol)

    async def close(self):
        """Closes each unique exchange connection exactly once."""
        clients = [self.local, self.global_fut]
        clients.extend(entry.get("client") for entry in self.balance_exchanges.values() if entry.get("client") is not None)
        closed = set()
        for exchange in clients:
            if exchange is None or id(exchange) in closed:
                continue
            closed.add(id(exchange))
            closer = getattr(exchange, "close", None)
            if callable(closer):
                await closer()


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
        configured_exchanges=getattr(config, "exchanges", {}),
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

def _balance_fetch_params(exchange: Any, market: str) -> Dict[str, Any]:
    """Return explicit CCXT market params where a venue needs them."""
    exchange_id = str(getattr(exchange, "id", "") or "").lower()
    if market == "futures" and exchange_id == "mexc":
        return {"type": "swap"}
    return {}


async def _fetch_exchange_balance(exchange: Any, market: str) -> Dict[str, Any]:
    """Fetch a balance without changing legacy no-argument adapter contracts."""
    params = _balance_fetch_params(exchange, market)
    if params:
        return await exchange.fetch_balance(params)
    return await exchange.fetch_balance()


def _resolve_default_type(exchange_name: str, default_type: str) -> str:
    """Map the executor's generic futures type to each venue's CCXT market type."""
    if exchange_name.lower() == "mexc" and default_type.lower() in {"future", "futures"}:
        return "swap"
    return default_type


def init_ccxt_exchange(exchange_name: str, creds: Any, default_type: str = "spot") -> Any:
    """Helper to initialize a CCXT async exchange with proper credentials and settings."""
    normalized_name = exchange_name.lower()
    ex_class = getattr(ccxt, normalized_name)
    resolved_default_type = _resolve_default_type(normalized_name, default_type)
    cfg: Dict[str, Any] = {
        "apiKey": getattr(creds, "api_key", ""),
        "secret": getattr(creds, "api_secret", ""),
        "enableRateLimit": True,
        "options": {"defaultType": resolved_default_type},
    }
    if hasattr(creds, "options") and isinstance(creds.options, dict):
        cfg["options"].update(creds.options)
    # MEXC's CCXT futures market is named "swap". Keep it explicit even if
    # an old config.json still contains options.defaultType="future".
    if normalized_name == "mexc" and default_type.lower() in {"future", "futures"}:
        cfg["options"]["defaultType"] = "swap"

    if getattr(creds, "passphrase", None):
        cfg["password"] = creds.passphrase

    instance = ex_class(cfg)
    if getattr(creds, "testnet", False) and hasattr(instance, "set_sandbox_mode"):
        instance.set_sandbox_mode(True)
    return instance

BALANCE_MARKETS = {
    "tokocrypto": "spot",
    "indodax": "spot",
    "okx": "futures",
    "mexc": "futures",
    "binance": "futures",
    "bybit": "futures",
}


def create_ccxt_client(local_name: str, global_name: str, local_creds, global_creds, configured_exchanges=None) -> ExchangeClient:
    """Create the active trading pair clients and independent read-only balance clients."""
    local_ex = init_local_exchange(local_name, local_creds, default_type="spot")
    global_ex = init_ccxt_exchange(global_name, global_creds, default_type="future")
    balance_exchanges = {
        local_name.lower(): {"market": "spot", "client": local_ex},
        global_name.lower(): {"market": "futures", "client": global_ex},
    }
    credentials = configured_exchanges or {}
    for name, market in BALANCE_MARKETS.items():
        if name in balance_exchanges:
            continue
        creds = credentials.get(name)
        if creds is None:
            balance_exchanges[name] = {"market": market, "client": None, "error": "API key/secret not configured"}
            continue
        api_key = str(getattr(creds, "api_key", "") or "").strip()
        api_secret = str(getattr(creds, "api_secret", "") or "").strip()
        if not api_key or not api_secret:
            balance_exchanges[name] = {"market": market, "client": None, "error": "API key/secret not configured"}
            continue
        try:
            client = init_local_exchange(name, creds, default_type="spot") if market == "spot" else init_ccxt_exchange(name, creds, default_type="future")
            balance_exchanges[name] = {"market": market, "client": client}
        except Exception as exc:
            balance_exchanges[name] = {"market": market, "client": None, "error": f"balance client initialization failed: {exc}"}
    return ExchangeClient(local_ex, global_ex, balance_exchanges)
