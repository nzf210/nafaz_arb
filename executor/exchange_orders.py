"""exchange order methods for the exchange facade."""

from typing import Optional, Dict, Any

from executor.indodax_v2 import IndodaxTAPIv2
from executor.quantity import AmountSpec, base_to_order_amount
from executor.exchange_common import (
    _accepted_client_order_ids,
    _base_order_result, _expose_client_order_id, _local_order_params,
    _normalize_open_order, _require_amount_spec, _find_order_by_client_id,
    venue_client_order_id,
)



SPOT_BASE_AMOUNT_ERROR = "spot order amount rules must use BASE units"


class ExchangeOrderMixin:
    """Focused asynchronous exchange adapter operations."""

    @staticmethod
    def _strip_venue_prefix(symbol: str) -> str:
        """Drop a scanner venue tag ("toko:ATH_USDT" -> "ATH_USDT").

        The Go scanner prefixes non-Indodax spot markets with their venue, and
        the tag is carried inside order records. Venue APIs take the bare market
        id, so the tag must never reach the wire.
        """
        raw_id = str(symbol or "").strip()
        if ":" in raw_id:
            prefix, remainder = raw_id.split(":", 1)
            if prefix.strip().lower() in {"toko", "okx", "mexc", "indodax"}:
                return remainder
        return raw_id

    @staticmethod
    def _normalized_market_id(symbol: str, exchange_id: str) -> str:
        raw_id = ExchangeOrderMixin._strip_venue_prefix(symbol)
        return "".join(char for char in raw_id.upper() if char.isalnum())

    @staticmethod
    def _market_matches_pair(
        market: Any, normalized_id: str, pair_base: str,
        pair_quote: str, market_kind: str,
    ) -> bool:
        if not isinstance(market, dict):
            return False
        market_id = "".join(
            char for char in str(market.get("id", "")).upper()
            if char.isalnum()
        )
        if market_id != normalized_id:
            return False
        if pair_base and str(market.get("base", "")).upper() != pair_base:
            return False
        market_quote = str(market.get("quote", "")).upper()
        if pair_quote and market_quote != pair_quote:
            quote_aliases = {
                pair_quote,
                "BIDR" if pair_quote == "IDR" else pair_quote,
            }
            if market_quote not in quote_aliases:
                return False
        is_contract = bool(
            market.get("swap") or market.get("future") or market.get("contract")
        )
        return not is_contract if market_kind == "spot" else is_contract

    async def _resolve_symbol(
        self, exchange: Any, symbol: str, market_kind: str
    ) -> str:
        """Resolve scanner market IDs to the venue's CCXT unified symbol."""
        if isinstance(exchange, IndodaxTAPIv2):
            # The scanner tags non-Indodax spot markets with a venue prefix
            # ("toko:ATH_USDT"), and its order records carry that tag through to
            # recovery. Indodax's API rejects it with -1121 Invalid symbol, so
            # the tag is stripped before the id reaches the wire.
            return self._strip_venue_prefix(symbol)
        markets = getattr(exchange, "markets", None)
        if not markets:
            loader = getattr(exchange, "load_markets", None)
            if callable(loader):
                markets = await loader()
        if not isinstance(markets, dict) or not markets:
            return symbol

        exchange_id = str(getattr(exchange, "id", "") or "").lower()
        normalized_id = self._normalized_market_id(symbol, exchange_id)
        pair_base = str(
            getattr(self._pair_info, "base_asset", "") or ""
        ).upper()
        quote_asset = (
            getattr(self._pair_info, "global_quote_asset", "")
            if market_kind == "futures"
            else getattr(self._pair_info, "quote_asset", "")
        )
        pair_quote = str(quote_asset or "").upper()
        candidates = [
            market for market in markets.values()
            if self._market_matches_pair(
                market, normalized_id, pair_base, pair_quote, market_kind
            )
        ]
        if len(candidates) == 1 and candidates[0].get("symbol"):
            return str(candidates[0]["symbol"])
        if len(candidates) > 1:
            raise ValueError(f"ambiguous {market_kind} market symbol {symbol}")
        raise ValueError(
            f"scanner symbol {symbol} did not match a {market_kind} market "
            f"on {exchange_id or 'exchange'}"
        )

    def _normalize_local_client_order_id(
        self, order: Dict[str, Any], logical_client_order_id: str = ""
    ) -> Dict[str, Any]:
        """Translate a venue wire id back to the durable logical client id.

        Tokocrypto rewrites the id into a hash-based alphanumeric form, so the
        wire value cannot be mapped back by reversing the transform. The caller
        already knows which logical id it searched for, and that is exactly the
        id the registry is keyed on, so it is adopted when supplied.
        """
        normalized = _expose_client_order_id(order)
        prefix = str(getattr(self, "_local_broker_prefix", "") or "")
        if not isinstance(normalized, dict):
            return normalized
        client_id = str(
            normalized.get("client_order_id")
            or normalized.get("clientOrderId")
            or ""
        )
        logical_id = ""
        if logical_client_order_id and client_id != logical_client_order_id:
            exchange_id = str(getattr(self.local, "id", "") or "").lower()
            if exchange_id == "tokocrypto" and client_id in _accepted_client_order_ids(
                self.local, logical_client_order_id
            ):
                logical_id = logical_client_order_id
        if not logical_id and prefix and client_id.startswith(prefix):
            logical_id = client_id[len(prefix):]
        if logical_id and logical_id != client_id:
            normalized = dict(normalized)
            normalized["clientOrderId"] = logical_id
            normalized["client_order_id"] = logical_id
        return normalized

    async def fetch_local_order_book(
        self, symbol: str, limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Fetch the live local spot book using the venue-native adapter."""
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        fetcher = getattr(self.local, "fetch_order_book", None)
        if not callable(fetcher):
            raise NotImplementedError(
                f"{getattr(self.local, 'id', 'local')} does not expose an order book"
            )
        book = await fetcher(order_symbol, limit)
        if not isinstance(book, dict):
            raise ValueError("local venue returned a malformed order book")
        bids, asks = book.get("bids"), book.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list):
            raise ValueError("local venue returned malformed order book levels")
        return book

    async def place_local_limit_buy(
        self, symbol: str, amount: float, price: float,
        client_order_id: Optional[str] = None, amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """Places a Maker Limit Buy on local spot exchange."""
        if amount <= 0 or price <= 0:
            raise ValueError(f"invalid buy params: amount={amount}, price={price}")
        spec = _require_amount_spec(amount_spec)
        if spec.unit != "BASE":
            raise ValueError(SPOT_BASE_AMOUNT_ERROR)
        order_amount = base_to_order_amount(amount, spec)
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.create_limit_buy_order(
            order_symbol,
            order_amount,
            price,
            *_local_order_params(
                self.local, client_order_id, spec, require_amount_step=True,
                broker_prefix=self._local_broker_prefix,
                require_amount_step=True,
                require_post_only=(
                    isinstance(self.local, IndodaxTAPIv2)
                    or str(getattr(self.local, "id", "")).lower() == "tokocrypto"
                ),
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
            raise ValueError(SPOT_BASE_AMOUNT_ERROR)
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.create_limit_sell_order(
            order_symbol,
            base_to_order_amount(amount, spec),
            price,
            *_local_order_params(
                self.local, client_order_id, spec,
                broker_prefix=self._local_broker_prefix,
                require_post_only=(
                    isinstance(self.local, IndodaxTAPIv2)
                    or str(getattr(self.local, "id", "")).lower() == "tokocrypto"
                ),
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
            raise ValueError(SPOT_BASE_AMOUNT_ERROR)
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.create_market_sell_order(
            order_symbol, base_to_order_amount(amount, spec),
            *_local_order_params(
                self.local, client_order_id, spec,
                broker_prefix=self._local_broker_prefix,
                require_amount_step=(
                    str(getattr(self.local, "id", "")).lower() == "tokocrypto"
                ),
            ),
        )
        return _base_order_result(order, spec)

    async def fetch_local_order(self, order_id: str, symbol: str) -> Dict[str, Any]:
        """Fetches status of an order on local spot exchange."""
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        order = await self.local.fetch_order(order_id, order_symbol)
        return self._normalize_local_client_order_id(order)

    async def fetch_local_order_by_client_id(
        self, client_order_id: str, symbol: str, amount_spec: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Finds an order acknowledged by the spot venue using its client id."""
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        order_symbol = await self._resolve_symbol(self.local, symbol, "spot")
        # The finder derives every accepted wire spelling from the logical id;
        # passing an already-rewritten id would make it rewrite a second time.
        order = await _find_order_by_client_id(
            self.local, order_symbol, client_order_id, self._local_broker_prefix
        )
        if order is None:
            return None
        normalized = _base_order_result(order, spec) if spec is not None else dict(order)
        return self._normalize_local_client_order_id(normalized, client_order_id)

    async def fetch_local_open_orders(
        self, symbol: Optional[str] = None, amount_spec: Any = None
    ) -> list:
        """Opens orders resting on the local spot venue.

        Raises when the venue cannot answer, so a caller can never mistake
        "unknown" for "nothing is resting" and release funds that are still
        committed. ``amount_spec`` normalizes venue units back to base units.
        """
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        is_tokocrypto = str(getattr(self.local, "id", "")).lower() == "tokocrypto"
        if is_tokocrypto and not symbol:
            raise ValueError("Tokocrypto open-order discovery requires a symbol")
        resolved_symbol = await self._resolve_symbol(self.local, symbol, "spot") if symbol else None
        if is_tokocrypto:
            raw = await self.local.fetch_open_orders(resolved_symbol, None, 1000)
        else:
            raw = await self.local.fetch_open_orders(resolved_symbol)
        if raw is None or not isinstance(raw, list):
            raise ValueError("local venue returned a malformed open orders payload")
        return [
            self._normalize_local_client_order_id(
                _normalize_open_order(order, symbol or "", spec)
            )
            for order in raw
        ]
