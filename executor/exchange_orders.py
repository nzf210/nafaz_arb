"""exchange order methods for the exchange facade."""

from typing import Optional, Dict, Any

from executor.indodax_v2 import IndodaxTAPIv2
from executor.quantity import AmountSpec, base_to_order_amount
from executor.exchange_common import (
    _base_order_result, _expose_client_order_id, _local_order_params,
    _normalize_open_order, _require_amount_spec,
)



class ExchangeOrderMixin:
    """Focused asynchronous exchange adapter operations."""

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
