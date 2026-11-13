"""exchange futures methods for the exchange facade."""

import time
from typing import Optional, Dict, Any

from executor.quantity import AmountSpec, base_to_order_amount
from executor.exchange_common import (
    _base_order_result, _expose_client_order_id, acknowledged_order_result,
    _normalize_open_order, _require_amount_spec, _find_order_by_client_id,
    futures_position_side_params,
)


class ExchangeFuturesMixin:
    """Focused asynchronous exchange adapter operations."""

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

    async def fetch_position_mode(self, symbol: Optional[str] = None) -> dict:
        """Read the account's current futures position mode without changing it."""
        fetcher = getattr(self.global_fut, "fetch_position_mode", None)
        if not callable(fetcher):
            raise RuntimeError("exchange does not expose a verifiable futures position-mode endpoint")
        if symbol is None:
            markets = getattr(self.global_fut, "markets", None)
            if not markets:
                loader = getattr(self.global_fut, "load_markets", None)
                if not callable(loader):
                    raise RuntimeError("futures markets are unavailable for position-mode lookup")
                markets = await loader()
            if isinstance(markets, dict):
                symbol = next(
                    (
                        str(market.get("symbol"))
                        for market in markets.values()
                        if isinstance(market, dict)
                        and market.get("symbol")
                        and (market.get("swap") or market.get("future") or market.get("contract"))
                    ),
                    None,
                )
        if not symbol:
            raise RuntimeError("no futures symbol is available for position-mode lookup")
        mode = await fetcher(symbol)
        if not isinstance(mode, dict) or not isinstance(mode.get("hedged"), bool):
            raise ValueError("exchange returned an unknown futures position mode")
        return mode

    async def fetch_all_positions(self) -> list:
        """Fetch every open futures position on this authenticated venue.

        Account-wide reconciliation must discover positions that are not present
        in the bot's local state. An empty list is accepted only when the venue
        returns a well-formed list; errors and malformed rows remain UNKNOWN.
        """
        positions = await self.global_fut.fetch_positions()
        if positions is None or not isinstance(positions, (list, tuple)):
            raise ValueError("exchange returned an invalid account-wide positions payload")
        normalized = []
        for position in positions:
            if not isinstance(position, dict):
                raise ValueError("exchange returned a malformed account-wide position")
            symbol = position.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ValueError(
                    "account-wide futures position is missing its symbol; exposure cannot be classified"
                )
            normalized.append(position)
        return normalized

    async def open_global_short_market(
        self, symbol: str, amount: float, client_order_id: Optional[str] = None,
        amount_spec: Any = None, price: float = 0.0,
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
        # Market Sell on Futures opens a Short position. CCXT accepts one
        # params mapping; merge the id and reduceOnly flag rather than passing
        # two positional mappings (which raises before the venue request).
        params = {"reduceOnly": False}
        params.update(futures_position_side_params(self.global_fut, "short"))
        if client_order_id:
            params["clientOrderId"] = client_order_id
        order = await self.global_fut.create_market_sell_order(
            order_symbol, order_amount, params
        )
        return acknowledged_order_result(
            self.global_fut, order, spec, client_order_id,
            self._global_broker_prefix, self._global_scope,
        )

    async def close_global_short_market(
        self, symbol: str, amount: float, client_order_id: Optional[str] = None,
        amount_spec: Any = None,
    ) -> Dict[str, Any]:
        """
        Closes the short using the account-mode-specific venue parameters.
        OKX hedge mode closes by (side=buy, posSide=short); reduceOnly is only
        supported for OKX FUTURES/SWAP in net mode. Other supported venues use
        reduceOnly in their required one-way mode. Startup verifies these modes.
        """
        if amount <= 0:
            raise ValueError(f"invalid close amount: {amount}")
        spec = _require_amount_spec(amount_spec)
        order_amount = base_to_order_amount(amount, spec)
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        params = futures_position_side_params(self.global_fut, "short")
        if str(getattr(self.global_fut, "id", "") or "").lower() != "okx":
            params["reduceOnly"] = True
        if client_order_id:
            params["clientOrderId"] = client_order_id
        order = await self.global_fut.create_market_buy_order(order_symbol, order_amount, params)
        return acknowledged_order_result(
            self.global_fut, order, spec, client_order_id,
            self._global_broker_prefix, self._global_scope,
        )

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

    async def fetch_global_order_by_client_id(
        self, client_order_id: str, symbol: str, amount_spec: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Finds an acknowledged futures order using its exact client id."""
        spec = AmountSpec.from_dict(amount_spec) if amount_spec else None
        order_symbol = await self._resolve_symbol(self.global_fut, symbol, "futures")
        order = await _find_order_by_client_id(
            self.global_fut, order_symbol, client_order_id,
            self._global_broker_prefix, self._global_scope,
        )
        if order is None:
            return None
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
        clients.extend(
            entry.get("client")
            for entry in self.balance_exchanges.values()
            if entry.get("client") is not None
        )
        closed = set()
        for exchange in clients:
            if exchange is None or id(exchange) in closed:
                continue
            closed.add(id(exchange))
            closer = getattr(exchange, "close", None)
            if callable(closer):
                await closer()
