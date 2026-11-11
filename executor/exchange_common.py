"""Shared venue order normalization and client-id helpers."""

from typing import Optional, Dict, Any

from executor.indodax_v2 import IndodaxTAPIv2
from executor.quantity import AmountSpec, order_amount_to_base



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
