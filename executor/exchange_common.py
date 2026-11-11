"""Shared venue order normalization and client-id helpers."""

import hashlib
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
    require_post_only: bool = False,
) -> tuple:
    venue_id = venue_client_order_id(exchange, client_order_id, broker_prefix)
    params = {"clientOrderId": venue_id} if venue_id else {}
    if isinstance(exchange, IndodaxTAPIv2):
        if amount_spec.unit != "BASE":
            raise ValueError("Indodax spot amount rules must use BASE units")
        if amount_spec.step <= 0 and require_amount_step:
            raise ValueError("Indodax orders require the market quantity increment")
        params["quantityIncrement"] = amount_spec.step
        # MOC is attached only by the explicit limit-order call sites.
        if require_post_only:
            params["timeInForce"] = "MOC"
    return (params,) if params else ()

VENUE_CLIENT_ORDER_ID_MAX_LEN = 36

# Tokocrypto validates client order ids far more strictly than its ccxt
# defaults suggest: the venue rejects anything longer than 32 characters and
# anything containing a non-alphanumeric character with error 3703
# "Invalid client ID" (verified against the live venue). Indodax, Binance and
# OKX all accept the dashed ``{pair}-{leg}-{seq}`` ids, so the tightening is
# applied only to this exchange.
TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN = 32

def _tokocrypto_client_order_id(
    client_order_id: str, broker_prefix: str = ""
) -> str:
    """Rewrites a logical client id into the form Tokocrypto accepts.

    A dashed ``{pair}-{leg}-{seq}`` id is both too long and wrongly
    punctuated, so the venue answers 3703 and the order is never accepted.
    The pair slug is the only free-form part, so it is replaced by a short
    hash; the ``{leg}{seq}`` tail is kept verbatim so a retried submit still
    presents the same id and the venue can drop the duplicate instead of
    double-filling.
    """
    parts = client_order_id.rsplit("-", 2)
    if len(parts) == 3:
        leg, seq = parts[1], parts[2]
    else:
        leg, seq = "", ""
    tail = "".join(c for c in f"{leg}{seq}" if c.isalnum())
    prefix = "".join(c for c in broker_prefix if c.isalnum())
    digest = hashlib.sha256(client_order_id.encode("utf-8")).hexdigest()
    room = TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(prefix) - len(tail)
    if room < 1:
        # Keep the tail: it carries the idempotency guarantee.
        tail = tail[: TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(prefix) - 1]
        room = TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(prefix) - len(tail)
        if room < 1:
            raise ValueError(
                "broker prefix leaves no room for a Tokocrypto client order id"
            )
    return f"{prefix}{digest[:room]}{tail}"

def venue_client_order_id(
    exchange: Any, client_order_id: Optional[str], broker_prefix: str = ""
) -> Optional[str]:
    """Adapts a logical client id to the exact form its venue accepts.

    Tokocrypto (Binance Cloud) rejects ids that are longer than 32 characters
    or contain anything but alphanumerics, so those ids are rewritten instead
    of being sent unchanged and bouncing with error 3703. Every other venue
    keeps the id verbatim: a broker prefix is prepended only when the account
    actually has one registered, because a fabricated prefix is rejected even
    more reliably than a malformed id.
    """
    if not client_order_id:
        return client_order_id
    exchange_id = str(getattr(exchange, "id", "") or "").lower()
    if exchange_id != "tokocrypto":
        return client_order_id
    already_charset_safe = all(c.isalnum() for c in client_order_id)
    if already_charset_safe and len(client_order_id) <= TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN:
        return client_order_id
    if broker_prefix and client_order_id.startswith(broker_prefix):
        stripped = client_order_id[len(broker_prefix):]
        if all(c.isalnum() for c in stripped) and len(stripped) <= (
            TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(broker_prefix)
        ):
            return client_order_id
    return _tokocrypto_client_order_id(client_order_id, broker_prefix)


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
    """Expose venue-specific client-id fields through one canonical alias."""
    if not isinstance(order, dict) or order.get("client_order_id") is not None:
        return order
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    alias = next(
        (
            value for value in (
                order.get("clientOrderId"), order.get("clientId"),
                order.get("clOrdId"), order.get("orderLinkId"),
                order.get("origClientOrderId"), order.get("clientOid"),
                info.get("clientOrderId"), info.get("clientId"),
                info.get("clOrdId"), info.get("orderLinkId"),
                info.get("origClientOrderId"), info.get("clientOid"),
                info.get("newClientOrderId"),
            )
            if value not in (None, "")
        ),
        None,
    )
    if alias is None:
        return order
    normalized = dict(order)
    normalized["client_order_id"] = str(alias)
    return normalized

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



def _accepted_client_order_ids(
    exchange: Any, client_order_id: str, broker_prefix: str = ""
) -> set:
    """Every id spelling that may legitimately appear on the venue.

    Tokocrypto rewrites the logical id (see ``venue_client_order_id``), so a
    venue response carries the rewritten form rather than the logical one.
    Accepting both keeps recovery working without loosening the exact-match
    guarantee: only ids derived from this one logical id are admitted.
    """
    accepted = {client_order_id}
    exchange_id = str(getattr(exchange, "id", "") or "").lower()
    if exchange_id != "tokocrypto":
        return accepted
    accepted.add(venue_client_order_id(exchange, client_order_id, broker_prefix))
    accepted.add(_tokocrypto_client_order_id(client_order_id, broker_prefix))
    if broker_prefix:
        accepted.add(f"{broker_prefix}{client_order_id}")
    accepted.discard(None)
    return accepted

# OKX accounts in long/short (hedged) position mode require an explicit posSide
# on every contract order; omitting it is rejected by the venue. Accounts in net
# mode must NOT receive it. CCXT leaves the choice to the caller, so the side is
# declared here instead of relying on ccxt's ``hedged`` option.
_OKX_POSITION_SIDE = {"short": "short", "long": "long"}

def futures_position_side_params(exchange: Any, position_side: str) -> Dict[str, Any]:
    """Venue params that select which futures position an order acts on.

    Returns an empty mapping for venues that infer the position from the order
    side (net mode), so callers can merge unconditionally.
    """
    if str(getattr(exchange, "id", "") or "").lower() != "okx":
        return {}
    side = _OKX_POSITION_SIDE.get(str(position_side or "").lower())
    if side is None:
        raise ValueError(f"unsupported futures position side: {position_side}")
    return {"posSide": side}

async def _find_order_by_client_id(
    exchange: Any, symbol: str, client_order_id: str, broker_prefix: str = ""
) -> Optional[Dict[str, Any]]:
    """Find an acknowledged order by exact client id without submitting again.

    Prefer venue-native direct lookup, then open/terminal order history, and
    finally private trades. A failed history read is never proof of absence.
    """
    if not client_order_id:
        raise ValueError("client order id is required for acknowledgement recovery")
    accepted_ids = _accepted_client_order_ids(exchange, client_order_id, broker_prefix)

    def matches(order: Any) -> bool:
        if not isinstance(order, dict):
            return False
        info = order.get("info") if isinstance(order.get("info"), dict) else {}
        candidates = (
            order.get("clientOrderId"), order.get("client_order_id"),
            order.get("clientId"), order.get("clOrdId"), order.get("orderLinkId"),
            order.get("origClientOrderId"), order.get("clientOid"),
            info.get("clientOrderId"), info.get("client_order_id"),
            info.get("clientId"), info.get("clOrdId"), info.get("orderLinkId"),
            info.get("origClientOrderId"), info.get("clientOid"),
            info.get("newClientOrderId"),
        )
        return any(str(value) in accepted_ids for value in candidates if value)

    def unique_order(rows: list) -> Optional[Dict[str, Any]]:
        matched = {}
        for row in rows:
            if not matches(row):
                continue
            order_id = str(row.get("id") or row.get("orderId") or "")
            identity = order_id or repr(sorted(row.items(), key=lambda item: item[0]))
            matched[identity] = row
        if len(matched) > 1:
            raise RuntimeError(
                "multiple exchange orders share client order id "
                f"{client_order_id}; manual reconciliation is required"
            )
        if not matched:
            return None
        return _expose_client_order_id(dict(next(iter(matched.values()))))

    failures = []
    direct_lookup = getattr(exchange, "fetch_order_by_client_id", None)
    if callable(direct_lookup):
        try:
            direct = await direct_lookup(client_order_id, symbol)
            if direct is not None:
                if not isinstance(direct, dict) or not matches(direct):
                    raise ValueError(
                        "fetch_order_by_client_id returned a mismatched client id"
                    )
                return _expose_client_order_id(dict(direct))
        except Exception as exc:
            failures.append(f"fetch_order_by_client_id: {type(exc).__name__}: {exc}")

    historical_queries = 0
    discovered_orders = []
    for method_name in (
        "fetch_open_orders", "fetch_closed_orders", "fetch_canceled_orders"
    ):
        method = getattr(exchange, method_name, None)
        if not callable(method):
            continue
        try:
            if method_name == "fetch_open_orders":
                rows = await method(symbol)
            else:
                try:
                    rows = await method(symbol, None, 1000)
                except TypeError:
                    rows = await method(symbol)
            if rows is None or not isinstance(rows, (list, tuple)):
                raise ValueError(f"{method_name} returned a malformed order collection")
            if method_name != "fetch_open_orders":
                historical_queries += 1
            discovered_orders.extend(row for row in rows if matches(row))
        except Exception as exc:
            if type(exc).__name__ not in ("NotSupported", "NotImplementedError"):
                failures.append(f"{method_name}: {type(exc).__name__}: {exc}")

    found = unique_order(discovered_orders)
    if found is not None:
        return found

    trades_method = getattr(exchange, "fetch_my_trades", None)
    if callable(trades_method):
        try:
            try:
                trades = await trades_method(symbol, None, 1000)
            except TypeError:
                trades = await trades_method(symbol)
            if trades is None or not isinstance(trades, (list, tuple)):
                raise ValueError("fetch_my_trades returned a malformed trade collection")
            historical_queries += 1
            matching_trades = [trade for trade in trades if matches(trade)]
            if matching_trades:
                trade_order_ids = {
                    str(trade.get("order") or trade.get("orderId") or "")
                    for trade in matching_trades
                }
                trade_order_ids.discard("")
                if len(trade_order_ids) > 1:
                    raise RuntimeError(
                        "multiple exchange orders share client order id "
                        f"{client_order_id}; manual reconciliation is required"
                    )
                trade = matching_trades[0]
                info = trade.get("info") if isinstance(trade.get("info"), dict) else {}
                order_id = str(
                    trade.get("order") or trade.get("orderId") or info.get("orderId") or ""
                )
                if not order_id:
                    raise ValueError("matching private trade has no exchange order id")
                fetch_order = getattr(exchange, "fetch_order", None)
                if not callable(fetch_order):
                    raise NotImplementedError(
                        "venue cannot fetch the order referenced by its private trade"
                    )
                order = await fetch_order(order_id, symbol)
                if not isinstance(order, dict):
                    raise ValueError("fetch_order returned a malformed order payload")
                if not matches(order):
                    order = dict(order)
                    order.setdefault("clientOrderId", client_order_id)
                return _expose_client_order_id(order)
        except Exception as exc:
            if type(exc).__name__ not in ("NotSupported", "NotImplementedError"):
                failures.append(f"fetch_my_trades: {type(exc).__name__}: {exc}")

    if failures:
        raise RuntimeError(
            "client-order-id lookup was not authoritative: " + "; ".join(failures)
        )
    if historical_queries == 0:
        raise NotImplementedError(
            "venue exposes no supported historical endpoint for client-order-id recovery"
        )
    return None


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
