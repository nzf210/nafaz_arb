"""Shared venue order normalization and client-id helpers."""

import hashlib
import logging
from typing import Optional, Dict, Any

from executor.indodax_v2 import IndodaxTAPIv2
from executor.quantity import AmountSpec, order_amount_to_base

logger = logging.getLogger("executor.exchange")

# Tokocrypto (and Binance Cloud generally) exposes one order list for open and
# finished orders and no client-id filter, so recovery has to page it.
_TOKOCRYPTO_ORDER_ARCHIVE_SIZE = 100
_TOKOCRYPTO_ORDER_ARCHIVE_MAX_PAGES = 10



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

class ClientOrderIdAliasStore:
    """Remembers the wire id a venue actually accepted for a logical id.

    The rewrite above is not invertible: a venue echoes the hashed id and no
    amount of prefix stripping recovers the logical one. The durable order
    registry is the only place the logical id survives, so recovering a submit
    whose acknowledgement was lost means remembering the wire form while the
    order is still in memory.
    """

    def __init__(self) -> None:
        self._wire_by_scope: Dict[tuple, str] = {}

    def remember(
        self, exchange: Any, scope: str, logical_id: Optional[str],
        wire_id: Optional[str],
    ) -> None:
        """Records one accepted (logical, wire) pair for the wiring venue."""
        if not logical_id or not wire_id:
            return
        venue = str(getattr(exchange, "id", "") or "").strip().lower()
        scope_key = str(scope or "").strip().lower()
        if not venue or not scope_key:
            return
        self._wire_by_scope[(venue, scope_key, str(logical_id))] = str(wire_id)

    def wire_id(self, exchange: Any, scope: str, logical_id: str) -> Optional[str]:
        venue = str(getattr(exchange, "id", "") or "").strip().lower()
        return self._wire_by_scope.get(
            (venue, str(scope or "").strip().lower(), str(logical_id))
        )

# One store per process: an order is only ambiguous while this process holds it.
CLIENT_ORDER_ID_ALIASES = ClientOrderIdAliasStore()

def accepted_client_order_id(
    exchange: Any,
    order: Dict[str, Any],
    logical_id: Optional[str],
    broker_prefix: str = "",
    scope: str = "",
) -> Optional[str]:
    """Learns the exact client id a venue acknowledged, keyed on the order itself.

    A venue may report the id it minted instead of the one submitted (Binance
    Cloud drops a broker prefix, Tokocrypto echoes a hash), so the spelling a
    later lookup has to search for can only come from the acknowledgement.
    One row's own alias is never handed to another row: accepting a *different*
    id during recovery would hide a genuinely ambiguous submit.
    """
    if not isinstance(order, dict) or not isinstance(logical_id, str):
        return None
    venue = str(getattr(exchange, "id", "") or "").lower()
    wire = str(order.get("client_order_id") or order.get("clientOrderId") or "")
    if not venue or not logical_id or not wire or wire == logical_id:
        return None
    if wire not in _accepted_client_order_ids(
        exchange, logical_id, broker_prefix, scope
    ):
        return None
    return wire

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

def acknowledged_order_result(
    exchange: Any,
    order: Dict[str, Any],
    spec: Optional[AmountSpec],
    client_order_id: Optional[str] = None,
    broker_prefix: str = "",
    scope: str = "",
) -> Dict[str, Any]:
    """Normalizes a submit acknowledgement and learns its wire client id.

    Every create path returns through here so the exact id the venue accepted
    is recorded before the order id is even known; a later lookup by client id
    would otherwise search for a spelling the venue never stored.
    """
    normalized = _base_order_result(order, spec) if spec is not None else dict(order)
    # Order matters: the exposure helper prefers the venue's own alias, so the
    # accepted wire id is written over both after it has run.
    normalized = _expose_client_order_id(normalized)
    wire = _remember_accepted_client_order_id(
        exchange, order, client_order_id, broker_prefix, scope
    )
    if wire is not None:
        normalized["client_order_id"] = wire
        normalized["clientOrderId"] = wire
    return normalized

def _remember_accepted_client_order_id(
    exchange: Any,
    order: Dict[str, Any],
    logical_id: Optional[str],
    broker_prefix: str,
    scope: str,
) -> Optional[str]:
    """Records the wire id a venue acknowledged for a submitted logical id."""
    if not logical_id or not isinstance(order, dict):
        return None
    info = order.get("info") if isinstance(order.get("info"), dict) else {}
    # Tokocrypto answers with two ids: ``clientId`` (the prefixed, hashed id it
    # actually stores) and the raw Binance-shaped ``clientOrderId`` echo, which
    # is the *forwarded* id and therefore unbranded on a broker account. The
    # stored one wins because that is what every later read returns.
    wire = str(
        info.get("clientId") or info.get("clientOrderId")
        or order.get("clientId") or order.get("clientOrderId") or ""
    )
    if not wire:
        return None
    # Binance Cloud verifies the signature against the id it forwarded, and a
    # broker-registered account can see that id prefixed. Anything else is a
    # venue echo this process never caused, so it is not learned.
    expected = venue_client_order_id(exchange, logical_id, broker_prefix)
    allowed = {logical_id}
    if expected:
        allowed.add(str(expected))
        if broker_prefix:
            allowed.add(f"{broker_prefix}{expected}")
    if wire not in allowed:
        return None
    CLIENT_ORDER_ID_ALIASES.remember(exchange, scope, logical_id, wire)
    return wire

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
    exchange: Any, client_order_id: str, broker_prefix: str = "", scope: str = ""
) -> set:
    """Every id spelling that may legitimately appear on the venue.

    Tokocrypto rewrites the logical id (see ``venue_client_order_id``), so a
    venue response carries the rewritten form rather than the logical one.
    Accepting both keeps recovery working without loosening the exact-match
    guarantee: only ids derived from this one logical id are admitted.
    """
    accepted = {client_order_id}
    # An id the venue already acknowledged is authoritative for any venue, not
    # only the one whose rewrite rules this module knows.
    accepted.add(CLIENT_ORDER_ID_ALIASES.wire_id(exchange, scope, client_order_id))
    exchange_id = str(getattr(exchange, "id", "") or "").lower()
    if exchange_id == "tokocrypto":
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

def _order_archive_page_reader(exchange: Any, symbol: str) -> Any:
    """Recent-order reader for venues whose history ignores the client id filter.

    Tokocrypto's only order list is the Binance-Cloud ``/open/v1/orders``
    endpoint: it covers resting and finished orders alike and has no client-id
    filter, so a lost acknowledgement can only be recovered by walking it.
    """
    venue = getattr(exchange, "local", exchange)
    if str(getattr(venue, "id", "") or "").lower() != "tokocrypto":
        return None
    api = getattr(venue, "privateGetOpenV1Orders", None)
    if not callable(api):
        return None

    async def page(
        page_size: int = _TOKOCRYPTO_ORDER_ARCHIVE_SIZE, cursor: str = ""
    ) -> tuple:
        # ``fromId``/``direct`` page backwards from a known order id, so one
        # extra row repeats at the boundary; the pager de-duplicates by id.
        request = {"symbol": symbol, "limit": int(page_size)}
        if cursor:
            request["fromId"] = str(cursor)
            request["direct"] = "prev"
        response = await api(request)
        data = response.get("data") if isinstance(response, dict) else None
        rows = data.get("list") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise ValueError("tokocrypto returned a malformed order archive page")
        parsed = [
            _parse_archive_order(venue, row, symbol)
            for row in rows if isinstance(row, dict)
        ]
        return parsed, len(rows) < int(page_size)

    return page

def _parse_archive_order(venue: Any, row: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    """Maps one raw archive row onto the same shape the rest of the bot reads.

    The endpoint answers with the Binance-shaped payload (``orderId``,
    ``clientId``, numeric ``side``/``status``), so it is run through the venue's
    own parser when available rather than hand-mapped here.
    """
    parser = getattr(venue, "parse_order", None)
    if callable(parser):
        try:
            market = venue.market(symbol) if hasattr(venue, "market") else None
        except Exception:
            market = None
        try:
            return parser(row, market)
        except Exception:
            logger.debug("could not parse %s archive row; using it verbatim", venue)
    return dict(row)

async def _find_order_in_paged_archive(
    exchange: Any, symbol: str, matches: Any, client_order_id: str
) -> Optional[Dict[str, Any]]:
    """Searches a venue archive that its own API cannot filter by client id."""
    page_reader = _order_archive_page_reader(exchange, symbol)
    if page_reader is None:
        return None
    observed: dict = {}
    cursor = ""
    for _ in range(_TOKOCRYPTO_ORDER_ARCHIVE_MAX_PAGES):
        rows, exhausted = await page_reader(_TOKOCRYPTO_ORDER_ARCHIVE_SIZE, cursor)
        if not rows and not exhausted:
            return None
        for row in rows:
            identity = str(
                row.get("id") or row.get("orderId")
                or row.get("clientId") or row.get("clientOrderId") or ""
            )
            if identity:
                observed[identity] = row
        found = _unique_archive_match(list(observed.values()), matches, client_order_id)
        if found is not None:
            return _expose_client_order_id(found)
        if exhausted:
            return None
        # ``direct=prev`` is inclusive of the cursor it is given, so the next
        # page must start strictly before the last id already read.
        cursor = str(rows[-1].get("id") or rows[-1].get("orderId") or "") if rows else ""
        if not cursor:
            return None
    # The archive was longer than this reader may walk, so absence is not
    # established. Refusing to answer keeps the caller fail-closed.
    raise RuntimeError(
        f"{getattr(exchange, 'id', 'venue')} order archive exceeded "
        f"{_TOKOCRYPTO_ORDER_ARCHIVE_MAX_PAGES} pages without a match"
    )

def _unique_archive_match(
    rows: list, matches: Any, client_order_id: str
) -> Optional[Dict[str, Any]]:
    """One matching row, or a hard failure when the id is not unique."""
    matched = {}
    for row in rows:
        if not matches(row):
            continue
        identity = str(row.get("id") or row.get("orderId") or repr(row))
        matched[identity] = row
    if len(matched) > 1:
        raise RuntimeError(
            "multiple exchange orders share client order id "
            f"{client_order_id}; manual reconciliation is required"
        )
    if not matched:
        return None
    return dict(next(iter(matched.values())))

async def _find_order_by_client_id(
    exchange: Any,
    symbol: str,
    client_order_id: str,
    broker_prefix: str = "",
    scope: str = "",
) -> Optional[Dict[str, Any]]:
    """Find an acknowledged order by exact client id without submitting again.

    Prefer venue-native direct lookup, then open/terminal order history, and
    finally private trades. A failed history read is never proof of absence.
    """
    if not client_order_id:
        raise ValueError("client order id is required for acknowledgement recovery")
    accepted_ids = _accepted_client_order_ids(
        exchange, client_order_id, broker_prefix, scope
    )

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
            discovered_orders.extend([row for row in rows if isinstance(row, dict)])
        except Exception as exc:
            if type(exc).__name__ not in ("NotSupported", "NotImplementedError"):
                failures.append(f"{method_name}: {type(exc).__name__}: {exc}")

    found = unique_order(discovered_orders)
    if found is not None:
        return found

    try:
        archived = await _find_order_in_paged_archive(
            exchange, symbol, matches, client_order_id
        )
        historical_queries += 1
    except RuntimeError:
        raise
    except Exception as exc:
        archived = None
        failures.append(
            f"order archive: {type(exc).__name__}: {exc}"
        )
    if archived is not None:
        return archived

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
