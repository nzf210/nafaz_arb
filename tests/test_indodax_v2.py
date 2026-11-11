"""Indodax Trade API 2.0 adapter: signing, symbol/order mapping, order lifecycle."""

import hashlib
import hmac

import pytest

from executor.indodax_v2 import IndodaxAPIError, IndodaxTAPIv2, _fmt, normalize_symbol
from executor.exchange_client import ExchangeClient

def _adapter():
    return IndodaxTAPIv2("TEST-KEY", "TEST-SECRET")

def test_symbol_normalization_matches_v2_form():
    assert normalize_symbol("btc_idr") == "BTCIDR"
    assert normalize_symbol("usdt/idr") == "USDTIDR"
    assert normalize_symbol("BTCIDR") == "BTCIDR"

def test_signature_is_hmac_sha256_of_query_string():
    ex = _adapter()
    payload = "symbol=BTCIDR&side=BUY&timestamp=1578304294000&recvWindow=5000"
    expected = hmac.new(b"TEST-SECRET", payload.encode(), hashlib.sha256).hexdigest()
    assert ex.sign(payload) == expected

def test_fmt_never_emits_exponent_or_float_noise():
    assert _fmt(0.00012300000000000001) == "0.000123"
    assert _fmt(1e-07) == "0.0000001"
    assert _fmt(1000.0) == "1000"
    assert _fmt(500000000) == "500000000"

def test_order_mapping_translates_status_and_quantities():
    mapped = IndodaxTAPIv2._order(
        {
            "symbol": "BTCIDR",
            "orderId": 6423,
            "clientOrderId": "btcidr-buy-1",
            "side": "SELL",
            "type": "LIMIT",
            "price": "421004000",
            "origQty": "4988.0",
            "executedQty": "1000.0",
            "status": "PARTIALLY_FILLED",
            "time": 1723442692520,
        },
        "btc_idr",
    )
    assert mapped["id"] == "6423"
    assert mapped["symbol"] == "btc_idr"
    assert mapped["side"] == "sell"
    assert mapped["status"] == "open"  # partially filled must stay pollable
    assert mapped["amount"] == 4988.0
    assert mapped["filled"] == 1000.0
    assert mapped["remaining"] == 3988.0

@pytest.mark.parametrize(
    "venue_status,expected",
    [("NEW", "open"), ("FILLED", "closed"), ("CANCELLED", "canceled"), ("REJECTED", "rejected")],
)
def test_order_status_map(venue_status, expected):
    mapped = IndodaxTAPIv2._order({"orderId": 1, "status": venue_status}, "btcidr")
    assert mapped["status"] == expected

class _StubSession:
    """Minimal aiohttp session double recording the request it received."""

    def __init__(self, payload):
        self.payload = payload
        self.calls = []
        self.closed = False

    def request(self, method, url, data=None, headers=None):
        self.calls.append({"method": method, "url": url, "data": data, "headers": headers})
        return _StubResponse(self.payload)

class _StubResponse:
    def __init__(self, payload):
        self.payload = payload
        self.status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        import json

        return json.dumps(self.payload)

@pytest.mark.asyncio
async def test_create_limit_buy_posts_signed_body_with_client_order_id():
    session = _StubSession(
        {
            "symbol": "BTCIDR",
            "orderId": 9,
            "clientOrderId": "btcidr-entry-1",
            "side": "BUY",
            "type": "LIMIT",
            "price": "1000000",
            "origQty": "0.01",
            "executedQty": "0",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())
    order = await client.place_local_limit_buy(
        "btc_idr", 0.01, 1_000_000.0,
        client_order_id="btcidr-entry-1",
        amount_spec={"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
    )

    call = session.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "https://api.indodax.com/api/v2/order"
    body = call["data"]
    assert "symbol=BTCIDR" in body
    assert "side=BUY" in body
    assert "type=LIMIT" in body
    assert "quantity=0.01" in body
    assert "price=1000000" in body
    assert "newClientOrderId=btcidr-entry-1" in body
    assert "timeInForce=MOC" in body
    assert call["headers"]["X-APIKEY"] == "K"
    assert call["headers"]["Sign"] == ex.sign(body)
    assert order["status"] == "open"


@pytest.mark.asyncio
async def test_indodax_limit_sell_uses_post_only_moc():
    session = _StubSession(
        {
            "symbol": "BTCIDR", "orderId": 13, "side": "SELL",
            "type": "LIMIT", "price": "1100000",
            "origQty": "0.01", "executedQty": "0",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())

    await client.place_local_limit_sell(
        "btc_idr", 0.01, 1_100_000.0,
        client_order_id="btcidr-exit-1",
        amount_spec={"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
    )

    body = session.calls[0]["data"]
    assert "side=SELL" in body
    assert "timeInForce=MOC" in body


@pytest.mark.asyncio
async def test_indodax_moc_rejection_does_not_retry_as_gtc():
    session = _StubSession({"code": -1013, "msg": "post-only order would take"})
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())

    with pytest.raises(IndodaxAPIError, match="post-only order would take"):
        await client.place_local_limit_buy(
            "btc_idr", 0.01, 1_000_000.0,
            client_order_id="btcidr-entry-rejected",
            amount_spec={"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
        )

    assert len(session.calls) == 1
    assert "timeInForce=MOC" in session.calls[0]["data"]
    assert "timeInForce=GTC" not in session.calls[0]["data"]


@pytest.mark.asyncio
async def test_indodax_market_sell_does_not_send_limit_time_in_force():
    session = _StubSession(
        {
            "symbol": "BTCIDR", "orderId": 14, "side": "SELL",
            "type": "MARKET", "origQty": "0.01", "executedQty": "0.01",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())

    await client.place_local_market_sell(
        "btc_idr", 0.01,
        amount_spec={"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
    )

    assert "timeInForce" not in session.calls[0]["data"]


@pytest.mark.asyncio
async def test_indodax_v2_fetch_order_book_uses_unsigned_public_depth_endpoint(monkeypatch):
    adapter = _adapter()
    captured = {}

    async def fake_request(method, path, params=None, signed=True):
        captured.update({"method": method, "path": path, "params": params, "signed": signed})
        return {
            "timestamp": 1723442692520,
            "bids": [["100", "2"], ["101", "1"]],
            "asks": [["103", "3"], ["102", "4"]],
        }

    monkeypatch.setattr(adapter, "_request", fake_request)
    book = await adapter.fetch_order_book("btc_idr", limit=20)

    assert captured == {
        "method": "GET", "path": "/api/v2/depth",
        "params": {"symbol": "BTCIDR", "limit": 20}, "signed": False,
    }
    assert book["bids"] == [[101.0, 1.0], [100.0, 2.0]]
    assert book["asks"] == [[102.0, 4.0], [103.0, 3.0]]
    assert book["timestamp"] == 1723442692520


@pytest.mark.asyncio
async def test_exchange_client_exposes_indodax_local_order_book():
    adapter = _adapter()
    async def fake_book(symbol, limit=None):
        return {"symbol": symbol, "bids": [[100.0, 1.0]], "asks": [[101.0, 1.0]]}
    adapter.fetch_order_book = fake_book
    client = ExchangeClient(adapter, object())

    book = await client.fetch_local_order_book("btc_idr", limit=5)

    assert book["bids"][0][0] == 100.0
    assert book["asks"][0][0] == 101.0


@pytest.mark.asyncio
async def test_indodax_order_quantity_is_floored_to_market_increment():
    session = _StubSession(
        {
            "symbol": "BTCIDR",
            "orderId": 10,
            "side": "BUY",
            "type": "LIMIT",
            "price": "1000000",
            "origQty": "0.12345678",
            "executedQty": "0",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())

    await client.place_local_limit_buy(
        "btc_idr", 0.123456789012, 1_000_000.0,
        amount_spec={"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
    )

    assert "quantity=0.12345678" in session.calls[0]["data"]

@pytest.mark.asyncio
async def test_indodax_order_respects_whole_unit_increment():
    session = _StubSession(
        {
            "symbol": "BTCIDR",
            "orderId": 11,
            "side": "BUY",
            "type": "LIMIT",
            "price": "1000000",
            "origQty": "1",
            "executedQty": "0",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())

    await client.place_local_limit_buy(
        "btc_idr", 1.999999, 1_000_000.0,
        amount_spec={"unit": "BASE", "step": 1.0, "min": 1.0, "base_per_unit": 1.0},
    )

    assert "quantity=1" in session.calls[0]["data"]

@pytest.mark.asyncio
async def test_indodax_order_requires_market_increment():
    ex = IndodaxTAPIv2("K", "S", session=_StubSession({}))
    client = ExchangeClient(ex, object())

    with pytest.raises(ValueError, match="market amount rules"):
        await client.place_local_limit_buy("btc_idr", 0.1, 1_000_000.0)

@pytest.mark.asyncio
async def test_indodax_emergency_market_sell_remains_available_for_legacy_positions():
    session = _StubSession(
        {
            "symbol": "BTCIDR",
            "orderId": 12,
            "side": "SELL",
            "type": "MARKET",
            "origQty": "0.1",
            "executedQty": "0.1",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    client = ExchangeClient(ex, object())

    await client.place_local_market_sell(
        "btc_idr", 0.1,
        amount_spec={"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
    )

    assert "quantity=0.1" in session.calls[0]["data"]

@pytest.mark.asyncio
async def test_cancel_without_status_field_is_treated_as_terminal():
    session = _StubSession(
        {
            "symbol": "USDTIDR",
            "orderId": 74748075,
            "origClientOrderId": "usdtidr-limit-74748075",
            "side": "SELL",
            "type": "LIMIT",
            "price": "18053",
            "origQty": "49.9332464",
            "executedQty": "0",
        }
    )
    ex = IndodaxTAPIv2("K", "S", session=session)
    cancelled = await ex.cancel_order("74748075", "usdt_idr")

    assert cancelled["status"] == "canceled"
    assert cancelled["filled"] == 0.0
    assert session.calls[0]["method"] == "DELETE"
    assert "orderId=74748075" in session.calls[0]["url"]

def test_adapter_requires_credentials():
    with pytest.raises(ValueError):
        IndodaxTAPIv2("", "")


@pytest.mark.asyncio
async def test_indodax_fetch_my_trades_maps_real_fill_fields(monkeypatch):
    adapter = _adapter()
    captured = {}

    async def fake_request(method, path, params=None, signed=True):
        captured.update({"method": method, "path": path, "params": params, "signed": signed})
        return {
            "data": [{
                "tradeId": "trade-1",
                "orderId": "order-1",
                "clientOrderId": "client-1",
                "symbol": "BTCIDR",
                "price": "100000",
                "qty": "0.01",
                "quoteQty": "1000",
                "commission": "2",
                "commissionAsset": "IDR",
                "isBuyer": True,
                "isMaker": False,
                "time": 1723442692520,
            }]
        }

    monkeypatch.setattr(adapter, "_request", fake_request)
    trades = await adapter.fetch_my_trades(
        "btc_idr", 1723442690000, 100,
        {"clientOrderId": "client-1", "endTime": 1723442700000},
    )

    assert captured["method"] == "GET"
    assert captured["path"] == "/api/v2/myTrades"
    assert captured["signed"] is True
    assert captured["params"]["clientOrderId"] == "client-1"
    assert trades[0]["id"] == "trade-1"
    assert trades[0]["order"] == "order-1"
    assert trades[0]["amount"] == pytest.approx(0.01)
    assert trades[0]["cost"] == pytest.approx(1000)
    assert trades[0]["fee"] == {"cost": 2.0, "currency": "IDR"}
    assert trades[0]["timestamp"] == 1723442692520


@pytest.mark.asyncio
async def test_exchange_client_fetch_order_trades_filters_by_client_order_id(monkeypatch):
    adapter = _adapter()

    async def fake_my_trades(symbol, since=None, limit=None, params=None):
        return [{
            "id": "trade-2", "order": "order-2", "clientOrderId": "client-2",
            "amount": 0.5, "price": 10, "cost": 5, "timestamp": 1723442692520,
            "fee": {"cost": 0.01, "currency": "BTC"},
        }]

    monkeypatch.setattr(adapter, "fetch_my_trades", fake_my_trades)
    client = ExchangeClient(adapter, object())
    fills = await client.fetch_order_trades(
        "spot", "btc_idr", client_order_id="client-2", order_id="numeric-order",
    )
    assert len(fills) == 1
    assert fills[0]["id"] == "trade-2"
    assert fills[0]["amount"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_exchange_client_converts_contract_fill_quantity_to_base_units():
    class FuturesVenue:
        id = "okx"

        async def fetch_my_trades(self, symbol, since=None, limit=None):
            return [{
                "id": "futures-trade-1",
                "order": "futures-order-1",
                "symbol": symbol,
                "amount": 2,
                "price": 100,
                "cost": 200,
                "timestamp": 1723442692520,
                "fee": {"cost": 0.02, "currency": "USDT"},
            }]

    client = ExchangeClient(_adapter(), FuturesVenue())
    fills = await client.fetch_order_trades(
        "perpetual",
        "BTC/USDT:USDT",
        order_id="futures-order-1",
        amount_spec={
            "unit": "CONTRACT",
            "step": 1,
            "min": 1,
            "base_per_unit": 0.01,
        },
    )

    assert fills[0]["amount"] == pytest.approx(0.02)
    assert fills[0]["cost"] == pytest.approx(2)


@pytest.mark.asyncio
async def test_indodax_direct_client_id_lookup_uses_orig_client_order_id(monkeypatch):
    adapter = _adapter()
    captured = {}

    async def fake_request(method, path, params=None, signed=True):
        captured.update({"method": method, "path": path, "params": params})
        return {
            "symbol": "BTCIDR", "orderId": 902, "clientOrderId": "recover-me",
            "side": "BUY", "type": "LIMIT", "origQty": "0.02",
            "executedQty": "0.02", "status": "FILLED", "time": 1723442692520,
        }

    monkeypatch.setattr(adapter, "_request", fake_request)
    order = await adapter.fetch_order_by_client_id("recover-me", "btc_idr")

    assert captured["method"] == "GET"
    assert captured["path"] == "/api/v2/order"
    assert captured["params"] == {"symbol": "BTCIDR", "origClientOrderId": "recover-me"}
    assert order["id"] == "902"
    assert order["clientOrderId"] == "recover-me"
    assert order["status"] == "closed"


@pytest.mark.asyncio
async def test_indodax_terminal_history_maps_filled_and_canceled_orders(monkeypatch):
    adapter = _adapter()
    captured = []

    async def fake_request(method, path, params=None, signed=True):
        captured.append((method, path, params))
        return {"data": [
            {"symbol": "BTCIDR", "orderId": 11, "clientOrderId": "filled-1",
             "side": "BUY", "type": "LIMIT", "origQty": "1", "executedQty": "1",
             "status": "FILLED", "time": 1723442692520},
            {"symbol": "BTCIDR", "orderId": 12, "clientOrderId": "cancel-1",
             "side": "BUY", "type": "LIMIT", "origQty": "2", "executedQty": "0.5",
             "status": "CANCELLED", "time": 1723442692521},
        ]}

    monkeypatch.setattr(adapter, "_request", fake_request)
    closed = await adapter.fetch_closed_orders("btc_idr")
    canceled = await adapter.fetch_canceled_orders("btc_idr")

    assert closed[0]["clientOrderId"] == "filled-1"
    assert closed[0]["status"] == "closed"
    assert canceled[0]["clientOrderId"] == "cancel-1"
    assert canceled[0]["status"] == "canceled"
    assert all(call[1] == "/api/v2/order/histories" for call in captured)
    assert all(call[2]["symbol"] == "BTCIDR" for call in captured)
    assert all(call[2]["endTime"] >= call[2]["startTime"] for call in captured)


@pytest.mark.asyncio
async def test_indodax_client_id_recovery_prefers_direct_order_lookup(monkeypatch):
    adapter = _adapter()

    async def fake_lookup(client_order_id, symbol):
        return {
            "id": "77", "clientOrderId": client_order_id, "symbol": symbol,
            "side": "buy", "type": "limit", "amount": 0.1,
            "filled": 0.1, "remaining": 0.0, "status": "closed",
        }

    async def forbidden_history(*args, **kwargs):
        raise AssertionError("direct lookup should avoid broad history queries")

    monkeypatch.setattr(adapter, "fetch_order_by_client_id", fake_lookup)
    monkeypatch.setattr(adapter, "fetch_open_orders", forbidden_history)
    monkeypatch.setattr(adapter, "fetch_closed_orders", forbidden_history)
    monkeypatch.setattr(adapter, "fetch_canceled_orders", forbidden_history)

    client = ExchangeClient(adapter, object())
    recovered = await client.fetch_local_order_by_client_id("recover-77", "btc_idr")
    assert recovered["id"] == "77"
    assert recovered["client_order_id"] == "recover-77"
