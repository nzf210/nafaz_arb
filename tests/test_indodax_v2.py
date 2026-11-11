"""Indodax Trade API 2.0 adapter: signing, symbol/order mapping, order lifecycle."""

import hashlib
import hmac

import pytest

from executor.indodax_v2 import IndodaxTAPIv2, _fmt, normalize_symbol
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
    assert call["headers"]["X-APIKEY"] == "K"
    assert call["headers"]["Sign"] == ex.sign(body)
    assert order["status"] == "open"

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
