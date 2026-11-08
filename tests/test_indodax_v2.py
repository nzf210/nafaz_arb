"""Indodax Trade API 2.0 adapter: signing, symbol/order mapping, order lifecycle."""

import hashlib
import hmac

import pytest

from executor.indodax_v2 import IndodaxTAPIv2, _fmt, normalize_symbol

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
    order = await ex.create_limit_buy_order(
        "btc_idr", 0.01, 1_000_000.0, {"clientOrderId": "btcidr-entry-1"}
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
