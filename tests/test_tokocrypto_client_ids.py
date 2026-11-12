"""Offline contract tests for the real CCXT Tokocrypto adapter."""

import pytest


@pytest.mark.asyncio
async def test_tokocrypto_ccxt_request_uses_registered_prefix_and_spot_order_fields():
    import ccxt.async_support as ccxt

    from executor.exchange_client import ExchangeClient
    from executor.quantity import AmountSpec

    broker_id = "x-TKT5PX2F"
    logical_id = "tokocrypto-toko-floki-idr-entry-1700000001"
    venue = ccxt.tokocrypto({
        "apiKey": "test-key",
        "secret": "test-secret",
        "options": {"defaultType": "spot", "broker_id": broker_id},
    })
    market = {
        "id": "FLOKI_IDR",
        "symbol": "FLOKI/IDR",
        "base": "FLOKI",
        "quote": "IDR",
        "baseId": "FLOKI",
        "quoteId": "IDR",
        "spot": True,
        "active": True,
        "precision": {"amount": 3, "price": 2},
        "limits": {
            "amount": {"min": 0.001, "max": None},
            "price": {"min": 0.01, "max": None},
            "cost": {"min": None, "max": None},
        },
        "info": {"orderTypes": ["LIMIT", "MARKET", "LIMIT_MAKER"]},
    }
    venue.precisionMode = ccxt.DECIMAL_PLACES
    venue.markets = {"FLOKI/IDR": market}
    venue.markets_by_id = {"FLOKI_IDR": [market]}
    captured = {}

    async def submit_order(request):
        captured.update(request)
        return {
            "data": {
                "orderId": 12345,
                "clientId": request["clientId"],
                "symbol": "FLOKI_IDR",
                "side": 0,
                "type": 7,
                "price": "1000",
                "origQty": "1",
                "executedQty": "0",
                "executedPrice": "0",
                "executedQuoteQty": "0",
                "timeInForce": 1,
                "status": 0,
                "createTime": 1700000000000,
            }
        }

    venue.privatePostOpenV1Orders = submit_order
    client = ExchangeClient(venue, object())
    try:
        order = await client.place_local_limit_buy(
            "FLOKI/IDR",
            1.0,
            1000.0,
            client_order_id=logical_id,
            amount_spec=AmountSpec(
                unit="BASE", step=0.001, minimum=0.001, base_per_unit=1.0
            ),
        )
        assert captured["symbol"] == "FLOKI_IDR"
        assert captured["side"] == 0
        assert captured["type"] == "7"
        assert captured["quantity"] == "1"
        assert captured["price"] == "1000"
        assert "timeInForce" not in captured
        assert captured["clientId"].startswith(broker_id)
        assert len(captured["clientId"]) <= 32
        assert captured["clientId"][len(broker_id):].isalnum()
        assert order["id"] == "12345"
        assert order["postOnly"] is True

        captured.clear()

        async def submit_sell_order(request):
            captured.update(request)
            return {
                "data": {
                    "orderId": 12346,
                    "clientId": request["clientId"],
                    "symbol": "FLOKI_IDR",
                    "side": 1,
                    "type": 7,
                    "price": "1000",
                    "origQty": "1",
                    "executedQty": "0",
                    "executedPrice": "0",
                    "executedQuoteQty": "0",
                    "timeInForce": 1,
                    "status": 0,
                    "createTime": 1700000000000,
                }
            }

        venue.privatePostOpenV1Orders = submit_sell_order
        sell = await client.place_local_limit_sell(
            "FLOKI/IDR",
            1.0,
            1000.0,
            client_order_id=logical_id.replace("entry", "exit"),
            amount_spec=AmountSpec(
                unit="BASE", step=0.001, minimum=0.001, base_per_unit=1.0
            ),
        )
        assert captured["side"] == 1
        assert captured["type"] == "7"
        assert "timeInForce" not in captured
        assert captured["clientId"].startswith(broker_id)
        assert sell["postOnly"] is True
    finally:
        await venue.close()


@pytest.mark.asyncio
async def test_tokocrypto_open_order_discovery_requires_symbol_and_uses_max_page():
    from executor.exchange_client import ExchangeClient

    class FakeTokocrypto:
        id = "tokocrypto"
        options = {"broker_id": "x-TKT5PX2F"}
        markets = {
            "BTC/IDR": {
                "id": "BTC_IDR",
                "symbol": "BTC/IDR",
                "base": "BTC",
                "quote": "IDR",
                "active": True,
                "spot": True,
            }
        }

        def __init__(self):
            self.request = None

        async def load_markets(self):
            return self.markets

        async def fetch_open_orders(self, symbol=None, since=None, limit=None):
            self.request = (symbol, since, limit)
            return []

    venue = FakeTokocrypto()
    client = ExchangeClient(venue, object())

    with pytest.raises(ValueError, match="requires a symbol"):
        await client.fetch_local_open_orders()

    assert await client.fetch_local_open_orders("BTC/IDR") == []
    assert venue.request == ("BTC/IDR", None, 1000)


@pytest.mark.asyncio
async def test_tokocrypto_fetch_order_uses_documented_order_detail_endpoint():
    from executor.tokocrypto_adapter import TokocryptoAdapter

    venue = TokocryptoAdapter({
        "apiKey": "test-key",
        "secret": "test-secret",
        "options": {"defaultType": "spot"},
    })
    market = {
        "id": "FLOKI_IDR",
        "symbol": "FLOKI/IDR",
        "base": "FLOKI",
        "quote": "IDR",
        "baseId": "FLOKI",
        "quoteId": "IDR",
        "spot": True,
        "active": True,
        "precision": {"amount": 3, "price": 2},
        "limits": {"amount": {"min": 0.001}, "price": {"min": 0.01}},
        "info": {"orderTypes": ["LIMIT", "LIMIT_MAKER", "MARKET"]},
    }
    venue.markets = {"FLOKI/IDR": market}
    venue.markets_by_id = {"FLOKI_IDR": [market]}
    captured = {}

    async def query_order_detail(request):
        captured.update(request)
        return {
            "code": 0,
            "message": "success",
            "data": {
                "orderId": "12345",
                "clientId": "x-TKT5PX2Fabc123",
                "symbol": "FLOKI_IDR",
                "side": 0,
                "type": 7,
                "price": "1000",
                "origQty": "1",
                "executedQty": "0.25",
                "executedPrice": "1000",
                "executedQuoteQty": "250",
                "timeInForce": 1,
                "status": 1,
                "createTime": 1700000000000,
            },
        }

    venue.privateGetOpenV1OrdersDetail = query_order_detail
    try:
        order = await venue.fetch_order("12345", "FLOKI/IDR")
        assert captured == {"orderId": "12345"}
        assert order["id"] == "12345"
        assert order["symbol"] == "FLOKI/IDR"
        assert order["filled"] == 0.25
        assert order["status"] == "open"
    finally:
        await venue.close()


@pytest.mark.asyncio
async def test_tokocrypto_fetch_order_rejects_malformed_detail_response():
    from executor.tokocrypto_adapter import TokocryptoAdapter

    venue = TokocryptoAdapter({"apiKey": "test-key", "secret": "test-secret"})
    async def malformed(_request):
        return {"code": 0, "data": {"status": 0}}

    venue.privateGetOpenV1OrdersDetail = malformed
    try:
        with pytest.raises(ValueError, match="valid order id"):
            await venue.fetch_order("12345")
    finally:
        await venue.close()
