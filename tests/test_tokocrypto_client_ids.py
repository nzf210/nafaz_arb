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
                "type": 1,
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
        assert captured["type"] == "1"
        assert captured["quantity"] == "1"
        assert captured["price"] == "1000"
        assert captured["timeInForce"] == "1"
        assert captured["clientId"].startswith(broker_id)
        assert len(captured["clientId"]) <= 32
        assert captured["clientId"][len(broker_id):].isalnum()
        assert order["id"] == "12345"
    finally:
        await venue.close()
