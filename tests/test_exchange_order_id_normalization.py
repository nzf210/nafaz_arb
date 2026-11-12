"""Regression tests for venue-native exchange order id extraction."""

from types import SimpleNamespace

import pytest

from executor.engine_order_intents import EngineOrderIntentMixin
from executor.exchange_common import (
    acknowledged_order_result,
    exchange_order_id_from,
)
from executor.supabase import SupabaseWriter


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"id": "ccxt-1", "info": {"orderId": "raw-1"}}, "ccxt-1"),
        ({"info": {"orderId": 123456}}, "123456"),
        ({"info": {"ordId": "okx-ord-1"}}, "okx-ord-1"),
        ({"info": {"data": [{"ordId": "okx-nested-1"}]}}, "okx-nested-1"),
        ({"data": [{"orderId": "toko-nested-1"}]}, "toko-nested-1"),
        ({"info": {"clientId": "client-only", "clientOrderId": "client-only-2"}}, ""),
    ],
)
def test_exchange_order_id_from_venue_payloads(payload, expected):
    assert exchange_order_id_from(payload) == expected


def test_acknowledgement_promotes_tokocrypto_order_id_to_canonical_id():
    result = acknowledged_order_result(
        SimpleNamespace(id="tokocrypto"),
        {"info": {"orderId": 987654, "clientId": "venue-client-1"}},
        None,
        client_order_id=None,
    )
    assert result["id"] == "987654"


def test_acknowledgement_promotes_okx_ord_id_to_canonical_id():
    result = acknowledged_order_result(
        SimpleNamespace(id="okx"),
        {"info": {"ordId": "OKX-123"}},
        None,
        client_order_id=None,
    )
    assert result["id"] == "OKX-123"


class _Registry:
    def __init__(self):
        self.record = None

    async def register(self, record):
        self.record = record
        return record


class _Engine(EngineOrderIntentMixin):
    def __init__(self):
        self.order_registry = _Registry()
        self.config = SimpleNamespace(mock_mode=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exchange", "order", "expected"),
    [
        ("tokocrypto", {"info": {"orderId": "T-100"}}, "T-100"),
        ("okx", {"info": {"ordId": "O-200"}}, "O-200"),
    ],
)
async def test_order_registry_persists_native_exchange_id(exchange, order, expected):
    engine = _Engine()
    order.update({"status": "open", "amount": 1.0, "filled": 0.0})
    record = await engine._register_order(
        "btc-usdt",
        "bot-client-1",
        order,
        exchange=exchange,
        market_type="spot",
        symbol="BTC/USDT",
        role="ENTRY",
        side="buy",
        order_type="limit",
        price=100.0,
        quantity=1.0,
    )
    assert record is not None
    assert record.exchange_order_id == expected


def test_supabase_order_analytics_persists_native_exchange_id():
    cfg = SimpleNamespace(
        enabled=True,
        url="https://example.supabase.co",
        key="sb_secret_test",
        outbox_path=":memory:",
    )
    writer = SupabaseWriter(cfg)
    try:
        assert writer.order(
            "btc-usdt",
            "spot_entry",
            "tokocrypto",
            "spot",
            "BTC/USDT",
            {"info": {"orderId": "T-300"}, "status": "open", "amount": 1.0},
            client_order_id="bot-client-2",
            is_mock=False,
        )
        pending = writer._outbox.pending(100)
        rows = [row for row in pending if row[1] == "orders"]
        assert rows
        assert rows[-1][3]["exchange_order_id"] == "T-300"
    finally:
        writer.close()
