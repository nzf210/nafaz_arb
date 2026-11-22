"""Regression tests for the durable order journal being authoritative in LIVE mode."""
from types import SimpleNamespace

import pytest

from executor.engine_order_intents import EngineOrderIntentMixin
from executor.models import OrderRecord
from executor.order_registry import UniversalOrderRegistry


class _Redis:
    async def set(self, *_args, **_kwargs):
        return True

    async def hset(self, *_args, **_kwargs):
        return 1


class _Writer:
    def __init__(self, enabled=True, result=True):
        self.enabled = enabled
        self.result = result
        self.rows = []

    def order_record(self, row):
        self.rows.append(row)
        return self.result


def _record():
    return OrderRecord(
        local_order_id="local-1",
        client_order_id="client-1",
        exchange="indodax",
        market_type="spot",
        symbol="btc_idr",
        side="buy",
        order_role="ENTRY",
        original_quantity=1.0,
        remaining_quantity=1.0,
        status="submitting",
        is_mock=False,
    )


@pytest.mark.asyncio
async def test_redis_success_does_not_mask_failed_durable_order_journal():
    writer = _Writer(enabled=True, result=False)
    registry = UniversalOrderRegistry(_Redis(), writer)
    record = _record()

    with pytest.raises(RuntimeError, match="durable Supabase outbox"):
        await registry._persist(record)

    assert len(writer.rows) == 1


@pytest.mark.asyncio
async def test_successful_durable_order_journal_allows_registration():
    writer = _Writer(enabled=True, result=True)
    registry = UniversalOrderRegistry(_Redis(), writer)

    await registry._persist(_record())

    assert len(writer.rows) == 1
    assert writer.rows[0]["client_order_id"] == "client-1"


@pytest.mark.asyncio
async def test_disabled_durable_writer_keeps_redis_only_mock_compatibility():
    writer = _Writer(enabled=False, result=False)
    registry = UniversalOrderRegistry(_Redis(), writer)

    await registry._persist(_record())

    assert writer.rows == []



class _IntentHarness(EngineOrderIntentMixin):
    def __init__(self, *, supabase_enabled: bool):
        self.config = SimpleNamespace(
            mock_mode=False,
            postgres_dsn="postgresql://global-stop-only",
        )
        self.global_stop = SimpleNamespace(enabled=True)
        self.supabase = SimpleNamespace(enabled=supabase_enabled)
        self.register_calls = 0

    async def _register_order(self, *args, **kwargs):
        self.register_calls += 1
        return _record()


@pytest.mark.asyncio
async def test_global_stop_database_does_not_substitute_for_durable_order_registry():
    """A STOP ledger alone cannot satisfy the separate LIVE order-registry requirement."""
    engine = _IntentHarness(supabase_enabled=False)

    with pytest.raises(RuntimeError, match="durable order registry/outbox is disabled"):
        await engine._register_order_intent(
            "indodax:btc_idr__okx:btc_usdt_swap",
            "live-entry-client-id",
            exchange="indodax",
            market_type="spot",
            symbol="btc_idr",
            role="ENTRY",
            side="buy",
            order_type="limit",
            price=100.0,
            quantity=1.0,
            required=True,
        )

    assert engine.register_calls == 0
