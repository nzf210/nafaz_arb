"""Regression tests for the durable order journal being authoritative in LIVE mode."""
import pytest

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

    with pytest.raises(RuntimeError, match="durable Supabase outbox"):
        await registry._persist(_record())

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
