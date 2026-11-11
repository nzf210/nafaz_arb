"""Cross-instance safety tests for the distributed order-recovery lease."""

import asyncio

import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine

from tests.conftest import MockExchangeClient


def _engine(redis):
    return ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True),
        client=MockExchangeClient(),
        redis_client=redis,
    )


@pytest.mark.asyncio
async def test_only_one_executor_runs_recovery_at_a_time(fake_redis):
    first = _engine(fake_redis)
    second = _engine(fake_redis)
    entered = asyncio.Event()
    release = asyncio.Event()
    second_called = False

    async def hold_recovery():
        entered.set()
        await release.wait()

    async def second_recovery():
        nonlocal second_called
        second_called = True

    first._run_order_recovery_with_lock = hold_recovery
    second._run_order_recovery_with_lock = second_recovery

    first_task = asyncio.create_task(first._run_order_recovery())
    await asyncio.wait_for(entered.wait(), timeout=1)

    await second._run_order_recovery()

    assert second_called is False
    assert second._entry_blocked is True
    assert second._recovery_scope_halts["*"]

    release.set()
    await asyncio.wait_for(first_task, timeout=1)
    assert await fake_redis.exists("arb:executor:order-recovery:lock") == 0


@pytest.mark.asyncio
async def test_recovery_lease_is_released_when_reconciliation_raises(fake_redis):
    first = _engine(fake_redis)
    second = _engine(fake_redis)
    second_called = False

    async def failing_recovery():
        raise RuntimeError("simulated recovery failure")

    async def succeeding_recovery():
        nonlocal second_called
        second_called = True

    first._run_order_recovery_with_lock = failing_recovery
    second._run_order_recovery_with_lock = succeeding_recovery

    with pytest.raises(RuntimeError, match="simulated recovery failure"):
        await first._run_order_recovery()

    assert first._entry_blocked is True
    assert first._recovery_scope_halts["*"]

    await second._run_order_recovery()

    assert second_called is True
    assert await fake_redis.exists("arb:executor:order-recovery:lock") == 0


@pytest.mark.asyncio
async def test_executor_can_reacquire_lease_after_previous_owner_finishes(fake_redis):
    first = _engine(fake_redis)
    second = _engine(fake_redis)
    calls = []

    async def record_first():
        calls.append("first")

    async def record_second():
        calls.append("second")

    first._run_order_recovery_with_lock = record_first
    second._run_order_recovery_with_lock = record_second

    await first._run_order_recovery()
    await second._run_order_recovery()

    assert calls == ["first", "second"]
