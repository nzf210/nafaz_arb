"""Tests for the bounded mutation-intent recovery scheduler."""
from __future__ import annotations

import asyncio

import pytest

from executor.mutation_intent_recovery_scheduler import MutationIntentRecoveryScheduler


class _Worker:
    def __init__(self, results=None, error=None):
        self.results = results or []
        self.error = error
        self.limits = []
        self.calls = 0

    async def reconcile_batch(self, *, limit):
        self.calls += 1
        self.limits.append(limit)
        if self.error:
            raise self.error
        return self.results


@pytest.mark.asyncio
async def test_scheduler_uses_bounded_single_flight_batches_and_stops_cleanly():
    worker = _Worker(results=[{"outcome": "ACKNOWLEDGED"}])
    scheduler = MutationIntentRecoveryScheduler(
        worker, interval_seconds=0.01, max_interval_seconds=0.02, batch_limit=7,
    )
    task = asyncio.create_task(scheduler.run())
    for _ in range(20):
        if worker.calls >= 2:
            break
        await asyncio.sleep(0.005)
    await scheduler.stop()
    await asyncio.wait_for(task, timeout=0.2)

    assert worker.calls >= 1
    assert set(worker.limits) == {7}
    assert scheduler._running is False


@pytest.mark.asyncio
async def test_scheduler_propagates_cancellation_and_resets_running_state():
    worker = _Worker(results=[])
    scheduler = MutationIntentRecoveryScheduler(
        worker, interval_seconds=10, max_interval_seconds=20,
    )
    task = asyncio.create_task(scheduler.run())
    for _ in range(20):
        if worker.calls:
            break
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert scheduler._running is False


def test_scheduler_rejects_unbounded_or_invalid_configuration():
    worker = _Worker()
    with pytest.raises(ValueError):
        MutationIntentRecoveryScheduler(worker, interval_seconds=0)
    with pytest.raises(ValueError):
        MutationIntentRecoveryScheduler(worker, interval_seconds=10, max_interval_seconds=5)
    with pytest.raises(ValueError):
        MutationIntentRecoveryScheduler(worker, batch_limit=501)
