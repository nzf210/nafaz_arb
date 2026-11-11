"""Priority queue execution order tests."""

import asyncio
import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_signal_priority_queue_processes_highest_profit_first(engine, fake_redis):
    engine._running = True
    handled = []

    async def record(pair_tag):
        handled.append(pair_tag)
        if len(handled) == 3:
            engine._running = False

    engine.handle_signal_notification = record
    await fake_redis.zadd(
        "arb:signal_priority",
        {
            "indodax:btc_idr__okx:btcusdt": 1.0,
            "indodax:eth_idr__okx:ethusdt": 4.0,
            "tokocrypto:sol_idr__mexc:solusdt": 2.5,
        },
    )
    engine._signal_queue_event.set()
    worker = asyncio.create_task(engine._process_signal_priority_queue())
    try:
        await asyncio.wait_for(worker, timeout=2.0)
    finally:
        engine._running = False
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)

    assert handled == [
        "indodax:eth_idr__okx:ethusdt",
        "tokocrypto:sol_idr__mexc:solusdt",
        "indodax:btc_idr__okx:btcusdt",
    ]



@pytest.mark.asyncio
async def test_signal_notification_loads_payload_once(engine):
    payload = object()
    engine._load_signal_payload = AsyncMock(return_value=payload)
    handled = []

    async def capture(pair_tag, received_payload=None):
        handled.append((pair_tag, received_payload))

    engine._handle_signal_notification_bound = capture

    await engine.handle_signal_notification("indodax:btc_idr__okx:btcusdt")

    engine._load_signal_payload.assert_awaited_once_with(
        "indodax:btc_idr__okx:btcusdt"
    )
    assert handled == [("indodax:btc_idr__okx:btcusdt", payload)]
