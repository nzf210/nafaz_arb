"""Priority queue execution order tests."""

import asyncio
import pytest


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
