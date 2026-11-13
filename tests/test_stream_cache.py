"""P3-05 private-stream order cache: freshness, eviction, REST fallback."""

import asyncio
import time


import fakeredis.aioredis as fake_aioredis
import pytest

from tests.conftest import _Clock, _entry_order_info, _exposure_engine, engine, fake_redis

def test_p3_05_cache_serves_fresh_orders():
    from executor.private_stream import PrivateStreamCache

    clock = _Clock()
    cache = PrivateStreamCache(max_age_seconds=5.0, clock=clock)
    cache.update_order({"id": "1", "status": "closed", "filled": 0.5})

    assert cache.get_order("1")["filled"] == 0.5
    assert cache.stats["hits"] == 1

def test_p3_05_cache_rejects_stale_orders():
    from executor.private_stream import PrivateStreamCache

    clock = _Clock()
    cache = PrivateStreamCache(max_age_seconds=5.0, clock=clock)
    cache.update_order({"id": "1", "status": "open"})

    clock.now += 6.0
    assert cache.get_order("1") is None
    assert cache.stats["misses"] == 1

def test_p3_05_cache_ignores_orders_without_id():
    from executor.private_stream import PrivateStreamCache

    cache = PrivateStreamCache()
    cache.update_order({"status": "open"})
    assert cache.stats["orders"] == 0

def test_p3_05_cache_forget_order_frees_the_slot():
    from executor.private_stream import PrivateStreamCache

    cache = PrivateStreamCache()
    cache.update_order({"id": "9", "status": "closed"})
    cache.forget_order("9")
    assert cache.stats["orders"] == 0

@pytest.mark.asyncio
async def test_p3_05_fetch_prefers_cache_then_falls_back_to_rest():
    from executor.private_stream import PrivateStreamCache, fetch_order_preferring_stream

    clock = _Clock()
    cache = PrivateStreamCache(max_age_seconds=5.0, clock=clock)
    rest_calls = []

    async def rest():
        rest_calls.append(1)
        return {"id": "7", "status": "open", "filled": 0.0}

    # Empty cache -> REST, and the result is remembered.
    order = await fetch_order_preferring_stream(cache, rest, "7")
    assert order["status"] == "open"
    assert len(rest_calls) == 1

    # Second check hits the cache instead of the venue.
    order = await fetch_order_preferring_stream(cache, rest, "7")
    assert order["status"] == "open"
    assert len(rest_calls) == 1

    # Once stale, REST is used again rather than trusting an old fill.
    clock.now += 10.0
    await fetch_order_preferring_stream(cache, rest, "7")
    assert len(rest_calls) == 2

@pytest.mark.asyncio
async def test_p3_05_disabled_by_default_keeps_rest_polling(fake_redis):
    engine, _ = _exposure_engine(fake_redis, cap=0.0)
    assert engine.private_stream is None

@pytest.mark.asyncio
async def test_p3_05_enabled_engine_uses_stream_cache(fake_redis):
    engine, client = _exposure_engine(fake_redis, cap=0.0)
    engine.config.private_stream_enabled = True
    from executor.private_stream import PrivateStreamCache

    engine.private_stream = PrivateStreamCache(max_age_seconds=30.0)

    engine.pending_entry_orders["indodax:btc_idr"] = _entry_order_info(
        "indodax:btc_idr", int(time.time() * 1000)
    )
    info = engine.pending_entry_orders["indodax:btc_idr"]
    # The feed reports the fill before REST would have noticed it.
    engine.private_stream.update_order(
        {"id": info["order_id"], "status": "closed", "filled": info["qty"], "price": info["price"]}
    )

    calls = []
    original = client.fetch_local_order

    async def counting_fetch(order_id, symbol):
        calls.append(order_id)
        return await original(order_id, symbol)

    client.fetch_local_order = counting_fetch
    await engine._process_pending_entry("indodax:btc_idr", info)

    assert calls == [], "fresh stream data must avoid a REST poll"



@pytest.mark.asyncio
async def test_p3_05_private_order_worker_normalizes_ccxt_order():
    from executor.private_stream import PrivateStreamCache, PrivateOrderStreamWorker

    cache = PrivateStreamCache(max_age_seconds=5.0)

    class FakePrivateClient:
        def __init__(self):
            self.calls = 0
            self.ready = asyncio.Event()

        async def watch_orders(self):
            self.calls += 1
            self.ready.set()
            if self.calls == 1:
                return [{
                    "id": "42",
                    "status": "closed",
                    "filled": 1.25,
                    "info": {"orderId": "42"},
                }]
            await asyncio.Future()

    client = FakePrivateClient()
    worker = PrivateOrderStreamWorker("tokocrypto", client, cache)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(client.ready.wait(), timeout=1.0)
        for _ in range(10):
            if cache.get_order("42") is not None:
                break
            await asyncio.sleep(0.01)
        cached = cache.get_order("42")
        assert cached is not None
        assert cached["status"] == "closed"
        assert cached["filled"] == pytest.approx(1.25)
        assert worker.connected is True
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_p3_05_private_order_worker_keeps_rest_fallback_without_watch_orders():
    from executor.private_stream import PrivateStreamCache, PrivateOrderStreamWorker

    class RestOnlyClient:
        pass

    worker = PrivateOrderStreamWorker(
        "indodax", RestOnlyClient(), PrivateStreamCache()
    )
    await worker.run()
    assert worker.connected is False
    assert "CCXT Pro required" in worker.last_error
