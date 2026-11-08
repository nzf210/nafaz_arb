"""Cross-signal, cross-worker exposure reservation regression tests."""

import asyncio
import json
import time

import pytest

from executor.models import ActivePosition
from tests.conftest import _exposure_engine, _half_budget_signal


@pytest.mark.asyncio
async def test_redis_pending_reservation_counts_against_shared_exchange_cap(fake_redis):
    engine, client = _exposure_engine(fake_redis, cap=60.0)
    now_ms = int(time.time() * 1000)

    # Simulate a resting order created by another executor process.
    await fake_redis.set(
        "arb:pending_entry:indodax:btc_idr",
        json.dumps({
            "order_id": "remote-order-1",
            "symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "local_exchange": "indodax",
            "global_exchange": "okx",
            "qty": 0.005,
            "filled": 0.0,
            "hedged_short_qty": 0.0,
            "spot_cost_usdt": 50.0,
            "futures_margin_usdt": 50.0,
        }),
    )

    sig = _half_budget_signal("eth_idr", "ETHUSDT", now_ms)
    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig))
    await engine.handle_signal_notification("indodax:eth_idr")

    assert "indodax:btc_idr" in engine.pending_entry_orders
    assert "indodax:eth_idr" not in engine.pending_entry_orders
    assert client.orders == {}


@pytest.mark.asyncio
async def test_concurrent_different_pairs_share_venue_entry_lock(fake_redis):
    engine, client = _exposure_engine(fake_redis, cap=60.0)
    now_ms = int(time.time() * 1000)

    for symbol in ("btc_idr", "eth_idr"):
        sig = _half_budget_signal(symbol, symbol.upper().replace("_IDR", "USDT"), now_ms)
        await fake_redis.set(f"arb:signal:indodax:{symbol}", json.dumps(sig))

    await asyncio.gather(
        engine.handle_signal_notification("indodax:btc_idr"),
        engine.handle_signal_notification("indodax:eth_idr"),
    )

    assert len(engine.pending_entry_orders) == 1
    assert len(client.orders) == 1


@pytest.mark.asyncio
async def test_live_entry_fails_closed_without_redis_distributed_lock(fake_redis):
    class RedisWithoutLocks:
        async def get(self, key):
            return await fake_redis.get(key)

        async def keys(self, pattern):
            return await fake_redis.keys(pattern)

    from executor.config import ExecutorConfig
    from executor.engine import ArbitrageEngine
    from tests.conftest import MockExchangeClient, _fresh_signal_payload

    pair_tag = "indodax:btc_idr"
    await fake_redis.set(
        f"arb:signal:{pair_tag}",
        json.dumps(_fresh_signal_payload(int(time.time() * 1000))),
    )
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False),
        client=MockExchangeClient(),
        redis_client=RedisWithoutLocks(),
    )

    await engine.handle_signal_notification(pair_tag)

    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag].value == "FAILED"
    assert engine._failure_reasons[pair_tag]["reason"] == "VENUE_ENTRY_LOCK_FAILED"
