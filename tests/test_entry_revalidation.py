"""Entry signal freshness, spread revalidation, and IDR FX guards."""

import asyncio
import json
import re
import time

from executor.models import PositionState

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine

from tests.conftest import MockExchangeClient, _entry_order_info, _fresh_signal_payload, _revalidation_payload, engine, fake_redis, mock_client

@pytest.mark.asyncio
async def test_entry_revalidation_rejects_collapsed_spread(engine, fake_redis, mock_client):
    """Global bid drops after snapshot: live spread < floor, entry must be rejected."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    # Live bid falls to 64350: (64350 - 64935) / 64350 ≈ -0.91% < 0.10% floor.
    mock_client.update_market_price("BTC/USDT", 64350.0, 64360.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 0
    assert pair_tag not in engine.states

@pytest.mark.asyncio
async def test_entry_revalidation_passes_healthy_spread(engine, fake_redis, mock_client):
    """Global bid unchanged: live spread meets the floor, entry proceeds."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    mock_client.update_market_price("BTC/USDT", 65000.0, 65010.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 1
    assert engine.states[pair_tag] == PositionState.ENTRY_PENDING

@pytest.mark.asyncio
async def test_entry_revalidation_fails_open_on_ticker_error(engine, fake_redis, mock_client):
    """A ticker read failure must not block the entry the scanner already validated."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    async def boom(symbol):
        raise RuntimeError("ticker down")

    mock_client.fetch_global_ticker = boom

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 1

@pytest.mark.asyncio
async def test_entry_revalidation_disabled_by_zero_floor(engine, fake_redis, mock_client):
    """Floor 0 in config AND payload: guard off, no ticker re-quote consulted."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms, min_spread_pct=0.0)

    called = []
    original = mock_client.fetch_global_ticker

    async def spy(symbol):
        called.append(symbol)
        return await original(symbol)

    mock_client.fetch_global_ticker = spy

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    await engine.handle_signal_notification(pair_tag)

    assert called == []
    assert len(mock_client.placed_buys) == 1

@pytest.mark.asyncio
async def test_entry_revalidation_config_floor_overrides_payload(engine, fake_redis, mock_client):
    """An explicit config floor wins over the payload's min_entry_spread_pct."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms, min_spread_pct=0.0)  # payload floor off

    engine.config.entry_revalidation_min_spread_pct = 5.0  # config floor on
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    # Spread ≈ 0.1% < 5% config floor → rejected.
    mock_client.update_market_price("BTC/USDT", 65000.0, 65010.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 0


@pytest.mark.asyncio
async def test_idr_entry_rejects_missing_live_fx_rate(engine, fake_redis, mock_client):
    """IDR spot entries must fail closed even when executor runs in mock mode."""
    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    payload["pricing_snapshot"]["usdt_idr_rate"] = 0.0
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    await engine.handle_signal_notification(pair_tag)

    assert mock_client.placed_buys == []
    assert engine.states[pair_tag] == PositionState.FAILED


@pytest.mark.asyncio
async def test_filled_idr_order_without_persisted_fx_requires_reconciliation(engine, fake_redis, mock_client):
    """Never invent a conversion rate when reconciling an already-filled IDR order."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info.pop("usdt_idr_rate", None)
    await engine._handle_entry_fills(
        pair_tag,
        order_info,
        {"status": "closed", "filled": 0.001},
    )

    assert mock_client.global_short_sells == []
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_idr_entry_rejects_missing_fx_field_without_model_default(engine, fake_redis, mock_client):
    """An omitted FX field must not be silently replaced by a static rate."""
    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    payload["pricing_snapshot"].pop("usdt_idr_rate", None)
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    await engine.handle_signal_notification(pair_tag)

    assert mock_client.placed_buys == []
    assert engine.states[pair_tag] == PositionState.FAILED

@pytest.mark.asyncio
async def test_signal_for_unconfigured_venue_is_rejected(engine, fake_redis, mock_client):
    """A single-venue executor must never route a signal to a different exchange."""
    pair_tag = "indodax:btc_idr"
    engine.config.mock_mode = False
    payload = _fresh_signal_payload(int(time.time() * 1000))
    payload["pair_info"]["local_exchange"] = "tokocrypto"
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    await engine.handle_signal_notification(pair_tag)

    assert mock_client.placed_buys == []
    assert engine.states[pair_tag] == PositionState.FAILED
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "EXECUTOR_VENUE_MISMATCH"
