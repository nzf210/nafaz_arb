"""State 1/2 entry flow: pegging, adverse selection, TTL, partial fills, hedging, slippage guards, entry revalidation."""

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
async def test_state1_entry_order_pegging_fresh_vs_stale(engine, fake_redis, mock_client):
    """Test State 1: Fresh signal places limit buy, stale signal (> 500ms) is rejected."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # 1. Fresh signal
    fresh_payload = {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "binance",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1000.0,
            "local_lot_size": 0.00000001,
            "local_amount_spec": {"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
            "global_amount_spec": {"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0},
        },
        "pricing_snapshot": {
            "local_best_bid": 1000000000.0,
            "local_best_ask": 1002000000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms - 50,  # 50ms latency (fresh!)
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3600000,
            "minutes_to_next_funding": 60.0,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1000001000.0,
            "entry_spread_pct": 0.35,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 500.0,
            "cancel_if_global_bid_drops_below_usdt": 64800.0,
        },
    }

    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(fresh_payload))
    await engine.handle_signal_notification(pair_tag)

    # Order should be placed on local exchange
    assert len(mock_client.placed_buys) == 1
    placed = mock_client.placed_buys[0]
    assert placed["symbol"] == "btc_idr"
    assert placed["price"] == 1000001000.0
    assert placed["amount"] == pytest.approx(0.00799999)
    assert engine.states[pair_tag] == PositionState.ENTRY_PENDING

    # 2. Stale signal test (> 500ms)
    stale_tag = "indodax:eth_idr"
    stale_payload = dict(fresh_payload)
    stale_payload["pair_info"] = dict(fresh_payload["pair_info"])
    stale_payload["pair_info"]["local_symbol"] = "eth_idr"
    stale_payload["pair_info"]["global_symbol"] = "ETHUSDT"
    stale_payload["pricing_snapshot"] = dict(fresh_payload["pricing_snapshot"])
    stale_payload["pricing_snapshot"]["timestamp_ms"] = now_ms - 800  # 800ms old!

    await fake_redis.set(f"arb:signal:{stale_tag}", json.dumps(stale_payload))
    await engine.handle_signal_notification(stale_tag)

    # No new buy placed
    assert len(mock_client.placed_buys) == 1


@pytest.mark.asyncio
async def test_entry_quantity_is_floored_to_market_increment(engine, fake_redis, mock_client):
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _fresh_signal_payload(now_ms)
    payload["pair_info"]["local_lot_size"] = 0.0005
    payload["pair_info"]["local_amount_spec"] = {
        "unit": "BASE", "step": 0.0005, "min": 0.0005, "base_per_unit": 1.0
    }
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 1
    # Sizing yields 0.0016 BTC; floor to the venue's 0.0005 increment.
    assert mock_client.placed_buys[0]["amount"] == pytest.approx(0.0015)
    assert mock_client.placed_buys[0]["amount"] / 0.0005 == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_entry_without_market_increment_is_rejected(engine, fake_redis, mock_client):
    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    payload["pair_info"].pop("local_lot_size")
    payload["pair_info"].pop("local_amount_spec")
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    await engine.handle_signal_notification(pair_tag)

    assert mock_client.placed_buys == []
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == (
        "MARKET_AMOUNT_RULES_MISSING"
    )

@pytest.mark.asyncio
async def test_state1_adverse_selection_cancellation(engine, fake_redis, mock_client):
    """Test State 1: When Global Bid drops below cancel threshold, cancel local Limit Buy."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # Place entry order
    engine.pending_entry_orders[pair_tag] = {
        "order_id": "101",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1000001000.0,
        "qty": 0.008,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "cancel_threshold_usdt": 64800.0,
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["101"] = {
        "id": "101",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0,
    }

    # Simulate Global Bid dropping to 64700 (below 64800 threshold)
    live_snapshot = {
        "global_best_bid_usdt": 64700.0,
        "global_best_ask_usdt": 64710.0,
        "usdt_idr_rate": 16000.0,
        "timestamp_ms": now_ms,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps(live_snapshot))

    # Run check
    await engine._check_entry_orders()

    # Verify order was cancelled
    assert ("101", "btc_idr") in mock_client.cancelled_orders
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.IDLE

@pytest.mark.asyncio
async def test_state2_partial_fill_and_instant_short_hedge(engine, fake_redis, mock_client):
    """
    Test State 2: When local order has partial fill >= $10,
    immediately executes Market Sell (Short 1x) on Global Futures for exact coin qty!
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.pending_entry_orders[pair_tag] = {
        "order_id": "201",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1000000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "cancel_threshold_usdt": 60000.0,
        "usdt_idr_rate": 16000.0,
        "next_funding_time_ms": now_ms + 3600000,
        "base_target_exit_spread_pct": 0.3,
        "current_funding_rate": 0.0003,
        "global_amount_spec": {
            "unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0
        },
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    # Local order receives partial fill of 0.005 BTC (~$312.50 USD > $10)
    mock_client.orders["201"] = {
        "id": "201",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.005,
    }

    await engine._check_entry_orders()

    # Verify Global Market Short executed for exact 0.005 BTC
    assert len(mock_client.global_short_sells) == 1
    short_exec = mock_client.global_short_sells[0]
    assert short_exec["symbol"] == "BTCUSDT"
    assert short_exec["side"] == "sell"
    assert short_exec["amount"] == 0.005

    # Verify active position created and written to Redis arb:active_pos:indodax:btc_idr
    active_raw = await fake_redis.get(f"arb:active_pos:{pair_tag}")
    assert active_raw is not None
    pos_data = json.loads(active_raw)
    assert pos_data["qty"] == 0.005
    assert pos_data["symbol"] == "btc_idr"
    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE
    # Partial fill extends Redis pending entry TTL and records PARTIALLY_FILLED in Supabase
    ttl = await fake_redis.ttl(f"arb:pending_entry:{pair_tag}")
    assert ttl > 0
    assert engine.supabase._position_rows[pair_tag]["status"] == "PARTIALLY_FILLED"

@pytest.mark.asyncio
async def test_entry_order_ttl_expiration(engine, fake_redis, mock_client):
    """
    Test State 1 Order TTL Expiration:
    When a maker limit buy order is placed, but the market never touches it
    and max_order_ttl_seconds expires, the order must be cancelled cleanly.
    """
    pair_tag = "indodax:btc_idr"
    engine.config.max_order_ttl_seconds = 10

    # Order placed 15 seconds ago without fill
    engine.pending_entry_orders[pair_tag] = {
        "order_id": "701",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "price": 1000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "placed_time": time.time() - 15.0,  # 15s elapsed > 10s TTL
    }
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["701"] = {
        "id": "701",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0,
    }

    await engine._check_entry_orders()

    # Order was cancelled due to TTL
    assert ("701", "btc_idr") in mock_client.cancelled_orders
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.IDLE

@pytest.mark.asyncio
async def test_entry_order_ttl_cancels_remainder_and_hedges_partial_fill(engine, fake_redis, mock_client):
    """An aged partially-filled maker order must cancel its remainder; a partial
    fill below the hedge threshold must NOT open a futures hedge (wait_until_covered
    policy) — it is routed to reconciliation instead of silently bypassing policy."""
    pair_tag = "indodax:btc_idr"
    engine.config.max_order_ttl_seconds = 10
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time() - 15
    order_info["qty"] = 0.01
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.000048,
    }

    await engine._check_entry_orders()

    assert ("301", "btc_idr") in mock_client.cancelled_orders
    # Fill is 0.48% (< 20% threshold) and $3 (< $10 min-notional): no hedge may
    # be submitted under the configured hedging policy.
    assert len(mock_client.global_short_sells) == 0
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_entry_order_ttl_cancels_remainder_and_hedges_fill_above_threshold(engine, fake_redis, mock_client):
    """When the fill has crossed the hedge threshold, the TTL cancel must still
    hedge the remaining delta normally."""
    pair_tag = "indodax:btc_idr"
    engine.config.max_order_ttl_seconds = 10
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time() - 15
    order_info["qty"] = 0.01
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005,
    }

    await engine._check_entry_orders()

    assert ("301", "btc_idr") in mock_client.cancelled_orders
    assert len(mock_client.global_short_sells) == 1
    assert mock_client.global_short_sells[0]["amount"] == pytest.approx(0.005)
    assert pair_tag in engine.active_positions
    assert engine.active_positions[pair_tag].qty == pytest.approx(0.005)
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE


@pytest.mark.asyncio
async def test_pending_partial_fill_progress_is_persisted_for_restart(engine, fake_redis, mock_client):
    """A below-threshold partial fill is durably recorded before the next monitoring cycle."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["qty"] = 0.1
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.000048,
    }

    await engine._check_entry_orders()

    persisted = json.loads(await fake_redis.get("arb:pending_entry:" + pair_tag))
    assert persisted["filled"] == pytest.approx(0.000048)
    assert persisted["hedged_short_qty"] == pytest.approx(0.0)
    assert engine.states[pair_tag] == PositionState.ENTRY_PARTIAL


@pytest.mark.asyncio
async def test_stale_unfilled_maker_quote_is_cancelled_for_fresh_scanner_reentry(
    engine, fake_redis, mock_client
):
    """Refresh cancels safely and only requeues when a fresh scanner payload exists."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    engine.config.maker_refresh_after_seconds = 45
    engine.config.maker_refresh_cooldown_seconds = 300
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 60
    order_info["min_entry_spread_pct"] = 0.10
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.0,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps({
        "local_best_bid": 1_000_001_000.0,
        "local_best_ask": 1_002_000_000.0,
        "global_best_bid_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "timestamp_ms": now_ms,
    }))

    await engine._check_entry_orders()

    assert ("301", "btc_idr") in mock_client.cancelled_orders
    assert mock_client.placed_buys == []
    assert pair_tag not in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.IDLE
    assert await fake_redis.exists(f"arb:maker_refresh_cooldown:{pair_tag}")


@pytest.mark.asyncio
async def test_stale_maker_quote_with_any_partial_fill_is_never_refreshed(
    engine, fake_redis, mock_client
):
    """A partial spot fill remains on the fill/hedge path, never the refresh path."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    engine.config.maker_refresh_after_seconds = 1
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 60
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.000048,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps({
        "local_best_bid": 1_000_001_000.0,
        "local_best_ask": 1_002_000_000.0,
        "global_best_bid_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "timestamp_ms": now_ms,
    }))

    await engine._check_entry_orders()

    assert mock_client.cancelled_orders == []
    assert pair_tag in engine.pending_entry_orders
    assert engine.states[pair_tag] == PositionState.ENTRY_PARTIAL


@pytest.mark.asyncio
async def test_stale_maker_quote_is_not_refreshed_when_live_edge_is_below_floor(
    engine, fake_redis, mock_client
):
    """Refresh is denied if the best-bid candidate no longer meets the edge floor."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    engine.config.maker_refresh_after_seconds = 1
    order_info = _entry_order_info(pair_tag, now_ms)
    order_info["placed_time"] = time.time() - 60
    order_info["min_entry_spread_pct"] = 4.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.0,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps({
        "local_best_bid": 1_000_001_000.0,
        "local_best_ask": 1_002_000_000.0,
        "global_best_bid_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "timestamp_ms": now_ms,
    }))

    await engine._check_entry_orders()

    assert mock_client.cancelled_orders == []
    assert pair_tag in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_p2_01_partial_fill_below_threshold_sets_entry_partial(engine, fake_redis, mock_client):
    """BUG-08/P2-01: below-threshold partial fills must be visible as ENTRY_PARTIAL."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    order_info = _entry_order_info(pair_tag, now_ms)
    # $3 of value is well under the $10 hedge threshold.
    order_info["qty"] = 0.1
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.000048,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.ENTRY_PARTIAL
    # Nothing was hedged, so no position and no order left the pending list.
    assert pair_tag not in engine.active_positions
    assert mock_client.global_short_sells == []
    assert pair_tag in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_p2_01_partial_fill_above_threshold_hedges_and_activates(engine, fake_redis, mock_client):
    """Over-threshold fills hedge immediately and never linger in ENTRY_PARTIAL."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE
    assert pair_tag in engine.active_positions

@pytest.mark.asyncio
async def test_hedging_fill_threshold_waits_below_pct(engine, fake_redis, mock_client):
    """A USD-significant fill below the configured cumulative fill threshold stays unhedged."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    # 15% of 0.01 BTC ≈ $93 — above the $10 min-notional floor but below the
    # 20% hedge activation threshold, so the hedge must wait.
    order_info = _entry_order_info(pair_tag, now_ms)
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.0015,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.ENTRY_PARTIAL
    assert mock_client.global_short_sells == []
    assert pair_tag in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_hedging_fill_threshold_hedges_once_crossed(engine, fake_redis, mock_client):
    """Crossing the cumulative fill threshold activates the incremental hedge."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    order_info = _entry_order_info(pair_tag, now_ms)
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE
    assert len(mock_client.global_short_sells) == 1

@pytest.mark.asyncio
async def test_hedging_disabled_restores_usd_delta_behavior(engine, fake_redis, mock_client):
    """hedging.enabled=false falls back to the legacy USD-delta hedge trigger."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.config.hedging.enabled = False
    order_info = _entry_order_info(pair_tag, now_ms)
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {
        "id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.0015,
    }

    await engine._check_entry_orders()

    assert engine.states[pair_tag] == PositionState.HEDGED_ACTIVE
    assert len(mock_client.global_short_sells) == 1

@pytest.mark.asyncio
async def test_p2_02_excessive_slippage_aborts_and_rolls_back(fake_redis):
    """An ask far above the expected short price must abort the hedge, not open a bad position."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    # Expected 65_000, config allows 0.3% -> limit 65_195. Simulate a 2% gap.
    client.update_market_price("btcusdt", best_bid=63_700.0, best_ask=63_700.0)
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.003),
        client=client,
        redis_client=fake_redis,
    )

    order_info = _entry_order_info(pair_tag, now_ms)
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    # No short was opened and the spot leg was sold back
    assert client.global_short_sells == []
    assert pair_tag not in engine.active_positions
    assert len(client.placed_sells) == 1
    assert client.placed_sells[0]["type"] == "market"
    assert order_info["hedged_short_qty"] == 0.0

@pytest.mark.asyncio
async def test_p2_02_acceptable_slippage_still_hedges(fake_redis):
    """A small gap inside the tolerance must not block a legitimate hedge."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    # 65_100 is +0.154%, inside the 0.3% limit.
    client.update_market_price("btcusdt", best_bid=65_090.0, best_ask=65_100.0)
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.003),
        client=client,
        redis_client=fake_redis,
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert len(client.global_short_sells) == 1
    assert pair_tag in engine.active_positions
    assert client.placed_sells == []

@pytest.mark.asyncio
async def test_p2_02_missing_live_ask_fails_open(fake_redis):
    """Without a readable live ask the guard must not block the hedge."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()  # no market price seeded -> ask == 0
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.003),
        client=client,
        redis_client=fake_redis,
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert len(client.global_short_sells) == 1
    assert pair_tag in engine.active_positions

@pytest.mark.asyncio
async def test_p2_02_zero_slippage_disables_guard(fake_redis):
    """max_entry_slippage_pct=0 disables the guard entirely."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    client.update_market_price("btcusdt", best_bid=80_000.0, best_ask=80_000.0)
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True, max_entry_slippage_pct=0.0),
        client=client,
        redis_client=fake_redis,
    )

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert len(client.global_short_sells) == 1

@pytest.mark.asyncio
async def test_p0_04_short_entry_uses_futures_ask_not_spot_price(engine, fake_redis, mock_client):
    """BUG-01/P0-04: the recorded short entry must come from the futures ask, never spot."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    pos = engine.active_positions[pair_tag]
    # spot price would be 1_000_000_000 / 16_000 = 62_500 USDT; futures ask is 65_000
    assert pos.short_entry_usdt == 65_000.0
    assert mock_client.global_short_sells[0]["price"] == 65_000.0

@pytest.mark.asyncio
async def test_p0_04_missing_futures_price_requires_manual_reconciliation(fake_redis):
    """
    A futures order is known to have been accepted; missing fill price must not
    trigger a second risk-changing order against an unresolved hedge.
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    class NoPriceShortClient(MockExchangeClient):
        async def open_global_short_market(self, symbol, amount, price=0.0, client_order_id=None, amount_spec=None):
            record = {
                "id": "no-price-hedge",
                "symbol": symbol,
                "side": "sell",
                "amount": amount,
                "filled": amount,
                "status": "closed",
            }
            self.global_short_sells.append(record)
            return record

    client = NoPriceShortClient()
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    engine.pending_entry_orders[pair_tag] = _entry_order_info(pair_tag, now_ms)
    client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.005}

    await engine._check_entry_orders()

    assert pair_tag not in engine.active_positions
    assert client.placed_sells == []
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert pair_tag in engine._recovery_scope_halts

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

@pytest.mark.asyncio
async def test_spot_submit_timeout_recovers_existing_order_by_client_id(
    engine, fake_redis, mock_client
):
    """A spot order accepted before a lost response is adopted, never resubmitted."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)
    mock_client.update_market_price("BTC/USDT", 65000.0, 65010.0)
    engine.client = mock_client
    original_submit = mock_client.place_local_limit_buy
    submit_calls = []
    lookup_calls = []

    async def submit_then_lose_response(symbol, amount, price, client_order_id=None, amount_spec=None):
        submit_calls.append(client_order_id)
        await original_submit(
            symbol, amount, price,
            client_order_id=client_order_id, amount_spec=amount_spec,
        )
        raise TimeoutError("exchange accepted order but response was lost")

    async def lookup_by_client_id(client_order_id, symbol, amount_spec=None):
        lookup_calls.append(client_order_id)
        for order in mock_client.orders.values():
            if order.get("client_order_id") == client_order_id:
                recovered = dict(order)
                recovered["clientOrderId"] = client_order_id
                return recovered
        return None

    mock_client.place_local_limit_buy = submit_then_lose_response
    mock_client.fetch_local_order_by_client_id = lookup_by_client_id
    await fake_redis.set(
        f"arb:signal:{pair_tag}", json.dumps(payload)
    )

    await engine.handle_signal_notification(pair_tag)

    assert len(submit_calls) == 1
    assert lookup_calls == submit_calls
    assert len(mock_client.placed_buys) == 1
    assert engine.pending_entry_orders[pair_tag]["client_order_id"] == submit_calls[0]
    assert engine.pending_entry_orders[pair_tag]["order_id"] == mock_client.placed_buys[0]["id"]
    assert engine.states[pair_tag] == PositionState.ENTRY_PENDING


@pytest.mark.asyncio
async def test_fresh_maker_quote_scales_usdt_local_book_to_idr(engine, fake_redis):
    """USDT spot live snapshots must be converted to IDR before comparing against IDR-normalized tick/price."""
    pair_tag = "tokocrypto:toko:ath_usdt__okx:ath_usdt_swap"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(
        f"arb:live_price:{pair_tag}",
        json.dumps({
            "local_best_bid": 0.0043,
            "local_best_ask": 0.0044,
            "global_best_bid_usdt": 0.0045,
            "global_best_ask_usdt": 0.0046,
            "usdt_idr_rate": 18000.0,
            "timestamp_ms": now_ms,
        }),
    )
    order_info = {
        "symbol": "toko:ATH_USDT",
        "quote_asset": "USDT",
        "price": 75.6,  # 0.0042 * 18000
        "tick_size": 1.8,  # 0.0001 * 18000
        "usdt_idr_rate": 18000.0,
        "min_entry_spread_pct": 0.2,
    }
    quote = await engine._fresh_maker_quote(pair_tag, order_info)
    assert quote is not None
    assert quote["candidate_price"] > 70.0
    assert quote["local_bid"] == pytest.approx(0.0043 * 18000.0)


@pytest.mark.asyncio
async def test_early_spot_slot_limit_skips_rest_calls(engine, fake_redis, mock_client):
    """When venue spot slots are full, reject immediately without burning exchange REST calls."""
    pair_tag = "tokocrypto:toko:ath_usdt__okx:ath_usdt_swap"
    engine.config.max_open_spot_orders_per_exchange = {"tokocrypto": 1}
    engine.pending_entry_orders["tokocrypto:toko:soon_idr__okx:soon_usdt_swap"] = {
        "local_exchange": "tokocrypto"
    }
    calls = []

    async def fail_if_called(*args, **kwargs):
        calls.append(args)
        raise AssertionError("REST probe must not run when venue slot limit is already reached")

    mock_client.fetch_global_ticker = fail_if_called
    payload = _fresh_signal_payload(int(time.time() * 1000))
    payload["pair_info"]["local_exchange"] = "tokocrypto"
    payload["pair_info"]["global_exchange"] = "okx"
    payload["pair_info"]["local_symbol"] = "toko:ATH_USDT"
    payload["pair_info"]["global_symbol"] = "ATH-USDT-SWAP"
    payload["pair_info"]["quote_asset"] = "USDT"
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    await engine.handle_signal_notification(pair_tag)

    assert calls == []
    assert mock_client.placed_buys == []
    assert pair_tag not in engine._pair_halted


@pytest.mark.asyncio
async def test_prepare_entry_sizing_fetches_balance_once(engine, fake_redis, mock_client):
    """_prepare_entry_sizing must reuse balance snapshot for dual-leg funding preflight."""
    pair_tag = "tokocrypto:toko:ath_usdt__okx:ath_usdt_swap"
    engine.config.mock_mode = False
    balance_calls = 0

    async def counted_get_balances(**kwargs):
        nonlocal balance_calls
        balance_calls += 1
        return {
            "spot_available": 50.0,
            "futures_available": 100.0,
            "spot_total": 50.0,
            "futures_total": 100.0,
        }

    mock_client.get_balances = counted_get_balances
    payload_dict = _fresh_signal_payload(int(time.time() * 1000))
    payload_dict["pair_info"]["local_exchange"] = "tokocrypto"
    payload_dict["pair_info"]["global_exchange"] = "okx"
    from executor.models import ExecutorPayload
    payload = ExecutorPayload.from_dict(payload_dict)

    sizing = await engine._prepare_entry_sizing(
        pair_tag, payload, payload.pair_info, payload.execution_directives
    )
    assert sizing is not None
    assert balance_calls == 1


@pytest.mark.asyncio
async def test_maker_refresh_requeues_only_fresh_scanner_signal(engine, fake_redis):
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(
        f"arb:signal:{pair_tag}",
        json.dumps(_fresh_signal_payload(now_ms)),
    )

    requeued = await engine._requeue_fresh_signal_after_maker_refresh(pair_tag)

    assert requeued is True
    assert await fake_redis.zscore("arb:signal_priority", pair_tag) == pytest.approx(0.0)
    assert engine._signal_queue_event.is_set()


@pytest.mark.asyncio
async def test_maker_refresh_does_not_requeue_when_position_is_active(engine, fake_redis):
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(
        f"arb:signal:{pair_tag}",
        json.dumps(_fresh_signal_payload(now_ms)),
    )
    engine.active_positions[pair_tag] = object()

    requeued = await engine._requeue_fresh_signal_after_maker_refresh(pair_tag)

    assert requeued is False
    assert await fake_redis.zscore("arb:signal_priority", pair_tag) is None


@pytest.mark.asyncio
async def test_independent_heartbeat_loop_publishes_status(engine, fake_redis):
    """Heartbeat loop publishes arb:executor:status independently with TTL."""
    engine._running = True
    engine.config.heartbeat_interval_seconds = 0.05
    heartbeat_task = asyncio.create_task(engine._heartbeat_loop())
    try:
        await asyncio.sleep(0.12)
        raw = await fake_redis.get("arb:executor:status")
        assert raw is not None
        status = json.loads(raw)
        assert status["status"] == "RUNNING"
        ttl = await fake_redis.ttl("arb:executor:status")
        assert ttl > 0
    finally:
        engine._running = False
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_slot_exhausted_does_not_increment_order_failures(engine):
    """SlotExhaustedError must publish SPOT_SLOT_EXHAUSTED and NOT increment venue failure breaker."""
    from unittest.mock import MagicMock
    from executor.order_slots import SlotExhaustedError
    pair_tag = "tokocrypto:toko:spx_idr__okx:spx_usdt_swap"
    pair_info = MagicMock()
    pair_info.local_exchange = "tokocrypto"
    pair_info.local_symbol = "toko:spx_idr"

    initial_failures = engine._consecutive_order_failures.get("tokocrypto", 0)
    exc = SlotExhaustedError("All 1 spot slots occupied on tokocrypto:default (active: {0})")

    await engine._handle_entry_submission_failure(pair_tag, pair_info, 10.0, exc)

    assert engine._consecutive_order_failures.get("tokocrypto", 0) == initial_failures
    assert "tokocrypto" not in engine._exchange_halted


@pytest.mark.asyncio
async def test_global_stop_fence_does_not_increment_order_failures(engine):
    """GlobalStopError from in-flight fence must NOT increment venue failure breaker."""
    from unittest.mock import MagicMock
    from executor.global_stop import GlobalStopError
    pair_tag = "indodax:dodo_idr__okx:dodo_usdt_swap"
    pair_info = MagicMock()
    pair_info.local_exchange = "indodax"
    pair_info.local_symbol = "dodo_idr"

    initial_failures = engine._consecutive_order_failures.get("indodax", 0)
    exc = GlobalStopError(
        "unresolved in-flight submit blocks new entries: mexc:DODO_USDT client_order_id=xyz status=UNKNOWN"
    )

    await engine._handle_entry_submission_failure(pair_tag, pair_info, 10.0, exc)

    assert engine._consecutive_order_failures.get("indodax", 0) == initial_failures
    assert "indodax" not in engine._exchange_halted


@pytest.mark.asyncio
async def test_mexc_6002_classified_as_trading_permission_denied():
    """MEXC code 6002 futures position-opening restriction must be recognized immediately."""
    from executor.helpers import is_trading_permission_denied
    exc = Exception(
        'mexc {"success":false,"code":6002,"message":"Your futures position-opening has been restricted. Please check your email or in-app message for details."}'
    )
    assert is_trading_permission_denied(exc) is True




@pytest.mark.asyncio
async def test_disabled_execution_module_blocks_entry_before_market_preflight(
    engine, fake_redis, monkeypatch
):
    from unittest.mock import AsyncMock, MagicMock
    from executor.config import ExecutionModuleConfig

    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    engine.config.modules = {
        "spot_idr": ExecutionModuleConfig(
            id="spot_idr",
            quote="IDR",
            spot_venues=["indodax"],
            enabled=False,
        )
    }
    monkeypatch.setattr(
        engine, "_is_stale_or_duplicate_signal", AsyncMock(return_value=False)
    )
    failure = MagicMock()
    monkeypatch.setattr(engine, "_mark_failed", failure)
    monkeypatch.setattr(engine, "publish_status", AsyncMock())
    spread_check = AsyncMock()
    monkeypatch.setattr(engine, "_validate_entry_spread", spread_check)

    await engine._handle_signal_locked(pair_tag)

    failure.assert_called_once()
    assert failure.call_args.args[1] == "EXECUTION_MODULE_DISABLED"
    spread_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_zero_module_slot_cap_blocks_entry_before_market_preflight(
    engine, fake_redis, monkeypatch
):
    from unittest.mock import AsyncMock
    from executor.config import ExecutionModuleConfig

    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    engine.config.modules = {
        "spot_idr": ExecutionModuleConfig(
            id="spot_idr",
            quote="IDR",
            spot_venues=["indodax"],
            enabled=True,
            max_slots_per_venue=0,
        )
    }
    monkeypatch.setattr(
        engine, "_is_stale_or_duplicate_signal", AsyncMock(return_value=False)
    )
    spread_check = AsyncMock()
    monkeypatch.setattr(engine, "_validate_entry_spread", spread_check)

    await engine._handle_signal_locked(pair_tag)

    spread_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_module_max_position_cap_reduces_entry_sizing(engine, fake_redis, monkeypatch):
    from unittest.mock import AsyncMock
    from executor.config import ExecutionModuleConfig

    pair_tag = "indodax:btc_idr"
    payload_data = _fresh_signal_payload(int(time.time() * 1000))
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload_data))
    payload = await engine._load_signal_payload(pair_tag)
    engine.config.mock_mode = False
    engine.config.modules = {
        "spot_idr": ExecutionModuleConfig(
            id="spot_idr",
            quote="IDR",
            spot_venues=["indodax"],
            enabled=True,
            max_position_usdt=10.0,
        )
    }
    monkeypatch.setattr(engine, "_compute_entry_sizing", AsyncMock(return_value=(100.0, None)))
    monkeypatch.setattr(engine, "_verify_dual_leg_funding", AsyncMock(return_value=True))

    result = await engine._prepare_entry_sizing(
        pair_tag, payload, payload.pair_info, payload.execution_directives
    )

    assert result is not None
    _, actual_cost_usdt, _ = result
    assert actual_cost_usdt <= 10.0 + 1e-8


@pytest.mark.asyncio
async def test_execution_module_hedge_allowlist_blocks_unconfigured_venue_before_spot_preflight(
    engine, fake_redis, monkeypatch
):
    from unittest.mock import AsyncMock, MagicMock
    from executor.config import ExecutionModuleConfig

    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    payload["pair_info"]["global_exchange"] = "mexc"
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    engine.config.modules = {
        "spot_idr": ExecutionModuleConfig(
            id="spot_idr",
            quote="IDR",
            spot_venues=["indodax"],
            hedge_venues=["okx"],
            enabled=True,
        )
    }
    monkeypatch.setattr(
        engine, "_is_stale_or_duplicate_signal", AsyncMock(return_value=False)
    )
    failure = MagicMock()
    monkeypatch.setattr(engine, "_mark_failed", failure)
    monkeypatch.setattr(engine, "publish_status", AsyncMock())
    spread_check = AsyncMock(return_value=True)
    monkeypatch.setattr(engine, "_validate_entry_spread", spread_check)

    await engine._handle_signal_locked(pair_tag)

    failure.assert_called_once()
    assert failure.call_args.args[1] == "EXECUTION_HEDGE_VENUE_NOT_CONFIGURED"
    assert failure.call_args.kwargs["detail"]["hedge_exchange"] == "mexc"
    spread_check.assert_not_awaited()


@pytest.mark.asyncio
async def test_module_slot_cap_is_forwarded_to_durable_reservation_boundary(
    engine, fake_redis, monkeypatch
):
    from unittest.mock import AsyncMock
    from executor.config import ExecutionModuleConfig

    pair_tag = "indodax:btc_idr"
    payload = _fresh_signal_payload(int(time.time() * 1000))
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))
    engine.config.modules = {
        "spot_idr": ExecutionModuleConfig(
            id="spot_idr",
            quote="IDR",
            spot_venues=["indodax"],
            enabled=True,
            max_slots_per_venue=1,
        )
    }
    monkeypatch.setattr(
        engine, "_is_stale_or_duplicate_signal", AsyncMock(return_value=False)
    )
    monkeypatch.setattr(engine, "_validate_entry_spread", AsyncMock(return_value=True))
    monkeypatch.setattr(engine, "_refresh_fee_snapshot", AsyncMock())
    monkeypatch.setattr(engine, "_validate_dynamic_fee_edge", AsyncMock(return_value=True))
    monkeypatch.setattr(engine, "_validate_entry_market_rules", AsyncMock(return_value=True))
    monkeypatch.setattr(engine, "_validate_hedge_leg_tradable", AsyncMock(return_value=True))
    monkeypatch.setattr(engine, "_apply_indodax_adaptive_maker_quote", AsyncMock(return_value=True))
    monkeypatch.setattr(
        engine, "_prepare_entry_sizing", AsyncMock(return_value=(0.001, 10.0, 100000.0))
    )
    submit = AsyncMock()
    monkeypatch.setattr(engine, "_submit_entry_order", submit)

    await engine._handle_signal_locked(pair_tag)

    submit.assert_awaited_once()
    assert submit.await_args.kwargs["module_slot_limit"] == 1
