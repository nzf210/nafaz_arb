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
    assert engine.states[pair_tag] == PositionState.ENTRY_PENDING

    # 2. Stale signal test (> 500ms)
    stale_tag = "indodax:eth_idr"
    stale_payload = dict(fresh_payload)
    stale_payload["pair_info"] = dict(fresh_payload["pair_info"])
    stale_payload["pair_info"]["local_symbol"] = "eth_idr"
    stale_payload["pricing_snapshot"] = dict(fresh_payload["pricing_snapshot"])
    stale_payload["pricing_snapshot"]["timestamp_ms"] = now_ms - 800  # 800ms old!

    await fake_redis.set(f"arb:signal:{stale_tag}", json.dumps(stale_payload))
    await engine.handle_signal_notification(stale_tag)

    # No new buy placed
    assert len(mock_client.placed_buys) == 1

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
async def test_partial_fill_publishes_telegram_event_once(engine, fake_redis):
    """A partial Spot fill must produce one actionable Telegram trade event."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["qty"] = 0.1
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    pubsub = fake_redis.pubsub()
    await pubsub.subscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

    partial_order = {"status": "open", "filled": 0.000048}
    await engine._handle_entry_fills(pair_tag, order_info, partial_order)
    await engine._handle_entry_fills(pair_tag, order_info, partial_order)

    event = None
    for _ in range(10):
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if message and message.get("type") == "message":
            event = json.loads(message["data"])
            break

    assert event is not None
    assert event["event"] == "PARTIAL_FILL"
    assert event["pair_tag"] == pair_tag
    assert event["filled_qty"] == pytest.approx(0.000048)
    assert event["total_qty"] == pytest.approx(0.1)
    assert event["state"] == PositionState.ENTRY_PARTIAL.value
    assert order_info["partial_fill_alerted"] is True
    assert engine.states[pair_tag] == PositionState.ENTRY_PARTIAL
    await pubsub.unsubscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.aclose()


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
async def test_p2_02_excessive_slippage_aborts_and_rolls_back(fake_redis):
    """An ask far above the expected short price must abort the hedge, not open a bad position."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    client = MockExchangeClient()
    # Expected 65_000, config allows 0.3% -> limit 65_195. Simulate a 2% gap.
    client.update_market_price("btcusdt", best_bid=66_300.0, best_ask=66_300.0)
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
async def test_p0_04_missing_futures_price_aborts_and_rolls_back(fake_redis):
    """
    If the futures short returns no usable price, the engine must not fall back to
    the spot price. The hedge is aborted and the spot leg rolled back instead.
    """
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)

    class NoPriceShortClient(MockExchangeClient):
        async def open_global_short_market(self, symbol, amount, price=0.0, client_order_id=None):
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
    assert len(client.placed_sells) == 1
    assert engine.states[pair_tag] == PositionState.CLOSED

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
async def test_entry_revalidation_fails_closed_on_ticker_error(engine, fake_redis, mock_client):
    """A failed fresh quote must block Spot entry and publish an actionable alert."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    payload = _revalidation_payload(now_ms)

    async def boom(symbol):
        raise RuntimeError("ticker down")

    mock_client.fetch_global_ticker = boom
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(payload))

    pubsub = fake_redis.pubsub()
    await pubsub.subscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

    await engine.handle_signal_notification(pair_tag)

    assert len(mock_client.placed_buys) == 0
    assert pair_tag not in engine.pending_entry_orders

    event = None
    for _ in range(10):
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if message and message.get("type") == "message":
            event = json.loads(message["data"])
            break
    assert event is not None
    assert event["event"] == "EXECUTION_FAILURE"
    assert event["reason"] == "ENTRY_REVALIDATION_UNAVAILABLE"
    await pubsub.unsubscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.aclose()

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
async def test_spot_order_rejection_emits_failure_event_and_keeps_no_pending_order(engine, fake_redis, mock_client):
    """A rejected Spot submit must be visible to Telegram and must not create pending state."""
    pair_tag = "indodax:btc_idr"

    async def reject_spot_order(symbol, amount, price, client_order_id=None):
        raise RuntimeError("exchange rejected order: invalid quantity")

    mock_client.place_local_limit_buy = reject_spot_order
    await fake_redis.set(
        f"arb:signal:{pair_tag}",
        json.dumps(_fresh_signal_payload(int(time.time() * 1000))),
    )

    pubsub = fake_redis.pubsub()
    await pubsub.subscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

    await engine.handle_signal_notification(pair_tag)

    assert pair_tag not in engine.pending_entry_orders
    assert mock_client.placed_buys == []

    event = None
    for _ in range(10):
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if message and message.get("type") == "message":
            event = json.loads(message["data"])
            break
    assert event is not None
    assert event["event"] == "EXECUTION_FAILURE"
    assert event["reason"] == "SPOT_ORDER_OPEN_FAILED"
    assert event["pair_tag"] == pair_tag
    await pubsub.unsubscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.aclose()
