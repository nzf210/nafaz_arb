"""Paper-mode validation for Indodax adaptive maker lifecycle safeguards."""

import json
import time

import pytest

from tests.conftest import _entry_order_info


def test_indodax_limit_order_params_require_explicit_moc(monkeypatch):
    """Indodax limit paths carry MOC while non-limit paths do not."""
    from executor import exchange_common
    from executor.quantity import AmountSpec

    class FakeIndodaxTAPIv2:
        pass

    monkeypatch.setattr(exchange_common, "IndodaxTAPIv2", FakeIndodaxTAPIv2)
    spec = AmountSpec(
        unit="BASE", step=0.00000001, minimum=0.00000001, base_per_unit=1.0
    )
    exchange = FakeIndodaxTAPIv2()

    limit_params = exchange_common._local_order_params(
        exchange, "indodax-btc-idr-entry-1", spec,
        require_amount_step=True, require_post_only=True,
    )
    market_params = exchange_common._local_order_params(
        exchange, "indodax-btc-idr-market-1", spec,
        require_post_only=False,
    )

    assert limit_params == ({
        "clientOrderId": "indodax-btc-idr-entry-1",
        "quantityIncrement": 0.00000001,
        "timeInForce": "MOC",
    },)
    assert market_params == ({
        "clientOrderId": "indodax-btc-idr-market-1",
        "quantityIncrement": 0.00000001,
    },)


@pytest.mark.asyncio
async def test_indodax_idr_refresh_uses_15_second_policy_not_generic_policy(
    engine, fake_redis, mock_client
):
    """An aged, unfilled IDR maker order uses the dedicated 15s refresh policy."""
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    engine.config.indodax_maker_refresh_after_seconds = 15
    engine.config.indodax_maker_refresh_cooldown_seconds = 15
    # Deliberately disable the generic policy: Indodax IDR must use its own.
    engine.config.maker_refresh_after_seconds = 0

    order_info = _entry_order_info(pair_tag, now_ms)
    order_info.update({
        "placed_time": time.time() - 16,
        "quote_asset": "IDR",
        "min_entry_spread_pct": 0.10,
    })
    engine.pending_entry_orders[pair_tag] = order_info
    mock_client.orders["301"] = {
        "id": "301",
        "symbol": "btc_idr",
        "status": "open",
        "filled": 0.0,
    }
    await fake_redis.set(f"arb:live_price:{pair_tag}", json.dumps({
        "local_best_bid": 1_001_000_000.0,
        "local_best_ask": 1_002_000_000.0,
        "global_best_bid_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "timestamp_ms": now_ms,
    }))

    await engine._check_entry_orders()

    assert ("301", "btc_idr") in mock_client.cancelled_orders
    assert mock_client.placed_buys == []
    assert pair_tag not in engine.pending_entry_orders
    assert await fake_redis.exists(f"arb:maker_refresh_cooldown:{pair_tag}")


@pytest.mark.asyncio
async def test_indodax_order_fetch_rate_limit_uses_backoff_without_duplicate_poll(
    engine, fake_redis, monkeypatch
):
    """A 429-like adapter error is throttled instead of polled every monitor tick."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    engine.pending_entry_orders[pair_tag] = order_info
    calls = 0

    class IndodaxRateLimitError(Exception):
        pass

    async def rate_limited_fetch(order_id, symbol):
        nonlocal calls
        calls += 1
        raise IndodaxRateLimitError("HTTP 429 rate limit")

    monkeypatch.setattr(engine.client, "fetch_local_order", rate_limited_fetch)

    await engine._check_entry_orders()
    first_retry_after = order_info["fetch_retry_after"]
    await engine._check_entry_orders()

    assert calls == 1
    assert order_info["fetch_failures"] == 1
    assert first_retry_after > time.time()
    assert order_info["fetch_retry_after"] == first_retry_after
    assert pair_tag in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_spot_open_order_cap_counts_positions_and_pending_entries_per_venue(engine):
    """Open positions and pending entries share one per-spot-CEX cap."""
    from types import SimpleNamespace

    engine.config.max_open_spot_orders_per_exchange = {"indodax": 3}
    engine.active_positions["indodax:btc_idr"] = SimpleNamespace(
        local_exchange="indodax", qty=0.5
    )
    # Same pair is counted once even if its pending entry overlaps position state.
    engine.pending_entry_orders["indodax:btc_idr"] = {"local_exchange": "indodax"}
    engine.pending_entry_orders["indodax:eth_idr"] = {"local_exchange": "indodax"}
    engine.pending_entry_orders["tokocrypto:sol_usdt"] = {"local_exchange": "tokocrypto"}

    assert engine._spot_open_order_count("indodax") == 2
    assert engine._spot_open_order_count("tokocrypto") == 1
    assert engine._spot_order_limit("indodax") == 3
    assert engine._spot_order_limit("tokocrypto") == 2


def test_spot_order_limit_update_normalizes_exchange_and_rejects_invalid_values(engine):
    engine.config.exchanges = {"indodax": object(), "tokocrypto": object()}
    engine.config.max_open_spot_orders_per_exchange = {}

    assert engine._apply_spot_order_limit("TOKOCRYPTO", "4") is True
    assert engine.config.max_open_spot_orders_per_exchange["tokocrypto"] == 4
    assert engine._apply_spot_order_limit("new_cex-1", "4") is True
    assert engine.config.max_open_spot_orders_per_exchange["new_cex-1"] == 4
    assert engine._apply_spot_order_limit("bad cex", "4") is False
    assert engine._apply_spot_order_limit("indodax", "0") is False
    assert engine._spot_order_limit("indodax") == 2
