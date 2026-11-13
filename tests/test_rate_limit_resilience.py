"""Regression tests for venue rate-limit resilience (HTTP 429 hot-loop).

Background: the entry status poller retried a failed fetch with no counter and
no backoff, 20x/second per pending order. Indodax answered HTTP 429 with an HTML
body, the client raised a JSONDecodeError, and the loop hammered the venue
forever -- pinning pending orders (and their reserved funds) open. These tests
lock in the fixes: classified rate-limit errors, per-order exponential backoff
with a failure cap, clock-based TTL enforcement when status is unfetchable, and
the dual-leg funding preflight.
"""

import asyncio
import time

import pytest

from executor.config import ExecutorConfig
from executor.indodax_v2 import IndodaxAPIError, IndodaxRateLimitError, _parse_retry_after
from executor.models import PositionState

from tests.conftest import _entry_order_info, engine, fake_redis, mock_client


def test_rate_limit_error_is_a_kind_of_api_error():
    """Existing handlers catch IndodaxAPIError; the 429 type must remain caught."""
    err = IndodaxRateLimitError("http_429", "rate limited", retry_after=12.5)
    assert isinstance(err, IndodaxAPIError)
    assert err.retry_after == 12.5
    assert err.code == "http_429"


def test_parse_retry_after_accepts_delta_seconds_and_rejects_junk():
    assert _parse_retry_after("30") == 30.0
    assert _parse_retry_after(" 4.5 ") == 4.5
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("") is None
    assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None
    assert _parse_retry_after("-1") is None


def test_monitor_interval_default_is_not_a_hot_loop():
    """A sub-100ms tick drains the venue quota; the default must be sane."""
    assert ExecutorConfig().monitor_interval_seconds >= 0.1


def test_new_config_knobs_are_clamped_to_safe_ranges():
    """A typo in config.json must not disable the guards via a zero value."""
    cfg = ExecutorConfig()
    cfg.monitor_interval_seconds = 0.0
    cfg.max_order_fetch_failures = 0
    cfg.order_fetch_backoff_max_seconds = 0.0
    cfg.venue_rate_limit_cooldown_seconds = 0.0
    warnings = cfg.sanitize()

    assert cfg.monitor_interval_seconds >= 0.05
    assert cfg.max_order_fetch_failures >= 1
    assert cfg.order_fetch_backoff_max_seconds >= 0.1
    assert cfg.venue_rate_limit_cooldown_seconds >= 1.0
    assert warnings


@pytest.mark.asyncio
async def test_failed_status_fetch_backs_off_instead_of_hot_looping(engine, fake_redis, mock_client):
    """One failed fetch must set a retry window and count the failure."""
    pair_tag = "indodax:btc_idr"
    engine.config.monitor_interval_seconds = 0.5
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time()
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    async def _boom(*_args, **_kwargs):
        raise IndodaxRateLimitError("http_429", "non-JSON response: <!doctype html>", 5.0)

    mock_client.fetch_local_order = _boom

    before = time.time()
    await engine._check_entry_orders()

    assert order_info["fetch_failures"] == 1
    assert order_info["fetch_retry_after"] > before, "no backoff window was installed"
    # The order is still tracked: a single failure must not abandon it.
    assert pair_tag in engine.pending_entry_orders
    assert "IndodaxRateLimitError" in order_info["last_fetch_error"]


@pytest.mark.asyncio
async def test_backoff_skips_the_tick_and_does_not_call_the_venue(engine, fake_redis, mock_client):
    """While the retry window is open the venue must not be polled at all."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time()
    order_info["fetch_failures"] = 3
    order_info["fetch_retry_after"] = time.time() + 60.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    calls = {"n": 0}
    original = mock_client.fetch_local_order

    async def _counting(*args, **kwargs):
        calls["n"] += 1
        return await original(*args, **kwargs)

    mock_client.fetch_local_order = _counting

    await engine._check_entry_orders()

    assert calls["n"] == 0, "venue was polled while the backoff window was open"


@pytest.mark.asyncio
async def test_backoff_grows_exponentially_and_is_capped(engine, fake_redis, mock_client):
    """Backoff must increase per failure and never exceed the configured ceiling."""
    engine.config.monitor_interval_seconds = 0.5
    engine.config.order_fetch_backoff_max_seconds = 60.0

    delays = []
    for failure_index in range(8):
        pair_tag = f"indodax:coin{failure_index}_idr"
        order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
        order_info["order_id"] = f"90{failure_index}"
        order_info["symbol"] = f"coin{failure_index}_idr"
        order_info["placed_time"] = time.time()
        order_info["fetch_failures"] = failure_index
        order_info["fetch_retry_after"] = 0.0
        engine.pending_entry_orders[pair_tag] = order_info
        engine.states[pair_tag] = PositionState.ENTRY_PENDING

        async def _boom(*_args, **_kwargs):
            raise IndodaxRateLimitError("http_429", "throttled", None)

        mock_client.fetch_local_order = _boom
        before = time.time()
        await engine._check_entry_orders()
        delays.append(order_info["fetch_retry_after"] - before)
        engine.pending_entry_orders.pop(pair_tag, None)

    assert delays[0] < delays[1] < delays[2], f"backoff did not grow: {delays}"
    # The cap is a hard ceiling; allow only the clock skew between `before` and
    # the value stamped inside the loop.
    assert all(d <= engine.config.order_fetch_backoff_max_seconds + 0.5 for d in delays), delays
    assert delays[-1] == pytest.approx(engine.config.order_fetch_backoff_max_seconds, abs=0.5)


@pytest.mark.asyncio
async def test_unfetchable_order_is_reconciled_after_failure_cap(engine, fake_redis, mock_client):
    """A venue that never answers must trigger reconciliation, not endless polling.

    When the venue throttles both the status read and the confirming read, the
    cancel is still issued but the terminal state cannot be confirmed, so the
    order is deliberately retained rather than abandoned. That is the safe
    outcome: funds are never released on an unverified assumption.
    """
    pair_tag = "indodax:btc_idr"
    engine.config.max_order_fetch_failures = 3
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time()
    order_info["fetch_failures"] = 3  # already at the cap
    order_info["fetch_retry_after"] = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    async def _boom(*_args, **_kwargs):
        raise IndodaxRateLimitError("http_429", "throttled", None)

    mock_client.fetch_local_order = _boom

    await engine._check_entry_orders()

    # Reconciliation was attempted despite the unusable status feed.
    assert ("301", "btc_idr") in mock_client.cancelled_orders
    # Unconfirmed terminal state is retained, never silently dropped.
    assert pair_tag in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_expired_order_is_cancelled_on_the_clock_when_status_unfetchable(engine, fake_redis, mock_client):
    """TTL must be enforced from the local clock, not only from a fetched payload.

    The status feed answers for the initial read only; the cancel-confirmation
    read then succeeds, so the order resolves to terminal and is released.
    """
    pair_tag = "indodax:btc_idr"
    engine.config.max_order_ttl_seconds = 10
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time() - 120  # well past TTL
    order_info["fetch_failures"] = 1
    order_info["fetch_retry_after"] = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "canceled", "filled": 0.0}

    calls = {"n": 0}
    original = mock_client.fetch_local_order

    async def _fail_first_only(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise IndodaxRateLimitError("http_429", "throttled", None)
        return await original(*args, **kwargs)

    mock_client.fetch_local_order = _fail_first_only

    await engine._check_entry_orders()

    assert calls["n"] >= 2, "expiry never attempted a cancel-confirmation fetch"
    assert ("301", "btc_idr") in mock_client.cancelled_orders
    assert pair_tag not in engine.pending_entry_orders


@pytest.mark.asyncio
async def test_successful_fetch_resets_failure_counters(engine, fake_redis, mock_client):
    """A recovered venue must not carry stale failure state into the next tick."""
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    order_info["placed_time"] = time.time()
    order_info["fetch_failures"] = 4
    order_info["fetch_retry_after"] = 0.0
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING
    mock_client.orders["301"] = {"id": "301", "symbol": "btc_idr", "status": "open", "filled": 0.0}

    await engine._check_entry_orders()

    assert order_info["fetch_failures"] == 0
    assert order_info["fetch_retry_after"] == 0.0


@pytest.mark.asyncio
async def test_rate_limited_venue_is_paused_then_auto_resumed(engine, fake_redis, mock_client):
    """A 429 pauses new entries for a bounded cooldown and clears on its own."""
    engine.config.venue_rate_limit_cooldown_seconds = 1.0
    pair_tag = "indodax:btc_idr"
    engine._pair_infos[pair_tag] = type("PairInfo", (), {
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "local_symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "IDR",
    })()

    engine._pause_venue_for_cooldown("indodax", 1.0)
    assert engine._venue_halted_reason(pair_tag) != ""

    await asyncio.sleep(1.3)
    assert engine._venue_halted_reason(pair_tag) == ""


@pytest.mark.asyncio
async def test_newer_cooldown_is_not_cleared_by_a_stale_timer(engine, fake_redis, mock_client):
    """A second cooldown must survive the first one's expiring timer."""
    pair_tag = "indodax:btc_idr"
    engine._pair_infos[pair_tag] = type("PairInfo", (), {
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "local_symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "IDR",
    })()

    engine._pause_venue_for_cooldown("indodax", 1.0)
    await asyncio.sleep(0.6)
    engine._pause_venue_for_cooldown("indodax", 1.0)  # newer, longer-lived cooldown
    await asyncio.sleep(0.7)  # the first timer fires inside the second window

    assert engine._venue_halted_reason(pair_tag) != "", "stale timer cleared a live cooldown"


def _pair_info():
    from executor.models import PairInfo
    return PairInfo(
        local_exchange="indodax",
        global_exchange="okx",
        local_symbol="btc_idr",
        global_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="IDR",
    )


def _funded_engine(spot: float, futures: float):
    """Builds an engine whose client can report both legs' available balance."""
    import fakeredis.aioredis as fake_aioredis
    from executor.config import ExecutorConfig
    from executor.engine import ArbitrageEngine
    from tests.conftest import BalancesMockClient

    client = BalancesMockClient(spot_available=spot, futures_available=futures)
    cfg = ExecutorConfig(min_allocated_capital=10.0)
    redis = fake_aioredis.FakeRedis()
    return ArbitrageEngine(config=cfg, client=client, redis_client=redis), redis


async def _subscribe_failures(engine):
    """Subscribes to the trade-events channel before an action under test."""
    pubsub = engine.redis.pubsub()
    await pubsub.subscribe(engine.ARB_TRADE_EVENTS)
    await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
    return pubsub


async def _read_failures(pubsub) -> list:
    """Drains EXECUTION_FAILURE payloads that arrived after subscribing."""
    import json as _json
    messages = []
    while True:
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
        if msg is None:
            break
        payload = _json.loads(msg["data"])
        if payload.get("event") == "EXECUTION_FAILURE":
            messages.append(payload)
        messages_len = len(messages)
        if messages_len >= 5:
            break
    await pubsub.aclose()
    return messages


@pytest.mark.asyncio
async def test_dual_leg_preflight_rejects_when_spot_is_short():
    """A short spot leg must block the entry before either order is submitted."""
    pair_tag = "indodax:btc_idr"
    engine, _ = _funded_engine(spot=5.0, futures=1000.0)
    pubsub = await _subscribe_failures(engine)

    ok = await engine._verify_dual_leg_funding(pair_tag, _pair_info(), 50.0, 16000.0)

    assert ok is False
    published = await _read_failures(pubsub)
    assert any(p.get("reason") == "DUAL_LEG_UNDERFUNDED" for p in published), published


@pytest.mark.asyncio
async def test_dual_leg_preflight_rejects_when_futures_is_short():
    """A short futures leg must block the entry: an unhedgeable spot buy is the
    exact failure mode this gate exists to prevent."""
    pair_tag = "indodax:btc_idr"
    engine, _ = _funded_engine(spot=1000.0, futures=5.0)
    pubsub = await _subscribe_failures(engine)

    ok = await engine._verify_dual_leg_funding(pair_tag, _pair_info(), 50.0, 16000.0)

    assert ok is False
    published = await _read_failures(pubsub)
    failure = next(p for p in published if p.get("reason") == "DUAL_LEG_UNDERFUNDED")
    assert failure["detail"]["futures_available_usdt"] == pytest.approx(5.0)
    assert failure["detail"]["required_usdt_per_leg"] == pytest.approx(50.0)


@pytest.mark.asyncio
async def test_dual_leg_preflight_passes_when_both_legs_are_funded():
    pair_tag = "indodax:btc_idr"
    engine, _ = _funded_engine(spot=1000.0, futures=1000.0)
    pubsub = await _subscribe_failures(engine)

    ok = await engine._verify_dual_leg_funding(pair_tag, _pair_info(), 50.0, 16000.0)

    assert ok is True
    assert await _read_failures(pubsub) == []


@pytest.mark.asyncio
async def test_dual_leg_preflight_fails_closed_on_probe_error():
    """An unknown account balance must block both legs until the source recovers."""
    pair_tag = "indodax:btc_idr"
    engine, _ = _funded_engine(spot=1000.0, futures=1000.0)

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("balance endpoint unavailable")

    engine.client.get_balances = _boom

    ok = await engine._verify_dual_leg_funding(pair_tag, _pair_info(), 50.0, 16000.0)

    assert ok is False
    assert "indodax" in engine._exchange_halted
    assert "okx" in engine._exchange_halted


@pytest.mark.asyncio
async def test_indodax_client_classifies_429_html_body_as_rate_limit():
    """A throttle page (HTML, not JSON) must raise the rate-limit type, not a
    generic decode error -- the executor keys its backoff on that distinction."""
    from aiohttp import web
    from executor.indodax_v2 import IndodaxTAPIv2, IndodaxRateLimitError

    async def handler(request):
        return web.Response(
            status=429,
            text="<!doctype html><html><body>Too Many Requests</body></html>",
            headers={"Retry-After": "17"},
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/api/v2/order", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    client = IndodaxTAPIv2("k", "s", base_url=f"http://127.0.0.1:{port}")
    try:
        with pytest.raises(IndodaxRateLimitError) as excinfo:
            await client.fetch_order("123", "btc_idr")
        assert excinfo.value.retry_after == 17.0
    finally:
        await client.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_indodax_client_still_raises_api_error_for_plain_json_errors():
    """A normal JSON error payload must keep raising the base API error type."""
    from aiohttp import web
    from executor.indodax_v2 import IndodaxAPIError, IndodaxRateLimitError, IndodaxTAPIv2

    async def handler(request):
        return web.json_response({"code": -2010, "msg": "Insufficient balance"})

    app = web.Application()
    app.router.add_get("/api/v2/order", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    client = IndodaxTAPIv2("k", "s", base_url=f"http://127.0.0.1:{port}")
    try:
        with pytest.raises(IndodaxAPIError) as excinfo:
            await client.fetch_order("123", "btc_idr")
        assert not isinstance(excinfo.value, IndodaxRateLimitError)
        assert str(excinfo.value.code) == "-2010"
    finally:
        await client.close()
        await runner.cleanup()
