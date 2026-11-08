import json
import uuid

import httpx
import pytest

from executor.supabase_repository import SupabasePersistenceError, SupabaseRepository


def _repository(handler, retries=0):
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    return SupabaseRepository(
        "https://example.supabase.co",
        "service-role-secret",
        client=client,
        retries=retries,
    ), client


@pytest.mark.asyncio
async def test_position_open_persists_position_legs_and_deduplicated_event():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(201)

    repo, client = _repository(handler)
    position_id = str(uuid.uuid4())
    event = {
        "event": "POSITION_OPENED",
        "pair_key": "indodax:btc_idr",
        "position_id": position_id,
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "local_symbol": "btc_idr",
        "global_symbol": "BTC/USDT:USDT",
        "base_asset": "BTC",
        "quote_asset": "IDR",
        "qty": 0.01,
        "remaining_qty": 0.01,
        "initial_qty": 0.01,
        "entry_price_idr": 1_000_000_000,
        "short_entry_usdt": 65000,
        "entry_usdt_idr_rate": 16000,
        "spot_cost_quote": 10_000_000,
        "futures_margin_quote": 650,
        "opened_at_ms": 1794100000000,
        "is_mock": False,
        "timestamp_ms": 1794100001000,
    }

    try:
        await repo.persist_event(event)
        await repo.persist_event(event)
    finally:
        await client.aclose()

    position_posts = [r for r in requests if r.url.path.endswith("/positions") and r.method == "POST"]
    leg_posts = [r for r in requests if r.url.path.endswith("/position_legs")]
    event_posts = [r for r in requests if r.url.path.endswith("/executor_events")]
    assert len(position_posts) == 2  # retry-safe upsert by primary key
    assert len(leg_posts) == 2
    assert len(event_posts) == 2
    row = json.loads(position_posts[0].content)
    assert row["id"] == position_id
    assert row["status"] == "HEDGED_ACTIVE"
    assert row["futures_quote_asset"] == "USDT"
    assert "resolution=merge-duplicates" in position_posts[0].headers["Prefer"]
    assert "resolution=ignore-duplicates" in event_posts[0].headers["Prefer"]


@pytest.mark.asyncio
async def test_closed_trade_is_idempotent_and_updates_remaining_position():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204)

    repo, client = _repository(handler)
    position_id = str(uuid.uuid4())
    event = {
        "event": "POSITION_CLOSED",
        "trade_id": "trade-abc-123",
        "pair_key": "indodax:btc_idr",
        "position_id": position_id,
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "symbol": "btc_idr",
        "global_symbol": "BTC/USDT:USDT",
        "qty": 0.004,
        "remaining_qty": 0.006,
        "entry_price_idr": 1_000_000_000,
        "exit_price_idr": 1_005_000_000,
        "short_entry_usdt": 65000,
        "short_exit_usdt": 64900,
        "spot_pnl_idr": 20000,
        "futures_pnl_usdt": 0.4,
        "accumulated_funding_usdt": 0.01,
        "total_fees_usdt": 0.02,
        "net_profit_usdt": 1.64,
        "net_profit_idr": 26240,
        "net_roi_pct": 0.25,
        "entry_usdt_idr_rate": 16000,
        "exit_usdt_idr_rate": 16000,
        "opened_at_ms": 1794100000000,
        "closed_at_ms": 1794101000000,
        "is_mock": False,
    }
    try:
        await repo.persist_event(event)
        await repo.persist_event(event)
    finally:
        await client.aclose()

    trade_posts = [r for r in requests if r.url.path.endswith("/closed_trades")]
    position_upserts = [r for r in requests if r.url.path.endswith("/positions") and r.method == "POST"]
    assert len(trade_posts) == 2
    assert len(position_upserts) == 2
    trade = json.loads(trade_posts[0].content)
    assert trade["trade_key"] == "trade-abc-123"
    assert trade["fees"] == 0.02
    position = json.loads(position_upserts[0].content)
    assert position["status"] == "HEDGED_ACTIVE"
    assert position["qty"] == 0.006


@pytest.mark.asyncio
async def test_incomplete_force_close_pnl_is_not_written_as_realized_trade():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(204)

    repo, client = _repository(handler)
    event = {
        "event": "POSITION_CLOSED",
        "trade_id": "force-unverified",
        "pair_key": "indodax:btc_idr",
        "position_id": str(uuid.uuid4()),
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "symbol": "btc_idr",
        "qty": 0.01,
        "remaining_qty": 0,
        "pnl_complete": False,
        "pnl_unverified": True,
        "is_mock": False,
    }
    try:
        await repo.persist_event(event)
    finally:
        await client.aclose()

    assert not any(r.url.path.endswith("/closed_trades") for r in requests)
    assert any(r.url.path.endswith("/executor_events") for r in requests)
    position_upserts = [r for r in requests if r.url.path.endswith("/positions") and r.method == "POST"]
    assert len(position_upserts) == 1
    assert json.loads(position_upserts[0].content)["status"] == "CLOSED"


@pytest.mark.asyncio
async def test_execution_failure_is_written_to_risk_and_event_ledgers():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(201)

    repo, client = _repository(handler)
    event = {
        "event": "EXECUTION_FAILURE",
        "pair_tag": "indodax:btc_idr",
        "reason": "HEDGE_STATE_UNKNOWN",
        "message": "futures position could not be verified",
        "state": "RECONCILIATION_REQUIRED",
        "detail": {"symbol": "BTC/USDT:USDT"},
        "is_mock": False,
        "timestamp_ms": 1794101000000,
    }
    try:
        await repo.persist_event(event)
    finally:
        await client.aclose()

    risk = [r for r in requests if r.url.path.endswith("/risk_events")]
    events = [r for r in requests if r.url.path.endswith("/executor_events")]
    patches = [r for r in requests if r.url.path.endswith("/positions") and r.method == "PATCH"]
    assert len(risk) == len(events) == len(patches) == 1
    assert json.loads(risk[0].content)["severity"] == "critical"


@pytest.mark.asyncio
async def test_transient_server_error_retries_with_same_idempotent_position_id():
    attempts = {"count": 0}
    bodies = []

    def transient_handler(request):
        attempts["count"] += 1
        bodies.append(request.content)
        if attempts["count"] == 1:
            return httpx.Response(503, text="temporary database failover")
        return httpx.Response(201)

    repo, client = _repository(transient_handler, retries=2)
    event = {
        "event": "POSITION_OPENED",
        "position_id": str(uuid.uuid4()),
        "pair_key": "indodax:btc_idr",
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "symbol": "btc_idr",
        "qty": 0.01,
        "is_mock": True,
    }
    try:
        await repo.persist_event(event)
    finally:
        await client.aclose()
    assert attempts["count"] == 4
    assert bodies[0] == bodies[1]
    assert json.loads(bodies[1])["id"] == event["position_id"]


@pytest.mark.asyncio
async def test_non_retryable_http_error_is_reported_without_retry():
    attempts = {"count": 0}

    def handler(request):
        attempts["count"] += 1
        return httpx.Response(400, text="column does not exist")

    repo, client = _repository(handler, retries=3)
    event = {
        "event": "POSITION_OPENED",
        "position_id": str(uuid.uuid4()),
        "pair_key": "indodax:btc_idr",
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "symbol": "btc_idr",
        "qty": 0.01,
        "is_mock": True,
    }
    try:
        with pytest.raises(SupabasePersistenceError, match="HTTP 400"):
            await repo.persist_event(event)
    finally:
        await client.aclose()
    assert attempts["count"] == 1
