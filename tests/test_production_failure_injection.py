"""Deterministic production-failure injection tests.

All exchange activity is mocked. These tests exercise process restarts,
transient venue failures, Redis disconnects, and Supabase delivery outages
without placing live orders or requiring production credentials.
"""

import asyncio
from types import SimpleNamespace

import pytest

from executor.order_registry import UniversalOrderRegistry
from executor.supabase import SupabaseWriter
from tests.test_order_recovery import (
    PAIR_TAG,
    _build,
    _entry_record,
    _venue_order,
)
from tests.conftest import MockExchangeClient


class _DurableReaderUnavailable:
    enabled = True

    async def fetch_open_order_records(self):
        raise ConnectionError("Supabase registry read timed out")


class _DisconnectedRedis:
    async def hkeys(self, *_args, **_kwargs):
        raise ConnectionError("Redis connection reset")

    async def get(self, *_args, **_kwargs):
        raise ConnectionError("Redis connection reset")

    async def hgetall(self, *_args, **_kwargs):
        raise ConnectionError("Redis connection reset")


class _HttpResponse:
    def __init__(self, status, body="injected response"):
        self.status = status
        self.body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def text(self):
        return self.body


class _HttpSession:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = 0

    def post(self, *_args, **_kwargs):
        self.calls += 1
        status = self.statuses.pop(0) if self.statuses else 201
        if isinstance(status, BaseException):
            raise status
        return _HttpResponse(status)


def _writer(path):
    return SupabaseWriter(
        SimpleNamespace(
            enabled=True,
            url="https://example.supabase.co",
            key="sb_secret_test",
            outbox_path=str(path),
        )
    )


@pytest.mark.asyncio
async def test_restart_replays_reconciliation_audit_from_sqlite_outbox(tmp_path):
    """An unacknowledged audit row survives process death and is not re-enqueued."""
    path = tmp_path / "failure-injection.sqlite3"
    first = _writer(path)
    first.event(
        "ORDER_RECONCILIATION_REQUIRED",
        pair_key=PAIR_TAG,
        payload={"local_order_id": "crash-order-1", "state": "RECONCILIATION_REQUIRED"},
        event_id="crash-audit-event-1",
    )
    assert first._outbox.pending_count() == 2  # event journal + reconciliation audit
    first._outbox.close()
    first._outbox = None

    restarted = _writer(path)
    restarted.event(
        "ORDER_RECONCILIATION_REQUIRED",
        pair_key=PAIR_TAG,
        payload={"local_order_id": "crash-order-1", "state": "RECONCILIATION_REQUIRED"},
        event_id="crash-audit-event-1",
    )

    audit_rows = restarted._outbox.pending_table("reconciliation_audit")
    assert len(audit_rows) == 1
    assert audit_rows[0][3]["event_id"] == "crash-audit-event-1"
    assert restarted._outbox.pending_count() == 3  # replayed event journal + old audit + no new audit
    restarted._outbox.close()


@pytest.mark.asyncio
async def test_supabase_timeout_and_server_failure_retain_outbox_for_retry(tmp_path, monkeypatch):
    """HTTP timeout/5xx never acknowledges rows; a later success drains them."""
    writer = _writer(tmp_path / "supabase-outage.sqlite3")
    writer.event(
        "ORDER_RECONCILIATION_REQUIRED",
        pair_key=PAIR_TAG,
        payload={"local_order_id": "outage-order-1"},
        event_id="outage-event-1",
    )
    audit = writer._outbox.pending_table("reconciliation_audit")
    assert len(audit) == 1

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_wait)
    writer._session = _HttpSession([
        asyncio.TimeoutError("injected Supabase request timeout"), 503, 503
    ])
    await writer._deliver_outbox_group(
        "reconciliation_audit", "event_id", [(audit[0][0], audit[0][3])]
    )
    assert writer._outbox.pending_count() == 2
    stored = writer._outbox.pending_table("reconciliation_audit")
    assert len(stored) == 1
    attempt_count = writer._outbox._db.execute(
        "SELECT attempts FROM pending_outbox WHERE id=?", (stored[0][0],)
    ).fetchone()[0]
    assert attempt_count == 1

    writer._session = _HttpSession([201])
    await writer._deliver_outbox_group(
        "reconciliation_audit", "event_id", [(stored[0][0], stored[0][3])]
    )
    assert writer._outbox.pending_table("reconciliation_audit") == []
    assert writer._outbox.pending_count() == 1  # executor_events remains independently queued
    writer._outbox.close()


@pytest.mark.asyncio
async def test_redis_disconnect_plus_durable_read_failure_is_not_a_clean_recovery():
    """Simultaneous cache and durable-store outages must fail closed."""
    registry = UniversalOrderRegistry(_DisconnectedRedis(), _DurableReaderUnavailable())

    with pytest.raises(ConnectionError, match="Supabase registry read timed out"):
        await registry.active_records()


@pytest.mark.asyncio
async def test_cancelled_partial_fill_is_hedged_once_across_repeated_restarts(fake_redis):
    """Repeated process reconstruction cannot hedge the same partial fill twice."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.004, status="canceled")
    first = await _build(fake_redis, client)
    record = _entry_record()
    await first.order_registry.register(record)

    for _restart in range(4):
        engine = await _build(fake_redis, client)
        await engine._run_order_recovery()

    assert len(client.global_short_sells) == 1
    assert client.global_short_sells[0]["amount"] == pytest.approx(0.004)
    recovered = await UniversalOrderRegistry(fake_redis).load(record.local_order_id)
    assert recovered.filled_quantity == pytest.approx(0.004)
    assert recovered.hedged_quantity == pytest.approx(0.004)
