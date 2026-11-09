"""Tests for the best-effort Supabase writer.

The writer must never raise into the trading path, must be idempotent on retry,
and must derive fills from cumulative quantities exactly once.
"""

import asyncio
from types import SimpleNamespace

import pytest

from executor.models import ActivePosition
from executor.supabase import SupabaseWriter, _ms_to_iso
from executor.supabase_outbox import DurableOutbox, OutboxFull


def _writer(enabled: bool = True) -> SupabaseWriter:
    cfg = SimpleNamespace(
        enabled=enabled,
        url="https://example.supabase.co",
        key="sb_secret_test",
        outbox_path=":memory:",
    )
    writer = SupabaseWriter(cfg)
    if enabled:
        writer._queue = asyncio.Queue(maxsize=512)
    return writer


def _drain(writer: SupabaseWriter) -> list:
    items = []
    while not writer._queue.empty():
        items.append(writer._queue.get_nowait())
    return items


def _pos(**overrides) -> ActivePosition:
    data = dict(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="usdt_idr",
        entry_price_idr=18000.0,
        short_entry_usdt=1.0,
        qty=10.0,
        initial_qty=10.0,
        opened_at_ms=1_700_000_000_000,
    )
    data.update(overrides)
    return ActivePosition(**data)


class TestDisabledWriter:
    def test_emit_is_noop_when_disabled(self):
        writer = _writer(enabled=False)
        writer.position_opened("indodax:usdt_idr", _pos(), None)
        writer.event("PROBE")
        assert writer._queue is None

    def test_missing_credentials_disables(self):
        writer = SupabaseWriter(SimpleNamespace(enabled=True, url="", key="k"))
        assert writer.enabled is False

    @pytest.mark.asyncio
    async def test_start_noop_when_disabled(self):
        writer = _writer(enabled=False)
        await writer.start()
        assert writer._task is None


class TestEventQueue:
    def test_event_has_deterministic_id(self):
        writer = _writer()
        writer.event(
            "EXECUTION_FAILURE", pair_key="indodax:usdt_idr", occurred_at_ms=1234
        )
        table, on_conflict, row = _drain(writer)[0]
        assert table == "executor_events"
        assert on_conflict == "event_id"
        assert row["event_id"] == "EXECUTION_FAILURE:indodax:usdt_idr:1234"
        assert row["is_mock"] is True

    def test_explicit_event_id_wins(self):
        writer = _writer()
        writer.event("PROBE", event_id="fixed-id")
        assert _drain(writer)[0][2]["event_id"] == "fixed-id"

    def test_execution_failure_is_written_to_deduplicated_risk_ledger(self):
        writer = _writer()
        writer.event(
            "EXECUTION_FAILURE",
            pair_key="indodax:btc_idr",
            payload={
                "reason": "HEDGE_STATE_UNKNOWN",
                "message": "futures position could not be verified",
                "state": "RECONCILIATION_REQUIRED",
            },
            is_mock=False,
            occurred_at_ms=1234,
        )

        rows = _drain(writer)
        risk_rows = [row for table, _, row in rows if table == "risk_events"]
        assert len(risk_rows) == 1
        risk = risk_rows[0]
        assert risk["event_key"] == "EXECUTION_FAILURE:indodax:btc_idr:1234"
        assert risk["severity"] == "critical"
        assert risk["message"] == "futures position could not be verified"
        assert risk["is_mock"] is False
        assert risk["details"]["reason"] == "HEDGE_STATE_UNKNOWN"

    def test_full_wakeup_queue_does_not_drop_durable_rows(self):
        writer = _writer()
        writer._queue = asyncio.Queue(maxsize=1)
        writer.event("A")
        writer.event("B")
        assert writer._dropped == 1  # only the in-memory wakeup was dropped
        assert writer._outbox.pending_count() == 2

    def test_ms_to_iso_rejects_invalid(self):
        assert _ms_to_iso(0) is None
        assert _ms_to_iso("x") is None
        assert _ms_to_iso(1_700_000_000_000).startswith("2023-11-14")


class TestPositions:
    def test_position_id_is_stable_across_calls(self):
        writer = _writer()
        pos = _pos()
        writer.position_opened("indodax:usdt_idr", pos, None)
        writer.position_opened("indodax:usdt_idr", pos, None)
        rows = _drain(writer)
        assert rows[0][2]["id"] == rows[1][2]["id"]
        assert rows[0][1] == "id"

    def test_status_update_requires_known_position(self):
        writer = _writer()
        writer.position_status("unknown:pair", "CLOSED", _pos())
        assert _drain(writer) == []

    def test_status_closed_sets_closed_at(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None)
        writer.position_status("p", "CLOSED", _pos(qty=0.0))
        row = _drain(writer)[1][2]
        assert row["status"] == "CLOSED"
        assert row["closed_at"]

    def test_is_mock_follows_config_not_position(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None, is_mock=False)
        assert _drain(writer)[0][2]["is_mock"] is False


class TestOrdersAndFills:
    def test_order_upsert_uses_client_order_id_key(self):
        writer = _writer()
        writer.order(
            "p",
            "spot_entry",
            "indodax",
            "spot",
            "usdt_idr",
            {"id": "1", "amount": 10.0, "filled": 0.0, "status": "open"},
            client_order_id="coid-1",
        )
        table, on_conflict, row = _drain(writer)[0]
        assert (table, on_conflict) == ("orders", "id")
        assert row["side"] == "buy"
        assert row["leg"] == "spot_entry"
        assert row["remaining_qty"] == 10.0

    def test_fill_emitted_once_per_cumulative_quantity(self):
        writer = _writer()
        order = {"id": "1", "amount": 10.0, "filled": 4.0, "status": "open"}
        writer.order("p", "spot_entry", "indodax", "spot", "usdt_idr", order)
        writer.order("p", "spot_entry", "indodax", "spot", "usdt_idr", order)
        fills = [r for t, _, r in _drain(writer) if t == "order_fills"]
        assert len(fills) == 1
        assert fills[0]["qty"] == 4.0

    def test_second_partial_fill_has_new_id_and_delta(self):
        writer = _writer()
        writer.order(
            "p", "spot_entry", "indodax", "spot", "usdt_idr",
            {"id": "1", "amount": 10.0, "filled": 4.0, "status": "open"},
        )
        writer.order(
            "p", "spot_entry", "indodax", "spot", "usdt_idr",
            {"id": "1", "amount": 10.0, "filled": 7.0, "status": "open"},
        )
        fills = [r for t, _, r in _drain(writer) if t == "order_fills"]
        assert [f["qty"] for f in fills] == [4.0, 3.0]
        assert fills[0]["exchange_fill_id"] != fills[1]["exchange_fill_id"]

    def test_side_mapping_per_leg(self):
        writer = _writer()
        for leg, expected in (
            ("spot_exit", "sell"),
            ("futures_entry", "sell"),
            ("futures_exit", "buy"),
            ("rollback", "sell"),
        ):
            writer.order("p", leg, "x", "spot", "s", {"id": leg, "amount": 1, "filled": 1})
        sides = {r["leg"]: r["side"] for t, _, r in _drain(writer) if t == "orders"}
        assert sides == {
            "spot_exit": "sell",
            "futures_entry": "sell",
            "futures_exit": "buy",
            "rollback": "sell",
        }

    def test_terminal_order_gets_completed_at(self):
        writer = _writer()
        writer.order(
            "p", "spot_entry", "x", "spot", "s",
            {"id": "1", "amount": 1, "filled": 1, "status": "closed"},
        )
        assert _drain(writer)[0][2]["completed_at"]

    def test_malformed_order_ignored(self):
        writer = _writer()
        writer.order("p", "spot_entry", "x", "spot", "s", None)
        writer.order("p", "spot_entry", "x", "spot", "s", {})
        assert _drain(writer) == []


class TestClosedTrades:
    TRADE = {
        "trade_id": "trade-1",
        "local_exchange": "indodax",
        "global_exchange": "okx",
        "symbol": "usdt_idr",
        "qty": 10.0,
        "entry_price_idr": 18000.0,
        "exit_price_idr": 18100.0,
        "short_entry_usdt": 1.0,
        "short_exit_usdt": 0.99,
        "spot_pnl_idr": 1000.0,
        "futures_pnl_usdt": 0.1,
        "accumulated_funding_usdt": 0.01,
        "net_profit_usdt": 0.1,
        "net_profit_idr": 1800.0,
        "net_roi_pct": 1.0,
        "is_mock": True,
        "closed_at_ms": 1_700_000_000_000,
    }

    def test_trade_upsert_and_event_and_position_close(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None)
        _drain(writer)
        writer.trade_closed("p", self.TRADE, _pos(qty=0.0))
        rows = _drain(writer)
        tables = [t for t, _, _ in rows]
        assert tables == ["closed_trades", "positions", "executor_events"]
        assert rows[0][1] == "trade_key"

    def test_trade_without_id_ignored(self):
        writer = _writer()
        writer.trade_closed("p", {"net_profit_usdt": 1.0}, None)
        assert _drain(writer) == []

class TestDurableOutbox:
    def test_pending_rows_survive_reopen_and_ack(self, tmp_path):
        path = str(tmp_path / "outbox.sqlite3")
        first = DurableOutbox(path)
        first.enqueue("executor_events", "event_id", {"event_id": "evt-1"})
        first.close()

        reopened = DurableOutbox(path)
        pending = reopened.pending()
        assert len(pending) == 1
        assert pending[0][1:] == (
            "executor_events", "event_id", {"event_id": "evt-1"}
        )
        reopened.ack([pending[0][0]])
        assert reopened.pending_count() == 0
        reopened.close()

    def test_retry_retains_row_for_reconciliation(self):
        outbox = DurableOutbox(":memory:")
        row_id = outbox.enqueue("risk_events", "event_key", {"event_key": "risk-1"})
        outbox.retry([row_id], "HTTP 503")
        assert outbox.pending_count() == 1
        assert outbox.pending() == []
        outbox.close()

    def test_capacity_failure_is_explicit(self):
        outbox = DurableOutbox(":memory:", max_pending=1)
        outbox.enqueue("executor_events", "event_id", {"event_id": "one"})
        with pytest.raises(OutboxFull):
            outbox.enqueue("executor_events", "event_id", {"event_id": "two"})
        assert outbox.pending_count() == 1
        outbox.close()

    def test_fill_watermark_and_outbox_insert_are_atomic_across_restart(self, tmp_path):
        path = str(tmp_path / "fills.sqlite3")
        first = DurableOutbox(path)
        first.enqueue_cumulative_fill(
            "order_fills", "id",
            {"id": "fill-4", "qty": 0, "price": 2.0, "quote_qty": 0},
            "okx:order-1", 4.0,
        )
        first.close()

        reopened = DurableOutbox(path)
        delta = reopened.enqueue_cumulative_fill(
            "order_fills", "id",
            {"id": "fill-7", "qty": 0, "price": 2.0, "quote_qty": 0},
            "okx:order-1", 7.0,
        )
        duplicate = reopened.enqueue_cumulative_fill(
            "order_fills", "id",
            {"id": "fill-7", "qty": 0, "price": 2.0, "quote_qty": 0},
            "okx:order-1", 7.0,
        )
        pending = reopened.pending()
        assert delta == 3.0
        assert duplicate == 0.0
        assert [item[3]["qty"] for item in pending] == [4.0, 3.0]
        assert [item[3]["quote_qty"] for item in pending] == [8.0, 6.0]
        reopened.close()

