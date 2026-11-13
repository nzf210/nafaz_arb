"""Tests for the best-effort Supabase writer.

The writer must never raise into the trading path, must be idempotent on retry,
and must derive fills from cumulative quantities exactly once.
"""

from types import SimpleNamespace

import pytest

from executor.models import ActivePosition, PairInfo
from executor.supabase import SupabaseWriter, _ms_to_iso
from executor.supabase_outbox import DurableOutbox, OutboxFull


def _writer(enabled: bool = True) -> SupabaseWriter:
    cfg = SimpleNamespace(
        enabled=enabled,
        url="https://example.supabase.co",
        key="sb_secret_test",
        outbox_path=":memory:",
    )
    return SupabaseWriter(cfg)


def _drain(writer: SupabaseWriter) -> list:
    if writer._outbox is None:
        return []
    pending = writer._outbox.pending(1000)
    writer._outbox.ack([item[0] for item in pending])
    return [(table, conflict, row) for _, table, conflict, row in pending]


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
        assert writer._outbox is None

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

    def test_multiple_events_are_durably_journaled(self):
        writer = _writer()
        writer.event("A", occurred_at_ms=1)
        writer.event("B", occurred_at_ms=2)
        assert writer._outbox.pending_count() == 2
        assert [row[2]["event_type"] for row in _drain(writer)] == ["A", "B"]
        assert writer._outbox.pending_count() == 0

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
        rows = [row for row in _drain(writer) if row[0] == "positions"]
        assert len(rows) == 2
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
        rows = [row for row in _drain(writer) if row[0] == "positions"]
        row = rows[-1][2]
        assert row["status"] == "CLOSED"
        assert row["closed_at"]

    def test_is_mock_follows_config_not_position(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None, is_mock=False)
        assert _drain(writer)[0][2]["is_mock"] is False


class TestOperationalTables:
    def test_position_legs_are_upserted_for_both_venues_and_status_changes(self):
        writer = _writer()
        pair_info = PairInfo(
            local_exchange="indodax",
            global_exchange="okx",
            local_symbol="BTC/IDR",
            global_symbol="BTC/USDT:USDT",
            base_asset="BTC",
            quote_asset="IDR",
        )
        writer.position_opened("pair-btc", _pos(symbol="BTC/IDR"), pair_info, is_mock=False)
        legs = [row for row in _drain(writer) if row[0] == "position_legs"]
        assert len(legs) == 2
        by_leg = {row[2]["leg"]: row for row in legs}
        assert by_leg["spot"][1] == "position_id,leg"
        assert by_leg["spot"][2]["side"] == "long"
        assert by_leg["spot"][2]["symbol"] == "BTC/IDR"
        assert by_leg["spot"][2]["average_entry_price"] == 18000.0
        assert by_leg["perpetual"][2]["side"] == "short"
        assert by_leg["perpetual"][2]["symbol"] == "BTC/USDT:USDT"
        assert by_leg["perpetual"][2]["average_entry_price"] == 1.0

        writer.position_status("pair-btc", "CLOSED", _pos(symbol="BTC/IDR", qty=0.0))
        closed = [row for row in _drain(writer) if row[0] == "position_legs"]
        assert len(closed) == 2
        assert all(row[2]["status"] == "CLOSED" for row in closed)
        assert all(row[2]["qty"] == 0.0 for row in closed)

    def test_confirmed_funding_payment_is_signed_and_idempotent(self):
        writer = _writer()
        pos = _pos(symbol="BTC/USDT:USDT")
        writer.position_opened("pair-btc", pos, None, is_mock=False)
        _drain(writer)
        payment = {
            "id": "funding-001",
            "symbol": "BTC/USDT:USDT",
            "timestamp": 1_700_000_000_000,
            "amount": -0.025,
            "code": "USDT",
            "rate": 0.0003,
            "contracts": 0.01,
            "price": 65000.0,
        }
        writer.funding_payment("pair-btc", pos, payment["symbol"], payment, is_mock=False)
        writer.funding_payment("pair-btc", pos, payment["symbol"], payment, is_mock=False)
        rows = [row for row in _drain(writer) if row[0] == "funding_payments"]
        assert len(rows) == 2
        assert rows[0][2]["id"] == rows[1][2]["id"]
        assert rows[0][2]["exchange_payment_id"] == "funding-001"
        assert rows[0][2]["payment_status"] == "confirmed"
        assert rows[0][2]["amount"] == -0.025
        assert rows[0][2]["asset"] == "USDT"
        assert rows[0][2]["position_qty"] == 0.01
        assert rows[0][2]["settled_at"].startswith("2023-11-14")
        assert rows[0][2]["raw_payload"]["is_mock"] is False

    def test_funding_payment_without_timestamp_or_amount_is_ignored(self):
        writer = _writer()
        writer.funding_payment("p", _pos(), "BTC/USDT:USDT", {"timestamp": 10}, is_mock=False)
        assert _drain(writer) == []

    def test_balance_snapshots_flatten_successful_venue_assets_and_are_idempotent(self):
        writer = _writer()
        reports = [
            {
                "name": "indodax", "market": "spot", "status": "ok",
                "assets": [{"asset": "idr", "free": 100.0, "used": 5.0, "total": 105.0}],
            },
            {
                "name": "mexc", "market": "futures", "status": "ok",
                "assets": [{"asset": "usdt", "free": 8.0, "used": 2.0, "total": 10.0}],
            },
            {"name": "okx", "market": "futures", "status": "error", "assets": []},
        ]
        assert writer.balance_snapshot(reports, is_mock=False, observed_at_ms=1234567890000) == 2
        first = [row for row in _drain(writer) if row[0] == "balance_snapshots"]
        assert len(first) == 2
        by_exchange = {row[2]["exchange_name"]: row[2] for row in first}
        assert by_exchange["indodax"]["market_type"] == "spot"
        assert by_exchange["indodax"]["asset"] == "IDR"
        assert by_exchange["mexc"]["market_type"] == "perpetual"
        assert by_exchange["mexc"]["free"] == 8.0
        assert all(row[2]["is_mock"] is False for row in first)

        writer.balance_snapshot(reports, is_mock=False, observed_at_ms=1234567890000)
        second = [row for row in _drain(writer) if row[0] == "balance_snapshots"]
        assert [row[2]["id"] for row in first] == [row[2]["id"] for row in second]

    def test_balance_snapshot_skips_invalid_values_instead_of_recording_zero(self):
        writer = _writer()
        reports = [{
            "name": "okx", "market": "futures", "status": "ok",
            "assets": [{"asset": "USDT", "free": -1, "used": 0, "total": 1}],
        }]
        assert writer.balance_snapshot(reports, observed_at_ms=1234567890000) == 0
        assert _drain(writer) == []

    def test_executor_status_snapshots_emit_on_change_and_minute_heartbeat(self, monkeypatch):
        clock = [100.0]
        monkeypatch.setattr("executor.supabase.time.monotonic", lambda: clock[0])
        writer = _writer()
        snapshot = {
            "status": "RUNNING",
            "mock_mode": True,
            "trading_halted": False,
            "timestamp_ms": 1_700_000_000_000,
        }
        assert writer.executor_status(snapshot, instance_id="test-executor") is True
        first = [row for row in _drain(writer) if row[0] == "executor_status_snapshots"]
        assert len(first) == 1
        assert first[0][2]["mode"] == "MOCK"
        assert first[0][2]["instance_id"] == "test-executor"

        unchanged = dict(snapshot, timestamp_ms=1_700_000_000_001)
        clock[0] += 0.5
        assert writer.executor_status(unchanged, instance_id="test-executor") is False
        assert _drain(writer) == []

        clock[0] += 61
        heartbeat = dict(unchanged, timestamp_ms=1_700_000_000_002)
        assert writer.executor_status(heartbeat, instance_id="test-executor") is True
        heartbeat_rows = [
            row for row in _drain(writer) if row[0] == "executor_status_snapshots"
        ]
        assert len(heartbeat_rows) == 1

        changed = dict(heartbeat, trading_halted=True)
        assert writer.executor_status(changed, instance_id="test-executor") is True
        changed_rows = [
            row for row in _drain(writer) if row[0] == "executor_status_snapshots"
        ]
        assert len(changed_rows) == 1
        assert changed_rows[0][2]["snapshot"]["trading_halted"] is True


class TestOrdersAndFills:
    def test_order_id_is_stable_across_writer_restarts(self):
        order = {"id": "exchange-order-7", "amount": 2.0, "filled": 0.0, "status": "open"}
        first = _writer()
        first.order("pair-a", "spot_entry", "indodax", "spot", "btc_idr", order)
        first_id = _drain(first)[0][2]["id"]

        restarted = _writer()
        restarted.order("pair-a", "spot_entry", "indodax", "spot", "btc_idr", order)
        restarted_id = _drain(restarted)[0][2]["id"]
        assert first_id == restarted_id

    def test_order_ids_are_scoped_to_pair_and_exchange(self):
        writer = _writer()
        order = {"id": "reused-client-id", "amount": 1.0, "filled": 0.0, "status": "open"}
        writer.order("pair-a", "spot_entry", "indodax", "spot", "btc_idr", order, client_order_id="same")
        writer.order("pair-b", "spot_entry", "indodax", "spot", "eth_idr", order, client_order_id="same")
        ids = [row[2]["id"] for row in _drain(writer) if row[0] == "orders"]
        assert len(ids) == 2
        assert ids[0] != ids[1]

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
        assert tables == [
            "closed_trades", "positions", "position_legs", "position_legs", "executor_events"
        ]
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
            {"id": "fill-4", "qty": 0, "price": 100.0, "quote_qty": 0},
            "okx:order-1", 4.0, 400.0,
        )
        first.close()

        reopened = DurableOutbox(path)
        delta = reopened.enqueue_cumulative_fill(
            "order_fills", "id",
            {"id": "fill-7", "qty": 0, "price": 105.0, "quote_qty": 0},
            "okx:order-1", 7.0, 735.0,
        )
        duplicate = reopened.enqueue_cumulative_fill(
            "order_fills", "id",
            {"id": "fill-7", "qty": 0, "price": 105.0, "quote_qty": 0},
            "okx:order-1", 7.0, 735.0,
        )
        pending = reopened.pending()
        assert delta == 3.0
        assert duplicate == 0.0
        assert [item[3]["qty"] for item in pending] == [4.0, 3.0]
        assert [item[3]["quote_qty"] for item in pending] == [400.0, 335.0]
        assert [item[3]["price"] for item in pending] == [100.0, pytest.approx(335.0 / 3.0)]
        reopened.close()

    def test_partial_fill_uses_incremental_cumulative_notional(self):
        writer = _writer()
        first = {
            "id": "order-1", "amount": 10.0, "filled": 4.0,
            "average": 100.0, "cost": 400.0, "status": "open",
        }
        second = {
            "id": "order-1", "amount": 10.0, "filled": 7.0,
            "average": 105.0, "cost": 735.0, "status": "open",
        }
        writer.order("pair-a", "spot_entry", "indodax", "spot", "btc_idr", first)
        writer.order("pair-a", "spot_entry", "indodax", "spot", "btc_idr", second)
        fills = [row for table, _, row in _drain(writer) if table == "order_fills"]
        assert [row["qty"] for row in fills] == [4.0, 3.0]
        assert [row["quote_qty"] for row in fills] == [400.0, 335.0]
        assert [row["price"] for row in fills] == [100.0, pytest.approx(335.0 / 3.0)]

    def test_fill_primary_keys_are_scoped_to_pair(self):
        writer = _writer()
        order = {
            "id": "reused-order-id", "amount": 1.0, "filled": 1.0,
            "average": 10.0, "cost": 10.0, "status": "closed",
        }
        writer.order("pair-a", "spot_entry", "indodax", "spot", "btc_idr", order)
        writer.order("pair-b", "spot_entry", "indodax", "spot", "eth_idr", order)
        fills = [row for table, _, row in _drain(writer) if table == "order_fills"]
        assert len(fills) == 2
        assert fills[0]["id"] != fills[1]["id"]
        assert fills[0]["exchange_fill_id"] != fills[1]["exchange_fill_id"]

class _FakeResponse:
    def __init__(self, status: int):
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def text(self):
        return "simulated response"


class _FakeSession:
    closed = False

    def __init__(self, status: int):
        self.status = status

    def post(self, *_args, **_kwargs):
        return _FakeResponse(self.status)


class TestOutboxDelivery:
    @pytest.mark.asyncio
    async def test_successful_upsert_acknowledges_durable_rows(self):
        writer = _writer()
        writer.event("PROBE", event_id="delivery-1")
        writer._session = _FakeSession(204)
        batch = writer._outbox.pending()
        await writer._flush(batch)
        assert writer._outbox.pending_count() == 0
        writer._outbox.close()

    @pytest.mark.asyncio
    async def test_permanent_http_error_retains_row_for_replay(self):
        writer = _writer()
        writer.event("PROBE", event_id="delivery-2")
        writer._session = _FakeSession(401)
        batch = writer._outbox.pending()
        await writer._flush(batch)
        assert writer._outbox.pending_count() == 1
        assert writer._outbox.pending() == []
        writer._outbox.close()

