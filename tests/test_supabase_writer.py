"""Tests for the best-effort Supabase writer.

The writer must never raise into the trading path, must be idempotent on retry,
and must derive fills from cumulative quantities exactly once.
"""

import asyncio
from types import SimpleNamespace

import pytest

from executor.models import ActivePosition
from executor.supabase import SupabaseWriter, _ms_to_iso


def _writer(enabled: bool = True) -> SupabaseWriter:
    cfg = SimpleNamespace(
        enabled=enabled,
        url="https://example.supabase.co",
        key="sb_secret_test",
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

    def test_queue_full_drops_instead_of_raising(self):
        writer = _writer()
        writer._queue = asyncio.Queue(maxsize=1)
        writer.event("A")
        writer.event("B")
        assert writer._dropped == 1

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
        position_rows = [row for table, _, row in rows if table == "positions"]
        assert len(position_rows) == 2
        assert position_rows[0]["id"] == position_rows[1]["id"]

    def test_status_update_requires_known_position(self):
        writer = _writer()
        writer.position_status("unknown:pair", "CLOSED", _pos())
        assert _drain(writer) == []

    def test_status_closed_sets_closed_at(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None)
        writer.position_status("p", "CLOSED", _pos(qty=0.0))
        rows = _drain(writer)
        row = [row for table, _, row in rows if table == "positions"][-1]
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

    def test_cumulative_filled_does_not_create_synthetic_fill(self):
        writer = _writer()
        writer.order(
            "p", "spot_entry", "indodax", "spot", "usdt_idr",
            {"id": "1", "amount": 10.0, "filled": 4.0, "status": "open"},
        )
        assert not [r for t, _, r in _drain(writer) if t == "order_fills"]

    def test_exchange_trade_fill_is_idempotent_across_writer_restarts(self):
        order = {
            "id": "1", "amount": 10.0, "filled": 4.0, "status": "open",
            "trades": [{"id": "trade-77", "amount": 4.0, "price": 100.0,
                        "cost": 400.0, "timestamp": 1_700_000_000_000,
                        "fee": {"cost": 0.4, "currency": "USDT"}}],
        }
        first = _writer()
        first.order("p", "spot_entry", "indodax", "spot", "BTC/USDT", order, client_order_id="coid-1")
        first_rows = [r for t, _, r in _drain(first) if t == "order_fills"]
        second = _writer()
        second.order("p", "spot_entry", "indodax", "spot", "BTC/USDT", order, client_order_id="coid-1")
        second_rows = [r for t, _, r in _drain(second) if t == "order_fills"]
        assert len(first_rows) == len(second_rows) == 1
        assert first_rows[0]["id"] == second_rows[0]["id"]
        assert first_rows[0]["exchange_fill_id"] == "trade-77"
        assert first_rows[0]["qty"] == 4.0

    def test_order_id_is_stable_across_writer_restarts(self):
        order = {"id": "venue-order-1", "amount": 10.0, "filled": 0.0, "status": "open"}
        first = _writer()
        first.order("p", "spot_entry", "indodax", "spot", "BTC/IDR", order, client_order_id="stable-coid")
        first_id = [r for t, _, r in _drain(first) if t == "orders"][0]["id"]
        second = _writer()
        second.order("p", "spot_entry", "indodax", "spot", "BTC/IDR", order, client_order_id="stable-coid")
        second_id = [r for t, _, r in _drain(second) if t == "orders"][0]["id"]
        assert first_id == second_id

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
        assert tables == ["closed_trades", "positions", "position_legs", "position_legs", "executor_events"]
        assert rows[0][1] == "trade_key"

    def test_trade_without_id_ignored(self):
        writer = _writer()
        writer.trade_closed("p", {"net_profit_usdt": 1.0}, None)
        assert _drain(writer) == []


class TestOperationalSnapshots:
    def test_balance_risk_and_funding_rows_match_schema(self):
        writer = _writer()
        writer.balance_snapshot(
            "okx", "perpetual", "USDT", 100, 80, 20,
            is_mock=False, observed_at_ms=1234,
        )
        writer.risk_event(
            "BALANCE_FEED_UNAVAILABLE", "balance API timeout",
            pair_key="indodax:btc_idr", exchange_name="okx",
            severity="critical", details={"code": "timeout"},
            is_mock=False, occurred_at_ms=1234,
        )
        writer.position_opened("p", _pos(), SimpleNamespace(
            local_symbol="BTC/IDR", global_symbol="BTC/USDT:USDT",
            base_asset="BTC", quote_asset="IDR",
        ), is_mock=False)
        writer.funding_payment(
            "p", "okx", "BTC/USDT:USDT",
            {"id": "funding-1", "timestamp": 1_700_000_000_000, "amount": 0.02, "code": "USDT"},
            amount=0.02, is_mock=False,
        )
        rows = _drain(writer)
        by_table = {}
        for table, _, row in rows:
            by_table.setdefault(table, []).append(row)
        assert by_table["balance_snapshots"][0]["market_type"] == "perpetual"
        assert by_table["balance_snapshots"][0]["free"] == 80
        assert by_table["risk_events"][0]["severity"] == "critical"
        assert by_table["risk_events"][0]["event_key"]
        assert by_table["funding_payments"][0]["exchange_payment_id"] == "funding-1"
        assert len(by_table["position_legs"]) == 2
        assert {row["leg"] for row in by_table["position_legs"]} == {"spot", "perpetual"}

    @pytest.mark.asyncio
    async def test_worker_marks_queue_tasks_done_after_unexpected_flush_error(self):
        writer = _writer()
        async def fail_flush(_batch):
            raise RuntimeError("simulated persistence failure")
        writer._flush = fail_flush
        await writer.start()
        writer.event("TEST")
        await asyncio.wait_for(writer._queue.join(), timeout=1.0)
        await writer.close()
