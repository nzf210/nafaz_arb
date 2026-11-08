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
        rows = [r for r in _drain(writer) if r[0] == "positions"]
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
        row = [r for r in _drain(writer) if r[0] == "positions"][-1][2]
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
        tables = [t for t, _, _ in rows if t != "position_legs"]
        assert tables == ["closed_trades", "positions", "executor_events"]
        assert rows[0][1] == "trade_key"

    def test_trade_without_id_ignored(self):
        writer = _writer()
        writer.trade_closed("p", {"net_profit_usdt": 1.0}, None)
        assert _drain(writer) == []

class TestPositionLegs:
    def test_open_emits_both_legs(self):
        writer = _writer()
        writer.position_opened("indodax:usdt_idr", _pos(), None)
        legs = [r[2] for r in _drain(writer) if r[0] == "position_legs"]
        assert {leg["leg"] for leg in legs} == {"spot", "perpetual"}
        sides = {leg["leg"]: leg["side"] for leg in legs}
        assert sides == {"spot": "long", "perpetual": "short"}

    def test_leg_ids_are_stable_across_updates(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None)
        first = {r[2]["leg"]: r[2]["id"] for r in _drain(writer) if r[0] == "position_legs"}
        writer.position_status("p", "EXIT_PENDING", _pos(qty=4.0))
        second = {r[2]["leg"]: r[2]["id"] for r in _drain(writer) if r[0] == "position_legs"}
        assert first == second

    def test_status_propagates_to_legs(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None)
        _drain(writer)
        writer.position_status("p", "CLOSED", _pos(qty=0.0))
        legs = [r[2] for r in _drain(writer) if r[0] == "position_legs"]
        assert all(leg["status"] == "CLOSED" for leg in legs)


class TestFundingPayments:
    def test_payment_id_is_deterministic(self):
        writer = _writer()
        payment = {"id": "pay-1", "amount": 0.5, "fundingRate": 0.0001}
        writer.funding_payment("p", "okx", "BTC-USDT-SWAP", payment, 1_700_000_000_000)
        writer.funding_payment("p", "okx", "BTC-USDT-SWAP", payment, 1_700_000_000_000)
        rows = _drain(writer)
        assert rows[0][2]["id"] == rows[1][2]["id"]
        assert rows[0][1] == "id"

    def test_missing_payment_id_falls_back_to_settle_time(self):
        writer = _writer()
        writer.funding_payment(
            "p", "okx", "BTC-USDT-SWAP", {"amount": 1.0}, 1_700_000_000_000
        )
        row = _drain(writer)[0][2]
        assert row["exchange_payment_id"] == "BTC-USDT-SWAP:1700000000000"
        assert row["payment_status"] == "confirmed"

    def test_non_dict_payment_ignored(self):
        writer = _writer()
        writer.funding_payment("p", "okx", "s", None, 1)
        assert _drain(writer) == []


class TestBalanceSnapshots:
    REPORTS = [
        {"name": "indodax", "market": "spot", "assets": [
            {"asset": "usdt", "free": 1.0, "used": 2.0, "total": 3.0}]},
        {"name": "okx", "market": "futures", "assets": [
            {"asset": "USDT", "free": 4.0, "used": 0.0, "total": 4.0}]},
    ]

    def test_non_perpetual_market_is_skipped(self):
        writer = _writer()
        writer.balance_snapshot(self.REPORTS)
        rows = _drain(writer)
        assert len(rows) == 1
        assert rows[0][2]["exchange_name"] == "indodax"
        assert rows[0][2]["asset"] == "USDT"

    def test_invalid_report_ignored(self):
        writer = _writer()
        writer.balance_snapshot(None)
        writer.balance_snapshot({"not": "a list"})
        assert _drain(writer) == []


class TestStatusSnapshots:
    def test_snapshot_row_shape(self):
        writer = _writer()
        writer.status_snapshot("exec-1", {"status": "RUNNING"}, is_mock=False)
        row = _drain(writer)[0][2]
        assert row["instance_id"] == "exec-1"
        assert row["status"] == "RUNNING"
        assert row["mode"] == "LIVE"
        assert row["is_mock"] is False
        assert row["snapshot"] == {"status": "RUNNING"}


class TestRiskEvents:
    def test_event_key_conflict_target(self):
        writer = _writer()
        writer.risk_event("k1", "critical", "HEDGE_FAILED", "boom", pair_key="p")
        table, on_conflict, row = _drain(writer)[0]
        assert table == "risk_events"
        assert on_conflict == "event_key"
        assert row["severity"] == "critical"
        assert row["message"] == "boom"

    def test_unknown_severity_becomes_error(self):
        writer = _writer()
        writer.risk_event("k2", "bogus", "T", "m")
        assert _drain(writer)[0][2]["severity"] == "error"
