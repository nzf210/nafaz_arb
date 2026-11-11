"""Tests for the best-effort Supabase writer.

The writer must never raise into the trading path, must be idempotent on retry,
and must persist immutable exchange fills exactly once.
"""

from types import SimpleNamespace
import json

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
        rows = _drain(writer)
        row = next(row for table, _, row in rows if table == "positions")
        legs = [row for table, _, row in rows if table == "position_legs"]
        assert row["status"] == "CLOSED"
        assert row["closed_at"]
        assert len(legs) == 4
        assert {row["leg"] for row in legs} == {"spot", "perpetual"}
        assert all(row["status"] == "CLOSED" for row in legs)

    def test_is_mock_follows_config_not_position(self):
        writer = _writer()
        writer.position_opened("p", _pos(), None, is_mock=False)
        rows = _drain(writer)
        parent = next(row for table, _, row in rows if table == "positions")
        assert parent["is_mock"] is False

    def test_position_opened_persists_independent_spot_and_perpetual_legs(self):
        writer = _writer()
        pair = SimpleNamespace(local_symbol="BTC/IDR", global_symbol="BTC/USDT")
        writer.position_opened("indodax:btc_idr", _pos(), pair, is_mock=False)
        rows = _drain(writer)
        legs = [row for table, _, row in rows if table == "position_legs"]
        assert len(legs) == 2
        by_leg = {row["leg"]: row for row in legs}
        assert by_leg["spot"]["symbol"] == "BTC/IDR"
        assert by_leg["spot"]["side"] == "long"
        assert by_leg["perpetual"]["symbol"] == "BTC/USDT"
        assert by_leg["perpetual"]["side"] == "short"
        assert by_leg["spot"]["qty"] == 10.0
        assert by_leg["spot"]["position_id"] == by_leg["perpetual"]["position_id"]

    def test_confirmed_funding_payment_is_deduplicated(self):
        writer = _writer()
        pos = _pos()
        writer.position_opened("p", pos, None, is_mock=False)
        _drain(writer)
        payment = {
            "id": "funding-123",
            "timestamp": 1_700_000_000_000,
            "amount": 0.025,
            "code": "USDT",
            "symbol": "BTC/USDT:USDT",
            "rate": 0.0001,
        }
        assert writer.funding_payment("p", pos, payment, is_mock=False)
        assert writer.funding_payment("p", pos, payment, is_mock=False)
        rows = _drain(writer)
        funding = [row for table, _, row in rows if table == "funding_payments"]
        assert len(funding) == 1
        assert funding[0]["exchange_payment_id"] == "funding-123"
        assert funding[0]["amount"] == pytest.approx(0.025)
        assert funding[0]["asset"] == "USDT"
        assert funding[0]["payment_status"] == "confirmed"
        assert funding[0]["position_id"] == writer._position_ids["p"]

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

    def test_order_updates_do_not_invent_exchange_fills_from_cumulative_quantity(self):
        writer = _writer()
        writer.order(
            "p", "spot_entry", "indodax", "spot", "usdt_idr",
            {"id": "1", "amount": 10.0, "filled": 4.0, "status": "open"},
        )
        writer.order(
            "p", "spot_entry", "indodax", "spot", "usdt_idr",
            {"id": "1", "amount": 10.0, "filled": 7.0, "status": "open"},
        )
        rows = _drain(writer)
        assert len([row for row in rows if row[0] == "orders"]) == 2
        assert not [row for row in rows if row[0] == "order_fills"]

    def test_exchange_fill_persists_actual_trade_id_fee_and_quantity(self):
        writer = _writer()
        writer.order(
            "p", "spot_entry", "indodax", "spot", "btc_idr",
            {"id": "venue-order-1", "amount": 1.0, "filled": 0.25, "status": "closed"},
            client_order_id="client-order-1",
            is_mock=False,
        )
        order_id = writer.order_id_for("p", "indodax", "client-order-1")
        fill = {
            "id": "venue-trade-99",
            "amount": 0.25,
            "price": 100.0,
            "cost": 25.0,
            "timestamp": 1_700_000_000_000,
            "fee": {"cost": 0.01, "currency": "BTC"},
            "info": {"tradeId": "venue-trade-99"},
        }
        assert writer.order_fill(
            order_id=order_id, exchange="indodax", fill=fill, is_mock=False
        ) is True
        assert writer.order_fill(
            order_id=order_id, exchange="indodax", fill=fill, is_mock=False
        ) is True
        rows = _drain(writer)
        fills = [row for table, _, row in rows if table == "order_fills"]
        assert len(fills) == 1
        assert fills[0]["exchange_fill_id"] == "venue-trade-99"
        assert fills[0]["fill_source"] == "exchange_trade"
        assert fills[0]["qty"] == 0.25
        assert fills[0]["quote_qty"] == 25.0
        assert fills[0]["fee_amount"] == 0.01
        assert fills[0]["fee_asset"] == "BTC"
        assert fills[0]["fee_verified"] is True
        assert fills[0]["order_id"] == order_id

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

    def test_order_submission_timestamp_is_stable_across_status_updates(self):
        writer = _writer()
        submitted_at_ms = 1_700_000_000_000
        writer.order(
            "p", "spot_entry", "indodax", "spot", "btc_idr",
            {"id": "1", "amount": 1, "filled": 0, "status": "open"},
            submitted_at_ms=submitted_at_ms,
        )
        first = _drain(writer)[0][2]["submitted_at"]
        writer.order(
            "p", "spot_entry", "indodax", "spot", "btc_idr",
            {"id": "1", "amount": 1, "filled": 0, "status": "canceled"},
            submitted_at_ms=submitted_at_ms,
        )
        terminal = _drain(writer)[0][2]
        assert terminal["submitted_at"] == first
        assert terminal["completed_at"] is not None

    def test_completed_timestamp_is_not_cleared_by_later_open_snapshot(self):
        writer = _writer()
        writer.order(
            "p", "spot_entry", "indodax", "spot", "btc_idr",
            {"id": "1", "amount": 1, "filled": 0, "status": "canceled"},
            submitted_at_ms=1_700_000_000_000,
        )
        completed_at = _drain(writer)[0][2]["completed_at"]
        writer.order(
            "p", "spot_entry", "indodax", "spot", "btc_idr",
            {"id": "1", "amount": 1, "filled": 0, "status": "open"},
            submitted_at_ms=1_700_000_000_000,
        )
        assert _drain(writer)[0][2]["completed_at"] == completed_at

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

    def test_trade_fee_breakdown_is_saved_in_metadata(self):
        writer = _writer()
        trade = {
            **self.TRADE,
            "fees_usdt": 0.25,
            "spot_fees_usdt": 0.15,
            "futures_fees_usdt": 0.10,
            "fee_source": "EXCHANGE_API",
            "fee_verified": True,
        }
        writer.trade_closed("p", trade, _pos())
        row = _drain(writer)[0][2]
        assert row["fees"] == pytest.approx(0.25)
        assert row["metadata"] == {
            "pnl_complete": None,
            "spot_liquidation_failed": None,
            "spot_fees_usdt": pytest.approx(0.15),
            "futures_fees_usdt": pytest.approx(0.10),
            "fee_source": "EXCHANGE_API",
            "fee_verified": True,
            "fee_estimation": "four_fill_round_trip",
        }

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



class TestOrderRegistryWriter:
    """The registry row must describe the real order, not just its role."""

    def test_force_close_futures_buy_keeps_its_side(self):
        writer = _writer()
        writer.order_record(
            {
                "local_order_id": "11111111-1111-1111-1111-111111111111",
                "exchange_name": "binance",
                "market_type": "perpetual",
                "order_role": "FORCE_CLOSE",
                "exchange_order_id": "venue-force-close-1",
                "symbol": "BTCUSDT",
                "side": "buy",
                "order_type": "market",
                "status": "closed",
                "original_quantity": 1.0,
                "filled_quantity": 1.0,
                "pair_key": "indodax:btc_idr__binance:btc_usdt",
                "position_key": "position:indodax:btc_idr__binance:btc_usdt",
                "recovery_context": {"usdt_idr_rate": 16000.0},
            }
        )
        queued = _drain(writer)
        row = [r for table, _, r in queued if table == "order_registry"][0]
        analytics = [r for table, _, r in queued if table == "orders"][0]
        assert analytics["id"] == writer.order_id_for(
            "indodax:btc_idr__binance:btc_usdt", "binance", "venue-force-close-1"
        )
        assert analytics["leg"] == "futures_exit"
        assert analytics["market_type"] == "perpetual"
        assert analytics["is_mock"] is True
        assert row["side"] == "buy"
        assert row["market_type"] == "perpetual"
        assert row["leg"] == "futures_exit"
        assert row["pair_key"].startswith("indodax:btc_idr")
        assert row["position_key"] == "position:indodax:btc_idr__binance:btc_usdt"
        assert "usdt_idr_rate" in row["raw_payload"]

    def test_unlinked_orphan_row_omits_pair_and_position(self):
        writer = _writer()
        writer.order_record(
            {
                "local_order_id": "22222222-2222-2222-2222-222222222222",
                "exchange_name": "indodax",
                "order_role": "ENTRY",
                "symbol": "eth_idr",
                "side": "buy",
                "status": "open",
                "recovery_status": "ORPHAN_ORDER",
            }
        )
        row = [r for t, _, r in _drain(writer) if t == "order_registry"][0]
        assert "pair_key" not in row
        assert "position_key" not in row
        assert row["recovery_status"] == "ORPHAN_ORDER"


    def test_order_registry_persists_hedge_watermark_in_raw_payload(self):
        writer = _writer()
        writer.order_record(
            {
                "local_order_id": "33333333-3333-3333-3333-333333333333",
                "exchange_name": "indodax",
                "market_type": "spot",
                "order_role": "ENTRY",
                "symbol": "btc_idr",
                "side": "buy",
                "status": "open",
                "original_quantity": 0.01,
                "filled_quantity": 0.01,
                "hedged_quantity": 0.006,
                "pair_key": "indodax:btc_idr",
                "recovery_context": {"usdt_idr_rate": 16000.0},
            }
        )
        row = [r for t, _, r in _drain(writer) if t == "order_registry"][0]
        payload = json.loads(row["raw_payload"])
        assert payload["hedged_quantity"] == pytest.approx(0.006)


class TestBalanceSnapshots:
    def test_balance_snapshot_appends_valid_spot_and_perpetual_rows(self):
        writer = _writer()
        assert writer.balance_snapshot(
            exchange="Indodax",
            market_type="spot",
            asset="idr",
            total=1000,
            free=700,
            used=300,
            is_mock=False,
        ) is True
        assert writer.balance_snapshot(
            exchange="binance",
            market_type="perpetual",
            asset="usdt",
            total=10,
            free=8,
            used=2,
            is_mock=False,
        ) is True
        rows = _drain(writer)
        assert [row[0] for row in rows] == ["balance_snapshots", "balance_snapshots"]
        assert rows[0][2]["exchange_name"] == "indodax"
        assert rows[0][2]["market_type"] == "spot"
        assert rows[0][2]["asset"] == "IDR"
        assert rows[1][2]["market_type"] == "perpetual"
        assert rows[1][2]["total"] == 10
        assert "observed_at" in rows[0][2]

    def test_balance_snapshot_rejects_invalid_values_and_market(self):
        writer = _writer()
        assert writer.balance_snapshot(
            exchange="indodax", market_type="spot", asset="idr",
            total=-1, free=0, used=0,
        ) is False
        assert writer.balance_snapshot(
            exchange="indodax", market_type="margin", asset="idr",
            total=1, free=1, used=0,
        ) is False
        assert _drain(writer) == []

    def test_balance_snapshot_is_noop_when_disabled(self):
        writer = _writer(enabled=False)
        assert writer.balance_snapshot(
            exchange="indodax", market_type="spot", asset="idr",
            total=1, free=1, used=0,
        ) is False


class TestFillSyncWatermark:
    def test_fill_sync_watermark_survives_outbox_restart(self, tmp_path):
        path = str(tmp_path / "fill-sync.sqlite3")
        first = DurableOutbox(path)
        first.set_numeric_state("fill-sync:exchange:spot:order-1", 2.5)
        first.close()

        reopened = DurableOutbox(path)
        assert reopened.get_numeric_state("fill-sync:exchange:spot:order-1") == pytest.approx(2.5)
        reopened.close()
