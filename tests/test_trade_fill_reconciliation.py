from types import SimpleNamespace

import pytest

from executor.trade_fill_reconciliation import TradeFillReconciliationWorker
from executor.models import PositionState


class FakeWriter:
    enabled = True

    def __init__(self, candidates):
        self.candidates = candidates
        self.fills = {}
        self.watermarks = {}
        self.parent_orders = []

    def order(self, *args, **kwargs):
        self.parent_orders.append((args, kwargs))
        return True

    def fill_sync_watermark(self, identity):
        return self.watermarks.get(identity)

    def mark_fill_sync(self, identity, cumulative_filled):
        self.watermarks[identity] = float(cumulative_filled)
        return True

    async def fetch_recent_order_fill_candidates(self, days=90, limit=5000):
        return self.candidates

    def order_id_for(self, pair_key, exchange, key):
        return f"order-row:{exchange}:{pair_key}:{key}"

    def order_fill(self, *, order_id, exchange, fill, is_mock=False):
        fill_id = (exchange, fill["id"])
        if fill_id in self.fills:
            return True
        self.fills[fill_id] = {
            "order_id": order_id, "exchange": exchange, "fill": fill,
            "is_mock": is_mock,
        }
        return True


class FakeClient:
    def __init__(self, trades):
        self.trades = trades
        self.calls = []

    async def fetch_order_trades(self, market_type, symbol, **kwargs):
        self.calls.append((market_type, symbol, kwargs))
        return self.trades


class FakeEngine:
    def __init__(self, candidates, trades):
        self.config = SimpleNamespace(mock_mode=False)
        self.supabase = FakeWriter(candidates)
        self.client_for_exchange = FakeClient(trades)
        self.order_registry = SimpleNamespace(records={})
        self._recovered_order_records = {}
        self._recovery_scope_halts = {}
        self.failures = []

    def _pair_info_for_tag(self, _pair_key):
        raise LookupError("pair registry not loaded in this isolated test")

    def _bind_pair_client(self, _pair_info):
        raise LookupError("no pair client")

    def _client_for_exchange(self, _exchange):
        return self.client_for_exchange

    def _reset_pair_client(self, _token):
        return None

    def _halt_exposure_scope(self, scope, reason):
        self._recovery_scope_halts[scope] = reason

    async def _publish_execution_failure(self, *args, **kwargs):
        self.failures.append((args, kwargs))

    async def _clear_exposure_halt(self, scope, _reason):
        self._recovery_scope_halts.pop(scope, None)


def _candidate(**overrides):
    row = {
        "id": "00000000-0000-0000-0000-000000000001",
        "pair_key": "indodax:BTC_IDR:okx:BTC_USDT",
        "exchange_name": "indodax",
        "market_type": "spot",
        "leg": "spot_entry",
        "exchange_order_id": "order-1",
        "client_order_id": "client-1",
        "symbol": "btc_idr",
        "side": "buy",
        "filled_qty": 0.5,
        "submitted_at": 1_700_000_000_000,
        "is_mock": False,
    }
    row.update(overrides)
    return row


def _trade(**overrides):
    row = {
        "id": "trade-1",
        "amount": 0.5,
        "price": 100.0,
        "cost": 50.0,
        "timestamp": 1_700_000_000_100,
        "fee": {"cost": 0.05, "currency": "IDR"},
        "info": {"tradeId": "trade-1"},
    }
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_reconciles_and_journals_real_trade_id_and_fee():
    engine = FakeEngine([_candidate()], [_trade()])
    worker = TradeFillReconciliationWorker(engine)

    await worker.reconcile_once()
    await worker.reconcile_once()

    assert len(engine.supabase.fills) == 1
    saved = next(iter(engine.supabase.fills.values()))
    assert saved["fill"]["id"] == "trade-1"
    assert saved["fill"]["fee"]["cost"] == pytest.approx(0.05)
    assert saved["order_id"] == "00000000-0000-0000-0000-000000000001"
    assert engine.failures == []
    assert len(engine.client_for_exchange.calls) == 1
    assert engine.supabase.watermarks["indodax:spot:client-1"] == pytest.approx(0.5)


@pytest.mark.asyncio
async def test_mismatched_cumulative_quantity_halts_affected_pair():
    engine = FakeEngine([_candidate()], [_trade(amount=0.2)])
    worker = TradeFillReconciliationWorker(engine)

    await worker.reconcile_once()

    scope = "indodax:BTC_IDR:okx:BTC_USDT"
    assert "trade-fill reconciliation:" in engine._recovery_scope_halts[scope]
    assert len(engine.failures) == 1
    assert engine.failures[0][1]["detail"]["order_filled_qty"] == pytest.approx(0.5)
    assert engine.failures[0][1]["detail"]["trade_fill_qty"] == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_filled_order_without_trade_history_fails_closed():
    engine = FakeEngine([_candidate()], [])
    worker = TradeFillReconciliationWorker(engine)

    await worker.reconcile_once()

    assert engine._recovery_scope_halts
    assert engine.failures[0][1]["state"] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_mock_orders_are_never_sent_to_exchange_history():
    engine = FakeEngine([_candidate(is_mock=True)], [_trade()])
    worker = TradeFillReconciliationWorker(engine)

    await worker.reconcile_once()

    assert engine.client_for_exchange.calls == []
    assert engine.supabase.fills == {}


@pytest.mark.asyncio
async def test_pair_halt_remains_until_all_mismatched_orders_are_verified():
    engine = FakeEngine([], [])
    worker = TradeFillReconciliationWorker(engine)
    scope = "indodax:BTC_IDR:okx:BTC_USDT"
    engine._recovery_scope_halts[scope] = "order recovery: trade-fill reconciliation: mismatch"
    worker._reported = {"order-1": scope, "order-2": scope}

    worker._clear("order-1")
    await worker._flush_clears()
    assert scope in engine._recovery_scope_halts

    worker._clear("order-2")
    await worker._flush_clears()
    assert scope not in engine._recovery_scope_halts


@pytest.mark.asyncio
async def test_mismatch_is_retried_until_exchange_trade_history_matches():
    engine = FakeEngine([_candidate()], [_trade(amount=0.2)])
    worker = TradeFillReconciliationWorker(engine)
    await worker.reconcile_once()

    assert engine.supabase.watermarks == {}
    engine.client_for_exchange.trades = [
        _trade(amount=0.2),
        _trade(id="trade-2", amount=0.3, cost=30.0, timestamp=1_700_000_000_200),
    ]
    await worker.reconcile_once()

    assert engine.supabase.watermarks["indodax:spot:client-1"] == pytest.approx(0.5)
    assert "indodax:BTC_IDR:okx:BTC_USDT" not in engine._recovery_scope_halts


@pytest.mark.asyncio
async def test_registry_only_candidate_recreates_parent_order_before_fills():
    engine = FakeEngine(
        [_candidate(
            id=None,
            requested_qty=0.5,
            status="closed",
            leg="spot_entry",
            reduce_only=False,
        )],
        [_trade()],
    )
    worker = TradeFillReconciliationWorker(engine)

    await worker.reconcile_once()

    assert len(engine.supabase.parent_orders) == 1
    parent_args, parent_kwargs = engine.supabase.parent_orders[0]
    assert parent_args[0] == "indodax:BTC_IDR:okx:BTC_USDT"
    assert parent_args[1] == "spot_entry"
    assert parent_kwargs["is_mock"] is False
    saved = next(iter(engine.supabase.fills.values()))
    assert saved["order_id"].startswith("order-row:")


@pytest.mark.asyncio
async def test_fills_with_missing_fee_details_are_not_marked_reconciled():
    engine = FakeEngine([_candidate()], [_trade(fee=None)])
    worker = TradeFillReconciliationWorker(engine)

    await worker.reconcile_once()

    scope = "indodax:BTC_IDR:okx:BTC_USDT"
    assert "trade-fill reconciliation:" in engine._recovery_scope_halts[scope]
    assert engine.failures[0][0][1] == "TRADE_FILL_RECONCILIATION_REQUIRED"
    assert engine.supabase.watermarks == {}


@pytest.mark.asyncio
async def test_durable_fill_ledger_read_failure_blocks_live_recovery():
    engine = FakeEngine([], [])

    async def unavailable(**_kwargs):
        raise RuntimeError("Supabase read timeout")

    engine.supabase.fetch_recent_order_fill_candidates = unavailable
    worker = TradeFillReconciliationWorker(engine)

    with pytest.raises(RuntimeError, match="Supabase read timeout"):
        await worker.reconcile_once()

    assert "trade-fill reconciliation:" in engine._recovery_scope_halts["*"]
    assert engine.failures
