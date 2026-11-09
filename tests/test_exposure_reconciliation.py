import asyncio
from types import SimpleNamespace

import pytest

from executor.exposure_reconciliation import (
    ExposureReconciliationWorker,
    _position_short_qty,
)


def test_position_short_qty_accepts_short_contracts():
    assert _position_short_qty([
        {"side": "short", "contracts": 1.25, "contractSize": 1},
        {"side": "short", "contracts": 0.75, "contractSize": 1},
    ]) == pytest.approx(2.0)


@pytest.mark.parametrize(
    "positions",
    [
        [{"side": "long", "contracts": 1}],
        [{"positionAmt": 1}],
        [{"side": "short", "contracts": float("nan")}],
        [None],
    ],
)
def test_position_short_qty_rejects_ambiguous_or_invalid_exposure(positions):
    with pytest.raises((ValueError, TypeError)):
        _position_short_qty(positions)


class FakeClient:
    def __init__(self):
        self.order_calls = 0

    async def fetch_balance_report(self):
        return [
            {
                "name": "indodax", "market": "spot", "status": "ok",
                "assets": [{"asset": "BTC", "free": 0.5, "used": 0.0, "total": 0.5}],
            },
            {"name": "okx", "market": "futures", "status": "ok", "assets": []},
        ]

    async def fetch_positions(self, symbol):
        return [{"symbol": symbol, "side": "short", "contracts": 0.5, "contractSize": 1}]

    async def place_local_limit_buy(self, *args, **kwargs):
        self.order_calls += 1

    async def open_global_short_market(self, *args, **kwargs):
        self.order_calls += 1


class FakeSupabase:
    def __init__(self):
        self.rows = []

    def _emit(self, table, row):
        self.rows.append((table, row))


@pytest.mark.asyncio
async def test_reconciliation_halts_new_entries_on_confirmed_mismatch_without_orders(monkeypatch):
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "60")
    client = FakeClient()
    position = SimpleNamespace(qty=1.0)
    pair_info = SimpleNamespace(
        local_exchange="indodax",
        global_exchange="okx",
        base_asset="BTC",
        global_symbol="BTC/USDT:USDT",
    )
    engine = SimpleNamespace(
        config=SimpleNamespace(mock_mode=False),
        client=client,
        active_positions={"indodax:BTC_IDR:okx:BTC_USDT": position},
        _pair_infos={"indodax:BTC_IDR:okx:BTC_USDT": pair_info},
        _exchange_halted={},
        supabase=FakeSupabase(),
        failures=[],
    )

    async def publish_failure(*args, **kwargs):
        engine.failures.append((args, kwargs))

    engine._publish_execution_failure = publish_failure
    worker = ExposureReconciliationWorker(engine)
    await worker.reconcile_once()

    assert engine._exchange_halted["indodax"].startswith("exposure reconciliation:")
    assert engine._exchange_halted["okx"].startswith("exposure reconciliation:")
    assert len(engine.failures) == 2
    assert client.order_calls == 0
    assert engine.supabase.rows
    assert all(row["is_mock"] is False for table, row in engine.supabase.rows)
