from types import SimpleNamespace

import pytest

from executor.exchange_client import ExchangeClient
from executor.models import ActivePosition
from executor.exposure_reconciliation import (
    ExposureReconciliationWorker,
    _env_float,
    _env_interval,
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
    position = SimpleNamespace(
        qty=1.0,
        local_exchange="indodax",
        global_exchange="okx",
        base_asset="BTC",
        global_symbol="BTC/USDT:USDT",
    )
    engine = SimpleNamespace(
        config=SimpleNamespace(mock_mode=False),
        client=client,
        active_positions={"indodax:BTC_IDR:okx:BTC_USDT": position},
        _pair_infos={},
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



@pytest.mark.asyncio
async def test_missing_restored_position_mapping_halts_both_venues(monkeypatch):
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "60")
    client = FakeClient()
    position = SimpleNamespace(
        qty=1.0, local_exchange="indodax", global_exchange="okx",
        base_asset="", global_symbol="",
    )
    engine = SimpleNamespace(
        config=SimpleNamespace(mock_mode=False),
        client=client,
        active_positions={"legacy-pair": position},
        _pair_infos={},
        _exchange_halted={},
        supabase=FakeSupabase(),
        failures=[],
    )

    async def publish_failure(*args, **kwargs):
        engine.failures.append((args, kwargs))

    engine._publish_execution_failure = publish_failure
    await ExposureReconciliationWorker(engine).reconcile_once()

    assert "indodax" in engine._exchange_halted
    assert "okx" in engine._exchange_halted
    assert len(engine.failures) == 2
    assert client.order_calls == 0


@pytest.mark.asyncio
async def test_unavailable_exchange_reads_halt_new_entries(monkeypatch):
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "60")

    class UnavailableClient(FakeClient):
        async def fetch_balance_report(self):
            return [
                {"name": "indodax", "market": "spot", "status": "error", "assets": []},
                {"name": "okx", "market": "futures", "status": "ok", "assets": []},
            ]

        async def fetch_positions(self, symbol):
            raise RuntimeError("private endpoint unavailable")

    client = UnavailableClient()
    position = SimpleNamespace(
        qty=1.0, local_exchange="indodax", global_exchange="okx",
        base_asset="BTC", global_symbol="BTC/USDT:USDT",
    )
    engine = SimpleNamespace(
        config=SimpleNamespace(mock_mode=False),
        client=client,
        active_positions={"pair": position},
        _pair_infos={},
        _exchange_halted={},
        supabase=FakeSupabase(),
        failures=[],
    )

    async def publish_failure(*args, **kwargs):
        engine.failures.append((args, kwargs))

    engine._publish_execution_failure = publish_failure
    await ExposureReconciliationWorker(engine).reconcile_once()

    assert "indodax" in engine._exchange_halted
    assert "okx" in engine._exchange_halted
    assert len(engine.failures) == 2
    assert client.order_calls == 0



def test_active_position_round_trip_keeps_reconciliation_metadata():
    position = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000,
        short_entry_usdt=65_000,
        qty=0.01,
        base_asset="BTC",
        global_symbol="BTC/USDT:USDT",
    )

    restored = ActivePosition.from_dict(position.to_dict())

    assert restored.base_asset == "BTC"
    assert restored.global_symbol == "BTC/USDT:USDT"



class FakeBalanceVenue:
    def __init__(self, venue_id, balance=None, error=None):
        self.id = venue_id
        self.balance = balance or {}
        self.error = error

    async def fetch_balance(self):
        if self.error:
            raise self.error
        return self.balance


@pytest.mark.asyncio
async def test_exchange_client_balance_report_normalizes_both_venues():
    local = FakeBalanceVenue(
        "indodax",
        {"free": {"BTC": 0.4}, "used": {"BTC": 0.1}, "total": {"BTC": 0.5}},
    )
    futures = FakeBalanceVenue(
        "okx",
        {"free": {"USDT": 80}, "used": {"USDT": 20}, "total": {"USDT": 100}},
    )

    reports = await ExchangeClient(local, futures).fetch_balance_report()

    assert reports[0]["name"] == "indodax"
    assert reports[0]["market"] == "spot"
    assert reports[0]["assets"] == [
        {"asset": "BTC", "free": 0.4, "used": 0.1, "total": 0.5}
    ]
    assert reports[1]["name"] == "okx"
    assert reports[1]["market"] == "futures"
    assert reports[1]["assets"][0]["total"] == 100


@pytest.mark.asyncio
async def test_exchange_client_reports_failed_venue_without_faking_zero_balance():
    local = FakeBalanceVenue("indodax", error=RuntimeError("private API unavailable"))
    futures = FakeBalanceVenue(
        "okx", {"free": {"USDT": 10}, "used": {"USDT": 0}, "total": {"USDT": 10}}
    )

    reports = await ExchangeClient(local, futures).fetch_balance_report()

    assert reports[0]["status"] == "error"
    assert reports[0]["assets"] == []
    assert reports[1]["status"] == "ok"



@pytest.mark.asyncio
async def test_reconciliation_restores_preexisting_operator_halt_after_issue_clears(monkeypatch):
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "60")

    class MutableClient(FakeClient):
        spot_total = 0.5
        short_qty = 0.5

        async def fetch_balance_report(self):
            return [
                {
                    "name": "indodax", "market": "spot", "status": "ok",
                    "assets": [{"asset": "BTC", "free": self.spot_total,
                                "used": 0.0, "total": self.spot_total}],
                },
                {"name": "okx", "market": "futures", "status": "ok", "assets": []},
            ]

        async def fetch_positions(self, symbol):
            return [{"symbol": symbol, "side": "short", "contracts": self.short_qty}]

    client = MutableClient()
    position = SimpleNamespace(
        qty=1.0, local_exchange="indodax", global_exchange="okx",
        base_asset="BTC", global_symbol="BTC/USDT:USDT",
    )
    engine = SimpleNamespace(
        config=SimpleNamespace(mock_mode=False),
        client=client,
        active_positions={"pair": position},
        _pair_infos={},
        _exchange_halted={"okx": "operator kill switch"},
        supabase=FakeSupabase(),
        failures=[],
    )

    async def publish_failure(*args, **kwargs):
        engine.failures.append((args, kwargs))

    engine._publish_execution_failure = publish_failure
    worker = ExposureReconciliationWorker(engine)
    await worker.reconcile_once()

    assert engine._exchange_halted["okx"].startswith("exposure reconciliation:")

    client.spot_total = 1.0
    client.short_qty = 1.0
    await worker.reconcile_once()

    assert "indodax" not in engine._exchange_halted
    assert engine._exchange_halted["okx"] == "operator kill switch"



def test_reconciliation_environment_values_fail_safe(monkeypatch):
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "not-a-number")
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_REL_TOLERANCE", "nan")
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_ABS_TOLERANCE", "-1")

    assert _env_interval() == 300
    assert _env_float("EXPOSURE_RECONCILIATION_REL_TOLERANCE", 0.005, 0.0, 0.05) == 0.005
    assert _env_float("EXPOSURE_RECONCILIATION_ABS_TOLERANCE", 1e-8, 1e-12, 1.0) == 1e-12


def test_reconciliation_blank_environment_values_use_defaults(monkeypatch):
    # compose forwards `${VAR:-}` as an empty string; that means unset, not invalid.
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_INTERVAL_SECONDS", "")
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_REL_TOLERANCE", "")
    monkeypatch.setenv("EXPOSURE_RECONCILIATION_ABS_TOLERANCE", "")

    assert _env_interval() == 300
    assert _env_float("EXPOSURE_RECONCILIATION_REL_TOLERANCE", 0.005, 0.0, 0.05) == 0.005
    assert _env_float("EXPOSURE_RECONCILIATION_ABS_TOLERANCE", 1e-8, 1e-12, 1.0) == 1e-8
