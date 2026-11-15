"""Tahap 4: immutable fill ledger, expected-vs-realized attribution, hardening."""

import json
import math
import time
import uuid

import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine
from executor.fill_ledger import ExecutionFillLedger, _fee_details, _utc
from executor.global_stop import GlobalStopError, GlobalStopService
from executor.ledger_hooks import LedgerMixin
from executor.models import PositionState
from tests.conftest import MockExchangeClient


class FakeConn:
    """Minimal asyncpg connection double recording parameter tuples."""

    def __init__(self, store):
        self.store = store

    def transaction(self):
        return _NullTxn()

    async def execute(self, sql, *args):
        return "ok"

    async def fetchrow(self, sql, *args):
        self.store.append((sql, args))
        if "execution_fill_ledger" in sql:
            fill_id = str(uuid.uuid4())
            return {"fill_id": fill_id}
        if "pair_pnl_deviations" in sql:
            return {"deviation_id": str(uuid.uuid4())}
        return None


class _NullTxn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakePool:
    """Pool stub whose acquire() yields a recording connection."""

    def __init__(self):
        self.calls = []
        self.closed = False

    def acquire(self):
        return _Acquire(self)

    async def close(self):
        self.closed = True


class _Acquire:
    def __init__(self, pool):
        self.pool = pool

    async def __aenter__(self):
        return FakeConn(self.pool.calls)

    async def __aexit__(self, *exc):
        return False


class LedgerEngine(LedgerMixin):
    """Stand-in engine exposing only what the ledger hooks touch."""

    def __init__(self, ledger, mock_mode=True):
        self.fill_ledger = ledger
        self.config = ExecutorConfig(mock_mode=mock_mode)


# --------------------------------------------------------------------- unit

def test_fee_details_prefers_single_fee_row():
    amount, currency, verified = _fee_details(
        {"fee": {"cost": 0.12, "currency": "usdt"}, "fees": [{"cost": 0.12, "currency": "USDT"}]}
    )
    assert amount == pytest.approx(0.12)
    assert currency == "USDT"
    assert verified is True


def test_fee_details_rejects_multi_currency_fee():
    amount, currency, verified = _fee_details(
        {
            "fee": {"cost": 1.0, "currency": "USDT"},
            "fees": [{"cost": 0.5, "currency": "USDT"}, {"cost": 0.5, "currency": "BTC"}],
        }
    )
    assert amount == pytest.approx(1.0)
    assert currency == "USDT"
    # Multiple currencies means the single-currency assumption does not hold.
    assert verified is False


def test_fee_details_without_currency_is_unverified():
    _, currency, verified = _fee_details({"fee": {"cost": 0.3}})
    assert currency is None
    assert verified is False


def test_utc_normalises_ms_epoch_and_iso():
    assert _utc(1_700_000_000_000).year == 2023
    assert _utc("2026-11-15T10:00:00Z").tzinfo is not None
    assert _utc(None).tzinfo is not None


@pytest.mark.asyncio
async def test_ledger_rejects_non_positive_quantity():
    pool = FakePool()
    ledger = ExecutionFillLedger(pool=pool)
    fill_id = await ledger.record_fill(
        pair_key="indodax:btc_idr", leg="spot_entry", exchange="indodax",
        market_type="spot", symbol="btc_idr", side="buy",
        qty=0.0, actual_vwap_price=1000.0,
    )
    assert fill_id is None
    assert pool.calls == []


@pytest.mark.asyncio
async def test_ledger_rejects_unknown_leg():
    pool = FakePool()
    ledger = ExecutionFillLedger(pool=pool)
    fill_id = await ledger.record_fill(
        pair_key="indodax:btc_idr", leg="not_a_leg", exchange="indodax",
        market_type="spot", symbol="btc_idr", side="buy",
        qty=1.0, actual_vwap_price=1000.0,
    )
    assert fill_id is None
    assert pool.calls == []


@pytest.mark.asyncio
async def test_ledger_records_all_four_legs_with_actual_values():
    pool = FakePool()
    ledger = ExecutionFillLedger(pool=pool)
    legs = ("spot_entry", "futures_entry", "spot_exit", "futures_exit")
    for index, leg in enumerate(legs):
        fill_id = await ledger.record_fill(
            pair_key="indodax:btc_idr", leg=leg, exchange="indodax",
            market_type="spot", symbol="btc_idr", side="buy",
            qty=0.005, actual_vwap_price=1_000_000_000.0 + index,
            fill={"id": f"t{index}", "fee": {"cost": 0.01, "currency": "IDR"}},
            expected_vwap_price=1_000_000_000.0,
            fx_rate_applied=18000.0, fx_rate_source="scanner_payload",
            realized_funding_usd=0.25,
        )
        assert fill_id
    recorded_legs = [args[2] for sql, args in pool.calls if "execution_fill_ledger" in sql]
    assert recorded_legs == list(legs)


@pytest.mark.asyncio
async def test_ledger_persists_fx_rate_and_funding_on_every_row():
    pool = FakePool()
    ledger = ExecutionFillLedger(pool=pool)
    await ledger.record_fill(
        pair_key="indodax:btc_idr", leg="spot_exit", exchange="indodax",
        market_type="spot", symbol="btc_idr", side="sell", qty=0.5,
        actual_vwap_price=1.0, fx_rate_applied=18123.5,
        fx_rate_source="exit_snapshot", realized_funding_usd=1.75,
        fill={"id": "t1", "fee": {"cost": 2.0, "currency": "IDR"}},
    )
    sql, args = pool.calls[0]
    assert "execution_fill_ledger" in sql
    assert 18123.5 in args          # fx_rate_applied
    assert "exit_snapshot" in args  # fx_rate_source
    assert 1.75 in args             # realized_funding_usd


@pytest.mark.asyncio
async def test_ledger_duplicate_venue_fill_is_idempotent_not_an_error():
    class DedupConn(FakeConn):
        async def fetchrow(self, sql, *args):
            self.store.append((sql, args))
            return None  # ON CONFLICT DO NOTHING

    class DedupPool(FakePool):
        def acquire(self):
            pool = self

            class _A:
                async def __aenter__(self_inner):
                    return DedupConn(pool.calls)

                async def __aexit__(self_inner, *exc):
                    return False

            return _A()

    ledger = ExecutionFillLedger(pool=DedupPool())
    dup = await ledger.record_fill(
        pair_key="p", leg="spot_entry", exchange="indodax", market_type="spot",
        symbol="btc_idr", side="buy", qty=1.0, actual_vwap_price=10.0,
        fill={"id": "same-trade"},
    )
    assert dup is None
    assert ledger.dropped == 0  # replay is not a drop


@pytest.mark.asyncio
async def test_ledger_swallows_database_errors_without_raising():
    class BrokenPool(FakePool):
        def acquire(self):
            raise RuntimeError("connection refused")

    ledger = ExecutionFillLedger(pool=BrokenPool())
    assert await ledger.record_fill(
        pair_key="p", leg="spot_entry", exchange="x", market_type="spot",
        symbol="s", side="buy", qty=1.0, actual_vwap_price=1.0,
    ) is None
    assert ledger.dropped == 1


# ------------------------------------------------- deviations (Tahap 4.1)

@pytest.mark.asyncio
async def test_deviation_decomposes_fee_and_slippage():
    pool = FakePool()
    ledger = ExecutionFillLedger(pool=pool)
    await ledger.record_deviation(
        pair_key="indodax:btc_idr",
        expected_net_pnl_usd=10.0,
        realized_net_pnl_usd=7.0,
        expected_fees_usd=2.0,
        realized_fees_usd=3.0,
        trade_key="trade-1",
    )
    sql, args = pool.calls[0]
    assert "pair_pnl_deviations" in sql
    expected, realized, net_delta = args[2], args[3], args[4]
    expected_fees, realized_fees, fee_delta, slippage = args[5], args[6], args[7], args[8]
    assert expected == 10.0 and realized == 7.0
    assert net_delta == pytest.approx(-3.0)
    assert fee_delta == pytest.approx(-1.0)       # expected 2 - realized 3
    # Net is -3.0; fees explain -1.0 of it, so price/execution slippage is -2.0.
    assert slippage == pytest.approx(-2.0)
    assert (expected_fees, realized_fees) == (2.0, 3.0)


@pytest.mark.asyncio
async def test_deviation_records_zero_when_realized_matches_expected():
    pool = FakePool()
    ledger = ExecutionFillLedger(pool=pool)
    await ledger.record_deviation(
        pair_key="p", expected_net_pnl_usd=5.0, realized_net_pnl_usd=5.0,
        expected_fees_usd=1.0, realized_fees_usd=1.0, trade_key="t",
    )
    _, args = pool.calls[0]
    assert args[4] == 0.0   # net deviation
    assert args[7] == 0.0   # fee deviation
    assert args[8] == 0.0   # slippage deviation


# --------------------------------------------------- hook integration

@pytest.mark.asyncio
async def test_spot_entry_hook_uses_fill_average_not_requested_price():
    pool = FakePool()
    engine = LedgerEngine(ExecutionFillLedger(pool=pool))
    order_info = {
        "local_exchange": "indodax", "symbol": "btc_idr", "quote_asset": "IDR",
        "client_order_id": "entry-1", "price": 1_000_000_000.0,
    }
    order = {
        "id": "42", "filled": 0.005, "average": 999_500_000.0,
        "fee": {"cost": 0.001, "currency": "BTC"},
    }
    await engine._record_spot_entry_fill(
        "indodax:btc_idr", order_info, order, 0.005, 999_500_000.0, 18000.0
    )
    sql, args = pool.calls[0]
    assert "execution_fill_ledger" in sql
    assert args[2] == "spot_entry"
    assert args[11] == pytest.approx(999_500_000.0)   # actual vwap
    assert args[12] == pytest.approx(1_000_000_000.0)  # expected vwap
    assert args[19] == pytest.approx(18000.0)          # fx rate


@pytest.mark.asyncio
async def test_ledger_skips_fill_when_no_executed_price():
    pool = FakePool()
    engine = LedgerEngine(ExecutionFillLedger(pool=pool))
    order = {"id": "42", "filled": 0.005, "average": 0.0, "status": "open"}
    result = await engine._record_spot_entry_fill(
        "p", {"local_exchange": "indodax", "symbol": "btc_idr"}, order,
        0.005, 0.0, 18000.0,
    )
    assert result is None
    assert pool.calls == []


@pytest.mark.asyncio
async def test_ledger_disabled_is_silent_noop():
    engine = LedgerEngine(ExecutionFillLedger(pool=None))
    order = {"id": "1", "filled": 1.0, "average": 5.0}
    assert await engine._record_spot_entry_fill(
        "p", {"local_exchange": "x", "symbol": "s"}, order, 1.0, 5.0, 1.0
    ) is None


# ------------------------------------- expected vs realized on full close

def _active_position(**overrides):
    from executor.position_models import ActivePosition

    values = dict(
        local_exchange="indodax", global_exchange="okx", symbol="btc_idr",
        entry_price_idr=1_000_000_000.0, short_entry_usdt=65_000.0,
        qty=0.0, initial_qty=0.005, opened_at_ms=int(time.time() * 1000) - 3_600_000,
        expected_net_profit_usdt=12.0, expected_fees_usdt=4.0,
        expected_hold_hours=8.0,
    )
    values.update(overrides)
    return ActivePosition(**values)


@pytest.mark.asyncio
async def test_deviation_written_only_after_full_close():
    pool = FakePool()
    engine = LedgerEngine(ExecutionFillLedger(pool=pool))
    trade = {
        "trade_id": "trade-9", "net_profit_usdt": 9.0, "fees_usdt": 4.5,
        "fees_usdt_override": None, "closed_at_ms": int(time.time() * 1000),
    }
    # Position still holds half the quantity: partial close, no attribution yet.
    partial = _active_position(qty=0.0025)
    assert await engine._record_expected_vs_realized(
        "indodax:btc_idr", partial, trade, 0.0025
    ) is None
    assert pool.calls == []

    flat = _active_position(qty=0.0)
    assert await engine._record_expected_vs_realized(
        "indodax:btc_idr", flat, trade, 0.005
    )
    sql, args = pool.calls[0]
    assert "pair_pnl_deviations" in sql
    assert args[2] == pytest.approx(12.0)   # expected
    assert args[3] == pytest.approx(9.0)    # realized
    assert args[4] == pytest.approx(-3.0)   # net deviation


# ---------------------------------------------- STOP fencing (Tahap 4.2)

class FencePool(FakePool):
    """Pool whose control row can be toggled between stopped and running."""

    def __init__(self, is_stopped=False, unresolved=None):
        super().__init__()
        self.is_stopped = is_stopped
        self.unresolved = unresolved

    def acquire(self):
        pool = self

        class _A:
            async def __aenter__(self_inner):
                return FenceConn(pool)

            async def __aexit__(self_inner, *exc):
                return False

        return _A()


class FenceConn(FakeConn):
    def __init__(self, pool):
        super().__init__(pool.calls)
        self.pool = pool

    async def execute(self, sql, *args):
        # The advisory lock/unlock go through execute(), not fetchrow().
        self.store.append((sql, args))
        return "ok"

    async def fetchrow(self, sql, *args):
        self.store.append((sql, args))
        if "executor_global_control" in sql:
            return {
                "is_stopped": self.pool.is_stopped,
                "epoch": 3,
                "reason": "manual" if self.pool.is_stopped else None,
            }
        if "executor_submit_attempts" in sql and sql.strip().startswith("select"):
            return self.pool.unresolved
        if sql.strip().startswith("update public.executor_submit_attempts"):
            return {"client_order_id": args[0]}
        return None


@pytest.mark.asyncio
async def test_entry_fence_blocks_while_stopped():
    service = GlobalStopService("postgresql://x")
    service.pool = FencePool(is_stopped=True)
    with pytest.raises(GlobalStopError, match="global STOP active"):
        async with service.order_submit_fence(client_order_id="entry-1"):
            pytest.fail("entry must not reach the venue while STOP is active")


@pytest.mark.asyncio
async def test_derisk_fence_allows_exit_while_stopped():
    pool = FencePool(is_stopped=True)
    service = GlobalStopService("postgresql://x")
    service.pool = pool
    reached = False
    async with service.derisk_submit_fence(client_order_id="exit-1"):
        reached = True
    assert reached, "risk-reducing submit must not be blocked by STOP"
    assert any("executor_global_control" in sql for sql, _ in pool.calls)


@pytest.mark.asyncio
async def test_derisk_fence_still_serializes_on_advisory_lock():
    pool = FencePool(is_stopped=False)
    service = GlobalStopService("postgresql://x")
    service.pool = pool
    async with service.derisk_submit_fence(client_order_id="unwind-1"):
        pass
    sqls = [sql for sql, _ in pool.calls]
    assert any("pg_advisory_lock" in sql for sql in sqls)
    assert any("pg_advisory_unlock" in sql for sql in sqls)


@pytest.mark.asyncio
async def test_derisk_fence_proceeds_despite_unresolved_entry():
    """An ambiguous entry must not prevent closing existing exposure."""
    pool = FencePool(
        is_stopped=False,
        unresolved={
            "client_order_id": "stuck-entry", "exchange": "indodax",
            "symbol": "btc_idr", "status": "UNKNOWN",
        },
    )
    service = GlobalStopService("postgresql://x")
    service.pool = pool
    async with service.derisk_submit_fence(client_order_id="exit-9"):
        pass  # must not raise


@pytest.mark.asyncio
async def test_entry_fence_blocks_on_unresolved_submit():
    pool = FencePool(
        is_stopped=False,
        unresolved={
            "client_order_id": "stuck-entry", "exchange": "indodax",
            "symbol": "btc_idr", "status": "UNKNOWN",
        },
    )
    service = GlobalStopService("postgresql://x")
    service.pool = pool
    with pytest.raises(GlobalStopError, match="unresolved in-flight submit"):
        async with service.order_submit_fence(client_order_id="entry-2"):
            pytest.fail("a new entry must not bypass an unresolved submit")


@pytest.mark.asyncio
async def test_order_fence_reconnects_after_connection_loss():
    service = GlobalStopService("postgresql://x")
    service.pool = None       # simulates a dropped pool

    async def fake_reconnect():
        service.pool = FencePool(is_stopped=False)
        return True

    service.ensure_pool = fake_reconnect
    async with service.order_submit_fence(client_order_id="entry-3"):
        pass
    assert service.pool is not None


@pytest.mark.asyncio
async def test_broken_pool_is_demoted_for_reattempt():
    service = GlobalStopService("postgresql://x")
    pool = FencePool()
    service.pool = pool
    service._demote_broken_pool(ConnectionResetError("socket closed"))
    assert service.pool is None
    # A statement error is not a connection error and must not drop the pool.
    service.pool = FencePool()
    service._demote_broken_pool(ValueError("bad sql"))
    assert service.pool is not None


# ---------------------------------------- dual-leg startup recovery

@pytest.mark.asyncio
async def test_startup_recovery_blocks_when_spot_inventory_missing(fake_redis, monkeypatch):
    """A confirmed futures short with no spot inventory is not a hedge."""
    client = MockExchangeClient()
    client.set_spot_balance("BTC", 0.0)
    position = __import__("executor.position_models", fromlist=["ActivePosition"]).ActivePosition(
        local_exchange="indodax", global_exchange="okx", symbol="btc_idr",
        entry_price_idr=1_000_000_000.0, short_entry_usdt=65_000.0,
        qty=0.005, base_asset="BTC", global_symbol="BTC/USDT:USDT",
    )

    async def matching_short(symbol):
        return [{"symbol": symbol, "contracts": 0.005, "contractSize": 1.0, "side": "short"}]

    client.fetch_positions = matching_short
    await fake_redis.set("arb:active_pos:indodax:btc_idr", json.dumps(position.to_dict()))

    async def fake_start(self):
        self.pool = object()

    async def fake_read_state(self):
        return {"is_stopped": False, "epoch": 0, "reason": None}

    async def fake_unresolved(self, *, limit=100):
        return []

    monkeypatch.setattr(GlobalStopService, "start", fake_start)
    monkeypatch.setattr(GlobalStopService, "read_state", fake_read_state)
    monkeypatch.setattr(GlobalStopService, "list_unresolved_submit_attempts", fake_unresolved)

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False, postgres_dsn="postgresql://test"),
        client=client, redis_client=fake_redis,
    )
    await engine.initialize()

    # Recovery migrates the legacy key to the venue-qualified canonical tag.
    tag = "indodax:btc_idr__okx:btc_usdt:usdt"
    assert engine.states[tag] == PositionState.RECONCILIATION_REQUIRED
    reason = (engine._failure_reasons.get(tag) or {}).get("reason")
    assert reason == "RECOVERY_POSITION_MISMATCH"


@pytest.mark.asyncio
async def test_startup_recovery_unlocks_when_both_legs_match(fake_redis, monkeypatch):
    client = MockExchangeClient()
    client.set_spot_balance("BTC", 0.005)
    position = __import__("executor.position_models", fromlist=["ActivePosition"]).ActivePosition(
        local_exchange="indodax", global_exchange="okx", symbol="btc_idr",
        entry_price_idr=1_000_000_000.0, short_entry_usdt=65_000.0,
        qty=0.005, base_asset="BTC", global_symbol="BTC/USDT:USDT",
    )

    async def matching_short(symbol):
        return [{"symbol": symbol, "contracts": 0.005, "contractSize": 1.0, "side": "short"}]

    client.fetch_positions = matching_short
    await fake_redis.set("arb:active_pos:indodax:btc_idr", json.dumps(position.to_dict()))

    async def fake_start(self):
        self.pool = object()

    async def fake_read_state(self):
        return {"is_stopped": False, "epoch": 0, "reason": None}

    async def fake_unresolved(self, *, limit=100):
        return []

    monkeypatch.setattr(GlobalStopService, "start", fake_start)
    monkeypatch.setattr(GlobalStopService, "read_state", fake_read_state)
    monkeypatch.setattr(GlobalStopService, "list_unresolved_submit_attempts", fake_unresolved)

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False, postgres_dsn="postgresql://test"),
        client=client, redis_client=fake_redis,
    )
    await engine.initialize()

    assert engine.states["indodax:btc_idr__okx:btc_usdt:usdt"] == PositionState.HEDGED_ACTIVE


@pytest.mark.asyncio
async def test_cumulative_poll_watermark_keeps_each_increment_distinct():
    """A chunked maker fill polled repeatedly must not collapse into one row."""
    from executor.ledger_hooks import _fill_from_order

    first = _fill_from_order({"id": "77", "filled": 0.002, "average": 100.0})
    repeat = _fill_from_order({"id": "77", "filled": 0.002, "average": 100.0})
    second = _fill_from_order({"id": "77", "filled": 0.005, "average": 100.0})

    # Re-polling the same watermark is idempotent...
    assert first["id"] == repeat["id"]
    # ...but a new increment is a distinct execution.
    assert first["id"] != second["id"]


@pytest.mark.asyncio
async def test_single_trade_payload_keeps_venue_trade_id():
    """When the venue gives real trades, use the venue fill id verbatim."""
    from executor.ledger_hooks import _fill_from_order

    payload = _fill_from_order(
        {"id": "order-1", "filled": 0.002, "trades": [{"id": "venue-trade-9", "amount": 0.002}]}
    )
    assert payload["id"] == "venue-trade-9"


def test_base_asset_derivation_from_venue_symbols():
    derive = ArbitrageEngine._base_asset_from_symbol
    assert derive("btc_idr") == "BTC"
    assert derive("BTC/IDR") == "BTC"
    assert derive("eth_usdt") == "ETH"
    assert derive("SOLUSDT") == "SOL"
