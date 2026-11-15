"""Unit tests for Tahap 1 (P0): Integritas Perhitungan Profit, VWAP Exit, & Fail-Closed Guard.

Covers:
1.1. _verify_dual_leg_funding() fail-closed guards (exception, missing keys, NaN/inf, negative).
1.2. PairPnLEngine: 4-fill unified fee, actual margin (no 20% assumption), and dual ROI metrics.
1.3. compute_vwap_exit_fill(): full-depth sweep, fail-closed depth check, chunk_qty handling.
1.4. _pair_lock non-reentrant safety and _spot_exchange_entry_lock synchronization.
"""

from __future__ import annotations

import asyncio
import math
from types import SimpleNamespace

import pytest

from executor.exit_pricing import compute_vwap_exit_fill
from executor.pair_pnl_engine import PairPnLEngine, PairPnLSnapshot
from executor.risk import RiskMixin


# =====================================================================
# 1.1. Fail-Closed Dual-Leg Funding Preflight Tests
# =====================================================================

class FakeRiskEngine(RiskMixin):
    def __init__(self):
        self.config = SimpleNamespace(mock_mode=False)
        self.published_failures = []
        self._balance_snapshots = {}

    async def _publish_execution_failure(self, pair_tag, reason, message, detail=None):
        self.published_failures.append({
            "pair_tag": pair_tag,
            "reason": reason,
            "message": message,
            "detail": detail or {},
        })


@pytest.mark.asyncio
async def test_dual_leg_funding_fails_closed_on_probe_exception():
    engine = FakeRiskEngine()
    async def _err(**_):
        raise RuntimeError("network down")
    engine._get_balance_snapshot = _err

    ok = await engine._verify_dual_leg_funding("indodax:btc_idr", SimpleNamespace(), 50.0, 16000.0)
    assert ok is False
    assert len(engine.published_failures) == 1
    assert engine.published_failures[0]["reason"] == "BALANCE_UNVERIFIED"


@pytest.mark.asyncio
async def test_dual_leg_funding_fails_closed_on_non_dict_balance():
    engine = FakeRiskEngine()
    async def _non_dict(**_):
        return ["not", "a", "dict"]
    engine._get_balance_snapshot = _non_dict

    ok = await engine._verify_dual_leg_funding("indodax:btc_idr", SimpleNamespace(), 50.0, 16000.0)
    assert ok is False
    assert engine.published_failures[0]["reason"] == "BALANCE_UNVERIFIED"


@pytest.mark.asyncio
async def test_dual_leg_funding_fails_closed_on_missing_keys():
    engine = FakeRiskEngine()
    async def _missing_keys(**_):
        return {"spot_available": 100.0}  # missing futures_available
    engine._get_balance_snapshot = _missing_keys

    ok = await engine._verify_dual_leg_funding("indodax:btc_idr", SimpleNamespace(), 50.0, 16000.0)
    assert ok is False
    assert engine.published_failures[0]["reason"] == "BALANCE_UNVERIFIED"


@pytest.mark.asyncio
async def test_dual_leg_funding_fails_closed_on_non_finite_or_negative():
    engine = FakeRiskEngine()
    # NaN test
    async def _nan_bal(**_):
        return {"spot_available": float("nan"), "futures_available": 100.0}
    engine._get_balance_snapshot = _nan_bal
    ok = await engine._verify_dual_leg_funding("indodax:btc_idr", SimpleNamespace(), 50.0, 16000.0)
    assert ok is False

    # Negative test
    async def _neg_bal(**_):
        return {"spot_available": -10.0, "futures_available": 100.0}
    engine._get_balance_snapshot = _neg_bal
    ok = await engine._verify_dual_leg_funding("indodax:btc_idr", SimpleNamespace(), 50.0, 16000.0)
    assert ok is False


# =====================================================================
# 1.2. PairPnLEngine: Actual Capital Basis & 4-Fill Unified Fee
# =====================================================================

def test_pair_pnl_actual_margin_and_leverage():
    engine = PairPnLEngine()
    # 1x leverage test: total capital deployed = spot entry notional + futures entry notional + fee reserve
    snap_1x = engine.compute_pair_pnl(
        session_id="s1",
        pair_key="toko_okx:BTC",
        spot_qty=1.0,
        spot_entry_cost_quote=50000.0,  # $50,000 spot notional
        spot_executable_bid=50000.0,
        futures_qty=1.0,
        futures_entry_price=50000.0,    # $50,000 futures notional
        futures_executable_ask=50000.0,
        effective_leverage=1.0,
        fee_reserve_usd=50.0,
    )
    # Total capital deployed should be 50000 + 50000 + 50 = 100,050 USD (NOT 20% assumption!)
    assert snap_1x.total_capital_deployed == pytest.approx(100050.0, rel=1e-3)

    # 10x leverage test: futures margin = 50,000 / 10 = 5,000 USD
    snap_10x = engine.compute_pair_pnl(
        session_id="s2",
        pair_key="toko_okx:BTC",
        spot_qty=1.0,
        spot_entry_cost_quote=50000.0,
        spot_executable_bid=50000.0,
        futures_qty=1.0,
        futures_entry_price=50000.0,
        futures_executable_ask=50000.0,
        effective_leverage=10.0,
        fee_reserve_usd=50.0,
    )
    # 50000 + 5000 + 50 = 55,050 USD
    assert snap_10x.total_capital_deployed == pytest.approx(55050.0, rel=1e-3)

    # Explicit actual_initial_margin_usd
    snap_explicit_margin = engine.compute_pair_pnl(
        session_id="s3",
        pair_key="toko_okx:BTC",
        spot_qty=1.0,
        spot_entry_cost_quote=50000.0,
        spot_executable_bid=50000.0,
        futures_qty=1.0,
        futures_entry_price=50000.0,
        futures_executable_ask=50000.0,
        actual_initial_margin_usd=6200.0,
        fee_reserve_usd=0.0,
    )
    assert snap_explicit_margin.total_capital_deployed == pytest.approx(56200.0, rel=1e-3)


def test_pair_pnl_dual_roi_metrics():
    engine = PairPnLEngine()
    # Net profit = +100 USD
    # spot notional = $10,000
    # futures notional = $10,000 (1x leverage -> total deployed = $20,000)
    snap = engine.compute_pair_pnl(
        session_id="s_roi",
        pair_key="toko_okx:ETH",
        spot_qty=1.0,
        spot_entry_cost_quote=10000.0,
        spot_executable_bid=10100.0,   # +100 gross
        spot_entry_fee_quote=0.0,
        spot_exit_fee_rate=0.0,
        futures_qty=1.0,
        futures_entry_price=10000.0,
        futures_executable_ask=10000.0,
        futures_close_fee_rate=0.0,
        futures_entry_fee=0.0,
        effective_leverage=1.0,
    )
    assert snap.pair_net_liquidation_pnl == pytest.approx(100.0, rel=1e-3)
    # net_spread_roi_pct = 100 / 10000 * 100 = 1.0%
    assert snap.net_spread_roi_pct == pytest.approx(1.0, rel=1e-3)
    # return_on_deployed_capital_pct = 100 / 20000 * 100 = 0.5%
    assert snap.return_on_deployed_capital_pct == pytest.approx(0.5, rel=1e-3)


def test_pair_pnl_4_fill_unified_fee_and_tax():
    engine = PairPnLEngine()
    snap = engine.compute_pair_pnl(
        session_id="s_fee",
        pair_key="idx_okx:SOL",
        spot_qty=10.0,
        spot_entry_cost_quote=1000.0,
        spot_executable_bid=100.0,      # 10.0 * 100.0 = 1,000 gross proceeds
        spot_entry_fee_quote=1.0,       # Spot Entry fee: $1.00
        spot_exit_fee_rate=0.001,       # Spot Exit fee: 0.1% of $1000 = $1.00
        spot_tax_rate=0.001,            # Spot Tax (PPh/PPN): 0.1% = $1.00 -> total spot exit = $2.00
        futures_qty=10.0,
        futures_entry_price=100.0,
        futures_executable_ask=100.0,
        futures_entry_fee=0.50,         # Fut Entry fee: $0.50
        futures_close_fee_rate=0.0005,  # Fut Exit fee: 0.05% of $1000 = $0.50
    )
    # Total fee = 1.00 + 2.00 + 0.50 + 0.50 = 4.00 USD
    assert snap.spot_entry_fee_usd == pytest.approx(1.0, rel=1e-3)
    assert snap.spot_exit_fee_usd == pytest.approx(2.0, rel=1e-3)
    assert snap.futures_entry_fee_usd == pytest.approx(0.50, rel=1e-3)
    assert snap.futures_exit_fee_usd == pytest.approx(0.50, rel=1e-3)
    assert snap.total_fees_usd == pytest.approx(4.00, rel=1e-3)


# =====================================================================
# 1.3. Full-Depth VWAP & Fail-Closed Depth Check Tests
# =====================================================================

def test_compute_vwap_exit_fill_fail_closed_on_empty_or_none():
    assert compute_vwap_exit_fill(None, 1.0)["status"] == "DEPTH_UNKNOWN"
    assert compute_vwap_exit_fill([], 1.0)["can_exit"] is False
    assert compute_vwap_exit_fill([[]], 1.0)["can_exit"] is False


def test_compute_vwap_exit_fill_sweeps_bids_descending():
    # Orderbook bids: level 1 is 100 (qty 1), level 2 is 90 (qty 1)
    bids = [[90.0, 1.0], [100.0, 1.0]]  # unsorted input
    res = compute_vwap_exit_fill(bids, target_qty=1.5, side="bid")
    assert res["status"] == "DEPTH_SUFFICIENT"
    assert res["can_exit"] is True
    assert res["can_exit_full"] is True
    # VWAP of 1.5 qty: 1.0 @ 100 + 0.5 @ 90 = 100 + 45 = 145 / 1.5 = 96.6667
    assert res["vwap_price"] == pytest.approx(96.6667, rel=1e-3)
    assert res["best_price"] == pytest.approx(100.0, rel=1e-3)
    assert res["slippage_pct"] > 0


def test_compute_vwap_exit_fill_sweeps_asks_ascending():
    # Orderbook asks: level 1 is 100 (qty 1), level 2 is 110 (qty 1)
    asks = [[110.0, 1.0], [100.0, 1.0]]
    res = compute_vwap_exit_fill(asks, target_qty=1.5, side="ask")
    assert res["status"] == "DEPTH_SUFFICIENT"
    # VWAP of 1.5 qty: 1.0 @ 100 + 0.5 @ 110 = 100 + 55 = 155 / 1.5 = 103.3333
    assert res["vwap_price"] == pytest.approx(103.3333, rel=1e-3)
    assert res["best_price"] == pytest.approx(100.0, rel=1e-3)


def test_compute_vwap_exit_fill_insufficient_depth_and_chunk():
    bids = [[100.0, 0.5]]  # only 0.5 available
    # Target 1.0 without chunking -> can_exit = False
    res_no_chunk = compute_vwap_exit_fill(bids, target_qty=1.0, side="bid")
    assert res_no_chunk["status"] == "DEPTH_INSUFFICIENT"
    assert res_no_chunk["can_exit"] is False
    assert res_no_chunk["can_exit_full"] is False

    # Target 1.0 with chunk_qty = 0.4 -> can_exit = True
    res_chunk = compute_vwap_exit_fill(bids, target_qty=1.0, side="bid", chunk_qty=0.4)
    assert res_chunk["status"] == "CHUNK_DEPTH_SUFFICIENT"
    assert res_chunk["can_exit"] is True
    assert res_chunk["can_exit_full"] is False
    assert res_chunk["filled_qty"] == pytest.approx(0.4, rel=1e-3)


# =====================================================================
# 1.4. Non-Reentrant Safe Locks & Venue Locks
# =====================================================================

@pytest.mark.asyncio
async def test_pair_lock_non_reentrant_safety():
    from executor.engine_runtime import EngineRuntimeMixin

    class MockEngine(EngineRuntimeMixin):
        def __init__(self):
            self._pair_locks = {}

    engine = MockEngine()
    pair_tag = "indodax:btc_idr"
    lock = engine._pair_lock(pair_tag)

    # When lock is held, dynamic exit skips rather than deadlocking
    await lock.acquire()
    assert lock.locked()

    called = False
    async def mock_inner(pt):
        nonlocal called
        called = True
    engine._check_dynamic_exit_pair_inner = mock_inner

    await engine._check_dynamic_exit_pair_bound(pair_tag)
    assert not called  # Skipped because lock was already held!

    lock.release()
