from types import SimpleNamespace

import pytest

from executor.entry_hedge import EntryHedgeMixin
from executor.models import PositionState


class _FinalizerHarness(EntryHedgeMixin):
    def __init__(self, order_info):
        self.client = object()
        self.slot_manager = None
        self.config = SimpleNamespace(
            hedging=SimpleNamespace(futures_target_hedge_ratio=1.0)
        )
        self.pending_entry_orders = {"indodax:btc": order_info}
        self.active_positions = {}
        self.states = {"indodax:btc": PositionState.ENTRY_PARTIAL}
        self.global_stop = None
        self.persisted = []
        self.cleared = []
        self.failures = []

    async def _persist_pending_entry(self, pair_tag, order_info, ttl=None):
        self.persisted.append((pair_tag, dict(order_info), ttl))

    async def _clear_pending_entry(self, pair_tag):
        self.cleared.append(pair_tag)

    def _mark_failed(self, *args, **kwargs):
        self.failures.append((args, kwargs))
        self.states[args[0]] = kwargs.get("state", PositionState.FAILED)


@pytest.mark.asyncio
async def test_terminal_spot_partial_attempt_keeps_durable_residual_work_item():
    info = {
        "order_id": "spot-order-1",
        "spot_terminal": True,
        "hedge_waiting_for_coverage": True,
        "hedge_waiting_reason": "terminal_partial_attempt",
        "filled": 1.0,
        "qty": 1.0,
        "hedged_short_qty": 0.4,
        "rolled_back_qty": 0.0,
    }
    engine = _FinalizerHarness(info)

    await engine._finalize_entry_order("indodax:btc", "closed")

    assert "indodax:btc" in engine.pending_entry_orders
    assert engine.states["indodax:btc"] == PositionState.ENTRY_PARTIAL
    assert engine.cleared == []
    assert engine.failures == []
    assert engine.persisted[-1][0] == "indodax:btc"
    assert engine.persisted[-1][2] is None


@pytest.mark.asyncio
async def test_terminal_spot_below_minimum_notional_still_requires_manual_reconciliation():
    info = {
        "order_id": "spot-order-2",
        "spot_terminal": True,
        "hedge_waiting_for_coverage": True,
        "hedge_waiting_reason": "below_min_notional",
        "filled": 1.0,
        "qty": 1.0,
        "hedged_short_qty": 0.4,
        "rolled_back_qty": 0.0,
    }
    engine = _FinalizerHarness(info)

    await engine._finalize_entry_order("indodax:btc", "closed")

    assert "indodax:btc" not in engine.pending_entry_orders
    assert engine.cleared == ["indodax:btc"]
    assert len(engine.failures) == 1
    assert engine.states["indodax:btc"] == PositionState.RECONCILIATION_REQUIRED
