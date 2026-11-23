from types import SimpleNamespace

import pytest

from executor.trade_fill_reconciliation import TradeFillReconciliationWorker


class _Writer:
    def __init__(self):
        self.synced = []

    def mark_fill_sync(self, identity, quantity):
        self.synced.append((identity, quantity))
        return True


class _AttemptLedger:
    enabled = True

    def __init__(self, *, matched=True, finalize=True):
        self.matched = matched
        self.finalize = finalize
        self.trades = []
        self.finalizations = []

    async def record_trade_for_client_order_id(self, **kwargs):
        self.trades.append(kwargs)
        return {"inserted": True} if self.matched else None

    async def finalize_by_client_order_id(self, **kwargs):
        self.finalizations.append(kwargs)
        return self.finalize


@pytest.mark.asyncio
async def test_verified_terminal_hedge_trades_are_committed_and_attempt_finalized():
    ledger = _AttemptLedger()
    engine = SimpleNamespace(
        hedge_attempt_ledger=ledger,
        config=SimpleNamespace(mock_mode=False),
    )
    worker = TradeFillReconciliationWorker(engine)
    writer = _Writer()
    trades = [{
        "id": "trade-1",
        "amount": 0.25,
        "price": 100.0,
        "fee": {"cost": 0.001, "currency": "USDT"},
        "timestamp": 1795400000000,
    }]

    await worker._verify_candidate_fills(
        writer, "okx:perpetual:coid-1", "BTC-USDT-SWAP",
        "okx", "BTC-USDT-SWAP", "venue-order-1", "coid-1",
        0.25, trades, 0.25, [], order_status="closed",
    )

    assert len(ledger.trades) == 1
    assert ledger.trades[0]["client_order_id"] == "coid-1"
    assert ledger.finalizations == [{
        "client_order_id": "coid-1",
        "terminal_status": "closed",
        "order_reconciled": True,
        "fills_reconciled": True,
        "actor_id": "trade-fill-reconciliation",
    }]
    assert writer.synced == [("okx:perpetual:coid-1", 0.25)]


@pytest.mark.asyncio
async def test_open_hedge_order_records_verified_fills_without_terminalizing_attempt():
    ledger = _AttemptLedger()
    engine = SimpleNamespace(
        hedge_attempt_ledger=ledger,
        config=SimpleNamespace(mock_mode=False),
    )
    worker = TradeFillReconciliationWorker(engine)
    writer = _Writer()
    trades = [{
        "id": "trade-2",
        "amount": 0.1,
        "price": 100.0,
        "fee": {"cost": 0.0001, "currency": "USDT"},
    }]

    await worker._verify_candidate_fills(
        writer, "okx:perpetual:coid-2", "BTC-USDT-SWAP",
        "okx", "BTC-USDT-SWAP", "venue-order-2", "coid-2",
        0.1, trades, 0.1, [], order_status="open",
    )

    assert len(ledger.trades) == 1
    assert ledger.finalizations == []
    assert writer.synced == [("okx:perpetual:coid-2", 0.1)]


@pytest.mark.asyncio
async def test_non_hedge_order_does_not_touch_attempt_finalization():
    ledger = _AttemptLedger(matched=False)
    engine = SimpleNamespace(
        hedge_attempt_ledger=ledger,
        config=SimpleNamespace(mock_mode=False),
    )
    worker = TradeFillReconciliationWorker(engine)
    writer = _Writer()
    trades = [{
        "id": "trade-3",
        "amount": 0.1,
        "price": 100.0,
        "fee": {"cost": 0.0001, "currency": "USDT"},
    }]

    await worker._verify_candidate_fills(
        writer, "okx:perpetual:other-order", "BTC-USDT-SWAP",
        "okx", "BTC-USDT-SWAP", "venue-order-3", "other-order",
        0.1, trades, 0.1, [], order_status="closed",
    )

    assert len(ledger.trades) == 1
    assert ledger.finalizations == []
    assert writer.synced == [("okx:perpetual:other-order", 0.1)]
