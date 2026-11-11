"""P0 regression coverage for independent spot/futures entry-leg recovery."""

import pytest

from executor.models import OrderRecord, OrderRole, RecoveryStatus
from tests.test_order_recovery import PAIR_TAG, _build, _entry_record


@pytest.mark.asyncio
async def test_terminal_futures_hedge_advances_parent_spot_watermark_once(fake_redis):
    engine = await _build(fake_redis)
    parent = _entry_record(
        filled_quantity=0.006,
        recovery_context={
            "usdt_idr_rate": 16000.0,
            "hedge_fill_orders": {},
        },
    )
    await engine.order_registry.register(parent)

    hedge = OrderRecord(
        local_order_id=OrderRecord.local_id_for("binance", "hedge-coid-1", "901"),
        client_order_id="hedge-coid-1",
        exchange_order_id="901",
        pair_key=PAIR_TAG,
        exchange="binance",
        market_type="perpetual",
        symbol="BTCUSDT",
        side="sell",
        order_type="market",
        order_role=OrderRole.HEDGE.value,
        original_quantity=0.006,
        filled_quantity=0.006,
        status="open",
        recovery_status=RecoveryStatus.ADOPTED.value,
        recovery_context={
            "parent_spot_exchange": "indodax",
            "parent_spot_order_id": "501",
            "parent_spot_client_order_id": "btc-idr-entry-7",
            "hedge_requested_for_spot_fill": 0.006,
        },
    )
    await engine.order_registry.register(hedge)

    venue_order = {
        "id": "901",
        "clientOrderId": "hedge-coid-1",
        "status": "closed",
        "amount": 0.006,
        "filled": 0.006,
    }
    assert await engine._reconcile_recovered_entry_hedge(hedge, venue_order, PAIR_TAG)
    restored_parent = await engine.order_registry.find_by_client_id(
        "indodax", "btc-idr-entry-7"
    )
    assert restored_parent.hedged_quantity == pytest.approx(0.006)
    assert restored_parent.recovery_context["hedge_fill_orders"]["hedge-coid-1"] == pytest.approx(0.006)

    # A repeated recovery pass sees the durable marker and cannot double hedge.
    assert await engine._reconcile_recovered_entry_hedge(hedge, venue_order, PAIR_TAG)
    restored_parent = await engine.order_registry.find_by_client_id(
        "indodax", "btc-idr-entry-7"
    )
    assert restored_parent.hedged_quantity == pytest.approx(0.006)


@pytest.mark.asyncio
async def test_futures_hedge_without_parent_spot_record_stays_blocked(fake_redis):
    engine = await _build(fake_redis)
    hedge = OrderRecord(
        local_order_id=OrderRecord.local_id_for("binance", "orphan-hedge", "902"),
        client_order_id="orphan-hedge",
        exchange_order_id="902",
        pair_key=PAIR_TAG,
        exchange="binance",
        market_type="perpetual",
        symbol="BTCUSDT",
        side="sell",
        order_type="market",
        order_role=OrderRole.HEDGE.value,
        original_quantity=0.002,
        filled_quantity=0.002,
        status="open",
        recovery_context={
            "parent_spot_exchange": "indodax",
            "parent_spot_order_id": "missing-spot-order",
            "parent_spot_client_order_id": "missing-spot-client-id",
        },
    )
    await engine.order_registry.register(hedge)

    assert not await engine._reconcile_recovered_entry_hedge(
        hedge,
        {"id": "902", "status": "closed", "filled": 0.002},
        PAIR_TAG,
    )
    pending = await engine.order_registry.pending_recovery_ids()
    assert hedge.local_order_id in pending
    assert PAIR_TAG in engine._recovery_scope_halts


@pytest.mark.asyncio
async def test_terminal_spot_recovery_preflights_existing_futures_hedge(fake_redis):
    engine = await _build(fake_redis)
    parent = _entry_record(
        filled_quantity=0.004,
        recovery_context={"usdt_idr_rate": 16000.0, "hedge_fill_orders": {}},
    )
    await engine.order_registry.register(parent)
    hedge = OrderRecord(
        local_order_id=OrderRecord.local_id_for("binance", "hedge-coid-preflight", "903"),
        client_order_id="hedge-coid-preflight",
        exchange_order_id="903",
        pair_key=PAIR_TAG,
        exchange="binance",
        market_type="perpetual",
        symbol="BTCUSDT",
        side="sell",
        order_type="market",
        order_role=OrderRole.HEDGE.value,
        original_quantity=0.004,
        status="open",
        recovery_context={
            "parent_spot_exchange": "indodax",
            "parent_spot_order_id": "501",
            "parent_spot_client_order_id": "btc-idr-entry-7",
            "hedge_requested_for_spot_fill": 0.004,
        },
    )
    await engine.order_registry.register(hedge)

    async def fetch_exact_futures_order(record, order_id, symbol):
        assert order_id == "903"
        assert symbol == "BTCUSDT"
        return {"id": "903", "status": "closed", "amount": 0.004, "filled": 0.004}

    engine._fetch_order_by_role = fetch_exact_futures_order
    spot_info = {
        "local_exchange": "indodax",
        "order_id": "501",
        "client_order_id": "btc-idr-entry-7",
        "hedged_short_qty": 0.0,
    }
    assert await engine._reconcile_pending_entry_hedges(PAIR_TAG, spot_info)
    assert spot_info["hedged_short_qty"] == pytest.approx(0.004)
    assert hedge.local_order_id not in await engine.order_registry.pending_recovery_ids()
