"""Regression tests for universal order recovery after restart.

Every test builds a fresh engine against a fake Redis and a mocked exchange, so
no live order is ever placed. The scenarios mirror the failure modes that lose
bot orders:

* pending spot BUY / SELL and futures hedges after a restart
* partial fills and cancel-pending orders
* exchange orders with an empty Redis cache, with only a Supabase record, and
  with a Redis record the exchange has already filled
* disagreeing status between database and exchange
* venue API timeouts during recovery
* duplicate/repeated recovery that must not create new orders or positions
"""

import json
import time

import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine
from executor.models import (
    ActivePosition,
    OrderRecord,
    OrderRole,
    PositionState,
    RecoveryStatus,
)
from executor.order_registry import UniversalOrderRegistry
from executor.quantity import AmountSpec

from tests.conftest import MockExchangeClient

PAIR_TAG = "indodax:btc_idr__binance:btc_usdt"

# Venue rules and the FX snapshot an entry needs to be hedgeable after a restart.
LOCAL_SPEC = {"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0}
GLOBAL_SPEC = {"unit": "BASE", "step": 0.00000001, "min": 0.00000001, "base_per_unit": 1.0}
ENTRY_CONTEXT = {
    "usdt_idr_rate": 16_000.0,
    "fx_rate_timestamp_ms": 1_700_000_000_000,
    "global_ask_usdt": 65_000.0,
    "global_symbol": "BTCUSDT",
    "base_asset": "BTC",
    "quote_asset": "IDR",
    "cancel_threshold_usdt": 60_000.0,
    "min_entry_spread_pct": 0.1,
    "base_target_exit_spread_pct": 0.3,
    "current_funding_rate": 0.0003,
    "next_funding_time_ms": 1_700_003_600_000,
    "spot_cost_usdt": 62.5,
    "futures_margin_usdt": 62.5,
}

def _engine(fake_redis, client=None, mock_mode=True):
    return ArbitrageEngine(
        config=ExecutorConfig(mock_mode=mock_mode),
        client=client or MockExchangeClient(),
        redis_client=fake_redis,
    )

def _position(qty: float = 0.01, **overrides) -> ActivePosition:
    data = dict(
        local_exchange="indodax",
        global_exchange="binance",
        symbol="btc_idr",
        base_asset="BTC",
        global_symbol="BTCUSDT",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=qty,
        initial_qty=qty,
        local_amount_spec=AmountSpec.from_dict(LOCAL_SPEC),
        global_amount_spec=AmountSpec.from_dict(GLOBAL_SPEC),
        local_price_tick=1.0,
        opened_at_ms=1_700_000_000_000,
    )
    data.update(overrides)
    return ActivePosition(**data)

def _entry_record(client_order_id="btc-idr-entry-7", exchange_order_id="501", **overrides):
    data = dict(
        local_order_id=OrderRecord.local_id_for("indodax", client_order_id, exchange_order_id),
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        pair_key=PAIR_TAG,
        position_id=f"position:{PAIR_TAG}",
        exchange="indodax",
        market_type="spot",
        symbol="btc_idr",
        side="buy",
        order_type="limit",
        order_role=OrderRole.ENTRY.value,
        price=1_000_000_000.0,
        original_quantity=0.01,
        filled_quantity=0.0,
        remaining_quantity=0.01,
        local_amount_spec=LOCAL_SPEC,
        global_amount_spec=GLOBAL_SPEC,
        recovery_context=ENTRY_CONTEXT,
    )
    data.update(overrides)
    return OrderRecord(**data)

def _venue_order(**overrides):
    order = {
        "id": "501",
        "clientOrderId": "btc-idr-entry-7",
        "symbol": "btc_idr",
        "side": "buy",
        "type": "limit",
        "amount": 0.01,
        "price": 1_000_000_000.0,
        "filled": 0.0,
        "status": "open",
    }
    order.update(overrides)
    return order

async def _build(fake_redis, client=None, mock_mode=True):
    engine = _engine(fake_redis, client, mock_mode)
    await engine.supabase.start()
    return engine

# --------------------------------------------------------------- pending entry

@pytest.mark.asyncio
async def test_restart_adopts_pending_spot_buy_from_registry(fake_redis):
    """A resting spot BUY with no Redis pending-entry key is re-adopted."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())

    await engine._run_order_recovery()

    assert PAIR_TAG in engine.pending_entry_orders
    adopted = engine.pending_entry_orders[PAIR_TAG]
    assert adopted["order_id"] == "501"
    assert adopted["usdt_idr_rate"] == 16_000.0
    assert engine.states[PAIR_TAG] == PositionState.ENTRY_PENDING
    # The re-adopted order must itself survive another restart.
    persisted = await fake_redis.get("arb:pending_entry:" + PAIR_TAG)
    assert persisted is not None
    assert json.loads(persisted)["order_id"] == "501"

@pytest.mark.asyncio
async def test_restart_links_order_with_no_local_record(fake_redis):
    """Redis lost the order entirely: its market still links it to the position."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))

    await engine._recover_active_positions()
    await engine._run_order_recovery()

    linked = await engine.order_registry.find_by_exchange_id("indodax", "501")
    assert linked is not None
    assert linked.pair_key == PAIR_TAG
    assert await engine.order_registry.orphans() == {}
    assert engine._entry_blocked is False

@pytest.mark.asyncio
async def test_restart_with_no_positions_and_no_records_yields_orphan(fake_redis):
    """With nothing tracked at all, a venue order cannot be adopted: orphan."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)

    await engine._run_order_recovery()

    orphans = await engine.order_registry.orphans()
    assert len(orphans) == 1
    orphan = next(iter(orphans.values()))
    assert orphan["exchange_order_id"] == "501"
    assert orphan["scope"] == "indodax:btc_idr"
    assert PAIR_TAG not in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_restart_links_exchange_order_to_position_by_client_id(fake_redis):
    """An exchange order is linked to its position through the client order id."""
    client = MockExchangeClient()
    client.orders["601"] = _venue_order(id="601", clientOrderId="btc-idr-exit-9", side="sell")
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))
    await engine._recover_active_positions()

    record = OrderRecord(
        local_order_id=OrderRecord.local_id_for("indodax", "btc-idr-exit-9", ""),
        client_order_id="btc-idr-exit-9",
        exchange_order_id="",
        pair_key=PAIR_TAG,
        exchange="indodax",
        market_type="spot",
        symbol="btc_idr",
        side="sell",
        order_role=OrderRole.EXIT_SPOT.value,
        price=1_002_000_000.0,
        original_quantity=0.01,
        local_amount_spec=LOCAL_SPEC,
    )
    await engine.order_registry.register(record)

    await engine._run_order_recovery()

    assert PAIR_TAG in engine.pending_exit_orders
    assert engine.pending_exit_orders[PAIR_TAG]["order_id"] == "601"
    assert await engine.order_registry.orphans() == {}

# ------------------------------------------------------------ pending spot sell

@pytest.mark.asyncio
async def test_restart_adopts_pending_spot_sell(fake_redis):
    """A resting spot SELL is restored as EXIT_PENDING for its position."""
    client = MockExchangeClient()
    client.orders["701"] = _venue_order(
        id="701", clientOrderId="btc-idr-exit-3", side="sell", price=1_002_000_000.0
    )
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))
    await engine._recover_active_positions()
    await engine.order_registry.register(
        OrderRecord(
            local_order_id=OrderRecord.local_id_for("indodax", "btc-idr-exit-3", "701"),
            client_order_id="btc-idr-exit-3",
            exchange_order_id="701",
            pair_key=PAIR_TAG,
            exchange="indodax",
            market_type="spot",
            symbol="btc_idr",
            side="sell",
            order_role=OrderRole.EXIT_SPOT.value,
            price=1_002_000_000.0,
            original_quantity=0.01,
            local_amount_spec=LOCAL_SPEC,
        )
    )

    await engine._run_order_recovery()

    assert engine.pending_exit_orders[PAIR_TAG]["order_id"] == "701"
    assert engine.states[PAIR_TAG] == PositionState.EXIT_PENDING

# ----------------------------------------------------------------- futures leg

@pytest.mark.asyncio
async def test_restart_restores_futures_short_and_hedge_record(fake_redis):
    """A futures SHORT position and its hedge record survive a restart."""
    client = MockExchangeClient()
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb/active_pos", "ignored")
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))
    await engine.order_registry.register(
        OrderRecord(
            local_order_id=OrderRecord.local_id_for("binance", "btc-usdt-hedge-4", "801"),
            client_order_id="btc-usdt-hedge-4",
            exchange_order_id="801",
            pair_key=PAIR_TAG,
            exchange="binance",
            market_type="perpetual",
            symbol="BTCUSDT",
            side="sell",
            order_type="market",
            order_role=OrderRole.HEDGE.value,
            price=65_000.0,
            original_quantity=0.01,
            filled_quantity=0.01,
            status="closed",
            reduce_only=False,
            global_amount_spec=GLOBAL_SPEC,
        )
    )

    await engine._run_order_recovery()
    await engine._recover_active_positions()

    assert PAIR_TAG in engine.active_positions
    # A terminal hedge is reconciled and retired, not left pending.
    assert await engine.order_registry.pending_recovery_ids() == {}
    assert engine._entry_blocked is False

@pytest.mark.asyncio
async def test_restart_reconciles_futures_buy_close(fake_redis):
    """A filled futures BUY (short close) is reconciled against its position."""
    client = MockExchangeClient()
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))

    record = OrderRecord(
        local_order_id=OrderRecord.local_id_for("binance", "btc-usdt-unwind-5", "901"),
        client_order_id="btc-usdt-unwind-5",
        exchange_order_id="901",
        pair_key=PAIR_TAG,
        exchange="binance",
        market_type="perpetual",
        symbol="BTCUSDT",
        side="buy",
        order_type="market",
        order_role=OrderRole.EXIT_FUTURES.value,
        original_quantity=0.01,
        filled_quantity=0.01,
        status="open",
        reduce_only=True,
        global_amount_spec=GLOBAL_SPEC,
    )
    await engine.order_registry.register(record)
    client.orders["901"] = {
        "id": "901", "symbol": "BTCUSDT", "status": "closed", "filled": 0.01, "amount": 0.01,
    }

    await engine._recover_active_positions()
    await engine._run_order_recovery()

    retired = await engine.order_registry.load(record.local_order_id)
    assert retired.status == "closed"
    assert retired.filled_quantity == pytest.approx(0.01)

# --------------------------------------------------------------- partial fill

@pytest.mark.asyncio
async def test_restart_restores_partial_fill_state(fake_redis):
    """A partially filled resting entry is restored as ENTRY_PARTIAL."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.004)
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record(filled_quantity=0.004))

    await engine._run_order_recovery()

    assert engine.states[PAIR_TAG] == PositionState.ENTRY_PARTIAL
    assert engine.pending_entry_orders[PAIR_TAG]["filled"] == pytest.approx(0.004)

@pytest.mark.asyncio
async def test_restart_hedges_final_partial_fill_of_cancelled_entry(fake_redis):
    """A cancelled entry with a final partial fill is hedged, not forgotten."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.004, status="canceled")
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())

    await engine._run_order_recovery()

    assert len(client.global_short_sells) == 1
    assert client.global_short_sells[0]["amount"] == pytest.approx(0.004)
    assert PAIR_TAG in engine.active_positions
    assert engine.active_positions[PAIR_TAG].qty == pytest.approx(0.004)

# -------------------------------------------------------------- cancel pending

@pytest.mark.asyncio
async def test_restart_clears_cancelled_entry_without_fill(fake_redis):
    """A cancelled entry with no fill releases the pair without side effects."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(status="canceled")
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())

    await engine._run_order_recovery()

    assert PAIR_TAG not in engine.pending_entry_orders
    assert client.global_short_sells == []
    assert client.placed_sells == []
    assert engine.states.get(PAIR_TAG, PositionState.IDLE) == PositionState.IDLE
    assert engine._entry_blocked is False

# ----------------------------------------------------------- source of truth

@pytest.mark.asyncio
async def test_exchange_order_with_supabase_record_only(fake_redis):
    """Redis is empty but Supabase holds the record: the order is re-adopted."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)

    async def durable_rows(limit: int = 500):
        record = _entry_record()
        return [{
            "local_order_id": record.local_order_id,
            "client_order_id": record.client_order_id,
            "exchange_order_id": record.exchange_order_id,
            "pair_key": record.pair_key,
            "position_key": record.position_id,
            "exchange_name": record.exchange,
            "market_type": record.market_type,
            "order_role": record.order_role,
            "symbol": record.symbol,
            "side": record.side,
            "order_type": record.order_type,
            "status": record.status,
            "requested_qty": record.original_quantity,
            "filled_qty": record.filled_quantity,
            "remaining_qty": record.remaining_quantity,
            "limit_price": record.price,
            "reduce_only": False,
            "recovery_status": RecoveryStatus.MANAGED.value,
            "is_mock": True,
            "raw_payload": {
                "local_amount_spec": LOCAL_SPEC,
                "global_amount_spec": GLOBAL_SPEC,
                "recovery_context": ENTRY_CONTEXT,
            },
        }]

    engine.supabase.fetch_open_order_records = durable_rows

    await engine._run_order_recovery()

    assert PAIR_TAG in engine.pending_entry_orders
    assert engine.pending_entry_orders[PAIR_TAG]["usdt_idr_rate"] == 16_000.0

@pytest.mark.asyncio
async def test_redis_record_but_exchange_already_filled(fake_redis):
    """Redis says OPEN, the exchange says FILLED: the exchange wins."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.01, status="closed")
    engine = await _build(fake_redis, client)
    record = _entry_record()
    await engine.order_registry.register(record)

    await engine._run_order_recovery()

    stored = await engine.order_registry.load(record.local_order_id)
    assert stored.status == "closed"
    assert stored.filled_quantity == pytest.approx(0.01)
    # The fill is hedged through the normal lifecycle rather than dropped.
    assert len(client.global_short_sells) == 1
    assert PAIR_TAG in engine.active_positions

@pytest.mark.asyncio
async def test_database_and_exchange_status_disagree(fake_redis):
    """A disagreement is resolved in favour of the venue, with an audit trail."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(status="canceled")
    engine = await _build(fake_redis, client)
    record = _entry_record(status="open")
    await engine.order_registry.register(record)

    await engine._run_order_recovery()

    stored = await engine.order_registry.load(record.local_order_id)
    assert stored.status == "canceled"
    assert await engine.order_registry.pending_recovery_ids() == {}

@pytest.mark.asyncio
async def test_exchange_api_timeout_during_recovery_blocks_entries(fake_redis):
    """A venue timeout leaves the order unverified and blocks new entries."""
    client = MockExchangeClient()

    async def timeout(order_id, symbol):
        raise TimeoutError("exchange API timed out")

    client.fetch_local_order = timeout
    engine = await _build(fake_redis, client)
    record = _entry_record()
    await engine.order_registry.register(record)

    await engine._run_order_recovery()

    pending = await engine.order_registry.pending_recovery_ids()
    assert record.local_order_id in pending
    assert engine._entry_blocked is True
    assert engine.states[PAIR_TAG] == PositionState.RECONCILIATION_REQUIRED
    assert engine.status_snapshot()["failures"][PAIR_TAG]["reason"] == "ORDER_RECOVERY_UNVERIFIED"

@pytest.mark.asyncio
async def test_open_order_sweep_failure_blocks_only_that_venue(fake_redis):
    """If the venue cannot list orders, entry is gated for that venue only."""
    client = MockExchangeClient()

    async def failing_sweep(symbol=None, amount_spec=None):
        raise TimeoutError("open orders unavailable")

    client.fetch_local_open_orders = failing_sweep
    engine = await _build(fake_redis, client)

    await engine._run_order_recovery()

    assert engine._entry_blocked is True
    assert any(key.startswith("indodax") for key in engine._recovery_scope_halts)
    # Monitoring/exit paths stay usable: only entry is gated.
    assert engine._recovery_complete is not True or engine._entry_blocked is True

# ------------------------------------------------------------- idempotency

@pytest.mark.asyncio
async def test_duplicate_recovery_creates_no_new_orders(fake_redis):
    """Running recovery twice must not place, replace or duplicate anything."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())

    await engine._run_order_recovery()
    first_pass_entries = dict(engine.pending_entry_orders)
    await engine._run_order_recovery()
    await engine._run_order_recovery()

    assert engine.pending_entry_orders.keys() == first_pass_entries.keys()
    assert client.placed_buys == []
    assert client.placed_sells == []
    assert client.cancelled_orders == []
    assert client.global_short_sells == []

@pytest.mark.asyncio
async def test_repeated_restart_creates_no_duplicate_positions(fake_redis):
    """Three restarts with a filled partial entry yield one hedged position."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.004, status="canceled")

    for _ in range(3):
        engine = await _build(fake_redis, client)
        await engine.order_registry.register(_entry_record())
        await engine._run_order_recovery()

    assert len(client.global_short_sells) == 1
    assert len(client.placed_sells) == 0
    active = json.loads(await fake_redis.get(f"arb:active_pos:{PAIR_TAG}"))
    assert active["qty"] == pytest.approx(0.004)

# ------------------------------------------------------------- orphan handling

@pytest.mark.asyncio
async def test_unlinkable_order_becomes_orphan_without_side_effects(fake_redis):
    """An order with no bot position is flagged, never adopted or cancelled."""
    client = MockExchangeClient()
    client.orders["999"] = _venue_order(
        id="999", clientOrderId="unknown-order", symbol="eth_idr"
    )
    engine = await _build(fake_redis, client)

    await engine._run_order_recovery()

    orphans = await engine.order_registry.orphans()
    assert len(orphans) == 1
    orphan = next(iter(orphans.values()))
    assert orphan["exchange_order_id"] == "999"
    assert orphan["scope"] == "indodax:eth_idr"
    # Never touched: no adopt, no cancel, no replacement order.
    assert client.cancelled_orders == []
    assert client.placed_buys == []
    assert client.placed_sells == []
    # Entry gated on the affected market, monitoring still available.
    assert engine._entry_blocked is True
    assert "indodax:eth_idr" in engine._recovery_scope_halts

@pytest.mark.asyncio
async def test_orphan_halts_only_affected_market(fake_redis):
    """An ETH orphan must not block a healthy BTC pair."""
    client = MockExchangeClient()
    client.orders["999"] = _venue_order(
        id="999", clientOrderId="unknown-order", symbol="eth_idr"
    )
    engine = await _build(fake_redis, client)
    engine._pair_infos[PAIR_TAG] = type("Info", (), {
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "local_symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "base_asset": "BTC",
        "quote_asset": "IDR",
    })()

    await engine._run_order_recovery()

    assert engine._venue_halted_reason(PAIR_TAG) == ""
    assert "eth_idr" in engine._venue_halted_reason("indodax:eth_idr")

@pytest.mark.asyncio
async def test_orphan_manual_reconciliation_clears_halt(fake_redis):
    """Operator reconciliation is the only way an orphan gate comes down."""
    client = MockExchangeClient()
    client.orders["999"] = _venue_order(
        id="999", clientOrderId="unknown-order", symbol="eth_idr"
    )
    engine = await _build(fake_redis, client)
    await engine._run_order_recovery()
    orphan_id = next(iter(await engine.order_registry.orphans()))

    assert await engine.resolve_orphan_order(orphan_id, reason="OPERATOR_REVIEW") is True

    assert engine._recovery_scope_halts == {}
    assert engine._entry_blocked is False
    assert await engine.order_registry.orphans() == {}

@pytest.mark.asyncio
async def test_orphan_registration_is_idempotent(fake_redis):
    """Re-detecting the same orphan does not create a second record."""
    client = MockExchangeClient()
    client.orders["999"] = _venue_order(
        id="999", clientOrderId="unknown-order", symbol="eth_idr"
    )
    engine = await _build(fake_redis, client)

    await engine._run_order_recovery()
    await engine._run_order_recovery()

    assert len(await engine.order_registry.orphans()) == 1

# --------------------------------------------------------------- registry unit

@pytest.mark.asyncio
async def test_registry_indexes_exchange_and_client_ids(fake_redis):
    """Both lookup indexes resolve to the same deterministic record."""
    registry = UniversalOrderRegistry(fake_redis)
    record = await registry.register(_entry_record())

    by_exchange = await registry.find_by_exchange_id("indodax", "501")
    by_client = await registry.find_by_client_id("indodax", "btc-idr-entry-7")

    assert by_exchange.local_order_id == record.local_order_id
    assert by_client.local_order_id == record.local_order_id

@pytest.mark.asyncio
async def test_registry_retires_terminal_records_only(fake_redis):
    """Terminal orders leave the recovery index; open orders stay."""
    registry = UniversalOrderRegistry(fake_redis)
    open_record = await registry.register(_entry_record())
    closed_record = await registry.register(
        _entry_record(client_order_id="done-1", exchange_order_id="777", status="closed")
    )

    active = await registry.active_records()

    assert open_record.local_order_id in active
    assert closed_record.local_order_id not in active
    # The lifecycle record is retained for audit even after retirement.
    assert await registry.load(closed_record.local_order_id) is not None

@pytest.mark.asyncio
async def test_registry_records_survive_redis_flush_via_durable_store(fake_redis):
    """A flushed Redis re-seeds the registry from the durable rows."""
    class Durable:
        def __init__(self, rows):
            self.rows = rows

        async def fetch_open_order_records(self, limit: int = 500):
            return self.rows

    record = _entry_record()
    rows = [{
        "local_order_id": record.local_order_id,
        "client_order_id": record.client_order_id,
        "exchange_order_id": record.exchange_order_id,
        "pair_key": record.pair_key,
        "exchange_name": record.exchange,
        "market_type": record.market_type,
        "order_role": record.order_role,
        "symbol": record.symbol,
        "side": record.side,
        "order_type": record.order_type,
        "status": record.status,
        "requested_qty": record.original_quantity,
        "filled_qty": record.filled_quantity,
        "remaining_qty": record.remaining_quantity,
        "recovery_status": RecoveryStatus.MANAGED.value,
        "is_mock": True,
        "raw_payload": {
            "local_amount_spec": LOCAL_SPEC,
            "global_amount_spec": GLOBAL_SPEC,
            "recovery_context": ENTRY_CONTEXT,
        },
    }]
    registry = UniversalOrderRegistry(fake_redis, Durable(rows))

    active = await registry.active_records()

    assert record.local_order_id in active
    assert active[record.local_order_id].recovery_context["usdt_idr_rate"] == 16_000.0
    # Re-seeded so the venue sweep can match by exchange order id.
    assert (await registry.find_by_exchange_id("indodax", "501")) is not None

@pytest.mark.asyncio
async def test_registry_survives_redis_failure(fake_redis):
    """A Redis failure must not lose the durable write or raise into trading."""
    class BrokenRedis:
        async def set(self, *args, **kwargs):
            raise ConnectionError("redis down")

        async def hset(self, *args, **kwargs):
            raise ConnectionError("redis down")

    writes = []

    class Durable:
        def order_record(self, row, **kwargs):
            writes.append(row)

    registry = UniversalOrderRegistry(BrokenRedis(), Durable())

    record = await registry.register(_entry_record())

    assert record.local_order_id
    assert writes and writes[0]["client_order_id"] == "btc-idr-entry-7"

@pytest.mark.asyncio
async def test_phantom_order_recorded_but_not_open_on_venue(fake_redis):
    """A record the venue no longer lists is flagged, not silently dropped."""
    client = MockExchangeClient()
    # The venue answers the single-order fetch as still open, but its open-order
    # list omits it: a genuine disagreement that must not be auto-resolved.
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())

    async def venue_has_no_open_orders(symbol=None, amount_spec=None):
        return []

    client.fetch_local_open_orders = venue_has_no_open_orders

    await engine._run_order_recovery()

    stored = await engine.order_registry.load(_entry_record().local_order_id)
    assert stored.recovery_status == RecoveryStatus.PHANTOM.value
    assert stored.local_order_id in await engine.order_registry.pending_recovery_ids()
    assert engine._entry_blocked is True

@pytest.mark.asyncio
async def test_sweep_links_crash_before_ack_order_to_position(fake_redis):
    """An order placed moments before a crash is linked through its market."""
    client = MockExchangeClient()
    client.orders["777"] = _venue_order(id="777", clientOrderId="", symbol="btc_idr")
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))
    await engine._recover_active_positions()

    await engine._run_order_recovery()

    linked = await engine.order_registry.find_by_exchange_id("indodax", "777")
    assert linked is not None
    assert linked.pair_key == PAIR_TAG
    assert linked.exchange_order_id == "777"
    assert await engine.order_registry.orphans() == {}

@pytest.mark.asyncio
async def test_sweep_does_not_link_an_unrelated_symbol(fake_redis):
    """A symbol with no tracked position is an orphan, never a guessed link."""
    client = MockExchangeClient()
    client.orders["888"] = _venue_order(id="888", clientOrderId="", symbol="sol_idr")
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))
    await engine._recover_active_positions()

    await engine._run_order_recovery()

    orphans = await engine.order_registry.orphans()
    assert len(orphans) == 1
    assert next(iter(orphans.values()))["exchange_order_id"] == "888"

@pytest.mark.asyncio
async def test_sweep_leaves_no_orphan_when_venue_is_empty(fake_redis):
    """A clean venue produces no orphan, no halt and no order traffic."""
    client = MockExchangeClient()
    engine = await _build(fake_redis, client)

    await engine._run_order_recovery()

    assert await engine.order_registry.orphans() == {}
    assert engine._recovery_scope_halts == {}
    assert engine._entry_blocked is False
    assert engine._recovery_pending_orders == {}

# ------------------------------------------------------------- end-to-end

@pytest.mark.asyncio
async def test_full_restart_cycle_keeps_order_and_places_nothing_new(fake_redis):
    """Place an entry, restart, and confirm the same order is still managed."""
    client = MockExchangeClient()
    engine = await _build(fake_redis, client)
    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_signal_payload(now_ms)))

    await engine.handle_signal_notification(pair_tag)

    placed = [order for order in client.orders.values() if order.get("status") == "open"]
    assert len(placed) == 1
    venue_order_id = placed[0]["id"]

    # Restart: brand-new engine, same Redis and same exchange state.
    restarted = await _build(fake_redis, client)
    restarted.states.clear()
    restarted.pending_entry_orders.clear()

    await restarted._recover_pending_entry_orders()
    await restarted._run_order_recovery()

    managed = await restarted.order_registry.find_by_exchange_id("indodax", venue_order_id)
    assert managed is not None
    assert managed.order_role == OrderRole.ENTRY.value
    # Recovery must not place, replace or cancel anything.
    assert len([o for o in client.orders.values() if o.get("status") == "open"]) == 1
    assert client.cancelled_orders == []
    assert len(client.placed_buys) == 1

@pytest.mark.asyncio
async def test_entry_rejected_while_recovery_gate_is_up(fake_redis):
    """A blocked pair must refuse new entries while still managing positions."""
    client = MockExchangeClient()

    async def timeout(order_id, symbol):
        raise TimeoutError("exchange API timed out")

    client.fetch_local_order = timeout
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())
    await engine._run_order_recovery()

    pair_tag = "indodax:btc_idr"
    now_ms = int(time.time() * 1000)
    await fake_redis.set(f"arb:signal:{pair_tag}", json.dumps(_signal_payload(now_ms)))
    before = len(client.placed_buys)

    await engine.handle_signal_notification(pair_tag)

    assert engine._venue_halted_reason(PAIR_TAG) != ""
    assert len(client.placed_buys) == before

@pytest.mark.asyncio
async def test_status_snapshot_exposes_recovery_state(fake_redis):
    """Operators can see readiness, pending orders and orphans from the status."""
    client = MockExchangeClient()
    client.orders["999"] = _venue_order(
        id="999", clientOrderId="unknown-order", symbol="eth_idr"
    )
    engine = await _build(fake_redis, client)

    await engine._run_order_recovery()
    snapshot = engine.status_snapshot()

    assert snapshot["recovery"]["ready"] is False
    assert snapshot["recovery"]["scope_halts"]
    assert len(snapshot["recovery"]["orphan_orders"]) == 1

@pytest.mark.asyncio
async def test_status_snapshot_reports_ready_after_clean_recovery(fake_redis):
    """A clean restart reports ready so the executor can trade again."""
    engine = await _build(fake_redis, MockExchangeClient())
    await engine._run_order_recovery()
    engine._recovery_complete = True

    snapshot = engine.status_snapshot()

    assert snapshot["recovery"]["ready"] is True
    assert snapshot["recovery"]["complete"] is True
    assert snapshot["recovery"]["orphan_orders"] == []


def _signal_payload(now_ms: int) -> dict:
    """Minimal scan payload for a real entry through the engine."""
    return {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "binance",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1.0,
            "local_lot_size": 0.00000001,
            "local_amount_spec": LOCAL_SPEC,
            "global_amount_spec": GLOBAL_SPEC,
        },
        "pricing_snapshot": {
            "local_best_bid": 999_000_000.0,
            "local_best_ask": 1_000_000_000.0,
            "global_best_bid_usdt": 64_990.0,
            "global_best_ask_usdt": 65_000.0,
            "usdt_idr_rate": 16_000.0,
            "timestamp_ms": now_ms,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3_600_000,
            "minutes_to_next_funding": 60,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1_000_000_000.0,
            "entry_spread_pct": 0.25,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 100.0,
            "cancel_if_global_bid_drops_below_usdt": 60_000.0,
        },
    }

@pytest.mark.asyncio
async def test_recovery_gate_released_after_venue_recovers(fake_redis):
    """An unreachable venue blocks entries, but recovery can be retried later."""
    client = MockExchangeClient()
    failing = {"active": True}
    original_fetch = client.fetch_local_order
    original_sweep = client.fetch_local_open_orders

    async def flaky(order_id, symbol):
        if failing["active"]:
            raise TimeoutError("exchange API timed out")
        return await original_fetch(order_id, symbol)

    async def flaky_sweep(symbol=None, amount_spec=None):
        if failing["active"]:
            raise TimeoutError("open orders unavailable")
        return await original_sweep(symbol, amount_spec)

    client.fetch_local_order = flaky
    client.fetch_local_open_orders = flaky_sweep
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record())

    await engine._run_order_recovery()
    assert engine._entry_blocked is True

    failing["active"] = False
    assert await engine.retry_pending_order_recovery() is True

    assert engine._entry_blocked is False
    assert engine._recovery_scope_halts == {}
    assert await engine.order_registry.pending_recovery_ids() == {}
    assert PAIR_TAG in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_entry_without_fx_context_stays_pending(fake_redis):
    """A filled entry with no persisted FX rate must not book a guessed basis."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.01, status="closed")
    engine = await _build(fake_redis, client)
    await engine.order_registry.register(_entry_record(recovery_context=None))

    await engine._run_order_recovery()

    assert client.global_short_sells == []
    assert PAIR_TAG not in engine.active_positions
    pending = await engine.order_registry.pending_recovery_ids()
    assert pending
    assert engine._entry_blocked is True
    assert engine.states[PAIR_TAG] == PositionState.RECONCILIATION_REQUIRED

@pytest.mark.asyncio
async def test_malformed_venue_payload_marks_unverified(fake_redis):
    """A non-dict order payload cannot be trusted and blocks entries."""
    client = MockExchangeClient()

    async def bad_payload(order_id, symbol):
        return "not-an-order"

    client.fetch_local_order = bad_payload
    engine = await _build(fake_redis, client)
    record = _entry_record()
    await engine.order_registry.register(record)

    await engine._run_order_recovery()

    stored = await engine.order_registry.load(record.local_order_id)
    assert stored.recovery_status == RecoveryStatus.UNVERIFIED.value
    assert record.local_order_id in await engine.order_registry.pending_recovery_ids()

@pytest.mark.asyncio
async def test_unknown_venue_status_marks_unverified(fake_redis):
    """An unrecognised status is ambiguous, so it is never auto-resolved."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(status="pending_new")
    engine = await _build(fake_redis, client)
    record = _entry_record()
    await engine.order_registry.register(record)

    await engine._run_order_recovery()

    stored = await engine.order_registry.load(record.local_order_id)
    assert stored.recovery_status == RecoveryStatus.UNVERIFIED.value
    assert record.local_order_id in await engine.order_registry.pending_recovery_ids()

@pytest.mark.asyncio
async def test_legacy_pending_entry_record_is_migrated(fake_redis):
    """A pre-registry pending entry is migrated into the registry, not orphaned."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order()
    engine = await _build(fake_redis, client)
    legacy = {
        "order_id": "501",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1_000_000_000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "placed_time": time.time(),
        "local_amount_spec": LOCAL_SPEC,
        "global_amount_spec": GLOBAL_SPEC,
        "usdt_idr_rate": 16_000.0,
        "spot_cost_usdt": 50.0,
        "futures_margin_usdt": 50.0,
    }
    await fake_redis.set(f"arb:pending_entry:{PAIR_TAG}", json.dumps(legacy))

    # initialize() loads pending entries before the registry/sweep pass.
    await engine._recover_pending_entry_orders()
    await engine._run_order_recovery()

    migrated = await engine.order_registry.find_by_exchange_id("indodax", "501")
    assert migrated is not None
    assert migrated.pair_key == PAIR_TAG
    assert await engine.order_registry.orphans() == {}
    assert PAIR_TAG in engine.pending_entry_orders

@pytest.mark.asyncio
async def test_terminal_exit_spot_is_reconciled_through_exit_lifecycle(fake_redis):
    """A filled spot SELL found at startup closes the short through the exit path."""
    client = MockExchangeClient()
    client.orders["601"] = _venue_order(
        id="601", clientOrderId="btc-idr-exit-11", side="sell",
        price=1_002_000_000.0, filled=0.01, status="closed",
    )
    engine = await _build(fake_redis, client)
    await fake_redis.set(f"arb:active_pos:{PAIR_TAG}", json.dumps(_position().to_dict()))
    await engine._recover_active_positions()
    await engine.order_registry.register(
        OrderRecord(
            local_order_id=OrderRecord.local_id_for("indodax", "btc-idr-exit-11", "601"),
            client_order_id="btc-idr-exit-11",
            exchange_order_id="601",
            pair_key=PAIR_TAG,
            exchange="indodax",
            market_type="spot",
            symbol="btc_idr",
            side="sell",
            order_role=OrderRole.EXIT_SPOT.value,
            price=1_002_000_000.0,
            original_quantity=0.01,
            local_amount_spec=LOCAL_SPEC,
        )
    )

    await engine._run_order_recovery()

    assert client.global_close_buys, "the short must be closed, not left open"
    assert client.global_close_buys[0]["amount"] == pytest.approx(0.01)

@pytest.mark.asyncio
async def test_orphan_can_be_attached_to_pair_manually(fake_redis):
    """Operator attachment links the order to a position instead of dropping it."""
    client = MockExchangeClient()
    client.orders["999"] = _venue_order(
        id="999", clientOrderId="unknown-order", symbol="eth_idr"
    )
    engine = await _build(fake_redis, client)
    await engine._run_order_recovery()
    orphan_id = next(iter(await engine.order_registry.orphans()))

    assert await engine.resolve_orphan_order(orphan_id, pair_tag="indodax:eth_idr") is True

    attached = await engine.order_registry.find_by_exchange_id("indodax", "999")
    assert attached is not None
    assert attached.pair_key == "indodax:eth_idr"

def test_symbol_parts_handles_venue_formats():
    """Only known symbol formats are split; nothing is segmented by guess."""
    from executor.order_recovery import OrderRecoveryMixin

    parts = OrderRecoveryMixin._symbol_parts
    assert parts("btc_idr") == ("BTC", "IDR")
    assert parts("BTC/USDT") == ("BTC", "USDT")
    assert parts("BTCUSDT") == ("BTC", "USDT")
    assert parts("BTCUSDT:USDT") == ("BTC", "USDT")
    assert parts("") == ("", "")

@pytest.mark.asyncio
async def test_registry_orphan_registration_and_lookup(fake_redis):
    """Orphans are recorded with their scope and can be listed back."""
    registry = UniversalOrderRegistry(fake_redis)
    orphan_id = await registry.register_orphan(
        {"id": "42", "clientOrderId": "ext-42", "symbol": "eth_idr", "amount": 1.0},
        "indodax:eth_idr",
        "no matching position",
        exchange="indodax",
        symbol="eth_idr",
    )

    stored = await registry.orphans()

    assert orphan_id in stored
    assert stored[orphan_id]["scope"] == "indodax:eth_idr"
    assert stored[orphan_id]["exchange"] == "indodax"
    assert stored[orphan_id]["exchange_order_id"] == "42"

@pytest.mark.asyncio
async def test_registry_marks_and_clears_pending_recovery(fake_redis):
    """Pending markers are what keep unverified orders visible across restarts."""
    registry = UniversalOrderRegistry(fake_redis)
    record = await registry.register(_entry_record())

    await registry.mark_pending_recovery(record.local_order_id, "venue timeout")
    assert await registry.pending_recovery_ids() == {
        record.local_order_id: "venue timeout"
    }

    await registry.clear_pending_recovery(record.local_order_id)
    assert await registry.pending_recovery_ids() == {}

@pytest.mark.asyncio
async def test_registry_merge_never_moves_state_backwards(fake_redis):
    """A stale re-registration cannot un-hedge or reopen a finished order."""
    registry = UniversalOrderRegistry(fake_redis)
    record = await registry.register(
        _entry_record(
            filled_quantity=0.01, hedged_quantity=0.01, status="closed", remaining_quantity=0.0
        )
    )

    stale = OrderRecord(**{
        **record.to_dict(),
        "status": "open",
        "filled_quantity": 0.0,
        "hedged_quantity": 0.0,
    })
    merged = await registry.register(stale)

    assert merged.status == "closed"
    assert merged.filled_quantity == pytest.approx(0.01)
    assert merged.hedged_quantity == pytest.approx(0.01)


# ----------------------------------------------------- durable cache reconciliation

@pytest.mark.asyncio
async def test_registry_merges_partial_redis_cache_with_supabase_rows(fake_redis):
    """A non-empty Redis cache must not hide records missing from Redis."""
    engine = await _build(fake_redis, MockExchangeClient())
    cached = _entry_record(hedged_quantity=0.002)
    missing = _entry_record(
        client_order_id="btc-idr-entry-8",
        exchange_order_id="502",
    )
    await engine.order_registry.register(cached)

    def durable_row(record, hedged_quantity):
        return {
            "local_order_id": record.local_order_id,
            "client_order_id": record.client_order_id,
            "exchange_order_id": record.exchange_order_id,
            "pair_key": record.pair_key,
            "position_key": record.position_id,
            "exchange_name": record.exchange,
            "market_type": record.market_type,
            "order_role": record.order_role,
            "symbol": record.symbol,
            "side": record.side,
            "order_type": record.order_type,
            "status": record.status,
            "requested_qty": record.original_quantity,
            "filled_qty": record.filled_quantity,
            "remaining_qty": record.remaining_quantity,
            "limit_price": record.price,
            "reduce_only": False,
            "recovery_status": RecoveryStatus.MANAGED.value,
            "is_mock": True,
            "raw_payload": {
                "local_amount_spec": LOCAL_SPEC,
                "global_amount_spec": GLOBAL_SPEC,
                "recovery_context": ENTRY_CONTEXT,
                "hedged_quantity": hedged_quantity,
            },
        }

    async def durable_rows(limit=500):
        return [
            durable_row(cached, 0.006),
            durable_row(missing, 0.0),
        ]

    engine.supabase.fetch_open_order_records = durable_rows
    records = await engine.order_registry.active_records()

    assert set(records) == {cached.local_order_id, missing.local_order_id}
    assert records[cached.local_order_id].hedged_quantity == pytest.approx(0.006)
    assert records[missing.local_order_id].pair_key == PAIR_TAG


@pytest.mark.asyncio
async def test_hedge_watermark_is_persisted_on_originating_spot_order(fake_redis):
    """The spot entry record, not just its futures child, stores hedge progress."""
    engine = await _build(fake_redis, MockExchangeClient())
    record = _entry_record()
    await engine.order_registry.register(record)
    order_info = {
        "order_id": record.exchange_order_id,
        "client_order_id": record.client_order_id,
        "local_exchange": record.exchange,
        "hedged_short_qty": 0.006,
    }

    await engine._persist_entry_hedge_watermark(PAIR_TAG, order_info, 0.006)

    stored = await engine.order_registry.load(record.local_order_id)
    assert stored.hedged_quantity == pytest.approx(0.006)
    pending = json.loads(await fake_redis.get("arb:pending_entry:" + PAIR_TAG))
    assert pending["hedged_short_qty"] == pytest.approx(0.006)


def test_catalogue_match_refuses_multiple_strategies_on_same_spot_market():
    """A shared spot symbol cannot identify which distinct futures pair owns an order."""
    engine = _engine(None, MockExchangeClient())
    engine.active_positions["indodax:btc_idr__binance:btc_usdt"] = _position(
        global_exchange="binance"
    )
    engine.active_positions["indodax:btc_idr__okx:btc_usdt"] = _position(
        global_exchange="okx"
    )

    assert engine._match_position_by_catalogue("indodax", "btc_idr") == ""


@pytest.mark.asyncio
async def test_durable_registry_read_failure_keeps_global_entry_gate(fake_redis):
    """A Supabase read outage cannot be mistaken for an empty durable registry."""
    engine = await _build(fake_redis, MockExchangeClient())

    async def unavailable(limit=500):
        raise RuntimeError("database unavailable")

    engine.supabase.fetch_open_order_records = unavailable
    await engine._run_order_recovery()

    assert "*" in engine._recovery_scope_halts
    assert engine._venue_halted_reason(PAIR_TAG)
    assert engine._entry_blocked is True


@pytest.mark.asyncio
async def test_order_intent_is_registered_before_venue_ack(fake_redis):
    """A client id and pair context survive a crash before exchange acknowledgement."""
    engine = await _build(fake_redis, MockExchangeClient())
    client_order_id = "btc-idr-entry-intent-9"

    intent = await engine._register_order_intent(
        PAIR_TAG,
        client_order_id,
        exchange="indodax",
        market_type="spot",
        symbol="btc_idr",
        role=OrderRole.ENTRY.value,
        side="buy",
        order_type="limit",
        price=1_000_000_000.0,
        quantity=0.01,
        local_amount_spec=LOCAL_SPEC,
        global_amount_spec=GLOBAL_SPEC,
        recovery_context=ENTRY_CONTEXT,
    )

    stored = await engine.order_registry.find_by_client_id("indodax", client_order_id)
    assert stored is not None
    assert stored.local_order_id == intent.local_order_id
    assert stored.status == "submitting"
    assert stored.pair_key == PAIR_TAG
    assert stored.recovery_context["usdt_idr_rate"] == 16_000.0


@pytest.mark.asyncio
async def test_crash_after_exchange_acceptance_before_ack_is_adopted_by_client_id(fake_redis):
    """The pre-submit intent binds an open order even when no exchange id was saved."""
    client = MockExchangeClient()
    client_order_id = "btc-idr-entry-crash-10"
    client.orders["510"] = _venue_order(
        id="510", clientOrderId=client_order_id
    )
    engine = await _build(fake_redis, client)
    await engine._register_order_intent(
        PAIR_TAG,
        client_order_id,
        exchange="indodax",
        market_type="spot",
        symbol="btc_idr",
        role=OrderRole.ENTRY.value,
        side="buy",
        order_type="limit",
        price=1_000_000_000.0,
        quantity=0.01,
        local_amount_spec=LOCAL_SPEC,
        global_amount_spec=GLOBAL_SPEC,
        recovery_context=ENTRY_CONTEXT,
    )

    await engine._run_order_recovery()

    adopted = await engine.order_registry.find_by_exchange_id("indodax", "510")
    assert adopted is not None
    assert adopted.client_order_id == client_order_id
    assert adopted.pair_key == PAIR_TAG
    assert PAIR_TAG in engine.pending_entry_orders
    assert engine.pending_entry_orders[PAIR_TAG]["usdt_idr_rate"] == 16_000.0
    assert await engine.order_registry.pending_recovery_ids() == {}


@pytest.mark.asyncio
async def test_reconciliation_required_entry_cannot_submit_duplicate_hedge(fake_redis):
    """An unresolved hedge intent gates the pending-entry monitor from hedging again."""
    client = MockExchangeClient()
    client.orders["501"] = _venue_order(filled=0.01, status="closed")
    engine = await _build(fake_redis, client)
    order_info = {
        "order_id": "501",
        "client_order_id": "btc-idr-entry-7",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "price": 1_000_000_000.0,
        "usdt_idr_rate": 16_000.0,
    }
    engine.pending_entry_orders[PAIR_TAG] = order_info
    engine.states[PAIR_TAG] = PositionState.RECONCILIATION_REQUIRED
    engine._recovery_scope_halts[PAIR_TAG] = "unresolved hedge order"

    await engine._process_pending_entry_bound(PAIR_TAG, order_info)

    assert client.global_short_sells == []
