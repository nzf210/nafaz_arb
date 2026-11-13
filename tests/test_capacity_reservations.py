import asyncio
from types import SimpleNamespace

import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine


class AtomicCapacityStore:
    """Concurrent fake for engine integration tests; SQL serialization is covered by schema smoke."""

    def __init__(self, max_orders=2, capital_limit=1000.0):
        self.max_orders = max_orders
        self.capital_limit = capital_limit
        self.active = {}
        self.lock = asyncio.Lock()
        self.next_id = 0

    async def reserve_execution_capacity(
        self, account_scope, exchange, pair_key, client_order_id, amount_usdt, max_capital_usdt=None
    ):
        async with self.lock:
            key = (account_scope, exchange.lower(), pair_key)
            if key in self.active:
                item = self.active[key]
                if item["client_order_id"] == client_order_id and item["amount"] == amount_usdt:
                    return {"accepted": True, "reservation_id": item["id"], "reason": "already_reserved"}
                reason = (
                    "reservation_amount_mismatch"
                    if item["client_order_id"] == client_order_id
                    else "pair_already_reserved"
                )
                return {"accepted": False, "reservation_id": None, "reason": reason}
            active_for_exchange = [
                item for item in self.active.values()
                if item["account_scope"] == account_scope and item["exchange"] == exchange.lower()
            ]
            if len(active_for_exchange) >= self.max_orders:
                return {"accepted": False, "reason": "spot_order_limit_reached"}
            total = sum(item["amount"] for item in active_for_exchange)
            if max_capital_usdt is not None and self.capital_limit is not None:
                cap = min(self.capital_limit, max_capital_usdt)
            else:
                cap = max_capital_usdt if max_capital_usdt is not None else self.capital_limit
            if cap is not None and total + amount_usdt > cap:
                return {"accepted": False, "reason": "capital_limit_reached"}
            self.next_id += 1
            reservation_id = f"reservation-{self.next_id}"
            self.active[key] = {
                "id": reservation_id,
                "account_scope": account_scope,
                "exchange": exchange.lower(),
                "amount": amount_usdt,
                "client_order_id": client_order_id,
            }
            return {"accepted": True, "reservation_id": reservation_id, "reason": "reserved"}

    async def release_execution_capacity_for_pair(self, account_scope, exchange, pair_key):
        async with self.lock:
            return self.active.pop((account_scope, exchange.lower(), pair_key), None) is not None


def make_engine(fake_redis, store):
    config = ExecutorConfig(mock_mode=False, max_exposure_per_exchange_usdt=1000.0)
    config.local_exchange_name = "indodax"
    config.global_exchange_name = "okx"
    config.local_credentials.account_scope = "portfolio-main"
    engine = ArbitrageEngine(config=config, client=SimpleNamespace(), redis_client=fake_redis)
    engine.supabase.reserve_execution_capacity = store.reserve_execution_capacity
    engine.supabase.release_execution_capacity_for_pair = store.release_execution_capacity_for_pair
    return engine


@pytest.mark.asyncio
async def test_spot_capacity_rejects_different_client_order_for_active_pair(fake_redis):
    store = AtomicCapacityStore(max_orders=2, capital_limit=1000.0)
    engine = make_engine(fake_redis, store)
    await engine._reserve_spot_capacity(
        "indodax:btc_idr", "indodax", "entry-1", 200.0
    )
    with pytest.raises(RuntimeError, match="pair_already_reserved"):
        await engine._reserve_spot_capacity(
            "indodax:btc_idr", "indodax", "entry-2", 200.0
        )
    with pytest.raises(RuntimeError, match="reservation_amount_mismatch"):
        await engine._reserve_spot_capacity(
            "indodax:btc_idr", "indodax", "entry-1", 250.0
        )
    assert len(store.active) == 1


@pytest.mark.asyncio
async def test_live_spot_intent_is_postgres_acked_before_capacity_reservation():
    from executor.engine_order_intents import EngineOrderIntentMixin

    events = []

    class IntentHarness(EngineOrderIntentMixin):
        def __init__(self):
            self.config = SimpleNamespace(mock_mode=False)
            self.supabase = SimpleNamespace(enabled=True)
            self.order_registry = object()

        def _account_scope_for_exchange(self, exchange):
            return f"{exchange}:main"

        async def _register_order(self, pair_tag, client_order_id, order, **metadata):
            events.append("intent")
            return SimpleNamespace(
                local_order_id="local-intent-1", pair_key=pair_tag,
                exchange="indodax", client_order_id=client_order_id,
                market_type="spot", order_role="ENTRY",
                recovery_context=metadata["recovery_context"], filled_quantity=0.0,
            )

        async def _reserve_spot_capacity(self, pair_key, exchange, client_order_id, amount_usdt):
            events.append(("reserve", amount_usdt))
            return "capacity-1"

    async def confirm(_local_order_id):
        events.append("postgres_ack")
        return True

    harness = IntentHarness()
    harness.supabase.confirm_order_registry = confirm
    record = await harness._register_order_intent(
        "indodax:btc_idr", "entry-client-1",
        exchange="indodax", market_type="spot", symbol="btc_idr",
        role="ENTRY", side="buy", order_type="limit", price=100.0,
        quantity=2.0, recovery_context={
            "spot_cost_usdt": 100.0, "pair_capital_reservation_usdt": 200.0,
        },
    )
    assert events == ["intent", "postgres_ack", ("reserve", 200.0)]
    assert record.capacity_reservation_id == "capacity-1"
    assert record.recovery_context["capacity_account_scope"] == "indodax:main"


@pytest.mark.asyncio
async def test_spot_capacity_reservation_is_idempotent_and_restart_safe(fake_redis):
    store = AtomicCapacityStore(max_orders=2, capital_limit=1000.0)
    first_engine = make_engine(fake_redis, store)
    first = await first_engine._reserve_spot_capacity(
        "indodax:btc_idr", "indodax", "entry-1", 200.0
    )
    same_pair = await first_engine._reserve_spot_capacity(
        "indodax:btc_idr", "indodax", "entry-1", 200.0
    )
    assert same_pair == first
    assert len(store.active) == 1

    # A new engine instance sees the durable store's active reservation.
    restarted_engine = make_engine(fake_redis, store)
    second = await restarted_engine._reserve_spot_capacity(
        "indodax:eth_idr", "indodax", "entry-2", 200.0
    )
    assert second != first
    with pytest.raises(RuntimeError, match="spot_order_limit_reached"):
        await restarted_engine._reserve_spot_capacity(
            "indodax:sol_idr", "indodax", "entry-3", 200.0
        )


@pytest.mark.asyncio
async def test_spot_capacity_concurrency_never_exceeds_two_slots(fake_redis):
    store = AtomicCapacityStore(max_orders=2, capital_limit=5000.0)
    engine = make_engine(fake_redis, store)

    async def reserve(index):
        try:
            return await engine._reserve_spot_capacity(
                f"indodax:pair{index}_idr", "indodax", f"entry-{index}", 100.0
            )
        except RuntimeError:
            return None

    results = await asyncio.gather(*(reserve(index) for index in range(12)))
    assert sum(result is not None for result in results) == 2
    assert len(store.active) == 2


@pytest.mark.asyncio
async def test_spot_capacity_reservation_enforces_total_pair_capital(fake_redis):
    store = AtomicCapacityStore(max_orders=10, capital_limit=300.0)
    engine = make_engine(fake_redis, store)
    await engine._reserve_spot_capacity(
        "indodax:btc_idr", "indodax", "entry-1", 200.0
    )
    with pytest.raises(RuntimeError, match="capital_limit_reached"):
        await engine._reserve_spot_capacity(
            "indodax:eth_idr", "indodax", "entry-2", 200.0
        )


@pytest.mark.asyncio
async def test_terminal_capacity_release_allows_next_entry(fake_redis):
    store = AtomicCapacityStore(max_orders=1, capital_limit=1000.0)
    engine = make_engine(fake_redis, store)
    await engine._reserve_spot_capacity(
        "indodax:btc_idr", "indodax", "entry-1", 200.0
    )
    assert await engine._release_spot_capacity_reservation("indodax:btc_idr", "indodax")
    next_id = await engine._reserve_spot_capacity(
        "indodax:eth_idr", "indodax", "entry-2", 200.0
    )
    assert next_id == "reservation-2"


@pytest.mark.asyncio
async def test_capacity_rpc_failure_fails_closed(fake_redis):
    store = AtomicCapacityStore()
    engine = make_engine(fake_redis, store)

    async def unavailable(*args, **kwargs):
        raise RuntimeError("Postgres unavailable")

    engine.supabase.reserve_execution_capacity = unavailable
    with pytest.raises(RuntimeError, match="Postgres unavailable"):
        await engine._reserve_spot_capacity(
            "indodax:btc_idr", "indodax", "entry-1", 200.0
        )
    assert store.active == {}
