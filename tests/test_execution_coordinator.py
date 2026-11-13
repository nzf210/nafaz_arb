from types import SimpleNamespace

import pytest

from executor.execution_coordinator import ExecutionCoordinator
from executor.mutation_gateway import MutationGatewayClient


class FakeSupabase:
    def __init__(self, *, acquired=True, valid=True):
        self.acquired = acquired
        self.valid = valid
        self.assert_calls = 0
        self.renew_calls = 0
        self.release_calls = 0

    async def acquire_execution_coordinator_lease(self, scope, owner_id, lease_seconds):
        return {"acquired": self.acquired, "fencing_token": 17}

    async def renew_execution_coordinator_lease(self, scope, owner_id, token, lease_seconds):
        self.renew_calls += 1
        return self.valid

    async def assert_execution_coordinator_lease(self, scope, owner_id, token):
        self.assert_calls += 1
        return self.valid

    async def release_execution_coordinator_lease(self, scope, owner_id, token):
        self.release_calls += 1
        return True


class FakeRegistry:
    def __init__(self):
        self.client_records = {}
        self.exchange_records = {}

    async def find_by_client_id(self, exchange, client_order_id):
        return self.client_records.get((exchange, client_order_id))

    async def find_by_exchange_id(self, exchange, order_id):
        return self.exchange_records.get((exchange, order_id))


class FakeExchange:
    def __init__(self):
        self.local = SimpleNamespace(id="indodax")
        self.global_fut = SimpleNamespace(id="okx")
        self.submissions = []
        self.cancellations = []

    async def place_local_limit_buy(self, symbol, amount, price, client_order_id=None):
        self.submissions.append((symbol, amount, price, client_order_id))
        return {"id": "spot-1", "status": "open"}

    async def cancel_local_order(self, order_id, symbol):
        self.cancellations.append((order_id, symbol))
        return {"id": order_id, "status": "canceled"}


@pytest.mark.asyncio
async def test_coordinator_requires_postgres_lease_and_fencing():
    supabase = FakeSupabase()
    coordinator = ExecutionCoordinator(
        supabase, renew_interval_seconds=60
    )
    assert await coordinator.acquire() is True
    assert coordinator.fencing_token == 17

    result = await coordinator.execute_mutation(lambda: _resolved("ok"))
    assert result == "ok"
    assert supabase.assert_calls == 2

    assert await coordinator.release() is True
    assert supabase.release_calls == 1


@pytest.mark.asyncio
async def test_coordinator_blocks_mutation_when_fencing_token_is_stale():
    supabase = FakeSupabase(valid=False)
    coordinator = ExecutionCoordinator(
        supabase, renew_interval_seconds=60
    )
    assert await coordinator.acquire() is True
    called = False

    async def operation():
        nonlocal called
        called = True

    with pytest.raises(RuntimeError, match="fencing token is stale"):
        await coordinator.execute_mutation(operation)

    assert called is False
    assert coordinator.active is False
    await coordinator.release()


@pytest.mark.asyncio
async def test_coordinator_rechecks_fencing_after_venue_mutation():
    supabase = FakeSupabase()
    coordinator = ExecutionCoordinator(supabase, renew_interval_seconds=60)
    assert await coordinator.acquire() is True
    called = False

    async def operation():
        nonlocal called
        called = True
        supabase.valid = False
        return {"id": "possibly-accepted"}

    with pytest.raises(RuntimeError, match="fencing token is stale"):
        await coordinator.execute_mutation(operation)
    assert called is True
    assert supabase.assert_calls == 2
    assert coordinator.active is False
    await coordinator.release()


@pytest.mark.asyncio
async def test_coordinator_can_reacquire_after_lease_loss():
    supabase = FakeSupabase()
    coordinator = ExecutionCoordinator(supabase, renew_interval_seconds=60)
    assert await coordinator.acquire() is True
    await coordinator._lose("simulated lease loss")
    assert coordinator.active is False

    assert await coordinator.acquire() is True
    assert coordinator.active is True
    assert coordinator.fencing_token == 17
    assert coordinator._heartbeat_task is not None
    await coordinator.release()


@pytest.mark.asyncio
async def test_coordinator_does_not_start_without_lease_owner():
    supabase = FakeSupabase(acquired=False)
    coordinator = ExecutionCoordinator(
        supabase, renew_interval_seconds=60
    )
    assert await coordinator.acquire() is False
    with pytest.raises(RuntimeError, match="does not own"):
        await coordinator.execute_mutation(lambda: _resolved(None))


@pytest.mark.asyncio
async def test_mutation_gateway_requires_registered_intent_before_live_create():
    supabase = FakeSupabase()
    coordinator = ExecutionCoordinator(
        supabase, renew_interval_seconds=60
    )
    assert await coordinator.acquire() is True
    registry = FakeRegistry()
    exchange = FakeExchange()
    config = SimpleNamespace(
        mock_mode=False, local_exchange_name="indodax", global_exchange_name="okx"
    )
    gateway = MutationGatewayClient(exchange, coordinator, registry, config)

    with pytest.raises(RuntimeError, match="durable order intent"):
        await gateway.place_local_limit_buy(
            "btc_idr", 1.0, 100.0, client_order_id="missing-intent"
        )
    assert exchange.submissions == []

    registry.client_records[("indodax", "intent-1")] = SimpleNamespace(
        exchange="indodax", market_type="spot", side="buy",
        client_order_id="intent-1", is_terminal=False,
    )
    result = await gateway.place_local_limit_buy(
        "btc_idr", 1.0, 100.0, client_order_id="intent-1"
    )
    assert result["id"] == "spot-1"
    assert len(exchange.submissions) == 1
    await coordinator.release()


@pytest.mark.asyncio
async def test_mutation_gateway_rejects_intent_with_wrong_side_or_market():
    coordinator = ExecutionCoordinator(FakeSupabase(), renew_interval_seconds=60)
    assert await coordinator.acquire() is True
    registry = FakeRegistry()
    exchange = FakeExchange()
    config = SimpleNamespace(
        mock_mode=False, local_exchange_name="indodax", global_exchange_name="okx"
    )
    gateway = MutationGatewayClient(exchange, coordinator, registry, config)
    registry.client_records[("indodax", "wrong-side")] = SimpleNamespace(
        exchange="indodax", market_type="spot", side="sell",
        client_order_id="wrong-side", is_terminal=False,
    )
    with pytest.raises(RuntimeError, match="intent side"):
        await gateway.place_local_limit_buy(
            "btc_idr", 1.0, 100.0, client_order_id="wrong-side"
        )
    registry.client_records[("indodax", "wrong-market")] = SimpleNamespace(
        exchange="indodax", market_type="perpetual", side="buy",
        client_order_id="wrong-market", is_terminal=False,
    )
    with pytest.raises(RuntimeError, match="intent market"):
        await gateway.place_local_limit_buy(
            "btc_idr", 1.0, 100.0, client_order_id="wrong-market"
        )
    assert exchange.submissions == []
    await coordinator.release()


@pytest.mark.asyncio
async def test_mutation_gateway_blocks_cancel_for_terminal_order():
    coordinator = ExecutionCoordinator(FakeSupabase(), renew_interval_seconds=60)
    assert await coordinator.acquire() is True
    registry = FakeRegistry()
    exchange = FakeExchange()
    config = SimpleNamespace(
        mock_mode=False, local_exchange_name="indodax", global_exchange_name="okx"
    )
    gateway = MutationGatewayClient(exchange, coordinator, registry, config)
    registry.exchange_records[("indodax", "terminal-1")] = SimpleNamespace(
        exchange="indodax", market_type="spot", is_terminal=True,
    )
    with pytest.raises(RuntimeError, match="already terminal"):
        await gateway.cancel_local_order("terminal-1", "btc_idr")
    assert exchange.cancellations == []
    await coordinator.release()


@pytest.mark.asyncio
async def test_mutation_gateway_blocks_cancel_without_owned_order():
    supabase = FakeSupabase()
    coordinator = ExecutionCoordinator(
        supabase, renew_interval_seconds=60
    )
    assert await coordinator.acquire() is True
    registry = FakeRegistry()
    exchange = FakeExchange()
    config = SimpleNamespace(
        mock_mode=False, local_exchange_name="indodax", global_exchange_name="okx"
    )
    gateway = MutationGatewayClient(exchange, coordinator, registry, config)

    with pytest.raises(RuntimeError, match="no owned registry record"):
        await gateway.cancel_local_order("unowned-order", "btc_idr")
    assert exchange.cancellations == []
    await coordinator.release()


async def _resolved(value):
    return value
