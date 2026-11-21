import asyncio

import pytest

from executor.mutation_gateway import MutationGateway, MutationResultType


class FakeSlotManager:
    def __init__(self):
        self.released = []

    async def release_slot(self, exchange, client_order_id, reason=""):
        self.released.append((exchange, client_order_id, reason))


class FakeSpotFacade:
    def __init__(self, venue="tokocrypto", states=None, cancel_error=None):
        self.local = type("Venue", (), {"id": venue})()
        self.states = list(states or [])
        self.cancel_error = cancel_error
        self.cancel_calls = []

    async def fetch_local_order(self, order_id, symbol):
        if not self.states:
            raise AssertionError("unexpected order fetch")
        state = self.states.pop(0)
        if isinstance(state, Exception):
            raise state
        return dict(state)

    async def cancel_local_order(self, order_id, symbol):
        self.cancel_calls.append((order_id, symbol))
        if self.cancel_error:
            raise self.cancel_error
        return {"id": order_id}

    async def fetch_local_order_by_client_id(self, client_order_id, symbol):
        return {"id": "ex-123", "clientOrderId": client_order_id, "status": "open"}


class FakeRootClient:
    def __init__(self, facade):
        self.facade = facade
        self.routes = []

    def for_venue(self, exchange, market_type):
        self.routes.append((exchange, market_type))
        return self.facade


def order(status, order_id="ex-123"):
    return {"id": order_id, "symbol": "BTC/USDT", "status": status}


@pytest.mark.asyncio
async def test_cancel_confirms_only_after_final_terminal_state_and_releases_slot():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(states=[order("open"), order("canceled")])
    root = FakeRootClient(facade)

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        root,
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-123",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.CONFIRMED
    assert root.routes == [("tokocrypto", "spot")]
    assert facade.cancel_calls == [("ex-123", "BTC/USDT")]
    assert slots.released == [("tokocrypto", "logical-123", "TERMINAL_CANCELED")]


@pytest.mark.asyncio
async def test_cancel_ack_with_order_still_open_is_unknown_and_keeps_slot():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(states=[order("open"), order("open")])

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        FakeRootClient(facade),
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-123",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.SUBMISSION_UNKNOWN
    assert "open" in result.error_message
    assert slots.released == []


@pytest.mark.asyncio
async def test_cancel_timeout_can_still_be_confirmed_by_final_venue_read():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(
        states=[order("open"), order("canceled")],
        cancel_error=TimeoutError("response lost"),
    )

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        FakeRootClient(facade),
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-123",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.CONFIRMED
    assert slots.released == [("tokocrypto", "logical-123", "TERMINAL_CANCELED")]


@pytest.mark.asyncio
async def test_missing_cancel_capability_fails_closed_without_releasing_slot():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(states=[order("open")])
    facade.cancel_local_order = None

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        FakeRootClient(facade),
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-123",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.REJECTED
    assert "lacks cancel/fetch order capability" in result.error_message
    assert slots.released == []


@pytest.mark.asyncio
async def test_venue_mismatch_fails_closed_before_any_order_mutation():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(venue="indodax", states=[order("open")])

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        FakeRootClient(facade),
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-123",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.REJECTED
    assert "venue mismatch" in result.error_message
    assert facade.cancel_calls == []
    assert slots.released == []


@pytest.mark.asyncio
async def test_client_order_id_is_resolved_before_cancel():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(states=[order("open"), order("canceled")])

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        FakeRootClient(facade),
        exchange="tokocrypto",
        market_type="spot",
        order_id="",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.CONFIRMED
    assert facade.cancel_calls == [("ex-123", "BTC/USDT")]
    assert result.exchange_order_id == "ex-123"


@pytest.mark.asyncio
async def test_final_order_fetch_failure_is_unknown_and_keeps_slot():
    slots = FakeSlotManager()
    facade = FakeSpotFacade(states=[order("open"), RuntimeError("venue unavailable")])

    result = await MutationGateway(slot_manager=slots).execute_cancel_order(
        FakeRootClient(facade),
        exchange="tokocrypto",
        market_type="spot",
        order_id="ex-123",
        symbol="BTC/USDT",
        client_order_id="logical-123",
    )

    assert result.status is MutationResultType.SUBMISSION_UNKNOWN
    assert "terminal state could not be verified" in result.error_message
    assert slots.released == []
