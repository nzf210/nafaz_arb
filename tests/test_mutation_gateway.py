from contextlib import asynccontextmanager

import pytest

from executor.global_stop import GlobalStopError
from executor.mutation_gateway import MutationGateway, MutationResultType
from executor.order_slots import SlotExhaustedError


class _FakeGlobalStop:
    enabled = True

    def __init__(self, events, *, fail_ack=False):
        self.events = events
        self.fail_ack = fail_ack

    async def prepare_submit_attempt(self, **kwargs):
        self.events.append("prepare")

    async def mark_submit_aborted(self, client_order_id, reason):
        self.events.append("aborted")

    async def mark_submit_rejected(self, client_order_id, error):
        self.events.append("rejected")

    async def mark_submit_unknown(self, client_order_id, error):
        self.events.append("unknown")

    async def mark_submit_acknowledged(self, client_order_id, order):
        self.events.append("ack")
        if self.fail_ack:
            raise GlobalStopError("simulated ACK persistence failure")

    @asynccontextmanager
    async def order_submit_fence(self, **kwargs):
        self.events.append("fence_enter")
        try:
            yield object()
        finally:
            self.events.append("fence_exit")

    async def request_stop_in_transaction(self, *args, **kwargs):
        raise AssertionError("IP-ban path is not expected in this test")

    async def notify_state(self, state):
        return None


class _RecordingSlots:
    def __init__(self, events, *, exhausted=False):
        self.events = events
        self.exhausted = exhausted

    async def reserve_slot(self, *args, **kwargs):
        self.events.append("reserve_slot")
        if self.exhausted:
            raise SlotExhaustedError("spot slots are full")

    async def activate_slot(self, *args, **kwargs):
        self.events.append("activate_slot")

    async def release_slot(self, *args, **kwargs):
        self.events.append("release_slot")


class _CreateVenue:
    async def create_order(self, **kwargs):
        self.events.append("venue_create")
        return {"id": "venue-order-1", "filled": 0.0, "average": 10.0}

    def __init__(self, events):
        self.events = events


def _create_args():
    return {
        "exchange": "tokocrypto",
        "market_type": "spot",
        "symbol": "BTC/USDT",
        "side": "buy",
        "order_type": "limit",
        "quantity": 0.01,
        "price": 10.0,
        "pair_key": "tokocrypto:BTC/USDT",
        "extra_params": {"client_order_id": "stable-entry-client-id"},
    }


@pytest.mark.asyncio
async def test_generic_create_persists_prepared_before_slot_and_keeps_slot_on_ack_failure():
    events = []
    stop = _FakeGlobalStop(events, fail_ack=True)
    slots = _RecordingSlots(events)
    venue = _CreateVenue(events)
    gateway = MutationGateway(stop, slots)

    result = await gateway.execute_create_order(venue, **_create_args())

    assert result.status == MutationResultType.SUBMISSION_UNKNOWN
    assert events.index("prepare") < events.index("reserve_slot")
    assert events.index("reserve_slot") < events.index("fence_enter")
    assert events.index("fence_enter") < events.index("venue_create")
    assert "ack" in events
    assert "unknown" in events
    assert "release_slot" not in events
    assert "activate_slot" not in events


@pytest.mark.asyncio
async def test_generic_create_aborts_prepared_when_slot_reservation_is_rejected():
    events = []
    stop = _FakeGlobalStop(events)
    slots = _RecordingSlots(events, exhausted=True)
    venue = _CreateVenue(events)
    gateway = MutationGateway(stop, slots)

    result = await gateway.execute_create_order(venue, **_create_args())

    assert result.status == MutationResultType.REJECTED
    assert events.index("prepare") < events.index("reserve_slot")
    assert "aborted" in events
    assert "release_slot" in events
    assert "fence_enter" not in events
    assert "venue_create" not in events


class _CancelFacade:
    def __init__(self, events, states):
        self.events = events
        self.states = list(states)
        self.resolved = []

    def for_venue(self, exchange, market):
        self.resolved.append((exchange, market))
        return self

    async def cancel_local_order(self, order_id, symbol):
        self.events.append("cancel")
        return {"id": order_id, "status": "canceled"}

    async def fetch_local_order(self, order_id, symbol):
        self.events.append("fetch")
        if self.states:
            return self.states.pop(0)
        return {"id": order_id, "status": "open", "filled": 0.0}


@pytest.mark.asyncio
async def test_operator_cancel_routes_exchange_facade_and_releases_only_terminal_zero_fill():
    events = []
    slots = _RecordingSlots(events)
    client = _CancelFacade(
        events,
        [
            {"id": "venue-order-2", "status": "open", "filled": 0.0},
            {"id": "venue-order-2", "status": "canceled", "filled": 0.0},
        ],
    )
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        client,
        exchange="tokocrypto",
        market_type="spot",
        order_id="venue-order-2",
        symbol="BTC/USDT",
        client_order_id="stable-entry-client-id",
    )

    assert result.status == MutationResultType.CONFIRMED
    assert client.resolved == [("tokocrypto", "spot")]
    assert events.count("cancel") == 1
    assert events.count("fetch") == 2
    assert events.count("release_slot") == 1


@pytest.mark.asyncio
async def test_cancel_without_supported_facade_is_not_reported_confirmed_or_released():
    events = []
    slots = _RecordingSlots(events)

    class NoCancelFacade:
        def for_venue(self, exchange, market):
            return self

    gateway = MutationGateway(slot_manager=slots)
    result = await gateway.execute_cancel_order(
        NoCancelFacade(),
        exchange="tokocrypto",
        market_type="spot",
        order_id="venue-order-3",
        symbol="BTC/USDT",
        client_order_id="stable-entry-client-id",
    )

    assert result.status == MutationResultType.REJECTED
    assert "cancel capability" in result.error_message
    assert "release_slot" not in events


@pytest.mark.asyncio
async def test_cancel_with_terminal_partial_fill_retains_slot_for_lifecycle_reconciliation():
    events = []
    slots = _RecordingSlots(events)
    client = _CancelFacade(
        events,
        [{"id": "venue-order-4", "status": "canceled", "filled": 0.25}],
    )
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        client,
        exchange="tokocrypto",
        market_type="spot",
        order_id="venue-order-4",
        symbol="BTC/USDT",
        client_order_id="stable-entry-client-id",
    )

    assert result.status == MutationResultType.SUBMISSION_UNKNOWN
    assert result.filled_qty == 0.25
    assert "slot retained" in result.error_message
    assert "release_slot" not in events


@pytest.mark.asyncio
async def test_spot_cancel_without_logical_client_id_does_not_release_slot():
    events = []
    slots = _RecordingSlots(events)
    client = _CancelFacade(
        events,
        [{"id": "venue-order-5", "status": "canceled", "filled": 0.0}],
    )
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        client,
        exchange="tokocrypto",
        market_type="spot",
        order_id="venue-order-5",
        symbol="BTC/USDT",
        client_order_id=None,
    )

    assert result.status == MutationResultType.SUBMISSION_UNKNOWN
    assert "logical client order ID is missing" in result.error_message
    assert "release_slot" not in events


@pytest.mark.asyncio
async def test_closed_zero_fill_is_not_treated_as_cancelled_for_slot_release():
    events = []
    slots = _RecordingSlots(events)
    client = _CancelFacade(
        events,
        [{"id": "venue-order-6", "status": "closed", "filled": 0.0}],
    )
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        client,
        exchange="tokocrypto",
        market_type="spot",
        order_id="venue-order-6",
        symbol="BTC/USDT",
        client_order_id="stable-entry-client-id",
    )

    assert result.status == MutationResultType.SUBMISSION_UNKNOWN
    assert "does not prove a zero-fill cancel" in result.error_message
    assert "release_slot" not in events
