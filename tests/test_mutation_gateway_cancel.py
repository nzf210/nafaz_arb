import pytest

from executor.mutation_gateway import MutationGateway, MutationResultType


class SlotManager:
    def __init__(self):
        self.released = []

    async def release_slot(self, exchange, client_order_id, reason):
        self.released.append((exchange, client_order_id, reason))


class SpotVenue:
    def __init__(self, *, status="canceled", exchange="indodax"):
        self.id = exchange
        self.status = status
        self.cancel_calls = []
        self.fetch_calls = []
        self.fetch_error = None
        self.order = {
            "id": "venue-order-123",
            "symbol": "BTC/IDR",
            "status": status,
            "filled": 0.0,
            "average": 0.0,
        }

    async def cancel_local_order(self, order_id, symbol):
        self.cancel_calls.append((order_id, symbol))
        return {"id": order_id, "status": "canceled"}

    async def fetch_local_order(self, order_id, symbol):
        self.fetch_calls.append((order_id, symbol))
        if self.fetch_error:
            raise self.fetch_error
        return dict(self.order)


@pytest.mark.asyncio
async def test_cancel_requires_cancel_and_fetch_capabilities():
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        object(),
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.REJECTED
    assert "lacks cancel and order-fetch capabilities" in result.error_message
    assert slots.released == []


@pytest.mark.asyncio
async def test_cancel_ack_with_open_order_is_not_confirmed_or_released():
    venue = SpotVenue(status="open")
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        venue,
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.SUBMISSION_UNKNOWN
    assert "not terminal" in result.error_message
    assert venue.cancel_calls == [("venue-order-123", "BTC/IDR")]
    assert venue.fetch_calls == [("venue-order-123", "BTC/IDR")]
    assert slots.released == []


@pytest.mark.asyncio
async def test_cancel_is_confirmed_only_after_terminal_cancel_state():
    venue = SpotVenue(status="canceled")
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        venue,
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.CONFIRMED
    assert result.raw_response["status"] == "canceled"
    assert slots.released == [
        ("indodax", "logical-client-123", "TERMINAL_CANCELED")
    ]


@pytest.mark.asyncio
async def test_cancel_race_that_filled_reports_terminal_fill_quantity():
    venue = SpotVenue(status="closed")
    venue.order.update({"filled": 0.025, "average": 65000.0})
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        venue,
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.CONFIRMED
    assert result.filled_qty == pytest.approx(0.025)
    assert result.average_price == pytest.approx(65000.0)
    assert slots.released == [
        ("indodax", "logical-client-123", "TERMINAL_CLOSED")
    ]


@pytest.mark.asyncio
async def test_post_cancel_fetch_failure_remains_unknown_and_keeps_slot():
    venue = SpotVenue(status="canceled")
    venue.fetch_error = RuntimeError("venue read unavailable")
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        venue,
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.SUBMISSION_UNKNOWN
    assert "terminal state could not be fetched" in result.error_message
    assert slots.released == []


@pytest.mark.asyncio
async def test_wrong_venue_identity_is_rejected_before_mutation():
    venue = SpotVenue(exchange="tokocrypto")
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        venue,
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.REJECTED
    assert "venue identity mismatch" in result.error_message
    assert venue.cancel_calls == []
    assert slots.released == []


@pytest.mark.asyncio
async def test_cancel_timeout_is_unknown_and_does_not_release_slot():
    class TimeoutVenue(SpotVenue):
        async def cancel_local_order(self, order_id, symbol):
            self.cancel_calls.append((order_id, symbol))
            raise TimeoutError("request timed out")

    venue = TimeoutVenue()
    slots = SlotManager()
    gateway = MutationGateway(slot_manager=slots)

    result = await gateway.execute_cancel_order(
        venue,
        exchange="indodax",
        market_type="spot",
        order_id="venue-order-123",
        symbol="BTC/IDR",
        client_order_id="logical-client-123",
    )

    assert result.status is MutationResultType.SUBMISSION_UNKNOWN
    assert slots.released == []
