"""Contract tests for venue lookup eventual consistency and pending close orders.

These use deterministic fake CCXT-shaped endpoints. They validate executor
behavior, not live exchange API guarantees; live contract probes remain a
separate release gate.
"""
import pytest

from executor.exchange_common import _find_order_by_client_id


class DelayedVisibilityVenue:
    def __init__(self, venue_id, client_id):
        self.id = venue_id
        self.client_id = client_id
        self.visible = False
        self.order = {
            "id": "venue-order-1",
            "clientOrderId": client_id,
            "status": "open",
            "filled": 0.0,
            "amount": 1.0,
        }

    async def fetch_order_by_client_id(self, client_order_id, symbol):
        return None

    async def fetch_open_orders(self, symbol=None):
        return [dict(self.order)] if self.visible and self.order["status"] == "open" else []

    async def fetch_closed_orders(self, symbol=None, since=None, limit=None):
        return [dict(self.order)] if self.visible and self.order["status"] in ("closed", "filled") else []

    async def fetch_canceled_orders(self, symbol=None, since=None, limit=None):
        return [dict(self.order)] if self.visible and self.order["status"] in ("canceled", "rejected", "expired") else []


@pytest.mark.asyncio
@pytest.mark.parametrize("venue_id", ["indodax", "tokocrypto", "mexc", "okx"])
async def test_delayed_client_id_visibility_stays_not_found_then_recovers(venue_id):
    client_id = "eventual-visibility-001"
    venue = DelayedVisibilityVenue(venue_id, client_id)

    # A negative lookup is not a definitive rejection; it is simply not visible yet.
    first = await _find_order_by_client_id(venue, "BTC/USDT", client_id)
    assert first is None

    venue.visible = True
    second = await _find_order_by_client_id(venue, "BTC/USDT", client_id)
    assert second is not None
    assert second["id"] == "venue-order-1"
    assert second["client_order_id"] == client_id


@pytest.mark.asyncio
@pytest.mark.parametrize("venue_id", ["mexc", "okx"])
async def test_pending_reduce_only_close_is_found_and_not_misclassified_as_absent(venue_id):
    client_id = "pending-reduce-only-001"
    venue = DelayedVisibilityVenue(venue_id, client_id)
    venue.visible = True
    venue.order.update({"status": "open", "reduceOnly": True, "side": "buy"})

    found = await _find_order_by_client_id(venue, "BTC/USDT:USDT", client_id)
    assert found is not None
    assert found["status"] == "open"
    assert found["reduceOnly"] is True
