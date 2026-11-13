"""Contract tests for venue lookup eventual consistency and pending close orders.

These use deterministic fake CCXT-shaped endpoints. They validate executor
behavior, not live exchange API guarantees; live contract probes remain a
separate release gate.
"""
import pytest
from types import SimpleNamespace

from executor.force_close_legs import ForceCloseLegMixin
from executor.order_registry_storage import OrderRegistryStorageMixin
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
@pytest.mark.parametrize("venue_id", ["indodax", "tokocrypto", "mexc", "okx", "binance", "bybit"])
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


class ExistingIntentRegistry:
    async def find_by_client_id(self, exchange, client_order_id):
        return {"client_order_id": client_order_id, "status": "submitting"}


class MissingLookupClient:
    def __init__(self):
        self.lookups = []
        self.submits = 0

    async def fetch_global_order_by_client_id(self, client_order_id, symbol, amount_spec):
        self.lookups.append((client_order_id, symbol))
        return None

    async def close_global_short_market(self, *args, **kwargs):
        self.submits += 1
        raise AssertionError("ambiguous prior close must never be resubmitted")


class ForceCloseHarness(ForceCloseLegMixin):
    def __init__(self):
        self.config = SimpleNamespace(mock_mode=False)
        self.order_registry = ExistingIntentRegistry()
        self.client = MissingLookupClient()

    def _resolve_global_symbol(self, symbol):
        return symbol


@pytest.mark.asyncio
async def test_force_close_prior_reduce_only_intent_not_found_never_resubmits():
    engine = ForceCloseHarness()
    position = SimpleNamespace(
        symbol="BTC/USDT:USDT",
        qty=0.01,
        opened_at_ms=1730000000000,
        global_exchange="okx",
        global_amount_spec=None,
    )
    closed, fill_price = await engine._close_global_short_leg("okx:BTC_USDT", position)
    assert closed is False
    assert fill_price == 0.0
    assert len(engine.client.lookups) == 1
    assert engine.client.submits == 0


class UnavailableRedis:
    async def hget(self, *args):
        raise ConnectionError("redis unavailable")


class EmptyDurableOrderReader:
    async def fetch_open_order_records(self):
        return []


class RegistryLookupHarness(OrderRegistryStorageMixin):
    def __init__(self):
        self.redis = UnavailableRedis()
        self.supabase = EmptyDurableOrderReader()
        self.records = {}


@pytest.mark.asyncio
async def test_order_registry_does_not_treat_redis_and_active_journal_gap_as_absence():
    registry = RegistryLookupHarness()
    with pytest.raises(RuntimeError, match="lookup is inconclusive"):
        await registry.find_by_client_id("okx", "possibly-used-close-id")
