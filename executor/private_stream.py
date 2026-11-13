"""
Private websocket support for order and balance updates (P3-05).

The executor has always discovered fills by polling ``fetch_local_order`` and
``get_balances``. That works, but it costs a REST round-trip per open order per
monitoring tick and adds latency between a fill and the hedge.

This module keeps a small authoritative cache that a private feed can push into.
The polling loops consult the cache first and fall back to REST for anything the
feed has not reported recently, so enabling the feed never removes a safety net:
a dropped socket degrades straight back to today's behaviour.
"""

import logging
import time
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("executor.private_stream")

# A cached order older than this is treated as unknown and re-polled over REST.
DEFAULT_MAX_AGE_SECONDS = 5.0


class PrivateStreamCache:
    """
    Holds the latest order/balance updates received from a private feed.

    Updates are stamped with local receive time; venue timestamps are unreliable
    across exchanges and we only need to know how long *we* have held the value.
    """

    def __init__(
        self,
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
        clock: Callable[[], float] = time.time,
    ):
        self.max_age_seconds = max_age_seconds
        self._clock = clock
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._order_at: Dict[str, float] = {}
        self._balances: Optional[Dict[str, float]] = None
        self._balances_at = 0.0
        self.hits = 0
        self.misses = 0

    def update_order(self, order: Dict[str, Any]) -> None:
        """Records an order update. Orders without an id are ignored."""
        order_id = str(order.get("id") or "")
        if not order_id:
            return
        self._orders[order_id] = dict(order)
        self._order_at[order_id] = self._clock()

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Returns the cached order if it is still fresh, else None."""
        key = str(order_id)
        entry = self._orders.get(key)
        if entry is None:
            self.misses += 1
            return None
        if self._clock() - self._order_at.get(key, 0.0) > self.max_age_seconds:
            self.misses += 1
            return None
        self.hits += 1
        return dict(entry)

    def update_balances(self, balances: Dict[str, float]) -> None:
        self._balances = dict(balances)
        self._balances_at = self._clock()

    def get_balances(self) -> Optional[Dict[str, float]]:
        if self._balances is None:
            self.misses += 1
            return None
        if self._clock() - self._balances_at > self.max_age_seconds:
            self.misses += 1
            return None
        self.hits += 1
        return dict(self._balances)

    def forget_order(self, order_id: str) -> None:
        """Drops a terminal order so the cache does not grow without bound."""
        key = str(order_id)
        self._orders.pop(key, None)
        self._order_at.pop(key, None)

    @property
    def stats(self) -> Dict[str, int]:
        return {"hits": self.hits, "misses": self.misses, "orders": len(self._orders)}


async def fetch_order_preferring_stream(
    cache: Optional[PrivateStreamCache],
    fetch_rest: Callable[[], Any],
    order_id: str,
) -> Any:
    """
    Returns the order from the private feed when fresh, else polls REST.

    The REST result is written back into the cache so a burst of checks in the
    same tick does not hammer the venue.
    """
    if cache is not None:
        cached = cache.get_order(order_id)
        if cached is not None:
            return cached

    order = await fetch_rest()
    if cache is not None and isinstance(order, dict):
        cache.update_order(order)
    return order



class PrivateOrderStreamWorker:
    """Consume a CCXT Pro-compatible private order stream for one venue client.

    Standard ccxt.async_support does not expose watch_orders. In that case the
    worker reports the capability as unavailable and exits; callers must keep
    the REST fallback enabled. When a Pro-compatible client is injected, each
    normalized CCXT order is pushed into the shared freshness cache.
    """

    def __init__(self, venue: str, client: Any, cache: PrivateStreamCache):
        self.venue = str(venue or "unknown").lower()
        self.client = client
        self.cache = cache
        self.connected = False
        self.last_event_at = 0.0
        self.last_error = ""

    async def run(self) -> None:
        import asyncio

        watch_orders = getattr(self.client, "watch_orders", None)
        if not callable(watch_orders):
            self.last_error = "watch_orders is not available (CCXT Pro required)"
            logger.warning(
                "[Private WS] %s: %s; REST polling remains enabled",
                self.venue, self.last_error,
            )
            return

        delay = 1.0
        while True:
            try:
                updates = await watch_orders()
                if isinstance(updates, dict):
                    updates = [updates]
                if not isinstance(updates, (list, tuple)):
                    continue
                self.connected = True
                self.last_error = ""
                self.last_event_at = time.time()
                for order in updates:
                    if not isinstance(order, dict):
                        continue
                    normalized = dict(order)
                    info = normalized.get("info")
                    info = info if isinstance(info, dict) else {}
                    order_id = normalized.get("id") or info.get("orderId") or info.get("order_id")
                    if order_id is None:
                        continue
                    normalized["id"] = str(order_id)
                    # CCXT's unified filled field is cumulative. Preserve that
                    # semantic when a venue adapter exposes only raw payloads.
                    if normalized.get("filled") is None:
                        raw_filled = info.get("executedQty", info.get("dealVol"))
                        if raw_filled is not None:
                            try:
                                normalized["filled"] = float(raw_filled)
                            except (TypeError, ValueError):
                                continue
                    self.cache.update_order(normalized)
                delay = 1.0
            except asyncio.CancelledError:
                self.connected = False
                raise
            except Exception as exc:
                self.connected = False
                self.last_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "[Private WS] %s disconnected; REST fallback active: %s",
                    self.venue, self.last_error,
                )
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2.0)
