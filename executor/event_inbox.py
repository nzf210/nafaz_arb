"""Private Stream Event Normalizer & Deduplicated Event Inbox (Executor V2).

Implements Section 8 of EXECUTOR-ARCHITECTURE-V2.md:
- Normalizes venue-specific private WebSocket and REST events into canonical types:
  ORDER_UPDATE, FILL, POSITION_UPDATE
- Deduplicates events using unique venue + trade_id / fill_id / event_id
- Updates PrivateStreamCache and notifies TwoLegCoordinator / LegManagers
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, Set

from executor.private_stream import PrivateStreamCache

logger = logging.getLogger("executor.event_inbox")


class StreamEventType(str, Enum):
    ORDER_UPDATE = "ORDER_UPDATE"
    FILL = "FILL"
    POSITION_UPDATE = "POSITION_UPDATE"


@dataclass
class NormalizedEvent:
    event_id: str
    event_type: StreamEventType
    venue: str
    symbol: str
    exchange_order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    trade_id: Optional[str] = None
    side: str = ""
    status: str = ""
    price: float = 0.0
    quantity: float = 0.0
    filled_qty: float = 0.0
    fee: float = 0.0
    fee_currency: str = ""
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    raw_payload: Dict[str, Any] = field(default_factory=dict)


class EventNormalizer:
    """Normalizes raw exchange WebSocket / REST payloads into NormalizedEvent."""

    @staticmethod
    def normalize_order(venue: str, raw: dict) -> NormalizedEvent:
        venue = venue.lower().strip()
        order_id = str(raw.get("id") or raw.get("orderId") or "")
        client_id = str(raw.get("clientOrderId") or raw.get("client_order_id") or "")
        symbol = str(raw.get("symbol") or "")
        side = str(raw.get("side") or "").lower()
        status = str(raw.get("status") or "").upper()
        price = float(raw.get("price") or 0.0)
        qty = float(raw.get("amount") or raw.get("origQty") or 0.0)
        filled = float(raw.get("filled") or raw.get("executedQty") or 0.0)

        event_id = f"{venue}:order:{order_id}:{filled}"
        return NormalizedEvent(
            event_id=event_id,
            event_type=StreamEventType.ORDER_UPDATE,
            venue=venue,
            symbol=symbol,
            exchange_order_id=order_id,
            client_order_id=client_id,
            side=side,
            status=status,
            price=price,
            quantity=qty,
            filled_qty=filled,
            raw_payload=raw,
        )

    @staticmethod
    def normalize_trade(venue: str, raw: dict) -> NormalizedEvent:
        venue = venue.lower().strip()
        trade_id = str(raw.get("id") or raw.get("tradeId") or "")
        order_id = str(raw.get("order") or raw.get("orderId") or "")
        symbol = str(raw.get("symbol") or "")
        side = str(raw.get("side") or "").lower()
        price = float(raw.get("price") or 0.0)
        qty = float(raw.get("amount") or raw.get("qty") or 0.0)

        fee_info = raw.get("fee") or {}
        fee_cost = float(fee_info.get("cost") or 0.0)
        fee_currency = str(fee_info.get("currency") or "")

        event_id = f"{venue}:trade:{trade_id or order_id}:{qty}:{price}"
        return NormalizedEvent(
            event_id=event_id,
            event_type=StreamEventType.FILL,
            venue=venue,
            symbol=symbol,
            exchange_order_id=order_id,
            trade_id=trade_id,
            side=side,
            price=price,
            quantity=qty,
            filled_qty=qty,
            fee=fee_cost,
            fee_currency=fee_currency,
            raw_payload=raw,
        )


class EventInbox:
    """Ingests and deduplicates normalized events, dispatching to listeners and stream cache."""

    def __init__(self, stream_cache: Optional[PrivateStreamCache] = None, *, max_dedupe_history: int = 10000):
        self.stream_cache = stream_cache or PrivateStreamCache()
        self.max_dedupe_history = max_dedupe_history
        self._seen_event_ids: Set[str] = set()
        self._handlers: list[Callable[[NormalizedEvent], Any]] = []

    def register_handler(self, handler: Callable[[NormalizedEvent], Any]) -> None:
        self._handlers.append(handler)

    def ingest_event(self, event: NormalizedEvent) -> bool:
        """Ingest event with strict deduplication. Returns True if accepted, False if duplicate."""
        if event.event_id in self._seen_event_ids:
            logger.debug("Duplicate event ignored: %s", event.event_id)
            return False

        if len(self._seen_event_ids) >= self.max_dedupe_history:
            self._seen_event_ids.clear()
        self._seen_event_ids.add(event.event_id)

        # Update stream cache
        if event.event_type in (StreamEventType.ORDER_UPDATE, StreamEventType.FILL):
            if event.exchange_order_id:
                self.stream_cache.update_order({
                    "id": event.exchange_order_id,
                    "symbol": event.symbol,
                    "status": event.status,
                    "filled": event.filled_qty,
                    "price": event.price,
                    "clientOrderId": event.client_order_id,
                })

        # Dispatch to registered handlers
        for handler in self._handlers:
            try:
                handler(event)
            except Exception as exc:
                logger.error("Handler error for event %s: %s", event.event_id, exc)

        return True
