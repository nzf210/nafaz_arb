"""order models definitions."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional
import time
import uuid

REGISTRY_NS = uuid.UUID("0f2f8f2f-6f7a-4f2f-9c0c-2b7c1a6d5e10")


class OrderRole(Enum):
    """What an order is for. Needed because the same side/leg means different
    things: a spot SELL is an exit or a rollback, never an entry."""

    ENTRY = "ENTRY"
    HEDGE = "HEDGE"
    EXIT_SPOT = "EXIT_SPOT"
    EXIT_FUTURES = "EXIT_FUTURES"
    EMERGENCY = "EMERGENCY"
    FORCE_CLOSE = "FORCE_CLOSE"
    ROLLBACK = "ROLLBACK"

class OrderTerminal(Enum):
    """Recovery classification of an exchange order status."""

    OPEN = "OPEN"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"

class RecoveryStatus(Enum):
    """How much the bot trusts its own record of an order."""

    MANAGED = "MANAGED"            # linked to a position and re-adopted
    ADOPTED = "ADOPTED"            # found in durable storage, re-linked at startup
    UNVERIFIED = "UNVERIFIED"      # venue state could not be read
    ORPHAN_ORDER = "ORPHAN_ORDER"  # active on the venue, no bot position to attach to
    PHANTOM = "PHANTOM"            # recorded locally but absent at the venue

# ccxt status vocabulary plus indodax v2 labels mapped by the adapter.
TERMINAL_FILLED_STATUSES = ("closed", "filled")
TERMINAL_CANCELED_STATUSES = ("canceled", "cancelled", "rejected", "expired")
# Venue labels that mean "resting right now". Anything else is UNKNOWN.
OPEN_STATUSES = ("open", "new", "pending", "live", "partially_filled", "partiallyfilled")


class PositionState(Enum):
    IDLE = "IDLE"
    ENTRY_PENDING = "ENTRY_PENDING"
    ENTRY_PARTIAL = "ENTRY_PARTIAL"
    HEDGED_ACTIVE = "HEDGED_ACTIVE"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"
    # Terminal error state: the pair is flat but the last operation left the bot
    # in a state an operator must inspect (e.g. hedge rollback failed, spot leg
    # could not be liquidated). Distinguishing this from CLOSED means a failure
    # is observable through the status API instead of looking like a clean exit.
    FAILED = "FAILED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"

def _as_spec_dict(value: Any) -> Optional[Dict[str, Any]]:
    """Normalizes an amount spec (dataclass or mapping) for persistence."""
    if value is None:
        return None
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, dict):
        return value
    return None

def _record_string(data: Dict[str, Any], key: str, default: str = "") -> str:
    return str(data.get(key) or default)


def _record_float(data: Dict[str, Any], key: str) -> float:
    return float(data.get(key) or 0.0)


def _record_int(data: Dict[str, Any], key: str) -> int:
    return int(data.get(key) or 0)


def _record_context(data: Dict[str, Any]):
    context = data.get("recovery_context")
    return context if isinstance(context, dict) else None


@dataclass
class OrderRecord:
    """One row of the universal order registry.

    Identity rules, in order of authority:
    ``exchange_order_id`` (venue truth) -> ``client_order_id`` (survives a crash
    between submit and acknowledgement) -> ``local_order_id`` (golden id).

    ``pair_key`` is empty for an ORPHAN_ORDER: the whole point of an orphan is
    that no bot position could be linked to it.
    """

    local_order_id: str
    client_order_id: str
    # Spelling the venue actually stored for ``client_order_id``. A Binance
    # Cloud venue rewrites a client id into a prefixed hash, and the rewrite is
    # not invertible, so a restart that only remembers the logical id can never
    # match the order again. Empty means the venue kept the id verbatim.
    venue_client_order_id: str = ""
    exchange_order_id: str = ""
    position_id: str = ""
    pair_key: str = ""
    exchange: str = ""
    market_type: str = ""
    symbol: str = ""
    side: str = ""
    order_type: str = ""
    order_role: str = OrderRole.ENTRY.value
    price: float = 0.0
    original_quantity: float = 0.0
    filled_quantity: float = 0.0
    remaining_quantity: float = 0.0
    average_fill_price: float = 0.0
    status: str = "open"
    reduce_only: bool = False
    is_mock: bool = True
    recovery_status: str = RecoveryStatus.MANAGED.value
    # Venue amount rules are required to rebuild a hedgeable entry. They cannot
    # be re-derived from an exchange order id, so they travel with the record.
    local_amount_spec: Optional[Dict[str, Any]] = None
    global_amount_spec: Optional[Dict[str, Any]] = None
    # Everything the entry lifecycle needs to finish a fill after a restart and
    # cannot re-derive from the venue: the FX snapshot, fee profile, funding
    # window and spread target that were valid when the order was submitted.
    recovery_context: Optional[Dict[str, Any]] = None
    # Quantity already hedged against this entry's fills. Without it, a restart
    # that re-reads the same fill would submit a second hedge for the same spot.
    hedged_quantity: float = 0.0
    created_at: int = field(default_factory=lambda: int(time.time() * 1000))
    updated_at: int = field(default_factory=lambda: int(time.time() * 1000))
    last_reconciled_at: int = 0

    @staticmethod
    def local_id_for(exchange: str, client_order_id: str, exchange_order_id: str = "") -> str:
        """Deterministic id from the most stable identifier available."""
        key = client_order_id or exchange_order_id
        return str(uuid.uuid5(REGISTRY_NS, f"order:{exchange}:{key}"))

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_FILLED_STATUSES + TERMINAL_CANCELED_STATUSES

    def to_dict(self) -> Dict[str, Any]:
        return {
            "local_order_id": self.local_order_id,
            "client_order_id": self.client_order_id,
            "venue_client_order_id": self.venue_client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "position_id": self.position_id,
            "pair_key": self.pair_key,
            "exchange": self.exchange,
            "market_type": self.market_type,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "order_role": self.order_role,
            "price": self.price,
            "original_quantity": self.original_quantity,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "average_fill_price": self.average_fill_price,
            "status": self.status,
            "reduce_only": self.reduce_only,
            "is_mock": self.is_mock,
            "recovery_status": self.recovery_status,
            "local_amount_spec": self.local_amount_spec,
            "global_amount_spec": self.global_amount_spec,
            "recovery_context": self.recovery_context,
            "hedged_quantity": self.hedged_quantity,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "last_reconciled_at": self.last_reconciled_at,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "OrderRecord":
        record = cls(
            local_order_id=_record_string(d, "local_order_id"),
            client_order_id=_record_string(d, "client_order_id"),
            venue_client_order_id=_record_string(d, "venue_client_order_id"),
            exchange_order_id=_record_string(d, "exchange_order_id"),
            position_id=_record_string(d, "position_id"),
            pair_key=_record_string(d, "pair_key"),
            exchange=_record_string(d, "exchange"),
            market_type=_record_string(d, "market_type"),
            symbol=_record_string(d, "symbol"),
            side=_record_string(d, "side"),
            order_type=_record_string(d, "order_type"),
            order_role=_record_string(d, "order_role", OrderRole.ENTRY.value),
            price=_record_float(d, "price"),
            original_quantity=_record_float(d, "original_quantity"),
            filled_quantity=_record_float(d, "filled_quantity"),
            remaining_quantity=_record_float(d, "remaining_quantity"),
            average_fill_price=_record_float(d, "average_fill_price"),
            status=_record_string(d, "status", "open"),
            reduce_only=bool(d.get("reduce_only", False)),
            is_mock=bool(d.get("is_mock", True)),
            recovery_status=_record_string(
                d, "recovery_status", RecoveryStatus.MANAGED.value
            ),
            local_amount_spec=_as_spec_dict(d.get("local_amount_spec")),
            global_amount_spec=_as_spec_dict(d.get("global_amount_spec")),
            recovery_context=_record_context(d),
            hedged_quantity=_record_float(d, "hedged_quantity"),
            created_at=_record_int(d, "created_at"),
            updated_at=_record_int(d, "updated_at"),
            last_reconciled_at=_record_int(d, "last_reconciled_at"),
        )
        if not record.local_order_id and record.client_order_id:
            record.local_order_id = cls.local_id_for(
                record.exchange, record.client_order_id, record.exchange_order_id
            )
        return record

    def classify_status(self) -> OrderTerminal:
        """Maps a venue status onto the three states recovery can act on.

        An unrecognised status is UNKNOWN, never OPEN: adopting an order whose
        state the bot cannot interpret is how a half-canceled order ends up
        treated as resting.
        """
        status = str(self.status or "").strip().lower()
        if status in TERMINAL_FILLED_STATUSES:
            return OrderTerminal.FILLED
        if status in TERMINAL_CANCELED_STATUSES:
            return OrderTerminal.CANCELED
        if status in OPEN_STATUSES:
            return OrderTerminal.OPEN
        return OrderTerminal.UNKNOWN

