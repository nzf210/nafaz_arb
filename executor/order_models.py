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
            local_order_id=str(d.get("local_order_id") or ""),
            client_order_id=str(d.get("client_order_id") or ""),
            exchange_order_id=str(d.get("exchange_order_id") or ""),
            position_id=str(d.get("position_id") or ""),
            pair_key=str(d.get("pair_key") or ""),
            exchange=str(d.get("exchange") or ""),
            market_type=str(d.get("market_type") or ""),
            symbol=str(d.get("symbol") or ""),
            side=str(d.get("side") or ""),
            order_type=str(d.get("order_type") or ""),
            order_role=str(d.get("order_role") or OrderRole.ENTRY.value),
            price=float(d.get("price") or 0.0),
            original_quantity=float(d.get("original_quantity") or 0.0),
            filled_quantity=float(d.get("filled_quantity") or 0.0),
            remaining_quantity=float(d.get("remaining_quantity") or 0.0),
            average_fill_price=float(d.get("average_fill_price") or 0.0),
            status=str(d.get("status") or "open"),
            reduce_only=bool(d.get("reduce_only", False)),
            is_mock=bool(d.get("is_mock", True)),
            recovery_status=str(d.get("recovery_status") or RecoveryStatus.MANAGED.value),
            local_amount_spec=_as_spec_dict(d.get("local_amount_spec")),
            global_amount_spec=_as_spec_dict(d.get("global_amount_spec")),
            recovery_context=d.get("recovery_context") if isinstance(d.get("recovery_context"), dict) else None,
            hedged_quantity=float(d.get("hedged_quantity") or 0.0),
            created_at=int(d.get("created_at") or 0),
            updated_at=int(d.get("updated_at") or 0),
            last_reconciled_at=int(d.get("last_reconciled_at") or 0),
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

@dataclass
