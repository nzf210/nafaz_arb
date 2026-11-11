from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any
import time
import uuid

from executor.quantity import AmountSpec

# Namespace for every locally derived id (order registry rows, fill rows). A
# fixed namespace keeps ids stable across restarts, which is what makes a
# re-adopted order update its existing row instead of creating a duplicate.
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
class PairInfo:
    local_exchange: str
    global_exchange: str
    local_symbol: str
    global_symbol: str
    base_asset: str
    quote_asset: str
    local_price_tick: float = 1.0
    local_lot_size: float = 0.0
    local_amount_spec: Optional[AmountSpec] = None
    global_amount_spec: Optional[AmountSpec] = None
    global_quote_asset: str = ""

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PairInfo":
        local_lot_size = float(d.get("local_lot_size", 0.0))
        local_amount_spec = AmountSpec.from_dict(d.get("local_amount_spec"))
        if local_amount_spec is None and local_lot_size > 0:
            local_amount_spec = AmountSpec(
                unit="BASE",
                step=local_lot_size,
                minimum=local_lot_size,
                base_per_unit=1.0,
            )
        return cls(
            local_exchange=d.get("local_exchange", ""),
            global_exchange=d.get("global_exchange", ""),
            local_symbol=d.get("local_symbol", ""),
            global_symbol=d.get("global_symbol", ""),
            base_asset=d.get("base_asset", ""),
            quote_asset=d.get("quote_asset", ""),
            local_price_tick=float(d.get("local_price_tick", 1.0)),
            local_lot_size=local_lot_size,
            local_amount_spec=local_amount_spec,
            global_amount_spec=AmountSpec.from_dict(d.get("global_amount_spec")),
            global_quote_asset=d.get("global_quote_asset", ""),
        )


@dataclass
class PricingSnapshot:
    local_best_bid: float
    local_best_ask: float
    global_best_bid_usdt: float
    global_best_ask_usdt: float
    usdt_idr_rate: float
    timestamp_ms: int

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PricingSnapshot":
        return cls(
            local_best_bid=float(d.get("local_best_bid", 0.0)),
            local_best_ask=float(d.get("local_best_ask", 0.0)),
            global_best_bid_usdt=float(d.get("global_best_bid_usdt", 0.0)),
            global_best_ask_usdt=float(d.get("global_best_ask_usdt", 0.0)),
            usdt_idr_rate=float(d.get("usdt_idr_rate", 0.0)),
            timestamp_ms=int(d.get("timestamp_ms", 0)),
        )

    def is_stale(self, max_latency_ms: int = 500) -> bool:
        now_ms = int(time.time() * 1000)
        return (now_ms - self.timestamp_ms) > max_latency_ms


@dataclass
class FundingMetrics:
    current_funding_rate: float
    next_funding_time_ms: int
    minutes_to_next_funding: float
    is_sniping_window: bool

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FundingMetrics":
        return cls(
            current_funding_rate=float(d.get("current_funding_rate", 0.0)),
            next_funding_time_ms=int(d.get("next_funding_time_ms", 0)),
            minutes_to_next_funding=float(d.get("minutes_to_next_funding", 0.0)),
            is_sniping_window=bool(d.get("is_sniping_window", False)),
        )


@dataclass
class ExecutionDirectives:
    action: str
    recommended_limit_buy_idr: float
    entry_spread_pct: float
    min_entry_spread_pct: float
    base_target_exit_spread_pct: float
    max_position_usdt: float
    cancel_if_global_bid_drops_below_usdt: float

    # True when the scanner already re-validated this exact quote against the
    # live orderbook within the same scan. When False the executor must do its
    scanner_validated: bool
    scanner_validated: bool = False
    estimated_net_profit_usdt: float = 0.0
    spot_entry_fee_pct: float = 0.0
    spot_exit_fee_pct: float = 0.0
    perp_entry_fee_pct: float = 0.0
    perp_exit_fee_pct: float = 0.0
    fee_source: str = "LEGACY_UNVERIFIED"
    fee_verified: bool = False
    fee_fetched_at_ms: int = 0
    fee_asset_surcharge_pct: float = 0.0
    estimated_fees_usdt: float = 0.0
    quantity: float = 0.0
    spot_entry_price_usdt: float = 0.0
    spot_exit_price_usdt: float = 0.0
    perp_entry_price_usdt: float = 0.0
    perp_exit_price_usdt: float = 0.0

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ExecutionDirectives":
        return cls(
            action=d.get("action", "PLACE_MAKER_BUY"),
            recommended_limit_buy_idr=float(d.get("recommended_limit_buy_idr", 0.0)),
            entry_spread_pct=float(d.get("entry_spread_pct", 0.0)),
            min_entry_spread_pct=float(d.get("min_entry_spread_pct", 0.0)),
            base_target_exit_spread_pct=float(d.get("base_target_exit_spread_pct", 0.3)),
            max_position_usdt=float(d.get("max_position_usdt", 0.0)),
            cancel_if_global_bid_drops_below_usdt=float(d.get("cancel_if_global_bid_drops_below_usdt", 0.0)),
            scanner_validated=bool(d.get("scanner_validated", False)),
            estimated_net_profit_usdt=float(d.get("estimated_net_profit_usdt", 0.0)),
            spot_entry_fee_pct=float(d.get("spot_entry_fee_pct", 0.0)),
            spot_exit_fee_pct=float(d.get("spot_exit_fee_pct", 0.0)),
            perp_entry_fee_pct=float(d.get("perp_entry_fee_pct", 0.0)),
            perp_exit_fee_pct=float(d.get("perp_exit_fee_pct", 0.0)),
            fee_source=str(d.get("fee_source", "LEGACY_UNVERIFIED")),
            fee_verified=bool(d.get("fee_verified", False)),
            fee_fetched_at_ms=int(d.get("fee_fetched_at_ms", 0)),
            fee_asset_surcharge_pct=float(d.get("fee_asset_surcharge_pct", 0.0)),
            estimated_fees_usdt=float(d.get("estimated_fees_usdt", 0.0)),
            quantity=float(d.get("quantity", 0.0)),
            spot_entry_price_usdt=float(d.get("spot_entry_price_usdt", 0.0)),
            spot_exit_price_usdt=float(d.get("spot_exit_price_usdt", 0.0)),
            perp_entry_price_usdt=float(d.get("perp_entry_price_usdt", 0.0)),
            perp_exit_price_usdt=float(d.get("perp_exit_price_usdt", 0.0)),
        )


@dataclass
class ExecutorPayload:
    pair_info: PairInfo
    pricing_snapshot: PricingSnapshot
    funding_metrics: FundingMetrics
    execution_directives: ExecutionDirectives

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ExecutorPayload":
        return cls(
            pair_info=PairInfo.from_dict(d.get("pair_info", {})),
            pricing_snapshot=PricingSnapshot.from_dict(d.get("pricing_snapshot", {})),
            funding_metrics=FundingMetrics.from_dict(d.get("funding_metrics", {})),
            execution_directives=ExecutionDirectives.from_dict(d.get("execution_directives", {})),
        )


@dataclass
class ActivePosition:
    local_exchange: str
    global_exchange: str
    symbol: str
    entry_price_idr: float
    short_entry_usdt: float
    qty: float
    accumulated_funding_usdt: float = 0.0
    last_funding_time_ms: int = 0
    next_funding_time_ms: int = 0
    base_target_exit_spread_pct: float = 0.3
    current_funding_rate: float = 0.0
    local_price_tick: float = 1.0
    initial_qty: float = 0.0
    opened_at_ms: int = 0
    updated_at: int = field(default_factory=lambda: int(time.time() * 1000))
    current_spot_bid_idr: float = 0.0
    current_perp_ask_usdt: float = 0.0
    spot_unrealized_pnl_idr: float = 0.0
    futures_unrealized_pnl_usdt: float = 0.0
    net_unrealized_pnl_usdt: float = 0.0
    net_unrealized_pnl_idr: float = 0.0
    net_unrealized_roi_pct: float = 0.0
    spot_cost_usdt: float = 0.0
    futures_margin_usdt: float = 0.0
    state: str = "HEDGED_ACTIVE"
    base_asset: str = ""
    global_symbol: str = ""
    local_lot_size: float = 0.0
    local_amount_spec: Optional[AmountSpec] = None
    global_amount_spec: Optional[AmountSpec] = None
    spot_entry_fee_pct: float = 0.0
    spot_exit_fee_pct: float = 0.0
    perp_entry_fee_pct: float = 0.0
    perp_exit_fee_pct: float = 0.0
    fee_source: str = "LEGACY_UNVERIFIED"
    fee_verified: bool = False
    fee_fetched_at_ms: int = 0
    fee_asset_surcharge_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "local_exchange": self.local_exchange,
            "global_exchange": self.global_exchange,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "global_symbol": self.global_symbol,
            "local_lot_size": self.local_lot_size,
            "local_amount_spec": self.local_amount_spec.to_dict() if self.local_amount_spec else None,
            "global_amount_spec": self.global_amount_spec.to_dict() if self.global_amount_spec else None,
            "entry_price_idr": self.entry_price_idr,
            "short_entry_usdt": self.short_entry_usdt,
            "qty": self.qty,
            "accumulated_funding_usdt": self.accumulated_funding_usdt,
            "last_funding_time_ms": self.last_funding_time_ms,
            "next_funding_time_ms": self.next_funding_time_ms,
            "base_target_exit_spread_pct": self.base_target_exit_spread_pct,
            "current_funding_rate": self.current_funding_rate,
            "local_price_tick": self.local_price_tick,
            "initial_qty": self.initial_qty or self.qty,
            "opened_at_ms": self.opened_at_ms,
            "updated_at": self.updated_at,
            "current_spot_bid_idr": self.current_spot_bid_idr,
            "current_perp_ask_usdt": self.current_perp_ask_usdt,
            "spot_unrealized_pnl_idr": self.spot_unrealized_pnl_idr,
            "futures_unrealized_pnl_usdt": self.futures_unrealized_pnl_usdt,
            "net_unrealized_pnl_usdt": self.net_unrealized_pnl_usdt,
            "net_unrealized_pnl_idr": self.net_unrealized_pnl_idr,
            "net_unrealized_roi_pct": self.net_unrealized_roi_pct,
            "spot_cost_usdt": self.spot_cost_usdt,
            "futures_margin_usdt": self.futures_margin_usdt,
            "state": self.state,
            "spot_entry_fee_pct": self.spot_entry_fee_pct,
            "spot_exit_fee_pct": self.spot_exit_fee_pct,
            "perp_entry_fee_pct": self.perp_entry_fee_pct,
            "perp_exit_fee_pct": self.perp_exit_fee_pct,
            "fee_source": self.fee_source,
            "fee_verified": self.fee_verified,
            "fee_fetched_at_ms": self.fee_fetched_at_ms,
            "fee_asset_surcharge_pct": self.fee_asset_surcharge_pct,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ActivePosition":
        local_lot_size = float(d.get("local_lot_size", 0.0))
        local_amount_spec = AmountSpec.from_dict(d.get("local_amount_spec"))
        if local_amount_spec is None and local_lot_size > 0:
            local_amount_spec = AmountSpec(
                unit="BASE",
                step=local_lot_size,
                minimum=local_lot_size,
                base_per_unit=1.0,
            )
        if local_amount_spec is None:
            local_amount_spec = AmountSpec("BASE", 1e-12, 0.0, 1.0)
        global_amount_spec = AmountSpec.from_dict(d.get("global_amount_spec"))
        if global_amount_spec is None:
            # Existing positions were submitted in the amount units CCXT accepted
            # before normalized specs were persisted. Keep their de-risk path
            # compatible; new entries still require explicit rules.
            global_amount_spec = AmountSpec("BASE", 1e-12, 0.0, 1.0)
        return cls(
            local_exchange=d.get("local_exchange", ""),
            global_exchange=d.get("global_exchange", ""),
            symbol=d.get("symbol", ""),
            base_asset=d.get("base_asset", ""),
            global_symbol=d.get("global_symbol", ""),
            local_lot_size=local_lot_size,
            local_amount_spec=local_amount_spec,
            global_amount_spec=global_amount_spec,
            entry_price_idr=float(d.get("entry_price_idr", 0.0)),
            short_entry_usdt=float(d.get("short_entry_usdt", 0.0)),
            qty=float(d.get("qty", 0.0)),
            accumulated_funding_usdt=float(d.get("accumulated_funding_usdt", 0.0)),
            last_funding_time_ms=int(d.get("last_funding_time_ms", 0)),
            next_funding_time_ms=int(d.get("next_funding_time_ms", 0)),
            base_target_exit_spread_pct=float(d.get("base_target_exit_spread_pct", 0.3)),
            current_funding_rate=float(d.get("current_funding_rate", 0.0)),
            local_price_tick=float(d.get("local_price_tick", 1.0)),
            initial_qty=float(d.get("initial_qty", 0.0)),
            opened_at_ms=int(d.get("opened_at_ms", 0)),
            updated_at=int(d.get("updated_at", 0)),
            current_spot_bid_idr=float(d.get("current_spot_bid_idr", 0.0)),
            current_perp_ask_usdt=float(d.get("current_perp_ask_usdt", 0.0)),
            spot_unrealized_pnl_idr=float(d.get("spot_unrealized_pnl_idr", 0.0)),
            futures_unrealized_pnl_usdt=float(d.get("futures_unrealized_pnl_usdt", 0.0)),
            net_unrealized_pnl_usdt=float(d.get("net_unrealized_pnl_usdt", 0.0)),
            net_unrealized_pnl_idr=float(d.get("net_unrealized_pnl_idr", 0.0)),
            net_unrealized_roi_pct=float(d.get("net_unrealized_roi_pct", 0.0)),
            spot_cost_usdt=float(d.get("spot_cost_usdt", 0.0)),
            futures_margin_usdt=float(d.get("futures_margin_usdt", 0.0)),
            state=d.get("state", "HEDGED_ACTIVE"),
            spot_entry_fee_pct=float(d.get("spot_entry_fee_pct", 0.0)),
            spot_exit_fee_pct=float(d.get("spot_exit_fee_pct", 0.0)),
            perp_entry_fee_pct=float(d.get("perp_entry_fee_pct", 0.0)),
            perp_exit_fee_pct=float(d.get("perp_exit_fee_pct", 0.0)),
            fee_source=str(d.get("fee_source", "LEGACY_UNVERIFIED")),
            fee_verified=bool(d.get("fee_verified", False)),
            fee_fetched_at_ms=int(d.get("fee_fetched_at_ms", 0)),
            fee_asset_surcharge_pct=float(d.get("fee_asset_surcharge_pct", 0.0)),
        )


@dataclass
class ClosedTrade:
    trade_id: str
    local_exchange: str
    global_exchange: str
    symbol: str
    qty: float
    entry_price_idr: float
    exit_price_idr: float
    short_entry_usdt: float
    short_exit_usdt: float
    spot_pnl_idr: float
    futures_pnl_usdt: float
    accumulated_funding_usdt: float
    total_fees_usdt: float
    net_profit_usdt: float
    net_profit_idr: float
    net_roi_pct: float
    is_mock: bool = True
    opened_at_ms: int = 0
    closed_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "local_exchange": self.local_exchange,
            "global_exchange": self.global_exchange,
            "symbol": self.symbol,
            "qty": self.qty,
            "entry_price_idr": self.entry_price_idr,
            "exit_price_idr": self.exit_price_idr,
            "short_entry_usdt": self.short_entry_usdt,
            "short_exit_usdt": self.short_exit_usdt,
            "spot_pnl_idr": self.spot_pnl_idr,
            "futures_pnl_usdt": self.futures_pnl_usdt,
            "accumulated_funding_usdt": self.accumulated_funding_usdt,
            "total_fees_usdt": self.total_fees_usdt,
            "net_profit_usdt": self.net_profit_usdt,
            "net_profit_idr": self.net_profit_idr,
            "net_roi_pct": self.net_roi_pct,
            "is_mock": self.is_mock,
            "opened_at_ms": self.opened_at_ms,
            "closed_at_ms": self.closed_at_ms,
        }
