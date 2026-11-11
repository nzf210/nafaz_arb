from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any
import time

from executor.quantity import AmountSpec


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
