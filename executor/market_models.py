"""market models definitions."""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import time

from executor.quantity import AmountSpec
from executor.order_models import PositionState


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


