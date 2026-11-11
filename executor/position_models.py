"""position models definitions."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import time

from executor.quantity import AmountSpec
from executor.order_models import PositionState


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
