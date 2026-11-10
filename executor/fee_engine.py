"""Shared round-trip fee calculations for scanner/executor parity.

Rates are percentages (0.10 means 0.10%). Fee snapshots are immutable per
position so a later account-tier refresh cannot rewrite historical estimates.
"""
from dataclasses import dataclass
from math import isfinite


@dataclass(frozen=True)
class FeeRates:
    spot_entry_pct: float
    spot_exit_pct: float
    perp_entry_pct: float
    perp_exit_pct: float
    source: str = "CONFIG"
    verified: bool = False
    fetched_at_ms: int = 0
    surcharge_pct: float = 0.0

    def validate(self):
        for name in ("spot_entry_pct", "spot_exit_pct", "perp_entry_pct", "perp_exit_pct"):
            value = getattr(self, name)
            if not isfinite(value) or value < 0 or value > 100:
                raise ValueError(f"{name} must be finite and in [0,100]")
        if not isfinite(self.surcharge_pct) or self.surcharge_pct < 0 or self.surcharge_pct > 100:
            raise ValueError("surcharge_pct must be finite and in [0,100]")
        return self

    @classmethod
    def from_payload(cls, payload):
        data = payload if isinstance(payload, dict) else {}
        return cls(
            spot_entry_pct=float(data.get("spot_entry_fee_pct", 0.0)),
            spot_exit_pct=float(data.get("spot_exit_fee_pct", 0.0)),
            perp_entry_pct=float(data.get("perp_entry_fee_pct", 0.0)),
            perp_exit_pct=float(data.get("perp_exit_fee_pct", 0.0)),
            source=str(data.get("fee_source", "LEGACY_UNVERIFIED")),
            verified=bool(data.get("fee_verified", False)),
            fetched_at_ms=int(data.get("fee_fetched_at_ms", 0)),
            surcharge_pct=float(data.get("fee_asset_surcharge_pct", 0.0)),
        ).validate()

    def to_dict(self):
        return {
            "spot_entry_fee_pct": self.spot_entry_pct,
            "spot_exit_fee_pct": self.spot_exit_pct,
            "perp_entry_fee_pct": self.perp_entry_pct,
            "perp_exit_fee_pct": self.perp_exit_pct,
            "fee_source": self.source,
            "fee_verified": self.verified,
            "fee_fetched_at_ms": self.fetched_at_ms,
            "fee_asset_surcharge_pct": self.surcharge_pct,
        }


def round_trip_fee_breakdown_usdt(
    qty, spot_entry, spot_exit, perp_entry, perp_exit, rates
):
    """Return fee estimates split by spot/perpetual venue for all four fills."""
    rates.validate()
    values = (qty, spot_entry, spot_exit, perp_entry, perp_exit)
    if any(not isfinite(float(value)) or float(value) < 0 for value in values):
        raise ValueError("quantity and notionals must be finite and non-negative")

    spot_fees = qty * (
        spot_entry * (rates.spot_entry_pct + rates.surcharge_pct)
        + spot_exit * (rates.spot_exit_pct + rates.surcharge_pct)
    ) / 100.0
    futures_fees = qty * (
        perp_entry * (rates.perp_entry_pct + rates.surcharge_pct)
        + perp_exit * (rates.perp_exit_pct + rates.surcharge_pct)
    ) / 100.0
    return {
        "spot_fees_usdt": spot_fees,
        "futures_fees_usdt": futures_fees,
        "total_fees_usdt": spot_fees + futures_fees,
    }


def round_trip_fees_usdt(qty, spot_entry, spot_exit, perp_entry, perp_exit, rates):
    """Fee sum for both legs, both entry and exit, using executable notionals."""
    return round_trip_fee_breakdown_usdt(
        qty, spot_entry, spot_exit, perp_entry, perp_exit, rates
    )["total_fees_usdt"]


def required_spot_exit_usdt(
    qty, spot_entry, perp_entry, perp_exit, funding_usdt,
    target_profit_usdt, rates,
):
    """Solve the minimum spot exit notional that meets net-profit floor.

    The spot exit fee is included algebraically, so the returned price is not
    understated by ignoring the fee charged on the exit itself.
    """
    rates.validate()
    if qty <= 0 or spot_entry <= 0 or perp_entry <= 0 or perp_exit <= 0:
        raise ValueError("positive quantity and entry/exit prices are required")
    spot_entry_rate = (rates.spot_entry_pct + rates.surcharge_pct) / 100.0
    spot_exit_rate = (rates.spot_exit_pct + rates.surcharge_pct) / 100.0
    perp_entry_rate = (rates.perp_entry_pct + rates.surcharge_pct) / 100.0
    perp_exit_rate = (rates.perp_exit_pct + rates.surcharge_pct) / 100.0
    futures_pnl = qty * (perp_entry - perp_exit)
    fixed_per_unit = (
        spot_entry
        + spot_entry * spot_entry_rate
        + perp_entry * perp_entry_rate
        + perp_exit * perp_exit_rate
        + (target_profit_usdt - futures_pnl - funding_usdt) / qty
    )
    denominator = 1.0 - spot_exit_rate
    if denominator <= 0:
        raise ValueError("spot exit fee rate must be below 100%")
    return fixed_per_unit / denominator

def rates_for_position(position):
    """Return the immutable position snapshot; legacy positions use conservative rates."""
    if getattr(position, "fee_source", "") == "LEGACY_UNVERIFIED":
        return FeeRates(0.30, 0.30, 0.05, 0.05, "LEGACY_CONSERVATIVE_FALLBACK", False, 0)
    return FeeRates(
        float(position.spot_entry_fee_pct),
        float(position.spot_exit_fee_pct),
        float(position.perp_entry_fee_pct),
        float(position.perp_exit_fee_pct),
        str(position.fee_source),
        bool(position.fee_verified),
        int(position.fee_fetched_at_ms),
        float(getattr(position, "fee_asset_surcharge_pct", 0.0)),
    ).validate()
