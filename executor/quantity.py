"""Venue-neutral amount rules and conversions used by every order path."""

import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from math import gcd
from typing import Any, Iterable


@dataclass(frozen=True)
class AmountSpec:
    unit: str
    step: float
    minimum: float
    base_per_unit: float

    @classmethod
    def from_dict(cls, value: Any) -> "AmountSpec | None":
        if not isinstance(value, dict):
            return None
        try:
            spec = cls(
                unit=str(value.get("unit", "")).upper(),
                step=float(value["step"]),
                minimum=float(value.get("min", 0.0)),
                base_per_unit=float(value["base_per_unit"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid amount spec: {value}") from exc
        spec.validate()
        return spec

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit": self.unit,
            "step": self.step,
            "min": self.minimum,
            "base_per_unit": self.base_per_unit,
        }

    def validate(self) -> None:
        values = (self.step, self.minimum, self.base_per_unit)
        if (
            self.unit not in {"BASE", "CONTRACT"}
            or not all(math.isfinite(value) for value in values)
            or self.step <= 0
            or self.minimum < 0
            or self.base_per_unit <= 0
            or (self.unit == "BASE" and self.base_per_unit != 1)
        ):
            raise ValueError(f"invalid amount spec: {self}")


def _decimal(value: float | Decimal) -> Decimal:
    result = value if isinstance(value, Decimal) else Decimal(str(value))
    if not result.is_finite():
        raise ValueError(f"amount must be finite: {value}")
    return result


def base_step(spec: AmountSpec) -> Decimal:
    spec.validate()
    return _decimal(spec.step) * _decimal(spec.base_per_unit)


def _decimal_lcm(values: Iterable[Decimal]) -> Decimal:
    values = tuple(values)
    scale = max(0, max(-value.normalize().as_tuple().exponent for value in values))
    factor = 10**scale
    integers = [int(value * factor) for value in values]
    common = integers[0]
    for value in integers[1:]:
        common = abs(common * value) // gcd(common, value)
    return Decimal(common) / factor


def quantize_base_quantity(quantity: float, *specs: AmountSpec) -> float:
    """Round base quantity down to a size representable by every market leg."""
    requested = _decimal(quantity)
    if requested <= 0 or not specs:
        raise ValueError(f"invalid quantity or missing amount specs: {quantity}")
    for spec in specs:
        spec.validate()
    compatible_step = _decimal_lcm(base_step(spec) for spec in specs)
    amount = (requested / compatible_step).to_integral_value(rounding=ROUND_DOWN) * compatible_step
    for spec in specs:
        units = amount / _decimal(spec.base_per_unit)
        if units < _decimal(spec.minimum):
            raise ValueError(
                f"quantity {amount} is below {spec.unit} minimum {spec.minimum}"
            )
    if amount <= 0:
        raise ValueError("quantity rounds to zero under venue amount rules")
    return float(amount)


def base_to_order_amount(quantity: float, spec: AmountSpec) -> float:
    """Convert base quantity to an exact venue amount (base units or contracts)."""
    spec.validate()
    base_qty = _decimal(quantity)
    if base_qty <= 0:
        raise ValueError(f"invalid base quantity: {quantity}")
    per_unit = _decimal(spec.base_per_unit)
    step = _decimal(spec.step)
    units = base_qty / per_unit
    rounded_units = (units / step).to_integral_value(rounding=ROUND_DOWN) * step
    if rounded_units < _decimal(spec.minimum):
        raise ValueError(
            f"order amount {rounded_units} is below {spec.unit} minimum {spec.minimum}"
        )
    return float(rounded_units)


def order_amount_to_base(amount: float, spec: AmountSpec) -> float:
    """Convert a venue order amount or fill back to base-asset quantity."""
    spec.validate()
    value = _decimal(amount)
    if value < 0:
        raise ValueError(f"invalid order amount: {amount}")
    return float(value * _decimal(spec.base_per_unit))
