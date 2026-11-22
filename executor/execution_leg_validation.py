"""Strict venue-bound Spot/Futures leg admission validation."""
from __future__ import annotations

from typing import Any, Mapping

from executor.execution_serialization import _positive_decimal, _required_text


def validate_execution_legs(legs: list[Mapping[str, Any]]) -> list[dict]:
    if len(legs) != 2 or {str(x.get("leg_type", "")).upper() for x in legs} != {"SPOT", "FUTURES"}:
        raise ValueError("a new session requires exactly one SPOT and one FUTURES leg")
    result = []
    for raw in legs:
        leg = dict(raw)
        leg_type = str(leg.get("leg_type", "")).upper()
        quantity_unit = _required_text(leg.get("quantity_unit"), "leg.quantity_unit").upper()
        if quantity_unit not in {"BASE", "QUOTE", "CONTRACTS"}:
            raise ValueError("leg.quantity_unit must be BASE, QUOTE or CONTRACTS")
        base_per_unit = (
            _positive_decimal(leg["base_per_unit"], "leg.base_per_unit")
            if leg.get("base_per_unit") is not None else None
        )
        if quantity_unit in {"BASE", "CONTRACTS"} and base_per_unit is None:
            raise ValueError("BASE/CONTRACTS legs require explicit base_per_unit conversion metadata")
        if leg_type == "FUTURES":
            _required_text(leg.get("settlement_asset"), "Futures settlement_asset")
            _positive_decimal(leg.get("contract_size"), "Futures contract_size")
        market_type = _required_text(leg.get("market_type"), "leg.market_type").lower()
        base_asset = _required_text(leg.get("base_asset"), "leg.base_asset").upper()
        if leg_type == "SPOT" and market_type != "spot":
            raise ValueError("SPOT leg market_type must be spot")
        if leg_type == "FUTURES":
            if market_type not in {"swap", "future", "futures"}:
                raise ValueError("FUTURES leg market_type must be swap or future")
            if str(leg.get("position_side") or "").upper() not in {"LONG", "SHORT"}:
                raise ValueError("FUTURES leg position_side must be LONG or SHORT")
            contract_type = str(leg.get("contract_type") or "").strip().upper()
            if contract_type not in {
                "LINEAR", "INVERSE", "LINEAR_PERPETUAL", "INVERSE_PERPETUAL"
            }:
                raise ValueError("FUTURES leg contract_type must identify linear or inverse contracts")
        result.append({
            "leg_id": _required_text(leg.get("leg_id"), "leg.leg_id"),
            "leg_type": leg_type,
            "venue": _required_text(leg.get("venue"), "leg.venue").lower(),
            "market_type": market_type,
            "symbol": _required_text(leg.get("symbol"), "leg.symbol"),
            "normalized_symbol": _required_text(leg.get("normalized_symbol"), "leg.normalized_symbol"),
            "base_asset": base_asset,
            "quote_asset": _required_text(leg.get("quote_asset"), "leg.quote_asset").upper(),
            "settlement_asset": str(leg.get("settlement_asset") or "").upper() or None,
            "contract_type": str(leg.get("contract_type") or "") or None,
            "contract_size": (
                _positive_decimal(leg["contract_size"], "leg.contract_size")
                if leg.get("contract_size") is not None else None
            ),
            "position_side": str(leg.get("position_side") or "").upper() or None,
            "quantity_unit": quantity_unit,
            "base_per_unit": base_per_unit,
            "order_quantity": (
                _positive_decimal(leg["order_quantity"], "leg.order_quantity", allow_zero=True)
                if leg.get("order_quantity") is not None else None
            ),
            "exposure_quantity": (
                _positive_decimal(leg["exposure_quantity"], "leg.exposure_quantity", allow_zero=True)
                if leg.get("exposure_quantity") is not None else None
            ),
            "target_qty": _positive_decimal(
                leg.get("target_qty", 0), "leg.target_qty", allow_zero=True
            ),
        })
    by_type = {item["leg_type"]: item for item in result}
    if by_type["SPOT"]["base_asset"] != by_type["FUTURES"]["base_asset"]:
        raise ValueError("SPOT and FUTURES legs must represent the same base asset")
    return result
    
