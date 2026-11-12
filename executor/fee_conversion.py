"""Deterministic conversion of exchange fill fees into the fill quote asset."""

from math import isfinite
from typing import Any


def split_market_symbol(symbol: str) -> tuple[str, str] | None:
    """Return base/quote assets from a CCXT or venue market symbol."""
    value = str(symbol or "").strip().upper().split(":", 1)[0]
    for separator in ("/", "_", "-"):
        parts = value.split(separator)
        if len(parts) == 2 and all(parts):
            return parts[0], parts[1]
    return None


def fee_components_in_quote(
    fees: list[dict[str, Any]], symbol: str, price: float
) -> dict[str, Any]:
    """Convert base/quote-denominated fees; refuse unknown third-asset FX."""
    market = split_market_symbol(symbol)
    if not market or not fees:
        return _unverified(symbol)
    base_asset, quote_asset = market
    try:
        execution_price = float(price)
    except (TypeError, ValueError):
        return _unverified(symbol, quote_asset)
    if not isfinite(execution_price) or execution_price <= 0:
        return _unverified(symbol, quote_asset)

    converted = []
    total_quote = 0.0
    for fee in fees:
        if not isinstance(fee, dict):
            return _unverified(symbol, quote_asset)
        asset = str(fee.get("currency") or "").strip().upper()
        try:
            amount = float(fee.get("cost"))
        except (TypeError, ValueError):
            return _unverified(symbol, quote_asset)
        if not asset or not isfinite(amount):
            return _unverified(symbol, quote_asset)
        if asset == quote_asset:
            quote_amount = amount
            method = "DIRECT_QUOTE"
        elif asset == base_asset:
            quote_amount = amount * execution_price
            method = "BASE_TIMES_FILL_PRICE"
        else:
            return _unverified(symbol, quote_asset)
        if not isfinite(quote_amount):
            return _unverified(symbol, quote_asset)
        total_quote += quote_amount
        converted.append({
            "asset": asset,
            "amount": amount,
            "quote_amount": quote_amount,
            "quote_asset": quote_asset,
            "method": method,
        })
    if not isfinite(total_quote):
        return _unverified(symbol, quote_asset)
    return {
        "quote_amount": total_quote,
        "quote_asset": quote_asset,
        "verified": True,
        "components": converted,
    }


def _unverified(symbol: str, quote_asset: str | None = None) -> dict[str, Any]:
    market = split_market_symbol(symbol)
    return {
        "quote_amount": None,
        "quote_asset": quote_asset or (market[1] if market else None),
        "verified": False,
        "components": [],
    }
