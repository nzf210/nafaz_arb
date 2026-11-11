"""Pure pricing rules for Indodax IDR adaptive maker pegging."""

from decimal import Decimal, ROUND_DOWN
from math import isfinite


def dynamic_max_buy_price_idr(
    scanner_price_idr: float,
    estimated_net_profit_usdt: float,
    quantity: float,
    usdt_idr_rate: float,
    spot_entry_fee_pct: float,
    fee_surcharge_pct: float,
    min_net_profit_pct: float,
) -> float:
    """Return the highest IDR entry price retaining the dynamic net-profit floor.

    The scanner's estimated net profit is the baseline at scanner_price_idr.
    Any higher entry price consumes that edge plus its spot-entry fee. The
    target floor is a configurable percentage of the baseline spot notional.
    """
    values = (
        scanner_price_idr, estimated_net_profit_usdt, quantity,
        usdt_idr_rate, spot_entry_fee_pct, fee_surcharge_pct,
        min_net_profit_pct,
    )
    if any(not isfinite(float(value)) for value in values):
        raise ValueError("adaptive maker inputs must be finite")
    if (
        scanner_price_idr <= 0 or estimated_net_profit_usdt <= 0
        or quantity <= 0 or usdt_idr_rate <= 0
        or spot_entry_fee_pct < 0 or fee_surcharge_pct < 0
        or min_net_profit_pct < 0
    ):
        raise ValueError("adaptive maker inputs are outside safe bounds")

    baseline_notional_usdt = quantity * scanner_price_idr / usdt_idr_rate
    target_profit_usdt = baseline_notional_usdt * min_net_profit_pct / 100.0
    available_edge_usdt = estimated_net_profit_usdt - target_profit_usdt
    if available_edge_usdt <= 0:
        return 0.0

    entry_fee_multiplier = 1.0 + (
        spot_entry_fee_pct + fee_surcharge_pct
    ) / 100.0
    extra_entry_price_idr = (
        available_edge_usdt * usdt_idr_rate
        / (quantity * entry_fee_multiplier)
    )
    return scanner_price_idr + extra_entry_price_idr


def aggressive_maker_price_idr(
    bids, asks, tick_size: float, max_buy_price_idr: float
):
    """Peg one tick below best ask, capped by profit and never marketable."""
    try:
        tick = Decimal(str(tick_size))
        cap = Decimal(str(max_buy_price_idr))
        best_bid = Decimal(str(bids[0][0]))
        best_ask = Decimal(str(asks[0][0]))
    except (IndexError, TypeError, ValueError, ArithmeticError):
        return None
    if (
        not all(value.is_finite() and value > 0 for value in
                (tick, cap, best_bid, best_ask))
        or tick <= 0 or best_ask <= best_bid
    ):
        return None

    # Aggressive maker: improve toward the ask but leave one full tick so the
    # order cannot cross and become a taker order.
    raw_candidate = min(cap, best_ask - tick)
    candidate = (
        raw_candidate / tick
    ).to_integral_value(rounding=ROUND_DOWN) * tick
    if candidate < best_bid or candidate <= 0 or candidate >= best_ask:
        return None
    return float(candidate)
