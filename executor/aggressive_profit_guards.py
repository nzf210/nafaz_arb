from decimal import Decimal, ROUND_DOWN
from typing import Optional, Tuple

DEFAULT_MAKER_REFRESH_AFTER_SECONDS = 6.0
DEFAULT_MAKER_REFRESH_COOLDOWN_SECONDS = 12.0
DEFAULT_MAX_PRE_ENTRY_SLIPPAGE_PCT = Decimal('0.003')
DEFAULT_MAX_POST_FILL_HEDGE_SLIPPAGE_PCT = Decimal('0.012')
DEFAULT_FUNDING_RETENTION_RATIO = Decimal('0.65')
DEFAULT_HIGH_YIELD_FUNDING_THRESHOLD = Decimal('0.0003')

def calculate_dynamic_exit_quote(
    global_ask_usdt: Decimal,
    usdt_idr_bid_rate: Decimal,
    base_target_exit_spread_pct: Decimal,
    accumulated_funding_usdt: Decimal,
    position_value_usdt: Decimal,
    current_funding_rate: Decimal = Decimal('0'),
    estimated_round_trip_fee_pct: Decimal = Decimal('0.10'),
    funding_retention_ratio: Decimal = DEFAULT_FUNDING_RETENTION_RATIO,
    high_yield_funding_threshold: Decimal = DEFAULT_HIGH_YIELD_FUNDING_THRESHOLD,
) -> Tuple[Decimal, Decimal]:
    if position_value_usdt <= Decimal('0'):
        raise ValueError('position_value_usdt must be > 0')
    if current_funding_rate >= high_yield_funding_threshold:
        decay_subsidy_pct = Decimal('0')
    else:
        usable_funding = max(Decimal('0'), accumulated_funding_usdt) * (Decimal('1') - funding_retention_ratio)
        decay_subsidy_pct = (usable_funding / position_value_usdt) * Decimal('100')
    min_exit_spread_floor_pct = -decay_subsidy_pct + estimated_round_trip_fee_pct
    raw_exit_spread_pct = base_target_exit_spread_pct - decay_subsidy_pct
    effective_exit_spread_pct = max(raw_exit_spread_pct, min_exit_spread_floor_pct)
    limit_sell_idr = (global_ask_usdt * usdt_idr_bid_rate) * (Decimal('1') + (effective_exit_spread_pct / Decimal('100')))
    return effective_exit_spread_pct, limit_sell_idr

def compute_hedgeable_contract_qty(
    cumulative_spot_filled_base: Decimal,
    already_hedged_base: Decimal,
    base_per_unit: Decimal,
    step_unit: Decimal,
    min_unit: Decimal,
) -> Tuple[Decimal, Decimal]:
    unhedged_base = cumulative_spot_filled_base - already_hedged_base
    if unhedged_base <= Decimal('0') or base_per_unit <= Decimal('0'):
        return Decimal('0'), Decimal('0')
    raw_units = unhedged_base / base_per_unit
    rounded_units = (raw_units / step_unit).quantize(Decimal('1'), rounding=ROUND_DOWN) * step_unit
    if rounded_units < min_unit:
        return Decimal('0'), Decimal('0')
    return rounded_units, rounded_units * base_per_unit

def should_execute_post_fill_hedge(
    expected_short_usdt: Decimal,
    live_futures_bid_usdt: Optional[Decimal],
    max_post_fill_slippage_pct: Decimal = DEFAULT_MAX_POST_FILL_HEDGE_SLIPPAGE_PCT,
) -> Tuple[bool, str]:
    if live_futures_bid_usdt is None or live_futures_bid_usdt <= Decimal('0'):
        return True, 'FAIL_OPEN_TICKER_UNREADABLE'
    if expected_short_usdt <= Decimal('0'):
        return True, 'INVALID_EXPECTED_SHORT_PRICE_FAIL_OPEN'
    slippage_frac = (expected_short_usdt - live_futures_bid_usdt) / expected_short_usdt
    if slippage_frac > max_post_fill_slippage_pct:
        return False, f'EXTREME_POST_FILL_SLIPPAGE_{slippage_frac:.4%}'
    return True, 'OK'
