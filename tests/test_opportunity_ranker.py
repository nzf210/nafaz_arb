import time
import pytest
from executor.opportunity_ranker import OpportunityCandidate, OpportunityRanker


def test_expected_realized_profit_formula():
    ranker = OpportunityRanker()
    now_ms = int(time.time() * 1000)

    # Candidate with explicit P_entry, P_exit, risk, and hold hours
    candidate = OpportunityCandidate(
        opportunity_id="cand-1",
        pair_key="toko_okx:SOL",
        local_venue="tokocrypto",
        local_symbol="SOL/BIDR",
        global_venue="okx",
        global_symbol="SOL/USDT:USDT",
        local_bid=150.0,
        local_ask=150.1,
        global_bid=152.0,
        global_ask=152.1,
        gross_spread_pct=1.3,
        expected_net_profit_usdt=1.00,
        expected_net_roi_pct=1.00,
        allocated_capital_usdt=100.0,
        quote_age_ms=0.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
        p_entry=0.90,
        p_exit=0.90,
        estimated_slippage_and_fee_risk_usdt=0.50,
        estimated_hold_hours=0.50,
    )

    score = ranker.compute_score(candidate)

    # Expected Realized = (0.90 * 0.90 * 1.00) - ((1 - 0.81) * 0.50)
    #                   = 0.81 - (0.19 * 0.50) = 0.81 - 0.095 = 0.715
    # CapitalHourScore = 0.715 / (100.0 * 0.50) = 0.715 / 50.0 = 0.0143
    expected_realized = 0.715
    expected_score = round(expected_realized / (100.0 * 0.50), 6)

    assert abs(candidate.metadata["expected_realized_profit_usdt"] - expected_realized) < 1e-5
    assert abs(score - expected_score) < 1e-5


def test_estimated_hold_hours_from_velocity_and_mean_reversion():
    ranker = OpportunityRanker()
    now_ms = int(time.time() * 1000)

    # Allocated capital = $60
    # Trade velocity = $60/min -> fill hours = 60 / (60 * 60) = 1/60 hr
    # Reversion frequency = 2.0 / hr -> reversion hours = 1 / 2.0 = 0.5 hr
    # Total hold hours = 0.5 + 1/60 = 0.516667 hr
    candidate = OpportunityCandidate(
        opportunity_id="cand-vel",
        pair_key="toko_okx:ETH",
        local_venue="tokocrypto",
        local_symbol="ETH/BIDR",
        global_venue="okx",
        global_symbol="ETH/USDT:USDT",
        local_bid=3000.0,
        local_ask=3001.0,
        global_bid=3020.0,
        global_ask=3021.0,
        gross_spread_pct=0.6,
        expected_net_profit_usdt=2.00,
        expected_net_roi_pct=0.50,
        allocated_capital_usdt=60.0,
        quote_age_ms=0.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
        trade_velocity_usd_per_min=60.0,
        mean_reversion_frequency_per_hour=2.0,
    )

    ranker.compute_score(candidate)
    hold_hours = candidate.metadata["estimated_hold_hours"]
    expected_hold = 0.5 + (1.0 / 60.0)
    assert abs(hold_hours - expected_hold) < 1e-4


def test_capital_hour_ranking_fast_coin_beats_slow_high_profit_coin():
    """A fast-turning coin with lower net profit beats a slow, stagnant coin with high net profit."""
    ranker = OpportunityRanker()
    now_ms = int(time.time() * 1000)

    # Coin Fast: expected net profit $0.60, $50 capital, hold hours 0.25h (15 min)
    # Expected Realized ~ 0.50 USDT -> Score ~ 0.50 / (50 * 0.25) = 0.040
    c_fast = OpportunityCandidate(
        opportunity_id="fast-coin",
        pair_key="toko_okx:FAST",
        local_venue="tokocrypto",
        local_symbol="FAST/BIDR",
        global_venue="okx",
        global_symbol="FAST/USDT:USDT",
        local_bid=10.0,
        local_ask=10.05,
        global_bid=10.15,
        global_ask=10.20,
        gross_spread_pct=1.0,
        expected_net_profit_usdt=0.60,
        expected_net_roi_pct=1.20,
        allocated_capital_usdt=50.0,
        quote_age_ms=50.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
        p_entry=0.95,
        p_exit=0.95,
        estimated_slippage_and_fee_risk_usdt=0.20,
        estimated_hold_hours=0.25,
    )

    # Coin Slow: expected net profit $1.50 (2.5x higher!), $100 capital, hold hours 8.0h
    # Expected Realized ~ 1.0 USDT -> Score ~ 1.0 / (100 * 8.0) = 0.00125
    c_slow = OpportunityCandidate(
        opportunity_id="slow-coin",
        pair_key="toko_okx:SLOW",
        local_venue="tokocrypto",
        local_symbol="SLOW/BIDR",
        global_venue="okx",
        global_symbol="SLOW/USDT:USDT",
        local_bid=100.0,
        local_ask=100.5,
        global_bid=102.0,
        global_ask=102.5,
        gross_spread_pct=1.5,
        expected_net_profit_usdt=1.50,
        expected_net_roi_pct=1.50,
        allocated_capital_usdt=100.0,
        quote_age_ms=50.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
        p_entry=0.80,
        p_exit=0.80,
        estimated_slippage_and_fee_risk_usdt=0.60,
        estimated_hold_hours=8.0,
    )

    ranked = ranker.rank_candidates([c_slow, c_fast])
    assert len(ranked) == 2
    assert ranked[0].opportunity_id == "fast-coin"
    assert ranked[1].opportunity_id == "slow-coin"


def test_exitability_penalty_from_metadata():
    """Exitability score penalty in metadata (P_exit) lowers score significantly."""
    ranker = OpportunityRanker()
    now_ms = int(time.time() * 1000)

    # Good exitability candidate
    c_good_exit = OpportunityCandidate(
        opportunity_id="good-exit",
        pair_key="toko_okx:GOOD",
        local_venue="tokocrypto",
        local_symbol="GOOD/BIDR",
        global_venue="okx",
        global_symbol="GOOD/USDT:USDT",
        local_bid=10.0,
        local_ask=10.05,
        global_bid=10.15,
        global_ask=10.20,
        gross_spread_pct=1.0,
        expected_net_profit_usdt=0.80,
        expected_net_roi_pct=1.60,
        allocated_capital_usdt=50.0,
        quote_age_ms=50.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
        metadata={"exitability_score": 0.95},
        estimated_hold_hours=1.0,
    )

    # Bad exitability candidate (e.g. internal spread > 1.5% -> exitability_score = 0.10)
    c_bad_exit = OpportunityCandidate(
        opportunity_id="bad-exit",
        pair_key="toko_okx:BAD",
        local_venue="tokocrypto",
        local_symbol="BAD/BIDR",
        global_venue="okx",
        global_symbol="BAD/USDT:USDT",
        local_bid=10.0,
        local_ask=10.30,
        global_bid=10.40,
        global_ask=10.45,
        gross_spread_pct=1.5,
        expected_net_profit_usdt=0.80,
        expected_net_roi_pct=1.60,
        allocated_capital_usdt=50.0,
        quote_age_ms=50.0,
        timestamp_ms=now_ms,
        expiry_ms=now_ms + 5000,
        metadata={"exitability_score": 0.10},
        estimated_hold_hours=1.0,
    )

    score_good = ranker.compute_score(c_good_exit)
    score_bad = ranker.compute_score(c_bad_exit)

    assert score_good > score_bad
    # Bad exit candidate score should be zero or heavily reduced due to risk penalty
    assert score_bad <= 0.001
