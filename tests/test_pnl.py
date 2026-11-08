"""Balance constraint enforcement and realized-trade accounting."""

import asyncio
import json
import time

from executor.exchange_client import PaperTradingExchangeClient

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine

from tests.conftest import engine, fake_redis

@pytest.mark.asyncio
async def test_balance_constraint_and_realized_trade_updates(fake_redis):
    """
    Test $100 Balance per Exchange Constraint:
    1. Initial balance: Spot $100, Futures $100.
    2. Opens position 1 ($50): Locks $50 on Spot and Futures. Available = $50.
    3. Opens position 2 ($50): Locks $50 on Spot and Futures. Available = $0.
    4. Position 3 signal arrives: REJECTED due to $0 available balance!
    5. Position 1 exits with profit: Locked funds released, balances credited with PnL,
       and Redis arb:balances is updated with new equity.
    """
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(
        local_name="indodax",
        global_name="okx",
        initial_balance_usdt=100.0,
    )
    cfg = ExecutorConfig(
        max_stale_latency_ms=1000,
        min_partial_fill_usd=10.0,
        initial_exchange_balance_usdt=100.0,
        allocation_per_pair=0.5,  # 50% max per pair
        min_allocated_capital=10.0,
        # This test covers the capital-constraint flow, not fee handling: keep the
        # requested $50 budget intact so the locked-funds numbers stay exact.
        estimated_round_trip_fee_pct=0.0,
    )
    engine = ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis)

    now_ms = int(time.time() * 1000)

    # Seed fresh paper quotes because entry now revalidates both legs before Spot submit.
    client.update_market_price("btc_idr", 1_000_000_000.0, 1_001_000_000.0)
    client.update_market_price("BTCUSDT", 65_000.0, 65_010.0)
    client.update_market_price("eth_idr", 49_900_000.0, 50_010_000.0)
    client.update_market_price("ETHUSDT", 3_200.0, 3_210.0)
    client.update_market_price("sol_idr", 20_000_000.0, 20_100_000.0)
    client.update_market_price("SOLUSDT", 1_300.0, 1_301.0)

    # 1. Check initial balances
    bals = client.get_balances()
    assert bals["spot_total"] == 100.0
    assert bals["spot_available"] == 100.0
    assert bals["futures_total"] == 100.0
    assert bals["futures_available"] == 100.0

    # 2. First Signal for btc_idr ($50 position)
    sig1 = {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "okx",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1000.0,
        },
        "pricing_snapshot": {
            "local_best_bid": 1000000000.0,
            "local_best_ask": 1001000000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3600000,
            "minutes_to_next_funding": 60.0,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1000001000.0,
            "entry_spread_pct": 0.35,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 50.0,
            "cancel_if_global_bid_drops_below_usdt": 64800.0,
        },
    }
    await fake_redis.set("arb:signal:indodax:btc_idr", json.dumps(sig1))
    await engine.handle_signal_notification("indodax:btc_idr")

    # Order 1 placed -> Funds locked
    bals1 = client.get_balances()
    assert bals1["spot_locked"] == 50.0
    assert bals1["futures_locked"] == 50.0
    assert bals1["spot_available"] == 50.0
    assert bals1["futures_available"] == 50.0

    # 3. Second Signal for eth_idr ($50 position)
    sig2 = dict(sig1)
    sig2["pair_info"] = dict(sig1["pair_info"])
    sig2["pair_info"]["local_symbol"] = "eth_idr"
    sig2["pair_info"]["global_symbol"] = "ETHUSDT"
    sig2["execution_directives"] = dict(sig1["execution_directives"])
    sig2["execution_directives"]["recommended_limit_buy_idr"] = 50000000.0
    sig2["execution_directives"]["max_position_usdt"] = 50.0

    await fake_redis.set("arb:signal:indodax:eth_idr", json.dumps(sig2))
    await engine.handle_signal_notification("indodax:eth_idr")

    bals2 = client.get_balances()
    assert bals2["spot_locked"] == 100.0
    assert bals2["futures_locked"] == 100.0
    assert bals2["spot_available"] == 0.0
    assert bals2["futures_available"] == 0.0

    # 4. Third Signal for sol_idr ($50 position) -> MUST BE REJECTED!
    sig3 = dict(sig1)
    sig3["pair_info"] = dict(sig1["pair_info"])
    sig3["pair_info"]["local_symbol"] = "sol_idr"
    sig3["pair_info"]["global_symbol"] = "SOLUSDT"

    await fake_redis.set("arb:signal:indodax:sol_idr", json.dumps(sig3))
    await engine.handle_signal_notification("indodax:sol_idr")

    # Verify 3rd order was NOT placed
    assert "indodax:sol_idr" not in engine.pending_entry_orders
    assert "indodax:sol_idr" not in engine.states

    # 5. Simulate fill and close on position 1 (btc_idr) with positive realized PnL
    # Put live market price in Redis touching the Limit Buy price
    await fake_redis.set(
        "arb:live_price:indodax:btc_idr",
        json.dumps({
            "local_best_bid": 999000000.0,
            "local_best_ask": 1000001000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        }),
    )
    await engine._check_entry_orders()
    assert "indodax:btc_idr" in engine.active_positions

    # Trigger Limit Sell placement & calculate exit price
    await engine._check_dynamic_exits()
    assert "indodax:btc_idr" in engine.pending_exit_orders

    exit_price = engine.pending_exit_orders["indodax:btc_idr"]["price"]

    # Market bid in Redis rises to exit_price -> Fills Limit Sell!
    await fake_redis.set(
        "arb:live_price:indodax:btc_idr",
        json.dumps({
            "local_best_bid": exit_price,
            "local_best_ask": exit_price + 1000.0,
            "global_best_bid_usdt": 65000.0,
            "global_best_ask_usdt": 65000.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms,
        }),
    )
    await engine._check_exit_orders()

    # Position 1 is now closed
    assert "indodax:btc_idr" not in engine.active_positions

    # 6. Check updated balances after trade completion:
    # Locked funds for btc_idr (50 USDT) were released, spot & futures received realized gains
    final_bals = client.get_balances()
    assert final_bals["spot_locked"] == 50.0  # Only eth_idr still locked
    assert final_bals["spot_available"] > 50.0  # Spot gained profit!
    assert final_bals["futures_available"] > 40.0  # Futures unlocked 50 USDT minus basis delta
    assert final_bals["total_equity"] > 200.0  # Overall portfolio equity grew!

    # Verify Redis arb:balances is present
    redis_bals_raw = await fake_redis.get("arb:balances")
    assert redis_bals_raw is not None
    redis_bals = json.loads(redis_bals_raw)
    assert redis_bals["total_equity"] > 200.0

def test_executor_fee_profiles_override_root_defaults_and_split_both_legs():
    cfg = ExecutorConfig(
        spot_maker_fee_pct=0.10,
        spot_taker_fee_pct=0.20,
        perp_taker_fee_pct=0.05,
        fee_asset_surcharge_pct=0.01,
        fee_profiles={
            "tokocrypto": {"spot_maker_fee_pct": 0.15},
            "binance": {"perp_taker_fee_pct": 0.04},
        },
    )

    fees = cfg.estimate_trade_fees_usdt(
        "tokocrypto", "binance",
        spot_entry_notional_usdt=100.0,
        spot_exit_notional_usdt=110.0,
        perp_entry_notional_usdt=100.0,
        perp_exit_notional_usdt=120.0,
    )

    assert fees["spot_fees_usdt"] == pytest.approx(0.391)
    assert fees["futures_fees_usdt"] == pytest.approx(0.110)
    assert fees["fees_usdt"] == pytest.approx(0.501)
    assert cfg.estimate_round_trip_fee_fraction("tokocrypto", "binance") == pytest.approx(0.0047)


def test_executor_config_loads_scanner_fee_profiles(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "spot_maker_fee_pct": 0.10,
        "spot_taker_fee_pct": 0.30,
        "perp_taker_fee_pct": 0.06,
        "fee_asset_surcharge_pct": 0.02,
        "fee_profiles": {
            "indodax": {"spot_taker_fee_pct": 0.25},
            "okx": {"perp_taker_fee_pct": 0.05},
        },
    }))

    cfg = ExecutorConfig()
    cfg._apply_file(str(config_path))

    assert cfg.spot_maker_fee_pct == pytest.approx(0.10)
    assert cfg.trade_fee_rates("indodax", "okx")["spot_taker"] == pytest.approx(0.0025)
    assert cfg.trade_fee_rates("indodax", "okx")["perp_taker"] == pytest.approx(0.0005)
