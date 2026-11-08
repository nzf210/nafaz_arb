"""Exchange-state recovery, balance validation, and malformed payload handling."""

import asyncio
import json
import re
import time

from executor.models import ActivePosition, PositionState

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine, RetryExhausted, retry_with_backoff

from tests.conftest import BlockingSellClient, FailingHedgeClient, MockExchangeClient, OpenShortPositionClient, _entry_order_info, _fresh_signal_payload, engine, fake_redis, mock_client

@pytest.mark.asyncio
async def test_p0_pending_entry_terminal_partial_fill_is_hedged_on_recovery(engine, fake_redis, mock_client):
    """A canceled order with a final partial fill must hedge that fill before clearing state."""
    pair_tag = "indodax:btc_idr"
    order_info = {
        "order_id": "889",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1_000_000_000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "global_ask_usdt": 65_000.0,
        "usdt_idr_rate": 16_000.0,
        "next_funding_time_ms": int(time.time() * 1000) + 3_600_000,
        "base_target_exit_spread_pct": 0.3,
        "current_funding_rate": 0.0003,
        "tick_size": 1.0,
        "spot_cost_usdt": 625.0,
        "futures_margin_usdt": 625.0,
    }
    await fake_redis.set("arb:pending_entry:" + pair_tag, json.dumps(order_info))
    mock_client.orders["889"] = {
        "id": "889", "symbol": "btc_idr", "status": "canceled", "filled": 0.004
    }

    await engine._recover_pending_entry_orders()

    assert pair_tag in engine.active_positions
    assert engine.active_positions[pair_tag].qty == pytest.approx(0.004)
    assert len(mock_client.global_short_sells) == 1
    assert mock_client.global_short_sells[0]["amount"] == pytest.approx(0.004)
    assert await fake_redis.get("arb:pending_entry:" + pair_tag) is None


@pytest.mark.asyncio
async def test_p0_pending_entry_unknown_venue_state_is_retained_and_blocked(engine, fake_redis, mock_client):
    """An unresolvable venue order must remain durable and block the pair."""
    pair_tag = "indodax:btc_idr"
    order_info = {
        "order_id": "890",
        "symbol": "btc_idr",
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
    }
    await fake_redis.set("arb:pending_entry:" + pair_tag, json.dumps(order_info))

    async def fetch_unknown(order_id, symbol):
        raise TimeoutError("exchange API unavailable")

    mock_client.fetch_local_order = fetch_unknown
    await engine._recover_pending_entry_orders()

    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert pair_tag in engine.pending_entry_orders
    assert await fake_redis.get("arb:pending_entry:" + pair_tag) is not None
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "ENTRY_ORDER_STATE_UNKNOWN"


@pytest.mark.asyncio
async def test_partial_futures_hedge_fill_is_not_recorded_as_fully_hedged(fake_redis):
    """A partial futures fill blocks spot rollback and forces manual reconciliation."""
    client = OpenShortPositionClient()
    client.amount_unused = 0.002

    async def partial_hedge(symbol, amount, price=0.0, client_order_id=None):
        return {
            "id": "partial-hedge",
            "symbol": symbol,
            "status": "closed",
            "amount": amount,
            "filled": 0.002,
            "average": price,
        }

    client.open_global_short_market = partial_hedge
    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True),
        client=client,
        redis_client=fake_redis,
    )
    pair_tag = "indodax:btc_idr"
    order_info = _entry_order_info(pair_tag, int(time.time() * 1000))
    engine.pending_entry_orders[pair_tag] = order_info
    engine.states[pair_tag] = PositionState.ENTRY_PENDING

    await engine._execute_entry_hedge(
        pair_tag,
        order_info,
        unhedged_delta=0.005,
        price_idr=1_000_000_000.0,
        usdt_rate=16_000.0,
    )

    assert pair_tag not in engine.active_positions
    assert client.placed_sells == []
    assert engine.states[pair_tag] == PositionState.FAILED
    assert engine.status_snapshot()["failures"][pair_tag]["reason"] == "HEDGE_STATE_UNKNOWN"


@pytest.mark.asyncio
async def test_live_startup_recovery_verifies_futures_short_quantity(fake_redis):
    """A restored live position is active only when the exchange confirms its short size."""
    client = MockExchangeClient()
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    async def matching_short(symbol):
        return [{"symbol": symbol, "contracts": 0.005, "contractSize": 1.0, "side": "short"}]
    client.fetch_positions = matching_short
    await fake_redis.set("arb:active_pos:indodax:btc_idr", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False), client=client, redis_client=fake_redis
    )
    await engine.initialize()

    assert engine.states["indodax:btc_idr"] == PositionState.HEDGED_ACTIVE


@pytest.mark.asyncio
async def test_live_startup_recovery_blocks_position_quantity_mismatch(fake_redis):
    """A Redis snapshot must not resume automatic exits if exchange size differs."""
    client = MockExchangeClient()
    pos = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65_000.0,
        qty=0.005,
    )
    async def mismatched_short(symbol):
        return [{"symbol": symbol, "contracts": 0.003, "contractSize": 1.0, "side": "short"}]
    client.fetch_positions = mismatched_short
    await fake_redis.set("arb:active_pos:indodax:btc_idr", json.dumps(pos.to_dict()))

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False), client=client, redis_client=fake_redis
    )
    await engine.initialize()

    assert engine.states["indodax:btc_idr"] == PositionState.RECONCILIATION_REQUIRED
    restored = json.loads(await fake_redis.get("arb:active_pos:indodax:btc_idr"))
    assert restored["state"] == "RECONCILIATION_REQUIRED"
    assert engine.status_snapshot()["failures"]["indodax:btc_idr"]["reason"] == "RECOVERY_POSITION_MISMATCH"


@pytest.mark.asyncio
async def test_malformed_active_position_record_is_retained_and_force_close_blocked(fake_redis):
    """A corrupted snapshot must never be mistaken for an empty/flat position."""
    client = MockExchangeClient()
    pair_tag = "indodax:btc_idr"
    key = "arb:active_pos:" + pair_tag
    await fake_redis.set(key, "{invalid-json")

    engine = ArbitrageEngine(
        config=ExecutorConfig(mock_mode=True), client=client, redis_client=fake_redis
    )
    await engine.initialize()

    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED
    assert await fake_redis.get(key) == "{invalid-json"

    await engine.handle_force_close(json.dumps({"pair_key": pair_tag}))

    assert client.global_close_buys == []
    assert client.placed_sells == []
    assert engine.states[pair_tag] == PositionState.RECONCILIATION_REQUIRED


@pytest.mark.asyncio
async def test_fetch_positions_rejects_missing_symbol_in_exchange_payload():
    """A malformed non-empty positions response must not be mistaken for flat."""
    from executor.exchange_client import ExchangeClient

    class GlobalFutures:
        async def fetch_positions(self, symbols=None):
            return [{"contracts": 0.01, "contractSize": 1.0, "side": "short"}]

    client = ExchangeClient(local_exchange=object(), global_exchange=GlobalFutures())
    with pytest.raises(ValueError, match="missing its symbol"):
        await client.fetch_positions("BTC/USDT:USDT")


@pytest.mark.asyncio
async def test_fetch_positions_returns_empty_only_for_valid_empty_payload():
    """An actual empty exchange response remains the only valid empty result."""
    from executor.exchange_client import ExchangeClient

    class GlobalFutures:
        async def fetch_positions(self, symbols=None):
            return []

    client = ExchangeClient(local_exchange=object(), global_exchange=GlobalFutures())
    assert await client.fetch_positions("BTC/USDT:USDT") == []


@pytest.mark.asyncio
async def test_live_exchange_balance_snapshot_normalizes_idr_to_usdt():
    """Live sizing converts local IDR quote balance using the current scanner FX rate."""
    from executor.exchange_client import ExchangeClient

    class LocalSpot:
        async def fetch_balance(self):
            return {
                "free": {"IDR": 1_600_000.0},
                "used": {"IDR": 400_000.0},
                "total": {"IDR": 2_000_000.0},
            }

    class GlobalFutures:
        async def fetch_balance(self):
            return {
                "free": {"USDT": 75.0},
                "used": {"USDT": 25.0},
                "total": {"USDT": 100.0},
            }

    client = ExchangeClient(LocalSpot(), GlobalFutures())
    balances = await client.get_balances(
        local_quote_asset="IDR",
        global_quote_asset="USDT",
        usdt_idr_rate=16_000.0,
    )

    assert balances["spot_available"] == pytest.approx(100.0)
    assert balances["spot_total"] == pytest.approx(125.0)
    assert balances["futures_available"] == pytest.approx(75.0)
    assert balances["futures_total"] == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_live_exchange_balance_snapshot_requires_live_idr_fx():
    """Missing FX must fail closed instead of treating IDR as USDT."""
    from executor.exchange_client import ExchangeClient

    class LocalSpot:
        async def fetch_balance(self):
            return {"free": {"IDR": 1_600_000.0}, "total": {"IDR": 1_600_000.0}}

    class GlobalFutures:
        async def fetch_balance(self):
            return {"free": {"USDT": 100.0}, "total": {"USDT": 100.0}}

    client = ExchangeClient(LocalSpot(), GlobalFutures())
    with pytest.raises(ValueError, match="live USDT/IDR rate"):
        await client.get_balances(
            local_quote_asset="IDR",
            global_quote_asset="USDT",
            usdt_idr_rate=0.0,
        )


@pytest.mark.asyncio
async def test_fetch_positions_matches_ccxt_contract_symbol_aliases():
    """Equivalent CCXT futures symbols must not be mistaken for an empty position."""
    from executor.exchange_client import ExchangeClient

    class GlobalFutures:
        async def fetch_positions(self, symbols=None):
            return [{
                "symbol": "BTC/USDT:USDT",
                "contracts": 0.005,
                "contractSize": 1.0,
                "side": "short",
            }]

    client = ExchangeClient(local_exchange=object(), global_exchange=GlobalFutures())
    positions = await client.fetch_positions("BTCUSDT")
    assert len(positions) == 1
    assert positions[0]["side"] == "short"


@pytest.mark.asyncio
async def test_fetch_balance_report_returns_all_assets_and_isolates_venue_errors():
    from executor.exchange_client import ExchangeClient

    class LocalSpot:
        id = "indodax"

        async def fetch_balance(self):
            return {
                "free": {"IDR": 1_200_000.0, "BTC": 0.01, "XRP": 5.0},
                "used": {"IDR": 300_000.0, "BTC": 0.002},
                "total": {"IDR": 1_500_000.0, "BTC": 0.012, "XRP": 8.0},
            }

    class GlobalFutures:
        id = "okx"

        async def fetch_balance(self):
            raise RuntimeError("private API unavailable")

    reports = await ExchangeClient(LocalSpot(), GlobalFutures()).fetch_balance_report()

    assert reports[0]["name"] == "indodax"
    assert reports[0]["status"] == "ok"
    assert {item["asset"] for item in reports[0]["assets"]} == {"IDR", "BTC", "XRP"}
    btc = next(item for item in reports[0]["assets"] if item["asset"] == "BTC")
    assert btc == {"asset": "BTC", "free": 0.01, "used": 0.002, "total": 0.012}
    xrp = next(item for item in reports[0]["assets"] if item["asset"] == "XRP")
    assert xrp == {"asset": "XRP", "free": 5.0, "used": 3.0, "total": 8.0}
    assert reports[1]["name"] == "okx"
    assert reports[1]["status"] == "error"
    assert "private API unavailable" in reports[1]["error"]


@pytest.mark.asyncio
async def test_paper_balance_report_is_explicitly_simulated():
    from executor.exchange_client import PaperTradingExchangeClient

    reports = await PaperTradingExchangeClient().fetch_balance_report()

    assert len(reports) == 2
    assert all(report["status"] == "ok" for report in reports)
    assert all(report["assets"][0]["asset"] == "USDT" for report in reports)
