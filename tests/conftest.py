"""Shared fixtures, helper payloads and mock exchange clients.

Split out of the former monolithic tests/test_executor.py so the suite is
grouped by engine state instead of living in one 2750-line module.
"""

import asyncio
import json
import time
from unittest.mock import AsyncMock, MagicMock

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine, build_client_order_id, clean_pair_key, round_to_tick
from executor.exchange_client import ExchangeClient
from executor.models import (
    ActivePosition,
    ExecutionDirectives,
    ExecutorPayload,
    FundingMetrics,
    PairInfo,
    PositionState,
    PricingSnapshot,
)

class MockExchangeClient:
    """Mock Exchange Client implementing required methods."""

    def __init__(self):
        self.orders = {}
        self.order_counter = 100
        self.placed_buys = []
        self.placed_sells = []
        self.cancelled_orders = []
        self.global_short_sells = []
        self.global_close_buys = []
        self.market_prices = {
            "btc_idr": {
                "bid": 1_038_000_000.0,
                "ask": 1_040_000_000.0,
                "timestamp": int(time.time() * 1000),
            },
            "btc_usdt": {
                "bid": 65_000.0,
                "ask": 65_010.0,
                "timestamp": int(time.time() * 1000),
            },
        }
        self.client_order_ids = []
        self.funding_history = []
        self.funding_rate_data = {}

    async def fetch_funding_history(self, symbol: str, since=None, limit=100):
        return [
            payment for payment in self.funding_history
            if int(payment.get("timestamp", 0)) >= int(since or 0)
        ][:limit]

    async def fetch_funding_rate(self, symbol: str):
        return dict(self.funding_rate_data)

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id=None):
        self.order_counter += 1
        order_id = str(self.order_counter)
        order = {
            "id": order_id,
            "symbol": symbol,
            "side": "buy",
            "type": "limit",
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "status": "open",
        }
        order["client_order_id"] = client_order_id
        self.client_order_ids.append(client_order_id)
        self.orders[order_id] = order
        self.placed_buys.append(order)
        return order

    async def place_local_limit_sell(self, symbol: str, amount: float, price: float, client_order_id=None):
        self.order_counter += 1
        order_id = str(self.order_counter)
        order = {
            "id": order_id,
            "symbol": symbol,
            "side": "sell",
            "type": "limit",
            "amount": amount,
            "price": price,
            "filled": 0.0,
            "status": "open",
        }
        self.orders[order_id] = order
        self.placed_sells.append(order)
        return order

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id=None):
        self.order_counter += 1
        order_id = str(self.order_counter)
        order = {
            "id": order_id,
            "symbol": symbol,
            "side": "sell",
            "type": "market",
            "amount": amount,
            "price": 1000.0,
            "average": 1000.0,
            "filled": amount,
            "status": "closed",
        }
        order["client_order_id"] = client_order_id
        self.client_order_ids.append(client_order_id)
        self.orders[order_id] = order
        self.placed_sells.append(order)
        return order

    async def cancel_local_order(self, order_id: str, symbol: str):
        self.cancelled_orders.append((order_id, symbol))
        if order_id in self.orders:
            self.orders[order_id]["status"] = "canceled"
        return {"id": order_id, "status": "canceled"}

    async def fetch_local_order(self, order_id: str, symbol: str):
        if order_id in self.orders:
            return self.orders[order_id]
        return {"id": order_id, "status": "open", "filled": 0.0}

    async def fetch_positions(self, symbol: str):
        # Default mock: no futures positions open anywhere.
        return []

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        record = {
            "symbol": symbol,
            "side": "sell",
            "amount": amount,
            "filled": amount,
            "status": "closed",
            "price": price,
            "average": price,
            "client_order_id": client_order_id,
        }
        self.global_short_sells.append(record)
        return record

    async def close_global_short_market(self, symbol: str, amount: float, client_order_id=None):
        record = {
            "symbol": symbol,
            "side": "buy",
            "amount": amount,
            "filled": amount,
            "status": "closed",
            "price": 65050.0,
            "average": 65050.0,
            "params": {"reduceOnly": True},
            "client_order_id": client_order_id,
        }
        self.global_close_buys.append(record)
        return record

    def update_market_price(self, symbol: str, best_bid: float, best_ask: float, last_price: float = 0.0):
        clean = symbol.lower().replace("/", "_").replace("-", "_")
        self.market_prices[clean] = {
            "bid": float(best_bid),
            "ask": float(best_ask),
            "timestamp": int(time.time() * 1000),
        }

    async def fetch_local_ticker(self, symbol: str):
        clean = symbol.lower().replace("/", "_").replace("-", "_")
        price = self.market_prices.get(clean) or {}
        return {
            "symbol": symbol,
            "bid": float(price.get("bid", 0.0)),
            "ask": float(price.get("ask", 0.0)),
            "timestamp": int(price.get("timestamp", 0)),
        }

    async def fetch_global_ticker(self, symbol: str):
        clean = symbol.lower().replace("/", "_").replace("-", "_")
        price = self.market_prices.get(clean) or {}
        return {
            "symbol": symbol,
            "bid": float(price.get("bid", 0.0)),
            "ask": float(price.get("ask", 0.0)),
            "timestamp": int(price.get("timestamp", 0)),
        }

    async def close(self):
        pass

@pytest.fixture
def fake_redis():
    return fake_aioredis.FakeRedis(decode_responses=True)

@pytest.fixture
def mock_client():
    return MockExchangeClient()

@pytest.fixture
def engine(fake_redis, mock_client):
    cfg = ExecutorConfig(
        max_stale_latency_ms=500,
        min_partial_fill_usd=10.0,
        exit_price_move_threshold=0.0015,
    )
    return ArbitrageEngine(config=cfg, client=mock_client, redis_client=fake_redis)

def _entry_order_info(pair_tag: str, now_ms: int) -> dict:
    return {
        "order_id": "301",
        "symbol": "btc_idr",
        "global_symbol": "BTCUSDT",
        "local_exchange": "indodax",
        "global_exchange": "binance",
        "price": 1000000000.0,
        "qty": 0.01,
        "filled": 0.0,
        "hedged_short_qty": 0.0,
        "cancel_threshold_usdt": 60000.0,
        "tick_size": 1.0,
        "usdt_idr_rate": 16000.0,
        "global_ask_usdt": 65000.0,
        "next_funding_time_ms": now_ms + 3600000,
        "base_target_exit_spread_pct": 0.3,
        "current_funding_rate": 0.0003,
        "spot_cost_usdt": 50.0,
        "futures_margin_usdt": 50.0,
    }

def _fresh_signal_payload(now_ms: int) -> dict:
    return {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "binance",
            "local_symbol": "btc_idr",
            "global_symbol": "BTCUSDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1.0,
            "local_min_qty": 0.00001,
        },
        "pricing_snapshot": {
            "local_best_bid": 999_000_000.0,
            "local_best_ask": 1_000_000_000.0,
            "global_best_bid_usdt": 64_990.0,
            "global_best_ask_usdt": 65_000.0,
            "usdt_idr_rate": 16_000.0,
            "timestamp_ms": now_ms,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3_600_000,
            "minutes_to_next_funding": 60,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": 1_000_000_000.0,
            "entry_spread_pct": 0.25,
            "min_entry_spread_pct": 0.10,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 100.0,
            "cancel_if_global_bid_drops_below_usdt": 60_000.0,
        },
    }

def _half_budget_signal(local_symbol: str, global_symbol: str, now_ms: int) -> dict:
    sig = _fresh_signal_payload(now_ms)
    sig["pair_info"] = dict(sig["pair_info"])
    sig["pair_info"]["local_symbol"] = local_symbol
    sig["pair_info"]["global_symbol"] = global_symbol
    sig["execution_directives"] = dict(sig["execution_directives"])
    sig["execution_directives"]["max_position_usdt"] = 50.0
    return sig

def _exposure_engine(fake_redis, cap: float):
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(
        local_name="indodax", global_name="okx", initial_balance_usdt=100.0
    )
    cfg = ExecutorConfig(
        min_partial_fill_usd=10.0,
        initial_exchange_balance_usdt=100.0,
        min_allocated_capital=10.0,
        max_exposure_per_exchange_usdt=cap,
        estimated_round_trip_fee_pct=0.0,
    )
    return ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis), client

def _revalidation_payload(now_ms: int, *, min_spread_pct: float = 0.10) -> dict:
    base_bid_usdt = 65000.0
    return {
        "pair_info": {
            "local_exchange": "indodax",
            "global_exchange": "okx",
            "local_symbol": "btc_idr",
            "global_symbol": "BTC_USDT",
            "base_asset": "BTC",
            "quote_asset": "IDR",
            "local_price_tick": 1000.0,
        },
        "pricing_snapshot": {
            "local_best_bid": 1038000000.0,
            "local_best_ask": 1040000000.0,
            "global_best_bid_usdt": base_bid_usdt,
            "global_best_ask_usdt": 65010.0,
            "usdt_idr_rate": 16000.0,
            "timestamp_ms": now_ms - 50,
        },
        "funding_metrics": {
            "current_funding_rate": 0.0003,
            "next_funding_time_ms": now_ms + 3600000,
            "minutes_to_next_funding": 60.0,
            "is_sniping_window": False,
        },
        "execution_directives": {
            "action": "PLACE_MAKER_BUY",
            "recommended_limit_buy_idr": (base_bid_usdt - 65.0) * 16000.0,  # 0.1% below bid
            "entry_spread_pct": 0.10,
            "min_entry_spread_pct": min_spread_pct,
            "base_target_exit_spread_pct": 0.30,
            "max_position_usdt": 500.0,
            "cancel_if_global_bid_drops_below_usdt": 64800.0,
            "scanner_validated": True,
        },
    }

def _fee_buffer_engine(fake_redis, fee_pct: float):
    from executor.exchange_client import PaperTradingExchangeClient

    client = PaperTradingExchangeClient(
        local_name="indodax", global_name="okx", initial_balance_usdt=100.0
    )
    cfg = ExecutorConfig(
        min_partial_fill_usd=10.0,
        initial_exchange_balance_usdt=100.0,
        min_allocated_capital=10.0,
        estimated_round_trip_fee_pct=fee_pct,
    )
    return ArbitrageEngine(config=cfg, client=client, redis_client=fake_redis), client

async def fake_redis_set(engine, key, now_ms):
    await engine.redis.set(key, json.dumps(_fresh_signal_payload(now_ms)))

class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

class SlowLimitBuyClient(MockExchangeClient):
    """Counts placed entry orders and yields control mid-placement.

    The yield is what makes the race reproducible: without a per-pair lock the
    second signal reads the empty pending list while the first is still inside
    place_local_limit_buy().
    """

    def __init__(self):
        super().__init__()
        self.entry_orders = 0

    async def place_local_limit_buy(self, symbol: str, amount: float, price: float, client_order_id=None):
        self.entry_orders += 1
        await asyncio.sleep(0.05)
        return await super().place_local_limit_buy(symbol, amount, price)

class FailingHedgeClient(MockExchangeClient):
    """Client whose futures short always fails, to exercise the hedge rollback path."""

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        raise RuntimeError("global futures unavailable")

class BlockingSellClient(MockExchangeClient):
    """Client whose hedge AND emergency spot sell always fail, simulating total liquidity loss."""

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        raise RuntimeError("global futures unavailable")

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id=None):
        raise RuntimeError("local spot exchange unavailable")

class SpotSellFailureClient(MockExchangeClient):
    """Client whose futures work but whose spot market sell always fails."""

    async def place_local_market_sell(self, symbol: str, amount: float, client_order_id=None):
        raise RuntimeError("local spot exchange unavailable")

class OpenShortPositionClient(MockExchangeClient):
    """Client whose futures short is already open despite raising on the hedge call."""

    async def open_global_short_market(self, symbol: str, amount: float, price: float = 0.0, client_order_id=None):
        raise RuntimeError("timeout after venue accepted the order")

    async def fetch_positions(self, symbol: str):
        return [{"symbol": symbol, "contracts": amount_unused, "side": "short"}]

    amount_unused = 0.0

class BalancesMockClient(MockExchangeClient):

    def __init__(self, spot_available=1000.0, futures_available=1000.0):
        super().__init__()
        self.spot_available = spot_available
        self.futures_available = futures_available
        self.fail_next_buy = False

    def get_balances(self):
        return {
            "spot_available": self.spot_available,
            "futures_available": self.futures_available,
        }

    def lock_funds(self, spot, futures):
        return True

    async def place_local_limit_buy(self, symbol, amount, price, client_order_id=None):
        if self.fail_next_buy:
            raise RuntimeError("exchange down")
        return await super().place_local_limit_buy(symbol, amount, price, client_order_id)
