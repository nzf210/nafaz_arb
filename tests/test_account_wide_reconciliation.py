"""Account-wide live exposure and resting-order reconciliation tests."""

import pytest

from executor.config import ExecutorConfig
from executor.engine import ArbitrageEngine
from executor.models import ActivePosition


class AccountStateClient:
    def __init__(self, positions=None, orders=None):
        self.positions = positions or []
        self.orders = orders or []

    async def fetch_account_positions_report(self):
        return [{"name": "okx", "status": "ok", "positions": self.positions}]

    async def fetch_account_open_orders_report(self):
        return [
            {"name": "indodax", "market": "spot", "status": "ok", "orders": self.orders},
            {"name": "okx", "market": "perpetual", "status": "ok", "orders": []},
        ]


def _engine(fake_redis, client):
    return ArbitrageEngine(
        config=ExecutorConfig(mock_mode=False),
        client=client,
        redis_client=fake_redis,
    )


@pytest.mark.asyncio
async def test_account_wide_reconciliation_accepts_exact_known_short(fake_redis):
    client = AccountStateClient(
        positions=[{
            "symbol": "BTC/USDT:USDT",
            "contracts": 5,
            "contractSize": 0.001,
            "side": "short",
        }]
    )
    engine = _engine(fake_redis, client)
    engine.active_positions["indodax:btc_idr"] = ActivePosition(
        local_exchange="indodax",
        global_exchange="okx",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000,
        short_entry_usdt=65_000,
        qty=0.005,
        spot_cost_usdt=50,
        futures_margin_usdt=50,
    )

    await engine._verify_account_wide_exchange_state()

    assert "okx" not in engine._exchange_halted


@pytest.mark.asyncio
async def test_account_wide_reconciliation_halts_unknown_short(fake_redis):
    client = AccountStateClient(
        positions=[{
            "symbol": "ETH/USDT:USDT",
            "contracts": 2,
            "contractSize": 0.01,
            "side": "short",
        }]
    )
    engine = _engine(fake_redis, client)

    await engine._verify_account_wide_exchange_state()

    assert "okx" in engine._exchange_halted
    assert "recorded=0.0" in engine._exchange_halted["okx"]


@pytest.mark.asyncio
async def test_account_wide_reconciliation_halts_unknown_resting_spot_order(fake_redis):
    client = AccountStateClient(
        orders=[{"id": "manual-order-77", "symbol": "BTC/IDR", "side": "buy"}]
    )
    engine = _engine(fake_redis, client)

    await engine._verify_account_wide_exchange_state()

    assert "indodax" in engine._exchange_halted
    assert "untracked spot open order manual-order-77" in engine._exchange_halted["indodax"]


@pytest.mark.asyncio
async def test_account_wide_reconciliation_halts_when_reader_unavailable(fake_redis):
    engine = _engine(fake_redis, object())

    await engine._verify_account_wide_exchange_state()

    assert "okx" in engine._exchange_halted
    assert "indodax" in engine._exchange_halted

@pytest.mark.asyncio
async def test_account_wide_reconciliation_uses_persisted_usdc_contract_symbol(fake_redis):
    client = AccountStateClient(
        positions=[{
            "symbol": "ETH/USDC:USDC",
            "contracts": 3,
            "contractSize": 0.01,
            "side": "short",
        }]
    )
    engine = _engine(fake_redis, client)
    engine.active_positions["tokocrypto:eth_idr"] = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="okx",
        symbol="eth_idr",
        global_symbol="ETH/USDC:USDC",
        entry_price_idr=50_000_000,
        short_entry_usdt=3_000,
        qty=0.03,
        spot_cost_usdt=50,
        futures_margin_usdt=50,
    )

    await engine._verify_account_wide_exchange_state()

    assert "okx" not in engine._exchange_halted
