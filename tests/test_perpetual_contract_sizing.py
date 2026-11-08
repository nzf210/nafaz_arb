"""Perpetual contract sizing tests: hedge quantities stay in base-asset units."""

import pytest

from executor.exchange_client import ExchangeClient


class ContractVenue:
    id = "okx"

    def __init__(self):
        self.markets = {
            "BTC/USDT:USDT": {
                "symbol": "BTC/USDT:USDT",
                "contract": True,
                "swap": True,
                "contractSize": 0.001,
            }
        }
        self.orders = []

    def market(self, symbol):
        return self.markets[symbol]

    def amount_to_precision(self, symbol, amount):
        return f"{amount:.0f}"

    async def create_market_sell_order(self, symbol, amount, *params):
        self.orders.append(("sell", symbol, amount, params))
        return {"id": "short-1", "symbol": symbol, "amount": amount, "filled": amount, "status": "closed"}

    async def create_market_buy_order(self, symbol, amount, params):
        self.orders.append(("buy", symbol, amount, params))
        return {"id": "close-1", "symbol": symbol, "amount": amount, "filled": amount, "status": "closed"}

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_perpetual_orders_convert_base_quantity_to_contracts_and_back():
    venue = ContractVenue()
    client = ExchangeClient(
        spot_clients={"indodax": object()},
        perpetual_clients={"okx": venue},
        strict_routing=True,
    )
    client.set_route("indodax", "okx")

    opened = await client.open_global_short_market("BTC/USDT:USDT", 0.01, price=65000.0)
    assert venue.orders[-1][0:3] == ("sell", "BTC/USDT:USDT", 10.0)
    assert opened["amount"] == pytest.approx(0.01)
    assert opened["filled"] == pytest.approx(0.01)
    assert opened["_nafaz_contract_filled"] == pytest.approx(10.0)

    closed = await client.close_global_short_market("BTC/USDT:USDT", 0.01)
    assert venue.orders[-1][0:3] == ("buy", "BTC/USDT:USDT", 10.0)
    assert venue.orders[-1][3]["reduceOnly"] is True
    assert closed["filled"] == pytest.approx(0.01)


@pytest.mark.asyncio
async def test_perpetual_order_refuses_unhedgeable_precision():
    venue = ContractVenue()
    venue.amount_to_precision = lambda symbol, amount: f"{int(amount)}"
    client = ExchangeClient(
        spot_clients={"indodax": object()},
        perpetual_clients={"okx": venue},
        strict_routing=True,
    )
    client.set_route("indodax", "okx")

    with pytest.raises(ValueError, match="cannot be exactly hedged"):
        await client.open_global_short_market("BTC/USDT:USDT", 0.0105)


@pytest.mark.asyncio
async def test_perpetual_order_refuses_non_contract_market():
    venue = ContractVenue()
    venue.markets["BTC/USDT:USDT"]["contract"] = False
    client = ExchangeClient(
        spot_clients={"indodax": object()},
        perpetual_clients={"okx": venue},
        strict_routing=True,
    )
    client.set_route("indodax", "okx")

    with pytest.raises(ValueError, match="not a CCXT contract market"):
        await client.open_global_short_market("BTC/USDT:USDT", 0.01)
