import asyncio
import math
import redis
from executor.config import ExecutorConfig
from executor.exchange_client import create_exchange_client


async def resolve_ai():
    cfg = ExecutorConfig.load("config.json")
    client = create_exchange_client(cfg)
    r = redis.Redis(host="nafaz-redis", port=6380)

    # 1. Tokocrypto Spot Sell
    try:
        toko = client.balance_exchanges["tokocrypto"]["client"]
        bal = await toko.fetch_balance()
        free_ai = float((bal.get("free") or {}).get("AI", 0.0) or 0.0)
        print(f"Current Tokocrypto free AI: {free_ai}")
        if free_ai >= 0.1:
            sell_qty = math.floor(free_ai * 10) / 10.0
            print(f"Placing Tokocrypto market sell for {sell_qty} AI...")
            order = await toko.create_market_sell_order("AI/USDT", sell_qty)
            order_id = order.get("id")
            filled = order.get("filled")
            print(f"Tokocrypto sell success: order_id={order_id}, filled={filled}")
    except Exception as e:
        print(f"Tokocrypto sell failed: {e}")

    # 2. OKX Futures Buy to Close
    try:
        okx = client.balance_exchanges["okx"]["client"]
        positions = await okx.fetch_positions(["AI-USDT-SWAP"])
        short_contracts = 0.0
        for p in positions:
            side = p.get("side")
            contracts = float(p.get("contracts") or 0)
            if side == "short" or contracts > 0:
                short_contracts = contracts
        print(f"Current OKX AI short contracts: {short_contracts}")
        if short_contracts > 0:
            print(f"Placing OKX market buy for {short_contracts} contracts reduceOnly...")
            order = await okx.create_market_buy_order("AI/USDT:USDT", short_contracts, params={"reduceOnly": True})
            order_id = order.get("id")
            filled = order.get("filled")
            print(f"OKX buy success: order_id={order_id}, filled={filled}")
    except Exception as e:
        print(f"OKX buy to close failed: {e}")

    # 3. Clean Redis State for this position
    r.delete("arb:active_pos:tokocrypto:toko:ai_usdt__okx:ai_usdt_swap")
    r.hdel("arb:order_pending_recovery", "cf5e057c-99ee-5fcf-99ab-4a1579f8671c")
    print("Cleaned active_pos and force-close order from Redis.")

    await client.close()


if __name__ == "__main__":
    asyncio.run(resolve_ai())
