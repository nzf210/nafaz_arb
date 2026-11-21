"""One-off operator unwind for the legacy AI spot/OKX short position.

This utility is de-risk-only: it never opens a spot or futures position. It is
not part of the executor's durable entry-submit ledger. Redis lifecycle state
is retained unless both venue legs are independently verified flat.
"""
import asyncio
import math
import redis

from executor.config import ExecutorConfig
from executor.exchange_client import create_exchange_client
from executor.exchange_common import futures_position_side_params


def _asset_total(balance, asset):
    """Return free + used asset total, preferring the venue's explicit total."""
    total_map = balance.get("total") if isinstance(balance, dict) else None
    total_value = total_map.get(asset) if isinstance(total_map, dict) else None
    if total_value is not None:
        total = float(total_value)
    else:
        free_map = balance.get("free") if isinstance(balance, dict) else None
        used_map = balance.get("used") if isinstance(balance, dict) else None
        total = float((free_map or {}).get(asset, 0.0) or 0.0) + float(
            (used_map or {}).get(asset, 0.0) or 0.0
        )
    if not math.isfinite(total) or total < 0:
        raise ValueError(f"invalid {asset} total balance: {total}")
    return total


def _position_contracts(positions):
    """Return (long, short) quantities; unknown side is never guessed as short."""
    if not isinstance(positions, (list, tuple)):
        raise ValueError("OKX returned a malformed positions payload")
    long_qty = 0.0
    short_qty = 0.0
    for position in positions:
        if not isinstance(position, dict):
            raise ValueError("OKX returned a malformed position")
        info = position.get("info") if isinstance(position.get("info"), dict) else {}
        side = str(position.get("side") or info.get("posSide") or "").strip().lower()
        contracts = float(position.get("contracts") or 0.0)
        if not math.isfinite(contracts) or contracts < 0:
            raise ValueError(f"invalid OKX position contracts: {contracts}")
        if side == "short":
            short_qty += contracts
        elif side == "long":
            long_qty += contracts
        elif contracts > 1e-12:
            raise ValueError("OKX position side is missing; refusing to infer short exposure")
    return long_qty, short_qty


def _order_fully_filled(order, requested):
    """Only a terminal, quantitatively complete response confirms an unwind leg."""
    if not isinstance(order, dict):
        return False
    if str(order.get("status") or "").strip().lower() not in ("closed", "filled"):
        return False
    try:
        filled = float(order.get("filled") or 0.0)
        requested = float(requested)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(filled) or not math.isfinite(requested) or requested < 0:
        return False
    return filled + max(1e-12, requested * 1e-8) >= requested


async def resolve_ai():
    cfg = ExecutorConfig.load("config.json")
    client = create_exchange_client(cfg)
    redis_client = redis.Redis(host="nafaz-redis", port=6380)
    spot_flat = False
    futures_flat = False

    try:
        # 1. Tokocrypto spot SELL. A locked balance or partial/ambiguous fill
        # keeps the local position record for manual reconciliation.
        try:
            spot_entry = (
                client.balance_exchanges.get("tokocrypto:spot")
                or client.balance_exchanges.get("tokocrypto")
            )
            if not spot_entry or spot_entry.get("market") != "spot" or not spot_entry.get("client"):
                raise RuntimeError("Tokocrypto spot client is not configured")
            toko = spot_entry["client"]
            balance = await toko.fetch_balance()
            free_ai = float((balance.get("free") or {}).get("AI", 0.0) or 0.0)
            total_ai = _asset_total(balance, "AI")
            print(f"Current Tokocrypto AI balance: free={free_ai}, total={total_ai}")
            if total_ai <= 0.1:
                spot_flat = True
            elif free_ai > 0:
                sell_qty = math.floor(free_ai * 10) / 10.0
                if sell_qty <= 0:
                    raise RuntimeError("Tokocrypto AI balance is below sell precision")
                print(f"Placing Tokocrypto market SELL for {sell_qty} AI...")
                order = await toko.create_market_sell_order(
                    "AI/USDT",
                    sell_qty,
                    {"clientOrderId": "resolveaispotv1"},
                )
                if not _order_fully_filled(order, sell_qty):
                    order_id = str((order or {}).get("id") or "")
                    if order_id:
                        order = await toko.fetch_order(order_id, "AI/USDT")
                if not _order_fully_filled(order, sell_qty):
                    raise RuntimeError(
                        f"Tokocrypto SELL is not confirmed fully filled: {order}"
                    )
                after = await toko.fetch_balance()
                remaining_ai = _asset_total(after, "AI")
                spot_flat = remaining_ai <= 0.1
                print(
                    f"Tokocrypto SELL verified: order_id={order.get('id')}, "
                    f"filled={order.get('filled')}, remaining_total={remaining_ai}"
                )
            else:
                raise RuntimeError(
                    f"Tokocrypto AI is not freely sellable (free={free_ai}, total={total_ai})"
                )
        except Exception as exc:
            print(f"Tokocrypto spot unwind unresolved; state retained: {exc}")

        # 2. OKX short close. Long positions are never treated as shorts. The
        # venue's own position mode determines whether posSide=short is needed.
        try:
            futures_entry = (
                client.balance_exchanges.get("okx:futures")
                or client.balance_exchanges.get("okx")
            )
            if not futures_entry or futures_entry.get("market") != "futures" or not futures_entry.get("client"):
                raise RuntimeError("OKX futures client is not configured")
            okx = futures_entry["client"]
            positions = await okx.fetch_positions(["AI-USDT-SWAP"])
            long_contracts, short_contracts = _position_contracts(positions)
            print(
                f"Current OKX AI contracts: long={long_contracts}, short={short_contracts}"
            )
            if short_contracts > 1e-12:
                params = {"reduceOnly": True}
                params.update(futures_position_side_params(okx, "short"))
                order = await okx.create_market_buy_order(
                    "AI/USDT:USDT",
                    short_contracts,
                    {**params, "clientOrderId": "resolveaifuturesv1"},
                )
                if not _order_fully_filled(order, short_contracts):
                    order_id = str((order or {}).get("id") or "")
                    if order_id:
                        order = await okx.fetch_order(order_id, "AI/USDT:USDT")
                if not _order_fully_filled(order, short_contracts):
                    raise RuntimeError(
                        f"OKX reduceOnly BUY is not confirmed fully filled: {order}"
                    )
                positions = await okx.fetch_positions(["AI-USDT-SWAP"])
                long_contracts, short_contracts = _position_contracts(positions)
            futures_flat = long_contracts <= 1e-12 and short_contracts <= 1e-12
            if not futures_flat:
                raise RuntimeError(
                    f"OKX still has exposure after close: long={long_contracts}, "
                    f"short={short_contracts}"
                )
            print("OKX futures position verified flat.")
        except Exception as exc:
            print(f"OKX futures close unresolved; state retained: {exc}")

        # 3. Redis is bookkeeping, not proof of a venue close. Never delete the
        # position/recovery record unless both independent venue checks passed.
        if spot_flat and futures_flat:
            redis_client.delete("arb:active_pos:tokocrypto:toko:ai_usdt__okx:ai_usdt_swap")
            redis_client.hdel(
                "arb:order_pending_recovery",
                "cf5e057c-99ee-5fcf-99ab-4a1579f8671c",
            )
            print("Both legs verified flat; cleaned the Redis position/recovery state.")
        else:
            print(
                "At least one venue leg is unresolved; Redis state was retained "
                f"(spot_flat={spot_flat}, futures_flat={futures_flat})."
            )
    finally:
        try:
            redis_client.close()
        finally:
            await client.close()


if __name__ == "__main__":
    asyncio.run(resolve_ai())
