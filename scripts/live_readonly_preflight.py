"""Read-only LIVE connectivity preflight; this script never places or cancels orders."""

import argparse
import asyncio
import logging
import sys

from redis.asyncio import from_url

from executor.config import ExecutorConfig
from executor.exchange_client import create_ccxt_client
from executor.supabase import SupabaseWriter


logger = logging.getLogger("live_readonly_preflight")


def _spot_symbol_arg(value: str) -> tuple[str, str]:
    exchange, separator, symbol = value.partition("=")
    if not separator or not exchange.strip() or not symbol.strip():
        raise argparse.ArgumentTypeError(
            "spot symbols must use EXCHANGE=UNIFIED_SYMBOL, e.g. tokocrypto=BTC/IDR"
        )
    return exchange.strip().lower(), symbol.strip()


async def _run(args) -> None:
    config = ExecutorConfig.load(args.config)
    if config.mock_mode:
        raise RuntimeError("config is not LIVE; read-only preflight requires mode=LIVE")

    client = create_ccxt_client(
        config.local_exchange_name,
        config.global_exchange_name,
        config.local_credentials,
        config.global_credentials,
        config.exchanges,
    )
    if client is None:
        raise RuntimeError("exchange client factory returned no client")
    writer = SupabaseWriter(config.supabase)
    redis = None
    exchanges = []
    try:
        seen = set()
        connected = set()
        required = {config.local_exchange_name.lower(), config.global_exchange_name.lower()}
        requested_spot = dict(args.spot_symbol or {})
        required.update(requested_spot)

        for name, entry in sorted(client.balance_exchanges.items()):
            venue = entry.get("client") if isinstance(entry, dict) else None
            if venue is None:
                if name in required:
                    raise RuntimeError(
                        f"required exchange {name} has no authenticated client: "
                        f"{(entry or {}).get('error', 'not configured')}"
                    )
                continue
            connected.add(name)
            if id(venue) in seen:
                continue
            seen.add(id(venue))
            exchanges.append(venue)
            await venue.load_markets()
            balance = await venue.fetch_balance()
            if not isinstance(balance, dict):
                raise RuntimeError(f"{name} returned malformed private balance data")
            print(f"PASS exchange={name} markets={len(venue.markets)} private_balance=ok")

        missing = required - connected
        if missing:
            raise RuntimeError(
                "required exchange clients are missing: " + ", ".join(sorted(missing))
            )

        for name, symbol in requested_spot.items():
            entry = client.balance_exchanges.get(name) or {}
            if entry.get("market") != "spot":
                raise RuntimeError(f"{name} is not configured as a spot venue")
            venue_client = client.for_venue(name)
            orders = await venue_client.fetch_local_open_orders(symbol)
            print(f"PASS exchange={name} symbol={symbol} signed_open_orders={len(orders)}")

        if args.futures_symbol:
            positions = await client.fetch_positions(args.futures_symbol)
            orders = await client.fetch_global_open_orders(args.futures_symbol)
            print(
                f"PASS exchange={config.global_exchange_name} "
                f"symbol={args.futures_symbol} positions={len(positions)} "
                f"open_orders={len(orders)}"
            )

        if not writer.enabled or writer._outbox is None:
            raise RuntimeError("Supabase durable outbox could not be opened")
        await writer.start()
        durable_orders = await writer.fetch_open_order_records()
        durable_positions = await writer.fetch_open_position_records()
        fill_candidates = await writer.fetch_recent_order_fill_candidates(days=90, limit=10000)
        print(
            "PASS supabase "
            f"open_orders={len(durable_orders)} "
            f"active_positions={len(durable_positions)} "
            f"fill_candidates={len(fill_candidates)}"
        )

        redis = from_url(config.redis_url, decode_responses=True)
        await redis.ping()
        print("PASS redis ping=ok")
        print("READ-ONLY PREFLIGHT PASSED; no order was submitted or cancelled.")
    finally:
        if redis is not None:
            closer = getattr(redis, "aclose", None) or getattr(redis, "close", None)
            if callable(closer):
                result = closer()
                if hasattr(result, "__await__"):
                    await result
        await writer.close()
        for venue in exchanges:
            try:
                await venue.close()
            except Exception:
                logger.exception("failed to close exchange client %s", getattr(venue, "id", "unknown"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.json")
    parser.add_argument(
        "--spot-symbol",
        action="append",
        type=_spot_symbol_arg,
        help="signed spot open-order check, repeat as EXCHANGE=SYMBOL",
    )
    parser.add_argument(
        "--futures-symbol",
        help="signed futures positions/open-orders check on GLOBAL_EXCHANGE",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(_run(args))
    except Exception as exc:
        print(f"LIVE PREFLIGHT FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
