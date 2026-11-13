"""Shared helpers: client order ids, tick rounding, retry, key builders."""

import asyncio
import hashlib
import logging
import math
from typing import Any, Awaitable, Callable, Optional, TypeVar

logger = logging.getLogger("executor.engine")

T = TypeVar("T")


ARB_TRADE_HISTORY_KEY = "arb:trade_history"
ARB_TRADE_HISTORY_MAX_LEN = 49  # ltrim keeps indices 0..49 → 50 entries

# Binance Cloud venues (e.g. Tokocrypto) validate client order ids against the
# broker id registered to the account: anything else is rejected with error
# 3703 "Invalid client ID". The value is issued per-account by the venue, so it
# cannot be hardcoded -- it must come from the exchange credentials
# (``options["broker_id"]`` in config.json or ``<EXCHANGE>_BROKER_ID`` in the
# environment). When it is unset the executor sends no client order id at all
# and lets ccxt mint a venue-native one from ``options["broker"]``, which the
# venue already trusts; a wrong guess is worse than no id, because a made-up
# prefix rejects every order.
DEFAULT_BINANCE_BROKER_PREFIX = ""

def _broker_prefix_for(exchange_id: str, exchange: Any = None) -> str:
    """Returns the registered broker prefix for a Binance Cloud venue, if any.

    Read from the exchange client's own ``options`` (populated from the
    exchange credentials by ``init_ccxt_exchange``), so the value travels with
    the account instead of being guessed in code.
    """
    if str(exchange_id or "").lower() != "tokocrypto":
        return ""
    options = getattr(exchange, "options", None)
    if isinstance(options, dict):
        configured = options.get("broker_id") or options.get("brokerId")
        if configured:
            return str(configured)
        broker = options.get("broker")
        if isinstance(broker, dict):
            market_type = broker.get("marketType") or broker.get("market_type")
            if market_type:
                return str(market_type)
    return DEFAULT_BINANCE_BROKER_PREFIX

def build_client_order_id(pair_tag: str, leg: str, seq: int) -> str:
    """
    Builds a venue-safe client order id: ``{pair}-{leg}-{seq}``.

    The id is deterministic for a given (pair, leg, sequence) so a retry of the
    same logical order is recognised as a duplicate by the venue instead of
    double-filling. Kept to lowercase alphanumerics and dashes because Binance,
    OKX and Indodax each accept a different, partly overlapping character set.

    Binance Cloud broker prefixes are applied on the wire by
    ``executor.exchange_client.venue_client_order_id``, which also enforces the
    36-character budget, so no prefix room is reserved here.
    """
    clean_pair = "".join(c if c.isalnum() else "-" for c in pair_tag.lower()).strip("-")
    clean_leg = "".join(c if c.isalnum() else "-" for c in leg.lower()).strip("-")
    suffix = f"-{clean_leg}-{seq}"
    max_pair_length = 36 - len(suffix)
    if max_pair_length < 1:
        raise ValueError("client order ID leg and sequence exceed the 36-character limit")

    if len(clean_pair) > max_pair_length:
        pair_hash = hashlib.sha256(pair_tag.lower().encode("utf-8")).hexdigest()[:8]
        visible_length = max_pair_length - len(pair_hash) - 1
        if visible_length < 1:
            clean_pair = pair_hash[:max_pair_length]
        else:
            clean_pair = f"{clean_pair[:visible_length].rstrip('-')}-{pair_hash}"
    return f"{clean_pair}{suffix}"


class HedgeSlippageExceeded(Exception):
    """Raised when the futures ask moved past the allowed entry slippage."""


class RetryExhausted(Exception):
    """Raised by retry_with_backoff when every attempt failed."""

    def __init__(self, attempts: int, last_error: Exception):
        super().__init__(f"all {attempts} attempts failed: {last_error}")
        self.attempts = attempts
        self.last_error = last_error


async def retry_with_backoff(
    fn: Callable[[], Awaitable[T]],
    max_retries: int = 3,
    base_delay: float = 0.5,
    label: str = "operation",
) -> T:
    """
    Calls an async callable, retrying with exponential backoff (base_delay * 2^n).

    ``max_retries`` counts retries *after* the initial attempt, so the callable is
    invoked at most ``max_retries + 1`` times. Raises RetryExhausted carrying the
    last error once every attempt has failed.
    """
    attempts = max(1, max_retries + 1)
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(f"[Retry] {label} attempt {attempt}/{attempts} failed: {exc}")
            if attempt < attempts:
                await asyncio.sleep(base_delay * (2 ** (attempt - 1)))
    raise RetryExhausted(attempts, last_error) from last_error

def clean_pair_key(local_ex: str, symbol: str) -> str:
    """Matches Golang CleanPairKey format: {local_ex}:{clean_symbol}"""
    clean_ex = local_ex.lower().strip()
    clean_sym = symbol.lower().strip().replace("/", "_").replace("-", "_")
    return f"{clean_ex}:{clean_sym}"


def round_to_tick(price: float, tick_size: float) -> float:
    """Rounds price to nearest local tick size."""
    if tick_size <= 0:
        return price
    return round(price / tick_size) * tick_size


def ceil_to_tick(price: float, tick_size: float) -> float:
    """Rounds price UP (ceiling) to local tick size to prevent selling below target/break-even."""
    if tick_size <= 0:
        return price
    return math.ceil(round(price / tick_size, 9)) * tick_size


def floor_to_tick(price: float, tick_size: float) -> float:
    """Rounds price DOWN (floor) to local tick size to prevent buying above target."""
    if tick_size <= 0:
        return price
    return math.floor(round(price / tick_size, 9)) * tick_size

BALANCES_KEY = "arb:balances"
PENDING_ENTRY_KEY_PREFIX = "arb:pending_entry:"

# --- Universal order registry keys -------------------------------------------
# Redis is the fast, restart-surviving cache; Supabase is the durable record.
# Every key here must be reconstructible after a Redis flush by rebuilding from
# the exchange (see UniversalOrderRegistry.startup_recovery).
ORDER_RECORD_KEY_PREFIX = "arb:order:"            # -> order registry JSON by local id
ORDER_RECOVERY_INDEX_KEY = "arb:order_recovery_idx"  # hash: local_id -> "1"
ORDER_BY_EXCHANGE_ID_KEY = "arb:order_xid"        # hash: "{exchange}:{exchange_order_id}" -> local_id
ORDER_BY_CLIENT_ID_KEY = "arb:order_cid"          # hash: "{exchange}:{client_order_id}" -> local_id
ORDER_PENDING_RECOVERY_KEY = "arb:order_pending_recovery"  # hash: local_id -> reason
ORPHAN_ORDER_KEY_PREFIX = "arb:orphan_order:"     # -> orphan registry JSON
ORPHAN_INDEX_KEY = "arb:orphan_idx"               # hash: orphan_id -> "1"

def order_record_key(local_order_id: str) -> str:
    return ORDER_RECORD_KEY_PREFIX + local_order_id

def orphan_order_key(orphan_id: str) -> str:
    return ORPHAN_ORDER_KEY_PREFIX + orphan_id

def order_xid_field(exchange: str, exchange_order_id: str) -> str:
    """Idempotency key for an exchange order id, scoped per venue.

    The same numeric id is reused across venues (indodax 301 and okx 301 are
    different orders), so the venue must be part of the field name.
    """
    return f"{str(exchange or '').lower()}:{exchange_order_id}"

def order_cid_field(exchange: str, client_order_id: str) -> str:
    return f"{str(exchange or '').lower()}:{client_order_id}"


# Last-resort IDR<->USDT rate for bookkeeping when no live rate is available.
FALLBACK_USDT_IDR_RATE = 16000.0

# A live price snapshot older than this is stale and must not drive exit
# pegging or floating-PnL decisions.
LIVE_PRICE_STALE_MS = 120_000


def snapshot_is_stale(snapshot: dict) -> bool:
    """True when a live-price snapshot is missing or older than LIVE_PRICE_STALE_MS."""
    import time as _time

    ts = float(snapshot.get("timestamp_ms", 0) or 0)
    if ts <= 0:
        return True
    return (int(_time.time() * 1000) - ts) > LIVE_PRICE_STALE_MS


def extract_fill_price(order: dict, *fallbacks: float) -> float:
    """Returns the first positive price from the order payload or the given fallbacks."""
    for key in ("average", "price", "fillPrice"):
        try:
            value = float(order.get(key) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    for fallback in fallbacks:
        if fallback and fallback > 0:
            return float(fallback)
    return 0.0
