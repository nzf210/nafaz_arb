"""Shared Supabase row normalization, role mapping, and delivery constants."""

import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

logger = logging.getLogger("executor.supabase")


_MAX_ATTEMPTS = 3
_BATCH_MAX = 50
_TIMEOUT = aiohttp.ClientTimeout(total=8, connect=3)
_SIDE_BY_LEG = {
    "spot_entry": "buy",
    "spot_exit": "sell",
    "futures_entry": "sell",
    "futures_exit": "buy",
    "rollback": "sell",
    "force_close": "sell",
}

# Order role -> (leg per market, side, market) for the universal order registry.
# Sides are canonical because the schema stores `side` in ('buy','sell') while
# the bot describes the futures leg as "short". A role maps to different legs per
# market: FORCE_CLOSE buys on perpetual (close the short) but sells on spot, and
# the schema's leg vocabulary distinguishes them.
_ROLE_CATALOGUE = {
    "ENTRY": ({"spot": "spot_entry"}, "buy", "spot"),
    "HEDGE": ({"perpetual": "futures_entry"}, "sell", "perpetual"),
    "EXIT_SPOT": ({"spot": "spot_exit"}, "sell", "spot"),
    "EXIT_FUTURES": ({"perpetual": "futures_exit"}, "buy", "perpetual"),
    "EMERGENCY": ({"spot": "rollback"}, "sell", "spot"),
    "ROLLBACK": ({"spot": "rollback"}, "sell", "spot"),
    "FORCE_CLOSE": ({"spot": "force_close", "perpetual": "futures_exit"}, "sell", "spot"),
}

_LEG_TO_ROLE = {
    "spot_entry": "ENTRY",
    "futures_entry": "HEDGE",
    "spot_exit": "EXIT_SPOT",
    "futures_exit": "EXIT_FUTURES",
    "rollback": "ROLLBACK",
    "force_close": "FORCE_CLOSE",
}

def _role_catalogue(role: Any, leg: Optional[str], market: Any = "") -> tuple:
    """Returns (side, leg, market_type) for a registry row, filling gaps only."""
    key = str(role or "").upper()
    legs, side, default_market = _ROLE_CATALOGUE.get(
        key, ({"spot": "spot_entry"}, "sell", "spot")
    )
    resolved_market = "perpetual" if str(market or "").lower() in ("perpetual", "futures", "swap") else default_market
    if leg:
        resolved_leg = str(leg)
        resolved_market = "perpetual" if resolved_leg in ("futures_entry", "futures_exit") else "spot"
    else:
        resolved_leg = legs.get(resolved_market) or next(iter(legs.values()))
    return side, resolved_leg, resolved_market

def _canonical_side(value: Any) -> Optional[str]:
    """Maps a direction onto the schema's buy/sell vocabulary, or None.

    A FORCE_CLOSE role covers both a spot sell and a futures buy, so the role
    alone cannot state the side; the caller's own direction is authoritative.
    """
    side = str(value or "").strip().lower()
    if side in ("buy", "long"):
        return "buy"
    if side in ("sell", "short"):
        return "sell"
    return None
# Positions are keyed deterministically by pair so a restart reuses the same row.
# The schema allows only one active position per (pair_key, is_mock); a random id
# per process would collide with the previous process's row instead of updating it.
_POSITION_NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


def _position_uuid(pair_key: str, opened_at_ms: Any = 0) -> str:
    """Deterministic position id, stable across retries and restarts.

    Including the open timestamp keeps consecutive trades on the same pair as
    separate rows instead of overwriting each other.
    """
    try:
        stamp = int(opened_at_ms or 0)
    except (TypeError, ValueError):
        stamp = 0
    return str(uuid.uuid5(_POSITION_NS, f"{pair_key}:{stamp}"))


def _ms_to_iso(value: Any) -> Optional[str]:
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(result) else result  # drop NaN
