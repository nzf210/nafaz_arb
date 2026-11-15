"""OKX exchange venue module."""
import hashlib
from typing import Any, Dict, Optional
from executor.venues.base import BaseExchangeVenue

_OKX_POSITION_SIDE = {"short": "short", "long": "long"}
OKX_CLIENT_ORDER_ID_MAX_LEN = 32


class OKXVenue(BaseExchangeVenue):
    """OKX perpetual / futures exchange venue implementation."""

    @property
    def venue_name(self) -> str:
        return "okx"

    @property
    def default_market_type(self) -> str:
        return "futures"

    @property
    def min_notional_usdt(self) -> float:
        return 10.0

    def format_client_order_id(self, client_order_id: str, broker_prefix: str = "") -> str:
        """Format client order id as <=32 alphanumeric chars without truncation collisions."""
        if not client_order_id:
            return client_order_id
        if (
            len(client_order_id) <= OKX_CLIENT_ORDER_ID_MAX_LEN
            and client_order_id.isascii()
            and client_order_id.isalnum()
        ):
            return client_order_id
        # OKX requires alphanumeric IDs <= 32 chars. Hash the complete logical
        # ID instead of truncating its suffix, which could collapse distinct
        # sequence IDs into the same venue ID.
        return hashlib.sha256(client_order_id.encode("utf-8")).hexdigest()[
            :OKX_CLIENT_ORDER_ID_MAX_LEN
        ]

    def position_side_params(self, position_side: str) -> Dict[str, Any]:
        """Parameters for long/short hedged position mode on OKX."""
        side = _OKX_POSITION_SIDE.get(str(position_side or "").lower())
        if side is None:
            raise ValueError(f"unsupported futures position side: {position_side}")
        return {"posSide": side}
