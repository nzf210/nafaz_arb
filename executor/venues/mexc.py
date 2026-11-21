"""MEXC exchange venue module."""
import hashlib
from typing import Any, Dict
from executor.venues.base import BaseExchangeVenue

MEXC_CLIENT_ORDER_ID_MAX_LEN = 32


class MEXCVenue(BaseExchangeVenue):
    """MEXC exchange venue implementation."""

    @property
    def venue_name(self) -> str:
        return "mexc"

    @property
    def default_market_type(self) -> str:
        return "futures"

    def format_client_order_id(self, client_order_id: str, broker_prefix: str = "") -> str:
        """Format client order id as <=32 chars matching ^[0-9a-zA-Z_-]{1,32}$."""
        if not client_order_id:
            return client_order_id
        if (
            len(client_order_id) <= MEXC_CLIENT_ORDER_ID_MAX_LEN
            and all(c.isalnum() or c in ("-", "_") for c in client_order_id)
        ):
            return client_order_id
        parts = client_order_id.rsplit("-", 2)
        if len(parts) == 3:
            leg, seq = parts[1], parts[2]
            tail = f"-{leg}-{seq}"
        else:
            tail = ""
        digest = hashlib.sha256(client_order_id.encode("utf-8")).hexdigest()
        room = MEXC_CLIENT_ORDER_ID_MAX_LEN - len(tail)
        if room < 1:
            return digest[:MEXC_CLIENT_ORDER_ID_MAX_LEN]
        return f"{digest[:room]}{tail}"

    def balance_fetch_params(self, market: str) -> Dict[str, Any]:
        """Explicit CCXT market params for MEXC."""
        if market == "futures":
            return {"type": "swap"}
        return {}

