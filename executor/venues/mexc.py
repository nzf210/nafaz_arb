"""MEXC exchange venue module."""
from typing import Any, Dict
from executor.venues.base import BaseExchangeVenue


class MEXCVenue(BaseExchangeVenue):
    """MEXC exchange venue implementation."""

    @property
    def venue_name(self) -> str:
        return "mexc"

    @property
    def default_market_type(self) -> str:
        return "futures"

    def balance_fetch_params(self, market: str) -> Dict[str, Any]:
        """Explicit CCXT market params for MEXC."""
        if market == "futures":
            return {"type": "swap"}
        return {}
