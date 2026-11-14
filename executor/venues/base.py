"""Base class for venue-specific exchange modules."""
from typing import Any, Dict, List, Optional, Tuple


class BaseExchangeVenue:
    """Abstract base for exchange venue modules."""

    @property
    def venue_name(self) -> str:
        raise NotImplementedError

    @property
    def default_market_type(self) -> str:
        return "spot"

    @property
    def min_notional_usdt(self) -> float:
        """Minimum order notional in USDT for venue."""
        return 0.0

    def format_client_order_id(self, client_order_id: str, broker_prefix: str = "") -> str:
        """Format a client order id according to venue rules."""
        return client_order_id

    def format_wire_symbol(self, venue: Any, symbol: str) -> str:
        """Format an internal symbol to venue wire symbol format."""
        return symbol

    async def fetch_fee(self, exchange: Any, symbol: str) -> Optional[Tuple[float, float]]:
        """Fetch venue-specific maker/taker fee rates."""
        return None

    async def fetch_order_fallback(
        self,
        exchange: Any,
        order_id: Optional[str],
        symbol: Optional[str],
        client_order_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch an order when standard lookup fails or requires venue-specific fallback."""
        return None

    async def fetch_trade_history(
        self,
        exchange: Any,
        symbol: str,
        limit: int = 50,
        since: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch trade history for a symbol."""
        return []

    async def fetch_futures_balance(self, exchange: Any) -> Optional[Dict[str, Any]]:
        """Fetch futures balance for venue-specific balance endpoints."""
        return None
