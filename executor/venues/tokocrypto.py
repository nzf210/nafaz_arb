"""Tokocrypto exchange venue module."""
import hashlib
from typing import Any, Dict, List, Optional, Tuple
from executor.venues.base import BaseExchangeVenue

TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN = 32
_TOKOCRYPTO_ORDER_ARCHIVE_SIZE = 50


class TokocryptoVenue(BaseExchangeVenue):
    """Tokocrypto spot exchange venue implementation."""

    @property
    def venue_name(self) -> str:
        return "tokocrypto"

    @property
    def default_market_type(self) -> str:
        return "spot"

    def format_client_order_id(self, client_order_id: str, broker_prefix: str = "") -> str:
        """Rewrites a logical client id into the form Tokocrypto accepts."""
        parts = client_order_id.rsplit("-", 2)
        if len(parts) == 3:
            leg, seq = parts[1], parts[2]
        else:
            leg, seq = "", ""
        tail = "".join(c for c in f"{leg}{seq}" if c.isalnum())
        prefix = "".join(c for c in broker_prefix if c.isalnum())
        digest = hashlib.sha256(client_order_id.encode("utf-8")).hexdigest()
        room = TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(prefix) - len(tail)
        if room < 1:
            tail = tail[: TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(prefix) - 1]
            room = TOKOCRYPTO_CLIENT_ORDER_ID_MAX_LEN - len(prefix) - len(tail)
        if room < 1:
            raise ValueError(
                "broker prefix leaves no room for Tokocrypto client order id"
            )
        return f"{prefix}{digest[:room]}{tail}"

    def format_wire_symbol(self, venue: Any, symbol: str) -> str:
        """Translates a CCXT unified symbol to Tokocrypto's wire format (e.g. ATH_USDT)."""
        if not symbol or "/" not in symbol:
            return symbol
        try:
            market = venue.market(symbol)
        except Exception:
            market = None
        raw = market.get("id") if isinstance(market, dict) else None
        return str(raw) if raw else symbol.replace("/", "")

    async def fetch_fee(self, exchange: Any, symbol: str) -> Optional[Tuple[float, float]]:
        """Reads spot trading fee from Tokocrypto private API or CCXT."""
        endpoint = (
            getattr(exchange, "privateGetOpenV1AccountSpot", None)
            or getattr(exchange, "private_get_open_v1_account_spot", None)
        )
        if callable(endpoint):
            raw = await endpoint()
            account = raw.get("data", {}) if isinstance(raw, dict) else {}
            if not isinstance(account, dict):
                raise ValueError("invalid Tokocrypto account fee response")
            maker_bips = account.get("makerCommission")
            taker_bips = account.get("takerCommission")
            if maker_bips is None or taker_bips is None:
                raise ValueError(
                    "Tokocrypto account response omitted commission rates"
                )
            return float(maker_bips) / 100.0, float(taker_bips) / 100.0

        fetcher = getattr(exchange, "fetch_trading_fee", None)
        if not callable(fetcher):
            return None
        raw = await fetcher(symbol)
        maker, taker = raw.get("maker"), raw.get("taker")
        if maker is None or taker is None:
            raise ValueError(
                "incomplete Tokocrypto spot trading fee response"
            )
        return float(maker) * 100.0, float(taker) * 100.0
