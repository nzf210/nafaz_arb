"""Venue registry and factory for exchange modules."""
from typing import Dict, Optional
from executor.venues.base import BaseExchangeVenue
from executor.venues.indodax import IndodaxVenue
from executor.venues.okx import OKXVenue
from executor.venues.tokocrypto import TokocryptoVenue
from executor.venues.mexc import MEXCVenue

_REGISTRY: Dict[str, BaseExchangeVenue] = {
    "indodax": IndodaxVenue(),
    "okx": OKXVenue(),
    "tokocrypto": TokocryptoVenue(),
    "mexc": MEXCVenue(),
}

_DEFAULT_VENUE = BaseExchangeVenue()


def get_venue(venue_name: str) -> BaseExchangeVenue:
    """Return the venue module instance for a given exchange name."""
    if not venue_name:
        return _DEFAULT_VENUE
    name = str(venue_name).strip().lower()
    return _REGISTRY.get(name, _DEFAULT_VENUE)
