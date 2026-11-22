"""Shared mutation gateway result types."""
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class MutationResultType(str, Enum):
    CONFIRMED = "CONFIRMED"
    REJECTED = "REJECTED"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    DEFERRED = "DEFERRED"


@dataclass
class MutationResult:
    status: MutationResultType
    client_order_id: str
    exchange_order_id: Optional[str] = None
    filled_qty: float = 0.0
    average_price: float = 0.0
    error_message: str = ""
    raw_response: Optional[dict] = None

