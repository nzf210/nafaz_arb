"""Shared errors and validation for durable order intents."""
class OrderIntentError(RuntimeError):
    """Base error for durable mutation intent operations."""


class OrderIntentConflict(OrderIntentError):
    """An idempotency key or client ID was reused for different request data."""


class OrderIntentReservationConflict(OrderIntentError):
    """The new intent would exceed a caller-supplied reservation budget."""


class OrderIntentStaleOwner(OrderIntentError):
    """The mutation lease is missing, expired or superseded."""


def required(value, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result
