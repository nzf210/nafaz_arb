"""Shared validation and errors for V3 position/PnL persistence."""
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any


class PnLAccountingError(ValueError):
    """PnL evidence is incomplete, inconsistent, or not owned by this session."""


def _d(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PnLAccountingError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result <= 0 if positive else result < 0):
        raise PnLAccountingError(f"{name} must be {'positive' if positive else 'non-negative'}")
    return result


def _signed_d(value: Any, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PnLAccountingError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise PnLAccountingError(f"{name} must be a finite decimal")
    return result


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
