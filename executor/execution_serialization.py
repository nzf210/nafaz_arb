"""Canonical serialization and validation helpers for Executor V3 records."""

import json
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _positive_decimal(value: Any, field: str, *, allow_zero: bool = False) -> str:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal") from exc
    if not number.is_finite() or (number < 0 if allow_zero else number <= 0):
        comparator = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {comparator}")
    return format(number, "f")


def _required_text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{field} is required")
    return result


def _same_json(left: Any, right: str) -> bool:
    if isinstance(left, str):
        try:
            left = json.loads(left)
        except (TypeError, ValueError):
            return False
    return left == json.loads(right)


