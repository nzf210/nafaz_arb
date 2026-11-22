"""V3 execution-domain value objects, invariants and transition contracts."""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from typing import Any, Mapping


class ExecutionDomainError(RuntimeError):
    """Base error for durable execution-domain operations."""


class ExecutionSessionNotFound(ExecutionDomainError):
    pass


class ExecutionLegNotFound(ExecutionDomainError):
    pass


class StaleExecutionVersion(ExecutionDomainError):
    pass


class StaleExecutionFence(ExecutionDomainError):
    pass


class InvalidExecutionTransition(ExecutionDomainError):
    pass


class ExecutionIdempotencyConflict(ExecutionDomainError):
    pass


class ExecutionAdmissionConflict(ExecutionDomainError):
    pass


@dataclass(frozen=True)
class SessionCreationResult:
    session_id: str
    admission_key: str
    policy_snapshot_hash: str
    aggregate_version: int
    leg_ids: tuple[str, ...]
    duplicate: bool = False


@dataclass(frozen=True)
class TransitionResult:
    session_id: str
    entity_type: str
    entity_id: str
    from_state: str
    to_state: str
    entity_version: int
    aggregate_version: int
    event_id: str
    duplicate: bool = False


_SESSION_TRANSITIONS = {
    "CREATED": {"VALIDATING", "REJECTED", "ABORTED"},
    "VALIDATING": {"ADMITTED", "REJECTED", "BLOCKED", "ABORTED"},
    "ADMITTED": {"ENTRY_IN_PROGRESS", "BLOCKED", "FAILED", "ABORTED"},
    "ENTRY_IN_PROGRESS": {"HEDGE_PENDING", "BLOCKED", "RECONCILIATION_REQUIRED", "FAILED", "ABORTED"},
    "HEDGE_PENDING": {"PARTIALLY_HEDGED", "HEDGED", "BLOCKED", "RECONCILIATION_REQUIRED", "FAILED", "ABORTED"},
    "PARTIALLY_HEDGED": {"HEDGED", "EXIT_IN_PROGRESS", "BLOCKED", "RECONCILIATION_REQUIRED", "FAILED", "ABORTED"},
    "HEDGED": {"EXIT_IN_PROGRESS", "BLOCKED", "RECONCILIATION_REQUIRED", "FAILED", "ABORTED"},
    "EXIT_IN_PROGRESS": {"EXIT_PARTIAL", "COMPLETED", "BLOCKED", "RECONCILIATION_REQUIRED", "FAILED", "ABORTED"},
    "EXIT_PARTIAL": {"EXIT_IN_PROGRESS", "COMPLETED", "BLOCKED", "RECONCILIATION_REQUIRED", "FAILED", "ABORTED"},
    "RECONCILIATION_REQUIRED": {"RECOVERY_IN_PROGRESS", "BLOCKED", "FAILED", "ABORTED"},
    "RECOVERY_IN_PROGRESS": {
        "ENTRY_IN_PROGRESS", "HEDGE_PENDING", "PARTIALLY_HEDGED", "HEDGED",
        "EXIT_IN_PROGRESS", "EXIT_PARTIAL", "BLOCKED", "RECONCILIATION_REQUIRED",
    },
    "BLOCKED": {"RECONCILIATION_REQUIRED", "RECOVERY_IN_PROGRESS", "ABORTED", "FAILED"},
}

_LEG_TRANSITIONS = {
    "NOT_STARTED": {"PREPARED", "UNKNOWN"},
    "PREPARED": {"SUBMITTING", "CLOSED", "UNKNOWN"},
    "SUBMITTING": {"SUBMISSION_UNKNOWN", "OPEN", "PARTIALLY_FILLED", "FILLED", "REJECTED", "UNKNOWN"},
    "SUBMISSION_UNKNOWN": {"OPEN", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELED", "UNKNOWN"},
    "OPEN": {"PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "UNKNOWN"},
    "PARTIALLY_FILLED": {"FILLED", "CANCEL_PENDING", "CLOSING", "UNKNOWN"},
    "FILLED": {"CANCEL_PENDING", "CLOSING", "CLOSED", "UNKNOWN"},
    "CANCEL_PENDING": {"CANCELED", "PARTIALLY_FILLED", "FILLED", "UNKNOWN"},
    "CANCELED": {"CLOSING", "CLOSED", "UNKNOWN"},
    "CLOSING": {"CLOSED", "UNKNOWN"},
    "UNKNOWN": {"OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED", "CLOSING"},
}


def _validate_transition_evidence(
    entity_type: str, next_state: str, evidence: Mapping[str, Any]
) -> None:
    """Reject lifecycle states whose mandatory evidence is not supplied."""
    required: tuple[str, ...] = ()
    if entity_type == "SESSION" and next_state == "HEDGE_PENDING":
        required = ("hedge_obligation_id",)
    elif entity_type == "SESSION" and next_state == "HEDGED":
        required = ("hedge_coverage_verified",)
    elif entity_type == "LEG" and next_state == "CLOSED":
        required = ("verified_flat", "all_orders_terminal")
    elif entity_type == "SESSION" and next_state == "COMPLETED":
        required = (
            "spot_flat", "futures_flat", "all_orders_terminal",
            "no_unknown_submits", "fills_accounted", "hedge_obligations_zero",
            "allocations_zero", "pnl_reconciled", "risk_clear",
        )
    missing = [key for key in required if key not in evidence]
    if missing:
        raise InvalidExecutionTransition(
            f"{entity_type} -> {next_state} requires evidence: {', '.join(missing)}"
        )
    if next_state == "HEDGE_PENDING" and not str(evidence.get("hedge_obligation_id") or "").strip():
        raise InvalidExecutionTransition("HEDGE_PENDING requires a durable hedge_obligation_id")
    for key in required:
        if key != "hedge_obligation_id" and evidence.get(key) is not True:
            raise InvalidExecutionTransition(f"{entity_type} -> {next_state} requires {key}=true")


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid decimal") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return number


def canonical_policy_hash(snapshot: Mapping[str, Any]) -> str:
    """Stable SHA-256 over sorted compact JSON with an explicit schema domain."""
    encoded = json.dumps(
        snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(b"nafaz-executor-policy:v1\0" + encoded).hexdigest()
