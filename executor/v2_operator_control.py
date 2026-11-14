"""Durable PostgreSQL operator controls for the V2 Python executor.

Public symbols remain here for compatibility; persistence responsibilities are
split into focused mixins so each module stays reviewable.
"""
from __future__ import annotations

from typing import Any

from executor.v2_operator_models import (
    DuplicateCommandConflict,
    OperatorControlError,
    StartReadinessReport,
    TelegramIdentity,
    TelegramPolicy,
    confirmation_digest,
)
from executor.v2_operator_repository_common import OperatorRepositoryCommonMixin
from executor.v2_operator_repository_start import OperatorRepositoryStartMixin
from executor.v2_operator_repository_stop import OperatorRepositoryStopMixin


class PostgresOperatorControlRepository(
    OperatorRepositoryCommonMixin,
    OperatorRepositoryStartMixin,
    OperatorRepositoryStopMixin,
):
    """Transactional operator state, audit, idempotency and STOP fencing."""

    def __init__(self, pool: Any, *, consumer_lease_seconds: int = 30):
        if consumer_lease_seconds < 5:
            raise ValueError("consumer_lease_seconds must be at least 5")
        self._pool = pool
        self._consumer_lease_seconds = consumer_lease_seconds

__all__ = [
    "DuplicateCommandConflict",
    "OperatorControlError",
    "PostgresOperatorControlRepository",
    "StartReadinessReport",
    "TelegramIdentity",
    "TelegramPolicy",
    "confirmation_digest",
]
