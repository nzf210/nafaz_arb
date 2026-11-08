"""Compatibility facade for account and recovery reconciliation mixins."""

from executor.account_reconciliation import AccountWideReconciliationMixin
from executor.recovered_position import RecoveredPositionMixin


class ReconciliationMixin(AccountWideReconciliationMixin, RecoveredPositionMixin):
    """Composes account-wide and per-position recovery verification."""
