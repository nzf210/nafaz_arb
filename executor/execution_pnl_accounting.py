"""V3 PnL accounting service facade."""
from __future__ import annotations

from executor.execution_pnl_common import PnLAccountingError
from executor.execution_position_accounting import ExecutionPositionAccountingMixin
from executor.execution_pnl_ledger import ExecutionPnLLedgerMixin


class ExecutionPnLAccountingService(
    ExecutionPositionAccountingMixin,
    ExecutionPnLLedgerMixin,
):
    """One facade for position projections, estimated PnL and realized PnL."""

    def __init__(self, pool):
        if pool is None or not hasattr(pool, "acquire"):
            raise ValueError("a PostgreSQL async pool is required")
        self.pool = pool
