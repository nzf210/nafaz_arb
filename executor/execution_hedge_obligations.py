"""Public V3 hedge lifecycle service composed from narrow domain mixins."""
from executor.execution_hedge_obligations_core import HedgeObligationMixin
from executor.execution_hedge_reservations import HedgeReservationMixin
from executor.execution_hedge_allocation import HedgeAllocationMixin


class ExecutionHedgeObligationService(
    HedgeObligationMixin,
    HedgeReservationMixin,
    HedgeAllocationMixin,
):
    """Session-scoped durable obligations, reservations, and fill allocation."""
