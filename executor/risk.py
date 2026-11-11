"""Composed risk controls used by the arbitrage engine.

Implementation is split by responsibility so each module stays focused and
small while preserving the original mixin method surface.
"""

from executor.risk_exposure import RiskExposureMixin
from executor.risk_funding import RiskFundingMixin
from executor.risk_state import RiskStateMixin


class RiskMixin(RiskStateMixin, RiskExposureMixin, RiskFundingMixin):
    """Compatibility facade combining durable state, exposure, and funding."""
