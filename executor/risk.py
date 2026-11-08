"""Compatibility facade for balance and exposure risk controls."""

from executor.balance_risk import BalanceRiskMixin
from executor.exposure_risk import ExposureRiskMixin


class RiskMixin(BalanceRiskMixin, ExposureRiskMixin):
    """Composes live balance validation and exposure sizing controls."""
