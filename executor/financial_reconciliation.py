"""Financial Reconciliation Engine (Executor V2).

Implements Section 22.5 of EXECUTOR-ARCHITECTURE-V2.md:
- Reconciles internal trade fills and fee ledger with exchange trade history
- Detects discrepancies in fill volume, fee amount, and realized funding
- Generates structured reconciliation audit differences without mutating immutable ledger
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger("executor.financial_reconciliation")


class ReconciliationSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    MATERIAL_MISMATCH = "MATERIAL_MISMATCH"


@dataclass
class FinancialDifference:
    venue: str
    symbol: str
    order_id: str
    difference_type: str
    internal_value: float
    exchange_value: float
    delta: float
    severity: ReconciliationSeverity
    detail: str = ""


class FinancialReconciliationEngine:
    """Audits and matches internal accounting against exchange trade and funding history."""

    def __init__(self, *, material_qty_threshold: float = 1e-4, material_fee_threshold_usdt: float = 0.50):
        self.material_qty_threshold = material_qty_threshold
        self.material_fee_threshold_usdt = material_fee_threshold_usdt

    def reconcile_order_fills(
        self,
        *,
        venue: str,
        symbol: str,
        order_id: str,
        internal_fills: List[Dict[str, Any]],
        exchange_trades: List[Dict[str, Any]],
    ) -> List[FinancialDifference]:
        """Compare internal fill quantities and fees against exchange trade records."""
        diffs = []

        total_internal_qty = sum(float(f.get("amount") or f.get("qty") or 0.0) for f in internal_fills)
        total_exchange_qty = sum(float(t.get("amount") or t.get("qty") or 0.0) for t in exchange_trades)

        qty_delta = abs(total_internal_qty - total_exchange_qty)
        if qty_delta > self.material_qty_threshold:
            severity = (
                ReconciliationSeverity.MATERIAL_MISMATCH
                if qty_delta > self.material_qty_threshold * 10
                else ReconciliationSeverity.WARNING
            )
            diffs.append(
                FinancialDifference(
                    venue=venue,
                    symbol=symbol,
                    order_id=order_id,
                    difference_type="FILL_QTY_MISMATCH",
                    internal_value=total_internal_qty,
                    exchange_value=total_exchange_qty,
                    delta=qty_delta,
                    severity=severity,
                    detail=f"Internal cumulative qty ({total_internal_qty}) != exchange qty ({total_exchange_qty})",
                )
            )

        # Reconcile fee totals
        internal_fees = sum(float((f.get("fee") or {}).get("cost", 0.0)) for f in internal_fills)
        exchange_fees = sum(float((t.get("fee") or {}).get("cost", 0.0)) for t in exchange_trades)

        fee_delta = abs(internal_fees - exchange_fees)
        if fee_delta > self.material_fee_threshold_usdt:
            diffs.append(
                FinancialDifference(
                    venue=venue,
                    symbol=symbol,
                    order_id=order_id,
                    difference_type="FEE_AMOUNT_MISMATCH",
                    internal_value=internal_fees,
                    exchange_value=exchange_fees,
                    delta=fee_delta,
                    severity=ReconciliationSeverity.WARNING,
                    detail=f"Internal fee ({internal_fees}) != exchange fee ({exchange_fees})",
                )
            )

        return diffs
