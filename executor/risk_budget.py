"""Fail-closed evaluation of quantitative risk budgets at the mutation boundary."""
from __future__ import annotations

import math
from typing import Any, Mapping, Optional


class RiskBudgetViolation(RuntimeError):
    """A projected mutation exceeds a configured risk budget."""


class RiskBudgetEvaluator:
    """Evaluate configured limits against one immutable pre-submit metric snapshot."""

    LIMIT_TO_METRIC = {
        "max_gross_exposure_usdt": "gross_exposure_usdt",
        "max_net_exposure_usdt": "net_exposure_usdt",
        "max_deployed_capital_usdt": "deployed_capital_usdt",
        "max_unhedged_notional_usdt": "unhedged_notional_usdt",
        "max_unhedged_duration_ms": "unhedged_duration_ms",
        "max_open_order_count": "open_order_count",
        "max_daily_loss_usdt": "daily_loss_usdt",
        "max_drawdown_pct": "drawdown_pct",
        "max_slippage_bps": "slippage_bps",
        "max_funding_cost_pct": "funding_cost_pct",
        "max_market_data_age_ms": "market_data_age_ms",
    }

    @classmethod
    def evaluate(
        cls, *, metrics: Optional[Mapping[str, Any]],
        limits: Optional[Mapping[str, Any]], action_class: str,
    ) -> dict:
        action = str(action_class or "ENTRY").strip().upper()
        configured = {
            str(key): value for key, value in dict(limits or {}).items()
            if key in cls.LIMIT_TO_METRIC or key == "min_venue_health_score"
        }
        if action == "DERISK" or not configured:
            return {
                "action_class": action,
                "limits": configured,
                "metrics": dict(metrics or {}),
                "violations": [],
            }
        if metrics is None:
            raise RiskBudgetViolation(
                "risk metrics are unavailable while quantitative budgets are configured"
            )

        violations = []
        for limit_key, metric_key in cls.LIMIT_TO_METRIC.items():
            if limit_key not in configured:
                continue
            limit = cls._finite_number(configured[limit_key], limit_key)
            if limit <= 0:
                continue
            if metric_key not in metrics or metrics[metric_key] is None:
                violations.append(f"{metric_key}=UNAVAILABLE (limit {limit_key}={limit})")
                continue
            value = cls._finite_number(metrics[metric_key], metric_key)
            if value > limit:
                violations.append(f"{metric_key}={value:.8g} exceeds {limit_key}={limit:.8g}")

        if "min_venue_health_score" in configured:
            minimum = cls._finite_number(
                configured["min_venue_health_score"], "min_venue_health_score"
            )
            if minimum > 0:
                if metrics.get("venue_health_score") is None:
                    violations.append("venue_health_score=UNAVAILABLE")
                else:
                    score = cls._finite_number(metrics["venue_health_score"], "venue_health_score")
                    if score < minimum:
                        violations.append(
                            f"venue_health_score={score:.8g} below min_venue_health_score={minimum:.8g}"
                        )

        snapshot = {
            "action_class": action,
            "limits": configured,
            "metrics": dict(metrics),
            "violations": violations,
        }
        if violations:
            raise RiskBudgetViolation("; ".join(violations))
        return snapshot

    @staticmethod
    def _finite_number(value: Any, field: str) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise RiskBudgetViolation(f"{field} is not numeric") from exc
        if not math.isfinite(number) or number < 0:
            raise RiskBudgetViolation(f"{field} must be finite and non-negative")
        return number
