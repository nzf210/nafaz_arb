"""Fail-closed evaluation of quantitative risk budgets at the mutation boundary."""
from __future__ import annotations

import math
import time
from typing import Any, Mapping, Optional


class RiskBudgetViolation(RuntimeError):
    """A projected mutation exceeds a configured risk budget."""


class RiskBudgetMetricsBuilder:
    """Build a conservative projected risk snapshot from the executor's live state."""

    @staticmethod
    def _value(obj: Any, key: str, default: Any = 0.0) -> Any:
        if isinstance(obj, Mapping):
            return obj.get(key, default)
        return getattr(obj, key, default)

    @classmethod
    def build(
        cls, engine: Any, *, venue: str, action_class: str,
        proposed_notional_usdt: float = 0.0,
        planned_hedge_notional_usdt: float = 0.0,
        related_venues: Optional[list[str]] = None,
        slippage_bps: Optional[float] = None,
        funding_cost_pct: Optional[float] = None,
        market_data_age_ms: Optional[float] = None,
    ) -> dict:
        now_ms = int(time.time() * 1000)
        config = getattr(engine, "config", None)
        leverage = max(1.0, float(getattr(config, "futures_leverage", 1.0) or 1.0))
        positions = cls._position_metrics(engine, now_ms)
        pending = cls._pending_metrics(engine, leverage)
        projected = cls._project_action(
            positions, pending, action_class,
            proposed_notional_usdt, planned_hedge_notional_usdt, leverage,
        )
        capital = max(0.0, float(getattr(config, "capital_total", 0.0) or 0.0))
        daily_loss = max(
            0.0, float(getattr(engine, "_daily_realized_loss_usdt", 0.0) or 0.0)
        )
        drawdown_pct = cls._drawdown_pct(
            capital, daily_loss, positions["unrealized_loss_usdt"]
        )
        venue_key, venue_keys, health_score = cls._venue_health(
            engine, venue, related_venues
        )
        return {
            "snapshot_at_ms": now_ms,
            "target_venue": venue_key,
            "target_venues": venue_keys,
            "gross_exposure_usdt": (
                None if projected["gross_unpriced"] else projected["gross"]
            ),
            "net_exposure_usdt": (
                None if projected["net_unpriced"] else abs(projected["net"])
            ),
            "deployed_capital_usdt": (
                None if projected["deployed_unpriced"] else projected["deployed"]
            ),
            "unhedged_notional_usdt": (
                None if projected["unhedged_unpriced"] else projected["unhedged"]
            ),
            "unhedged_duration_ms": positions["unhedged_duration_ms"],
            "open_order_count": pending["open_order_count"] + projected["additional_orders"],
            "daily_loss_usdt": daily_loss,
            "drawdown_pct": drawdown_pct,
            "slippage_bps": slippage_bps,
            "funding_cost_pct": funding_cost_pct,
            "market_data_age_ms": market_data_age_ms,
            "venue_health_score": health_score,
        }

    @classmethod
    def _position_metrics(cls, engine: Any, now_ms: int) -> dict:
        totals = {
            "gross": 0.0, "deployed": 0.0, "net": 0.0, "unhedged": 0.0,
            "unrealized_loss_usdt": 0.0, "unhedged_duration_ms": 0,
            "gross_unpriced": False, "deployed_unpriced": False,
            "net_unpriced": False, "unhedged_unpriced": False,
        }
        positions = getattr(engine, "active_positions", {}) or {}
        values = positions.values() if isinstance(positions, Mapping) else positions
        for position in values:
            item = cls._single_position_metrics(position, now_ms)
            for key in ("gross", "deployed", "net", "unhedged", "unrealized_loss_usdt"):
                totals[key] += item[key]
            totals["unhedged_duration_ms"] = max(
                totals["unhedged_duration_ms"], item["unhedged_duration_ms"]
            )
            for key in ("gross_unpriced", "deployed_unpriced", "net_unpriced", "unhedged_unpriced"):
                totals[key] = totals[key] or item[key]
        return totals

    @classmethod
    def _single_position_metrics(cls, position: Any, now_ms: int) -> dict:
        spot = max(0.0, float(cls._value(position, "spot_cost_usdt", 0.0) or 0.0))
        future = max(0.0, float(cls._value(position, "short_entry_usdt", 0.0) or 0.0))
        margin = max(0.0, float(cls._value(position, "futures_margin_usdt", 0.0) or 0.0))
        qty = abs(float(cls._value(position, "qty", 0.0) or 0.0))
        state = str(cls._value(position, "state", "HEDGED_ACTIVE") or "HEDGED_ACTIVE").upper()
        unhedged_qty = abs(float(cls._value(position, "unhedged_exit_base_qty", 0.0) or 0.0))
        if unhedged_qty <= 0 and state not in {"HEDGED_ACTIVE", "CLOSED", "FLAT"}:
            unhedged_qty = qty
        mark = max(0.0, float(cls._value(position, "current_perp_ask_usdt", 0.0) or 0.0))
        opened_at = int(cls._value(position, "opened_at_ms", 0) or 0)
        unhedged_value = unhedged_qty * mark
        return {
            "gross": spot + future,
            "deployed": spot + margin,
            "net": spot - future,
            "unhedged": unhedged_value,
            "unrealized_loss_usdt": max(
                0.0, -float(cls._value(position, "net_unrealized_pnl_usdt", 0.0) or 0.0)
            ),
            "unhedged_duration_ms": (
                max(0, now_ms - opened_at) if unhedged_qty > 0 and opened_at > 0 else 0
            ),
            "gross_unpriced": qty > 0 and spot <= 0 and future <= 0,
            "deployed_unpriced": future > 0 and margin <= 0,
            "net_unpriced": qty > 0 and spot <= 0 and future <= 0,
            "unhedged_unpriced": unhedged_qty > 0 and mark <= 0,
        }

    @classmethod
    def _pending_metrics(cls, engine: Any, leverage: float) -> dict:
        entries = getattr(engine, "pending_entry_orders", {}) or {}
        exits = getattr(engine, "pending_exit_orders", {}) or {}
        hedges = getattr(engine, "pending_hedge_orders", {}) or {}
        totals = {
            "gross": 0.0, "deployed": 0.0, "net": 0.0, "unhedged": 0.0,
            "open_order_count": sum(
                len(value) if hasattr(value, "__len__") else 0
                for value in (entries, exits, hedges)
            ),
            "gross_unpriced": False, "deployed_unpriced": False,
            "net_unpriced": False, "unhedged_unpriced": False,
        }
        for pending in cls._iter_pending_orders(entries):
            notional = cls._pending_notional(pending, engine=engine)
            totals["gross"] += notional
            totals["deployed"] += notional
            totals["net"] += notional
            totals["unhedged"] += notional
            if notional <= 0:
                cls._mark_unpriced(totals)
        for pending in cls._iter_pending_orders(hedges):
            notional = cls._pending_notional(pending, engine=engine)
            totals["gross"] += notional
            totals["deployed"] += notional / leverage
            if notional <= 0:
                cls._mark_unpriced(totals)
        return totals

    @staticmethod
    def _mark_unpriced(metrics: dict) -> None:
        for key in ("gross_unpriced", "deployed_unpriced", "net_unpriced", "unhedged_unpriced"):
            metrics[key] = True

    @staticmethod
    def _project_action(
        positions: dict, pending: dict, action_class: str,
        proposed_notional_usdt: float, planned_hedge_notional_usdt: float,
        leverage: float,
    ) -> dict:
        projected = {
            key: positions[key] + pending[key]
            for key in ("gross", "deployed", "net", "unhedged")
        }
        for key in ("gross_unpriced", "deployed_unpriced", "net_unpriced", "unhedged_unpriced"):
            projected[key] = positions[key] or pending[key]
        projected["additional_orders"] = 0
        action = str(action_class or "ENTRY").strip().upper()
        proposed = max(0.0, float(proposed_notional_usdt or 0.0))
        if action == "ENTRY":
            hedge = max(0.0, float(planned_hedge_notional_usdt or 0.0))
            projected["gross"] += proposed + hedge
            projected["deployed"] += proposed + hedge / leverage
            projected["net"] += proposed - hedge
            projected["unhedged"] += proposed
            projected["additional_orders"] = 1
        elif action == "HEDGE":
            projected["gross"] += proposed
            projected["deployed"] += proposed / leverage
            projected["net"] = max(0.0, abs(projected["net"]) - proposed)
            projected["unhedged"] = max(0.0, projected["unhedged"] - proposed)
            projected["additional_orders"] = 1
        return projected

    @staticmethod
    def _drawdown_pct(capital: float, daily_loss: float, unrealized_loss: float) -> float:
        if capital <= 0:
            return 0.0
        return ((daily_loss + unrealized_loss) / capital) * 100.0

    @staticmethod
    def _venue_health(engine: Any, venue: str, related_venues: Optional[list[str]]) -> tuple:
        venue_key = str(venue or "").strip().lower()
        venue_keys = list(dict.fromkeys(
            key for key in [venue_key, *[
                str(item or "").strip().lower() for item in (related_venues or [])
            ]] if key
        ))
        config = getattr(engine, "config", None)
        halted = getattr(engine, "_exchange_halted", {}) or {}
        failures = getattr(engine, "_consecutive_order_failures", {}) or {}
        max_failures = max(1, int(getattr(config, "max_consecutive_order_failures", 50) or 50))
        scores = []
        for venue_name in venue_keys:
            failure_count = int(failures.get(venue_name, 0) or 0) if isinstance(failures, Mapping) else 0
            score = 0.0 if bool(halted.get(venue_name, False)) else max(
                0.0, min(1.0, 1.0 - failure_count / max_failures)
            )
            scores.append(score)
        return venue_key, venue_keys, min(scores, default=1.0)

    @staticmethod
    def _iter_pending_orders(value: Any) -> list:
        if isinstance(value, Mapping):
            return list(value.values())
        if isinstance(value, (list, tuple, set)):
            return list(value)
        return []

    @staticmethod
    def _pending_notional(pending: Any, *, engine: Any = None) -> float:
        def get(key: str, default: Any = 0.0) -> Any:
            if isinstance(pending, Mapping):
                return pending.get(key, default)
            return getattr(pending, key, default)
        for key in ("notional_usdt", "quote_amount_usdt", "cost_usdt", "amount_usdt"):
            value = float(get(key, 0.0) or 0.0)
            if value > 0:
                return value
        amount = float(get("quantity", get("amount", 0.0)) or 0.0)
        price = float(get("price", 0.0) or 0.0)
        quote_asset = str(
            get("quote_asset", get("quote_currency", "")) or ""
        ).upper()
        if quote_asset in {"USDT", "USD"}:
            return max(0.0, amount * price)
        if quote_asset == "IDR":
            rate = float(
                get(
                    "usdt_idr_rate",
                    getattr(engine, "usdt_idr_rate", getattr(engine, "_usdt_idr_rate", 0.0)),
                ) or 0.0
            )
            return max(0.0, amount * price / rate) if rate > 0 else 0.0
        # Never treat an unknown quote currency (especially IDR) as USDT.
        return 0.0

    @staticmethod
    def limits_from_config(config: Any) -> dict:
        capital = max(0.0, float(getattr(config, "capital_total", 0.0) or 0.0))
        allocation = max(0.0, float(getattr(config, "allocation_per_pair", 0.5) or 0.0))
        max_position = max(0.0, float(getattr(config, "max_position_usdt", 0.0) or 0.0))
        max_exchange = max(
            0.0, float(getattr(config, "max_exposure_per_exchange_usdt", 0.0) or 0.0)
        )
        daily_loss = max(0.0, float(getattr(config, "max_daily_loss_usdt", 0.0) or 0.0))
        max_slippage = max(
            0.0, float(getattr(config, "max_entry_slippage_pct", 0.0) or 0.0)
        )
        max_stale = max(0.0, float(getattr(config, "max_stale_latency_ms", 0) or 0))
        ttl_ms = max(0.0, float(getattr(config, "max_unhedged_duration_ms", 0) or 0))
        if ttl_ms <= 0:
            ttl_ms = max(0.0, float(getattr(config, "max_order_ttl_seconds", 0) or 0) * 1000)
        slot_limits = getattr(config, "max_open_spot_orders_per_exchange", {}) or {}
        max_open_orders = (
            sum(max(0, int(value or 0)) for value in slot_limits.values())
            if isinstance(slot_limits, Mapping) and slot_limits
            else 0
        )
        if max_open_orders <= 0:
            venues = getattr(config, "exchanges", {}) or {}
            max_open_orders = max(
                0, int(getattr(config, "max_slots_per_venue", 1) or 1)
                * max(1, len(venues) if isinstance(venues, Mapping) else 1)
            )
        max_drawdown = max(0.0, float(getattr(config, "max_drawdown_pct", 0.0) or 0.0))
        if max_drawdown <= 0 and capital > 0 and daily_loss > 0:
            max_drawdown = daily_loss / capital * 100.0
        gross_limit = min(
            [value for value in (capital * 2.0, max_exchange * 2.0) if value > 0],
            default=0.0,
        )
        pair_budget = max_position if max_position > 0 else capital * allocation
        return {
            "max_gross_exposure_usdt": gross_limit,
            "max_net_exposure_usdt": pair_budget,
            "max_deployed_capital_usdt": capital,
            "max_unhedged_notional_usdt": pair_budget,
            "max_unhedged_duration_ms": ttl_ms,
            "max_open_order_count": max_open_orders,
            "max_daily_loss_usdt": daily_loss,
            "max_drawdown_pct": max_drawdown,
            "max_slippage_bps": max_slippage * 10000.0,
            "max_funding_cost_pct": max(
                0.0, float(getattr(config, "max_funding_cost_pct", 0.0) or 0.0)
            ),
            "max_market_data_age_ms": max_stale,
            "min_venue_health_score": max(
                0.0, float(getattr(config, "min_venue_health_score", 0.0) or 0.0)
            ),
        }


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
        if action in {"HEDGE", "DERISK"} or not configured:
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
