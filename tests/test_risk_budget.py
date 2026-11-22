import json
from types import SimpleNamespace

import pytest

from executor.config import ExecutorConfig

from executor.risk_control import RiskControlPlane, RiskPermissionDenied
from executor.risk_budget import (
    RiskBudgetEvaluator,
    RiskBudgetMetricsBuilder,
    RiskBudgetViolation,
)


def _metrics(**overrides):
    values = {
        "gross_exposure_usdt": 100.0,
        "net_exposure_usdt": 20.0,
        "deployed_capital_usdt": 80.0,
        "unhedged_notional_usdt": 10.0,
        "unhedged_duration_ms": 1000,
        "open_order_count": 2,
        "daily_loss_usdt": 5.0,
        "drawdown_pct": 0.5,
        "slippage_bps": 10.0,
        "funding_cost_pct": 0.01,
        "market_data_age_ms": 100.0,
        "venue_health_score": 0.9,
    }
    values.update(overrides)
    return values


def _limits(**overrides):
    values = {
        "max_gross_exposure_usdt": 200.0,
        "max_net_exposure_usdt": 50.0,
        "max_deployed_capital_usdt": 150.0,
        "max_unhedged_notional_usdt": 25.0,
        "max_unhedged_duration_ms": 5000,
        "max_open_order_count": 4,
        "max_daily_loss_usdt": 20.0,
        "max_drawdown_pct": 2.0,
        "max_slippage_bps": 30.0,
        "max_funding_cost_pct": 0.05,
        "max_market_data_age_ms": 1000.0,
        "min_venue_health_score": 0.5,
    }
    values.update(overrides)
    return values


def test_risk_budget_accepts_metrics_inside_all_configured_limits():
    result = RiskBudgetEvaluator.evaluate(
        metrics=_metrics(), limits=_limits(), action_class="ENTRY"
    )
    assert result["violations"] == []
    assert result["metrics"]["gross_exposure_usdt"] == 100.0


@pytest.mark.parametrize(
    ("metric", "value"),
    [
        ("gross_exposure_usdt", 201.0),
        ("net_exposure_usdt", 51.0),
        ("deployed_capital_usdt", 151.0),
        ("unhedged_notional_usdt", 26.0),
        ("unhedged_duration_ms", 5001),
        ("open_order_count", 5),
        ("daily_loss_usdt", 21.0),
        ("drawdown_pct", 2.1),
        ("slippage_bps", 31.0),
        ("funding_cost_pct", 0.051),
        ("market_data_age_ms", 1001.0),
        ("venue_health_score", 0.49),
    ],
)
def test_risk_budget_rejects_each_exceeded_limit(metric, value):
    with pytest.raises(RiskBudgetViolation):
        RiskBudgetEvaluator.evaluate(
            metrics=_metrics(**{metric: value}),
            limits=_limits(),
            action_class="ENTRY",
        )


def test_configured_budget_fails_closed_when_required_metric_is_missing():
    metrics = _metrics()
    metrics["market_data_age_ms"] = None
    with pytest.raises(RiskBudgetViolation, match="UNAVAILABLE"):
        RiskBudgetEvaluator.evaluate(
            metrics=metrics,
            limits={"max_market_data_age_ms": 1000},
            action_class="ENTRY",
        )


@pytest.mark.parametrize("action", ["HEDGE", "DERISK"])
def test_protective_mutations_bypass_entry_budgets(action):
    result = RiskBudgetEvaluator.evaluate(
        metrics=_metrics(gross_exposure_usdt=1e9),
        limits=_limits(max_gross_exposure_usdt=1),
        action_class=action,
    )
    assert result["violations"] == []


def test_metrics_builder_projects_entry_risk_and_pending_orders():
    now_ms = 1_900_000_000_000
    position = SimpleNamespace(
        spot_cost_usdt=100.0,
        short_entry_usdt=100.0,
        futures_margin_usdt=10.0,
        net_unrealized_pnl_usdt=-5.0,
        unhedged_exit_base_qty=1.0,
        current_perp_ask_usdt=100.0,
        opened_at_ms=now_ms - 5000,
        state="HEDGED_ACTIVE",
    )
    engine = SimpleNamespace(
        active_positions={"pair-a": position},
        pending_entry_orders={"pair-b": {"notional_usdt": 20.0}},
        pending_exit_orders={"pair-c": {"notional_usdt": 15.0}},
        pending_hedge_orders={},
        _daily_realized_loss_usdt=5.0,
        _exchange_halted={},
        _consecutive_order_failures={"okx": 5},
        config=SimpleNamespace(
            capital_total=1000.0,
            allocation_per_pair=0.5,
            max_position_usdt=200.0,
            max_exposure_per_exchange_usdt=0.0,
            max_daily_loss_usdt=50.0,
            max_entry_slippage_pct=0.003,
            max_stale_latency_ms=2500,
            max_order_ttl_seconds=120,
            max_unhedged_duration_ms=0,
            max_open_spot_orders_per_exchange={"indodax": 2, "tokocrypto": 2},
            max_slots_per_venue=1,
            exchanges={"indodax": object(), "tokocrypto": object()},
            max_drawdown_pct=0.0,
            max_funding_cost_pct=0.0,
            min_venue_health_score=0.2,
            max_consecutive_order_failures=50,
            futures_leverage=2.0,
        ),
    )

    metrics = RiskBudgetMetricsBuilder.build(
        engine,
        venue="okx",
        action_class="ENTRY",
        proposed_notional_usdt=50.0,
        planned_hedge_notional_usdt=50.0,
        slippage_bps=12.0,
        funding_cost_pct=0.01,
        market_data_age_ms=250.0,
    )
    assert metrics["gross_exposure_usdt"] == 320.0
    assert metrics["deployed_capital_usdt"] == 205.0
    assert metrics["net_exposure_usdt"] == 20.0
    assert metrics["unhedged_notional_usdt"] == 170.0
    assert metrics["open_order_count"] == 3
    assert metrics["daily_loss_usdt"] == 5.0
    assert metrics["venue_health_score"] == 0.9


def test_config_limits_derive_safe_budgets_from_existing_executor_settings():
    config = SimpleNamespace(
        capital_total=1000.0,
        allocation_per_pair=0.5,
        max_position_usdt=200.0,
        max_exposure_per_exchange_usdt=300.0,
        max_daily_loss_usdt=25.0,
        max_entry_slippage_pct=0.003,
        max_stale_latency_ms=2500,
        max_order_ttl_seconds=120,
        max_unhedged_duration_ms=0,
        max_open_spot_orders_per_exchange={"indodax": 2, "tokocrypto": 1},
        max_slots_per_venue=1,
        exchanges={"indodax": object(), "tokocrypto": object()},
        max_drawdown_pct=0.0,
        max_funding_cost_pct=0.05,
        min_venue_health_score=0.2,
    )
    limits = RiskBudgetMetricsBuilder.limits_from_config(config)
    assert limits["max_gross_exposure_usdt"] == 600.0
    assert limits["max_deployed_capital_usdt"] == 1000.0
    assert limits["max_unhedged_notional_usdt"] == 200.0
    assert limits["max_unhedged_duration_ms"] == 120000.0
    assert limits["max_open_order_count"] == 3
    assert limits["max_drawdown_pct"] == 2.5
    assert limits["max_slippage_bps"] == 30.0
    assert limits["max_market_data_age_ms"] == 2500.0

def test_new_quantitative_risk_budget_fields_load_from_executor_config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({
        "executor": {
            "max_drawdown_pct": 4.5,
            "max_funding_cost_pct": 0.08,
            "max_unhedged_duration_ms": 90000,
            "min_venue_health_score": 0.35,
        }
    }), encoding="utf-8")
    config = ExecutorConfig()
    config._apply_file(str(path))

    assert config.max_drawdown_pct == 4.5
    assert config.max_funding_cost_pct == 0.08
    assert config.max_unhedged_duration_ms == 90000
    assert config.min_venue_health_score == 0.35

@pytest.mark.asyncio
async def test_durable_scope_limits_override_config_budget_defaults():
    class _Connection:
        async def fetch(self, query, scope_keys):
            return [{
                "scope_key": "global",
                "entry_blocked": False,
                "derisk_allowed": True,
                "version": 7,
                "limits": {"max_gross_exposure_usdt": 50.0},
            }]

    with pytest.raises(RiskPermissionDenied, match="RISK_BUDGET_BLOCKED"):
        await RiskControlPlane.assert_allowed(
            _Connection(),
            scope_keys=["global"],
            action_class="ENTRY",
            risk_metrics=_metrics(gross_exposure_usdt=100.0),
            risk_limits=_limits(max_gross_exposure_usdt=200.0),
        )


@pytest.mark.asyncio
async def test_durable_derisk_permission_is_separate_from_entry_block():
    class _Connection:
        async def fetch(self, query, scope_keys):
            return [{
                "scope_key": "global",
                "entry_blocked": True,
                "derisk_allowed": True,
                "version": 8,
                "limits": {"max_gross_exposure_usdt": 1.0},
            }]

    snapshot = await RiskControlPlane.assert_allowed(
        _Connection(),
        scope_keys=["global"],
        action_class="DERISK",
        risk_metrics=_metrics(gross_exposure_usdt=1e9),
        risk_limits=_limits(),
    )
    assert snapshot["action_class"] == "DERISK"
