"""ExecutorConfig sanitation, modular exchange config, scanner-validated execution directives."""

import asyncio
import json
import os

from executor.models import ExecutionDirectives

import fakeredis.aioredis as fake_aioredis
import pytest

from executor.config import ExecutorConfig

def test_p3_04_sanitize_clamps_out_of_range_values():
    cfg = ExecutorConfig(
        allocation_per_pair=1.5,
        max_stale_latency_ms=-5,
        max_entry_slippage_pct=2.0,
    )
    warnings = cfg.sanitize()

    assert cfg.allocation_per_pair == 1.0
    assert cfg.max_stale_latency_ms == 1
    assert cfg.max_entry_slippage_pct == 1.0
    assert any("allocation_per_pair" in w for w in warnings)
    assert any("max_stale_latency_ms" in w for w in warnings)
    assert any("max_entry_slippage_pct" in w for w in warnings)

def test_p3_04_sanitize_replaces_nan_and_garbage():
    cfg = ExecutorConfig(max_daily_loss_usdt=float("nan"))
    cfg.sanitize()
    assert cfg.max_daily_loss_usdt == 50.0

    cfg = ExecutorConfig()
    cfg.exit_price_move_threshold = "not-a-number"
    cfg.sanitize()
    assert cfg.exit_price_move_threshold == 0.0015

def test_p3_04_sanitize_keeps_valid_defaults_quiet():
    assert ExecutorConfig().sanitize() == []

def test_p3_04_sanitize_announces_disabled_safety_nets():
    cfg = ExecutorConfig(estimated_round_trip_fee_pct=0.0, max_entry_slippage_pct=0.0)
    warnings = cfg.sanitize()

    assert any("fee buffer" in w for w in warnings)
    assert any("slippage" in w for w in warnings)
    # 0 is a legal "disabled" value, so it must survive sanitize untouched.
    assert cfg.estimated_round_trip_fee_pct == 0.0
    assert cfg.max_entry_slippage_pct == 0.0

@pytest.mark.asyncio
async def test_modular_exchanges_config_and_env(monkeypatch):
    """Verify modular exchanges loaded from file and dynamic env vars."""
    import os
    cfg = ExecutorConfig()
    cfg.local_exchange_name = "indodax"
    cfg.global_exchange_name = "okx"

    # Test modular exchanges dictionary via _apply_file logic
    import tempfile
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f:
        json.dump({
            "exchanges": {
                "okx": {
                    "api_key": "okx_key_1",
                    "api_secret": "okx_secret_1",
                    "passphrase": "pass_1",
                    "testnet": True,
                    "options": {"defaultType": "swap"}
                },
                "bybit": {
                    "api_key": "bybit_key_1",
                    "api_secret": "bybit_secret_1"
                }
            }
        }, f)
        f_path = f.name

    try:
        cfg._apply_file(f_path)
        assert "okx" in cfg.exchanges
        assert cfg.exchanges["okx"].api_key == "okx_key_1"
        assert cfg.exchanges["okx"].options.get("defaultType") == "swap"
        assert "bybit" in cfg.exchanges
        assert cfg.exchanges["bybit"].api_key == "bybit_key_1"

        # Test dynamic env overrides: e.g. BINANCE_API_KEY
        monkeypatch.setenv("BINANCE_API_KEY", "binance_key_env")
        monkeypatch.setenv("BINANCE_API_SECRET", "binance_sec_env")
        cfg._apply_env()
        assert "binance" in cfg.exchanges
        assert cfg.exchanges["binance"].api_key == "binance_key_env"
        assert cfg.exchanges["binance"].api_secret == "binance_sec_env"

        # Binance Cloud broker id (Tokocrypto) comes from the environment too and
        # must reach the ccxt client options that prefix client order ids.
        monkeypatch.setenv("TOKOCRYPTO_API_KEY", "toko_key_env")
        monkeypatch.setenv("TOKOCRYPTO_API_SECRET", "toko_sec_env")
        monkeypatch.setenv("TOKOCRYPTO_BROKER_ID", "x-TKT5PX2F")
        cfg._apply_env()
        assert cfg.exchanges["tokocrypto"].options["broker_id"] == "x-TKT5PX2F"

        # Verify global credentials synced from okx
        assert cfg.global_credentials.api_key == "okx_key_1"
        assert cfg.global_credentials.passphrase == "pass_1"
        assert cfg.global_credentials.testnet is True

        # Test Supabase config from file
        cfg_sup = ExecutorConfig()
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".json") as f2:
            json.dump({
                "supabase": {
                    "enabled": True,
                    "url": "https://xyz.supabase.co",
                    "key": "secret_key_123"
                }
            }, f2)
            f2_path = f2.name
        try:
            cfg_sup._apply_file(f2_path)
            assert cfg_sup.supabase.enabled is True
            assert cfg_sup.supabase.url == "https://xyz.supabase.co"
            assert cfg_sup.supabase.key == "secret_key_123"
        finally:
            if os.path.exists(f2_path):
                os.remove(f2_path)
    finally:
        os.environ.pop("BINANCE_API_KEY", None)
        os.environ.pop("BINANCE_API_SECRET", None)
        os.environ.pop("TOKOCRYPTO_BROKER_ID", None)
        os.environ.pop("TOKOCRYPTO_API_KEY", None)
        os.environ.pop("TOKOCRYPTO_API_SECRET", None)
        if os.path.exists(f_path):
            os.remove(f_path)


def test_postgres_dsn_is_loaded_from_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://executor:secret@db.example:5432/nafaz")
    cfg = ExecutorConfig()
    cfg._apply_env()
    assert cfg.postgres_dsn == "postgresql://executor:secret@db.example:5432/nafaz"

def test_execution_directives_scanner_validated_defaults_false():
    """Old payloads without scanner_validated must not be trusted as validated."""
    d = ExecutionDirectives.from_dict({"action": "PLACE_MAKER_BUY"})
    assert d.scanner_validated is False


def test_executor_mode_follows_root_scanner_mode_and_allows_explicit_override(monkeypatch, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"mode": "LIVE"}), encoding="utf-8")

    cfg = ExecutorConfig()
    cfg._apply_file(str(config_path))
    assert cfg.mock_mode is False

    monkeypatch.setenv("EXECUTOR_MOCK_MODE", "true")
    cfg._apply_env()
    assert cfg.mock_mode is True

    monkeypatch.setenv("EXECUTOR_MOCK_MODE", "false")
    cfg._apply_env()
    assert cfg.mock_mode is False


def test_live_mode_rejects_missing_exchange_credentials():
    cfg = ExecutorConfig(mock_mode=False)
    cfg.local_exchange_name = "indodax"
    cfg.global_exchange_name = "okx"

    with pytest.raises(ValueError, match="credentials are incomplete"):
        cfg.validate_live_credentials()


def test_live_mode_accepts_complete_indodax_okx_credentials():
    from executor.config import ExchangeCredentials

    cfg = ExecutorConfig(mock_mode=False)
    cfg.local_exchange_name = "indodax"
    cfg.global_exchange_name = "okx"
    cfg.local_credentials = ExchangeCredentials(api_key="local-key", api_secret="local-secret")
    cfg.global_credentials = ExchangeCredentials(
        api_key="global-key", api_secret="global-secret", passphrase="okx-passphrase"
    )
    cfg.validate_live_credentials()


def test_executor_defaults_use_per_venue_failure_threshold_and_maker_ttl():
    cfg = ExecutorConfig()
    assert cfg.max_consecutive_order_failures == 50
    assert cfg.max_order_ttl_seconds == 120
    assert cfg.maker_refresh_after_seconds == 8
    assert cfg.maker_refresh_cooldown_seconds == 12
    assert cfg.indodax_maker_refresh_after_seconds == 15
    assert cfg.indodax_maker_refresh_cooldown_seconds == 15
    assert cfg.adaptive_maker_min_net_profit_pct == 0.10

    cfg.sanitize()
    assert cfg.max_consecutive_order_failures == 50
    assert cfg.max_order_ttl_seconds == 120
    assert cfg.maker_refresh_after_seconds == 8
    assert cfg.maker_refresh_cooldown_seconds == 12
    assert cfg.indodax_maker_refresh_after_seconds == 15
    assert cfg.indodax_maker_refresh_cooldown_seconds == 15



def test_funding_decay_retention_ratio_default_and_sanitization():
    cfg = ExecutorConfig()
    assert cfg.funding_decay_retention_ratio == pytest.approx(0.60)
    assert cfg.sanitize() == []
    assert cfg.funding_decay_retention_ratio == pytest.approx(0.60)

    cfg.funding_decay_retention_ratio = 1.5
    warnings = cfg.sanitize()
    assert cfg.funding_decay_retention_ratio == 1.0
    assert any("funding_decay_retention_ratio" in warning for warning in warnings)

    cfg.funding_decay_retention_ratio = -0.1
    warnings = cfg.sanitize()
    assert cfg.funding_decay_retention_ratio == pytest.approx(0.60)
    assert any("funding_decay_retention_ratio" in warning for warning in warnings)


@pytest.mark.asyncio
async def test_dynamic_config_sync_and_entry_sizing(mock_client):
    """Verify live reload from Redis config update and sizing sync."""
    from executor.engine import ArbitrageEngine
    from executor.models import ExecutorPayload
    from tests.conftest import _fresh_signal_payload
    import time

    cfg = ExecutorConfig(
        mock_mode=True,
        capital_total=100.0,
        allocation_per_pair=0.5,
        min_allocated_capital=10.0,
        max_position_usdt=50.0,
    )
    engine = ArbitrageEngine(config=cfg, client=mock_client)

    # Initial state
    assert engine.config.max_position_usdt == 50.0

    # Apply dynamic config from Redis arb:config snapshot
    dyn_data = {
        "capital_total": 200.0,
        "allocation_per_pair": 0.25,
        "min_allocated_capital": 15.0,
        "mode": "MOCK",
        "execution_mode": "MAKER_TAKER",
        "executor": {
            "allocation_per_pair": 0.25,
            "min_allocated_capital": 15.0,
            "max_position_usdt": 50.0,
        },
    }
    engine._apply_dynamic_config_dict(dyn_data)

    assert engine.config.capital_total == 200.0
    assert engine.config.allocation_per_pair == 0.25
    assert engine.config.min_allocated_capital == 15.0
    assert engine.config.max_position_usdt == 50.0
    assert engine.config.execution_mode == "MAKER_TAKER"
    assert engine.config.mock_mode is True

    # Test _compute_entry_sizing with dynamic cap
    raw_payload = _fresh_signal_payload(int(time.time() * 1000))
    raw_payload["execution_directives"]["max_position_usdt"] = 100.0
    payload = ExecutorPayload.from_dict(raw_payload)

    engine._balance_snapshots["indodax:btc_idr"] = {
        "spot_available": 100.0,
        "futures_available": 100.0,
    }
    sized, min_c = await engine._compute_entry_sizing("indodax:btc_idr", payload)
    assert min_c == 15.0
    # Sized must be capped at min(directives(100), max_pos_limit(50), usable(100)) - fees
    assert sized <= 50.0
    assert sized >= 15.0

