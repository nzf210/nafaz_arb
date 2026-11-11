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
async def test_modular_exchanges_config_and_env():
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
        os.environ["BINANCE_API_KEY"] = "binance_key_env"
        os.environ["BINANCE_API_SECRET"] = "binance_sec_env"
        cfg._apply_env()
        assert "binance" in cfg.exchanges
        assert cfg.exchanges["binance"].api_key == "binance_key_env"
        assert cfg.exchanges["binance"].api_secret == "binance_sec_env"

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
        if os.path.exists(f_path):
            os.remove(f_path)

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
    assert cfg.maker_refresh_after_seconds == 45
    assert cfg.maker_refresh_cooldown_seconds == 300

    cfg.sanitize()
    assert cfg.max_consecutive_order_failures == 50
    assert cfg.max_order_ttl_seconds == 120
