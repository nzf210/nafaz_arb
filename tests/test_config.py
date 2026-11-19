"""ExecutorConfig sanitation, modular exchange config, scanner-validated execution directives."""

import asyncio
import json
import os
import time

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

def test_hedging_defaults_match_spec():
    cfg = ExecutorConfig()
    hed = cfg.hedging
    assert hed.enabled is True
    assert hed.mode == "incremental"
    assert hed.spot_fill_trigger_pct == 20.0
    assert hed.futures_target_hedge_ratio == 1.0
    assert hed.futures_min_order_policy == "wait_until_covered"
    assert hed.futures_allow_min_notional_overhedge is False
    assert hed.futures_max_overhedge_pct == 0.0
    assert hed.execution_prevent_duplicate_hedges is True
    assert hed.execution_durable_hedge_intent is True
    assert cfg.sanitize() == []

def test_hedging_sanitize_clamps_and_normalizes():
    cfg = ExecutorConfig()
    cfg.hedging.spot_fill_trigger_pct = 150.0
    cfg.hedging.futures_target_hedge_ratio = -1.0
    cfg.hedging.futures_min_order_policy = "yolo"
    cfg.hedging.mode = "instant"
    warnings = cfg.hedging.sanitize()

    assert cfg.hedging.spot_fill_trigger_pct == 100.0
    assert cfg.hedging.futures_target_hedge_ratio == 0.0
    assert cfg.hedging.futures_min_order_policy == "wait_until_covered"
    assert cfg.hedging.mode == "incremental"
    assert len(warnings) == 4
    # Executor-level sanitize must propagate hedging warnings.
    assert ExecutorConfig().sanitize() == []
    propagated = ExecutorConfig()
    propagated.hedging.spot_fill_trigger_pct = "garbage"
    assert any("hedging.spot_fill_trigger_pct" in w for w in propagated.sanitize())

def test_hedging_section_loaded_from_file(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "hedging": {
            "enabled": False,
            "mode": "incremental",
            "spot": {"fill_trigger_pct": 35.5},
            "futures": {
                "target_hedge_ratio": 1.5,
                "min_order_policy": "allow_min_notional",
                "allow_min_notional_overhedge": True,
                "max_overhedge_pct": 2.0,
            },
            "execution": {
                "hedge_on_partial_fill": False,
                "prevent_duplicate_hedges": False,
                "durable_hedge_intent": False,
            },
        }
    }))
    cfg = ExecutorConfig()
    cfg._apply_file(str(config_path))

    hed = cfg.hedging
    assert hed.enabled is False
    assert hed.spot_fill_trigger_pct == 35.5
    assert hed.futures_target_hedge_ratio == 1.5
    assert hed.futures_min_order_policy == "allow_min_notional"
    assert hed.futures_allow_min_notional_overhedge is True
    assert hed.futures_max_overhedge_pct == 2.0
    assert hed.execution_hedge_on_partial_fill is False
    assert hed.execution_prevent_duplicate_hedges is False
    assert hed.execution_durable_hedge_intent is False
    # Untouched execution flags keep their defaults.
    assert hed.execution_use_cumulative_spot_fills is True

def test_hedging_malformed_sections_fail_safe(tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({
        "hedging": {
            "spot": "not-a-dict",
            "futures": None,
            "execution": [],
            "fill_trigger_pct": 42.0,
        }
    }))
    cfg = ExecutorConfig()
    cfg._apply_file(str(config_path))
    cfg.sanitize()

    assert cfg.hedging.enabled is True
    assert cfg.hedging.spot_fill_trigger_pct == 20.0
    assert cfg.hedging.futures_min_order_policy == "wait_until_covered"

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


@pytest.mark.asyncio
async def test_dynamic_config_sync_root_and_executor_subdict(mock_client):
    """Verify both root and executor sub-dictionary are kept in sync and stale nested keys don't overwrite."""
    from executor.engine import ArbitrageEngine

    cfg = ExecutorConfig(
        capital_total=100.0,
        allocation_per_pair=0.5,
        min_allocated_capital=10.0,
        max_position_usdt=50.0,
    )
    engine = ArbitrageEngine(config=cfg, client=mock_client)

    # 1. Update capital_total at root
    engine._apply_dynamic_config_dict({
        "capital_total": 400.0,
        "allocation_per_pair": 0.5,
        "executor": {
            "max_position_usdt": 50.0,  # Old stale nested value
        },
    })
    # Since capital_total was updated, max_position_usdt should recompute to cap * alloc = 200.0
    assert engine.config.capital_total == 400.0
    assert engine.config.max_position_usdt == 200.0
    assert engine.config["executor"]["max_position_usdt"] == 200.0
    assert engine.config["executor"]["allocation_per_pair"] == 0.5

    # 2. Update via single param
    engine._apply_dynamic_config_dict({"param": "capital", "value": 600.0})
    assert engine.config.capital_total == 600.0
    assert engine.config.max_position_usdt == 300.0
    assert engine.config["executor"]["max_position_usdt"] == 300.0


@pytest.mark.asyncio
async def test_market_snapshot_fallback_for_dynamic_exit(mock_client):
    """Verify _fetch_live_price and _exit_live_snapshot fallback to arb:market_snapshot:<pair_tag>."""
    import fakeredis.aioredis as fake_aioredis
    from executor.engine import ArbitrageEngine
    from executor.models import ActivePosition

    fake_redis = fake_aioredis.FakeRedis()
    cfg = ExecutorConfig()
    engine = ArbitrageEngine(config=cfg, client=mock_client, redis_client=fake_redis)

    pair_tag = "tokocrypto:btc_usdt__binance:btcusdt"
    snap = {
        "pair_tag": pair_tag,
        "spot_venue": "tokocrypto",
        "futures_venue": "binance",
        "spot_best_bid": 65000.0,
        "spot_best_ask": 65010.0,
        "spot_bid_qty": 1.0,
        "spot_ask_qty": 2.0,
        "futures_best_bid": 65100.0,
        "futures_best_ask": 65105.0,
        "usd_idr_rate": 16000.0,
        "updated_at_ms": int(time.time() * 1000),
    }
    await fake_redis.set(f"arb:market_snapshot:{pair_tag}", json.dumps(snap))

    # Test _fetch_live_price
    live_price = await engine._fetch_live_price(pair_tag)
    assert live_price is not None
    ask, rate = live_price
    assert ask == 65105.0
    assert rate == 16000.0

    # Test _exit_live_snapshot
    pos = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="binance",
        symbol="btc_usdt",
        qty=1.0,
        entry_price_idr=65000.0 * 16000.0,
        short_entry_usdt=65100.0,
    )
    snapshot, local_bid, local_ask = await engine._exit_live_snapshot(pair_tag, pos)
    assert snapshot is not None
    assert local_bid == 65000.0 * 16000.0
    assert local_ask == 65010.0 * 16000.0




def test_execution_modules_are_an_explicit_allowlist_and_normalize_bidr():
    from executor.config import ExecutionModuleConfig

    cfg = ExecutorConfig()
    disabled = ExecutionModuleConfig(
        id="spot_idr",
        quote="IDR",
        spot_venues=["indodax"],
        enabled=False,
    )
    cfg.modules = {"spot_idr": disabled}

    assert cfg.module_for("INDODAX", "BIDR") is disabled
    assert cfg.module_for("tokocrypto", "USDT") is None
