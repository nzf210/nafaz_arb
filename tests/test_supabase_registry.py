"""Regression tests for sanitized Supabase runtime registry persistence."""
import json
from types import SimpleNamespace

from executor.models import ActivePosition, PairInfo
from executor.supabase import SupabaseWriter


def _config():
    return SimpleNamespace(
        enabled=True,
        url="https://example.supabase.co",
        key="supabase-test-key",
        outbox_path=":memory:",
        local_exchange_name="tokocrypto",
        global_exchange_name="mexc",
        local_credentials=SimpleNamespace(
            api_key="local-secret-key",
            api_secret="local-secret-value",
            testnet=False,
        ),
        global_credentials=SimpleNamespace(
            api_key="global-secret-key",
            api_secret="global-secret-value",
            passphrase="global-passphrase",
            testnet=True,
        ),
        mock_mode=True,
        max_daily_loss_usdt=50.0,
        max_exposure_per_exchange_usdt=250.0,
        max_entry_slippage_pct=0.003,
        max_consecutive_order_failures=50,
        allocation_per_pair=0.25,
        min_allocated_capital=10.0,
        initial_exchange_balance_usdt=100.0,
        balance_mismatch_fraction=0.2,
        estimated_round_trip_fee_pct=0.001,
        entry_revalidation_min_spread_pct=0.0,
        max_order_ttl_seconds=120,
        max_stale_latency_ms=500,
        min_partial_fill_usd=10.0,
        exit_price_move_threshold=0.0015,
        private_stream_enabled=False,
        private_stream_max_age_seconds=5.0,
    )


def _rows(writer):
    pending = writer._outbox.pending(1000)
    writer._outbox.ack([item[0] for item in pending])
    return [(table, conflict, row) for _, table, conflict, row in pending]


def test_runtime_configuration_persists_only_non_secret_account_metadata():
    writer = SupabaseWriter(_config())
    writer.runtime_configuration(_config(), instance_id="executor-test")

    rows = _rows(writer)
    accounts = [row for table, _, row in rows if table == "exchange_accounts"]
    assert {(row["exchange_name"], row["market_type"]) for row in accounts} == {
        ("tokocrypto", "spot"),
        ("mexc", "perpetual"),
    }
    assert all(row["is_enabled"] for row in accounts)
    assert next(row for row in accounts if row["exchange_name"] == "mexc")["is_testnet"] is True
    assert all("api_key_ciphertext" not in row and "api_secret_ciphertext" not in row for row in accounts)

    serialized = json.dumps(rows)
    for secret in (
        "local-secret-key", "local-secret-value",
        "global-secret-key", "global-secret-value", "global-passphrase",
    ):
        assert secret not in serialized

    risk = next(row for table, _, row in rows if table == "risk_limits")
    assert risk["name"] == "executor-runtime:executor-test"
    assert risk["max_daily_loss_usdt"] == 50.0
    assert risk["max_exposure_per_exchange_usdt"] == 250.0
    assert risk["max_entry_slippage_pct"] == 0.003
    assert risk["max_consecutive_order_failures"] == 50
    assert risk["parameters"]["control_plane"] is False
    assert risk["parameters"]["mode"] == "MOCK"
    assert "api_key" not in json.dumps(risk)


def test_runtime_registry_ids_and_upsert_targets_are_stable():
    config = _config()
    first = SupabaseWriter(config)
    first.runtime_configuration(config, instance_id="stable")
    first_rows = _rows(first)

    restarted = SupabaseWriter(config)
    restarted.runtime_configuration(config, instance_id="stable")
    restarted_rows = _rows(restarted)

    first_ids = {(table, row.get("exchange_name", row.get("name"))): row["id"]
                 for table, _, row in first_rows}
    restarted_ids = {(table, row.get("exchange_name", row.get("name"))): row["id"]
                     for table, _, row in restarted_rows}
    assert first_ids == restarted_ids
    assert {conflict for table, conflict, _ in first_rows if table == "exchange_accounts"} == {
        "exchange_name,market_type,account_label"
    }
    assert [conflict for table, conflict, _ in first_rows if table == "risk_limits"] == ["name"]


def test_market_pair_is_persisted_with_exact_spot_and_perpetual_quotes():
    config = _config()
    writer = SupabaseWriter(config)
    writer.runtime_configuration(config, instance_id="pair-test")
    pair = PairInfo(
        local_exchange="tokocrypto",
        global_exchange="mexc",
        local_symbol="btc_idr",
        global_symbol="BTC/USDC:USDC",
        base_asset="BTC",
        quote_asset="IDR",
        local_price_tick=1000.0,
        local_lot_size=0.0001,
    )
    position = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="mexc",
        symbol="btc_idr",
        entry_price_idr=1_000_000_000.0,
        short_entry_usdt=65000.0,
        qty=0.001,
        opened_at_ms=1_700_000_000_000,
    )

    writer.position_opened("tokocrypto:btc_idr->mexc:BTC/USDC:USDC", position, pair, is_mock=False)
    rows = _rows(writer)
    market = next(row for table, _, row in rows if table == "market_pairs")

    assert market["local_exchange"] == "tokocrypto"
    assert market["global_exchange"] == "mexc"
    assert market["base_asset"] == "BTC"
    assert market["local_quote_asset"] == "IDR"
    assert market["global_quote_asset"] == "USDC"
    assert market["price_tick"] == 1000.0
    assert market["lot_size"] == 0.0001
    assert market["metadata"]["is_mock"] is False
    assert market["local_exchange_account_id"]
    assert market["global_exchange_account_id"]


def test_market_pair_requires_complete_symbols_and_is_idempotent():
    config = _config()
    writer = SupabaseWriter(config)
    writer.runtime_configuration(config, instance_id="pair-idempotency")
    pair = PairInfo(
        local_exchange="tokocrypto",
        global_exchange="mexc",
        local_symbol="btc_idr",
        global_symbol="BTC/USDT:USDT",
        base_asset="BTC",
        quote_asset="IDR",
    )
    position = ActivePosition(
        local_exchange="tokocrypto",
        global_exchange="mexc",
        symbol="btc_idr",
        entry_price_idr=1.0,
        short_entry_usdt=1.0,
        qty=1.0,
        opened_at_ms=1_700_000_000_000,
    )
    pair_key = "tokocrypto:btc_idr->mexc:BTC/USDT:USDT"
    writer.position_opened(pair_key, position, pair)
    first = [row for table, _, row in _rows(writer) if table == "market_pairs"][0]
    writer.position_opened(pair_key, position, pair)
    second = [row for table, _, row in _rows(writer) if table == "market_pairs"][0]
    assert first["id"] == second["id"]
    assert first["pair_key"] == second["pair_key"]
    assert second["global_quote_asset"] == "USDT"

    invalid = PairInfo(
        local_exchange="tokocrypto",
        global_exchange="mexc",
        local_symbol="btc_idr",
        global_symbol="BTCUSDT",
        base_asset="BTC",
        quote_asset="IDR",
    )
    writer.position_opened("invalid-pair", position, invalid)
    assert not [row for table, _, row in _rows(writer) if table == "market_pairs"]
