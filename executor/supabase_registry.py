"""Non-secret runtime registry snapshots for Supabase operational tables.

These rows are audit metadata only. The executor never reads them to make
trading, sizing, routing, or risk decisions; Redis and exchange APIs remain
authoritative for live state.
"""
import math
import os
import socket
import uuid
from typing import Any, Callable, Dict, Optional

_NAMESPACE = uuid.UUID("8e5a1e7c-6e9d-4f31-9b9e-9f6f9b4f2a11")
_Emit = Callable[[str, Dict[str, Any], Optional[str]], None]


def _get(source: Any, name: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _finite(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive(value: Any) -> Optional[float]:
    number = _finite(value)
    return number if number is not None and number > 0 else None


def _global_quote(symbol: str) -> str:
    contract_symbol = symbol.split(":", 1)[0]
    if "/" not in contract_symbol:
        return ""
    return contract_symbol.rsplit("/", 1)[-1].strip().upper()


def _instance_id(explicit: Optional[str] = None) -> str:
    value = _text(explicit or os.getenv("EXECUTOR_INSTANCE_ID") or socket.gethostname())
    return value[:128] or "executor"


class SupabaseRegistry:
    """Writes sanitized executor/account/risk and actually-used pair metadata."""

    def __init__(self, emit: _Emit):
        self._emit = emit
        self._account_ids: Dict[tuple[str, str], str] = {}

    def runtime_configuration(self, config: Any, instance_id: Optional[str] = None) -> None:
        """Journal active venue metadata and the sanitized risk policy.

        API keys, secrets, passphrases, and arbitrary config dictionaries are
        deliberately never serialized.
        """
        self._account_ids.clear()
        self._account(
            _text(_get(config, "local_exchange_name")).lower(),
            "spot",
            _get(config, "local_credentials"),
        )
        self._account(
            _text(_get(config, "global_exchange_name")).lower(),
            "perpetual",
            _get(config, "global_credentials"),
        )
        self._risk_limits(config, _instance_id(instance_id))

    def _account(self, exchange: str, market_type: str, credentials: Any) -> None:
        if not exchange:
            return
        api_key = _text(_get(credentials, "api_key"))
        api_secret = _text(_get(credentials, "api_secret"))
        configured = bool(api_key and api_secret)
        testnet = bool(_get(credentials, "testnet", False))
        account_label = "default"
        account_id = str(
            uuid.uuid5(_NAMESPACE, f"exchange-account:{exchange}:{market_type}:{account_label}")
        )
        self._account_ids[(exchange, market_type)] = account_id
        self._emit(
            "exchange_accounts",
            {
                "id": account_id,
                "exchange_name": exchange,
                "market_type": market_type,
                "account_label": account_label,
                "is_testnet": testnet,
                "is_enabled": configured,
                "permissions": {},
                "metadata": {
                    "managed_by": "nafaz-executor",
                    "credential_configured": configured,
                },
            },
            "exchange_name,market_type,account_label",
        )

    def _risk_limits(self, config: Any, instance_id: str) -> None:
        name = f"executor-runtime:{instance_id}"
        parameters: Dict[str, Any] = {
            "source": "executor_runtime_snapshot",
            "control_plane": False,
            "mode": "MOCK" if bool(_get(config, "mock_mode", True)) else "LIVE",
        }
        for field in (
            "allocation_per_pair",
            "min_allocated_capital",
            "initial_exchange_balance_usdt",
            "balance_mismatch_fraction",
            "estimated_round_trip_fee_pct",
            "entry_revalidation_min_spread_pct",
            "max_order_ttl_seconds",
            "max_stale_latency_ms",
            "min_partial_fill_usd",
            "exit_price_move_threshold",
            "private_stream_enabled",
            "private_stream_max_age_seconds",
        ):
            value = _get(config, field)
            if isinstance(value, bool):
                parameters[field] = value
            elif (number := _finite(value)) is not None:
                parameters[field] = int(number) if field == "max_order_ttl_seconds" or field == "max_stale_latency_ms" else number

        row: Dict[str, Any] = {
            "id": str(uuid.uuid5(_NAMESPACE, f"risk-limits:{name}")),
            "name": name,
            "is_enabled": True,
            "max_daily_loss_usdt": _finite(_get(config, "max_daily_loss_usdt")),
            "max_exposure_per_exchange_usdt": _finite(
                _get(config, "max_exposure_per_exchange_usdt")
            ),
            "max_entry_slippage_pct": _finite(_get(config, "max_entry_slippage_pct")),
            "max_consecutive_order_failures": self._integer(
                _get(config, "max_consecutive_order_failures")
            ),
            "parameters": parameters,
        }
        max_position = _finite(_get(config, "max_position_usdt"))
        if max_position is not None:
            row["max_position_usdt"] = max_position
        self._emit("risk_limits", row, "name")

    @staticmethod
    def _integer(value: Any) -> Optional[int]:
        number = _finite(value)
        if number is None:
            return None
        return int(number)

    def market_pair(self, pair_key: str, pair_info: Any, *, is_mock: bool) -> bool:
        """Upsert only a pair that the executor actually receives for a position."""
        if pair_info is None:
            return False
        local_exchange = _text(_get(pair_info, "local_exchange")).lower()
        global_exchange = _text(_get(pair_info, "global_exchange")).lower()
        local_symbol = _text(_get(pair_info, "local_symbol"))
        global_symbol = _text(_get(pair_info, "global_symbol"))
        base_asset = _text(_get(pair_info, "base_asset")).upper()
        local_quote = _text(_get(pair_info, "quote_asset")).upper()
        global_quote = _global_quote(global_symbol)
        resolved_key = _text(pair_key)
        if not all((resolved_key, local_exchange, global_exchange, local_symbol,
                    global_symbol, base_asset, local_quote, global_quote)):
            return False

        row: Dict[str, Any] = {
            "id": str(uuid.uuid5(_NAMESPACE, f"market-pair:{resolved_key}")),
            "pair_key": resolved_key,
            "local_exchange": local_exchange,
            "global_exchange": global_exchange,
            "local_symbol": local_symbol,
            "global_symbol": global_symbol,
            "base_asset": base_asset,
            "local_quote_asset": local_quote,
            "global_quote_asset": global_quote,
            "local_exchange_account_id": self._account_ids.get((local_exchange, "spot")),
            "global_exchange_account_id": self._account_ids.get((global_exchange, "perpetual")),
            "is_enabled": True,
            "metadata": {
                "source": "executor_position_opened",
                "is_mock": bool(is_mock),
            },
        }
        price_tick = _positive(_get(pair_info, "local_price_tick"))
        lot_size = _positive(_get(pair_info, "local_lot_size"))
        if price_tick is not None:
            row["price_tick"] = price_tick
        if lot_size is not None:
            row["lot_size"] = lot_size
        self._emit("market_pairs", row, "pair_key")
        return True
