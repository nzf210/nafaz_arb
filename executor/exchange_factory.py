"""Exchange construction and venue-specific adapter selection."""

from typing import Any, Dict

import ccxt.async_support as ccxt

from executor.exchange_client import ExchangeClient
from executor.indodax_v2 import IndodaxTAPIv2
from executor.paper_exchange_client import PaperTradingExchangeClient

def create_exchange_client(config: Any) -> Any:
    """Factory creating appropriate client (CCXT live or Paper Trading mock)."""
    if getattr(config, "mock_mode", False):
        return PaperTradingExchangeClient(
            local_name=config.local_exchange_name,
            global_name=config.global_exchange_name,
            initial_balance_usdt=getattr(config, "initial_exchange_balance_usdt", 100.0),
        )
    spot_names = list(getattr(config, "spot_exchanges", []) or [config.local_exchange_name])
    perp_names = list(getattr(config, "perpetual_exchanges", []) or [config.global_exchange_name])
    credentials = getattr(config, "exchanges", {}) or {}
    spot_clients = {}
    perpetual_clients = {}
    for name in sorted(set(str(x).lower() for x in spot_names)):
        creds = credentials.get(name)
        if creds is None and name == str(config.local_exchange_name).lower():
            creds = config.local_credentials
        if creds is None or not creds.api_key or not creds.api_secret:
            raise ValueError(f"LIVE spot route {name} has no complete API credentials")
        spot_clients[name] = init_local_exchange(name, creds, default_type="spot")
    for name in sorted(set(str(x).lower() for x in perp_names)):
        creds = credentials.get(name)
        if creds is None and name == str(config.global_exchange_name).lower():
            creds = config.global_credentials
        if creds is None or not creds.api_key or not creds.api_secret:
            raise ValueError(f"LIVE perpetual route {name} has no complete API credentials")
        if name == "okx" and not creds.passphrase:
            raise ValueError("LIVE perpetual route okx requires API passphrase")
        # CCXT exposes perpetual markets under different default types:
        # Binance USD-M uses "future"; OKX, MEXC and Bybit use "swap".
        default_type = "future" if name == "binance" else "swap"
        perpetual_clients[name] = init_ccxt_exchange(name, creds, default_type=default_type)
    return ExchangeClient(
        spot_clients=spot_clients,
        perpetual_clients=perpetual_clients,
        default_spot=config.local_exchange_name,
        default_perpetual=config.global_exchange_name,
        strict_routing=True,
    )


# Venues whose TAPI v1 support was retired. ccxt still signs their legacy
# endpoint, which rejects TAPIv2 keys with `invalid_version_key`, so these
# need the v2 adapter instead of a ccxt instance.
V2_ADAPTERS = {"indodax": IndodaxTAPIv2}

def init_local_exchange(exchange_name: str, creds: Any, default_type: str = "spot") -> Any:
    """Builds the local spot client: v2 adapter when required, else ccxt."""
    adapter = V2_ADAPTERS.get(exchange_name.lower())
    if adapter is not None:
        return adapter(
            api_key=getattr(creds, "api_key", ""),
            api_secret=getattr(creds, "api_secret", ""),
        )
    return init_ccxt_exchange(exchange_name, creds, default_type=default_type)

def init_ccxt_exchange(exchange_name: str, creds: Any, default_type: str = "spot") -> Any:
    """Helper to initialize a CCXT async exchange with proper credentials and settings."""
    ex_class = getattr(ccxt, exchange_name.lower())
    cfg: Dict[str, Any] = {
        "apiKey": getattr(creds, "api_key", ""),
        "secret": getattr(creds, "api_secret", ""),
        "enableRateLimit": True,
        "options": {"defaultType": default_type},
    }
    if hasattr(creds, "options") and isinstance(creds.options, dict):
        cfg["options"].update(creds.options)

    if getattr(creds, "passphrase", None):
        cfg["password"] = creds.passphrase

    instance = ex_class(cfg)
    if getattr(creds, "testnet", False) and hasattr(instance, "set_sandbox_mode"):
        instance.set_sandbox_mode(True)
    return instance

def create_ccxt_client(local_name: str, global_name: str, local_creds, global_creds) -> ExchangeClient:
    """Factory creating CCXT async instances."""
    local_ex = init_local_exchange(local_name, local_creds, default_type="spot")
    global_ex = init_ccxt_exchange(global_name, global_creds, default_type="future")
    return ExchangeClient(
        local_ex,
        global_ex,
        default_spot=local_name,
        default_perpetual=global_name,
    )
