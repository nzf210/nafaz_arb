"""Exchange construction and venue-specific adapter selection."""

from typing import Any, Dict

import ccxt.async_support as ccxt

from executor.exchange_client import ExchangeClient
from executor.indodax_v2 import IndodaxTAPIv2
from executor.paper_exchange_client import PaperTradingExchangeClient

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
