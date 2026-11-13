"""Unified exchange facade for spot and perpetual venue adapters."""

from typing import Optional, Dict, Any
import ccxt.async_support as ccxt

from executor.indodax_v2 import IndodaxTAPIv2
from executor.helpers import _broker_prefix_for
from executor.exchange_common import (
    VENUE_CLIENT_ORDER_ID_MAX_LEN, _base_order_result, _client_order_params,
    _expose_client_order_id, _local_order_params, _normalize_open_order,
    _require_amount_spec, venue_client_order_id,
    _balance_fetch_params, _fetch_exchange_balance,
)
from executor.exchange_fee_balance import ExchangeFeeBalanceMixin
from executor.exchange_futures import ExchangeFuturesMixin
from executor.exchange_orders import ExchangeOrderMixin
from executor.exchange_trades import ExchangeTradeMixin
from executor.paper_exchange_client import PaperTradingExchangeClient


class ExchangeClient(
    ExchangeOrderMixin,
    ExchangeFeeBalanceMixin,
    ExchangeTradeMixin,
    ExchangeFuturesMixin,
):
    """Unified client enforcing the spot/perpetual trading contract."""

    def __init__(
        self,
        local_exchange: Any,
        global_exchange: Any,
        balance_exchanges: Optional[Dict[str, Dict[str, Any]]] = None,
        pair_info: Any = None,
    ):
        self.local = local_exchange
        self.global_fut = global_exchange
        self._pair_info = pair_info
        self._fee_cache: Dict[tuple, tuple] = {}
        # Registered broker id for Binance Cloud venues (Tokocrypto). Read from
        # the local client's own options, so it travels with the account;
        # empty means "let the venue mint the client id".
        self._local_broker_prefix = _broker_prefix_for(
            getattr(local_exchange, "id", ""), local_exchange
        )
        self._global_broker_prefix = _broker_prefix_for(
            getattr(global_exchange, "id", ""), global_exchange
        )
        self.balance_exchanges = balance_exchanges or {
            getattr(local_exchange, "id", "local"): {"market": "spot", "client": local_exchange},
            getattr(global_exchange, "id", "global"): {"market": "futures", "client": global_exchange},
        }

    @property
    def _global_scope(self) -> str:
        """Venue leg name for global (perpetual) client-order-id aliases."""
        venue = getattr(self, "global_fut", None)
        return str(getattr(venue, "id", "") or "") if venue is not None else ""

    def for_venue(self, exchange: str) -> Optional["ExchangeClient"]:
        """Return a facade bound to one venue, independent of a registered pair.

        Recovery reads an order back from the venue that owns it, and that venue
        need not be the configured base pair or a pair bound earlier in this
        process (``for_pair`` only covers pairs already seen). The balance
        registry is built at startup from the configured credentials, so it can
        always answer "who owns venue X" -- or say None when X is not configured,
        which keeps a wrong-venue lookup from silently falling through.
        """
        name = str(exchange or "").strip().lower()
        if not name:
            return None
        entry = self.balance_exchanges.get(name)
        if not entry or entry.get("client") is None:
            return None
        if entry.get("market") == "futures":
            return ExchangeClient(
                self.local, entry["client"], self.balance_exchanges
            )
        return ExchangeClient(
            entry["client"], self.global_fut, self.balance_exchanges
        )

    def for_pair(self, pair_info: Any) -> "ExchangeClient":
        """Return an immutable facade bound to the exact spot/futures venue pair."""
        local_name = str(getattr(pair_info, "local_exchange", "") or "").strip().lower()
        global_name = str(getattr(pair_info, "global_exchange", "") or "").strip().lower()
        local_entry = self.balance_exchanges.get(local_name)
        global_entry = self.balance_exchanges.get(global_name)
        if not local_entry or local_entry.get("market") != "spot" or local_entry.get("client") is None:
            raise LookupError(f"spot client unavailable for venue: {local_name or 'unknown'}")
        if not global_entry or global_entry.get("market") != "futures" or global_entry.get("client") is None:
            raise LookupError(f"futures client unavailable for venue: {global_name or 'unknown'}")
        return ExchangeClient(
            local_entry["client"],
            global_entry["client"],
            self.balance_exchanges,
            pair_info=pair_info,
        )



def create_exchange_client(config: Any) -> Any:
    """Factory creating the active live or paper-trading client."""
    if getattr(config, "mock_mode", False):
        return PaperTradingExchangeClient(
            local_name=config.local_exchange_name,
            global_name=config.global_exchange_name,
            initial_balance_usdt=getattr(config, "initial_exchange_balance_usdt", 100.0),
        )
    return create_ccxt_client(
        local_name=config.local_exchange_name,
        global_name=config.global_exchange_name,
        local_creds=config.local_credentials,
        global_creds=config.global_credentials,
        configured_exchanges=getattr(config, "exchanges", {}),
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

def _balance_fetch_params(exchange: Any, market: str) -> Dict[str, Any]:
    """Return explicit CCXT market params where a venue needs them."""
    exchange_id = str(getattr(exchange, "id", "") or "").lower()
    if market == "futures" and exchange_id == "mexc":
        return {"type": "swap"}
    return {}


async def _fetch_exchange_balance(exchange: Any, market: str) -> Dict[str, Any]:
    """Fetch a balance without changing legacy no-argument adapter contracts."""
    params = _balance_fetch_params(exchange, market)
    if params:
        return await exchange.fetch_balance(params)
    return await exchange.fetch_balance()


def _resolve_default_type(exchange_name: str, default_type: str) -> str:
    """Map the executor's generic futures type to each venue's CCXT market type."""
    if exchange_name.lower() == "mexc" and default_type.lower() in {"future", "futures"}:
        return "swap"
    return default_type


def init_ccxt_exchange(exchange_name: str, creds: Any, default_type: str = "spot") -> Any:
    """Helper to initialize a CCXT async exchange with proper credentials and settings."""
    normalized_name = exchange_name.lower()
    ex_class = getattr(ccxt, normalized_name)
    resolved_default_type = _resolve_default_type(normalized_name, default_type)
    cfg: Dict[str, Any] = {
        "apiKey": getattr(creds, "api_key", ""),
        "secret": getattr(creds, "api_secret", ""),
        "enableRateLimit": True,
        "options": {"defaultType": resolved_default_type},
    }
    if hasattr(creds, "options") and isinstance(creds.options, dict):
        cfg["options"].update(creds.options)
    # MEXC's CCXT futures market is named "swap". Keep it explicit even if
    # an old config.json still contains options.defaultType="future".
    if normalized_name == "mexc" and default_type.lower() in {"future", "futures"}:
        cfg["options"]["defaultType"] = "swap"

    if getattr(creds, "passphrase", None):
        cfg["password"] = creds.passphrase

    instance = ex_class(cfg)
    if getattr(creds, "testnet", False) and hasattr(instance, "set_sandbox_mode"):
        instance.set_sandbox_mode(True)
    return instance

BALANCE_MARKETS = {
    "tokocrypto": "spot",
    "indodax": "spot",
    "okx": "futures",
    "mexc": "futures",
    "binance": "futures",
    "bybit": "futures",
}


def create_ccxt_client(local_name: str, global_name: str, local_creds, global_creds, configured_exchanges=None) -> ExchangeClient:
    """Create the active trading pair clients and independent read-only balance clients."""
    local_ex = init_local_exchange(local_name, local_creds, default_type="spot")
    global_ex = init_ccxt_exchange(global_name, global_creds, default_type="future")
    balance_exchanges = {
        local_name.lower(): {"market": "spot", "client": local_ex},
        global_name.lower(): {"market": "futures", "client": global_ex},
    }
    credentials = configured_exchanges or {}
    for name, market in BALANCE_MARKETS.items():
        if name in balance_exchanges:
            continue
        creds = credentials.get(name)
        if creds is None:
            balance_exchanges[name] = {"market": market, "client": None, "error": "API key/secret not configured"}
            continue
        api_key = str(getattr(creds, "api_key", "") or "").strip()
        api_secret = str(getattr(creds, "api_secret", "") or "").strip()
        if not api_key or not api_secret:
            balance_exchanges[name] = {"market": market, "client": None, "error": "API key/secret not configured"}
            continue
        try:
            client = init_local_exchange(name, creds, default_type="spot") if market == "spot" else init_ccxt_exchange(name, creds, default_type="future")
            balance_exchanges[name] = {"market": market, "client": client}
        except Exception as exc:
            balance_exchanges[name] = {"market": market, "client": None, "error": f"balance client initialization failed: {exc}"}
    return ExchangeClient(local_ex, global_ex, balance_exchanges)
