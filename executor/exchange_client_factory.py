"""Multi-venue exchange client factory."""

from typing import Any

from executor.paper_exchange_client import PaperTradingExchangeClient
from executor.exchange_factory import init_ccxt_exchange, init_local_exchange


def create_exchange_client(config: Any) -> Any:
    """Factory creating a multi-venue live client or paper trading client."""
    if getattr(config, "mock_mode", False):
        return PaperTradingExchangeClient(
            local_name=config.local_exchange_name,
            global_name=config.global_exchange_name,
            initial_balance_usdt=getattr(config, "initial_exchange_balance_usdt", 100.0),
        )
    spot_clients = _create_spot_clients(config)
    perpetual_clients = _create_perpetual_clients(config)
    from executor.exchange_client import ExchangeClient
    return ExchangeClient(
        spot_clients=spot_clients,
        perpetual_clients=perpetual_clients,
        default_spot=config.local_exchange_name,
        default_perpetual=config.global_exchange_name,
        strict_routing=True,
    )


def _create_spot_clients(config: Any) -> dict:
    names = _configured_exchange_names(
        getattr(config, "spot_exchanges", []), config.local_exchange_name
    )
    clients = {}
    for name in names:
        credentials = _route_credentials(config, name, "spot")
        if not credentials:
            raise ValueError(f"LIVE spot route {name} has no complete API credentials")
        clients[name] = init_local_exchange(name, credentials, default_type="spot")
    return clients


def _create_perpetual_clients(config: Any) -> dict:
    names = _configured_exchange_names(
        getattr(config, "perpetual_exchanges", []), config.global_exchange_name
    )
    clients = {}
    for name in names:
        credentials = _route_credentials(config, name, "perpetual")
        if not credentials:
            raise ValueError(f"LIVE perpetual route {name} has no complete API credentials")
        if name == "okx" and not credentials.passphrase:
            raise ValueError("LIVE perpetual route okx requires API passphrase")
        default_type = "future" if name == "binance" else "swap"
        clients[name] = init_ccxt_exchange(name, credentials, default_type=default_type)
    return clients


def _configured_exchange_names(configured, fallback: str) -> list[str]:
    names = configured or [fallback]
    return sorted(set(str(name).lower() for name in names))


def _route_credentials(config: Any, name: str, market: str):
    credentials = (getattr(config, "exchanges", {}) or {}).get(name)
    if credentials is not None:
        return credentials
    fallback_name = config.local_exchange_name if market == "spot" else config.global_exchange_name
    if name == str(fallback_name).lower():
        return config.local_credentials if market == "spot" else config.global_credentials
    return None
