"""Fail-closed proxy enforcing intent ownership and coordinator fencing."""

from typing import Any


_MUTATION_METHODS = {
    "place_local_limit_buy": "spot_create",
    "place_local_limit_sell": "spot_create",
    "place_local_market_sell": "spot_create",
    "open_global_short_market": "futures_create",
    "close_global_short_market": "futures_create",
    "cancel_local_order": "spot_cancel",
}


class MutationGatewayClient:
    """Proxy all live order creates/cancels through one fenced mutation gateway."""

    def __init__(self, client: Any, coordinator, order_registry, config):
        self._client = client
        self._coordinator = coordinator
        self._registry = order_registry
        self._config = config

    def __getattr__(self, name):
        target = getattr(self._client, name)
        mutation_kind = _MUTATION_METHODS.get(name)
        if mutation_kind is None or not callable(target):
            return target

        async def guarded(*args, **kwargs):
            if bool(getattr(self._config, "mock_mode", False)):
                return await target(*args, **kwargs)

            async def execute():
                if mutation_kind == "spot_cancel":
                    order_id = str(args[0] if args else kwargs.get("order_id") or "")
                    exchange = str(
                        getattr(getattr(self._client, "local", None), "id", "")
                        or getattr(self._config, "local_exchange_name", "")
                    ).lower()
                    record = await self._registry.find_by_exchange_id(exchange, order_id)
                    if record is None:
                        raise RuntimeError(
                            f"mutation gateway blocked cancel: no owned registry record for {exchange}/{order_id}"
                        )
                else:
                    client_order_id = str(kwargs.get("client_order_id") or "")
                    if not client_order_id:
                        raise RuntimeError(
                            f"mutation gateway blocked {name}: client_order_id is required"
                        )
                    if mutation_kind == "spot_create":
                        exchange = str(
                            getattr(getattr(self._client, "local", None), "id", "")
                            or getattr(self._config, "local_exchange_name", "")
                        ).lower()
                    else:
                        exchange = str(
                            getattr(getattr(self._client, "global_fut", None), "id", "")
                            or getattr(self._config, "global_exchange_name", "")
                        ).lower()
                    record = await self._registry.find_by_client_id(exchange, client_order_id)
                    if record is None:
                        raise RuntimeError(
                            f"mutation gateway blocked {name}: durable order intent "
                            f"{exchange}/{client_order_id} is missing"
                        )
                return await target(*args, **kwargs)

            return await self._coordinator.execute_mutation(execute)

        return guarded
