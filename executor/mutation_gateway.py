"""Fail-closed proxy enforcing intent ownership and coordinator fencing."""

from typing import Any




class ReadOnlyExchangeProxy:
    """Expose venue reads while preventing raw CCXT mutation bypasses."""

    _BLOCKED_PREFIXES = (
        "create", "cancel", "edit", "withdraw", "transfer", "borrow", "repay",
        "privatepost", "privatedelete", "privateput", "fapiprivatepost",
        "fapiprivatedelete", "fapiprivateput", "sapipost", "sapidelete",
        "sapiput", "papipost", "papidelete", "papiput",
    )

    def __init__(self, venue: Any):
        self._venue = venue

    def __getattr__(self, name):
        normalized = str(name).replace("_", "").lower()
        if normalized.startswith(self._BLOCKED_PREFIXES):
            raise RuntimeError(
                f"raw exchange mutation blocked outside MutationGateway: {name}"
            )
        return getattr(self._venue, name)

_MUTATION_METHODS = {
    "place_local_limit_buy": ("spot_create", "buy", 3),
    "place_local_limit_sell": ("spot_create", "sell", 3),
    "place_local_market_sell": ("spot_create", "sell", 2),
    "open_global_short_market": ("futures_create", "sell", 2),
    "close_global_short_market": ("futures_create", "buy", 2),
    "cancel_local_order": ("spot_cancel", "", -1),
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
        if name in {"local", "global_fut"} and target is not None:
            return ReadOnlyExchangeProxy(target)
        mutation_contract = _MUTATION_METHODS.get(name)
        if mutation_contract is None or not callable(target):
            return target
        mutation_kind, expected_side, client_id_index = mutation_contract

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
                    if str(getattr(record, "market_type", "")).lower() != "spot":
                        raise RuntimeError("mutation gateway blocked cancel: owned order is not a spot order")
                    if bool(getattr(record, "is_terminal", False)):
                        raise RuntimeError("mutation gateway blocked cancel: order is already terminal")
                else:
                    client_order_id = str(
                        kwargs.get("client_order_id")
                        or (args[client_id_index] if len(args) > client_id_index else "")
                        or ""
                    )
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
                    expected_market = (
                        "spot" if mutation_kind == "spot_create" else "perpetual"
                    )
                    actual_market = str(getattr(record, "market_type", "")).lower()
                    if expected_market == "perpetual":
                        market_matches = actual_market in {"perpetual", "future", "futures", "swap"}
                    else:
                        market_matches = actual_market == expected_market
                    if not market_matches:
                        raise RuntimeError(
                            f"mutation gateway blocked {name}: intent market {actual_market!r} "
                            f"does not match {expected_market!r}"
                        )
                    if str(getattr(record, "side", "")).lower() != expected_side:
                        raise RuntimeError(
                            f"mutation gateway blocked {name}: intent side does not match "
                            f"expected {expected_side!r}"
                        )
                    if bool(getattr(record, "is_terminal", False)):
                        raise RuntimeError(
                            f"mutation gateway blocked {name}: order intent is already terminal"
                        )
                return await target(*args, **kwargs)

            return await self._coordinator.execute_mutation(execute)

        return guarded
