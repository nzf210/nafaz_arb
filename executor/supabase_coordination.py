"""Synchronous PostgREST RPCs for the durable execution coordinator and capacity ledger."""

from typing import Any, Dict


class SupabaseCoordinationMixin:
    """Durable coordinator lease and capacity reservation RPCs."""

    async def _call_rpc(self, function_name: str, payload: Dict[str, Any]):
        if not self.enabled or not self.url or not self.key:
            raise RuntimeError("Supabase RPC unavailable: durable persistence is disabled")
        self._ensure_session()
        url = f"{self.url}/rest/v1/rpc/{function_name}"
        async with self._session.post(url, json=payload) as response:
            if response.status >= 300:
                body = (await response.text())[:1000]
                raise RuntimeError(
                    f"Supabase RPC {function_name} failed with HTTP {response.status}: {body}"
                )
            if response.status == 204:
                return None
            return await response.json()

    @staticmethod
    def _first_row(value):
        if isinstance(value, list):
            return value[0] if value else {}
        return value if isinstance(value, dict) else {}

    async def acquire_execution_coordinator_lease(
        self, scope: str, owner_id: str, lease_seconds: int = 90
    ) -> Dict[str, Any]:
        value = await self._call_rpc(
            "acquire_execution_coordinator_lease",
            {"p_scope": scope, "p_owner_id": owner_id, "p_lease_seconds": lease_seconds},
        )
        return self._first_row(value)

    async def renew_execution_coordinator_lease(
        self, scope: str, owner_id: str, fencing_token: int, lease_seconds: int = 90
    ) -> bool:
        value = await self._call_rpc(
            "renew_execution_coordinator_lease",
            {
                "p_scope": scope, "p_owner_id": owner_id,
                "p_fencing_token": fencing_token, "p_lease_seconds": lease_seconds,
            },
        )
        return value is True

    async def assert_execution_coordinator_lease(
        self, scope: str, owner_id: str, fencing_token: int
    ) -> bool:
        value = await self._call_rpc(
            "assert_execution_coordinator_lease",
            {"p_scope": scope, "p_owner_id": owner_id, "p_fencing_token": fencing_token},
        )
        return value is True

    async def release_execution_coordinator_lease(
        self, scope: str, owner_id: str, fencing_token: int
    ) -> bool:
        value = await self._call_rpc(
            "release_execution_coordinator_lease",
            {"p_scope": scope, "p_owner_id": owner_id, "p_fencing_token": fencing_token},
        )
        return value is True

    async def fetch_execution_capacity_configs(self, account_scope: str):
        if not self.enabled or not self.url or not self.key:
            raise RuntimeError("Supabase capacity config unavailable: persistence is disabled")
        self._ensure_session()
        url = f"{self.url}/rest/v1/execution_capacity_config"
        params = {
            "select": "exchange_name,max_open_spot_orders,max_capital_usdt",
            "account_scope": f"eq.{account_scope}",
        }
        async with self._session.get(url, params=params) as response:
            if response.status >= 300:
                body = (await response.text())[:1000]
                raise RuntimeError(
                    f"Supabase capacity config read failed with HTTP {response.status}: {body}"
                )
            return await response.json()

    async def reserve_execution_capacity(
        self, account_scope: str, exchange_name: str, pair_key: str,
        client_order_id: str, reserved_amount_usdt: float,
        max_capital_usdt=None,
    ) -> Dict[str, Any]:
        value = await self._call_rpc(
            "reserve_execution_capacity",
            {
                "p_account_scope": account_scope,
                "p_exchange_name": exchange_name,
                "p_pair_key": pair_key,
                "p_client_order_id": client_order_id,
                "p_reserved_amount_usdt": reserved_amount_usdt,
                "p_max_capital_usdt": max_capital_usdt,
            },
        )
        return self._first_row(value)

    async def release_execution_capacity_for_order(
        self, account_scope: str, exchange_name: str, client_order_id: str
    ) -> bool:
        value = await self._call_rpc(
            "release_execution_capacity_for_order",
            {
                "p_account_scope": account_scope,
                "p_exchange_name": exchange_name,
                "p_client_order_id": client_order_id,
            },
        )
        return value is True

    async def release_execution_capacity(self, reservation_id: str) -> bool:
        value = await self._call_rpc(
            "release_execution_capacity", {"p_reservation_id": reservation_id}
        )
        return value is True

    async def release_execution_capacity_for_order(
        self, account_scope: str, exchange_name: str, client_order_id: str
    ) -> bool:
        value = await self._call_rpc(
            "release_execution_capacity_for_order",
            {
                "p_account_scope": account_scope,
                "p_exchange_name": exchange_name,
                "p_client_order_id": client_order_id,
            },
        )
        return value is True

    async def release_execution_capacity_for_pair(
        self, account_scope: str, exchange_name: str, pair_key: str
    ) -> bool:
        value = await self._call_rpc(
            "release_execution_capacity_for_pair",
            {
                "p_account_scope": account_scope,
                "p_exchange_name": exchange_name,
                "p_pair_key": pair_key,
            },
        )
        return value is True

    async def set_execution_capacity_config(
        self, account_scope: str, exchange_name: str,
        max_open_spot_orders: int, max_capital_usdt=None,
    ) -> Dict[str, Any]:
        value = await self._call_rpc(
            "set_execution_capacity_config",
            {
                "p_account_scope": account_scope,
                "p_exchange_name": exchange_name,
                "p_max_open_spot_orders": max_open_spot_orders,
                "p_max_capital_usdt": max_capital_usdt,
            },
        )
        return self._first_row(value)
