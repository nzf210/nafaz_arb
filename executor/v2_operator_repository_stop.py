"""PostgreSQL STOP transition and open-order policy enforcement."""
from __future__ import annotations

import json
from typing import Any

from executor.v2_operator_models import OperatorControlError, TelegramIdentity
from executor.v2_operator_repository_common import OperatorRepositoryBase


class OperatorRepositoryStopMixin(OperatorRepositoryBase):
    """STOP transition serialized against in-flight entry submissions."""

    async def stop(
        self,
        identity: TelegramIdentity,
        *,
        reason: str,
        open_order_policy: str = "keep_and_manage",
    ) -> dict[str, Any]:
        if open_order_policy != "keep_and_manage":
            raise OperatorControlError("only keep_and_manage is currently supported; no automatic order cancellation")
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_exclusive(connection)
                action, created = await self._insert_action(
                    connection, identity, "trade_stop", reason, "ACCEPTED"
                )
                if not created:
                    return {"duplicate": True, "status": action["status"], "result": action["result"]}
                state = await connection.fetchrow(
                    "select state, state_version from public.operator_control_state where singleton = true for update"
                )
                if state is None:
                    raise OperatorControlError("durable operator control state is missing")
                next_version = int(state["state_version"]) + 1
                await connection.execute(
                    """
                    update public.operator_control_state
                       set state = 'STOPPING', trading_enabled = false,
                           state_version = $1,
                           open_order_stop_policy = 'keep_and_manage',
                           last_actor_user_id = $2, last_actor_chat_id = $3,
                           last_reason = $4, updated_at = now()
                     where singleton = true
                    """,
                    next_version, identity.user_id, identity.chat_id, reason.strip(),
                )
                next_version += 1
                await connection.execute(
                    """
                    update public.operator_control_state
                       set state = 'STOPPED', trading_enabled = false,
                           state_version = $1, updated_at = now()
                     where singleton = true
                    """,
                    next_version,
                )
                await connection.execute(
                    """
                    update public.operator_actions
                       set status = 'REJECTED', confirmation_hash = null,
                           result = '{"error":"CANCELLED_BY_STOP"}'::jsonb,
                           completed_at = now()
                     where command = 'trade_start'
                       and status = 'PENDING_CONFIRMATION'
                    """
                )
                result = {
                    "state": "STOPPED",
                    "trading_enabled": False,
                    "state_version": next_version,
                    "open_order_policy": "keep_and_manage",
                    "positions_closed": False,
                    "orders_cancelled": False,
                }
                await connection.execute(
                    """
                    update public.operator_actions
                       set status = 'COMPLETED', previous_state = $2,
                           resulting_state = 'STOPPED',
                           result = $3::jsonb, completed_at = now()
                     where action_id = $1
                    """,
                    action["action_id"], state["state"], json.dumps(result, sort_keys=True),
                )
                await self._emit_control_event(
                    connection, next_version, "operator_control.stopped",
                    "operator:stopped:" + str(identity.update_id), result,
                )
        return {"duplicate": False, **result}

