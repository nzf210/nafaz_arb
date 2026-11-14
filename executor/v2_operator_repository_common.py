"""Shared PostgreSQL operator audit, idempotency and startup-state methods."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Mapping, Optional

from executor.v2_operator_models import (
    DuplicateCommandConflict,
    ENTRY_FENCE_LOCK_NAME,
    OperatorControlError,
    SUPPORTED_COMMANDS,
    TelegramIdentity,
)


class OperatorRepositoryCommonMixin:
    """Common persistence methods used by START, STOP and status commands."""

    @staticmethod
    async def _lock_exclusive(connection: Any) -> None:
        await connection.execute(
            "select pg_advisory_xact_lock(hashtextextended($1, 0))",
            ENTRY_FENCE_LOCK_NAME,
        )

    @staticmethod
    async def _insert_action(
        connection: Any,
        identity: TelegramIdentity,
        command: str,
        reason: str,
        status: str,
        *,
        requested_config_version: Optional[int] = None,
        challenge_hash: Optional[str] = None,
        expires_at: Optional[datetime] = None,
    ) -> tuple[Any, bool]:
        if command not in SUPPORTED_COMMANDS:
            raise OperatorControlError("unsupported operator command")
        if identity.update_id <= 0 or identity.user_id <= 0 or identity.chat_id == 0:
            raise OperatorControlError("Telegram identity or update ID is invalid")
        command_id = "telegram:" + str(identity.update_id)
        row = await connection.fetchrow(
            """
            insert into public.operator_actions
                (command_id, telegram_update_id, command, actor_user_id, actor_chat_id,
                 reason, status, requested_config_version, confirmation_hash,
                 confirmation_expires_at)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            on conflict (telegram_update_id) do nothing
            returning action_id, command, actor_user_id, actor_chat_id, status, result
            """,
            command_id, identity.update_id, command, identity.user_id, identity.chat_id,
            reason.strip(), status, requested_config_version, challenge_hash, expires_at,
        )
        if row is not None:
            return row, True
        existing = await connection.fetchrow(
            """
            select action_id, command, actor_user_id, actor_chat_id, status, result
              from public.operator_actions
             where telegram_update_id = $1
            """,
            identity.update_id,
        )
        if existing is None:
            raise OperatorControlError("operator command idempotency record could not be resolved")
        if (
            existing["command"] != command
            or existing["actor_user_id"] != identity.user_id
            or existing["actor_chat_id"] != identity.chat_id
        ):
            raise DuplicateCommandConflict("Telegram update ID was replayed with a different identity")
        return existing, False

    @staticmethod
    async def _emit_control_event(
        connection: Any,
        state_version: int,
        event_type: str,
        event_key: str,
        payload: Mapping[str, Any],
    ) -> None:
        await connection.execute(
            """
            insert into public.outbox_events
                (event_key, aggregate_type, aggregate_version, event_type, payload)
            values ($1, 'operator_control', $2, $3, $4::jsonb)
            on conflict (event_key) do nothing
            """,
            event_key, state_version, event_type,
            json.dumps(dict(payload), sort_keys=True, separators=(",", ":")),
        )

    async def get_status(self) -> dict[str, Any]:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                select s.state, s.trading_enabled, s.state_version, s.config_version,
                       s.open_order_stop_policy, s.last_actor_user_id, s.last_actor_chat_id,
                       s.last_reason, s.updated_at, active.version as active_config_version,
                       (select count(*) from public.config_consumers c
                         where c.status in ('STARTING','ACTIVE','DEGRADED')
                           and c.last_seen_at >= now() - make_interval(secs => $1)
                       ) as live_executor_processes
                  from public.operator_control_state s
                  left join public.active_config_version active on active.singleton = true
                 where s.singleton = true
                """,
                self._consumer_lease_seconds,
            )
        if row is None:
            raise OperatorControlError("durable operator control state is missing")
        return {
            "state": row["state"],
            "trading_enabled": bool(row["trading_enabled"]),
            "state_version": int(row["state_version"]),
            "config_version": row["config_version"],
            "active_config_version": row["active_config_version"],
            "open_order_stop_policy": row["open_order_stop_policy"],
            "last_actor_user_id": row["last_actor_user_id"],
            "last_actor_chat_id": row["last_actor_chat_id"],
            "last_reason": row["last_reason"],
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            "live_executor_processes": int(row["live_executor_processes"] or 0),
        }

    async def enforce_stopped_on_startup(self, instance_id: str) -> dict[str, Any]:
        """Persist STOPPED on every process start; liveness never implies trading."""
        instance = str(instance_id or "").strip()
        if not instance or len(instance) > 128:
            raise OperatorControlError("executor instance ID is invalid")
        reason = (
            "system startup safety reset; explicit Telegram START is required; "
            f"instance={instance}"
        )
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_exclusive(connection)
                state = await connection.fetchrow(
                    """
                    select state, trading_enabled, state_version
                      from public.operator_control_state
                     where singleton = true
                     for update
                    """
                )
                if state is None:
                    raise OperatorControlError("durable operator control state is missing")
                next_version = int(state["state_version"]) + 1
                await connection.execute(
                    """
                    update public.operator_control_state
                       set state = 'STOPPED', trading_enabled = false,
                           state_version = $1,
                           last_actor_user_id = null, last_actor_chat_id = null,
                           last_reason = $2, updated_at = now()
                     where singleton = true
                    """,
                    next_version, reason,
                )
                await connection.execute(
                    """
                    update public.operator_actions
                       set status = 'REJECTED', confirmation_hash = null,
                           result = '{"error":"EXECUTOR_RESTART_REQUIRES_OPERATOR_START"}'::jsonb,
                           completed_at = now()
                     where command = 'trade_start'
                       and status = 'PENDING_CONFIRMATION'
                    """
                )
                result = {
                    "state": "STOPPED",
                    "trading_enabled": False,
                    "state_version": next_version,
                    "actor": "system",
                    "reason": reason,
                    "instance_id": instance,
                }
                await self._emit_control_event(
                    connection,
                    next_version,
                    "operator_control.startup_stopped",
                    f"operator:startup_stopped:{instance}:{next_version}",
                    result,
                )
        return result

    async def record_status_command(self, identity: TelegramIdentity) -> dict[str, Any]:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                action, created = await self._insert_action(
                    connection, identity, "trade_status", "", "COMPLETED"
                )
        status = await self.get_status()
        if created:
            async with self._pool.acquire() as connection:
                await connection.execute(
                    "update public.operator_actions set result = $2::jsonb, completed_at = now() where action_id = $1",
                    action["action_id"],
                    json.dumps(status, sort_keys=True),
                )
        return status

    async def record_rejected(
        self,
        identity: TelegramIdentity,
        command: str,
        reason: str,
        error_code: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                action, created = await self._insert_action(
                    connection, identity, command, reason, "REJECTED"
                )
                if created:
                    await connection.execute(
                        """
                        update public.operator_actions
                           set result = $2::jsonb, completed_at = now()
                         where action_id = $1
                        """,
                        action["action_id"],
                        json.dumps({"error": error_code}, sort_keys=True),
                    )

