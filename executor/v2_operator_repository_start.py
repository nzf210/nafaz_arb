"""PostgreSQL START request and confirmation transitions."""
from __future__ import annotations

import hmac
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from executor.v2_operator_models import (
    StartReadinessReport,
    TelegramIdentity,
    confirmation_digest,
)


class OperatorRepositoryStartMixin:
    """Two-step START transition guarded by readiness and config convergence."""

    async def request_start(
        self,
        identity: TelegramIdentity,
        *,
        reason: str,
        config_version: int,
        challenge_code: str,
        ttl_seconds: int,
    ) -> dict[str, Any]:
        digest = confirmation_digest(challenge_code)
        expires_at = datetime.now(timezone.utc).replace(microsecond=0)
        expires_at += timedelta(seconds=ttl_seconds)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_exclusive(connection)
                action, created = await self._insert_action(
                    connection, identity, "trade_start", reason, "PENDING_CONFIRMATION",
                    requested_config_version=config_version,
                    challenge_hash=digest,
                    expires_at=expires_at,
                )
                if not created:
                    return {"duplicate": True, "status": action["status"]}
                active_version = await connection.fetchval(
                    "select version from public.active_config_version where singleton = true"
                )
                if active_version != config_version:
                    await connection.execute(
                        """
                        update public.operator_actions
                           set status = 'REJECTED', confirmation_hash = null,
                               result = '{"error":"CONFIG_VERSION_CHANGED"}'::jsonb,
                               completed_at = now()
                         where action_id = $1
                        """,
                        action["action_id"],
                    )
                    return {"duplicate": False, "status": "REJECTED", "error": "ACTIVE_CONFIG_VERSION_CHANGED"}
                state = await connection.fetchrow(
                    "select state, state_version from public.operator_control_state where singleton = true for update"
                )
                if state is None or state["state"] not in {"STOPPED", "START_PENDING"}:
                    await connection.execute(
                        """
                        update public.operator_actions
                           set status = 'REJECTED', confirmation_hash = null,
                               result = '{"error":"CONTROL_STATE_NOT_STOPPED"}'::jsonb,
                               completed_at = now()
                         where action_id = $1
                        """,
                        action["action_id"],
                    )
                    return {"duplicate": False, "status": "REJECTED", "error": "CONTROL_STATE_NOT_STOPPED"}
                next_version = int(state["state_version"]) + 1
                await connection.execute(
                    """
                    update public.operator_control_state
                       set state = 'START_PENDING', trading_enabled = false,
                           state_version = $1, config_version = $2,
                           last_actor_user_id = $3, last_actor_chat_id = $4,
                           last_reason = $5, updated_at = now()
                     where singleton = true
                    """,
                    next_version, config_version, identity.user_id, identity.chat_id, reason.strip(),
                )
                await self._emit_control_event(
                    connection, next_version, "operator_control.start_pending",
                    "operator:start_pending:" + str(identity.update_id),
                    {"state": "START_PENDING", "trading_enabled": False, "config_version": config_version},
                )
        return {"duplicate": False, "status": "PENDING_CONFIRMATION", "expires_at": expires_at.isoformat()}

    async def confirm_start(
        self,
        identity: TelegramIdentity,
        *,
        code: str,
        config_version: int,
        config_sha256: str,
        signals_enabled: bool,
        readiness: StartReadinessReport,
    ) -> dict[str, Any]:
        digest = confirmation_digest(code)
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await self._lock_exclusive(connection)
                action, created = await self._insert_action(
                    connection, identity, "trade_confirm", "Confirm pending START request", "RECEIVED"
                )
                if not created:
                    return {"duplicate": True, "status": action["status"], "result": action["result"]}
                active = await connection.fetchrow(
                    """
                    select v.version, v.content_sha256
                      from public.active_config_version p
                      join public.config_versions v on v.version = p.version
                     where p.singleton = true and v.status = 'active'
                    """
                )
                pending = await connection.fetchrow(
                    """
                    select action_id, requested_config_version, confirmation_hash,
                           confirmation_expires_at, reason
                      from public.operator_actions
                     where command = 'trade_start' and status = 'PENDING_CONFIRMATION'
                       and actor_user_id = $1 and actor_chat_id = $2
                     order by received_at desc
                     limit 1 for update
                    """,
                    identity.user_id, identity.chat_id,
                )
                error = ""
                if active is None or int(active["version"]) != config_version:
                    error = "ACTIVE_CONFIG_VERSION_CHANGED"
                elif str(active["content_sha256"]).strip() != config_sha256:
                    error = "ACTIVE_CONFIG_HASH_CHANGED"
                elif pending is None:
                    error = "NO_PENDING_START_CONFIRMATION"
                elif int(pending["requested_config_version"] or 0) != config_version:
                    error = "START_CONFIRMATION_CONFIG_MISMATCH"
                elif pending["confirmation_expires_at"] is None or pending["confirmation_expires_at"] <= datetime.now(timezone.utc):
                    error = "START_CONFIRMATION_EXPIRED"
                elif not hmac.compare_digest(str(pending["confirmation_hash"] or "").strip(), digest):
                    error = "START_CONFIRMATION_INVALID"
                elif not signals_enabled:
                    error = "EXECUTION_SIGNALS_DISABLED"
                elif not readiness.ready:
                    error = "START_READINESS_FAILED"

                if not error:
                    convergence = await connection.fetchrow(
                        """
                        select count(*) filter (
                                   where status in ('STARTING','ACTIVE','DEGRADED')
                                     and last_seen_at >= now() - make_interval(secs => $1)
                               ) as active_count,
                               count(*) filter (
                                   where status in ('STARTING','ACTIVE','DEGRADED')
                                     and last_seen_at >= now() - make_interval(secs => $1)
                                     and (status <> 'ACTIVE'
                                       or acknowledged_version is distinct from $2
                                       or acknowledged_sha256 is distinct from $3)
                               ) as stale_count
                          from public.config_consumers
                        """,
                        self._consumer_lease_seconds, config_version, config_sha256,
                    )
                    if not convergence or int(convergence["active_count"] or 0) < 1:
                        error = "NO_LIVE_EXECUTOR_CONSUMER"
                    elif int(convergence["stale_count"] or 0) > 0:
                        error = "EXECUTOR_CONFIG_NOT_CONVERGED"

                if error:
                    if pending is not None:
                        await connection.execute(
                            """
                            update public.operator_actions
                               set status = 'REJECTED', confirmation_hash = null,
                                   result = $2::jsonb, completed_at = now()
                             where action_id = $1
                            """,
                            pending["action_id"], json.dumps({"error": error, "readiness": readiness.as_dict()}),
                        )
                    current_state = await connection.fetchval(
                        "select state from public.operator_control_state where singleton = true for update"
                    )
                    if pending is not None and current_state == "START_PENDING":
                        await connection.execute(
                            """
                            update public.operator_control_state
                               set state = 'STOPPED', trading_enabled = false,
                                   state_version = state_version + 1,
                                   last_actor_user_id = $1, last_actor_chat_id = $2,
                                   last_reason = $3, updated_at = now()
                             where singleton = true
                            """,
                            identity.user_id, identity.chat_id, error,
                        )
                    await connection.execute(
                        """
                        update public.operator_actions
                           set status = 'FAILED', result = $2::jsonb, completed_at = now()
                         where action_id = $1
                        """,
                        action["action_id"],
                        json.dumps({"error": error, "readiness": readiness.as_dict()}, sort_keys=True),
                    )
                    return {"duplicate": False, "status": "FAILED", "error": error, "readiness": readiness.as_dict()}

                state = await connection.fetchrow(
                    "select state, state_version from public.operator_control_state where singleton = true for update"
                )
                if state is None or state["state"] != "START_PENDING":
                    await connection.execute(
                        """
                        update public.operator_actions
                           set status = 'FAILED', result = '{"error":"CONTROL_STATE_NOT_START_PENDING"}'::jsonb,
                               completed_at = now()
                         where action_id = $1
                        """,
                        action["action_id"],
                    )
                    return {"duplicate": False, "status": "FAILED", "error": "CONTROL_STATE_NOT_START_PENDING"}
                next_version = int(state["state_version"]) + 1
                await connection.execute(
                    """
                    update public.operator_control_state
                       set state = 'STARTING', trading_enabled = false,
                           state_version = $1, config_version = $2,
                           last_actor_user_id = $3, last_actor_chat_id = $4,
                           last_reason = 'START readiness passed; enabling entries',
                           updated_at = now()
                     where singleton = true
                    """,
                    next_version, config_version, identity.user_id, identity.chat_id,
                )
                next_version += 1
                await connection.execute(
                    """
                    update public.operator_control_state
                       set state = 'RUNNING', trading_enabled = true,
                           state_version = $1, config_version = $2,
                           last_actor_user_id = $3, last_actor_chat_id = $4,
                           last_reason = $5, updated_at = now()
                     where singleton = true
                    """,
                    next_version, config_version, identity.user_id, identity.chat_id,
                    str(pending["reason"] or "operator START")[:240],
                )
                await connection.execute(
                    """
                    update public.operator_actions
                       set status = 'COMPLETED', confirmation_hash = null,
                           result = $2::jsonb, completed_at = now()
                     where action_id = $1
                    """,
                    pending["action_id"],
                    json.dumps({"state": "RUNNING", "config_version": config_version}),
                )
                await connection.execute(
                    """
                    update public.operator_actions
                       set status = 'COMPLETED',
                           result = $2::jsonb, completed_at = now()
                     where action_id = $1
                    """,
                    action["action_id"],
                    json.dumps({"state": "RUNNING", "config_version": config_version, "readiness": readiness.as_dict()}, sort_keys=True),
                )
                await self._emit_control_event(
                    connection, next_version, "operator_control.started",
                    "operator:started:" + str(identity.update_id),
                    {"state": "RUNNING", "trading_enabled": True, "config_version": config_version},
                )
        return {"duplicate": False, "status": "RUNNING", "config_version": config_version}

