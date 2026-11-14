"""Durable PostgreSQL operator controls for the V2 Python executor.

Telegram is an authenticated transport only. PostgreSQL owns command idempotency,
operator state, confirmation challenges and the entry/STOP serialization fence.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

ENTRY_FENCE_LOCK_NAME = "nafaz:v2:operator_entry_fence"
SUPPORTED_COMMANDS = frozenset(
    {"trade_start", "trade_stop", "trade_status", "trade_confirm"}
)
_REQUIRED_START_CHECKS = (
    "database",
    "config_version",
    "redis_snapshot",
    "credentials",
    "market_metadata",
    "live_fx",
    "reconciliation",
    "risk",
    "kill_switch",
    "python_sole_consumer",
    "signals_enabled",
)
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_SECRET_REF = re.compile(r"^secret://[A-Za-z0-9._/-]+$")


class OperatorControlError(RuntimeError):
    """A fail-closed operator-control or Telegram policy error."""


class DuplicateCommandConflict(OperatorControlError):
    """A Telegram update ID was replayed with different command identity."""


@dataclass(frozen=True)
class TelegramIdentity:
    user_id: int
    chat_id: int
    update_id: int


@dataclass(frozen=True)
class TelegramPolicy:
    enabled: bool
    bot_token_env: Optional[str]
    bot_token_ref: Optional[str]
    allowed_user_ids: frozenset[int]
    allowed_chat_ids: frozenset[int]
    confirmation_ttl_seconds: int

    @classmethod
    def from_snapshot(cls, snapshot: Mapping[str, Any]) -> "TelegramPolicy":
        raw = snapshot.get("telegram")
        if not isinstance(raw, Mapping) or raw.get("enabled") is not True:
            raise OperatorControlError("Telegram V2 control is not enabled in the active config")
        token_env = raw.get("bot_token_env")
        token_ref = raw.get("bot_token_ref")
        if bool(token_env) == bool(token_ref):
            raise OperatorControlError("Telegram requires exactly one bot-token secret reference")
        if token_env is not None and (
            not isinstance(token_env, str) or not _ENV_NAME.fullmatch(token_env)
        ):
            raise OperatorControlError("Telegram bot_token_env is not a valid environment name")
        if token_ref is not None and (
            not isinstance(token_ref, str) or not _SECRET_REF.fullmatch(token_ref)
        ):
            raise OperatorControlError("Telegram bot_token_ref must use secret:// reference syntax")

        users = cls._id_set(raw.get("allowed_user_ids"), "allowed_user_ids", allow_negative=False)
        chats = cls._id_set(raw.get("allowed_chat_ids"), "allowed_chat_ids", allow_negative=True)
        if not users or not chats:
            raise OperatorControlError("Telegram user and chat allowlists must both be non-empty")
        ttl = raw.get("start_confirmation_ttl_seconds", 300)
        if isinstance(ttl, bool) or not isinstance(ttl, int) or not 30 <= ttl <= 900:
            raise OperatorControlError("Telegram START confirmation TTL must be between 30 and 900 seconds")
        return cls(True, token_env, token_ref, users, chats, ttl)

    @staticmethod
    def _id_set(value: Any, field: str, *, allow_negative: bool) -> frozenset[int]:
        if not isinstance(value, list) or not value:
            raise OperatorControlError("Telegram " + field + " must be a non-empty integer array")
        result = set()
        for item in value:
            if isinstance(item, bool) or not isinstance(item, int) or item == 0:
                raise OperatorControlError("Telegram " + field + " contains an invalid ID")
            if not allow_negative and item < 0:
                raise OperatorControlError("Telegram " + field + " must contain positive user IDs")
            if item in result:
                raise OperatorControlError("Telegram " + field + " contains duplicate IDs")
            result.add(item)
        return frozenset(result)

    def authorize(self, identity: TelegramIdentity) -> bool:
        return (
            self.enabled
            and identity.user_id in self.allowed_user_ids
            and identity.chat_id in self.allowed_chat_ids
        )

    def resolve_token(self, secret_resolver=None) -> str:
        if self.bot_token_env:
            token = os.environ.get(self.bot_token_env, "")
        elif self.bot_token_ref and callable(secret_resolver):
            token = secret_resolver(self.bot_token_ref)
        else:
            token = ""
        if not isinstance(token, str) or not token.strip():
            raise OperatorControlError("Telegram bot token secret reference could not be resolved")
        return token.strip()


@dataclass(frozen=True)
class StartReadinessReport:
    checks: Mapping[str, bool]
    details: Mapping[str, str]

    @classmethod
    def build(
        cls,
        checks: Mapping[str, Any],
        details: Optional[Mapping[str, str]] = None,
    ) -> "StartReadinessReport":
        normalized = {
            key: checks.get(key) is True
            for key in _REQUIRED_START_CHECKS
        }
        safe_details = {
            key: str(value)[:240]
            for key, value in (details or {}).items()
            if key in _REQUIRED_START_CHECKS and value
        }
        return cls(normalized, safe_details)

    @property
    def failed(self) -> tuple[str, ...]:
        return tuple(key for key in _REQUIRED_START_CHECKS if self.checks.get(key) is not True)

    @property
    def ready(self) -> bool:
        return not self.failed

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "checks": dict(self.checks),
            "failed": list(self.failed),
            "details": dict(self.details),
        }


def confirmation_digest(code: str) -> str:
    if not isinstance(code, str) or len(code) < 24 or len(code) > 128:
        raise OperatorControlError("START confirmation code is invalid")
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


class PostgresOperatorControlRepository:
    """Transactional operator state, audit, idempotency and STOP fencing."""

    def __init__(self, pool: Any, *, consumer_lease_seconds: int = 30):
        if consumer_lease_seconds < 5:
            raise ValueError("consumer_lease_seconds must be at least 5")
        self._pool = pool
        self._consumer_lease_seconds = consumer_lease_seconds

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

    async def record_status_command(self, identity: TelegramIdentity) -> dict[str, Any]:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                action, created = await self._insert_action(
                    connection, identity, "trade_status", "", "COMPLETED"
                )
                if created:
                    result = await self.get_status()
                    await connection.execute(
                        "update public.operator_actions set result = $2::jsonb, completed_at = now() where action_id = $1",
                        action["action_id"],
                        json.dumps(result, sort_keys=True),
                    )
        return await self.get_status()

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
        from datetime import timedelta
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
                    raise OperatorControlError("active config changed before START request was recorded")
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
