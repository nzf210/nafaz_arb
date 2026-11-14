"""Telegram operator identity, policy and readiness value objects."""
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
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

