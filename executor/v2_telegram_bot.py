"""Python-owned Telegram long-polling transport for V2 operator commands.

The Go Telegram bot remains read-only. This module never submits exchange orders;
it writes durable control commands through PostgresOperatorControlRepository.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging
from typing import Any, Awaitable, Callable, Mapping, Optional

from executor.v2_config import ConfigSnapshot, PostgresConfigRepository
from executor.v2_operator_control import (
    OperatorControlError,
    PostgresOperatorControlRepository,
    StartReadinessReport,
    TelegramIdentity,
    TelegramPolicy,
)

logger = logging.getLogger("executor.v2_telegram_control")


class TelegramControlService:
    def __init__(
        self,
        repository: PostgresOperatorControlRepository,
        config_repository: PostgresConfigRepository,
        *,
        readiness_provider: Optional[Callable[[ConfigSnapshot], Awaitable[Any]]] = None,
    ):
        self._repository = repository
        self._config_repository = config_repository
        self._readiness_provider = readiness_provider

    @staticmethod
    def _identity(update: Mapping[str, Any]) -> Optional[TelegramIdentity]:
        message = update.get("message")
        if not isinstance(message, Mapping):
            return None
        sender = message.get("from")
        chat = message.get("chat")
        if not isinstance(sender, Mapping) or not isinstance(chat, Mapping):
            return None
        try:
            identity = TelegramIdentity(
                user_id=int(sender["id"]),
                chat_id=int(chat["id"]),
                update_id=int(update["update_id"]),
            )
        except (KeyError, TypeError, ValueError):
            return None
        if identity.user_id <= 0 or identity.chat_id == 0 or identity.update_id <= 0:
            return None
        return identity

    @staticmethod
    def _command(text: Any) -> tuple[str, str]:
        if not isinstance(text, str) or not text.strip().startswith("/"):
            return "", ""
        parts = text.strip().split(maxsplit=1)
        command = parts[0].split("@", 1)[0].lower().lstrip("/")
        return command, parts[1].strip() if len(parts) > 1 else ""

    @staticmethod
    def _confirmation_code(
        token: str,
        identity: TelegramIdentity,
        config_version: int,
    ) -> str:
        message = "{}:{}:{}:{}".format(
            identity.update_id, identity.user_id, identity.chat_id, config_version
        )
        digest = hmac.new(token.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
        return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

    async def handle_update(
        self,
        update: Mapping[str, Any],
        policy: TelegramPolicy,
        token: str,
    ) -> Optional[str]:
        identity = self._identity(update)
        if identity is None:
            return None
        message = update.get("message") or {}
        command, argument = self._command(message.get("text"))
        if command not in {"trade_start", "trade_stop", "trade_status", "trade_confirm"}:
            return None

        if not policy.authorize(identity):
            await self._repository.record_rejected(
                identity, command, argument, "TELEGRAM_ALLOWLIST_DENIED"
            )
            return "Akses ditolak. User ID dan chat ID tidak masuk allowlist."

        if command == "trade_status":
            status = await self._repository.record_status_command(identity)
            return self._format_status(status)

        if command == "trade_stop":
            if not argument:
                await self._repository.record_rejected(
                    identity, command, "", "STOP_REASON_REQUIRED"
                )
                return "STOP ditolak: sertakan alasan, contoh /trade_stop maintenance."
            result = await self._repository.stop(identity, reason=argument)
            if result.get("duplicate"):
                return "Update STOP ini sudah diproses sebelumnya; tidak ada aksi yang diulang."
            return (
                "STOP tersimpan di PostgreSQL.\n"
                "State: STOPPED\n"
                "Entry baru: diblokir\n"
                "Open-order policy: keep_and_manage\n"
                "Posisi terbuka tidak ditutup otomatis dan order tidak dibatalkan."
            )

        if command == "trade_start":
            if not argument:
                await self._repository.record_rejected(
                    identity, command, "", "START_REASON_REQUIRED"
                )
                return "START ditolak: sertakan alasan, contoh /trade_start operator-approved."
            config = await self._active_config(identity, command, argument)
            if config is None:
                return "START ditolak: active PostgreSQL config tidak dapat diverifikasi."
            code = self._confirmation_code(token, identity, config.version)
            result = await self._repository.request_start(
                identity,
                reason=argument,
                config_version=config.version,
                challenge_code=code,
                ttl_seconds=policy.confirmation_ttl_seconds,
            )
            if result.get("status") == "REJECTED":
                return "START ditolak: state kontrol tidak STOPPED."
            if result.get("duplicate"):
                return (
                    "Perintah START ini sudah tercatat. Jika challenge masih berlaku, konfirmasi "
                    "dengan kode yang sama:\n/trade_confirm " + code
                )
            return (
                "START belum aktif. Konfirmasi satu kali dalam "
                + str(policy.confirmation_ttl_seconds)
                + " detik; kode terikat ke user, chat, dan config version "
                + str(config.version)
                + ":\n/trade_confirm "
                + code
            )

        if not argument:
            await self._repository.record_rejected(
                identity, command, "", "START_CONFIRMATION_CODE_REQUIRED"
            )
            return "Konfirmasi ditolak: kode tidak ditemukan."
        try:
            config = await self._config_repository.fetch_active()
        except Exception:
            await self._repository.record_rejected(
                identity, command, "Confirm START", "ACTIVE_CONFIG_UNAVAILABLE"
            )
            return "START gagal: active config PostgreSQL tidak tersedia atau tidak valid."

        readiness = await self._readiness(config)
        execution = config.snapshot.get("execution")
        signals_enabled = (
            isinstance(execution, Mapping) and execution.get("signals_enabled") is True
        )
        result = await self._repository.confirm_start(
            identity,
            code=argument,
            config_version=config.version,
            config_sha256=config.content_sha256,
            signals_enabled=signals_enabled,
            readiness=readiness,
        )
        if result.get("duplicate"):
            return "Perintah konfirmasi ini sudah diproses; tidak ada transisi state yang diulang."
        if result.get("status") != "RUNNING":
            failed = result.get("readiness", {}).get("failed", [])
            suffix = ", ".join(failed) if failed else str(result.get("error", "unknown"))
            return "START gagal secara fail-closed. Pemeriksaan belum lulus: " + suffix
        return "START berhasil setelah seluruh pemeriksaan readiness dan convergence lulus."

    async def _active_config(
        self,
        identity: TelegramIdentity,
        command: str,
        reason: str,
    ) -> Optional[ConfigSnapshot]:
        try:
            return await self._config_repository.fetch_active()
        except Exception:
            await self._repository.record_rejected(
                identity, command, reason, "ACTIVE_CONFIG_UNAVAILABLE"
            )
            return None

    async def _readiness(self, config: ConfigSnapshot) -> StartReadinessReport:
        if self._readiness_provider is None:
            return StartReadinessReport.build({})
        try:
            result = await self._readiness_provider(config)
        except Exception as exc:
            logger.warning("START readiness provider failed: %s", type(exc).__name__)
            return StartReadinessReport.build({})
        if isinstance(result, StartReadinessReport):
            return result
        if isinstance(result, Mapping):
            return StartReadinessReport.build(result)
        return StartReadinessReport.build({})

    @staticmethod
    def _format_status(status: Mapping[str, Any]) -> str:
        return (
            "NAFAZ V2 control status\n"
            "Trading state: " + str(status["state"]) + "\n"
            "Trading enabled: " + str(status["trading_enabled"]) + "\n"
            "Config version: " + str(status["config_version"]) + "\n"
            "Active config version: " + str(status["active_config_version"]) + "\n"
            "Live executor processes: " + str(status["live_executor_processes"]) + "\n"
            "Open-order STOP policy: " + str(status["open_order_stop_policy"]) + "\n"
            "Last reason: " + str(status["last_reason"])
        )


class TelegramControlBot:
    """Minimal aiohttp long-polling adapter; token is never logged."""

    def __init__(
        self,
        policy: TelegramPolicy,
        service: TelegramControlService,
        *,
        secret_resolver=None,
        poll_timeout_seconds: int = 25,
    ):
        if not 1 <= poll_timeout_seconds <= 50:
            raise ValueError("poll_timeout_seconds must be between 1 and 50")
        self._policy = policy
        self._service = service
        self._token = policy.resolve_token(secret_resolver)
        self._poll_timeout = poll_timeout_seconds
        self._stopping = asyncio.Event()
        self._offset: Optional[int] = None

    def stop(self) -> None:
        self._stopping.set()

    async def _api(self, session: Any, method: str, payload: Mapping[str, Any]) -> Any:
        url = "https://api.telegram.org/bot" + self._token + "/" + method
        try:
            async with session.post(url, json=dict(payload)) as response:
                if response.status != 200:
                    raise OperatorControlError("Telegram API returned a non-success status")
                body = await response.json()
        except OperatorControlError:
            raise
        except Exception:
            raise OperatorControlError("Telegram API transport failed") from None
        if not isinstance(body, Mapping) or body.get("ok") is not True:
            raise OperatorControlError("Telegram API rejected the control request")
        return body.get("result")

    async def run(self) -> None:
        import aiohttp

        timeout = aiohttp.ClientTimeout(total=self._poll_timeout + 10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self._stopping.is_set():
                payload = {
                    "timeout": self._poll_timeout,
                    "allowed_updates": ["message"],
                }
                if self._offset is not None:
                    payload["offset"] = self._offset
                try:
                    updates = await self._api(session, "getUpdates", payload)
                    if not isinstance(updates, list):
                        raise OperatorControlError("Telegram getUpdates result is invalid")
                    for update in updates:
                        if self._stopping.is_set():
                            return
                        if not isinstance(update, Mapping):
                            continue
                        update_id = update.get("update_id")
                        if isinstance(update_id, bool) or not isinstance(update_id, int):
                            continue
                        reply = await self._service.handle_update(update, self._policy, self._token)
                        if reply:
                            message = update.get("message") or {}
                            chat = message.get("chat") or {}
                            chat_id = chat.get("id")
                            if isinstance(chat_id, int) and not isinstance(chat_id, bool):
                                await self._api(
                                    session,
                                    "sendMessage",
                                    {"chat_id": chat_id, "text": reply, "disable_web_page_preview": True},
                                )
                        self._offset = max(self._offset or 0, update_id + 1)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Telegram control polling failed: %s", type(exc).__name__)
                    try:
                        await asyncio.wait_for(self._stopping.wait(), timeout=2)
                    except asyncio.TimeoutError:
                        pass
