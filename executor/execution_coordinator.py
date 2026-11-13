"""Single-writer execution coordinator backed by a Postgres fencing lease."""

import asyncio
import logging
import uuid
from typing import Awaitable, Callable, Optional, TypeVar

logger = logging.getLogger("executor.coordinator")
T = TypeVar("T")


class ExecutionCoordinator:
    """Owns the global lease and serializes every live order mutation."""

    def __init__(
        self, supabase, *, scope: str = "global", lease_seconds: int = 90,
        renew_interval_seconds: float = 20.0,
        on_lost: Optional[Callable[[str], Awaitable[None]]] = None,
    ):
        self.supabase = supabase
        self.scope = scope
        self.owner_id = str(uuid.uuid4())
        self.lease_seconds = max(30, min(300, int(lease_seconds)))
        self.renew_interval_seconds = max(5.0, float(renew_interval_seconds))
        self.on_lost = on_lost
        self.fencing_token: Optional[int] = None
        self.active = False
        self.lost_reason = ""
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._mutation_lock = asyncio.Lock()

    async def acquire(self) -> bool:
        result = await self.supabase.acquire_execution_coordinator_lease(
            self.scope, self.owner_id, self.lease_seconds
        )
        if not bool(result.get("acquired")):
            self.active = False
            self.fencing_token = None
            return False
        token = int(result.get("fencing_token") or 0)
        if token <= 0:
            raise RuntimeError("Postgres coordinator lease returned an invalid fencing token")
        self.fencing_token = token
        self.active = True
        self.lost_reason = ""
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(self._heartbeat())
        logger.info(
            "Execution Coordinator acquired scope=%s owner=%s fencing_token=%d",
            self.scope, self.owner_id, token,
        )
        return True

    async def _lose(self, reason: str) -> None:
        if not self.active:
            return
        self.active = False
        self.lost_reason = reason
        logger.critical("Execution Coordinator lease lost: %s", reason)
        if self.on_lost is not None:
            try:
                await self.on_lost(reason)
            except Exception:
                logger.exception("Execution Coordinator loss callback failed")

    async def _heartbeat(self) -> None:
        try:
            while self.active and self.fencing_token is not None:
                await asyncio.sleep(self.renew_interval_seconds)
                try:
                    renewed = await self.supabase.renew_execution_coordinator_lease(
                        self.scope, self.owner_id, self.fencing_token, self.lease_seconds
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._lose(f"lease renewal failed: {type(exc).__name__}: {exc}")
                    return
                if not renewed:
                    await self._lose("Postgres rejected coordinator lease renewal")
                    return
        except asyncio.CancelledError:
            raise
        finally:
            if self._heartbeat_task is asyncio.current_task():
                self._heartbeat_task = None

    async def assert_active(self) -> None:
        if not self.active or self.fencing_token is None:
            raise RuntimeError(
                "order mutation blocked: this process does not own the active execution lease"
            )
        try:
            valid = await self.supabase.assert_execution_coordinator_lease(
                self.scope, self.owner_id, self.fencing_token
            )
        except Exception as exc:
            await self._lose(f"fencing validation failed: {type(exc).__name__}: {exc}")
            raise RuntimeError("order mutation blocked: coordinator fencing validation failed") from exc
        if not valid:
            await self._lose("stale or expired coordinator fencing token")
            raise RuntimeError("order mutation blocked: coordinator fencing token is stale")

    async def execute_mutation(self, operation: Callable[[], Awaitable[T]]) -> T:
        async with self._mutation_lock:
            await self.assert_active()
            result = await operation()
            # If the lease expired while the venue request was in flight, do not
            # report the mutation as cleanly committed. Callers must reconcile by
            # client order id instead of assuming the acknowledgement is safe.
            await self.assert_active()
            return result

    async def release(self) -> bool:
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        token = self.fencing_token
        if token is None:
            self.active = False
            return True
        released = False
        try:
            released = await self.supabase.release_execution_coordinator_lease(
                self.scope, self.owner_id, token
            )
        finally:
            self.active = False
            self.fencing_token = None
        if not released:
            logger.warning("Execution Coordinator release was not confirmed")
        return released
