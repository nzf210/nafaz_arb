"""Distributed recovery lease acquisition, renewal and release."""

import asyncio
import logging
import uuid

from redis.exceptions import WatchError

logger = logging.getLogger("executor.order_recovery")


class RecoveryLockMixin:
    """Owns the distributed recovery lease; never steals another owner token."""

        async def _with_order_recovery_lock(self, recovery_callback) -> bool:
            """Run a recovery callback under an owner-token Redis lease.
    
            A process-local asyncio lock is insufficient: two replicas can otherwise
            restore or reconcile the same venue orders concurrently. Redis' lock
            uses an owner token and compare-and-update Redis transactions; the heartbeat keeps
            the lease alive during slow venue/API reconciliation.
            """
            if self.redis is None or self.order_registry is None:
                return False
    
            lock_token = uuid.uuid4().hex
            lock_key = "arb:executor:order-recovery:lock"
            try:
                acquired = await self.redis.set(
                    lock_key, lock_token, nx=True, px=300_000
                )
            except Exception as exc:
                logger.critical(
                    "[Recovery] distributed lock unavailable; recovery skipped and "
                    "entries halted: %s",
                    exc,
                )
                self._halt_exposure_scope(
                    "*", f"distributed recovery lock unavailable: {type(exc).__name__}"
                )
                self._refresh_entry_gate()
                return False
    
            if not acquired:
                logger.warning(
                    "[Recovery] another executor owns the distributed recovery lease; "
                    "this instance remains entry-gated"
                )
                self._halt_exposure_scope(
                    "*", "distributed recovery is owned by another executor"
                )
                self._refresh_entry_gate()
                return False
    
            owner_task = asyncio.current_task()
            heartbeat = asyncio.create_task(
                self._renew_order_recovery_lock(lock_token, owner_task)
            )
            completed = False
            try:
                await recovery_callback()
                completed = True
            except Exception as exc:
                self._halt_exposure_scope(
                    "*", f"recovery callback failed: {type(exc).__name__}: {exc}"
                )
                self._refresh_entry_gate()
                raise
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
                try:
                    released = await self._release_order_recovery_lock(lock_token)
                    if not released:
                        raise RuntimeError("recovery lease is no longer owned by this executor")
                except Exception as exc:
                    # A lost/expired lease must never be treated as a successful
                    # release: retain the global entry gate until a later full pass.
                    logger.critical(
                        "[Recovery] distributed lock release failed; entries remain "
                        "halted until recovery can reacquire the lease: %s",
                        exc,
                    )
                    self._halt_exposure_scope(
                        "*", "distributed recovery lock release could not be verified"
                    )
                    self._refresh_entry_gate()
                    completed = False
            return completed
    
        async def _renew_order_recovery_lock(self, lock_token: str, owner_task) -> None:
            """Renew the lease only while its unique owner token is still present."""
            while True:
                await asyncio.sleep(60)
                try:
                    renewed = await self._update_order_recovery_lease(
                        lock_token, extend=True
                    )
                    if not renewed:
                        raise RuntimeError("Redis rejected recovery lease renewal")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.critical(
                        "[Recovery] distributed recovery lease lost; aborting this "
                        "recovery pass: %s",
                        exc,
                    )
                    self._halt_exposure_scope(
                        "*", "distributed recovery lease lost during reconciliation"
                    )
                    self._refresh_entry_gate()
                    if owner_task is not None and not owner_task.done():
                        owner_task.cancel()
                    return
    
        async def _update_order_recovery_lease(
            self, lock_token: str, *, extend: bool
        ) -> bool:
            """Atomically renew or release the lease only for its current owner."""
            lock_key = "arb:executor:order-recovery:lock"
            for _ in range(3):
                async with self.redis.pipeline(transaction=True) as pipe:
                    try:
                        await pipe.watch(lock_key)
                        current = await pipe.get(lock_key)
                        if isinstance(current, bytes):
                            current = current.decode("utf-8")
                        if current != lock_token:
                            await pipe.unwatch()
                            return False
                        pipe.multi()
                        if extend:
                            pipe.pexpire(lock_key, 300_000)
                        else:
                            pipe.delete(lock_key)
                        await pipe.execute()
                        return True
                    except WatchError:
                        continue
            return False
    
        async def _release_order_recovery_lock(self, lock_token: str) -> bool:
            """Compare-and-delete the lease; never delete another executor's lock."""
            return await self._update_order_recovery_lease(
                lock_token, extend=False
            )
