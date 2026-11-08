"""LifecycleMixin responsibilities for ArbitrageEngine."""

import asyncio
import json
import logging
import time

logger = logging.getLogger("executor.engine")


class LifecycleMixin:
    async def start(self):
        """Starts listeners and monitoring loops."""
        await self.initialize()
        self._running = True

        self._tasks.append(asyncio.create_task(self._listen_signals()))
        self._tasks.append(asyncio.create_task(self._listen_emergency_cancel()))
        self._tasks.append(asyncio.create_task(self._listen_force_close()))
        self._tasks.append(asyncio.create_task(self._listen_kill_switch()))
        self._tasks.append(asyncio.create_task(self._listen_balance_requests()))
        self._tasks.append(asyncio.create_task(self._monitoring_loop()))
        logger.info("Hybrid Arbitrage Engine started")

    async def stop(self):
        """Stops all running tasks and connections."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.supabase.enabled:
            await self.supabase.close()
        if self.client:
            await self.client.close()
        if self.redis:
            if hasattr(self.redis, "aclose"):
                await self.redis.aclose()
            else:
                await self.redis.close()
        logger.info("Hybrid Arbitrage Engine stopped")

    # -------------------------------------------------------------
    # Redis Pub/Sub Listeners
    # -------------------------------------------------------------

    async def _listen_signals(self):
        """Subscribes to arb:signal_notify channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:signal_notify")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    pair_tag = message.get("data")
                    if isinstance(pair_tag, bytes):
                        pair_tag = pair_tag.decode("utf-8")
                    if pair_tag:
                        await self.handle_signal_notification(pair_tag)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:signal_notify")
            await pubsub.close()

    async def _listen_emergency_cancel(self):
        """Subscribes to arb:emergency_cancel channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:emergency_cancel")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.handle_emergency_cancel(data)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:emergency_cancel")
            await pubsub.close()

    async def _listen_force_close(self):
        """Subscribes to arb:force_close channel."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:force_close")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.handle_force_close(data)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:force_close")
            await pubsub.close()


    async def _listen_balance_requests(self):
        """Handles read-only Telegram balance requests and stores short-lived replies."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:balance_request")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    try:
                        raw = message.get("data")
                        request = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                        request_id = str(request.get("request_id", "")).strip()
                        if not request_id or len(request_id) > 64 or not request_id.isalnum():
                            continue
                        reports = await self.client.fetch_balance_report()
                        payload = {"request_id": request_id, "timestamp_ms": int(time.time()*1000),
                                   "mock_mode": bool(getattr(self.config, "mock_mode", False)), "exchanges": reports}
                        await self.redis.set(f"arb:balance_reply:{request_id}", json.dumps(payload, allow_nan=False), ex=30)
                    except Exception:
                        logger.exception("failed to process Telegram balance request")
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:balance_request")
            await pubsub.close()

    async def _listen_kill_switch(self):
        """Subscribes to arb:kill_switch channel (operator ON/OFF switch)."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:kill_switch")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    await self.handle_kill_switch(data)
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:kill_switch")
            await pubsub.close()

    async def handle_kill_switch(self, message_data: str) -> None:
        """
        Handles the operator kill switch on arb:kill_switch.

        ON halts every new entry globally until an explicit OFF is received.
        Unlike the daily-loss breaker the kill switch does NOT auto-reset on a
        UTC rollover - only an operator OFF clears it.
        """
        action = "on"
        reason = "OPERATOR"
        try:
            parsed = json.loads(message_data)
            if isinstance(parsed, dict):
                action = str(parsed.get("action", action)).strip().lower()
                reason = str(parsed.get("reason", reason)) or reason
        except Exception:
            text = (message_data or "").strip().lower()
            if text in ("off", "resume", "clear"):
                action = "off"
            elif text:
                reason = message_data.strip()

        if action == "off":
            if self._trading_halted and self._halt_reason == "KILL_SWITCH":
                self._trading_halted = False
                self._halt_reason = ""
                logger.warning(f"[Kill Switch] Trading resumed ({reason})")
                await self.publish_status()
            return

        if not self._trading_halted:
            self._trading_halted = True
            self._halt_reason = "KILL_SWITCH"
            logger.critical(f"[Kill Switch] ALL new entries halted ({reason})")
            await self.publish_status()

    # -------------------------------------------------------------
    # State 1: Entry Order Pegging
    # -------------------------------------------------------------

    def _pair_lock(self, pair_tag: str) -> asyncio.Lock:
        """Returns the per-pair lock, creating it on first use."""
        lock = self._pair_locks.get(pair_tag)
        if lock is None:
            lock = asyncio.Lock()
            self._pair_locks[pair_tag] = lock
        return lock

    def _pair_venues(self, pair_tag: str) -> tuple[str, str]:
        """Best-effort (local_exchange, global_exchange) names for a pair tag."""
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        if pair_info is not None:
            return pair_info.local_exchange, pair_info.global_exchange
        pending = self.pending_entry_orders.get(pair_tag) or {}
        if pending:
            return pending.get("local_exchange", ""), pending.get("global_exchange", "")
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            return pos.local_exchange, pos.global_exchange
        return "", ""

