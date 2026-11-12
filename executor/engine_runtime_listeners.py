"""Redis listener and signal-queue methods for the arbitrage engine."""

import asyncio
import json

from executor.engine_common import logger


class EngineRuntimeListenersMixin:
    """Owns Redis pub/sub listeners and inbound control-message loops."""

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
                        self._signal_queue_event.set()
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:signal_notify")
            await pubsub.close()

    async def _coalesce_signal_batch(
        self, quiet_window: float = 0.02, max_batch_window: float = 0.1
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max_batch_window
        quiet_deadline = loop.time() + quiet_window
        while loop.time() < deadline:
            timeout = min(quiet_deadline - loop.time(), deadline - loop.time())
            if timeout <= 0:
                return
            try:
                await asyncio.wait_for(
                    self._signal_queue_event.wait(), timeout=timeout
                )
                self._signal_queue_event.clear()
                quiet_deadline = loop.time() + quiet_window
            except asyncio.TimeoutError:
                return

    async def _claim_ranked_signals(self, queue_key: str):
        try:
            return await self.redis.zpopmax(queue_key, count=100)
        except Exception:
            logger.exception("[Signal Priority] failed to claim ranked signals")
            return None

    async def _process_ranked_signal(self, raw_tag, score) -> None:
        pair_tag = (
            raw_tag.decode("utf-8")
            if isinstance(raw_tag, bytes)
            else str(raw_tag)
        )
        logger.info(
            "[Signal Priority] executing candidate pair=%s estimated_net_profit=%.8f USDT",
            pair_tag, float(score),
        )
        try:
            await self.handle_signal_notification(pair_tag)
        except Exception:
            logger.exception(
                "[Signal Priority] signal handler failed for %s", pair_tag
            )

    async def _process_signal_priority_queue(self):
        """Drain eligible signals in descending estimated net-profit order."""
        queue_key = "arb:signal_priority"
        while self._running:
            await self._signal_queue_event.wait()
            self._signal_queue_event.clear()
            await self._coalesce_signal_batch()
            while self._running:
                ranked = await self._claim_ranked_signals(queue_key)
                if not ranked:
                    break
                for raw_tag, score in ranked:
                    await self._process_ranked_signal(raw_tag, score)

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
                        await self._publish_balance_reply(message.get("data"))
                    except Exception:
                        logger.exception("failed to process Telegram balance request")
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:balance_request")
            await pubsub.close()

    async def _listen_recovery_requests(self):
        """Listen for Telegram requests to re-run read-only recovery."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:recovery_request")
        try:
            while self._running:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if message and message.get("type") == "message":
                    await self._handle_recovery_request(message.get("data"))
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:recovery_request")
            await pubsub.close()

    async def _listen_spot_order_limits(self):
        """Apply Telegram limit changes immediately without restarting executor."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:executor:spot_order_limit_update")
        try:
            while self._running:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message.get("type") == "message":
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                    try:
                        update = json.loads(data or "{}")
                        if isinstance(update, dict) and self._apply_spot_order_limit(
                            update.get("exchange"), update.get("max_open_spot_orders")
                        ):
                            await self.publish_status()
                    except (TypeError, ValueError, json.JSONDecodeError):
                        logger.warning("ignoring malformed spot order limit update")
                await asyncio.sleep(0.01)
        finally:
            await pubsub.unsubscribe("arb:executor:spot_order_limit_update")
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

