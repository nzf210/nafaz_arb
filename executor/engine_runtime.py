"""engine runtime methods for arbitrage engine orchestration."""

import asyncio
import json
import time

from executor.engine_common import logger



class EngineRuntimeMixin:
    """Focused orchestration responsibilities, composed by ArbitrageEngine."""

    async def _exchange_reconciliation_loop(self):
        """Periodically reconcile registered venue order states and cumulative fills.

        This complements startup recovery: an order can fill, be canceled, or
        an operator can change exposure while the process is running. The loop
        deliberately reuses the existing recovery and position-verification
        paths rather than introducing a second order lifecycle implementation.
        """
        try:
            interval = max(
                15.0,
                float(getattr(self.config, "exchange_reconciliation_interval_seconds", 60.0) or 60.0),
            )
        except (TypeError, ValueError):
            interval = 60.0
        while self._running:
            try:
                async with self._exchange_reconciliation_lock:
                    if not self._recovery_complete:
                        # A replica that started while another owner held the
                        # lease must retry the full startup restore, not only
                        # order polling, before it can become READY.
                        self._recovery_complete = await self._run_startup_recovery()
                        if not self._recovery_complete:
                            self._halt_exposure_scope(
                                "*", "startup recovery is waiting for distributed ownership"
                            )
                            self._refresh_entry_gate()
                    else:
                        await self._run_order_recovery()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("[Reconciliation] periodic exchange reconciliation failed")
                self._halt_exposure_scope(
                    "*", f"exchange reconciliation failed: {type(exc).__name__}: {exc}"
                )
                self._refresh_entry_gate()
            await asyncio.sleep(interval)

    async def start(self):
        """Starts listeners and monitoring loops."""
        await self.initialize()
        self._running = True

        self._tasks.append(asyncio.create_task(self._listen_signals()))
        self._tasks.append(asyncio.create_task(self._process_signal_priority_queue()))
        self._tasks.append(asyncio.create_task(self._listen_emergency_cancel()))
        self._tasks.append(asyncio.create_task(self._listen_force_close()))
        self._tasks.append(asyncio.create_task(self._listen_kill_switch()))
        self._tasks.append(asyncio.create_task(self._listen_balance_requests()))
        self._tasks.append(asyncio.create_task(self._listen_recovery_requests()))
        self._tasks.append(asyncio.create_task(self._monitoring_loop()))
        self._tasks.append(asyncio.create_task(self._exchange_reconciliation_loop()))
        self._tasks.append(asyncio.create_task(self.exposure_reconciliation.run()))
        self._tasks.append(asyncio.create_task(self.trade_fill_reconciliation.run()))
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


    @staticmethod
    def _balance_request_id(raw) -> str:
        request = json.loads(
            raw if isinstance(raw, str) else raw.decode("utf-8")
        )
        request_id = str(request.get("request_id", "")).strip()
        if not request_id or len(request_id) > 64 or not request_id.isalnum():
            return ""
        return request_id

    async def _publish_balance_reply(self, raw) -> None:
        request_id = self._balance_request_id(raw)
        if not request_id:
            return
        reports = await self.client.fetch_balance_report()
        payload = {
            "request_id": request_id,
            "timestamp_ms": int(time.time() * 1000),
            "mock_mode": bool(getattr(self.config, "mock_mode", False)),
            "exchanges": reports,
        }
        await self.redis.set(
            f"arb:balance_reply:{request_id}",
            json.dumps(payload, allow_nan=False),
            ex=30,
        )

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

    async def _handle_recovery_request(self, raw) -> None:
        """Handle read-only retry and venue-verified orphan resolution requests."""
        try:
            request = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            request_id = str(request.get("request_id", "")).strip()
            action = str(request.get("action", "")).strip().lower()
            if not request_id or len(request_id) > 64 or not request_id.isalnum():
                return
            if action not in ("retry", "resolve_orphan"):
                return
            result = {"resolved": False}
            if action == "retry":
                acquired = await self._with_order_recovery_lock(self.retry_pending_order_recovery)
                if not acquired:
                    result["error"] = "Recovery sedang dijalankan replica lain atau lease tidak tersedia."
                else:
                    self._recovery_pending_orders = await self.order_registry.pending_recovery_ids()
                    unresolved_states = any(
                        state.value == "RECONCILIATION_REQUIRED"
                        for state in self.states.values()
                    )
                    result["resolved"] = (
                        not self._recovery_pending_orders
                        and not self._recovery_scope_halts
                        and not unresolved_states
                    )
            else:
                orphan_id = str(request.get("orphan_id", "")).strip()
                pair_tag = str(request.get("pair_tag", "")).strip()
                if not orphan_id or len(orphan_id) > 256:
                    return
                async def resolve():
                    result["resolved"] = await self.resolve_orphan_order(
                        orphan_id, pair_tag=pair_tag, reason="TELEGRAM_OPERATOR"
                    )
                acquired = await self._with_order_recovery_lock(resolve)
                if not acquired:
                    result["error"] = "Recovery lease sedang dimiliki replica lain; orphan tidak diubah."
                    result["resolved"] = False
            self._recovery_pending_orders = await self.order_registry.pending_recovery_ids()
            self._refresh_entry_gate()
            reply = {
                "request_id": request_id,
                "action": action,
                "resolved": bool(result["resolved"]),
                "error": str(result.get("error") or ""),
                "complete": (
                    not result.get("error")
                    and not self._recovery_pending_orders
                    and not self._recovery_scope_halts
                    and not any(
                        state.value == "RECONCILIATION_REQUIRED"
                        for state in self.states.values()
                    )
                ),
                "pending_orders": self._recovery_pending_orders,
                "scope_halts": dict(self._recovery_scope_halts),
                "timestamp_ms": int(time.time() * 1000),
            }
            await self.redis.set(
                f"arb:recovery_reply:{request_id}", json.dumps(reply, default=str), ex=30
            )
            await self.publish_status()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to process operator recovery request")
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

    @staticmethod
    def _parse_kill_switch_message(message_data: str) -> tuple:
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
        return action, reason

    async def _resume_from_kill_switch(self, reason: str) -> None:
        if not self._trading_halted or self._halt_reason != "KILL_SWITCH":
            return
        self._trading_halted = False
        self._halt_reason = ""
        halt_saved = self._persist_risk_state(
            "trading_halt", scope="global", resolved=True
        )
        self._manual_resume_day = self._loss_day
        resume_saved = self._persist_risk_state(
            "trading_resume", scope="global", reason="manual resume",
            value={"loss_day": self._loss_day},
        )
        if halt_saved and resume_saved:
            self.supabase.clear_risk_persistence_failure()
            self._risk_persistence_failed = False
            self._refresh_entry_gate()
        logger.warning(f"[Kill Switch] Trading resumed ({reason})")
        await self.publish_status()

    async def handle_kill_switch(self, message_data: str) -> None:
        """Handles the operator kill switch on arb:kill_switch."""
        action, reason = self._parse_kill_switch_message(message_data)
        if action == "off":
            await self._resume_from_kill_switch(reason)
            return
        if self._trading_halted and self._halt_reason == "KILL_SWITCH":
            return
        self._trading_halted = True
        self._halt_reason = "KILL_SWITCH"
        self._persist_risk_state(
            "trading_halt", scope="global", reason="KILL_SWITCH",
            value={"halt_reason": "KILL_SWITCH", "source": "operator"},
        )
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

    def _pair_info_for_tag(self, pair_tag: str):
        info = self._pair_infos.get(pair_tag)
        if info is not None:
            return info
        pending = self.pending_entry_orders.get(pair_tag) or self.pending_exit_orders.get(pair_tag) or {}
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            local_symbol = pos.symbol
            global_symbol = pos.global_symbol
            return type("RecoveredPairInfo", (), {
                "local_exchange": pos.local_exchange,
                "global_exchange": pos.global_exchange,
                "local_symbol": local_symbol,
                "global_symbol": global_symbol or self._resolve_global_symbol(local_symbol),
                "base_asset": pos.base_asset or local_symbol.split("/")[0],
                "quote_asset": local_symbol.split("/")[-1] if "/" in local_symbol else "IDR",
            })()
        if pending.get("local_exchange") and pending.get("global_exchange"):
            return type("PendingPairInfo", (), {
                "local_exchange": pending["local_exchange"],
                "global_exchange": pending["global_exchange"],
                "local_symbol": pending.get("symbol", ""),
                "global_symbol": pending.get("global_symbol", ""),
                "base_asset": pending.get("base_asset", ""),
                "quote_asset": pending.get("quote_asset", "IDR"),
            })()
        raise LookupError(f"exchange pair metadata unavailable for {pair_tag}")

    async def _run_pair_bound(self, pair_tag: str, operation, *args, **kwargs):
        if getattr(self.config, "mock_mode", False):
            return await operation(*args, **kwargs)
        pair_info = self._pair_info_for_tag(pair_tag)
        token = self._bind_pair_client(pair_info)
        try:
            return await operation(*args, **kwargs)
        finally:
            self._reset_pair_client(token)

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
