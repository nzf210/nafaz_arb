"""Durable risk-state persistence, restoration, and venue circuit breakers."""

import asyncio
import json
import logging
import time

from executor.models import PositionState

logger = logging.getLogger("executor.engine")


class RiskStateMixin:
    """Persistence and lifecycle controls for durable risk state."""

    def _persist_risk_state(
        self, state_type: str, *, scope: str = "global", exchange: str = None,
        pair_key: str = None, reason: str = "", value=None, resolved: bool = False,
    ) -> bool:
        """Persist safety state synchronously to the durable local outbox."""
        writer = getattr(self, "supabase", None)
        if writer is None or not getattr(writer, "enabled", False):
            if not getattr(self.config, "mock_mode", False):
                self._entry_blocked = True
                self._risk_persistence_failed = True
                logger.critical("LIVE entry blocked: durable risk-state storage is unavailable")
            return False
        try:
            ok = writer.risk_state(
                state_type=state_type, scope=scope, exchange=exchange,
                pair_key=pair_key, reason=reason, value=value, resolved=resolved,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
        except Exception:
            logger.exception("Durable risk-state write raised for %s", state_type)
            ok = False
        if not ok and not getattr(self.config, "mock_mode", False):
            try:
                writer.mark_risk_persistence_failure()
            except Exception:
                logger.exception("Unable to persist risk journal failure sentinel")
            self._entry_blocked = True
            self._risk_persistence_failed = True
            logger.critical("LIVE entry blocked: risk state could not be journaled")
        return bool(ok)

    def _restore_daily_loss_record(self, value) -> None:
        state = value if isinstance(value, dict) else {}
        current_day = self._utc_day()
        if state.get("loss_day") == current_day:
            self._loss_day = str(state["loss_day"])
            self._daily_realized_loss_usdt = max(
                0.0, float(state.get("loss_usdt", 0.0))
            )
            return
        self._loss_day = current_day
        self._daily_realized_loss_usdt = 0.0

    def _restore_exchange_halt_record(self, exchange: str, reason: str, value) -> None:
        if isinstance(value, dict) and value.get("source") == "rate_limit_cooldown":
            remaining = float(value.get("expires_at", 0.0)) - time.time()
            if remaining > 0:
                self._pause_venue_for_cooldown(exchange, remaining)
            else:
                self._persist_risk_state(
                    "exchange_halt", scope="exchange", exchange=exchange, resolved=True
                )
            return
        self._exchange_halted[exchange] = reason

    def _restore_position_failure_record(self, pair_key: str, reason: str, value) -> None:
        failure = value if isinstance(value, dict) else {}
        self._failure_reasons[pair_key] = failure or {
            "reason": reason,
            "message": reason,
            "detail": {},
            "timestamp_ms": int(time.time() * 1000),
        }
        restored_state = str(failure.get("state") or "RECONCILIATION_REQUIRED")
        try:
            self.states[pair_key] = PositionState(restored_state)
        except ValueError:
            self.states[pair_key] = PositionState.RECONCILIATION_REQUIRED
        if self.states[pair_key] == PositionState.RECONCILIATION_REQUIRED:
            self._recovery_scope_halts[pair_key] = reason or (
                "restored unresolved position failure"
            )

    def _restore_global_risk_record(
        self, kind: str, value, reason: str
    ) -> bool:
        if kind == "trading_halt":
            self._trading_halted = True
            self._halt_reason = str(
                (value or {}).get("halt_reason") or reason or "RESTORED_MANUAL_HALT"
            )
            return True
        if kind == "trading_resume":
            self._manual_resume_day = str(
                (value or {}).get("loss_day") or self._utc_day()
            )
            return True
        if kind == "daily_loss":
            self._restore_daily_loss_record(value)
            return True
        return False

    def _restore_exchange_risk_record(
        self, kind: str, exchange: str, reason: str, value
    ) -> bool:
        if not exchange:
            return False
        if kind == "exchange_halt":
            self._restore_exchange_halt_record(exchange, reason, value)
            return True
        if kind == "consecutive_order_failures":
            self._consecutive_order_failures[exchange] = max(
                0, int((value or {}).get("count", 0))
            )
            return True
        return False

    def _restore_reconciliation_halt(
        self, scope: str, exchange: str, pair_key: str, reason: str
    ) -> None:
        key = "*" if scope == "global" else (
            exchange if scope == "exchange" else pair_key
        )
        if not key:
            return
        self._recovery_scope_halts[key] = reason
        if key == "*":
            self._entry_blocked = True

    def _restore_risk_record(self, row: dict) -> None:
        kind = str(row.get("state_type") or "")
        scope = str(row.get("scope") or "")
        exchange = str(row.get("exchange") or "").lower()
        pair_key = str(row.get("pair_key") or "")
        value = row.get("value")
        reason = str(row.get("reason") or "")
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (TypeError, ValueError):
                pass

        if scope == "global" and self._restore_global_risk_record(
            kind, value, reason
        ):
            return
        if scope == "exchange" and self._restore_exchange_risk_record(
            kind, exchange, reason, value
        ):
            return
        if kind == "reconciliation_halt":
            self._restore_reconciliation_halt(scope, exchange, pair_key, reason)
        elif kind == "position_failure" and pair_key:
            self._restore_position_failure_record(pair_key, reason, value)

    def _restore_risk_persistence_failure(self, writer) -> None:
        if not getattr(writer, "risk_persistence_failed", lambda: False)():
            return
        self._risk_persistence_failed = True
        self._trading_halted = True
        self._halt_reason = "RISK_STATE_PERSISTENCE_FAILED"
        self._entry_blocked = True

    async def _restore_daily_loss_ledger(self, writer) -> None:
        self._daily_realized_loss_usdt = await writer.fetch_daily_realized_loss_usdt(
            self._utc_day(), is_mock=bool(getattr(self.config, "mock_mode", False))
        )
        self._loss_day = self._utc_day()
        self._risk_state_restored = True
        limit = float(getattr(self.config, "max_daily_loss_usdt", 0.0) or 0.0)
        if self._halt_reason == "DAILY_LOSS_LIMIT" and (
            limit <= 0 or self._daily_realized_loss_usdt < limit
        ):
            self._trading_halted = False
            self._halt_reason = ""
            self._persist_risk_state("trading_halt", scope="global", resolved=True)
        manual_resume_today = getattr(self, "_manual_resume_day", None) == self._loss_day
        if limit > 0 and self._daily_realized_loss_usdt >= limit and (
            not self._trading_halted and not manual_resume_today
        ):
            self._trading_halted = True
            self._halt_reason = "DAILY_LOSS_LIMIT"
            self._persist_risk_state(
                "trading_halt",
                scope="global",
                reason="DAILY_LOSS_LIMIT",
                value={
                    "halt_reason": "DAILY_LOSS_LIMIT",
                    "loss_usdt": self._daily_realized_loss_usdt,
                    "loss_day": self._loss_day,
                },
            )
        self._roll_loss_day_if_needed()

    async def _restore_risk_state(self) -> None:
        """Restore unresolved durable halts before recovery/listeners can accept signals."""
        writer = getattr(self, "supabase", None)
        if writer is None or not getattr(writer, "enabled", False):
            self._risk_state_restored = True
            if not getattr(self.config, "mock_mode", False):
                self._trading_halted = True
                self._halt_reason = "RISK_STATE_PERSISTENCE_UNAVAILABLE"
                self._entry_blocked = True
                self._risk_persistence_failed = True
                logger.critical(
                    "LIVE startup fail-closed: configure durable Supabase risk-state storage"
                )
            return
        rows = await writer.fetch_active_risk_states(
            is_mock=bool(getattr(self.config, "mock_mode", False))
        )
        for row in rows:
            self._restore_risk_record(row)
        self._restore_risk_persistence_failure(writer)
        await self._restore_daily_loss_ledger(writer)
        logger.warning("Restored %d unresolved durable risk-state records", len(rows))

    def _record_order_success(self, exchange: str) -> None:
        """Clears the consecutive-failure counter for an exchange after a clean order."""
        if exchange:
            self._consecutive_order_failures.pop(exchange, None)
        self._persist_risk_state("consecutive_order_failures", scope="exchange", exchange=exchange, value={"count": 0}, resolved=True)

    def _record_order_failure(self, exchange: str, reason: str) -> None:
        """
        Counts a placement failure per exchange and halts the venue once the
        consecutive-failure threshold is crossed. Halting prevents the engine
        from repeatedly hitting a broken venue while open positions stay managed.
        """
        if not exchange:
            return

        # Venue-side business rejections describe this order, not venue health.
        # They must reject the opportunity without degrading an otherwise healthy CEX.
        normalized_reason = str(reason or "").strip().lower().replace("-", "_")
        business_rejections = (
            "insufficient_balance",
            "insufficient balance",
            "insufficient funds",
            "insufficientfunds",
            "min_notional",
            "minimum notional",
            "invalid_symbol",
            "invalid symbol",
        )
        if any(marker in normalized_reason for marker in business_rejections):
            logger.info(
                "[Risk Control] Business order rejection on %s is not counted "
                "toward the venue failure breaker: %s",
                exchange,
                reason,
            )
            return

        threshold = int(getattr(self.config, "max_consecutive_order_failures", 0) or 0)
        if threshold <= 0:
            return
        count = self._consecutive_order_failures.get(exchange, 0) + 1
        self._consecutive_order_failures[exchange] = count
        self._persist_risk_state("consecutive_order_failures", scope="exchange", exchange=exchange, reason=reason, value={"count": count})
        if count >= threshold:
            self._exchange_halted[exchange] = f"{count} consecutive order failures ({reason})"
            self._persist_risk_state("exchange_halt", scope="exchange", exchange=exchange, reason=self._exchange_halted[exchange], value={"source": "consecutive_order_failures"})
            logger.critical(
                f"[Risk Control] Exchange {exchange} HALTED for new entries: "
                f"{count} consecutive order failures ({reason})"
            )
            # Fire-and-forget status refresh; caller context may not be a coroutine.
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._maybe_publish_status())
            except RuntimeError:
                pass

    def resume_exchange(self, exchange: str, reason: str = "MANUAL") -> bool:
        """Clears a venue halt; returns True when a halt was actually removed."""
        if exchange in self._exchange_halted:
            del self._exchange_halted[exchange]
            self._persist_risk_state("exchange_halt", scope="exchange", exchange=exchange, resolved=True)
            self._consecutive_order_failures.pop(exchange, None)
            self._persist_risk_state("consecutive_order_failures", scope="exchange", exchange=exchange, resolved=True)
            logger.warning(f"[Risk Control] Exchange {exchange} resumed ({reason})")
            return True
        return False

    def _pause_venue_for_cooldown(self, exchange: str, seconds: float) -> None:
        """Pauses new entries on a venue for a bounded cooldown, then auto-clears.

        Used for rate limits: unlike the consecutive-failure halt this expires on
        its own so a transient throttle cannot stop trading until a manual
        resume, while open positions remain managed throughout.
        """
        if not exchange:
            return
        seconds = max(1.0, float(seconds))
        self._exchange_halted[exchange] = f"rate limited ({seconds:.0f}s cooldown)"
        self._persist_risk_state("exchange_halt", scope="exchange", exchange=exchange, reason=self._exchange_halted[exchange], value={"source": "rate_limit_cooldown", "seconds": seconds, "expires_at": time.time() + seconds})
        token = self._venue_cooldown_epoch = getattr(self, "_venue_cooldown_epoch", 0) + 1

        async def _clear_after_cooldown():
            await asyncio.sleep(seconds)
            # Only clear the halt this task installed; a newer cooldown or a
            # manual halt must not be undone by a stale timer.
            if getattr(self, "_venue_cooldown_epoch", 0) != token:
                return
            if self._exchange_halted.get(exchange, "").startswith("rate limited"):
                del self._exchange_halted[exchange]
                self._persist_risk_state(
                    "exchange_halt", scope="exchange", exchange=exchange, resolved=True
                )
                logger.warning(
                    f"[Risk Control] Exchange {exchange} rate-limit cooldown elapsed; "
                    "entries re-enabled"
                )

        try:
            loop = asyncio.get_running_loop()
            self._venue_cooldown_tasks = getattr(self, "_venue_cooldown_tasks", {})
            previous = self._venue_cooldown_tasks.get(exchange)
            if previous is not None and not previous.done():
                previous.cancel()
            self._venue_cooldown_tasks[exchange] = loop.create_task(_clear_after_cooldown())
        except RuntimeError:
            # No event loop (sync caller): fall back to a plain counter-based
            # halt that the next failure/success path will clear.
            pass
