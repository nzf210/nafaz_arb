"""RecoveryMixin responsibilities for ArbitrageEngine."""

import json
import logging
import time

from executor.helpers import PENDING_ENTRY_KEY_PREFIX
from executor.models import ActivePosition, PositionState

logger = logging.getLogger("executor.engine")


class RecoveryMixin:
    async def _persist_pending_entry(self, pair_tag: str, order_info: dict) -> None:
        """Persists a pending entry order so a restart can adopt or cancel it."""
        if not self.redis:
            return
        try:
            await self.redis.set(
                PENDING_ENTRY_KEY_PREFIX + pair_tag, json.dumps(order_info)
            )
        except Exception:
            logger.exception(f"[Recovery] failed to persist pending entry for {pair_tag}")

    async def _clear_pending_entry(self, pair_tag: str) -> None:
        """Removes the persisted pending-entry record once the order is resolved."""
        if not self.redis:
            return
        try:
            await self.redis.delete(PENDING_ENTRY_KEY_PREFIX + pair_tag)
        except Exception:
            logger.exception(f"[Recovery] failed to clear pending entry for {pair_tag}")

    async def _recover_pending_entry_orders(self) -> None:
        """
        Reconciles persisted pending entry orders against the local exchange.

        A crash between placing the limit buy and registering it in memory
        forgets the resting order: it can fill with no hedge ever placed. On
        startup, orders still open on the venue are re-adopted; records whose
        order no longer exists are cleared.
        """
        try:
            keys = await self.redis.keys(PENDING_ENTRY_KEY_PREFIX + "*")
        except Exception:
            logger.exception("[Recovery] failed to scan arb:pending_entry:* keys")
            return
        for key in keys or []:
            pair_tag = key.split(PENDING_ENTRY_KEY_PREFIX, 1)[-1]
            try:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                order_info = json.loads(raw)
                if hasattr(self.client, "set_route"):
                    self.client.set_route(
                        order_info.get("local_exchange", ""),
                        order_info.get("global_exchange", ""),
                    )
                order_id = str(order_info.get("order_id", ""))
                symbol = str(order_info.get("symbol", ""))
                if not order_id or not symbol:
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._mark_failed(
                        pair_tag,
                        "ENTRY_ORDER_RECORD_INVALID",
                        "Persisted pending entry is missing order_id or symbol; record retained.",
                        detail={"order_id": order_id, "symbol": symbol},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
                    logger.critical(
                        f"[Recovery] malformed pending entry record for {pair_tag}; "
                        "Redis record retained for manual reconciliation"
                    )
                    continue
                status, filled = await self._resolve_venue_order_state(order_id, symbol)
                terminal_statuses = ("closed", "canceled", "cancelled", "rejected", "expired")
                if status == "unknown":
                    # Missing/ambiguous venue state is not proof that an order is gone.
                    # Keep both durable and in-memory records so force-close cannot
                    # silently skip an order whose venue state is unknown.
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._mark_failed(
                        pair_tag,
                        "ENTRY_ORDER_STATE_UNKNOWN",
                        f"Cannot verify local order {order_id}; pending record retained.",
                        detail={"order_id": order_id, "symbol": symbol},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
                    logger.critical(
                        f"[Recovery] cannot verify venue state of order {order_id} "
                        f"({pair_tag}) - MANUAL CHECK REQUIRED"
                    )
                    continue

                order_info["filled"] = float(filled or 0.0)
                if status == "open":
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = (
                        PositionState.ENTRY_PARTIAL
                        if order_info["filled"] > 0
                        else PositionState.ENTRY_PENDING
                    )
                    # Keep the Redis key: in-memory adoption alone is not restart-safe.
                    await self._persist_pending_entry(pair_tag, order_info)
                    logger.warning(
                        f"[Recovery] re-adopted pending entry order {order_id} "
                        f"for {pair_tag} (filled={filled})"
                    )
                    continue

                if status in terminal_statuses:
                    # A terminal order can still contain fills that occurred before
                    # cancellation or before the process crashed. Hedge that final
                    # cumulative fill before releasing the unused reservation.
                    self.pending_entry_orders[pair_tag] = order_info
                    self.states[pair_tag] = PositionState.ENTRY_PENDING
                    await self._handle_entry_fills(
                        pair_tag,
                        order_info,
                        {"id": order_id, "status": status, "filled": order_info["filled"]},
                    )
                    if self.states.get(pair_tag) in (
                        PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
                    ):
                        logger.critical(
                            f"[Recovery] fill reconciliation failed for {pair_tag}; "
                            "pending record retained"
                        )
                        continue
                    await self._finalize_entry_order(pair_tag, status)
                    logger.info(
                        f"[Recovery] reconciled terminal order {order_id} for "
                        f"{pair_tag} (status={status}, filled={filled})"
                    )
                    continue

                self.pending_entry_orders[pair_tag] = order_info
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                self._mark_failed(
                    pair_tag,
                    "ENTRY_ORDER_STATE_UNSUPPORTED",
                    f"Unsupported exchange order status '{status}' for {order_id}.",
                    detail={"order_id": order_id, "symbol": symbol, "status": status},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                logger.critical(
                    f"[Recovery] unsupported order status '{status}' for {order_id}; "
                    "pending record retained"
                )
            except Exception:
                logger.exception(f"[Recovery] failed to restore pending entry from {key}")

    async def _resolve_venue_order_state(self, order_id: str, symbol: str) -> tuple:
        """Returns (status, filled) for a venue order; ('unknown', None) if it cannot be fetched."""
        try:
            order = await self.client.fetch_local_order(order_id, symbol)
        except Exception as exc:
            logger.warning(f"[Recovery] fetch order {order_id} failed: {exc}")
            return "unknown", None
        if not isinstance(order, dict) or not order.get("id") and order_id not in str(order):
            # A missing response is ambiguous (history retention, API lag, or
            # wrong symbol); never interpret it as a safely canceled order.
            return "unknown", None
        return str(order.get("status", "unknown")), float(order.get("filled", 0.0) or 0.0)

    async def _recover_active_positions(self) -> None:
        """
        Restores open positions from Redis after a restart/redeploy.

        Without this the engine forgets every hedged position it still holds on
        the exchange: no exit pegging, no funding accrual, and no force-close
        target. Recovered positions resume as HEDGED_ACTIVE so the monitoring
        loop immediately resumes managing them.
        """
        recovered = 0
        try:
            keys = await self.redis.keys("arb:active_pos:*")
        except Exception:
            logger.exception("[Recovery] failed to scan arb:active_pos:* keys")
            return

        for key in keys or []:
            pair_tag = key.split("arb:active_pos:", 1)[-1]
            try:
                raw = await self.redis.get(key)
                if not raw:
                    continue
                pos = ActivePosition.from_dict(json.loads(raw))
                if pos.qty <= 0:
                    # A zero/invalid Redis quantity is not proof that the exchange
                    # is flat. Retain the record and require explicit reconciliation.
                    self.active_positions[pair_tag] = pos
                    pos.state = "RECONCILIATION_REQUIRED"
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    await self.redis.set(key, json.dumps(pos.to_dict()))
                    self._mark_failed(
                        pair_tag,
                        "RECOVERED_POSITION_QTY_INVALID",
                        f"Recovered position quantity is invalid ({pos.qty}); exchange exposure must be checked.",
                        detail={"qty": pos.qty, "symbol": pos.symbol},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
                    logger.critical(
                        f"[Recovery] retaining {pair_tag}: recovered qty is {pos.qty}; "
                        "manual reconciliation required"
                    )
                    continue

                self.active_positions[pair_tag] = pos
                pos_state_str = getattr(pos, "state", "HEDGED_ACTIVE")
                if pos_state_str == "RECONCILIATION_REQUIRED":
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._failure_reasons[pair_tag] = {
                        "reason": "RESTORED_RECONCILIATION_REQUIRED",
                        "message": "Position restored from Redis with RECONCILIATION_REQUIRED state. Manual intervention required.",
                        "detail": {"qty": pos.qty, "symbol": pos.symbol},
                        "timestamp_ms": int(time.time() * 1000),
                    }
                    logger.critical(
                        f"[Recovery] Position {pair_tag} has RECONCILIATION_REQUIRED status! "
                        f"Manual intervention required; skipping automatic active exit management."
                    )
                else:
                    self.states[pair_tag] = PositionState.HEDGED_ACTIVE
                recovered += 1
                logger.warning(
                    f"[Recovery] Restored position {pair_tag}: qty={pos.qty}, "
                    f"entry_idr={pos.entry_price_idr}, short_entry_usdt={pos.short_entry_usdt}, "
                    f"state={self.states[pair_tag].value}"
                )
                self.supabase.position_opened(
                    pair_tag,
                    pos,
                    getattr(self, "_pair_infos", {}).get(pair_tag),
                    is_mock=bool(getattr(self.config, "mock_mode", False)),
                )
            except Exception as exc:
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                self._mark_failed(
                    pair_tag,
                    "ACTIVE_POSITION_RECORD_INVALID",
                    f"Persisted active position cannot be parsed: {exc}",
                    detail={"redis_key": key},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                logger.exception(
                    f"[Recovery] invalid active position record {key}; Redis key retained"
                )

        if recovered:
            logger.warning(
                f"[Recovery] {recovered} active position(s) restored - resuming management"
            )

