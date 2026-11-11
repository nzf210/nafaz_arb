"""recovery orphan helpers for the universal order lifecycle."""

import logging
from typing import Any, Dict

from executor.models import OrderRecord, OrderRole, PositionState

logger = logging.getLogger("executor.order_recovery")



class RecoveryOrphanMixin:
    """Focused order-recovery responsibilities."""

    def _position_id_for(self, pair_tag: str) -> str:
        """Returns the registry-level position identity for a pair.

        Deliberately pair-scoped and not a UUID: the registry stores the pair
        relationship, and the durable ``positions`` row keeps its own id.
        """
        return f"position:{pair_tag}" if pair_tag else ""

    @staticmethod
    def _market_scope(exchange: str, symbol: str) -> str:
        """Per-market gate key: blocks one symbol without blocking its venue."""
        if exchange and symbol:
            return f"{exchange}:{symbol}"
        return ""

    async def _orphan(
        self, order: Dict[str, Any], exchange: str, symbol: str, pair_tag: str, reason: str
    ) -> None:
        """Registers an orphan: never adopted, replaced or auto-cancelled.

        The halt scope follows the exposure the orphan can actually affect: its
        pair when known, otherwise just its market, and only the whole venue when
        even the symbol is unknown.
        """
        scope = pair_tag or self._market_scope(exchange, symbol) or exchange or "unknown"
        orphan_id = await self.order_registry.register_orphan(
            order, scope, reason, exchange=exchange, symbol=symbol
        )
        self._recovered_orphans[orphan_id] = {
            "orphan_id": orphan_id, "scope": scope, "exchange": exchange, "symbol": symbol,
        }
        self._halt_exposure_scope(scope, "orphan order requires manual reconciliation")
        logger.critical(
            "[Recovery] ORPHAN_ORDER %s on %s (%s): %s. Not adopted, not canceled, "
            "not replaced. New entries blocked on %s; monitoring and emergency "
            "de-risking remain available.",
            str(order.get("id") or order.get("clientOrderId") or "?"),
            exchange, symbol, reason, scope,
        )
        await self._publish_execution_failure(
            f"orphan:{orphan_id}",
            "ORPHAN_ORDER",
            f"Active order {order.get('id')} on {exchange} ({symbol}) could not be linked to a bot "
            f"position: {reason}. Manual reconciliation required. It was NOT adopted, replaced or canceled.",
            detail={
                "orphan_id": orphan_id,
                "exchange": exchange,
                "symbol": symbol,
                "exchange_order_id": str(order.get("id") or ""),
                "client_order_id": str(order.get("clientOrderId") or order.get("client_order_id") or ""),
                "side": str(order.get("side") or ""),
                "price": float(order.get("price") or 0.0),
                "amount": float(order.get("amount") or 0.0),
                "filled": float(order.get("filled") or 0.0),
                "scope": scope,
            },
            state=PositionState.RECONCILIATION_REQUIRED,
        )

    # --------------------------------------------------------- manual resolve

    async def resolve_orphan_order(
        self, orphan_id: str, pair_tag: str = "", reason: str = "MANUAL"
    ) -> bool:
        """Operator-only reconciliation of an orphan order.

        Attaches the order to a pair when ``pair_tag`` is given, otherwise
        acknowledges it as external. Either way the entry halt on its scope is
        released. Recovery never calls this: only a human can decide that an
        unlinkable order is safe.
        """
        orphans = await self.order_registry.orphans()
        orphan = orphans.get(orphan_id)
        if orphan is None:
            logger.warning("[Orphan] %s is not registered", orphan_id)
            return False
        scope = str(orphan.get("scope") or "")
        if pair_tag:
            await self.order_registry.register(
                OrderRecord(
                    local_order_id=OrderRecord.local_id_for(
                        orphan.get("exchange", ""),
                        orphan.get("client_order_id", ""),
                        orphan.get("exchange_order_id", ""),
                    ),
                    client_order_id=str(orphan.get("client_order_id") or ""),
                    exchange_order_id=str(orphan.get("exchange_order_id") or ""),
                    pair_key=pair_tag,
                    position_id=self._position_id_for(pair_tag),
                    exchange=str(orphan.get("exchange") or ""),
                    market_type=str(orphan.get("market_type") or "spot"),
                    symbol=str(orphan.get("symbol") or ""),
                    side=str(orphan.get("side") or ""),
                    order_type=str(orphan.get("order_type") or ""),
                    order_role=OrderRole.EXIT_SPOT.value,
                    price=float(orphan.get("price") or 0.0),
                    original_quantity=float(orphan.get("amount") or 0.0),
                    filled_quantity=float(orphan.get("filled") or 0.0),
                )
            )
        self._recovered_orphans.pop(orphan_id, None)
        await self.order_registry.remove_orphan(orphan_id)
        self._clear_exposure_halt(scope, "orphan resolved")
        await self._publish_execution_failure(
            f"orphan:{orphan_id}",
            "ORPHAN_ORDER_RESOLVED",
            f"Orphan order {orphan_id} resolved manually ({reason})"
            + (f" onto {pair_tag}" if pair_tag else " as external"),
            detail={"orphan_id": orphan_id, "pair_key": pair_tag, "reason": reason},
            state=PositionState.IDLE,
        )
        logger.warning("[Orphan] %s resolved manually (%s)", orphan_id, reason)
        return True


    async def retry_pending_order_recovery(self) -> bool:
        """Re-verifies every pending order and releases the gates it clears.

        The venue may become reachable long after startup, so a gate must be
        releasable without restarting the process. Returns True when nothing is
        left pending.
        """
        for local_id in list(await self.order_registry.pending_recovery_ids()):
            record = self._recovered_order_records.get(local_id)
            if record is None:
                record = await self.order_registry.load(local_id)
            if record is None:
                await self.order_registry.clear_pending_recovery(local_id)
                continue
            resolved = await self._retry_one_pending_order(record)
            if resolved and record.pair_key:
                remaining = await self.order_registry.pending_recovery_ids()
                if not any(
                    (self._recovered_order_records.get(other_id) or record).pair_key
                    == record.pair_key
                    for other_id in remaining
                ):
                    self.states[record.pair_key] = PositionState.IDLE
                    self._clear_exposure_halt(record.pair_key, "pending order verified")

        # A venue-scope gate from a failed open-order sweep is released by
        # re-running the sweep: the venue may be answering again now.
        if any(":" not in key for key in (self._recovery_scope_halts or {})):
            await self._reconcile_venue_open_orders()

        self._recovery_pending_orders = await self.order_registry.pending_recovery_ids()
        self._refresh_entry_gate()
        return not self._recovery_pending_orders and not self._recovery_scope_halts

    async def _retry_one_pending_order(self, record: OrderRecord) -> bool:
        """Re-runs venue verification for one record; True when it resolved."""
        token = None
        try:
            if not getattr(self.config, "mock_mode", False) and record.pair_key:
                token = self._bind_pair_client(self._pair_info_for_tag(record.pair_key))
            await self._reconcile_registered_order(record, record.exchange, record.symbol)
        except Exception:
            logger.exception("[Recovery] retry failed for order %s", record.local_order_id)
            return False
        finally:
            if token is not None:
                self._reset_pair_client(token)
        pending = await self.order_registry.pending_recovery_ids()
        return record.local_order_id not in pending

    # --------------------------------------------------------- halt plumbing

    def _risk_recovery_scope(self, scope: str) -> str:
        if scope == "*":
            return "global"
        venues = {
            str(getattr(self.config, "local_exchange_name", "") or "").lower(),
            str(getattr(self.config, "global_exchange_name", "") or "").lower(),
        }
        venues.update(str(name).lower() for name in (getattr(self.config, "exchanges", {}) or {}))
        for info in (getattr(self, "_pair_infos", {}) or {}).values():
            venues.add(str(getattr(info, "local_exchange", "") or "").lower())
            venues.add(str(getattr(info, "global_exchange", "") or "").lower())
        return "exchange" if str(scope).lower() in venues else "pair"

    def _halt_exposure_scope(self, scope: str, reason: str) -> None:
        """Halts new entries on a venue, pair or market without touching management.

        Entry is gated through ``_recovery_scope_halts`` (consulted by
        ``_venue_halted_reason``) so an open position keeps its monitoring, exit
        pegging and emergency de-risking while the gate is up.
        """
        if not scope:
            return
        halts = getattr(self, "_recovery_scope_halts", None)
        if halts is None:
            halts = self._recovery_scope_halts = {}
        halts[scope] = f"order recovery: {reason}"
        state_scope = self._risk_recovery_scope(scope)
        self._persist_risk_state("reconciliation_halt", scope=state_scope, exchange=scope if state_scope == "exchange" else None, pair_key=scope if state_scope == "pair" else None, reason=halts[scope], value={"source": "order_recovery"})
        self._entry_blocked = True

    def _clear_exposure_halt(self, scope: str, reason: str) -> None:
        halts = getattr(self, "_recovery_scope_halts", None)
        if halts is None:
            halts = self._recovery_scope_halts = {}
        if halts.pop(scope, None) is None:
            return
        state_scope = self._risk_recovery_scope(scope)
        self._persist_risk_state("reconciliation_halt", scope=state_scope, exchange=scope if state_scope == "exchange" else None, pair_key=scope if state_scope == "pair" else None, resolved=True)
        self._refresh_entry_gate()
        logger.warning("[Recovery] entry halt cleared for %s (%s)", scope, reason)

    def _refresh_entry_gate(self) -> None:
        """Recomputes the entry gate from every recovery reason currently known."""
        self._entry_blocked = bool(
            self._recovery_pending_orders or self._recovery_scope_halts
            or getattr(self, "_risk_persistence_failed", False)
        )
