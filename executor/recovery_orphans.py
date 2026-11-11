"""recovery orphan helpers for the universal order lifecycle."""

import logging
import json
import math
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

    async def _orphan_is_terminal_zero_fill(self, orphan: Dict[str, Any]) -> bool:
        """Require exact venue proof that an orphan can no longer create exposure."""
        exchange = str(orphan.get("exchange") or "").lower()
        symbol = str(orphan.get("symbol") or "")
        market_type = str(orphan.get("market_type") or "spot").lower()
        order_id = str(orphan.get("exchange_order_id") or "")
        client_id = str(orphan.get("client_order_id") or "")
        client = self._client_for_exchange(exchange)
        if client is None or not symbol or not (order_id or client_id):
            return False
        try:
            if market_type == "perpetual":
                fetch = getattr(client, "fetch_global_order", None)
                by_client = getattr(client, "fetch_global_order_by_client_id", None)
            else:
                fetch = getattr(client, "fetch_local_order", None)
                by_client = getattr(client, "fetch_local_order_by_client_id", None)
            if order_id and callable(fetch):
                venue_order = await fetch(order_id, symbol)
            elif client_id and callable(by_client):
                venue_order = await by_client(client_id, symbol)
            else:
                return False
        except Exception:
            logger.exception("[Orphan] venue verification failed for %s", orphan.get("orphan_id"))
            return False
        if not isinstance(venue_order, dict):
            return False
        if order_id and str(venue_order.get("id") or "") != order_id:
            return False
        observed_client_id = str(venue_order.get("clientOrderId") or venue_order.get("client_order_id") or "")
        if client_id and observed_client_id != client_id:
            return False
        status = str(venue_order.get("status") or "").lower()
        if status not in ("closed", "filled", "canceled", "cancelled", "rejected", "expired"):
            return False
        try:
            filled = float(venue_order.get("filled", 0.0) or 0.0)
        except (TypeError, ValueError):
            return False
        return math.isfinite(filled) and filled <= 1e-12

    async def _scope_has_unresolved_recovery(self, scope: str) -> bool:
        """Do not clear a shared halt while another incident still owns its scope."""
        for orphan in (await self.order_registry.orphans()).values():
            if str(orphan.get("scope") or "") == scope:
                return True
        pending = await self.order_registry.pending_recovery_ids()
        for local_id in pending:
            record = self._recovered_order_records.get(local_id)
            if record is None:
                record = await self.order_registry.load(local_id)
            if record is None:
                return True
            record_scope = record.pair_key or self._market_scope(record.exchange, record.symbol) or record.exchange
            if record_scope == scope or record.exchange == scope:
                return True
        if self.states.get(scope) == PositionState.RECONCILIATION_REQUIRED:
            return True
        return False

    async def resolve_orphan_order(
        self, orphan_id: str, pair_tag: str = "", reason: str = "MANUAL"
    ) -> bool:
        """Resolve only after exact venue lookup proves a terminal zero-fill order."""
        orphans = await self.order_registry.orphans()
        orphan = orphans.get(orphan_id)
        if orphan is None:
            logger.warning("[Orphan] %s is not registered", orphan_id)
            return False
        if not await self._orphan_is_terminal_zero_fill(orphan):
            logger.critical(
                "[Orphan] refusing to resolve %s: venue has not proven terminal zero-fill state",
                orphan_id,
            )
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
                    filled_quantity=0.0,
                    status="canceled",
                )
            )
        await self.order_registry.remove_orphan(orphan_id)
        self._recovered_orphans.pop(orphan_id, None)
        if not await self._scope_has_unresolved_recovery(scope):
            self._clear_exposure_halt(scope, "orphan venue state verified terminal with zero fill")
        await self._publish_execution_failure(
            f"orphan:{orphan_id}",
            "ORPHAN_ORDER_RESOLVED",
            f"Orphan order {orphan_id} resolved manually ({reason})"
            + (f" onto {pair_tag}" if pair_tag else " as external"),
            detail={"orphan_id": orphan_id, "pair_key": pair_tag, "reason": reason, "venue_verified": True},
            state=PositionState.IDLE,
        )
        logger.warning("[Orphan] %s resolved manually (%s)", orphan_id, reason)
        return True

    async def retry_pending_order_recovery(self) -> bool:
        """Re-run read-only recovery; never submit, cancel, or replace orders.

        A verified order lookup alone is insufficient to clear a pair halt.
        Position, fill, and aggregate exposure checks must also pass.
        """
        touched_pairs = {
            pair_tag for pair_tag, state in self.states.items()
            if state == PositionState.RECONCILIATION_REQUIRED
        }
        for local_id in await self.order_registry.pending_recovery_ids():
            record = self._recovered_order_records.get(local_id)
            if record is None:
                record = await self.order_registry.load(local_id)
            if record is None:
                self._halt_exposure_scope("*", f"pending recovery record {local_id} is missing")
                continue
            if record.pair_key:
                touched_pairs.add(record.pair_key)
            await self._retry_one_pending_order(record)

        if any(scope == "*" or ":" not in scope for scope in (self._recovery_scope_halts or {})):
            await self._reconcile_venue_open_orders()

        self._recovery_pending_orders = await self.order_registry.pending_recovery_ids()
        verified_positions = set()
        try:
            verified_positions = await self._verify_recovered_active_positions()
            if not getattr(self.config, "mock_mode", False):
                await self.trade_fill_reconciliation.reconcile_once()
                await self.exposure_reconciliation.reconcile_once()
        except Exception as exc:
            logger.exception("[Recovery] full reconciliation retry failed")
            self._halt_exposure_scope("*", f"full reconciliation retry failed: {type(exc).__name__}: {exc}")

        for pair_tag in touched_pairs:
            if pair_tag in verified_positions:
                await self._clear_resolved_pair_failure(pair_tag)

        self._recovery_pending_orders = await self.order_registry.pending_recovery_ids()
        self._refresh_entry_gate()
        await self.publish_status()
        return not self._recovery_pending_orders and not self._recovery_scope_halts

    async def _clear_resolved_pair_failure(self, pair_tag: str) -> bool:
        """Clear a pair incident only after order, fill, and exposure checks pass."""
        if self.states.get(pair_tag) != PositionState.RECONCILIATION_REQUIRED:
            return False
        for local_id in self._recovery_pending_orders:
            record = self._recovered_order_records.get(local_id)
            if record is None:
                record = await self.order_registry.load(local_id)
            if record is None or record.pair_key == pair_tag:
                return False
        orphans = await self.order_registry.orphans()
        if any(str(item.get("scope") or "") == pair_tag for item in orphans.values()):
            return False
        if getattr(self.exposure_reconciliation, "_reported", {}):
            return False
        if any(scope == pair_tag for scope in getattr(self.trade_fill_reconciliation, "_reported", {}).values()):
            return False
        position = self.active_positions.get(pair_tag)
        if position is None or self.redis is None:
            return False

        restored_state = PositionState.EXIT_PENDING if pair_tag in self.pending_exit_orders else PositionState.HEDGED_ACTIVE
        position.state = restored_state.value
        try:
            await self.redis.set(f"arb:active_pos:{pair_tag}", json.dumps(position.to_dict()))
        except Exception:
            logger.exception("[Recovery] failed to persist reconciled position state for %s", pair_tag)
            return False

        if not self._persist_risk_state("position_failure", scope="position", pair_key=pair_tag, resolved=True):
            if not getattr(self.config, "mock_mode", False):
                return False
        self.states[pair_tag] = restored_state
        self._failure_reasons.pop(pair_tag, None)
        self._clear_exposure_halt(pair_tag, "orders, fills and exposure verified")
        logger.warning("[Recovery] reconciliation incident cleared for %s (%s)", pair_tag, restored_state.value)
        return True
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
