"""recovery registry helpers for the universal order lifecycle."""

import asyncio
import logging
import time
from typing import Any, Dict

from executor.models import OrderRecord, PositionState, RecoveryStatus

logger = logging.getLogger("executor.order_recovery")



class RecoveryRegistryMixin:
    """Focused order-recovery responsibilities."""

    async def _run_order_recovery(self) -> None:
        """Run periodic order reconciliation under the shared Redis lease."""
        await self._with_order_recovery_lock(
            self._run_order_recovery_with_lock
        )

    async def _run_startup_recovery(self) -> bool:
        """Restore all startup state while holding one cross-instance lease."""
        return await self._with_order_recovery_lock(
            self._run_startup_recovery_with_lock
        )

    async def _run_startup_recovery_with_lock(self) -> None:
        """Restore positions, pending entries and orders as one exclusive pass."""
        await self._recover_active_positions()
        await self._recover_pending_entry_orders()
        await self._run_order_recovery_with_lock()
        if not getattr(self.config, "mock_mode", False):
            await self._verify_recovered_active_positions()

    async def _with_order_recovery_lock(self, recovery_callback) -> bool:
        """Run a recovery callback under an owner-token Redis lease.

        A process-local asyncio lock is insufficient: two replicas can otherwise
        restore or reconcile the same venue orders concurrently. Redis' lock
        uses an owner token and compare-and-delete release; the heartbeat keeps
        the lease alive during slow venue/API reconciliation.
        """
        if self.redis is None or self.order_registry is None:
            return False

        lock = self.redis.lock(
            "arb:executor:order-recovery:lock",
            timeout=300,
            thread_local=False,
        )
        try:
            acquired = await lock.acquire(blocking=False)
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
            self._renew_order_recovery_lock(lock, owner_task)
        )
        completed = False
        try:
            await recovery_callback()
            completed = True
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                await lock.release()
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

    async def _renew_order_recovery_lock(self, lock, owner_task) -> None:
        """Renew the recovery lease and abort work if ownership is lost."""
        while True:
            await asyncio.sleep(60)
            try:
                renewed = await lock.extend(300, replace_ttl=True)
                if renewed is False:
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

    async def _run_order_recovery_with_lock(self) -> None:
        """Reconciles durable registry + positions + venue open orders."""
        registry_loaded = True
        try:
            records = await self.order_registry.active_records()
        except Exception as exc:
            logger.exception("[Recovery] failed to load the durable order registry")
            records = {}
            registry_loaded = False
            # A partial cache plus an unavailable durable read is not a complete
            # inventory. Gate every pair until a later recovery pass can merge
            # both sources; venue open-order discovery alone cannot reveal
            # already-filled market orders.
            self._halt_exposure_scope(
                "*", f"durable registry unavailable: {type(exc).__name__}: {exc}"
            )
        self._recovered_order_records = records
        self._recovered_orphans = await self.order_registry.orphans()
        for orphan in self._recovered_orphans.values():
            scope = str(orphan.get("scope") or "")
            if scope:
                self._halt_exposure_scope(scope, "unresolved orphan order")

        await self._reconcile_registered_orders(records)
        await self._reconcile_venue_open_orders()
        await self._finalize_recovery_state()
        if registry_loaded:
            self._recovery_scope_halts.pop("*", None)
            self._refresh_entry_gate()

    # ------------------------------------------- durable registry reconciliation

    async def _reconcile_registered_group(
        self, exchange: str, symbol: str, pair_tag: str, group: list
    ) -> None:
        token = None
        try:
            if not getattr(self.config, "mock_mode", False) and pair_tag:
                try:
                    token = self._bind_pair_client(
                        self._pair_info_for_tag(pair_tag)
                    )
                except LookupError as exc:
                    logger.warning(
                        "[Recovery] no pair client for %s (%s); verifying with the default client",
                        pair_tag, exc,
                    )
            for record in group:
                await self._reconcile_registered_order(record, exchange, symbol)
        except Exception as exc:
            logger.critical(
                "[Recovery] cannot verify registry records for %s/%s: %s",
                exchange, symbol, exc,
            )
            for record in group:
                record.recovery_status = RecoveryStatus.UNVERIFIED.value
                await self.order_registry.register(record)
                await self.order_registry.mark_pending_recovery(
                    record.local_order_id, f"venue verification failed: {exc}"
                )
        finally:
            if token is not None:
                self._reset_pair_client(token)

    async def _reconcile_registered_orders(
        self, records: Dict[str, OrderRecord]
    ) -> None:
        """Verify non-terminal records in their venue/pair binding context."""
        groups: Dict[tuple, list] = {}
        for record in records.values():
            exchange = (
                record.exchange or self.config.local_exchange_name or ""
            ).lower()
            groups.setdefault(
                (exchange, record.symbol, record.pair_key, record.market_type), []
            ).append(record)
        for (exchange, symbol, pair_tag, _market_type), group in groups.items():
            await self._reconcile_registered_group(
                exchange, symbol, pair_tag, group
            )

    async def _reconcile_registered_order(
        self, record: OrderRecord, exchange: str, symbol: str
    ) -> None:
        """Verifies one durable record against the venue and re-adopts it.

        Verification and adoption are one step on purpose: an order confirmed as
        resting must also be re-bound to its lifecycle, otherwise the bot would
        hold an exchange order that no monitoring loop knows about.
        """
        if record.is_terminal:
            await self.order_registry.retire(record)
            return
        order_id = record.exchange_order_id
        if not order_id:
            await self.order_registry.mark_pending_recovery(
                record.local_order_id, "no exchange order id was ever acknowledged"
            )
            return
        try:
            order = await self._fetch_order_by_role(record, order_id, symbol)
        except Exception as exc:
            record.recovery_status = RecoveryStatus.UNVERIFIED.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id, f"fetch failed: {type(exc).__name__}: {exc}"
            )
            logger.warning("[Recovery] cannot fetch order %s (%s)", order_id, symbol)
            return

        if not isinstance(order, dict):
            record.recovery_status = RecoveryStatus.UNVERIFIED.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id, "venue returned a malformed order payload"
            )
            return
        if not order.get("id") and order_id not in str(order):
            # Venue history retention or API lag: absence is not proof of cancellation.
            record.recovery_status = RecoveryStatus.UNVERIFIED.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id,
                "order not found on the venue; absence is not proof of cancellation",
            )
            return

        previous_status = record.status
        market = "futures" if record.market_type == "perpetual" else "spot"
        amount_spec = record.local_amount_spec if market == "spot" else record.global_amount_spec
        adopted = await self._apply_adopted_order(
            record, order, record.pair_key, amount_spec
        )
        if adopted:
            await self.order_registry.clear_pending_recovery(record.local_order_id)
        if previous_status != record.status:
            logger.warning(
                "[Recovery] order %s (%s) reconciled: %s -> %s filled=%s",
                order_id, record.pair_key or "unlinked", previous_status,
                record.status, record.filled_quantity,
            )
        if adopted:
            logger.info("[Recovery] re-adopted order %s into %s", order_id, record.pair_key)

    async def _fetch_order_by_role(
        self, record: OrderRecord, order_id: str, symbol: str
    ) -> Dict[str, Any]:
        """Reads an order from the venue leg that actually holds it.

        Futures legs live on the perpetual venue: querying the spot venue for the
        same id would either miss it or, worse, return an unrelated spot order.
        """
        spec = record.global_amount_spec if record.market_type == "perpetual" else record.local_amount_spec
        if record.market_type == "perpetual":
            fetcher = getattr(self.client, "fetch_global_order", None)
            if fetcher is None:
                raise NotImplementedError(
                    "client cannot fetch a futures order; perpetual record cannot be verified"
                )
            return await fetcher(order_id, symbol, spec)
        return await self.client.fetch_local_order(order_id, symbol)

    @staticmethod
    def _apply_venue_state(record: OrderRecord, order: Dict[str, Any]) -> None:
        # Backfill the venue id: a crash between submit and acknowledgement, or a
        # record matched only by client id, has no exchange order id yet.
        venue_id = str(order.get("id") or "")
        if venue_id and not record.exchange_order_id:
            record.exchange_order_id = venue_id
        client_id = str(order.get("clientOrderId") or order.get("client_order_id") or "")
        if client_id and not record.client_order_id:
            record.client_order_id = client_id
        record.status = str(order.get("status") or "unknown").lower()
        record.filled_quantity = float(order.get("filled", 0.0) or 0.0)
        amount = float(
            order.get("amount", 0.0) or record.original_quantity or 0.0
        )
        if amount > 0:
            record.original_quantity = amount
        record.remaining_quantity = max(0.0, record.original_quantity - record.filled_quantity)
        record.average_fill_price = float(order.get("average", 0.0) or 0.0)
        record.last_reconciled_at = int(time.time() * 1000)

    async def _finalize_recovery_state(self) -> None:
        """Blocks new entries when any mismatch could not be classified."""
        pending = await self.order_registry.pending_recovery_ids()
        self._recovery_pending_orders = pending
        if not pending:
            logger.info(
                "[Recovery] order registry fully reconciled (%d managed, %d orphan)",
                len(self._recovered_order_records), len(self._recovered_orphans),
            )
            return
        for local_id, reason in pending.items():
            record = self._recovered_order_records.get(local_id)
            pair_tag = record.pair_key if record else ""
            exchange = (record.exchange if record else "") or ""
            if pair_tag:
                # Gate the pair itself: the order may belong to a market whose
                # venue is otherwise healthy.
                self._halt_exposure_scope(pair_tag, "unverifiable order after restart")
                self._mark_failed(
                    pair_tag,
                    "ORDER_RECOVERY_UNVERIFIED",
                    f"Order {local_id} could not be verified against the venue after restart: {reason}",
                    detail={"local_order_id": local_id, "reason": reason},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                continue
            scope = exchange or "unknown"
            self._halt_exposure_scope(scope, "unverifiable order after restart")
            await self._publish_execution_failure(
                f"recovery:{scope}",
                "ORDER_RECOVERY_UNVERIFIED",
                f"Order {local_id} on {scope} could not be verified: {reason}. "
                "New entries on this venue are blocked until an operator reconciles it.",
                detail={"local_order_id": local_id, "reason": reason, "exchange": exchange},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
        self._refresh_entry_gate()
        logger.critical(
            "[Recovery] %d order(s) require manual reconciliation; new entries blocked "
            "on the affected scope (monitoring and de-risking continue)",
            len(pending),
        )

    # ------------------------------------------------- venue open-order sweep

    async def _reconcile_venue_open_orders(self) -> None:
        """Adopts orders that exist on an active venue but nowhere in bot state."""
        targets = self._recovery_venue_targets()
        if not targets:
            return
        for exchange, symbol, pair_tag, market, amount_spec in targets:
            try:
                open_orders = await self._fetch_recovery_orders(exchange, symbol, market, amount_spec)
            except NotImplementedError as exc:
                logger.info("[Recovery] %s cannot list open orders: %s", exchange, exc)
                continue
            except Exception as exc:
                logger.critical(
                    "[Recovery] cannot read %s open orders for %s: %s; entry halted on that venue",
                    exchange, symbol or "account", exc,
                )
                scope = pair_tag or exchange
                self._halt_exposure_scope(scope, "open-order sweep failed after restart")
                await self._publish_execution_failure(
                    f"recovery:{scope}",
                    "RECOVERY_OPEN_ORDERS_UNAVAILABLE",
                    f"Cannot verify resting orders on {exchange} after restart: {exc}. "
                    "New entries are blocked until the venue is reachable again.",
                    detail={"exchange": exchange, "symbol": symbol, "error": str(exc)},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                continue
            for order in open_orders:
                await self._adopt_venue_order(order, exchange, symbol, pair_tag, market, amount_spec)
            await self._detect_phantom_orders(exchange, market, open_orders)
            self._clear_sweep_halt(exchange)

    @staticmethod
    def _is_phantom_candidate(
        record: OrderRecord, exchange: str, market_type: str, listed: set
    ) -> bool:
        if str(record.exchange or "").lower() != str(exchange or "").lower():
            return False
        if (record.market_type or "") != market_type:
            return False
        if record.is_terminal or record.recovery_status == RecoveryStatus.UNVERIFIED.value:
            return False
        return bool(
            record.exchange_order_id
            and record.exchange_order_id not in listed
        )

    async def _detect_phantom_orders(
        self, exchange: str, market: str, open_orders: list
    ) -> None:
        """Flag locally resting records missing from the venue open-order list."""
        listed = {
            str(order.get("id") or "")
            for order in open_orders if isinstance(order, dict)
        }
        market_type = "perpetual" if market == "futures" else "spot"
        for record in (self._recovered_order_records or {}).values():
            if not self._is_phantom_candidate(
                record, exchange, market_type, listed
            ):
                continue
            record.recovery_status = RecoveryStatus.PHANTOM.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id,
                "recorded locally but absent from the venue's open-order list",
            )
            logger.critical(
                "[Recovery] PHANTOM order %s (%s) is tracked but not open on %s; "
                "its final state must be reconciled before entries resume",
                record.exchange_order_id, record.pair_key or "unlinked", exchange,
            )

    def _clear_sweep_halt(self, exchange: str) -> None:
        """Lifts the gate a failed open-order sweep installed for this venue."""
        halts = getattr(self, "_recovery_scope_halts", None) or {}
        if exchange in halts and "open-order sweep" in halts[exchange]:
            self._clear_exposure_halt(exchange, "open-order sweep succeeded")
