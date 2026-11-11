"""Universal order recovery after restart, container restart or VPS reboot.

The recovery pass answers one question before the executor is allowed to trade
again: *which orders does the bot still own on the exchange, and can each one be
attached to a position?*

Order of work (matches the required startup safety sequence):

1. Durable registry is loaded by merging the Redis cache with the
   ``order_registry`` table in Supabase on every recovery pass; a partial cache
   must never hide records that exist only in durable storage.
2. Positions already tracked in Redis are loaded by ``initialize()``.
3. Every active venue is asked for its resting orders (whole account, so an order
   with no local record at all is still seen).
4. Each venue order is linked by client order id, then exchange order id, then a
   legacy pending-entry record, then an identical base+quote market on the same
   venue. Nothing is guessed.
5. Orders that cannot be linked are registered as ORPHAN_ORDER, entry is halted
   on the exposure scope they affect, and an operator alert is published.
6. Terminal states are reconciled; fills are hedged through the existing
   lifecycle so a filled-but-unhedged spot leg is never left forgotten.

Recovery never places an order except to complete a hedge that ``never happened``
for a spot quantity that is already filled, and it never cancels an exchange
order. An unverifiable venue therefore cannot be "fixed" by opening new exposure.
"""

import logging
import time
from typing import Any, Dict, Optional

from executor.models import (
    OrderRecord,
    OrderRole,
    OrderTerminal,
    PositionState,
    RecoveryStatus,
)

logger = logging.getLogger("executor.order_recovery")

ACKNOWLEDGED_EXIT_ROLES = (
    OrderRole.EXIT_FUTURES.value,
    OrderRole.ROLLBACK.value,
    OrderRole.EMERGENCY.value,
    OrderRole.FORCE_CLOSE.value,
)

class OrderRecoveryMixin:
    """Startup reconciliation of exchange orders against bot state."""

    async def _run_order_recovery(self) -> None:
        """Reconciles durable registry + positions + venue open orders."""
        if self.redis is None or self.order_registry is None:
            return
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

    async def _reconcile_registered_orders(self, records: Dict[str, OrderRecord]) -> None:
        """Verifies each non-terminal registry record against the exact venue.

        A record whose venue cannot be reached is retained and marked pending,
        never assumed filled or canceled: an ambiguous status is the one input
        that must block new entries but must not destroy the trail.
        """
        if not records:
            return
        groups: Dict[tuple, list] = {}
        for record in records.values():
            exchange = (record.exchange or self.config.local_exchange_name or "").lower()
            # Pair and market are part of the binding context. Two strategies
            # may share a venue symbol while hedging on different futures venues.
            groups.setdefault(
                (exchange, record.symbol, record.pair_key, record.market_type), []
            ).append(record)

        for (exchange, symbol, pair_tag, market_type), group in groups.items():
            token = None
            try:
                if not getattr(self.config, "mock_mode", False) and pair_tag:
                    # Missing pair metadata must not abort verification: the base
                    # client can still read the order, and the per-record path
                    # classifies whatever it cannot resolve.
                    try:
                        token = self._bind_pair_client(self._pair_info_for_tag(pair_tag))
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
            record, order, record.pair_key, market, amount_spec
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
            await self._clear_sweep_halt(exchange)

    async def _detect_phantom_orders(
        self, exchange: str, market: str, open_orders: list
    ) -> None:
        """Flags records the bot still believes are resting but the venue omits.

        The venue's open-order list is authoritative for "is it still resting".
        A record missing from it was filled or canceled while the bot was down;
        the individual fetch already tried to resolve that, so what is left is a
        genuine disagreement. It is surfaced as PHANTOM and blocks entries rather
        than being silently dropped: a filled entry may still need a hedge.
        """
        listed = {
            str(order.get("id") or "") for order in open_orders if isinstance(order, dict)
        }
        market_type = "perpetual" if market == "futures" else "spot"
        for record in (self._recovered_order_records or {}).values():
            if str(record.exchange or "").lower() != str(exchange or "").lower():
                continue
            if (record.market_type or "") != market_type:
                continue
            if record.is_terminal or record.recovery_status == RecoveryStatus.UNVERIFIED.value:
                continue
            if not record.exchange_order_id or record.exchange_order_id in listed:
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

    async def _clear_sweep_halt(self, exchange: str) -> None:
        """Lifts the gate a failed open-order sweep installed for this venue."""
        halts = getattr(self, "_recovery_scope_halts", None) or {}
        if exchange in halts and "open-order sweep" in halts[exchange]:
            await self._clear_exposure_halt(exchange, "open-order sweep succeeded")

    def _recovery_venue_targets(self) -> list:
        """Exchange/symbol pairs whose orders must be swept at startup.

        Every configured venue is swept even when nothing is locally tracked:
        the orders a restart loses are exactly the ones with no local record.
        """
        catalogues = self._recovery_symbol_catalogues()
        targets = []
        seen = set()
        # Account-wide catalogues come first so a single call per venue/market
        # covers both known symbols and orders with no local record at all.
        for catalogue in catalogues:
            key = (catalogue["exchange"], "", catalogue["market"])
            if key in seen:
                continue
            seen.add(key)
            targets.append((
                catalogue["exchange"], "", catalogue["pair_tag"],
                catalogue["market"], catalogue["amount_spec"],
            ))
        for entry in self._recovery_symbol_spec():
            key = (entry["exchange"], entry["symbol"], entry["market"])
            # Skip per-symbol targets the account-wide sweep already covers.
            if key in seen or (entry["exchange"], "", entry["market"]) in seen:
                continue
            seen.add(key)
            targets.append((
                entry["exchange"], entry["symbol"], entry["pair_tag"],
                entry["market"], entry["amount_spec"],
            ))
        return targets

    def _recovery_symbol_spec(self) -> list:
        """Symbols the bot currently cares about, taken from tracked state."""
        entries = []

        def add(local_exchange, symbol, pair_tag, market, spec):
            if not local_exchange or not symbol:
                return
            entries.append({
                "exchange": str(local_exchange).lower(),
                "symbol": str(symbol),
                "pair_tag": pair_tag,
                "market": market,
                "amount_spec": spec,
            })

        for pair_tag, pos in self.active_positions.items():
            add(
                pos.local_exchange, pos.symbol, pair_tag, "spot",
                _spec_dict(getattr(pos, "local_amount_spec", None)),
            )
            add(
                pos.global_exchange, pos.global_symbol, pair_tag, "futures",
                _spec_dict(getattr(pos, "global_amount_spec", None)),
            )
        for pair_tag, order in self.pending_entry_orders.items():
            add(
                order.get("local_exchange"), order.get("symbol"), pair_tag, "spot",
                order.get("local_amount_spec"),
            )
        for pair_tag, order in self.pending_exit_orders.items():
            add(
                order.get("local_exchange") or getattr(self.active_positions.get(pair_tag), "local_exchange", ""),
                order.get("symbol"), pair_tag, "spot", order.get("local_amount_spec"),
            )
        return entries

    def _recovery_symbol_catalogues(self) -> list:
        """Account-wide sweeps so orders carrying no local record are still found.

        A whole-account read (no symbol filter) is what catches an order placed
        one millisecond before a crash. Perpetual venues are included because a
        resting hedge leg is the most dangerous order to lose: it carries margin
        and is invisible in the local spot book.
        """
        catalogues = []
        for exchange, market in (
            (str(self.config.local_exchange_name or "").lower(), "spot"),
            (str(self.config.global_exchange_name or "").lower(), "futures"),
        ):
            if not exchange:
                continue
            if self._client_for_exchange(exchange) is None:
                continue
            catalogues.append({
                "exchange": exchange,
                "market": market,
                "pair_tag": "",
                "amount_spec": None,
            })
        return catalogues

    async def _fetch_recovery_orders(
        self, exchange: str, symbol: str, market: str, amount_spec: Any
    ) -> list:
        """Reads resting orders for one venue leg, resolving its own client."""
        client = self._client_for_exchange(exchange)
        if client is None:
            raise NotImplementedError(f"no client is bound to exchange {exchange}")
        spec = amount_spec
        if spec is None:
            spec = self._default_amount_spec_for(exchange, market)
        method_name = (
            "fetch_global_open_orders" if market == "futures" else "fetch_local_open_orders"
        )
        fetch = getattr(client, method_name, None)
        if not callable(fetch):
            # Without a listing endpoint the bot cannot rule out resting orders it
            # never recorded. Mock clients are exempt: paper mode has no venue to
            # lose an order on.
            if getattr(self.config, "mock_mode", False):
                raise NotImplementedError(
                    f"{exchange} client has no {method_name}; skipped in paper mode"
                )
            raise RuntimeError(
                f"{exchange} client cannot list {market} open orders; resting orders "
                "cannot be verified after restart"
            )
        return await fetch(symbol or None, spec)

    def _client_for_exchange(self, exchange: str) -> Any:
        """Resolves the exchange client that owns a venue, or None when unbound."""
        exchange = str(exchange or "").lower()
        if not exchange:
            return None
        configured_local = str(getattr(self.config, "local_exchange_name", "")).lower()
        configured_global = str(getattr(self.config, "global_exchange_name", "")).lower()
        if getattr(self.config, "mock_mode", False):
            if exchange in (configured_local, configured_global):
                return self._base_client
            return None
        registry = self._pair_client_registry
        keys_fn = getattr(registry, "keys", None)
        for key in (keys_fn() if callable(keys_fn) else ()):
            local_name = str(getattr(key, "local_exchange", "")).lower()
            global_name = str(getattr(key, "global_exchange", "")).lower()
            if exchange in (local_name, global_name):
                try:
                    return registry.resolve(key)
                except Exception:
                    continue
        if exchange in (configured_local, configured_global):
            return self._base_client
        return None

    @staticmethod
    def _default_amount_spec_for(exchange: str, market: str) -> Optional[dict]:
        """Fractional step used only when nothing better is known.

        Futures quantities are left in the venue's own units (contracts on some
        venues) because converting them needs the contract's real
        ``base_per_unit``; guessing 1.0 there would misreport a discovered hedge.
        """
        if market == "futures":
            return None
        return {"unit": "BASE", "step": 1e-12, "min": 0.0, "base_per_unit": 1.0}

    async def _adopt_venue_order(
        self,
        order: Dict[str, Any],
        exchange: str,
        requested_symbol: str,
        requested_pair_tag: str,
        market: str,
        amount_spec: Any,
    ) -> None:
        """Links one resting venue order to a position, or flags it as an orphan."""
        if not isinstance(order, dict):
            return
        exchange_order_id = str(order.get("id") or "")
        client_order_id = str(order.get("clientOrderId") or order.get("client_order_id") or "")
        symbol = str(order.get("symbol") or requested_symbol or "")
        if not exchange_order_id and not client_order_id:
            await self._orphan(
                order, exchange, symbol, requested_pair_tag,
                "venue order carries no order id or client order id",
            )
            return

        if await self._already_managed(exchange, exchange_order_id, client_order_id):
            return

        record, pair_tag = await self._link_venue_order(
            order, exchange, symbol, requested_pair_tag, market, exchange_order_id, client_order_id
        )
        if record is None:
            await self._orphan(
                order, exchange, symbol, requested_pair_tag,
                "active venue order matches no tracked pair, position, pending record or cancelled base order",
            )
            return

        adopted = await self._apply_adopted_order(record, order, pair_tag, market, amount_spec)
        if adopted:
            await self.order_registry.clear_pending_recovery(record.local_order_id)
            logger.warning(
                "[Recovery] adopted venue order %s (%s, %s) for %s",
                exchange_order_id or client_order_id, exchange, record.order_role, pair_tag,
            )

    async def _already_managed(
        self, exchange: str, exchange_order_id: str, client_order_id: str
    ) -> bool:
        """True when this venue order is already bound or already reconciled.

        A registry record that exists but is not yet bound (no acknowledged
        exchange order id, or marked pending) must still go through adoption:
        skipping it is exactly how an order stays unmanaged after a restart.
        """
        record = None
        if client_order_id:
            record = await self.order_registry.find_by_client_id(exchange, client_order_id)
        if record is None and exchange_order_id:
            record = await self.order_registry.find_by_exchange_id(exchange, exchange_order_id)
        if record is None:
            return False
        if record.is_terminal:
            return True
        return bool(record.exchange_order_id) and record.recovery_status in (
            RecoveryStatus.ADOPTED.value,
            RecoveryStatus.MANAGED.value,
        )

    async def _link_venue_order(
        self,
        order: Dict[str, Any],
        exchange: str,
        symbol: str,
        requested_pair_tag: str,
        market: str,
        exchange_order_id: str,
        client_order_id: str,
    ) -> tuple:
        """Returns ``(record, pair_tag)`` for a matched order, ``(None, "")`` otherwise."""
        record = None
        if client_order_id:
            record = await self.order_registry.find_by_client_id(exchange, client_order_id)
        if record is None and exchange_order_id:
            record = await self.order_registry.find_by_exchange_id(exchange, exchange_order_id)
        if record is None:
            record = await self._link_legacy_pending_record(exchange, exchange_order_id, client_order_id, symbol)

        if record is not None:
            # A record whose pair link was never written (crash between submit
            # and registration) is completed from the market it belongs to.
            pair_tag = record.pair_key or self._match_position_by_catalogue(
                exchange, symbol
            )
            record.pair_key = pair_tag
            record.position_id = record.position_id or self._position_id_for(pair_tag)
            record.market_type = record.market_type or ("perpetual" if market == "futures" else "spot")
            record.exchange = record.exchange or exchange
            record.symbol = record.symbol or symbol
            if not record.order_role:
                record.order_role = self._infer_role(symbol, pair_tag, market)
            if not pair_tag:
                return None, ""
            return record, pair_tag

        # Role depends on the pair: only after linking is it known whether this
        # is an entry, an exit against an open position, or an unhedged hedge leg.
        pair_tag = self._match_position_by_catalogue(exchange, symbol)
        # requested_pair_tag is only a scan hint, not proof of ownership. When
        # multiple tracked positions share this venue market, catalogue matching
        # deliberately returns no owner and the order must remain an orphan.
        if not pair_tag:
            return None, ""
        role = self._infer_role(symbol, pair_tag, market)
        return self._record_from_order(
            order, exchange, symbol, pair_tag, market, role, exchange_order_id, client_order_id
        ), pair_tag

    async def _link_legacy_pending_record(
        self, exchange: str, exchange_order_id: str, client_order_id: str, symbol: str
    ) -> Optional[OrderRecord]:
        """Resolves an order against the pre-registry pending-entry records.

        Those records predate the registry and only exist in Redis, so they are
        migrated here: the surviving record keeps the pair identity that the
        venue payload alone cannot provide.
        """
        if not exchange_order_id and not client_order_id:
            return None
        for pair_tag, order_info in list(self.pending_entry_orders.items()):
            stored_id = str(order_info.get("order_id") or "")
            stored_coid = str(order_info.get("client_order_id") or "")
            if exchange_order_id and stored_id == exchange_order_id:
                pass
            elif client_order_id and stored_coid == client_order_id:
                pass
            else:
                continue
            record = OrderRecord(
                local_order_id=OrderRecord.local_id_for(exchange, stored_coid, stored_id),
                client_order_id=stored_coid,
                exchange_order_id=stored_id,
                pair_key=pair_tag,
                position_id=self._position_id_for(pair_tag),
                exchange=exchange,
                market_type="spot",
                symbol=str(order_info.get("symbol") or symbol),
                side="buy",
                order_type="limit",
                order_role=OrderRole.ENTRY.value,
                price=float(order_info.get("price", 0.0) or 0.0),
                original_quantity=float(order_info.get("qty", 0.0) or 0.0),
                filled_quantity=float(order_info.get("filled", 0.0) or 0.0),
                is_mock=bool(order_info.get("is_mock", getattr(self.config, "mock_mode", False))),
            )
            return await self.order_registry.register(record)
        return None

    def _match_position_by_catalogue(self, exchange: str, symbol: str) -> str:
        """Last-resort link for an order with no known client or exchange id.

        This is what recovers an order placed moments before a crash: the client
        order id was never acknowledged and no record exists, so the only
        surviving evidence is the venue's own symbol. A match therefore requires
        identical base **and** quote on the same venue — never a fuzzy or
        same-base-only match, because that would attach an order to the wrong
        position and misreport exposure.
        """
        base, quote = self._symbol_parts(symbol)
        if not base or not quote or not exchange:
            return ""
        exchange = exchange.lower()
        candidates = set(self.active_positions) | set(self.pending_entry_orders)
        matches = []
        for pair_tag in candidates:
            tracked_symbol, tracked_exchanges = self._tracked_market(pair_tag)
            if exchange not in tracked_exchanges:
                continue
            tracked_base, tracked_quote = self._symbol_parts(tracked_symbol)
            if (tracked_base, tracked_quote) == (base, quote):
                matches.append(pair_tag)
        # A market can be shared by strategies whose futures leg differs. The
        # spot symbol alone cannot identify which strategy owns an unlabelled
        # order; choosing the first set member would attach it nondeterministically.
        return matches[0] if len(matches) == 1 else ""

    def _tracked_market(self, pair_tag: str) -> tuple:
        """Returns (local symbol, venues) for a tracked pair."""
        pos = self.active_positions.get(pair_tag)
        if pos is not None:
            return pos.symbol, {
                str(pos.local_exchange or "").lower(),
                str(pos.global_exchange or "").lower(),
            }
        pending = self.pending_entry_orders.get(pair_tag) or {}
        return str(pending.get("symbol", "")), {
            str(pending.get("local_exchange", "")).lower(),
            str(pending.get("global_exchange", "")).lower(),
        }

    @staticmethod
    def _symbol_parts(symbol: str) -> tuple:
        """Splits a venue symbol into (base, quote) using only known formats."""
        cleaned = str(symbol or "").upper().split(":")[0]
        if not cleaned:
            return "", ""
        for separator in ("/", "-", "_"):
            if separator in cleaned:
                base, _, quote = cleaned.partition(separator)
                return base.strip(), quote.strip()
        # Compact form, e.g. BTCIDR / BTCUSDT. Longest known quote wins so
        # BTCUSDT is not read as BTCU + ST.
        for quote in ("USDT", "USDC", "IDR", "BIDR", "BTC", "ETH"):
            if cleaned.endswith(quote) and len(cleaned) > len(quote):
                return cleaned[: -len(quote)], quote
        return "", ""

    def _infer_role(self, symbol: str, pair_tag: str, market: str) -> str:
        """Classifies a discovered order from the state that already tracks it.

        Every order the bot intentionally leaves resting is an entry or an exit;
        a resting futures order is an unexpected hedge leg. Nothing else is
        inferred, so an unknown order never starts controlling a position.
        """
        if pair_tag in self.pending_exit_orders:
            return OrderRole.EXIT_SPOT.value
        if pair_tag in self.pending_entry_orders:
            return OrderRole.ENTRY.value
        if market == "futures":
            return OrderRole.HEDGE.value
        if pair_tag in self.active_positions:
            return OrderRole.EXIT_SPOT.value
        return OrderRole.ENTRY.value

    def _record_from_order(
        self,
        order: Dict[str, Any],
        exchange: str,
        symbol: str,
        pair_tag: str,
        market: str,
        role: str,
        exchange_order_id: str,
        client_order_id: str,
    ) -> OrderRecord:
        return OrderRecord(
            local_order_id=OrderRecord.local_id_for(exchange, client_order_id, exchange_order_id),
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            pair_key=pair_tag,
            position_id=self._position_id_for(pair_tag),
            exchange=exchange,
            market_type="perpetual" if market == "futures" else "spot",
            symbol=symbol,
            side=str(order.get("side") or ""),
            order_type=str(order.get("type") or ""),
            order_role=role,
            price=float(order.get("price", 0.0) or 0.0),
            original_quantity=float(order.get("amount", 0.0) or 0.0),
            filled_quantity=float(order.get("filled", 0.0) or 0.0),
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )

    async def _apply_adopted_order(
        self,
        record: OrderRecord,
        order: Dict[str, Any],
        pair_tag: str,
        market: str,
        amount_spec: Any,
    ) -> bool:
        """Binds an adopted order back into the live lifecycle.

        Returns False when the order could neither be adopted nor safely
        classified, in which case it stays pending and blocks new entries.
        """
        # Venue amount rules discovered from the sweep are recorded on the
        # record: without them a later restart could not rebuild the order.
        if record.local_amount_spec is None and amount_spec is not None:
            record.local_amount_spec = (
                amount_spec.to_dict() if hasattr(amount_spec, "to_dict") else amount_spec
            )
        self._apply_venue_state(record, order)
        # Reconcile the durable hedge watermark from any live pending record: the
        # monitoring loop advances it as hedges land.
        live = self.pending_entry_orders.get(pair_tag)
        if isinstance(live, dict):
            record.hedged_quantity = max(
                float(record.hedged_quantity or 0.0),
                float(live.get("hedged_short_qty", 0.0) or 0.0),
            )
        status = record.classify_status()
        if status == OrderTerminal.UNKNOWN:
            record.recovery_status = RecoveryStatus.UNVERIFIED.value
            await self.order_registry.register(record)
            await self.order_registry.mark_pending_recovery(
                record.local_order_id, f"unknown venue status '{record.status}'"
            )
            return False

        if status in (OrderTerminal.FILLED, OrderTerminal.CANCELED):
            return await self._finalize_adopted_terminal(record, order, pair_tag, market, amount_spec)

        if record.order_role == OrderRole.ENTRY.value:
            if pair_tag in self.pending_entry_orders:
                # Already re-adopted by the pending-entry pass; the record itself
                # is verified, so it must not stay on the pending list.
                record.recovery_status = RecoveryStatus.ADOPTED.value
                await self.order_registry.register(record)
                await self.order_registry.clear_pending_recovery(record.local_order_id)
                return True
            info = self._rebuild_entry_order_info(record, order, amount_spec)
            if info is None:
                record.recovery_status = RecoveryStatus.UNVERIFIED.value
                await self.order_registry.register(record)
                await self.order_registry.mark_pending_recovery(
                    record.local_order_id,
                    "resting entry could not be rebuilt from bot state; manual reconciliation required",
                )
                return False
            self.pending_entry_orders[pair_tag] = info
            self.states[pair_tag] = (
                PositionState.ENTRY_PARTIAL if record.filled_quantity > 0 else PositionState.ENTRY_PENDING
            )
            await self._persist_pending_entry(pair_tag, info)
            record.recovery_status = RecoveryStatus.ADOPTED.value
            await self.order_registry.register(record)
            return True

        if record.order_role == OrderRole.EXIT_SPOT.value:
            pos = self.active_positions.get(pair_tag)
            if pos is None:
                await self.order_registry.mark_pending_recovery(
                    record.local_order_id, "resting exit order has no tracked position"
                )
                return False
            self.pending_exit_orders[pair_tag] = {
                "order_id": record.exchange_order_id,
                "client_order_id": record.client_order_id,
                "symbol": record.symbol,
                "price": record.price,
                "qty": record.original_quantity or pos.qty,
                "filled": record.filled_quantity,
                "closed_short_qty": float(
                    self.pending_exit_orders.get(pair_tag, {}).get("closed_short_qty", 0.0) or 0.0
                ),
                "tick_size": getattr(pos, "local_price_tick", 1.0),
                "local_exchange": pos.local_exchange,
                "local_amount_spec": _spec_dict(amount_spec),
            }
            if self.states.get(pair_tag) != PositionState.RECONCILIATION_REQUIRED:
                self.states[pair_tag] = PositionState.EXIT_PENDING
            record.recovery_status = RecoveryStatus.ADOPTED.value
            await self.order_registry.register(record)
            return True

        # Hedge, rollback, emergency and force-close legs are submitted as market
        # orders: one resting at the venue is unexpected. Re-adopting it silently
        # would be a guess, so it is surfaced instead.
        record.recovery_status = RecoveryStatus.UNVERIFIED.value
        await self.order_registry.register(record)
        await self.order_registry.mark_pending_recovery(
            record.local_order_id,
            f"unexpected resting {record.order_role} order; manual reconciliation required",
        )
        return False

    async def _finalize_adopted_terminal(
        self,
        record: OrderRecord,
        order: Dict[str, Any],
        pair_tag: str,
        market: str,
        amount_spec: Any,
    ) -> bool:
        """Reconciles an already-terminal order discovered during the sweep."""
        await self.order_registry.register(record)
        await self.order_registry.retire(record)

        if record.order_role == OrderRole.ENTRY.value:
            info = self.pending_entry_orders.get(pair_tag) or self._rebuild_entry_order_info(
                record, order, amount_spec
            )
            if info is None:
                await self.order_registry.mark_pending_recovery(
                    record.local_order_id,
                    "terminal entry cannot be reconciled without its pair context",
                )
                return False
            info["filled"] = record.filled_quantity
            self.pending_entry_orders[pair_tag] = info
            # Hedge any final fill through the normal lifecycle, then release the
            # pending record. This is the crash-between-order-and-hedge case.
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            await self._handle_entry_fills(pair_tag, info, order)
            if self.states.get(pair_tag) in (
                PositionState.FAILED, PositionState.RECONCILIATION_REQUIRED
            ):
                logger.critical(
                    "[Recovery] fill reconciliation failed for %s; pending record retained",
                    pair_tag,
                )
                return False
            await self._finalize_entry_order(pair_tag, record.status)
            return True

        if record.order_role == OrderRole.EXIT_SPOT.value:
            if pair_tag not in self.active_positions:
                return True
            self.pending_exit_orders[pair_tag] = {
                "order_id": record.exchange_order_id,
                "client_order_id": record.client_order_id,
                "symbol": record.symbol,
                "price": record.price,
                "qty": record.original_quantity,
                "filled": record.filled_quantity,
                "closed_short_qty": 0.0,
                "tick_size": getattr(self.active_positions.get(pair_tag), "local_price_tick", 1.0),
                "local_exchange": getattr(self.active_positions.get(pair_tag), "local_exchange", ""),
                "local_amount_spec": _spec_dict(amount_spec),
            }
            await self._process_pending_exit(pair_tag, force_reconcile=True)
            pos = self.active_positions.get(pair_tag)
            if pos is not None:
                await self._finalize_exit_position(pair_tag, pos)
            return True

        if record.order_role == OrderRole.HEDGE.value:
            return await self._reconcile_recovered_entry_hedge(record, order, pair_tag)

        if record.order_role in ACKNOWLEDGED_EXIT_ROLES:
            # A terminal futures close is already reflected in the position, and a
            # terminal rollback means the spot leg was liquidated. Neither needs
            # a replayed action; the restored position keeps the exit lifecycle.
            logger.info(
                "[Recovery] terminal %s order %s for %s acknowledged",
                record.order_role, record.exchange_order_id, pair_tag,
            )
        return True

    async def _reconcile_recovered_entry_hedge(
        self, hedge_record: OrderRecord, venue_order: Dict[str, Any], pair_tag: str
    ) -> bool:
        """Reconciles a futures fill independently against its originating spot order.

        The hedge intent is journaled as non-terminal until the spot watermark is
        durable. If the process dies after the futures fill, this path fetches the
        exact futures order and advances the parent spot watermark exactly once.
        """
        context = hedge_record.recovery_context or {}
        parent_exchange = str(context.get("parent_spot_exchange") or "").lower()
        parent_order_id = str(context.get("parent_spot_order_id") or "")
        parent_client_id = str(context.get("parent_spot_client_order_id") or "")
        if not parent_exchange or not (parent_order_id or parent_client_id):
            await self.order_registry.mark_pending_recovery(
                hedge_record.local_order_id,
                "futures hedge has no durable parent spot-order identity",
            )
            return False

        parent = None
        if parent_order_id:
            parent = await self.order_registry.find_by_exchange_id(parent_exchange, parent_order_id)
        if parent is None and parent_client_id:
            parent = await self.order_registry.find_by_client_id(parent_exchange, parent_client_id)
        if parent is None:
            await self.order_registry.mark_pending_recovery(
                hedge_record.local_order_id,
                "originating spot order is missing; cannot reconcile the futures leg safely",
            )
            self._halt_exposure_scope(pair_tag or parent_exchange, "futures hedge parent missing")
            return False

        try:
            filled = float(venue_order.get("filled", 0.0) or 0.0)
        except (TypeError, ValueError):
            filled = 0.0
        if filled <= 0:
            await self.order_registry.mark_pending_recovery(
                hedge_record.local_order_id,
                "terminal futures hedge has no verifiable positive filled quantity",
            )
            return False

        # Marker and cumulative watermark are committed in one parent record;
        # replaying startup recovery cannot count the same futures fill twice.
        parent_context = dict(parent.recovery_context or {})
        hedge_fills = parent_context.get("hedge_fill_orders")
        if not isinstance(hedge_fills, dict):
            hedge_fills = {}
        hedge_key = str(hedge_record.client_order_id or hedge_record.exchange_order_id)
        if hedge_key not in hedge_fills:
            parent.hedged_quantity = max(0.0, float(parent.hedged_quantity or 0.0)) + filled
            hedge_fills[hedge_key] = filled
        parent_context["hedge_fill_orders"] = hedge_fills
        parent.recovery_context = parent_context
        await self.order_registry.register(parent)

        live = self.pending_entry_orders.get(pair_tag)
        if isinstance(live, dict):
            live["hedged_short_qty"] = max(
                float(live.get("hedged_short_qty", 0.0) or 0.0),
                float(parent.hedged_quantity or 0.0),
            )
            await self._persist_pending_entry(pair_tag, live)

        hedge_record.status = str(venue_order.get("status") or "closed").lower()
        hedge_record.filled_quantity = max(float(hedge_record.filled_quantity or 0.0), filled)
        hedge_record.remaining_quantity = max(
            0.0, hedge_record.original_quantity - hedge_record.filled_quantity
        )
        hedge_record.recovery_status = RecoveryStatus.ADOPTED.value
        await self.order_registry.register(hedge_record)
        await self.order_registry.retire(hedge_record)
        await self.order_registry.clear_pending_recovery(hedge_record.local_order_id)
        logger.warning(
            "[Recovery] reconciled futures hedge %s independently: filled=%s, spot watermark=%s (%s)",
            hedge_key, filled, parent.hedged_quantity, pair_tag,
        )
        return True

    def _rebuild_entry_order_info(
        self, record: OrderRecord, order: Dict[str, Any], amount_spec: Any
    ) -> Optional[dict]:
        """Rebuilds the in-memory pending-entry shape from a durable record.

        Requires the persisted ``pair_key`` *and* the FX snapshot. A filled entry
        cannot be hedged without a real USDT/IDR rate, and inventing one would
        book a position on a guessed basis, so a record missing it stays pending
        for manual reconciliation instead.
        """
        pair_tag = record.pair_key
        if not pair_tag or not record.symbol:
            return None
        context = record.recovery_context or {}
        try:
            usdt_rate = float(context.get("usdt_idr_rate", 0.0) or 0.0)
        except (TypeError, ValueError):
            usdt_rate = 0.0
        if usdt_rate <= 0:
            return None
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        local_exchange = record.exchange or (
            getattr(pair_info, "local_exchange", "") if pair_info else self.config.local_exchange_name
        )
        global_exchange = (
            getattr(pair_info, "global_exchange", "") if pair_info else self.config.global_exchange_name
        )
        global_symbol = (
            context.get("global_symbol")
            or (getattr(pair_info, "global_symbol", "") if pair_info else "")
        )
        base_asset = context.get("base_asset") or (
            getattr(pair_info, "base_asset", "") if pair_info else ""
        )
        quote_asset = context.get("quote_asset") or (
            getattr(pair_info, "quote_asset", "") if pair_info else ""
        )
        if not global_symbol:
            base, _ = self._symbol_parts(record.symbol)
            if not base:
                return None
            global_symbol = f"{base.upper()}USDT"
        if not base_asset:
            base_asset, _ = self._symbol_parts(record.symbol)
        if not quote_asset:
            _, quote_asset = self._symbol_parts(record.symbol)
        return {
            "order_id": record.exchange_order_id,
            "client_order_id": record.client_order_id,
            "symbol": record.symbol,
            "global_symbol": global_symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "local_exchange": local_exchange,
            "global_exchange": global_exchange,
            # Persisted venue rules first: they are the only source that makes the
            # rebuilt order hedgeable at the correct quantity.
            "local_amount_spec": (
                record.local_amount_spec
                or amount_spec
                or self._default_amount_spec_for(local_exchange, "spot")
            ),
            "global_amount_spec": record.global_amount_spec,
            "price": record.price,
            "qty": record.original_quantity,
            "filled": record.filled_quantity,
            # Restored, not zeroed: a fill already hedged before the crash must
            # not be hedged a second time by the resumed monitoring loop.
            "hedged_short_qty": float(record.hedged_quantity or 0.0),
            "tick_size": getattr(
                self.active_positions.get(pair_tag), "local_price_tick", 1.0
            ),
            "placed_time": (record.created_at or int(time.time() * 1000)) / 1000.0,
            "recovered_from_registry": True,
            "is_mock": record.is_mock,
            # Restored verbatim so a post-restart hedge uses the same basis,
            # fees and funding window the order was sized against.
            "usdt_idr_rate": usdt_rate,
            "fx_rate_timestamp_ms": context.get("fx_rate_timestamp_ms", 0),
            "global_ask_usdt": float(context.get("global_ask_usdt", 0.0) or 0.0),
            "cancel_threshold_usdt": float(context.get("cancel_threshold_usdt", 0.0) or 0.0),
            "min_entry_spread_pct": float(context.get("min_entry_spread_pct", 0.0) or 0.0),
            "base_target_exit_spread_pct": float(
                context.get("base_target_exit_spread_pct", 0.3) or 0.3
            ),
            "current_funding_rate": float(context.get("current_funding_rate", 0.0) or 0.0),
            "next_funding_time_ms": int(context.get("next_funding_time_ms", 0) or 0),
            "spot_entry_fee_pct": float(context.get("spot_entry_fee_pct", 0.0) or 0.0),
            "spot_exit_fee_pct": float(context.get("spot_exit_fee_pct", 0.0) or 0.0),
            "perp_entry_fee_pct": float(context.get("perp_entry_fee_pct", 0.0) or 0.0),
            "perp_exit_fee_pct": float(context.get("perp_exit_fee_pct", 0.0) or 0.0),
            "fee_source": str(context.get("fee_source", "LEGACY_UNVERIFIED")),
            "fee_verified": bool(context.get("fee_verified", False)),
            "fee_fetched_at_ms": int(context.get("fee_fetched_at_ms", 0) or 0),
            "fee_asset_surcharge_pct": float(
                context.get("fee_asset_surcharge_pct", 0.0) or 0.0
            ),
            "spot_cost_usdt": float(context.get("spot_cost_usdt", 0.0) or 0.0),
            "futures_margin_usdt": float(context.get("futures_margin_usdt", 0.0) or 0.0),
        }

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
        await self._clear_exposure_halt(scope, "orphan resolved")
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
                    await self._clear_exposure_halt(record.pair_key, "pending order verified")

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
        self._entry_blocked = True

    async def _clear_exposure_halt(self, scope: str, reason: str) -> None:
        halts = getattr(self, "_recovery_scope_halts", None)
        if halts is None:
            halts = self._recovery_scope_halts = {}
        if halts.pop(scope, None) is None:
            return
        self._refresh_entry_gate()
        logger.warning("[Recovery] entry halt cleared for %s (%s)", scope, reason)

    def _refresh_entry_gate(self) -> None:
        """Recomputes the entry gate from every recovery reason currently known."""
        self._entry_blocked = bool(
            self._recovery_pending_orders or self._recovery_scope_halts
        )


def _spec_dict(amount_spec: Any) -> Optional[dict]:
    """Normalizes an amount spec (dataclass or mapping) to a plain mapping."""
    if amount_spec is None:
        return None
    if hasattr(amount_spec, "to_dict"):
        return amount_spec.to_dict()
    return amount_spec
