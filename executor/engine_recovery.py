"""engine recovery methods for arbitrage engine orchestration."""

import asyncio
import json
import math
import re
import time
from typing import Optional, Any

from executor.models import PositionState, ActivePosition
from executor.pair_registry import ExchangePairKey
from executor.helpers import PENDING_ENTRY_KEY_PREFIX, is_explicit_rate_limit_rejection
from executor.engine_common import logger
from executor.exchange_common import exchange_order_id_from



class EngineRecoveryMixin:
    """Focused orchestration responsibilities, composed by ArbitrageEngine."""

    async def _persist_reconciled_submit_order(
        self, exchange: str, client_order_id: str, order: dict
    ) -> None:
        """Attach a client-id lookup result to its durable pre-submit intent.

        Keep the record non-terminal until the normal order-recovery path fetches
        the exact exchange order. This is essential when the acknowledgement was
        lost and the found order is already terminal: terminalizing the intent
        here would remove it from the recovery index before its Spot/Futures
        lifecycle (including hedge-claim confirmation) is reconciled.
        """
        registry = getattr(self, "order_registry", None)
        if registry is None:
            raise RuntimeError(
                "durable order registry is unavailable; refusing to acknowledge "
                "a recovered venue order without persisting its lifecycle record"
            )
        record = await registry.find_by_client_id(exchange, client_order_id)
        if record is None:
            raise RuntimeError(
                "venue order was found by client order ID but its durable order intent "
                "is missing from the registry"
            )
        exchange_order_id = exchange_order_id_from(order)
        if not exchange_order_id:
            raise RuntimeError(
                "venue order lookup returned no exchange order ID for durable intent"
            )
        record.exchange_order_id = exchange_order_id
        record.status = "open"  # non-terminal until exact-order recovery fetches venue state
        try:
            filled = float(order.get("filled") or 0.0)
            if filled < 0:
                raise ValueError("negative filled quantity")
            record.filled_quantity = max(
                float(record.filled_quantity or 0.0), filled
            )
            requested = float(record.original_quantity or order.get("amount") or 0.0)
            record.remaining_quantity = max(0.0, requested - record.filled_quantity)
            average = order.get("average") or order.get("average_fill_price")
            if average is not None and float(average) > 0:
                record.average_fill_price = float(average)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"venue order lookup returned invalid fill metadata: {exc}"
            ) from exc
        venue_client_id = str(
            order.get("clientOrderId") or order.get("client_order_id") or ""
        )
        if venue_client_id and venue_client_id != client_order_id:
            record.venue_client_order_id = venue_client_id
        await registry.register(record)

    async def _reconcile_durable_submit_attempts(self) -> bool:
        """Resolve exact client-id matches without conflating lookup failure and NOT_FOUND."""
        service = getattr(self, "global_stop", None)
        if service is None:
            return True
        attempts = await service.list_unresolved_submit_attempts()

        async def _keep_unknown(client_order_id: str, error: Exception) -> None:
            try:
                await service.mark_submit_unknown(
                    client_order_id, f"submit recovery unresolved: {type(error).__name__}: {error}"
                )
            except Exception:
                # The already-committed SUBMITTING row remains globally blocking.
                logger.exception(
                    "[Submit Recovery] could not annotate unresolved attempt %s",
                    client_order_id,
                )

        for attempt in attempts:
            client_order_id = str(attempt.get("client_order_id") or "")
            exchange = str(attempt.get("exchange") or "").lower()
            symbol = str(attempt.get("symbol") or "")
            try:
                market_type = str(attempt.get("market_type") or "spot").lower()
                if market_type in {"perpetual", "future", "futures", "swap"}:
                    resolver_market = "futures"
                    lookup_name = "fetch_global_order_by_client_id"
                elif market_type in {"spot", "local"}:
                    resolver_market = "spot"
                    lookup_name = "fetch_local_order_by_client_id"
                else:
                    raise RuntimeError(
                        f"unsupported market_type for submit reconciliation: {market_type}"
                    )

                # Venue names are not sufficient when one account exposes both
                # spot and perpetual clients (notably MEXC). Resolve the exact
                # market before selecting the lookup method; a one-argument
                # resolver can silently return the configured/default market.
                resolver = getattr(self._base_client, "for_venue", None)
                if not callable(resolver):
                    raise RuntimeError(
                        "market-aware venue resolver is unavailable for submit reconciliation"
                    )
                venue_client = resolver(exchange, resolver_market)
                if venue_client is None:
                    raise RuntimeError(
                        f"no {resolver_market} client configured for submit venue {exchange}"
                    )
                lookup = getattr(venue_client, lookup_name, None)
                if not callable(lookup):
                    raise RuntimeError(
                        f"no {market_type} client-order-id lookup configured for venue {exchange}"
                    )
                order = await lookup(
                    client_order_id, symbol, attempt.get("amount_spec") or None
                )
            except Exception as exc:
                logger.exception(
                    "[Submit Recovery] exact client-id lookup failed for %s on %s",
                    client_order_id, exchange,
                )
                await _keep_unknown(client_order_id, exc)
                continue

            if order is None:
                try:
                    await service.reconcile_submit_attempt_not_found(
                        client_order_id,
                        evidence="exact client-order-id lookup returned no order; "
                        "retained unresolved because absence is not proof of rejection",
                    )
                except Exception as exc:
                    logger.exception(
                        "[Submit Recovery] could not persist NOT_FOUND lookup evidence for %s",
                        client_order_id,
                    )
                    await _keep_unknown(client_order_id, exc)
                continue

            if not isinstance(order, dict) or not exchange_order_id_from(order):
                malformed = RuntimeError(
                    "exact client-order-id lookup returned a malformed order without an exchange order ID"
                )
                logger.error(
                    "[Submit Recovery] malformed exact-ID result for %s on %s; retaining UNKNOWN",
                    client_order_id, exchange,
                )
                await _keep_unknown(client_order_id, malformed)
                continue

            try:
                # The venue order exists. Persist its lifecycle record before ACK;
                # any local persistence failure is UNKNOWN, never NOT_FOUND.
                await self._persist_reconciled_submit_order(
                    exchange, client_order_id, order
                )
                await service.reconcile_submit_attempt_found(client_order_id, order)
                logger.warning(
                    "[Submit Recovery] found unresolved venue order %s on %s; "
                    "durable ledger marked ACKNOWLEDGED",
                    client_order_id, exchange,
                )
            except Exception as exc:
                logger.exception(
                    "[Submit Recovery] venue order %s was found on %s but durable adoption/ACK failed",
                    client_order_id, exchange,
                )
                await _keep_unknown(client_order_id, exc)

        unresolved = await service.list_unresolved_submit_attempts()
        if unresolved:
            first = unresolved[0]
            reason = (
                "durable in-flight submit outcome unresolved; new entries are fenced "
                f"(exchange={first.get('exchange')} symbol={first.get('symbol')} "
                f"client_order_id={first.get('client_order_id')} status={first.get('status')})"
            )
            self._halt_exposure_scope("*", reason)
            self._refresh_entry_gate()
            await self._publish_execution_failure(
                "global-submit-recovery",
                "DURABLE_SUBMIT_RECONCILIATION_REQUIRED",
                "Ada submit order yang belum dapat dipastikan hasilnya. Entry baru diblokir; "
                "tidak ada retry otomatis. Periksa order melalui client order ID dan rekonsiliasi exposure.",
                detail={"unresolved_attempts": len(unresolved), "first": first},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            return False
        return True

    def _group_recovered_positions(self):
        groups = {}
        for pair_tag, pos in self.active_positions.items():
            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            groups.setdefault((pos.global_exchange, symbol), []).append((pair_tag, pos))
        return groups

    async def _mark_recovered_group_unverified(self, global_symbol: str, items) -> None:
        reason = "Live futures position API is unavailable after restart."
        for pair_tag, pos in items:
            pos.state = "RECONCILIATION_REQUIRED"
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            await self.redis.set(
                f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
            )
            self._mark_failed(
                pair_tag,
                "RECOVERY_POSITION_STATE_UNKNOWN",
                reason,
                detail={"global_symbol": global_symbol, "expected_qty": pos.qty},
                state=PositionState.RECONCILIATION_REQUIRED,
            )

    @staticmethod
    def _position_side(position: dict, info: dict) -> str:
        return str(
            position.get("side")
            or position.get("positionSide")
            or info.get("posSide")
            or info.get("positionSide")
            or ""
        ).lower()

    @classmethod
    def _short_quantity_from_position(cls, position: dict) -> float:
        if not isinstance(position, dict):
            raise ValueError("invalid position payload")
        info = position.get("info") or {}
        side = cls._position_side(position, info)
        raw_contracts = position.get("contracts")
        if raw_contracts is not None:
            size = abs(float(raw_contracts)) * abs(
                float(position.get("contractSize") or 1.0)
            )
            if size <= 1e-12:
                return 0.0
            if side != "short":
                raise ValueError(
                    f"unexpected non-short futures exposure (side={side or 'unknown'})"
                )
            return size
        raw_amount = position.get("positionAmt", info.get("positionAmt"))
        if raw_amount is None:
            raise ValueError("position size unavailable")
        signed_amount = float(raw_amount)
        if abs(signed_amount) <= 1e-12:
            return 0.0
        if signed_amount >= 0:
            raise ValueError("unexpected long or unsigned futures exposure")
        return abs(signed_amount)

    async def _fetch_recovered_short_quantity(self, global_symbol: str, items):
        """Return (exchange short quantity, mismatch reason) for one venue symbol."""
        token = None
        try:
            token = self._bind_pair_client(self._pair_info_for_tag(items[0][0]))
            positions = None
            for attempt in range(1, 4):
                try:
                    positions = await self.client.fetch_positions(global_symbol)
                    if positions is not None:
                        break
                except Exception:
                    if attempt < 3:
                        await asyncio.sleep(1.0 * attempt)
                    else:
                        raise
            if positions is None:
                raise ValueError("fetch_positions returned None")
            actual_short_qty = 0.0
            for position in positions:
                if not isinstance(position, dict):
                    raise ValueError("invalid position payload")
                actual_short_qty += self._short_quantity_from_position(position)
        except Exception as exc:
            return None, f"cannot verify futures position after restart: {exc}"
        finally:
            if token is not None:
                self._reset_pair_client(token)

        expected_qty = sum(pos.qty for _, pos in items)
        tolerance = max(1e-10, abs(expected_qty) * 0.01)
        if abs(actual_short_qty - expected_qty) <= tolerance:
            return actual_short_qty, None
        return actual_short_qty, (
            f"futures quantity mismatch after restart: expected={expected_qty}, "
            f"exchange={actual_short_qty}"
        )

    async def _mark_recovered_group_mismatch(
        self, global_symbol: str, items, reason: str, actual_short_qty
    ) -> None:
        logger.critical(
            f"[Recovery] {global_symbol}: {reason}; restored positions blocked"
        )
        for pair_tag, pos in items:
            pos.state = "RECONCILIATION_REQUIRED"
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            await self.redis.set(
                f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
            )
            self._mark_failed(
                pair_tag,
                "RECOVERY_POSITION_MISMATCH",
                reason,
                detail={
                    "global_symbol": global_symbol,
                    "expected_qty": pos.qty,
                    "exchange_total_short_qty": actual_short_qty,
                },
                state=PositionState.RECONCILIATION_REQUIRED,
            )

    async def _verify_recovered_active_positions(self) -> set:
        """Verify restored live positions on both legs before unlocking entry.

        A futures short with no spot inventory is not delta neutral: the hedge
        protects nothing, and the exit path cannot sell what the local venue
        does not hold. Both legs must agree with the restored snapshot.
        """
        verified_pairs = set()
        if not self.active_positions:
            return verified_pairs
        groups = self._group_recovered_positions()
        if not hasattr(self.client, "fetch_positions"):
            logger.critical(
                "[Recovery] live client cannot fetch futures positions; "
                "all restored positions require manual reconciliation"
            )
            for (_, symbol), items in groups.items():
                await self._mark_recovered_group_unverified(symbol, items)
            return verified_pairs

        spot_deficits = await self._verify_recovered_spot_legs()

        for (_, global_symbol), items in groups.items():
            actual_qty, reason = await self._fetch_recovered_short_quantity(
                global_symbol, items
            )
            if reason is None and global_symbol not in spot_deficits:
                verified_pairs.update(pair_tag for pair_tag, _ in items)
                logger.info(
                    f"[Recovery] verified {global_symbol} short quantity "
                    f"{actual_qty} against Redis snapshot "
                    f"{sum(pos.qty for _, pos in items)}"
                )
                continue
            if reason is None:
                reason = spot_deficits[global_symbol]
            await self._mark_recovered_group_mismatch(
                global_symbol, items, reason, actual_qty
            )
        return verified_pairs

    @staticmethod
    def _base_asset_from_symbol(symbol: str) -> str:
        """Best-effort base asset for a venue symbol ('toko:spx_idr' -> 'SPX')."""
        from executor.helpers import extract_base_asset

        return extract_base_asset(symbol)

    async def _verify_recovered_spot_legs(self) -> dict:
        """Return {global_symbol: reason} for pairs whose spot inventory is short.

        Read-only. Never sells, never sizes an order: an operator must see the
        mismatch before any position is re-opened for trading.
        """
        deficits: dict = {}
        if not self.active_positions:
            return deficits
        balance_of = getattr(self.client, "fetch_spot_total_balance", None) or getattr(
            self.client, "fetch_spot_free_balance", None
        )
        if not callable(balance_of):
            logger.warning(
                "[Recovery] client cannot read local spot balances; "
                "spot leg is unverified for %d restored position(s)",
                len(self.active_positions),
            )
            for pair_tag, pos in self.active_positions.items():
                symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
                deficits[symbol] = (
                    "local spot balance API is unavailable; spot leg could not be "
                    "verified after restart"
                )
            return deficits

        # Aggregate by (local venue, base asset), not by pair tag: several
        # futures routes can consume the same spot inventory on one CEX.
        expected: dict = {}
        for pair_tag, pos in self.active_positions.items():
            base_asset = str(
                getattr(pos, "base_asset", "")
                or getattr(self._pair_infos.get(pair_tag), "base_asset", "")
                or ""
            ).strip().upper()
            if not base_asset:
                base_asset = self._base_asset_from_symbol(
                    getattr(pos, "symbol", "") or ""
                )
            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            if not base_asset:
                deficits[symbol] = (
                    "base asset is unknown; local spot leg cannot be verified"
                )
                continue
            local_venue = str(getattr(pos, "local_exchange", "") or "").lower()
            try:
                quantity = float(getattr(pos, "qty", 0.0) or 0.0)
                if not math.isfinite(quantity) or quantity < 0:
                    raise ValueError("recovered spot quantity is invalid")
            except (TypeError, ValueError) as exc:
                deficits[symbol] = (
                    f"cannot verify local spot {base_asset} quantity: {exc}"
                )
                continue
            key = (local_venue, base_asset)
            group = expected.setdefault(
                key, {"quantity": 0.0, "symbols": set()}
            )
            group["quantity"] += quantity
            group["symbols"].add(symbol)

        for (local_venue, base_asset), group in expected.items():
            expected_qty = group["quantity"]
            symbols = group["symbols"]
            try:
                if not local_venue:
                    raise ValueError("local spot venue is missing; venue-specific balance cannot be verified")
                try:
                    actual = await balance_of(base_asset, venue=local_venue)
                except TypeError as exc:
                    default_venue = str(
                        getattr(getattr(self, "config", None), "local_exchange_name", "") or ""
                    ).strip().lower()
                    if local_venue != default_venue:
                        raise RuntimeError(
                            f"balance client does not support venue-scoped lookup for {local_venue}"
                        ) from exc
                    actual = await balance_of(base_asset)
                actual = float(actual)
                if not math.isfinite(actual) or actual < 0:
                    raise ValueError("venue returned an invalid spot balance")
            except Exception as exc:
                reason = (
                    f"cannot verify local spot {base_asset} balance on "
                    f"{local_venue or 'unknown'}: {type(exc).__name__}: {exc}"
                )
                for symbol in symbols:
                    deficits[symbol] = reason
                continue
            tolerance = max(1e-10, abs(expected_qty) * 0.01)
            if actual + tolerance < expected_qty:
                reason = (
                    f"local spot inventory short after restart: expected={expected_qty} "
                    f"{base_asset}, venue free balance={actual} on "
                    f"{local_venue or 'unknown'}"
                )
                for symbol in symbols:
                    deficits[symbol] = reason
                logger.critical(
                    "[Recovery] %s: aggregate spot leg short %s %s on %s (expected %s)",
                    ",".join(sorted(symbols)), expected_qty - actual,
                    base_asset, local_venue, expected_qty,
                )
        return deficits

    async def _persist_pending_entry(
        self,
        pair_tag: str,
        order_info: dict,
        ttl: Optional[int] = None,
        *,
        strict: bool = False,
    ) -> bool:
        """Persist a pending entry; strict callers must prove the shared marker exists."""
        if not self.redis:
            if strict:
                raise RuntimeError("shared Redis is unavailable for pending-entry persistence")
            return False
        try:
            key = PENDING_ENTRY_KEY_PREFIX + pair_tag
            payload = json.dumps(order_info)
            if ttl is not None and ttl > 0:
                if hasattr(self.redis, "setex"):
                    await self.redis.setex(key, int(ttl), payload)
                else:
                    await self.redis.set(key, payload, ex=int(ttl))
            else:
                await self.redis.set(key, payload)
            return True
        except Exception:
            logger.exception(f"[Recovery] failed to persist pending entry for {pair_tag}")
            if strict:
                raise
            return False

    async def _clear_pending_entry(self, pair_tag: str) -> None:
        """Removes the persisted pending-entry record once the order is resolved."""
        if not self.redis:
            return
        try:
            await self.redis.delete(PENDING_ENTRY_KEY_PREFIX + pair_tag)
        except Exception:
            logger.exception(f"[Recovery] failed to clear pending entry for {pair_tag}")

    async def _migrate_pending_entry_key(
        self, pair_tag: str, key: str, order_info: dict, symbol: str
    ):
        if (
            "__" in pair_tag
            or not order_info.get("local_exchange")
            or not order_info.get("global_exchange")
            or not order_info.get("global_symbol")
        ):
            return pair_tag, key
        migrated_tag = ExchangePairKey.from_pair_info({
            "local_exchange": order_info["local_exchange"],
            "local_symbol": symbol,
            "global_exchange": order_info["global_exchange"],
            "global_symbol": order_info["global_symbol"],
        }).tag.lower()
        if migrated_tag == pair_tag:
            return pair_tag, key
        migrated_key = PENDING_ENTRY_KEY_PREFIX + migrated_tag
        if await self.redis.exists(migrated_key):
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            self._mark_failed(
                pair_tag,
                "LEGACY_PENDING_KEY_COLLISION",
                "Legacy pending entry cannot be migrated because its venue-qualified key already exists.",
                detail={"legacy_key": key, "target_key": migrated_key},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            return None
        await self.redis.set(migrated_key, json.dumps(order_info))
        await self.redis.delete(key)
        return migrated_tag, migrated_key

    def _retain_invalid_pending_entry(
        self, pair_tag: str, order_info: dict, order_id: str, symbol: str
    ) -> None:
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

    def _retain_unknown_pending_entry(
        self, pair_tag: str, order_info: dict, order_id: str, symbol: str
    ) -> None:
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

    async def _adopt_open_pending_entry(
        self, pair_tag: str, order_info: dict, order_id: str, filled: float
    ) -> None:
        # Redis may contain a newer cumulative fill than a lagging venue
        # snapshot. Recovery must never move the Spot watermark backwards.
        previous_filled = float(order_info.get("filled", 0.0) or 0.0)
        venue_filled = float(filled or 0.0)
        if (
            not math.isfinite(previous_filled)
            or not math.isfinite(venue_filled)
            or previous_filled < 0
            or venue_filled < 0
        ):
            raise ValueError("invalid cumulative Spot fill watermark during adoption")
        order_info["filled"] = max(previous_filled, venue_filled)
        order_info["filled_spot_amount"] = order_info["filled"]
        self.pending_entry_orders[pair_tag] = order_info
        self.states[pair_tag] = (
            PositionState.ENTRY_PARTIAL
            if order_info["filled"] > 0
            else PositionState.ENTRY_PENDING
        )
        await self._persist_pending_entry(pair_tag, order_info)
        logger.warning(
            f"[Recovery] re-adopted pending entry order {order_id} "
            f"for {pair_tag} (filled={filled})"
        )

    async def _reconcile_terminal_pending_entry(
        self, pair_tag: str, order_info: dict, order_id: str,
        symbol: str, status: str,
    ) -> None:
        token = None
        try:
            if not getattr(self.config, "mock_mode", False):
                token = self._bind_pair_client(self._pair_info_for_tag(pair_tag))
            self.pending_entry_orders[pair_tag] = order_info
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            order_info["spot_terminal"] = True
            order_info["spot_terminal_status"] = str(status or "closed").lower()
            order_info["spot_terminal_order"] = {
                "id": order_id,
                "status": order_info["spot_terminal_status"],
                "filled": float(order_info.get("filled", 0.0) or 0.0),
                "average": order_info.get("average"),
                "price": order_info.get("price"),
            }
            if not await self._reconcile_pending_entry_hedges(pair_tag, order_info):
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                self._mark_failed(
                    pair_tag,
                    "HEDGE_RECOVERY_UNVERIFIED",
                    "A previously submitted futures hedge could not be verified; "
                    "the spot fill was retained without submitting a duplicate hedge.",
                    detail={"order_id": order_id, "symbol": symbol},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                return
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
                return
            await self._finalize_entry_order(pair_tag, status)
            logger.info(
                f"[Recovery] reconciled terminal order {order_id} for "
                f"{pair_tag} (status={status}, filled={order_info['filled']})"
            )
        finally:
            if token is not None:
                self._reset_pair_client(token)

    def _retain_unsupported_pending_entry(
        self, pair_tag: str, order_info: dict, order_id: str,
        symbol: str, status: str,
    ) -> None:
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

    async def _recover_pending_entry_record(self, key: str) -> None:
        pair_tag = key.split(PENDING_ENTRY_KEY_PREFIX, 1)[-1]
        raw = await self.redis.get(key)
        if not raw:
            return
        order_info = json.loads(raw)
        order_id = str(order_info.get("order_id", ""))
        symbol = str(order_info.get("symbol", ""))
        migrated = await self._migrate_pending_entry_key(
            pair_tag, key, order_info, symbol
        )
        if migrated is None:
            return
        pair_tag, _ = migrated
        self.pending_entry_orders[pair_tag] = order_info
        if not order_id or not symbol:
            self._retain_invalid_pending_entry(pair_tag, order_info, order_id, symbol)
            return

        # A record whose hedge venue and hedge symbol disagree cannot be bound
        # to any client, so every later monitor tick would raise. Treat it as a
        # corrupt record (quarantine + halt the scope) instead of adopting it.
        global_exchange = str(
            order_info.get("global_exchange") or getattr(self.config, "global_exchange_name", "") or "okx"
        ).strip().lower()
        local_exchange = str(
            order_info.get("local_exchange")
            or (pair_tag.split(":")[0] if ":" in pair_tag else getattr(self.config, "local_exchange_name", ""))
            or "indodax"
        ).strip().lower()
        local_symbol = str(symbol or "")
        global_symbol = str(order_info.get("global_symbol") or local_symbol or "").strip()
        try:
            ExchangePairKey.create(
                local_exchange, local_symbol, global_exchange, global_symbol
            )
        except ValueError as key_error:
            logger.critical(
                "[Recovery] quarantining pending entry %s: unroutable pair identity (%s)",
                pair_tag, key_error,
            )
            self.pending_entry_orders.pop(pair_tag, None)
            await self.redis.delete(PENDING_ENTRY_KEY_PREFIX + pair_tag)
            self._mark_failed(
                pair_tag,
                "PENDING_ENTRY_IDENTITY_INVALID",
                f"Persisted pending entry cannot be routed to a venue: {key_error}",
                detail={
                    "order_id": order_id,
                    "local_exchange": local_exchange,
                    "local_symbol": local_symbol,
                    "global_exchange": global_exchange,
                    "global_symbol": global_symbol,
                },
                state=PositionState.FAILED,
            )
            return

        status, filled = await self._resolve_venue_order_state(
            order_id, symbol, pair_tag
        )
        if status == "unknown":
            self._retain_unknown_pending_entry(
                pair_tag, order_info, order_id, symbol
            )
            return

        # The persisted value is a durable lower bound. Venue order snapshots
        # can temporarily regress (eventual consistency/private-stream lag),
        # so taking the venue value directly can erase a previously observed
        # Spot fill and under-hedge the position after restart.
        try:
            persisted_filled = float(order_info.get("filled", 0.0) or 0.0)
            venue_filled = float(filled or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid cumulative Spot fill watermark in recovery") from exc
        if (
            not math.isfinite(persisted_filled)
            or not math.isfinite(venue_filled)
            or persisted_filled < 0
            or venue_filled < 0
        ):
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            self._mark_failed(
                pair_tag,
                "ENTRY_FILL_WATERMARK_INVALID",
                "Persisted or venue cumulative Spot fill is invalid; recovery retained the record.",
                detail={
                    "order_id": order_id,
                    "persisted_filled": repr(order_info.get("filled")),
                    "venue_filled": repr(filled),
                },
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            return
        order_info["filled"] = max(persisted_filled, venue_filled)
        order_info["filled_spot_amount"] = order_info["filled"]
        if status == "open":
            await self._adopt_open_pending_entry(
                pair_tag, order_info, order_id, filled
            )
            return
        if status in ("closed", "canceled", "cancelled", "rejected", "expired"):
            # Preserve the monotonic max(persisted, venue) watermark above.
            # A terminal snapshot can be stale just like an open-order snapshot.
            await self._reconcile_terminal_pending_entry(
                pair_tag, order_info, order_id, symbol, status
            )
            return
        self._retain_unsupported_pending_entry(
            pair_tag, order_info, order_id, symbol, status
        )

    async def _recover_pending_entry_orders(self) -> None:
        """Restore persisted pending entries against their exact local venues."""
        try:
            keys = await self.redis.keys(PENDING_ENTRY_KEY_PREFIX + "*")
        except Exception as exc:
            logger.exception("[Recovery] failed to scan arb:pending_entry:* keys")
            self._halt_exposure_scope(
                "*",
                f"pending-entry inventory unavailable: {type(exc).__name__}: {exc}",
            )
            self._refresh_entry_gate()
            return
        for key in keys or []:
            try:
                await self._recover_pending_entry_record(key)
            except Exception as exc:
                pair_tag = str(key).split(PENDING_ENTRY_KEY_PREFIX, 1)[-1]
                logger.exception(
                    "[Recovery] failed to restore pending entry from %s", key
                )
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                self._halt_exposure_scope(
                    pair_tag or "*",
                    "persisted pending-entry record could not be reconciled",
                )
                self._mark_failed(
                    pair_tag or "startup",
                    "PENDING_ENTRY_RECOVERY_FAILED",
                    f"Pending entry recovery failed: {type(exc).__name__}: {exc}",
                    detail={"redis_key": str(key)},
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
        self._refresh_entry_gate()

    async def _resolve_venue_order_state(
        self, order_id: str, symbol: str, pair_tag: Optional[str] = None
    ) -> tuple:
        """Returns (status, filled) using the exact local venue when known."""
        token = None
        order = None
        try:
            if pair_tag and not getattr(self.config, "mock_mode", False):
                token = self._bind_pair_client(self._pair_info_for_tag(pair_tag))
            for attempt in range(1, 4):
                try:
                    order = await self.client.fetch_local_order(order_id, symbol)
                    break
                except Exception as exc:
                    if is_explicit_rate_limit_rejection(exc) and attempt < 3:
                        delay = 5.0
                        match = re.search(r"try again in (\d+)", str(exc))
                        if match:
                            delay = max(delay, float(match.group(1)) + 1.0)
                        logger.warning(
                            f"[Recovery] rate limited fetching order {order_id} ({symbol}) attempt {attempt}/3; waiting {delay:.1f}s"
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise
        except Exception as exc:
            logger.warning(f"[Recovery] fetch order {order_id} failed: {exc}")
            return "unknown", None
        finally:
            if token is not None:
                self._reset_pair_client(token)
        if not isinstance(order, dict) or not order.get("id") and order_id not in str(order):
            # A missing response is ambiguous (history retention, API lag, or
            # wrong symbol); never interpret it as a safely canceled order.
            return "unknown", None
        return str(order.get("status", "unknown")), float(order.get("filled", 0.0) or 0.0)

    async def _migrate_active_position_key(
        self, pair_tag: str, key: str, pos: ActivePosition
    ):
        if "__" in pair_tag or not pos.global_exchange or not pos.global_symbol:
            return pair_tag, key
        migrated_tag = ExchangePairKey.from_pair_info({
            "local_exchange": pos.local_exchange,
            "local_symbol": pos.symbol,
            "global_exchange": pos.global_exchange,
            "global_symbol": pos.global_symbol,
        }).tag.lower()
        if migrated_tag == pair_tag:
            return pair_tag, key
        migrated_key = f"arb:active_pos:{migrated_tag}"
        if await self.redis.get(migrated_key):
            pos.state = "RECONCILIATION_REQUIRED"
            self.active_positions[pair_tag] = pos
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            await self.redis.set(key, json.dumps(pos.to_dict()))
            self._mark_failed(
                pair_tag,
                "LEGACY_POSITION_KEY_COLLISION",
                "Legacy position cannot be migrated because the venue-qualified key already exists.",
                detail={"legacy_key": key, "target_key": migrated_key},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            return None
        await self.redis.set(migrated_key, json.dumps(pos.to_dict()))
        await self.redis.delete(key)
        return migrated_tag, migrated_key

    async def _retain_invalid_recovered_position(
        self, pair_tag: str, key: str, pos: ActivePosition
    ) -> None:
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

    def _restore_active_position_state(
        self, pair_tag: str, pos: ActivePosition
    ) -> None:
        self.active_positions[pair_tag] = pos
        if getattr(pos, "state", "HEDGED_ACTIVE") == "RECONCILIATION_REQUIRED":
            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
            self._failure_reasons[pair_tag] = {
                "reason": "RESTORED_RECONCILIATION_REQUIRED",
                "message": (
                    "Position restored from Redis with RECONCILIATION_REQUIRED state. "
                    "Manual intervention required."
                ),
                "detail": {"qty": pos.qty, "symbol": pos.symbol},
                "timestamp_ms": int(time.time() * 1000),
            }
            logger.critical(
                f"[Recovery] Position {pair_tag} has RECONCILIATION_REQUIRED status! "
                "Manual intervention required; skipping automatic active exit management."
            )
        else:
            self.states[pair_tag] = PositionState.HEDGED_ACTIVE

    async def _recover_active_position_record(self, key: str) -> bool:
        pair_tag = key.split("arb:active_pos:", 1)[-1]
        try:
            raw = await self.redis.get(key)
            if not raw:
                return False
            pos = ActivePosition.from_dict(json.loads(raw))
            migrated = await self._migrate_active_position_key(pair_tag, key, pos)
            if migrated is None:
                return False
            pair_tag, key = migrated
            if pos.qty <= 0:
                await self._retain_invalid_recovered_position(pair_tag, key, pos)
                return False

            self._restore_active_position_state(pair_tag, pos)
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
            return True
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
            return False

    async def _recover_active_positions(self) -> None:
        """Restore Redis positions and preserve unresolved records for reconciliation."""
        try:
            keys = await self.redis.keys("arb:active_pos:*")
        except Exception as exc:
            logger.exception("[Recovery] failed to scan arb:active_pos:* keys")
            self._halt_exposure_scope(
                "*",
                f"active-position inventory unavailable: {type(exc).__name__}: {exc}",
            )
            self._refresh_entry_gate()
            return

        recovered = 0
        for key in keys or []:
            if await self._recover_active_position_record(key):
                recovered += 1
        if recovered:
            logger.warning(
                f"[Recovery] {recovered} active position(s) restored - resuming management"
            )
