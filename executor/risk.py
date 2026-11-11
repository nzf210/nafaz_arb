"""Capital-exposure, venue-halt and balance-mismatch risk controls.

Extracted from executor/entry.py; mixed into ArbitrageEngine so the self.*
state and helper methods stay on one object.
"""

import asyncio
import inspect
import logging
import math
from typing import Dict, Optional

from executor.models import ExecutorPayload, PairInfo
from executor.pair_registry import ExchangePairKey

logger = logging.getLogger("executor.engine")


class RiskMixin:
    """Exposure caps, per-venue halts and balance-drift detection."""


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
        ok = writer.risk_state(
            state_type=state_type, scope=scope, exchange=exchange,
            pair_key=pair_key, reason=reason, value=value, resolved=resolved,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        if not ok and not getattr(self.config, "mock_mode", False):
            self._entry_blocked = True
            self._risk_persistence_failed = True
            logger.critical("LIVE entry blocked: risk state could not be journaled")
        return bool(ok)

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
                logger.critical("LIVE startup fail-closed: configure durable Supabase risk-state storage")
            return
        rows = await writer.fetch_active_risk_states(
            is_mock=bool(getattr(self.config, "mock_mode", False))
        )
        for row in rows:
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
            if kind == "trading_halt" and scope == "global":
                self._trading_halted = True
                self._halt_reason = str((value or {}).get("halt_reason") or reason or "RESTORED_MANUAL_HALT")
            elif kind == "daily_loss" and scope == "global":
                state = value if isinstance(value, dict) else {}
                if state.get("loss_day") == self._utc_day():
                    self._loss_day = str(state["loss_day"])
                    self._daily_realized_loss_usdt = max(0.0, float(state.get("loss_usdt", 0.0)))
                else:
                    self._loss_day = self._utc_day()
                    self._daily_realized_loss_usdt = 0.0
            elif kind == "exchange_halt" and scope == "exchange" and exchange:
                self._exchange_halted[exchange] = reason
            elif kind == "consecutive_order_failures" and scope == "exchange" and exchange:
                self._consecutive_order_failures[exchange] = max(0, int((value or {}).get("count", 0)))
            elif kind == "reconciliation_halt":
                key = "*" if scope == "global" else exchange if scope == "exchange" else pair_key
                if key:
                    self._recovery_scope_halts[key] = reason
                    if key == "*":
                        self._entry_blocked = True
            elif kind == "position_failure" and pair_key:
                failure = value if isinstance(value, dict) else {}
                self._failure_reasons[pair_key] = failure or {
                    "reason": reason, "message": reason, "detail": {},
                    "timestamp_ms": int(time.time() * 1000),
                }
                self.states[pair_key] = __import__("executor.models", fromlist=["PositionState"]).PositionState.RECONCILIATION_REQUIRED
        self._risk_state_restored = True
        # A restored loss halt remains active until the same UTC day rolls over.
        self._roll_loss_day_if_needed()
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
            self._exchange_halted[exchange] = f"{count} consecutive order failures ({reason})"\n            self._persist_risk_state("exchange_halt", scope="exchange", exchange=exchange, reason=self._exchange_halted[exchange], value={"source": "consecutive_order_failures"})
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
        self._exchange_halted[exchange] = f"rate limited ({seconds:.0f}s cooldown)"\n        self._persist_risk_state("exchange_halt", scope="exchange", exchange=exchange, reason=self._exchange_halted[exchange], value={"source": "rate_limit_cooldown", "seconds": seconds})
        token = self._venue_cooldown_epoch = getattr(self, "_venue_cooldown_epoch", 0) + 1

        async def _clear_after_cooldown():
            try:
                await asyncio.sleep(seconds)
            except asyncio.CancelledError:
                return
            # Only clear the halt this task installed; a newer cooldown or a
            # manual halt must not be undone by a stale timer.
            if getattr(self, "_venue_cooldown_epoch", 0) != token:
                return
            if self._exchange_halted.get(exchange, "").startswith("rate limited"):
                del self._exchange_halted[exchange]
                self._persist_risk_state("exchange_halt", scope="exchange", exchange=exchange, resolved=True)
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

    async def _get_balance_snapshot(
        self,
        pair_tag: Optional[str] = None,
        pair_info: Optional[PairInfo] = None,
        symbol: Optional[str] = None,
        usdt_idr_rate: float = 0.0,
    ) -> Optional[dict]:
        """Returns a normalized USDT-equivalent balance snapshot from live or paper client."""
        if pair_info is None and pair_tag:
            pair_info = self._pair_infos.get(pair_tag)

        local_symbol = (pair_info.local_symbol if pair_info else symbol) or symbol or ""
        quote_asset = (pair_info.quote_asset if pair_info else "").upper()
        if not quote_asset and local_symbol:
            normalized = local_symbol.upper().split(":")[0].replace("/", "_").replace("-", "_")
            quote_asset = normalized.split("_")[-1] if "_" in normalized else "IDR"

        global_symbol = pair_info.global_symbol.upper() if pair_info else ""
        global_quote_asset = "USDC" if "USDC" in global_symbol else "USDT"
        if not global_symbol and quote_asset == "USDC":
            global_quote_asset = "USDC"

        getter = getattr(self.client, "get_balances", None)
        if not callable(getter):
            if getattr(self.config, "mock_mode", False):
                return None
            raise RuntimeError("exchange client does not implement get_balances")

        balance_kwargs = {
            "local_quote_asset": quote_asset or "IDR",
            "global_quote_asset": global_quote_asset,
            "usdt_idr_rate": float(usdt_idr_rate or 0.0),
        }
        try:
            signature = inspect.signature(getter)
        except (TypeError, ValueError):
            signature = None
        if signature is None or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        ):
            result = getter(**balance_kwargs)
        else:
            accepted = {
                key: value for key, value in balance_kwargs.items()
                if key in signature.parameters
            }
            result = getter(**accepted)
        if inspect.isawaitable(result):
            result = await result

        if (
            not isinstance(result, dict)
            or "spot_available" not in result
            or "futures_available" not in result
        ):
            raise ValueError("balance response missing spot_available/futures_available")
        for field in ("spot_available", "futures_available"):
            value = float(result[field])
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"balance response contains invalid {field}")
            result[field] = value
        return result

    async def _check_balance_mismatch(
        self,
        pair_info: Optional[PairInfo] = None,
        usdt_idr_rate: float = 0.0,
        pair_tag: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Detects unexpected available-balance drops and returns the same normalized
        snapshot for entry sizing. Live balance read failures halt new entries.
        Existing positions remain managed; this check never blocks a required hedge.
        """
        local_name = pair_info.local_exchange if pair_info else self.config.local_exchange_name
        global_name = pair_info.global_exchange if pair_info else self.config.global_exchange_name
        try:
            balances = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                pair_info=pair_info,
                usdt_idr_rate=usdt_idr_rate,
            )
        except Exception as exc:
            reason = f"balance feed unavailable: {exc}"
            self._exchange_halted[local_name] = reason
            self._exchange_halted[global_name] = reason
            self._persist_risk_state("exchange_halt", scope="exchange", exchange=local_name, reason=reason, value={"source": "balance_feed_unavailable"})
            self._persist_risk_state("exchange_halt", scope="exchange", exchange=global_name, reason=reason, value={"source": "balance_feed_unavailable"})
            logger.exception(
                "[Risk Control] balance read failed; both venues halted for new entries"
            )
            await self._publish_execution_failure(
                pair_tag or "unknown",
                "BALANCE_FEED_UNAVAILABLE",
                "Gagal membaca saldo live; entry dihentikan pada kedua exchange.",
                detail={"local_exchange": local_name, "global_exchange": global_name, "error": str(exc)},
            )
            return None

        if balances is None:
            if pair_tag:
                self._balance_snapshots.pop(pair_tag, None)
            return None
        if pair_tag:
            self._balance_snapshots[pair_tag] = balances

        fraction = float(getattr(self.config, "balance_mismatch_fraction", 0.0) or 0.0)
        if fraction <= 0:
            return balances

        venue_available = {
            local_name: float(balances.get("spot_available", 0.0) or 0.0),
            global_name: float(balances.get("futures_available", 0.0) or 0.0),
        }
        pending_local, pending_global = self._pending_entry_exposure_all()
        committed = self._open_exposure_by_venue()
        committed[local_name] = committed.get(local_name, 0.0) + pending_local
        committed[global_name] = committed.get(global_name, 0.0) + pending_global

        for venue, avail in venue_available.items():
            last = self._last_balances.get(venue)
            self._last_balances[venue] = avail
            if last is None or last <= 0:
                continue
            drop = last - avail
            if drop <= 0 or drop <= last * fraction:
                continue
            explained = committed.get(venue, 0.0)
            if drop <= explained * (1.0 + 0.01):
                continue
            self._exchange_halted[venue] = (
                f"balance mismatch: available dropped {last:.2f} -> {avail:.2f} "
                f"(-{drop:.2f}) with only {explained:.2f} committed"
            )
            logger.critical(
                f"[Risk Control] Exchange {venue} HALTED: {self._exchange_halted[venue]}"
            )
            await self._publish_execution_failure(
                pair_tag or "unknown",
                "BALANCE_MISMATCH",
                f"Saldo {venue} berubah tidak sesuai exposure tercatat; entry dihentikan.",
                detail={"exchange": venue, "available_drop": drop, "committed_exposure": explained},
            )
        return balances

    def _open_exposure_by_venue(self) -> Dict[str, float]:
        """Capital currently committed to open positions, keyed by exchange name."""
        by_venue: Dict[str, float] = {}
        for pos in self.active_positions.values():
            if pos.local_exchange:
                by_venue[pos.local_exchange] = by_venue.get(pos.local_exchange, 0.0) + pos.spot_cost_usdt
            if pos.global_exchange:
                by_venue[pos.global_exchange] = by_venue.get(pos.global_exchange, 0.0) + pos.futures_margin_usdt
        return by_venue

    def _pending_entry_exposure_all(self) -> tuple[float, float]:
        """
        Capital reserved by resting entry orders across ALL pairs.
        Mirrors _pending_entry_exposure but venue-agnostic, for the global
        balance-mismatch sweep which runs outside any single pair's context.
        """
        total_spot = 0.0
        total_futures = 0.0
        for order_info in self.pending_entry_orders.values():
            remaining_qty = float(order_info.get("qty", 0.0)) - float(order_info.get("filled", 0.0))
            remaining_qty = max(0.0, remaining_qty)
            original_qty = float(order_info.get("qty", 0.0) or 0.0)
            if original_qty <= 0:
                continue
            remaining_fraction = min(1.0, remaining_qty / original_qty)
            # The pending order already stores the USDT-equivalent reservation
            # calculated from the validated live FX snapshot at placement time.
            # Scale that persisted amount by the unfilled fraction; never
            # recalculate with a hardcoded FX fallback.
            total_spot += float(order_info.get("spot_cost_usdt", 0.0) or 0.0) * remaining_fraction
            total_futures += float(order_info.get("futures_margin_usdt", 0.0) or 0.0) * remaining_fraction
        return total_spot, total_futures

    def _exposure_allows(
        self, pair_tag: str, pair_info: PairInfo, planned_usdt: float
    ) -> bool:
        """
        Checks the planned order against ``max_exposure_per_exchange_usdt``.

        Exposure is the capital already committed to open positions plus any
        entry orders still resting, counted separately per exchange so a single
        venue cannot be concentrated beyond the configured cap.
        """
        cap = float(getattr(self.config, "max_exposure_per_exchange_usdt", 0.0) or 0.0)
        if cap <= 0:
            return True

        local_exposure = 0.0
        global_exposure = 0.0
        for tag, pos in self.active_positions.items():
            if tag == pair_tag:
                continue
            if pos.local_exchange == pair_info.local_exchange:
                local_exposure += pos.spot_cost_usdt
            if pos.global_exchange == pair_info.global_exchange:
                global_exposure += pos.futures_margin_usdt

        pending_local, pending_global = self._pending_entry_exposure(
            pair_tag, pair_info
        )
        local_exposure += pending_local
        global_exposure += pending_global
        # The spot leg and the futures margin each consume their own venue's cap.
        blocked = (
            local_exposure + planned_usdt > cap
            or global_exposure + planned_usdt > cap
        )
        if blocked:
            logger.warning(
                f"[State 1 Exposure Cap] Rejecting {pair_tag}: planned ${planned_usdt:.2f} would "
                f"push {pair_info.local_exchange} to ${local_exposure + planned_usdt:.2f} "
                f"and {pair_info.global_exchange} to ${global_exposure + planned_usdt:.2f} "
                f"(cap ${cap:.2f} each)"
            )
            return False
        return True

    def _pending_entry_exposure(
        self, pair_tag: str, pair_info: PairInfo
    ) -> tuple[float, float]:
        """
        Capital already reserved by resting entry orders, per exchange.

        An order that was accepted but has not filled yet still consumes the cap:
        ignoring it lets a burst of signals oversubscribe the same venue before
        any of them turns into a position.
        """
        local_exposure = 0.0
        global_exposure = 0.0
        for tag, info in self.pending_entry_orders.items():
            if tag == pair_tag:
                continue
            if info.get("local_exchange") == pair_info.local_exchange:
                local_exposure += float(info.get("spot_cost_usdt", 0.0))
            if info.get("global_exchange") == pair_info.global_exchange:
                global_exposure += float(info.get("futures_margin_usdt", 0.0))
        return local_exposure, global_exposure

    def _recovery_scope_keys(self, pair_tag: str) -> list:
        """Candidate recovery gate keys for a pair, most specific first.

        Recovery gates exposure at whatever granularity it could prove: the pair,
        then the market, then the venue. Every level is checked so an unrelated
        symbol's orphan does not block trading on a healthy market.
        """
        keys = [pair_tag] if pair_tag else []
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag) if pair_tag else None
        # Recovery keys pair gates by the venue-qualified tag while an incoming
        # signal carries the local-only tag, so both must be checked.
        if pair_info is not None:
            try:
                keys.append(ExchangePairKey.from_pair_info(pair_info).tag.lower())
            except Exception:
                pass
        local_ex, global_ex = self._pair_venues(pair_tag)
        local_symbol = str(getattr(pair_info, "local_symbol", "") or "")
        global_symbol = str(getattr(pair_info, "global_symbol", "") or "")
        if not local_symbol and pair_tag and ":" in pair_tag:
            local_symbol = pair_tag.split(":", 1)[1]
        for venue, symbol in ((local_ex, local_symbol), (global_ex, global_symbol)):
            if venue and symbol:
                keys.append(f"{venue}:{symbol}")
        keys.extend(venue for venue in (local_ex, global_ex) if venue)
        return keys

    def _venue_halted_reason(self, pair_tag: str) -> str:
        """Returns the venue pair halted, or '' when clear."""
        # Recovery-installed gates first: an unresolved orphan or unverifiable
        # order blocks the pair/market/venue it affects, never the whole bot.
        recovery_halts = getattr(self, "_recovery_scope_halts", None) or {}
        if "*" in recovery_halts:
            return f"global: {recovery_halts['*']}"
        for key in self._recovery_scope_keys(pair_tag):
            if key in recovery_halts:
                return f"{key}: {recovery_halts[key]}"
        local_ex, global_ex = self._pair_venues(pair_tag)
        if local_ex and local_ex in self._exchange_halted:
            return f"{local_ex}: {self._exchange_halted[local_ex]}"
        if global_ex and global_ex in self._exchange_halted:
            return f"{global_ex}: {self._exchange_halted[global_ex]}"
        return ""

    async def _compute_entry_sizing(
        self, pair_tag: str, payload: ExecutorPayload
    ) -> Optional[tuple[float, float]]:
        """
        Sizes an entry from the signal budget, exposure cap and fee reserve.
        Both legs commit the same USDT notional, so the budget is capped to
        min(spot, futures) available on every venue mode; when the balance is
        unreadable the size is left uncapped and the funding preflight decides.
        """
        directives = payload.execution_directives
        pair_info = payload.pair_info

        max_pos_usdt = directives.max_position_usdt
        if max_pos_usdt <= 0:
            max_pos_usdt = 100.0  # fallback budget

        min_cap = getattr(self.config, "min_allocated_capital", 10.0)
        mock_mode = bool(getattr(self.config, "mock_mode", False))

        # Both legs commit the same USDT notional, so the entry must be capped to
        # min(spot, futures) for live venues too, not just simulated ones: a
        # directive that outruns the thinner leg would otherwise reach the
        # funding preflight and be rejected as underfunded instead of sized down.
        balances = self._balance_snapshots.pop(pair_tag, None)
        if balances is None and mock_mode:
            balances = await self._check_balance_mismatch(
                pair_info,
                payload.pricing_snapshot.usdt_idr_rate,
                pair_tag,
            )
        if balances is None and not mock_mode:
            # Live uses its own probe rather than _check_balance_mismatch: a
            # transient read failure must not halt the venues, and the funding
            # preflight downstream already fails open on an unreadable balance.
            try:
                balances = await self._get_balance_snapshot(
                    pair_tag=pair_tag,
                    pair_info=pair_info,
                    usdt_idr_rate=payload.pricing_snapshot.usdt_idr_rate,
                )
            except Exception as exc:
                logger.warning(
                    f"[State 1 Balance Check] balance probe failed for {pair_tag}; "
                    f"sizing uncapped and deferred to the funding preflight: {exc}"
                )
        if balances is not None:
            avail_spot = float(balances["spot_available"])
            avail_fut = float(balances["futures_available"])
            usable_capital = min(avail_spot, avail_fut)

            if usable_capital < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Insufficient available balance for {pair_tag}: "
                    f"Spot Avail=${avail_spot:.2f}, Futures Avail=${avail_fut:.2f}, "
                    f"Min Required=${min_cap:.2f}. Rejecting entry signal."
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "INSUFFICIENT_OR_UNBALANCED_BALANCE",
                    "Saldo Spot/Futures tidak mencukupi atau tidak seimbang untuk membuka posisi.",
                    detail={"spot_available_usdt": avail_spot, "futures_available_usdt": avail_fut, "minimum_required_usdt": min_cap},
                )
                return None

            max_pos_usdt = min(max_pos_usdt, usable_capital)

        if not mock_mode or balances is not None:
            if not self._exposure_allows(pair_tag, pair_info, max_pos_usdt):
                await self._publish_execution_failure(
                    pair_tag,
                    "EXPOSURE_LIMIT_REACHED",
                    "Entry ditolak karena batas exposure exchange akan terlampaui.",
                    detail={"planned_usdt": max_pos_usdt, "limit_usdt": getattr(self.config, "max_exposure_per_exchange_usdt", 0.0)},
                )
                return None

            fee_pct = float(getattr(self.config, "estimated_round_trip_fee_pct", 0.0) or 0.0)
            if fee_pct > 0:
                fee_buffer_usdt = max_pos_usdt * fee_pct
                max_pos_usdt -= fee_buffer_usdt
                logger.debug(
                    f"[State 1 Fee Buffer] {pair_tag} reserved ${fee_buffer_usdt:.4f} "
                    f"({fee_pct * 100:.3f}%) for fees"
                )

            if max_pos_usdt < min_cap:
                logger.warning(
                    f"[State 1 Balance Check] Position size ${max_pos_usdt:.2f} "
                    f"< min required ${min_cap:.2f} for {pair_tag}"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "INSUFFICIENT_AFTER_FEE_RESERVE",
                    "Ukuran posisi setelah cadangan fee berada di bawah minimum exchange.",
                    detail={"position_usdt": max_pos_usdt, "minimum_required_usdt": min_cap},
                )
                return None

        return max_pos_usdt, min_cap

    async def _verify_dual_leg_funding(
        self,
        pair_tag: str,
        pair_info: PairInfo,
        required_usdt: float,
        usdt_idr_rate: float,
    ) -> bool:
        """Confirms both legs can be funded before a live entry is submitted.

        Entry commits two legs at once: a spot buy that locks the local quote
        currency, and a 1x futures short that locks margin on the global venue.
        Checking them together prevents the failure mode where the spot leg fills
        but the hedge leg is rejected for margin, leaving unhedged exposure.
        Returns False (and publishes the reason) when either leg is short.
        """
        try:
            balances = await self._get_balance_snapshot(
                pair_tag=pair_tag,
                pair_info=pair_info,
                usdt_idr_rate=usdt_idr_rate,
            )
        except Exception as exc:
            # A balance probe failure must not silently pass the gate; surface it
            # and let the caller decide, matching the pre-existing fail-open on
            # unsupported clients while logging loudly.
            logger.error(
                f"[State 1 Funding Preflight] balance probe failed for {pair_tag}: "
                f"{type(exc).__name__}: {exc}"
            )
            return True

        if not isinstance(balances, dict):
            return True

        try:
            avail_spot = float(balances.get("spot_available", 0.0) or 0.0)
            avail_fut = float(balances.get("futures_available", 0.0) or 0.0)
        except (TypeError, ValueError):
            logger.error(
                f"[State 1 Funding Preflight] unreadable balance payload for {pair_tag}"
            )
            return True

        if not (math.isfinite(avail_spot) and math.isfinite(avail_fut)):
            logger.error(
                f"[State 1 Funding Preflight] non-finite balance for {pair_tag}"
            )
            return True

        required = float(required_usdt)
        missing = []
        if avail_spot < required:
            missing.append(f"spot ${avail_spot:.2f} < ${required:.2f}")
        if avail_fut < required:
            missing.append(f"futures ${avail_fut:.2f} < ${required:.2f}")
        if not missing:
            logger.debug(
                f"[State 1 Funding Preflight] {pair_tag} funded: "
                f"spot=${avail_spot:.2f} futures=${avail_fut:.2f} required=${required:.2f}"
            )
            return True

        # Both legs commit the same USDT-equivalent notional, so the entry is
        # fundable exactly when the thinner leg covers it. Sizing is normally
        # capped to min(spot, futures) upstream, so reaching here means the
        # directive outran the balance (e.g. scanner allocated capital_total
        # for one leg while the preflight needs it on both); report the cap the
        # caller can actually use instead of only the shortfall.
        fundable_usdt = min(avail_spot, avail_fut)
        logger.warning(
            f"[State 1 Funding Preflight] {pair_tag} underfunded for a dual-leg "
            f"entry ({'; '.join(missing)}); fundable per leg ${fundable_usdt:.2f}"
        )
        await self._publish_execution_failure(
            pair_tag,
            "DUAL_LEG_UNDERFUNDED",
            "Saldo Spot dan/atau Futures tidak cukup untuk mengeksekusi kedua kaki sekaligus.",
            detail={
                "required_usdt_per_leg": required,
                "spot_available_usdt": avail_spot,
                "futures_available_usdt": avail_fut,
                "fundable_usdt_per_leg": fundable_usdt,
                "shortfall": missing,
            },
        )
        return False
