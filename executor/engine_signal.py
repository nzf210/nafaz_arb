"""engine signal methods for arbitrage engine orchestration."""

import json
import time
from typing import Optional

from executor.models import ExecutorPayload
from executor.pair_registry import ExchangePairKey
from executor.engine_common import logger



class EngineSignalMixin:
    """Focused orchestration responsibilities, composed by ArbitrageEngine."""

    async def handle_signal_notification(self, pair_tag: str):
        """Bind the signal to its immutable venue pair before any exchange call."""
        payload = await self._load_signal_payload(pair_tag)
        if payload is None:
            return
        if getattr(self.config, "mock_mode", False):
            await self._handle_signal_notification_bound(pair_tag, payload)
            return
        try:
            token = self._bind_pair_client(payload.pair_info)
        except Exception as exc:
            self._pair_infos[pair_tag] = payload.pair_info
            self._mark_failed(
                pair_tag,
                "EXECUTOR_VENUE_MISMATCH",
                f"Signal rejected: {exc}",
                detail={
                    "local_exchange": payload.pair_info.local_exchange,
                    "global_exchange": payload.pair_info.global_exchange,
                },
            )
            await self.publish_status()
            return
        try:
            await self._handle_signal_notification_bound(pair_tag, payload)
        finally:
            self._reset_pair_client(token)

    async def _handle_signal_notification_bound(
        self, pair_tag: str, payload: Optional[ExecutorPayload] = None
    ):
        """
        State 1 (Entry Order Pegging):
        1. GET arb:signal:{local_ex}:{symbol}
        2. Validate timestamp freshness (<= 500ms).
        3. If no active position, place Maker Limit Buy at recommended_limit_buy_idr.
        """
        async with self._pair_lock(pair_tag):
            if getattr(self, "_risk_persistence_failed", False):
                logger.critical("[Risk] rejecting %s: durable risk-state persistence is not trustworthy", pair_tag)
                return
            self._roll_loss_day_if_needed()
            if payload is None:
                payload = await self._load_signal_payload(pair_tag)
            if payload is None:
                return
            configured_local = str(self.config.local_exchange_name or "").strip().lower()
            configured_global = str(self.config.global_exchange_name or "").strip().lower()
            signal_local = str(payload.pair_info.local_exchange or "").strip().lower()
            signal_global = str(payload.pair_info.global_exchange or "").strip().lower()
            pair_key = ExchangePairKey.from_pair_info(payload.pair_info)
            if (
                not getattr(self.config, "mock_mode", False)
                and not self._pair_client_registry.contains(pair_key)
            ):
                reason = "EXECUTOR_VENUE_MISMATCH"
                message = (
                    "Signal exchange pair does not match this executor instance; "
                    "entry rejected to prevent orders on the wrong venue."
                )
                self._pair_infos[pair_tag] = payload.pair_info
                self._mark_failed(
                    pair_tag,
                    reason,
                    message,
                    detail={
                        "configured_local_exchange": configured_local,
                        "configured_global_exchange": configured_global,
                        "signal_local_exchange": signal_local,
                        "signal_global_exchange": signal_global,
                    },
                )
                await self.publish_status()
                return
            self._pair_infos[pair_tag] = payload.pair_info
            mock_mode = bool(getattr(self.config, "mock_mode", False))
            if mock_mode:
                await self._check_balance_mismatch(
                    payload.pair_info,
                    payload.pricing_snapshot.usdt_idr_rate,
                    pair_tag,
                )
            if self._trading_halted:
                logger.warning(
                    f"[Circuit Breaker] rejecting entry for {pair_tag}: "
                    f"trading halted ({self._halt_reason})"
                )
                return
            if venue_reason := self._venue_halted_reason(pair_tag):
                logger.warning(
                    f"[Risk Control] rejecting entry for {pair_tag}: "
                    f"exchange halted ({venue_reason})"
                )
                return
            await self._handle_signal_locked(pair_tag, payload)

    def _next_order_seq(self) -> int:
        """Monotonic per-process sequence used to keep client order ids unique."""
        self._order_seq += 1
        return self._order_seq

    async def _load_signal_payload(self, pair_tag: str) -> Optional[ExecutorPayload]:
        """Reads and decodes the pending signal for a pair. None when absent or unparseable."""
        signal_key = f"arb:signal:{pair_tag}"
        raw_signal = await self.redis.get(signal_key)
        if not raw_signal:
            return None
        try:
            return ExecutorPayload.from_dict(json.loads(raw_signal))
        except Exception:
            logger.exception(f"failed to parse signal payload {signal_key}")
            return None

    async def _is_stale_or_duplicate_signal(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        """True when the signal is too old to trust or the pair is already being worked."""
        if payload.pricing_snapshot.is_stale(self.config.max_stale_latency_ms):
            logger.warning(
                f"stale signal rejected for {pair_tag}: latency > {self.config.max_stale_latency_ms}ms"
            )
            return True
        return await self._has_active_or_pending_position(pair_tag)

    async def _has_active_or_pending_position(self, pair_tag: str) -> bool:
        """True when this pair already has a live position or an order in flight."""
        if await self.redis.exists(f"arb:active_pos:{pair_tag}"):
            return True
        return pair_tag in self.active_positions or pair_tag in self.pending_entry_orders

    async def _validate_entry_spread(self, pair_tag: str, payload: ExecutorPayload) -> bool:
        """
        Pre-entry re-validation: re-quotes the global futures book and rejects
        the signal when the entry spread has collapsed below the floor the
        scanner itself used. Guards the sub-second window between the scanner's
        snapshot and the executor's order placement.

        Floor resolution: explicit config ``entry_revalidation_min_spread_pct``
        overrides; otherwise the payload's ``min_entry_spread_pct``. A ticker
        that cannot be read fails open — the scanner already validated the
        quote one scan ago, and refusing every entry on a ticker blip is worse.
        """
        min_spread = float(
            getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0
        )
        if min_spread <= 0:
            min_spread = float(payload.execution_directives.min_entry_spread_pct or 0.0)
        if min_spread <= 0:
            return True

        global_symbol = payload.pair_info.global_symbol
        try:
            ticker = await self.client.fetch_global_ticker(global_symbol)
        except Exception as exc:  # nosec BLE001 - fail open, see docstring
            logger.warning(
                f"[Entry Revalidation] could not read {global_symbol} ticker for {pair_tag}: "
                f"{exc}. Proceeding with the entry."
            )
            return True

        try:
            live_bid = float((ticker or {}).get("bid") or 0.0)
        except (TypeError, ValueError):
            live_bid = 0.0
        if live_bid <= 0:
            logger.info(
                f"[Entry Revalidation] no live bid for {global_symbol}; "
                f"skipping spread check on {pair_tag}"
            )
            return True

        snapshot = payload.pricing_snapshot
        if snapshot.global_best_bid_usdt <= 0 or snapshot.usdt_idr_rate <= 0:
            return True
        live_bid_idr = live_bid * snapshot.usdt_idr_rate
        buy_price_idr = payload.execution_directives.recommended_limit_buy_idr
        if buy_price_idr <= 0 or live_bid_idr <= 0:
            return True

        live_spread_pct = ((live_bid_idr - buy_price_idr) / live_bid_idr) * 100.0
        if live_spread_pct < min_spread:
            logger.info(
                f"[Entry Revalidation] rejecting {pair_tag}: live spread "
                f"{live_spread_pct:.3f}% < floor {min_spread:.3f}% "
                f"(live bid {live_bid}, snapshot bid {snapshot.global_best_bid_usdt})"
            )
            return False
        return True
