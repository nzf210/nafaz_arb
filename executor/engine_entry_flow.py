"""Signal-to-entry orchestration facade for the arbitrage engine."""

import math
from typing import Optional

from executor.models import ExecutorPayload
from executor.engine_common import logger


class EngineEntryFlowMixin:
    """Coordinates validation, sizing, and the existing spot entry lifecycle."""

    async def _handle_signal_locked(
        self, pair_tag: str, payload: Optional[ExecutorPayload] = None
    ):
        if payload is None:
            payload = await self._load_signal_payload(pair_tag)
        if payload is None:
            return

        # IDR sizing is only valid with the live FX rate carried by this scan.
        # Never let mock-mode defaults or a static fallback turn an invalid
        # FX snapshot into a tradable order.
        quote_asset = str(payload.pair_info.quote_asset or "").upper()
        if quote_asset in ("IDR", "BIDR"):
            fx_rate = float(payload.pricing_snapshot.usdt_idr_rate or 0.0)
            if not math.isfinite(fx_rate) or fx_rate <= 0:
                logger.error(
                    f"[FX Preflight] rejecting {pair_tag}: live USDT/IDR rate is missing or invalid"
                )
                self._mark_failed(
                    pair_tag,
                    "LIVE_FX_RATE_INVALID",
                    "IDR spot entry requires a finite, positive live USDT/IDR rate.",
                    detail={"quote_asset": quote_asset, "usdt_idr_rate": fx_rate},
                )
                await self.publish_status()
                return

        # Cache the venue names so venue-halt checks work before the next
        # payload read (and during monitoring after the signal key expires).
        self._pair_infos[pair_tag] = payload.pair_info

        if await self._is_stale_or_duplicate_signal(pair_tag, payload):
            return

        if not await self._validate_entry_spread(pair_tag, payload):
            return

        directives = payload.execution_directives
        # Refresh account-specific rates before any new LIVE order. If a venue
        # lacks an authenticated fee endpoint, retain the explicitly supplied
        # scanner profile and keep its source/verification status unchanged.

        await self._refresh_fee_snapshot(pair_tag, payload, directives)
        if not await self._validate_dynamic_fee_edge(pair_tag, directives):
            return

        pair_info = payload.pair_info
        snapshot = payload.pricing_snapshot
        if not await self._validate_entry_market_rules(pair_tag, pair_info):
            return

        sizing = await self._prepare_entry_sizing(
            pair_tag, payload, pair_info, directives
        )
        if sizing is None:
            return
        buy_qty, actual_cost_usdt, buy_price_idr = sizing
        await self._submit_entry_order(
            pair_tag, payload, pair_info, snapshot, directives,
            buy_qty, actual_cost_usdt, buy_price_idr,
        )
