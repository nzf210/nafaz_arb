"""Dedicated Redis order state synchronization worker."""

import asyncio
import logging
import re
import time
from typing import Any, Dict

from executor.models import OrderLifecycleState, OrderRole, PositionState

logger = logging.getLogger("executor.order_sync")


class OrderSyncWorker:
    """Single-owner background worker synchronizing Redis OrderRecord state with live lifecycle state."""

    def __init__(self, engine: Any):
        self.engine = engine
        self._venue_backoff_until: Dict[str, float] = {}
        self._last_sync_at: float = 0.0

    async def maybe_sync(self) -> Dict[str, int] | None:
        now = time.time()
        interval = max(
            0.5,
            float(getattr(getattr(self.engine, "config", None), "order_sync_interval_seconds", 2.0) or 2.0),
        )
        if now - self._last_sync_at < interval:
            return None
        self._last_sync_at = now
        return await self.sync_once()

    def record_venue_rate_limit(self, exchange: str, exc: Exception, default_delay: float = 5.0) -> float:
        venue = str(exchange or "").strip().lower()
        delay = max(1.0, float(default_delay))
        match = re.search(r"try again in (\d+)", str(exc))
        if match:
            delay = max(delay, float(match.group(1)) + 1.0)
        if venue:
            self._venue_backoff_until[venue] = max(
                self._venue_backoff_until.get(venue, 0.0),
                time.time() + delay,
            )
        return delay

    def venue_backoff_remaining(self, exchange: str) -> float:
        venue = str(exchange or "").strip().lower()
        if not venue:
            return 0.0
        return max(0.0, self._venue_backoff_until.get(venue, 0.0) - time.time())

    async def sync_once(self) -> Dict[str, int]:
        """Synchronize live pending order watermarks to Redis and retire reconciled terminal records."""
        registry = getattr(self.engine, "order_registry", None)
        if registry is None or getattr(self.engine, "redis", None) is None:
            return {"synced": 0, "retired": 0, "manual": 0}

        synced = 0
        retired = 0
        manual = 0

        # 1. Push in-memory live entry/exit progress into Redis OrderRecord
        for pair_tag, info in list((getattr(self.engine, "pending_entry_orders", {}) or {}).items()):
            if not isinstance(info, dict):
                continue
            exchange = str(info.get("local_exchange") or "").lower()
            order_id = str(info.get("order_id") or "")
            client_id = str(info.get("client_order_id") or "")
            rec = None
            if client_id and exchange:
                rec = await registry.find_by_client_id(exchange, client_id)
            if rec is None and order_id and exchange:
                rec = await registry.find_by_exchange_id(exchange, order_id)
            if rec is None:
                continue
            filled = max(float(rec.filled_quantity or 0.0), float(info.get("filled", 0.0) or 0.0))
            hedged = max(float(rec.hedged_quantity or 0.0), float(info.get("hedged_short_qty", 0.0) or 0.0))
            if abs(filled - float(rec.filled_quantity or 0.0)) > 1e-12 or abs(hedged - float(rec.hedged_quantity or 0.0)) > 1e-12:
                rec.filled_quantity = filled
                rec.hedged_quantity = hedged
                rec.remaining_quantity = max(0.0, float(rec.original_quantity or 0.0) - filled)
                await registry.register(rec)
                synced += 1

        # 2. Sweep indexed records to retire settled zero-residual terminal entries
        try:
            records = await registry._redis_active_records()
        except Exception:
            logger.exception("[OrderSync] failed reading Redis active order index")
            return {"synced": synced, "retired": retired, "manual": manual}

        for local_id, record in list(records.items()):
            if record.is_manual_required:
                manual += 1
                continue
            pair_tag = record.pair_key
            live_entry = (getattr(self.engine, "pending_entry_orders", {}) or {}).get(pair_tag)
            if (
                record.order_role == OrderRole.ENTRY.value
                and isinstance(live_entry, dict)
                and live_entry.get("order_id")
                and str(live_entry.get("order_id")) != str(record.exchange_order_id)
            ):
                unhedged = max(0.0, float(record.filled_quantity or 0.0) - float(record.hedged_quantity or 0.0))
                if record.is_venue_terminal and unhedged <= 1e-12:
                    record.lifecycle_state = OrderLifecycleState.RECONCILED.value
                    await registry.register(record)
                    await registry.retire(record)
                    await registry.clear_pending_recovery(local_id)
                    retired += 1
                    continue

            if record.is_venue_terminal and record.lifecycle_state == OrderLifecycleState.RECONCILED.value:
                await registry.retire(record)
                await registry.clear_pending_recovery(local_id)
                retired += 1

        return {"synced": synced, "retired": retired, "manual": manual}

    async def run(self) -> None:
        interval = max(
            0.5,
            float(getattr(getattr(self.engine, "config", None), "order_sync_interval_seconds", 2.0) or 2.0),
        )
        while getattr(self.engine, "_running", False):
            try:
                await self.sync_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[OrderSync] periodic sync failed")
            await asyncio.sleep(interval)
