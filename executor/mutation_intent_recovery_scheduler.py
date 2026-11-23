"""Supervised, bounded scheduler for read-only mutation-intent recovery.

The scheduler never submits or cancels exchange orders. It only drives the
worker's existing reconciliation API and backs off when evidence is unavailable.
It is intentionally not wired into the V2 executor until the V3 runtime cutover
provides a verified fill reconciler and venue-bound intent producer.
"""
from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

logger = logging.getLogger("executor.mutation_intent_recovery_scheduler")


class MutationIntentRecoveryScheduler:
    """Single-flight scheduler with bounded batches and exponential backoff."""

    def __init__(
        self,
        worker: Any,
        *,
        interval_seconds: float = 15.0,
        max_interval_seconds: float = 300.0,
        batch_limit: int = 50,
    ) -> None:
        if worker is None or not callable(getattr(worker, "reconcile_batch", None)):
            raise ValueError("a mutation-intent recovery worker is required")
        try:
            interval = float(interval_seconds)
            maximum = float(max_interval_seconds)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("recovery intervals must be finite numbers") from exc
        if (
            not math.isfinite(interval) or not math.isfinite(maximum)
            or interval <= 0 or maximum < interval
        ):
            raise ValueError("recovery intervals must be finite, positive and max >= base")
        if isinstance(batch_limit, bool) or not isinstance(batch_limit, int):
            raise ValueError("batch_limit must be an integer")
        if batch_limit < 1 or batch_limit > 500:
            raise ValueError("batch_limit must be between 1 and 500")
        self.worker = worker
        self.interval_seconds = interval
        self.max_interval_seconds = maximum
        self.batch_limit = batch_limit
        self._stop = asyncio.Event()
        self._running = False

    async def run(self) -> None:
        """Run sequential batches until cancelled or stop() is requested."""
        if self._running:
            raise RuntimeError("mutation-intent recovery scheduler is already running")
        self._running = True
        self._stop.clear()
        delay = self.interval_seconds
        try:
            while not self._stop.is_set():
                try:
                    results = await self.worker.reconcile_batch(limit=self.batch_limit)
                    # Only a definitive resolution resets the cadence. An
                    # ACKNOWLEDGED active order is not terminal evidence and
                    # must be polled less aggressively.
                    if any(
                        str(item.get("outcome", "")).upper() == "RESOLVED"
                        for item in (results or [])
                    ):
                        delay = self.interval_seconds
                    else:
                        delay = min(self.max_interval_seconds, delay * 2)
                    logger.debug(
                        "mutation-intent recovery cycle complete: inspected=%d next_delay=%.1fs",
                        len(results or []), delay,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("mutation-intent recovery cycle failed")
                    delay = min(self.max_interval_seconds, delay * 2)
                await self._wait_or_stop(delay)
        finally:
            self._running = False

    async def stop(self) -> None:
        """Request a graceful stop; an active read-only batch is allowed to finish."""
        self._stop.set()

    async def _wait_or_stop(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
