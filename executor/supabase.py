"""Durable, asynchronous Supabase audit persistence for the executor.

Callbacks commit rows to a local SQLite journal; background mixins deliver
position, order, fill, risk, and query records without blocking hedge paths.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

import aiohttp

from executor.supabase_common import (
    _BATCH_MAX, _LEG_TO_ROLE, _MAX_ATTEMPTS, _POSITION_NS, _ROLE_CATALOGUE,
    _SIDE_BY_LEG, _TIMEOUT, _canonical_side, _ms_to_iso, _num,
    _position_uuid, _role_catalogue, logger,
)
from executor.supabase_outbox import DurableOutbox
from executor.supabase_orders import SupabaseOrderMixin
from executor.supabase_positions import SupabasePositionMixin
from executor.supabase_queries import SupabaseQueryMixin
from executor.supabase_risk import SupabaseRiskMixin
from executor.supabase_trades import SupabaseTradeMixin
from executor.supabase_worker import SupabaseWorkerMixin


class SupabaseWriter(
    SupabasePositionMixin,
    SupabaseOrderMixin,
    SupabaseTradeMixin,
    SupabaseQueryMixin,
    SupabaseWorkerMixin,
    SupabaseRiskMixin,
):
    """Journaled writer for the nafaz core schema."""

    def __init__(self, config: Any):
        self.url = str(getattr(config, "url", "") or "").rstrip("/")
        self.key = str(getattr(config, "key", "") or "")
        self.enabled = bool(getattr(config, "enabled", False)) and bool(self.url) and bool(self.key)

        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._dropped = 0
        self._outbox: Optional[DurableOutbox] = None
        if self.enabled:
            outbox_path = str(
                getattr(config, "outbox_path", "")
                or os.getenv("SUPABASE_OUTBOX_PATH", "/app/data/executor/supabase-outbox.sqlite3")
            )
            try:
                self._outbox = DurableOutbox(outbox_path)
            except Exception:
                self.enabled = False
                logger.exception("Supabase persistence disabled: durable outbox could not be opened")

        # Client-generated ids keep upserts idempotent across retries and
        # restarts, and remove the need to read an id back from Supabase.
        self._position_ids: Dict[str, str] = {}
        self._position_rows: Dict[str, Dict[str, Any]] = {}
        self._order_ids: Dict[str, str] = {}
        self._order_submitted_at: Dict[str, str] = {}
        self._order_completed_at: Dict[str, str] = {}

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        if self._outbox is None:
            logger.error("Supabase persistence is enabled but durable outbox is unavailable")
            return
        self._task = asyncio.create_task(self._run())
        logger.info("Supabase persistence enabled -> %s (durable outbox: %s)", self.url, self._outbox.path)

    async def close(self) -> None:
        if self._task is None:
            return
        deadline = asyncio.get_running_loop().time() + 2.0
        while self._outbox is not None and self._outbox.pending_count() and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.05)
        if self._outbox is not None and self._outbox.pending_count():
            logger.warning("Supabase durable outbox retained %d pending rows for restart", self._outbox.pending_count())
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        self._task = None
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None
        if self._dropped:
            logger.error("Supabase durable outbox enqueue failures: %d rows were not journaled", self._dropped)
        if self._outbox is not None:
            self._outbox.close()
            self._outbox = None

    # ------------------------------------------------------------------- emit

    def _emit(self, table: str, row: Dict[str, Any], on_conflict: Optional[str] = None) -> bool:
        """Queues one row. Never blocks, never raises; reports journal success."""
        if not self.enabled or self._outbox is None:
            return False
        try:
            self._outbox.enqueue(table, on_conflict, row)
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: durable Supabase outbox enqueue failed for table=%s", table)
            return False
        # The worker polls the durable journal. No in-memory queue is needed,
        # so process restarts and backpressure cannot discard accepted rows.
        return True

    def _emit_once(
        self, table: str, row: Dict[str, Any], state_key: str,
        on_conflict: Optional[str] = None,
    ) -> bool:
        """Atomically enqueue an immutable row once across retries/restarts."""
        if not self.enabled or self._outbox is None:
            return False
        try:
            return bool(self._outbox.enqueue_once(table, on_conflict, row, state_key))
        except Exception:
            self._dropped += 1
            logger.exception("CRITICAL: durable idempotent outbox enqueue failed for table=%s", table)
            return False
