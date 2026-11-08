"""Best-effort Supabase persistence for the executor.

Every write is queued and flushed by a single background task, so a database
outage can never delay a hedge, rollback, or force close. Rows are upserted on
client-generated ids, which makes retries idempotent: replaying a queued write
cannot duplicate a position, an order, or a fill.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional

from executor.supabase_helpers import _ms_to_iso
from executor.supabase_orders import SupabaseOrderMixin
from executor.supabase_positions import SupabasePositionMixin

import aiohttp

logger = logging.getLogger("executor.supabase")

_QUEUE_MAX = 512
_MAX_ATTEMPTS = 3
_BATCH_MAX = 50
_TIMEOUT = aiohttp.ClientTimeout(total=8, connect=3)
_SIDE_BY_LEG = {
    "spot_entry": "buy",
    "spot_exit": "sell",
    "futures_entry": "sell",
    "futures_exit": "buy",
    "rollback": "sell",
    "force_close": "sell",
}
# Positions are keyed deterministically by pair so a restart reuses the same row.
# The schema allows only one active position per (pair_key, is_mock); a random id
# per process would collide with the previous process's row instead of updating it.
_POSITION_NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


def _position_uuid(pair_key: str, opened_at_ms: Any = 0) -> str:
    """Deterministic position id, stable across retries and restarts.

    Including the open timestamp keeps consecutive trades on the same pair as
    separate rows instead of overwriting each other.
    """
    try:
        stamp = int(opened_at_ms or 0)
    except (TypeError, ValueError):
        stamp = 0
    return str(uuid.uuid5(_POSITION_NS, f"{pair_key}:{stamp}"))


def _ms_to_iso(value: Any) -> Optional[str]:
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc).isoformat()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result else default  # drop NaN


class SupabaseWriter(SupabasePositionMixin, SupabaseOrderMixin):
    """Queued, non-blocking writer for the nafaz core schema.

    A disabled writer is a no-op object with the same interface, so call sites
    never branch on configuration.
    """

    def __init__(self, config: Any):
        self.url = str(getattr(config, "url", "") or "").rstrip("/")
        self.key = str(getattr(config, "key", "") or "")
        self.enabled = bool(getattr(config, "enabled", False)) and bool(self.url) and bool(self.key)

        self._queue: Optional[asyncio.Queue] = None
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._dropped = 0

        # Client-generated ids keep upserts idempotent across retries and
        # restarts, and remove the need to read an id back from Supabase.
        self._position_ids: Dict[str, str] = {}
        self._position_rows: Dict[str, Dict[str, Any]] = {}
        self._order_ids: Dict[str, str] = {}
        self._order_filled: Dict[str, float] = {}

    # ---------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        self._queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._task = asyncio.create_task(self._run())
        logger.info("Supabase persistence enabled -> %s", self.url)

    async def close(self) -> None:
        if self._task is None:
            return
        if self._queue is not None:
            try:
                await asyncio.wait_for(self._queue.join(), timeout=2.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "Supabase queue not drained on shutdown (%d pending)", self._queue.qsize()
                )
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
            logger.warning("Supabase writer dropped %d writes under backpressure", self._dropped)

    # ------------------------------------------------------------------- emit

    def _emit(self, table: str, row: Dict[str, Any], on_conflict: Optional[str] = None) -> None:
        """Queues one row. Never blocks, never raises."""
        if not self.enabled or self._queue is None:
            return
        item = (table, on_conflict, row)
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._dropped += 1

    def event(
        self,
        event_type: str,
        *,
        pair_key: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        is_mock: bool = True,
        occurred_at_ms: Optional[int] = None,
        event_id: Optional[str] = None,
    ) -> None:
        """Appends to the executor event journal. `event_id` makes it idempotent."""
        stamp = int(occurred_at_ms or time.time() * 1000)
        self._emit(
            "executor_events",
            {
                "event_id": event_id or f"{event_type}:{pair_key or '-'}:{stamp}",
                "event_type": event_type,
                "pair_key": pair_key,
                "is_mock": bool(is_mock),
                "occurred_at": _ms_to_iso(stamp),
                "payload": payload or {},
            },
            on_conflict="event_id",
        )

    # --------------------------------------------------------------- positions

    # ------------------------------------------------------------------- flush

    async def _run(self) -> None:
        while True:
            try:
                batch = [await self._queue.get()]
                while len(batch) < _BATCH_MAX:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                await self._flush(batch)
                for _ in batch:
                    self._queue.task_done()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("supabase writer loop error")

    async def _flush(self, batch: list) -> None:
        grouped = self._group_batch_rows(batch)
        grouped = self._deduplicate_batch_rows(grouped)
        await self._ensure_session()
        for (table, on_conflict, _keys), rows in grouped.items():
            await self._post_batch_group(table, on_conflict, rows)

    @staticmethod
    def _group_batch_rows(batch: list) -> Dict[tuple, list]:
        grouped: Dict[tuple, list] = {}
        for table, on_conflict, row in batch:
            grouped.setdefault((table, on_conflict, frozenset(row)), []).append(row)
        return grouped

    @staticmethod
    def _deduplicate_batch_rows(grouped: Dict[tuple, list]) -> Dict[tuple, list]:
        for key, rows in list(grouped.items()):
            table, on_conflict, _keys = key
            conflict_columns = [column.strip() for column in (on_conflict or "id").split(",")]
            unique_rows = {}
            for row in rows:
                identity = tuple(str(row.get(column)) for column in conflict_columns)
                unique_rows[identity] = row
            grouped[key] = list(unique_rows.values())
        return grouped

    async def _ensure_session(self) -> None:
        if self._session is not None and not self._session.closed:
            return
        self._session = aiohttp.ClientSession(
            timeout=_TIMEOUT,
            headers={
                "apikey": self.key,
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal",
                "Accept-Encoding": "identity",
            },
        )

    async def _post_batch_group(self, table: str, on_conflict: Optional[str], rows: list) -> None:
        url = f"{self.url}/rest/v1/{table}"
        if on_conflict:
            url = f"{url}?on_conflict={on_conflict}"
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                async with self._session.post(url, json=rows) as response:
                    if response.status < 300:
                        return
                    body = (await response.text())[:300]
                    raise RuntimeError(f"HTTP {response.status}: {body}")
            except Exception as exc:
                if attempt == _MAX_ATTEMPTS:
                    logger.warning(
                        "supabase write failed table=%s rows=%d: %s", table, len(rows), exc
                    )
                    return
                await asyncio.sleep(0.5 * attempt)
