"""supabase worker operations for the durable Supabase writer."""

import asyncio
import json
import os
import time
from typing import Any, Dict

import aiohttp

from executor.supabase_common import (
    _AUTHORIZATION_BEARER_PREFIX, _BATCH_MAX, _JSON_CONTENT_TYPE,
    _MAX_ATTEMPTS, _TIMEOUT, logger,
)



class SupabaseWorkerMixin:
    """Focused writer methods grouped by persistence responsibility."""

    async def _run(self) -> None:
        while True:
            try:
                if self._outbox is None:
                    await asyncio.sleep(1.0)
                    continue
                batch = await asyncio.to_thread(self._outbox.pending, _BATCH_MAX)
                if not batch:
                    await asyncio.sleep(0.25)
                    continue
                await self._flush(batch)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Supabase durable outbox worker error")
                await asyncio.sleep(1.0)

    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_TIMEOUT,
                headers={
                    "apikey": self.key,
                    "Authorization": _AUTHORIZATION_BEARER_PREFIX + self.key,
                    "Content-Type": _JSON_CONTENT_TYPE,
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                    "Accept-Encoding": "identity",
                },
            )

    @staticmethod
    def _group_outbox_batch(batch: list) -> Dict[tuple, list]:
        grouped: Dict[tuple, list] = {}
        for item_id, table, on_conflict, row in batch:
            grouped.setdefault((table, on_conflict, frozenset(row)), []).append(
                (item_id, row)
            )
        return grouped

    @staticmethod
    def _deduplicate_upsert_rows(on_conflict: str, items: list) -> list:
        conflict_cols = [column.strip() for column in (on_conflict or "id").split(",")]
        latest: Dict[tuple, Dict[str, Any]] = {}
        for _item_id, row in items:
            key = tuple(str(row.get(column)) for column in conflict_cols)
            latest[key] = row
        return list(latest.values())

    async def _deliver_outbox_group(
        self, table: str, on_conflict: str, items: list
    ) -> None:
        # PostgREST rejects duplicate conflict keys in one upsert statement.
        # Keep the newest row, but acknowledge superseded journal entries only
        # after that newest state has been accepted.
        rows = self._deduplicate_upsert_rows(on_conflict, items)
        url = f"{self.url}/rest/v1/{table}"
        if on_conflict:
            url = f"{url}?on_conflict={on_conflict}"
        last_error = "Supabase request failed"
        delivered = False
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                async with self._session.post(url, json=rows) as resp:
                    if resp.status < 300:
                        delivered = True
                        break
                    body = (await resp.text())[:300]
                    last_error = f"HTTP {resp.status}: {body}"
                    # Permanent 4xx errors remain replayable after schema or
                    # permission repairs; 408 and 429 remain retryable.
                    if 400 <= resp.status < 500 and resp.status not in (408, 429):
                        break
            except Exception as exc:
                last_error = str(exc)[:500]
            if attempt < _MAX_ATTEMPTS:
                await asyncio.sleep(0.5 * attempt)

        item_ids = [item_id for item_id, _row in items]
        if delivered:
            await asyncio.to_thread(self._outbox.ack, item_ids)
            return
        await asyncio.to_thread(self._outbox.retry, item_ids, last_error)
        logger.error(
            "Supabase delivery deferred table=%s rows=%d: %s",
            table, len(rows), last_error,
        )

    async def _flush(self, batch: list) -> None:
        grouped = self._group_outbox_batch(batch)
        await self._ensure_session()
        for (table, on_conflict, _keys), items in grouped.items():
            await self._deliver_outbox_group(table, on_conflict, items)


    # ---------------------------------------------------------- durable risk state
