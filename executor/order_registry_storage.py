"""order registry storage operations for durable order ownership."""

import json
import time
from typing import Any, Optional

from executor.models import OrderRecord, RecoveryStatus
from executor.helpers import (
    ORDER_BY_CLIENT_ID_KEY,
    ORDER_BY_EXCHANGE_ID_KEY,
    ORDER_RECOVERY_INDEX_KEY,
    order_cid_field,
    order_record_key,
    order_xid_field,
)
from executor.order_registry_common import logger



class OrderRegistryStorageMixin:
    """Focused persistence and recovery operations for order ownership."""

    async def fetch_open_orders(
        self, client: Any, symbol: Optional[str] = None
    ) -> tuple[Optional[list], Optional[str]]:
        """Reads resting orders for a symbol from the exchange client.

        Returns ``(orders, None)`` on success, ``(None, error)`` on failure. The
        difference matters: an empty list means "verified flat", while an error
        means "unknown" and must never be treated as "nothing is resting".
        """
        fn = getattr(client, "fetch_open_orders", None)
        if fn is None:
            return None, "client does not implement fetch_open_orders"
        try:
            raw = await fn(symbol) if symbol else await fn()
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"
        if raw is None:
            return None, "fetch_open_orders returned None"
        if not isinstance(raw, list):
            return None, f"fetch_open_orders returned {type(raw).__name__}"
        orders = []
        for item in raw:
            if isinstance(item, dict):
                orders.append(item)
            else:
                return None, f"invalid order payload {item!r}"
        return orders, None

    # ----------------------------------------------------------------- upsert

    async def register(self, record: OrderRecord) -> OrderRecord:
        """Persists one order record. Redis first (fast, restart-surviving),
        then the durable journal. Returns the record so callers can keep using
        the canonical local id."""
        if not record.local_order_id:
            record.local_order_id = OrderRecord.local_id_for(
                record.exchange, record.client_order_id, record.exchange_order_id
            )
        record.updated_at = int(time.time() * 1000)
        if record.remaining_quantity is None:
            record.remaining_quantity = max(
                0.0, record.original_quantity - record.filled_quantity
            )
        await self._merge_existing(record)
        if record.recovery_status == RecoveryStatus.MANAGED.value:
            record.recovery_status = RecoveryStatus.ADOPTED.value
        previous = self.records.get(record.local_order_id)
        try:
            await self._persist(record)
        except Exception:
            if previous is None:
                self.records.pop(record.local_order_id, None)
            else:
                self.records[record.local_order_id] = previous
            raise
        self.records[record.local_order_id] = record
        return record

    async def _merge_existing(self, record: OrderRecord) -> None:
        """Keeps monotonic facts when a caller re-registers the same order.

        A restart path may re-submit a record whose local copy is stale: the
        hedge watermark and the terminal status must never move backwards, or the
        bot would hedge an already-hedged fill or reopen a finished order.
        """
        existing = self.records.get(record.local_order_id)
        if existing is None:
            existing = await self._load_cached(record.local_order_id)
        if existing is None:
            return
        # Preserve the exact venue-side alias when a later lifecycle snapshot
        # omits it (Tokocrypto's rewritten ID is not reversible).
        if not record.venue_client_order_id:
            record.venue_client_order_id = existing.venue_client_order_id
        record.hedged_quantity = max(
            float(record.hedged_quantity or 0.0), float(existing.hedged_quantity or 0.0)
        )
        record.filled_quantity = max(
            float(record.filled_quantity or 0.0), float(existing.filled_quantity or 0.0)
        )
        record.remaining_quantity = max(
            0.0, float(record.original_quantity or 0.0) - float(record.filled_quantity or 0.0)
        )
        if existing.is_terminal and not record.is_terminal:
            record.status = existing.status
        if (
            existing.recovery_status == RecoveryStatus.UNVERIFIED.value
            and record.recovery_status == RecoveryStatus.MANAGED.value
        ):
            record.recovery_status = existing.recovery_status

    async def _load_cached(self, local_order_id: str) -> Optional[OrderRecord]:
        if not local_order_id:
            return None
        try:
            raw = await self.redis.get(order_record_key(local_order_id))
        except Exception:
            durable_reader = getattr(self.supabase, "fetch_open_order_records", None)
            if callable(durable_reader):
                durable_records = await self._durable_active_records()
                return durable_records.get(local_order_id)
            if callable(getattr(self.supabase, "order_record", None)):
                # Compatibility for writer-only durable adapters. Production
                # adapters expose the reader and therefore merge before writes.
                logger.warning(
                    "[OrderRegistry] Redis unavailable and durable reader absent; "
                    "relying on the durable writer for order %s",
                    local_order_id,
                )
                return None
            raise
        if not raw:
            return None
        try:
            return OrderRecord.from_dict(json.loads(raw))
        except Exception as exc:
            raise RuntimeError(
                f"order registry record {local_order_id} is malformed"
            ) from exc

    async def _persist(self, record: OrderRecord) -> None:
        payload = json.dumps(record.to_dict())
        redis_ok = False
        try:
            pipeline_factory = getattr(self.redis, "pipeline", None)
            if callable(pipeline_factory):
                async with pipeline_factory(transaction=True) as pipe:
                    pipe.set(order_record_key(record.local_order_id), payload)
                    pipe.hset(ORDER_RECOVERY_INDEX_KEY, record.local_order_id, "1")
                    if record.exchange_order_id:
                        pipe.hset(
                            ORDER_BY_EXCHANGE_ID_KEY,
                            order_xid_field(record.exchange, record.exchange_order_id),
                            record.local_order_id,
                        )
                    if record.client_order_id:
                        pipe.hset(
                            ORDER_BY_CLIENT_ID_KEY,
                            order_cid_field(record.exchange, record.client_order_id),
                            record.local_order_id,
                        )
                    await pipe.execute()
            else:
                # Minimal Redis-compatible adapters used in tests/tools may not
                # expose transactions. Production Redis always uses the atomic path.
                await self.redis.set(order_record_key(record.local_order_id), payload)
                await self.redis.hset(
                    ORDER_RECOVERY_INDEX_KEY, record.local_order_id, "1"
                )
                if record.exchange_order_id:
                    await self.redis.hset(
                        ORDER_BY_EXCHANGE_ID_KEY,
                        order_xid_field(record.exchange, record.exchange_order_id),
                        record.local_order_id,
                    )
                if record.client_order_id:
                    await self.redis.hset(
                        ORDER_BY_CLIENT_ID_KEY,
                        order_cid_field(record.exchange, record.client_order_id),
                        record.local_order_id,
                    )
            redis_ok = True
        except Exception:
            logger.exception(
                "[OrderRegistry] atomic Redis persist failed for %s; trying durable storage",
                record.local_order_id,
            )
        writer = getattr(self.supabase, "order_record", None)
        durable_enabled = getattr(self.supabase, "enabled", None) is not False
        durable_ok = False
        if callable(writer) and durable_enabled:
            write_result = writer(
                {
                    "local_order_id": record.local_order_id,
                    "pair_key": record.pair_key or None,
                    "exchange_name": record.exchange,
                    "market_type": record.market_type,
                    "order_role": record.order_role,
                    "client_order_id": record.client_order_id or None,
                    "venue_client_order_id": record.venue_client_order_id or None,
                    "exchange_order_id": record.exchange_order_id or None,
                    "position_key": (
                        f"position:{record.pair_key}" if record.pair_key else None
                    ),
                    "local_amount_spec": record.local_amount_spec,
                    "global_amount_spec": record.global_amount_spec,
                    "recovery_context": record.recovery_context,
                    "hedged_quantity": record.hedged_quantity,
                    "symbol": record.symbol,
                    "side": record.side,
                    "order_type": record.order_type,
                    "price": record.price,
                    "original_quantity": record.original_quantity,
                    "filled_quantity": record.filled_quantity,
                    "remaining_quantity": record.remaining_quantity,
                    "average_fill_price": record.average_fill_price or None,
                    "status": record.status,
                    "reduce_only": record.reduce_only,
                    "recovery_status": record.recovery_status,
                    "is_mock": record.is_mock,
                    "created_at": record.created_at,
                }
            )
            durable_ok = write_result is not False
        # When Supabase is enabled it is the durable system of record, not
        # an optional mirror. Redis may be flushed, evicted, or rebuilt; a
        # successful Redis write must never hide a failed local SQLite journal.
        if callable(writer) and durable_enabled and not durable_ok:
            raise RuntimeError(
                f"order {record.local_order_id} was cached in Redis but could not "
                "be committed to the durable Supabase outbox"
            )
        if not redis_ok and not durable_ok:
            raise RuntimeError(
                f"order {record.local_order_id} could not be persisted to Redis or durable storage"
            )

    # ----------------------------------------------------------------- lookup

    async def find_by_exchange_id(
        self, exchange: str, exchange_order_id: str
    ) -> Optional[OrderRecord]:
        return await self._lookup(ORDER_BY_EXCHANGE_ID_KEY, order_xid_field(exchange, exchange_order_id))

    async def find_by_client_id(
        self, exchange: str, client_order_id: str
    ) -> Optional[OrderRecord]:
        return await self._lookup(ORDER_BY_CLIENT_ID_KEY, order_cid_field(exchange, client_order_id))

    async def _lookup(self, index_key: str, field: str) -> Optional[OrderRecord]:
        if not field.split(":", 1)[-1]:
            return None  # empty id: never a match
        try:
            local_id = await self.redis.hget(index_key, field)
        except Exception:
            logger.exception("[OrderRegistry] Redis lookup failed for %s; consulting durable journal", field)
            reader = getattr(self.supabase, "fetch_open_order_records", None)
            if not callable(reader):
                raise RuntimeError(
                    f"order registry lookup is inconclusive for {field}: Redis unavailable "
                    "and durable reader is not configured"
                )
            durable_records = await self._durable_active_records()
            for record in durable_records.values():
                if index_key == ORDER_BY_CLIENT_ID_KEY:
                    matches_field = order_cid_field(record.exchange, record.client_order_id) == field
                elif index_key == ORDER_BY_EXCHANGE_ID_KEY:
                    matches_field = order_xid_field(record.exchange, record.exchange_order_id) == field
                else:
                    matches_field = record.local_order_id == field
                if matches_field:
                    self.records[record.local_order_id] = record
                    return record
            # The durable reader only returns active rows. A terminal
            # record may still exist in the venue/client-id index, so an empty
            # active result cannot prove that the id was never used.
            raise RuntimeError(
                f"order registry lookup is inconclusive for {field}: Redis index is unavailable "
                "and the durable active journal has no matching record"
            )
        if not local_id:
            if isinstance(local_id, bytes):
                local_id = local_id.decode()
            return None
        if isinstance(local_id, bytes):
            local_id = local_id.decode()
        return await self.load(str(local_id))

    async def load(self, local_order_id: str) -> Optional[OrderRecord]:
        cached = self.records.get(local_order_id)
        if cached is not None:
            return cached
        try:
            raw = await self.redis.get(order_record_key(local_order_id))
        except Exception:
            logger.exception("[OrderRegistry] cannot read record %s", local_order_id)
            return None
        if not raw:
            return None
        try:
            record = OrderRecord.from_dict(json.loads(raw))
        except Exception:
            logger.exception("[OrderRegistry] malformed record %s", local_order_id)
            return None
        self.records[local_order_id] = record
        return record
