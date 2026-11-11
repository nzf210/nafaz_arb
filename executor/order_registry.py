"""Universal order registry: the single place that knows which orders the bot owns.

Before this module an order's identity lived only in ``pending_entry_orders`` /
``pending_exit_orders`` (in-process dicts, one Redis key for entries) while
futures hedges, exits, rollbacks and force closes were written to Supabase and
then forgotten. A restart therefore lost the link between an exchange order and
its pair, position and lifecycle.

The registry persists one record per order in Redis *and* Supabase, keyed by a
stable local id derived from the client order id, and keeps two lookup indexes
(exchange order id -> local id, client order id -> local id) so an order
discovered directly on the venue can still be matched to the position it
belongs to.

Redis is a cache: losing it degrades matching to the synthetic
``pair_key:exchange_order_id`` fallback in ``rebuild_link_candidates`` rather
than losing orders, because Supabase keeps the durable registry.
"""

import json
import logging
import time
from typing import Any, Dict, Optional

from executor.helpers import (
    ORDER_BY_CLIENT_ID_KEY,
    ORDER_BY_EXCHANGE_ID_KEY,
    ORDER_PENDING_RECOVERY_KEY,
    ORDER_RECOVERY_INDEX_KEY,
    ORPHAN_INDEX_KEY,
    orphan_order_key,
    order_cid_field,
    order_record_key,
    order_xid_field,
)
from executor.models import OrderRecord, OrderRole, RecoveryStatus

logger = logging.getLogger("executor.order_registry")

# Roles whose quantity reduces an existing position. These must be adopted even
# when the matching position is gone, because the alternative is unmanaged
# exposure that no exit path knows about.
_EXIT_ROLES = (
    OrderRole.EXIT_SPOT.value,
    OrderRole.EXIT_FUTURES.value,
    OrderRole.EMERGENCY.value,
    OrderRole.FORCE_CLOSE.value,
    OrderRole.ROLLBACK.value,
)

class UniversalOrderRegistry:
    """Durable, idempotent registry of every order the executor submits."""

    def __init__(self, redis: Any, supabase: Any = None):
        self.redis = redis
        self.supabase = supabase
        # Hot cache of adopted records, so the startup pass does not re-read Redis.
        self.records: Dict[str, OrderRecord] = {}

    # ------------------------------------------------------------ venue truth

    async def fetch_open_orders(self, client: Any, symbol: Optional[str] = None) -> list:
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
        self.records[record.local_order_id] = record
        await self._persist(record)
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
            return None
        if not raw:
            return None
        try:
            return OrderRecord.from_dict(json.loads(raw))
        except Exception:
            return None

    async def _persist(self, record: OrderRecord) -> None:
        payload = json.dumps(record.to_dict())
        redis_ok = False
        try:
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
                "[OrderRegistry] Redis persist failed for %s; trying durable storage",
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
            logger.exception("[OrderRegistry] lookup failed for %s", field)
            return None
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

    async def active_records(self) -> Dict[str, OrderRecord]:
        """Merge the Redis cache with the durable registry on every recovery.

        Redis may be only partially populated after a crash, eviction or failed
        write. Treating any non-empty cache as complete silently loses orders.
        The durable rows are therefore unioned by local id and monotonic fill
        facts are merged before the cache is re-seeded.
        """
        redis_error = None
        try:
            records = await self._redis_active_records()
        except Exception as exc:
            logger.exception("[OrderRegistry] Redis recovery index is unavailable")
            records = {}
            redis_error = exc
        durable = await self._durable_active_records()
        if redis_error is not None and not bool(getattr(self.supabase, "enabled", False)):
            raise RuntimeError(
                "Redis order index is unavailable and no enabled durable registry can replace it"
            ) from redis_error
        for local_id, durable_record in durable.items():
            cached = records.get(local_id)
            if cached is None:
                records[local_id] = durable_record
                continue

            # Preserve the freshest available facts without ever moving a
            # cumulative quantity or hedge watermark backwards.
            cached.filled_quantity = max(
                float(cached.filled_quantity or 0.0),
                float(durable_record.filled_quantity or 0.0),
            )
            cached.hedged_quantity = max(
                float(cached.hedged_quantity or 0.0),
                float(durable_record.hedged_quantity or 0.0),
            )
            cached.original_quantity = max(
                float(cached.original_quantity or 0.0),
                float(durable_record.original_quantity or 0.0),
            )
            cached.remaining_quantity = max(
                0.0, cached.original_quantity - cached.filled_quantity
            )
            for field in (
                "exchange_order_id", "client_order_id", "pair_key", "position_id",
                "exchange", "market_type", "symbol", "side", "order_type",
            ):
                if not getattr(cached, field, None):
                    setattr(cached, field, getattr(durable_record, field, None))
            if not cached.local_amount_spec:
                cached.local_amount_spec = durable_record.local_amount_spec
            if not cached.global_amount_spec:
                cached.global_amount_spec = durable_record.global_amount_spec
            if not cached.recovery_context:
                cached.recovery_context = durable_record.recovery_context
            if not cached.price:
                cached.price = durable_record.price
            if not cached.average_fill_price:
                cached.average_fill_price = durable_record.average_fill_price
            if durable_record.recovery_status == RecoveryStatus.UNVERIFIED.value:
                cached.recovery_status = RecoveryStatus.UNVERIFIED.value
            records[local_id] = cached

        # Rebuild the cache and both lookup indexes for records recovered from
        # durable storage, as well as repairing incomplete Redis entries.
        for record in records.values():
            await self._persist(record)
        return records

    async def _redis_active_records(self) -> Dict[str, OrderRecord]:
        indexed = await self.redis.hkeys(ORDER_RECOVERY_INDEX_KEY)
        records: Dict[str, OrderRecord] = {}
        for raw_id in indexed or []:
            local_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            record = await self.load(local_id)
            if record is None:
                continue
            if record.is_terminal:
                await self.retire(record)
                continue
            records[local_id] = record
        return records

    async def _durable_active_records(self) -> Dict[str, OrderRecord]:
        reader = getattr(self.supabase, "fetch_open_order_records", None)
        if reader is None:
            return {}
        rows = await reader()
        if not isinstance(rows, list):
            raise RuntimeError("durable order registry returned a malformed row collection")
        records: Dict[str, OrderRecord] = {}
        for row in rows:
            try:
                raw_payload = row.get("raw_payload")
                if isinstance(raw_payload, str):
                    try:
                        raw_payload = json.loads(raw_payload)
                    except ValueError:
                        raw_payload = {}
                if not isinstance(raw_payload, dict):
                    raw_payload = {}
                record = OrderRecord.from_dict(
                    {
                        "local_order_id": row.get("local_order_id"),
                        "client_order_id": row.get("client_order_id"),
                        "exchange_order_id": row.get("exchange_order_id"),
                        "pair_key": row.get("pair_key"),
                        "position_id": row.get("position_key"),
                        "exchange": row.get("exchange_name"),
                        "market_type": row.get("market_type"),
                        "symbol": row.get("symbol"),
                        "side": row.get("side"),
                        "order_type": row.get("order_type"),
                        "order_role": row.get("order_role"),
                        "price": row.get("limit_price"),
                        "original_quantity": row.get("requested_qty"),
                        "filled_quantity": row.get("filled_qty"),
                        "remaining_quantity": row.get("remaining_qty"),
                        "average_fill_price": row.get("average_fill_price"),
                        "status": row.get("status"),
                        "reduce_only": row.get("reduce_only"),
                        "is_mock": row.get("is_mock"),
                        "recovery_status": row.get("recovery_status"),
                        "local_amount_spec": row.get("local_amount_spec"),
                        "global_amount_spec": row.get("global_amount_spec"),
                        "recovery_context": raw_payload.get("recovery_context"),
                        "hedged_quantity": raw_payload.get("hedged_quantity", 0.0),
                    }
                )
            except Exception as exc:
                logger.exception("[OrderRegistry] malformed durable row %r", row)
                raise RuntimeError(
                    f"durable order registry contains an unreadable row: {row.get('local_order_id')}"
                ) from exc
            if record.is_terminal:
                continue
            # Cache/index re-seeding happens after Redis and durable rows
            # have been merged in active_records(); never overwrite a richer
            # cache record with this potentially older durable snapshot here.
            records[record.local_order_id] = record
        if records:
            logger.warning(
                "[OrderRegistry] restored %d order(s) from the durable registry "
                "(local cache was empty)", len(records),
            )
        return records

    async def retire(self, record: OrderRecord) -> None:
        """Removes an order from the recovery index once it is terminal.

        The record itself is kept: it is the lifecycle history and the audit
        trail for anything that filled.
        """
        try:
            await self.redis.hdel(ORDER_RECOVERY_INDEX_KEY, record.local_order_id)
        except Exception:
            logger.exception("[OrderRegistry] cannot retire %s", record.local_order_id)

    # ---------------------------------------------- pending recovery / orphans

    async def mark_pending_recovery(self, local_order_id: str, reason: str) -> None:
        try:
            await self.redis.hset(ORDER_PENDING_RECOVERY_KEY, local_order_id, reason)
        except Exception:
            logger.exception("[OrderRegistry] cannot mark %s pending", local_order_id)

    async def clear_pending_recovery(self, local_order_id: str) -> None:
        try:
            await self.redis.hdel(ORDER_PENDING_RECOVERY_KEY, local_order_id)
        except Exception:
            logger.exception("[OrderRegistry] cannot clear %s pending", local_order_id)

    async def pending_recovery_ids(self) -> Dict[str, str]:
        try:
            raw = await self.redis.hgetall(ORDER_PENDING_RECOVERY_KEY)
        except Exception:
            logger.exception("[OrderRegistry] cannot read pending recovery state")
            return {}
        return {
            (k.decode() if isinstance(k, bytes) else str(k)): (
                v.decode() if isinstance(v, bytes) else str(v)
            )
            for k, v in (raw or {}).items()
        }

    async def register_orphan(
        self,
        order: Dict[str, Any],
        scope: str,
        reason: str,
        exchange: str = "",
        symbol: str = "",
    ) -> str:
        """Records an active venue order no bot position could be attached to.

        Orphans are never adopted, never replaced and never canceled
        automatically: only an operator may resolve one. They are stored with
        the exposure scope (venue or pair) so entry can be halted on what the
        orphan actually affects rather than on the whole bot.
        """
        # The sweep knows the venue/symbol even when the payload omits them, and
        # an orphan without that context cannot be scoped or reconciled.
        exchange = str(
            order.get("exchange") or order.get("exchange_name") or exchange or ""
        ).lower()
        exchange_order_id = str(order.get("id") or order.get("exchange_order_id") or "")
        client_order_id = str(
            order.get("clientOrderId") or order.get("client_order_id") or ""
        )
        symbol = str(order.get("symbol") or symbol or "")
        orphan_id = str(
            OrderRecord.local_id_for(exchange, client_order_id, exchange_order_id)
        )
        record = {
            "orphan_id": orphan_id,
            "scope": scope,
            "reason": reason,
            "exchange": exchange,
            "symbol": symbol,
            "exchange_order_id": exchange_order_id,
            "client_order_id": client_order_id,
            "side": str(order.get("side") or ""),
            "order_type": str(order.get("type") or order.get("order_type") or ""),
            "price": float(order.get("price") or 0.0),
            "amount": float(order.get("amount") or order.get("original_quantity") or 0.0),
            "filled": float(order.get("filled") or 0.0),
            "status": str(order.get("status") or "open"),
            "detected_at": int(time.time() * 1000),
            "raw": {k: v for k, v in order.items() if k != "info"},
        }
        try:
            await self.redis.set(orphan_order_key(orphan_id), json.dumps(record, default=str))
            await self.redis.hset(ORPHAN_INDEX_KEY, orphan_id, scope)
        except Exception:
            logger.exception("[OrderRegistry] cannot persist orphan %s", orphan_id)
        emitter = getattr(self.supabase, "event", None)
        if callable(emitter):
            emitter(
                "ORPHAN_ORDER_DETECTED",
                pair_key=None,
                is_mock=False,
                payload=record,
            )
        return orphan_id

    async def remove_orphan(self, orphan_id: str) -> None:
        """Drops a resolved orphan so it is not re-detected on the next startup."""
        try:
            await self.redis.hdel(ORPHAN_INDEX_KEY, orphan_id)
            await self.redis.delete(orphan_order_key(orphan_id))
        except Exception:
            logger.exception("[OrderRegistry] cannot remove orphan %s", orphan_id)

    async def orphans(self) -> Dict[str, Dict[str, Any]]:
        try:
            raw = await self.redis.hgetall(ORPHAN_INDEX_KEY)
        except Exception:
            logger.exception("[OrderRegistry] cannot read orphan index")
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        for key in raw or {}:
            orphan_id = key.decode() if isinstance(key, bytes) else str(key)
            try:
                payload = await self.redis.get(orphan_order_key(orphan_id))
                if payload:
                    result[orphan_id] = json.loads(payload)
            except Exception:
                logger.exception("[OrderRegistry] cannot read orphan %s", orphan_id)
        return result

    # --------------------------------------------------------- reconciliation
