"""order registry recovery operations for durable order ownership."""

import json
import time
from typing import Any, Dict

from executor.models import OrderRecord, RecoveryStatus
from executor.helpers import (
    ORDER_PENDING_RECOVERY_KEY,
    ORDER_RECOVERY_INDEX_KEY,
    ORPHAN_INDEX_KEY,
    orphan_order_key,
)
from executor.order_registry_common import logger



class OrderRegistryRecoveryMixin:
    """Focused persistence and recovery operations for order ownership."""

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
