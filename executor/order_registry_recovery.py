"""order registry recovery operations for durable order ownership."""

import json
import time
from typing import Any, Dict

from executor.models import OrderRecord, RecoveryStatus
from executor.helpers import (
    ORDER_BY_CLIENT_ID_KEY,
    ORDER_BY_EXCHANGE_ID_KEY,
    ORDER_PENDING_RECOVERY_KEY,
    ORDER_RECOVERY_INDEX_KEY,
    ORPHAN_INDEX_KEY,
    order_cid_field,
    order_record_key,
    order_xid_field,
    orphan_order_key,
)
from executor.order_registry_common import logger
from executor.exchange_common import exchange_order_id_from



class OrderRegistryRecoveryMixin:
    """Focused persistence and recovery operations for order ownership."""

    @staticmethod
    def _merge_durable_order_record(
        cached: OrderRecord, durable_record: OrderRecord
    ) -> OrderRecord:
        """Merge durable facts without moving cumulative recovery watermarks back."""
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
            "exchange_order_id", "client_order_id", "venue_client_order_id",
            "pair_key", "position_id", "exchange", "market_type", "symbol",
            "side", "order_type",
        ):
            if not getattr(cached, field, None):
                setattr(cached, field, getattr(durable_record, field, None))
        for field in (
            "local_amount_spec", "global_amount_spec", "recovery_context",
            "price", "average_fill_price",
        ):
            if not getattr(cached, field, None):
                setattr(cached, field, getattr(durable_record, field, None))
        if durable_record.recovery_status == RecoveryStatus.UNVERIFIED.value:
            cached.recovery_status = RecoveryStatus.UNVERIFIED.value
        return cached

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
            records[local_id] = (
                durable_record
                if cached is None
                else self._merge_durable_order_record(cached, durable_record)
            )

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
                        "venue_client_order_id": (
                            row.get("venue_client_order_id")
                            or raw_payload.get("venue_client_order_id")
                        ),
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
                        # Amount specs are serialized into raw_payload by the
                        # registry writer; they are not standalone SQL columns.
                        # Restore them here or a restarted executor cannot safely
                        # convert spot base quantities / hedge contracts.
                        "local_amount_spec": (
                            row.get("local_amount_spec")
                            or raw_payload.get("local_amount_spec")
                        ),
                        "global_amount_spec": (
                            row.get("global_amount_spec")
                            or raw_payload.get("global_amount_spec")
                        ),
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
        await self.redis.hdel(ORDER_RECOVERY_INDEX_KEY, record.local_order_id)

    # ---------------------------------------------- pending recovery / orphans

    async def mark_pending_recovery(self, local_order_id: str, reason: str) -> None:
        await self.redis.hset(ORDER_PENDING_RECOVERY_KEY, local_order_id, reason)

    async def clear_pending_recovery(self, local_order_id: str) -> None:
        await self.redis.hdel(ORDER_PENDING_RECOVERY_KEY, local_order_id)

    async def pending_recovery_ids(self) -> Dict[str, str]:
        raw = await self.redis.hgetall(ORDER_PENDING_RECOVERY_KEY)
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
        market_type: str = "spot",
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
        exchange_order_id = exchange_order_id_from(order) or str(order.get("exchange_order_id") or "")
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
            "market_type": str(market_type or "spot"),
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
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.set(orphan_order_key(orphan_id), json.dumps(record, default=str))
            pipe.hset(ORPHAN_INDEX_KEY, orphan_id, scope)
            await pipe.execute()
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
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hdel(ORPHAN_INDEX_KEY, orphan_id)
            pipe.delete(orphan_order_key(orphan_id))
            await pipe.execute()

    async def orphans(self) -> Dict[str, Dict[str, Any]]:
        raw = await self.redis.hgetall(ORPHAN_INDEX_KEY)
        result: Dict[str, Dict[str, Any]] = {}
        for key in raw or {}:
            orphan_id = key.decode() if isinstance(key, bytes) else str(key)
            payload = await self.redis.get(orphan_order_key(orphan_id))
            if not payload:
                raise RuntimeError(
                    f"orphan index references missing record {orphan_id}"
                )
            try:
                record = json.loads(payload)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"orphan record {orphan_id} is malformed"
                ) from exc
            if not isinstance(record, dict):
                raise RuntimeError(f"orphan record {orphan_id} is not an object")
            result[orphan_id] = record
        return result

    async def force_delete_order(self, target_id: str) -> Dict[str, Any]:
        """Force-delete an order or orphan across Redis cache, memory, outbox, and Supabase."""
        tid = str(target_id or "").strip()
        if not tid:
            return {"deleted_redis": 0, "deleted_db": 0, "pair_keys": [], "resolved_ids": []}

        matched_ids = {tid}
        pair_keys = set()
        records_to_delete: Dict[str, OrderRecord] = {}
        orphans_to_delete: set = set()
        deleted_redis = 0

        # 1. Inspect in-memory records
        for lid, rec in list(self.records.items()):
            candidates = {
                str(rec.local_order_id or ""),
                str(rec.client_order_id or ""),
                str(rec.exchange_order_id or ""),
                str(rec.venue_client_order_id or ""),
            }
            if tid in candidates or lid == tid:
                records_to_delete[lid] = rec
                matched_ids.update(c for c in candidates if c)
                if rec.pair_key:
                    pair_keys.add(rec.pair_key)

        # 2. Inspect Redis recovery index & direct record key
        try:
            direct = await self.load(tid)
            if direct is not None:
                records_to_delete[direct.local_order_id] = direct
                for c in (
                    direct.local_order_id,
                    direct.client_order_id,
                    direct.exchange_order_id,
                    direct.venue_client_order_id,
                ):
                    if c:
                        matched_ids.add(str(c))
                if direct.pair_key:
                    pair_keys.add(direct.pair_key)
        except Exception:
            pass

        try:
            indexed = await self.redis.hkeys(ORDER_RECOVERY_INDEX_KEY)
            for raw_id in indexed or []:
                lid = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
                rec = await self.load(lid)
                if rec is None:
                    if lid == tid:
                        deleted_redis += int(await self.redis.hdel(ORDER_RECOVERY_INDEX_KEY, lid) or 0)
                    continue
                candidates = {
                    str(rec.local_order_id or ""),
                    str(rec.client_order_id or ""),
                    str(rec.exchange_order_id or ""),
                    str(rec.venue_client_order_id or ""),
                }
                if matched_ids & candidates:
                    records_to_delete[lid] = rec
                    matched_ids.update(c for c in candidates if c)
                    if rec.pair_key:
                        pair_keys.add(rec.pair_key)
        except Exception:
            logger.exception("[OrderRegistry] error scanning recovery index for %s", tid)

        # 3. Inspect orphans in Redis
        try:
            orphan_map = await self.orphans()
            for oid, orec in orphan_map.items():
                candidates = {
                    str(oid),
                    str(orec.get("orphan_id") or ""),
                    str(orec.get("client_order_id") or ""),
                    str(orec.get("exchange_order_id") or ""),
                }
                if matched_ids & candidates:
                    orphans_to_delete.add(oid)
                    matched_ids.update(c for c in candidates if c)
                    scope = str(orec.get("scope") or "")
                    if ":" in scope:
                        pair_keys.add(scope)
        except Exception:
            # Even if orphan record payload was missing, clear direct key/index if oid matches
            try:
                if await self.redis.hexists(ORPHAN_INDEX_KEY, tid):
                    orphans_to_delete.add(tid)
            except Exception:
                pass

        # 4. Delete from Redis cache & indexes
        for lid, rec in records_to_delete.items():
            self.records.pop(lid, None)
            try:
                deleted_redis += int(await self.redis.delete(order_record_key(lid)) or 0)
                deleted_redis += int(await self.redis.hdel(ORDER_RECOVERY_INDEX_KEY, lid) or 0)
                deleted_redis += int(await self.redis.hdel(ORDER_PENDING_RECOVERY_KEY, lid) or 0)
                if rec.client_order_id and rec.exchange:
                    deleted_redis += int(
                        await self.redis.hdel(
                            ORDER_BY_CLIENT_ID_KEY,
                            order_cid_field(rec.exchange, rec.client_order_id),
                        )
                        or 0
                    )
                if rec.exchange_order_id and rec.exchange:
                    deleted_redis += int(
                        await self.redis.hdel(
                            ORDER_BY_EXCHANGE_ID_KEY,
                            order_xid_field(rec.exchange, rec.exchange_order_id),
                        )
                        or 0
                    )
            except Exception:
                logger.exception("[OrderRegistry] error deleting Redis cache for %s", lid)

        # Clean up any dangling pending_recovery / cid / xid / direct keys for tid
        try:
            deleted_redis += int(await self.redis.delete(order_record_key(tid)) or 0)
            deleted_redis += int(await self.redis.hdel(ORDER_RECOVERY_INDEX_KEY, tid) or 0)
            deleted_redis += int(await self.redis.hdel(ORDER_PENDING_RECOVERY_KEY, tid) or 0)
            for hkey in (ORDER_BY_CLIENT_ID_KEY, ORDER_BY_EXCHANGE_ID_KEY):
                all_fields = await self.redis.hgetall(hkey)
                for f_key, f_val in (all_fields or {}).items():
                    fk = f_key.decode() if isinstance(f_key, bytes) else str(f_key)
                    fv = f_val.decode() if isinstance(f_val, bytes) else str(f_val)
                    suffix = fk.split(":", 1)[-1]
                    if fv in matched_ids or suffix in matched_ids:
                        deleted_redis += int(await self.redis.hdel(hkey, fk) or 0)
        except Exception:
            pass

        for oid in orphans_to_delete:
            try:
                await self.remove_orphan(oid)
                deleted_redis += 1
            except Exception:
                pass

        # 5. Delete from Supabase & local SQLite outbox
        db_res = {"deleted_db": 0, "deleted_outbox": 0, "pair_keys": [], "resolved_ids": []}
        deleter = getattr(self.supabase, "force_delete_order_artifacts", None)
        if callable(deleter):
            db_res = await deleter(sorted(matched_ids), sorted(pair_keys))
            pair_keys.update(db_res.get("pair_keys") or [])
            matched_ids.update(db_res.get("resolved_ids") or [])

        return {
            "deleted_redis": deleted_redis,
            "deleted_db": int(db_res.get("deleted_db", 0)) + int(db_res.get("deleted_outbox", 0)),
            "pair_keys": sorted(pair_keys),
            "resolved_ids": sorted(matched_ids),
        }

    # --------------------------------------------------------- reconciliation
