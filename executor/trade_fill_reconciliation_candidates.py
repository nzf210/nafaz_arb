"""Durable and in-memory order candidate loading for trade-fill recovery."""

import logging
from typing import Any, Dict


logger = logging.getLogger("executor.trade_fill_reconciliation")


class TradeFillCandidateMixin:
        @staticmethod
        def _identity(row: Dict[str, Any]) -> str:
            exchange = str(row.get("exchange_name") or row.get("exchange") or "").lower()
            market = str(row.get("market_type") or "").lower()
            order_id = str(row.get("exchange_order_id") or "")
            client_id = str(row.get("client_order_id") or "")
            key = client_id or order_id
            return f"{exchange}:{market}:{key}" if exchange and market and key else ""
    
        @staticmethod
        def _registry_record_candidate(record: Any) -> Dict[str, Any]:
            role_legs = {
                "ENTRY": "spot_entry" if record.market_type == "spot" else "futures_entry",
                "HEDGE": "futures_entry",
                "EXIT_SPOT": "spot_exit",
                "EXIT_FUTURES": "futures_exit",
                "EMERGENCY": "rollback",
                "ROLLBACK": "rollback",
                "FORCE_CLOSE": (
                    "futures_exit" if record.market_type == "perpetual" else "force_close"
                ),
            }
            return {
                "pair_key": record.pair_key,
                "exchange_name": record.exchange,
                "market_type": record.market_type,
                "leg": role_legs.get(record.order_role, "spot_entry"),
                "exchange_order_id": record.exchange_order_id,
                "client_order_id": record.client_order_id,
                "symbol": record.symbol,
                "side": record.side,
                "status": record.status,
                "requested_qty": record.original_quantity,
                "filled_qty": record.filled_quantity,
                "remaining_qty": record.remaining_quantity,
                "limit_price": record.price,
                "average_fill_price": record.average_fill_price,
                "reduce_only": record.reduce_only,
                "submitted_at": record.created_at,
                "completed_at": record.updated_at if record.is_terminal else None,
                "local_amount_spec": record.local_amount_spec,
                "global_amount_spec": record.global_amount_spec,
                "is_mock": record.is_mock,
            }
    
        def _merge_registry_candidates(
            self, candidates: Dict[str, Dict[str, Any]], sources: tuple
        ) -> None:
            for source in sources:
                values = source.values() if isinstance(source, dict) else ()
                for record in values:
                    try:
                        row = self._registry_record_candidate(record)
                        identity = self._identity(row)
                        if identity and not row["is_mock"]:
                            existing = candidates.get(identity, {})
                            candidates[identity] = {**existing, **row}
                    except Exception:
                        logger.exception(
                            "invalid order registry record in fill reconciliation"
                        )
    
        async def _load_durable_fill_candidates(
            self, candidates: Dict[str, Dict[str, Any]]
        ) -> None:
            reader = getattr(
                self.engine.supabase, "fetch_recent_order_fill_candidates", None
            )
            if not callable(reader):
                raise RuntimeError(
                    "LIVE fill reconciliation requires durable recent-order query support"
                )
            rows = await reader(days=90, limit=10000)
            if not isinstance(rows, list):
                raise RuntimeError("durable recent-order query returned malformed rows")
            for row in rows:
                if not isinstance(row, dict):
                    raise RuntimeError("durable recent-order query contains a malformed row")
                if row.get("is_mock") is not False:
                    continue
                identity = self._identity(row)
                if identity:
                    candidates[identity] = dict(row)
    
        async def _load_candidates(self) -> Dict[str, Dict[str, Any]]:
            candidates: Dict[str, Dict[str, Any]] = {}
            await self._load_durable_fill_candidates(candidates)
            registry_sources = (
                getattr(self.engine, "_recovered_order_records", {}),
                getattr(getattr(self.engine, "order_registry", None), "records", {}),
            )
            self._merge_registry_candidates(candidates, registry_sources)
            return candidates
