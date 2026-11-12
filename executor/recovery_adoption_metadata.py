"""Recovery metadata reconstruction helpers for adopted entry orders."""

import time
from typing import Any, Dict, Optional

from executor.models import OrderRecord


class RecoveryAdoptionMetadataMixin:
    """Rebuild persisted entry metadata without coupling the recovery flow."""

    def _recovered_entry_pair_metadata(
        self, record: OrderRecord, context: dict, pair_info, amount_spec: Any
    ) -> Optional[dict]:
        local_exchange = record.exchange or (
            getattr(pair_info, "local_exchange", "")
            if pair_info else self.config.local_exchange_name
        )
        global_exchange = (
            getattr(pair_info, "global_exchange", "")
            if pair_info else self.config.global_exchange_name
        )
        global_symbol = context.get("global_symbol") or (
            getattr(pair_info, "global_symbol", "") if pair_info else ""
        )
        base_asset = context.get("base_asset") or (
            getattr(pair_info, "base_asset", "") if pair_info else ""
        )
        quote_asset = context.get("quote_asset") or (
            getattr(pair_info, "quote_asset", "") if pair_info else ""
        )
        local_base, local_quote = self._symbol_parts(record.symbol)
        if not global_symbol:
            if not local_base:
                return None
            global_symbol = f"{local_base.upper()}USDT"
        if not base_asset:
            base_asset = local_base
        if not quote_asset:
            quote_asset = local_quote
        return {
            "global_symbol": global_symbol,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "local_exchange": local_exchange,
            "global_exchange": global_exchange,
            "local_amount_spec": (
                record.local_amount_spec
                or amount_spec
                or self._default_amount_spec_for(local_exchange, "spot")
            ),
            "global_amount_spec": record.global_amount_spec,
        }
    
    @staticmethod
    def _recovered_entry_fee_metadata(context: dict) -> dict:
        return {
            "spot_entry_fee_pct": float(context.get("spot_entry_fee_pct", 0.0) or 0.0),
            "spot_exit_fee_pct": float(context.get("spot_exit_fee_pct", 0.0) or 0.0),
            "perp_entry_fee_pct": float(context.get("perp_entry_fee_pct", 0.0) or 0.0),
            "perp_exit_fee_pct": float(context.get("perp_exit_fee_pct", 0.0) or 0.0),
            "fee_source": str(context.get("fee_source", "LEGACY_UNVERIFIED")),
            "fee_verified": bool(context.get("fee_verified", False)),
            "fee_fetched_at_ms": int(context.get("fee_fetched_at_ms", 0) or 0),
            "fee_asset_surcharge_pct": float(
                context.get("fee_asset_surcharge_pct", 0.0) or 0.0
            ),
        }
    
    @staticmethod
    def _recovered_entry_usdt_rate(context: dict) -> float:
        try:
            return float(context.get("usdt_idr_rate", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0
    
    def _rebuild_entry_order_info(
        self, record: OrderRecord, order: Dict[str, Any], amount_spec: Any
    ) -> Optional[dict]:
        """Rebuild pending-entry state from durable order and recovery context."""
        pair_tag = record.pair_key
        if not pair_tag or not record.symbol:
            return None
        context = record.recovery_context or {}
        usdt_rate = self._recovered_entry_usdt_rate(context)
        if usdt_rate <= 0:
            return None
        pair_info = getattr(self, "_pair_infos", {}).get(pair_tag)
        metadata = self._recovered_entry_pair_metadata(
            record, context, pair_info, amount_spec
        )
        if metadata is None:
            return None
        metadata.update(self._recovered_entry_fee_metadata(context))
        return {
            "order_id": record.exchange_order_id,
            "client_order_id": record.client_order_id,
            "symbol": record.symbol,
            **metadata,
            "price": record.price,
            "qty": record.original_quantity,
            "filled": record.filled_quantity,
            "hedged_short_qty": float(record.hedged_quantity or 0.0),
            "tick_size": getattr(
                self.active_positions.get(pair_tag), "local_price_tick", 1.0
            ),
            "placed_time": (
                record.created_at or int(time.time() * 1000)
            ) / 1000.0,
            "recovered_from_registry": True,
            "is_mock": record.is_mock,
            "usdt_idr_rate": usdt_rate,
            "fx_rate_timestamp_ms": context.get("fx_rate_timestamp_ms", 0),
            "global_ask_usdt": float(context.get("global_ask_usdt", 0.0) or 0.0),
            "cancel_threshold_usdt": float(
                context.get("cancel_threshold_usdt", 0.0) or 0.0
            ),
            "min_entry_spread_pct": float(
                context.get("min_entry_spread_pct", 0.0) or 0.0
            ),
            "base_target_exit_spread_pct": float(
                context.get("base_target_exit_spread_pct", 0.3) or 0.3
            ),
            "current_funding_rate": float(
                context.get("current_funding_rate", 0.0) or 0.0
            ),
            "next_funding_time_ms": int(
                context.get("next_funding_time_ms", 0) or 0
            ),
            "spot_cost_usdt": float(context.get("spot_cost_usdt", 0.0) or 0.0),
            "futures_margin_usdt": float(
                context.get("futures_margin_usdt", 0.0) or 0.0
            ),
        }
