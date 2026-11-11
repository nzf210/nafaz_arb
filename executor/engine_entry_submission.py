"""Spot entry submission, durable intent registration, and failure handling."""

import json
import time

from executor.models import OrderRole, PositionState
from executor.helpers import build_client_order_id
from executor.engine_common import logger


class EngineEntrySubmissionMixin:
    """Submits the spot leg through the existing durable order lifecycle."""

    async def _submit_entry_order(
        self, pair_tag, payload, pair_info, snapshot, directives,
        buy_qty, actual_cost_usdt, buy_price_idr,
    ) -> None:
        try:
            logger.info(
                f"[State 1] Placing Maker Limit Buy on {pair_info.local_exchange} for {pair_info.local_symbol}: "
                f"qty={buy_qty}, price_idr={buy_price_idr}, cost_usd=${actual_cost_usdt:.2f}"
            )
            entry_seq = self._next_order_seq()
            entry_coid = build_client_order_id(pair_tag, "entry", entry_seq)
            placed_time = time.time()
            await self._register_order_intent(
                pair_tag,
                entry_coid,
                exchange=pair_info.local_exchange,
                market_type="spot",
                symbol=pair_info.local_symbol,
                role=OrderRole.ENTRY.value,
                side="buy",
                order_type="limit",
                price=buy_price_idr,
                quantity=buy_qty,
                local_amount_spec=pair_info.local_amount_spec,
                global_amount_spec=pair_info.global_amount_spec,
                recovery_context={
                    "usdt_idr_rate": snapshot.usdt_idr_rate,
                    "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                    "global_ask_usdt": snapshot.global_best_ask_usdt,
                    "global_symbol": pair_info.global_symbol,
                    "base_asset": pair_info.base_asset,
                    "quote_asset": pair_info.quote_asset,
                    "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                    "min_entry_spread_pct": directives.min_entry_spread_pct,
                    "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                    "current_funding_rate": payload.funding_metrics.current_funding_rate,
                    "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                    "spot_entry_fee_pct": directives.spot_entry_fee_pct,
                    "spot_exit_fee_pct": directives.spot_exit_fee_pct,
                    "perp_entry_fee_pct": directives.perp_entry_fee_pct,
                    "perp_exit_fee_pct": directives.perp_exit_fee_pct,
                    "fee_source": directives.fee_source,
                    "fee_verified": directives.fee_verified,
                    "fee_fetched_at_ms": directives.fee_fetched_at_ms,
                    "fee_asset_surcharge_pct": directives.fee_asset_surcharge_pct,
                    "spot_cost_usdt": actual_cost_usdt,
                    "futures_margin_usdt": actual_cost_usdt,
                },
            )
            order = await self.client.place_local_limit_buy(
                pair_info.local_symbol,
                buy_qty,
                buy_price_idr,
                client_order_id=entry_coid,
                amount_spec=pair_info.local_amount_spec,
            )
            order_id = str(order.get("id", ""))
            self._record_order_success(pair_info.local_exchange)
            self.supabase.order(
                pair_tag,
                "spot_entry",
                pair_info.local_exchange,
                "spot",
                pair_info.local_symbol,
                order,
                client_order_id=entry_coid,
                is_mock=bool(getattr(self.config, "mock_mode", False)),
                submitted_at_ms=int(placed_time * 1000),
            )
            await self._register_order(
                pair_tag,
                entry_coid,
                order,
                exchange=pair_info.local_exchange,
                market_type="spot",
                symbol=pair_info.local_symbol,
                role=OrderRole.ENTRY.value,
                side="buy",
                order_type="limit",
                price=buy_price_idr,
                quantity=buy_qty,
                local_amount_spec=pair_info.local_amount_spec,
                global_amount_spec=pair_info.global_amount_spec,
                # A fill must never be priced from a guessed FX rate or fee tier.
                # Persisting the exact snapshot makes a post-restart hedge
                # reproducible instead of requiring manual reconciliation.
                recovery_context={
                    "usdt_idr_rate": snapshot.usdt_idr_rate,
                    "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                    "global_ask_usdt": snapshot.global_best_ask_usdt,
                    "global_symbol": pair_info.global_symbol,
                    "base_asset": pair_info.base_asset,
                    "quote_asset": pair_info.quote_asset,
                    "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                    "min_entry_spread_pct": directives.min_entry_spread_pct,
                    "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                    "current_funding_rate": payload.funding_metrics.current_funding_rate,
                    "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                    "spot_entry_fee_pct": directives.spot_entry_fee_pct,
                    "spot_exit_fee_pct": directives.spot_exit_fee_pct,
                    "perp_entry_fee_pct": directives.perp_entry_fee_pct,
                    "perp_exit_fee_pct": directives.perp_exit_fee_pct,
                    "fee_source": directives.fee_source,
                    "fee_verified": directives.fee_verified,
                    "fee_fetched_at_ms": directives.fee_fetched_at_ms,
                    "fee_asset_surcharge_pct": directives.fee_asset_surcharge_pct,
                    "spot_cost_usdt": actual_cost_usdt,
                    "futures_margin_usdt": actual_cost_usdt,
                },
            )
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            self.pending_entry_orders[pair_tag] = {
                "order_id": order_id,
                "client_order_id": entry_coid,
                "symbol": pair_info.local_symbol,
                "global_symbol": pair_info.global_symbol,
                "base_asset": pair_info.base_asset,
                "local_lot_size": pair_info.local_lot_size,
                "local_amount_spec": pair_info.local_amount_spec.to_dict(),
                "global_amount_spec": pair_info.global_amount_spec.to_dict(),
                "local_exchange": pair_info.local_exchange,
                "global_exchange": pair_info.global_exchange,
                "price": buy_price_idr,
                "qty": buy_qty,
                "filled": 0.0,
                "hedged_short_qty": 0.0,
                "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
                "min_entry_spread_pct": (
                    float(self.config.entry_revalidation_min_spread_pct)
                    if float(getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0) > 0
                    else float(directives.min_entry_spread_pct or 0.0)
                ),
                "tick_size": pair_info.local_price_tick,
                "quote_asset": pair_info.quote_asset,
                "usdt_idr_rate": snapshot.usdt_idr_rate,
                "fx_rate_timestamp_ms": snapshot.timestamp_ms,
                "global_ask_usdt": snapshot.global_best_ask_usdt,
                "base_target_exit_spread_pct": directives.base_target_exit_spread_pct,
                "current_funding_rate": payload.funding_metrics.current_funding_rate,
                "next_funding_time_ms": payload.funding_metrics.next_funding_time_ms,
                "spot_entry_fee_pct": directives.spot_entry_fee_pct,
                "spot_exit_fee_pct": directives.spot_exit_fee_pct,
                "perp_entry_fee_pct": directives.perp_entry_fee_pct,
                "perp_exit_fee_pct": directives.perp_exit_fee_pct,
                "fee_source": directives.fee_source,
                "fee_verified": directives.fee_verified,
                "fee_fetched_at_ms": directives.fee_fetched_at_ms,
                "fee_asset_surcharge_pct": directives.fee_asset_surcharge_pct,
                "placed_time": placed_time,
                "spot_cost_usdt": actual_cost_usdt,
                "futures_margin_usdt": actual_cost_usdt,
            }
            await self._persist_pending_entry(pair_tag, self.pending_entry_orders[pair_tag])
        except Exception as exc:
            if (
                getattr(self.config, "mock_mode", False)
                and hasattr(self.client, "unlock_funds")
            ):
                self.client.unlock_funds(actual_cost_usdt, actual_cost_usdt)
            exchange_error = str(exc).strip() or type(exc).__name__
            normalized_error = f"{type(exc).__name__} {exchange_error}".lower()

            # A venue throttle is not a venue fault and not a business rejection:
            # pause new entries on that venue for a bounded cooldown, keep managing
            # open positions, and let the cooldown lapse on its own.
            is_rate_limited = (
                type(exc).__name__ == "IndodaxRateLimitError"
                or "http_429" in normalized_error
                or "http_503" in normalized_error
                or "rate limit" in normalized_error
                or "too many requests" in normalized_error
            )
            if is_rate_limited:
                cooldown = float(
                    getattr(exc, "retry_after", None)
                    or getattr(self.config, "venue_rate_limit_cooldown_seconds", 30.0)
                )
                self._pause_venue_for_cooldown(pair_info.local_exchange, cooldown)
                logger.warning(
                    f"[Risk Control] {pair_info.local_exchange} rate limited on entry "
                    f"placement; pausing new entries for {cooldown:.1f}s"
                )
                await self._publish_execution_failure(
                    pair_tag,
                    "VENUE_RATE_LIMITED",
                    f"Venue {pair_info.local_exchange} membatasi laju request; "
                    f"entry dijeda {cooldown:.1f}s.",
                    detail={
                        "exchange": pair_info.local_exchange,
                        "symbol": pair_info.local_symbol,
                        "cooldown_seconds": cooldown,
                        "error_type": type(exc).__name__,
                        "exchange_error": exchange_error[:500],
                    },
                )
                return

            insufficient_balance = any(
                marker in normalized_error
                for marker in (
                    "insufficientfunds",
                    "insufficient balance",
                    "insufficient funds",
                    "insufficient margin",
                    "not enough balance",
                    "not enough funds",
                    "saldo tidak cukup",
                    "saldo tidak mencukupi",
                )
            )
            min_notional = any(
                marker in normalized_error
                for marker in (
                    "min_notional",
                    "min notional",
                    "minimum notional",
                    "notional too small",
                    "cost.min",
                )
            )
            invalid_symbol = any(
                marker in normalized_error
                for marker in (
                    "invalid_symbol",
                    "invalid symbol",
                    "unknown symbol",
                    "market is not available",
                    "symbol not found",
                )
            )
            if insufficient_balance:
                reason = "INSUFFICIENT_BALANCE"
            elif min_notional:
                reason = "MIN_NOTIONAL"
            elif invalid_symbol:
                reason = "INVALID_SYMBOL"
            else:
                reason = "SPOT_ORDER_OPEN_FAILED"

            # Business rejections reject this opportunity, but do not count as
            # exchange-health failures. Transport/API/internal errors still count.
            failure_category = (
                reason if reason in ("INSUFFICIENT_BALANCE", "MIN_NOTIONAL", "INVALID_SYMBOL")
                else "entry order placement failed"
            )
            self._record_order_failure(pair_info.local_exchange, failure_category)
            logger.exception(f"[State 1] failed to place limit buy for {pair_tag}")
            await self._publish_execution_failure(
                pair_tag,
                reason,
                f"Exchange menolak order Spot: {exchange_error[:500]}",
                detail={
                    "exchange": pair_info.local_exchange,
                    "symbol": pair_info.local_symbol,
                    "error_type": type(exc).__name__,
                    "exchange_error": exchange_error[:1000],
                },
            )
