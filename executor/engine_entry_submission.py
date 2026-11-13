"""Spot entry submission, durable intent registration, and failure handling."""

import json
import math
import time

from executor.models import OrderRole, PositionState
from executor.helpers import build_client_order_id
from executor.engine_common import logger


class EngineEntrySubmissionMixin:
    """Submits the spot leg through the existing durable order lifecycle."""

    @staticmethod
    def _local_order_limit_price(pair_info, snapshot, buy_price_idr: float) -> float:
        """Convert the IDR-normalized strategy price to the venue quote unit."""
        quote_asset = str(getattr(pair_info, "quote_asset", "") or "").strip().upper()
        price_idr = float(buy_price_idr)
        if not math.isfinite(price_idr) or price_idr <= 0:
            raise ValueError("local spot limit price must be finite and positive")
        if quote_asset in ("IDR", "BIDR"):
            return price_idr
        if quote_asset in ("USDT", "USDC"):
            usdt_idr = float(getattr(snapshot, "usdt_idr_rate", 0.0) or 0.0)
            if not math.isfinite(usdt_idr) or usdt_idr <= 0:
                raise ValueError(
                    f"{quote_asset} spot order requires a finite positive USDT/IDR rate"
                )
            return price_idr / usdt_idr
        raise ValueError(
            f"unsupported local spot quote asset for IDR-normalized pricing: {quote_asset or 'unknown'}"
        )

    @staticmethod
    def _entry_recovery_context(payload, pair_info, snapshot, directives, actual_cost):
        return {
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
            "adaptive_max_buy_price_idr": float(
                getattr(directives, "adaptive_max_buy_price_idr", 0.0) or 0.0
            ),
            "spot_cost_usdt": actual_cost,
            "futures_margin_usdt": actual_cost,
        }

    def _pending_entry_record(
        self, pair_info, snapshot, directives, payload, buy_price_idr,
        buy_qty, actual_cost_usdt, placed_time,
    ) -> dict:
        configured_spread = float(
            getattr(self.config, "entry_revalidation_min_spread_pct", 0.0) or 0.0
        )
        min_spread = (
            configured_spread
            if configured_spread > 0
            else float(directives.min_entry_spread_pct or 0.0)
        )
        return {
            "symbol": pair_info.local_symbol,
            "global_symbol": pair_info.global_symbol,
            "base_asset": pair_info.base_asset,
            "local_lot_size": pair_info.local_lot_size,
            "local_amount_spec": pair_info.local_amount_spec.to_dict(),
            "global_amount_spec": pair_info.global_amount_spec.to_dict(),
            "local_exchange": pair_info.local_exchange,
            "global_exchange": pair_info.global_exchange,
            "price": buy_price_idr,
            "adaptive_max_buy_price_idr": float(
                getattr(directives, "adaptive_max_buy_price_idr", 0.0) or 0.0
            ),
            "qty": buy_qty,
            "filled": 0.0,
            "hedged_short_qty": 0.0,
            "cancel_threshold_usdt": directives.cancel_if_global_bid_drops_below_usdt,
            "min_entry_spread_pct": min_spread,
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

    @staticmethod
    def _entry_error_text(exc):
        exchange_error = str(exc).strip() or type(exc).__name__
        return exchange_error, f"{type(exc).__name__} {exchange_error}".lower()

    @staticmethod
    def _is_entry_rate_limited(exc, normalized_error: str) -> bool:
        return (
            type(exc).__name__ == "IndodaxRateLimitError"
            or "http_429" in normalized_error
            or "http_503" in normalized_error
            or "rate limit" in normalized_error
            or "too many requests" in normalized_error
        )

    @staticmethod
    def _entry_failure_reason(normalized_error: str) -> str:
        markers = {
            "INSUFFICIENT_BALANCE": (
                "insufficientfunds", "insufficient balance", "insufficient funds",
                "insufficient margin", "not enough balance", "not enough funds",
                "saldo tidak cukup", "saldo tidak mencukupi",
            ),
            "MIN_NOTIONAL": (
                "min_notional", "min notional", "minimum notional",
                "notional too small", "cost.min",
            ),
            "INVALID_SYMBOL": (
                "invalid_symbol", "invalid symbol", "unknown symbol",
                "market is not available", "symbol not found",
            ),
        }
        for reason, values in markers.items():
            if any(marker in normalized_error for marker in values):
                return reason
        return "SPOT_ORDER_OPEN_FAILED"

    async def _handle_entry_submission_failure(
        self, pair_tag, pair_info, actual_cost_usdt, exc
    ) -> None:
        if (
            getattr(self.config, "mock_mode", False)
            and hasattr(self.client, "unlock_funds")
        ):
            self.client.unlock_funds(actual_cost_usdt, actual_cost_usdt)
        exchange_error, normalized_error = self._entry_error_text(exc)
        if self._is_entry_rate_limited(exc, normalized_error):
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

        reason = self._entry_failure_reason(normalized_error)
        failure_category = (
            reason if reason in (
                "INSUFFICIENT_BALANCE", "MIN_NOTIONAL", "INVALID_SYMBOL"
            ) else "entry order placement failed"
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

    @staticmethod
    def _is_definitive_order_rejection(exc) -> bool:
        """Recognize explicit venue rejections; transport failures stay ambiguous."""
        error_name = type(exc).__name__.lower()
        error_text = str(exc).lower()
        definitive_types = {
            "insufficientfunds", "invalidorder", "badrequest",
            "authenticationerror", "permissiondenied", "invalidsymbol",
            "marketclosed", "argumentsrequired",
        }
        definitive_messages = (
            "insufficient funds", "insufficient balance", "insufficientfunds",
            "min_notional", "minimum notional", "invalid symbol",
            "unknown symbol", "market is not available",
            "does not have trading permission", '"code":"50123"',
        )
        return error_name in definitive_types or any(
            marker in error_text for marker in definitive_messages
        )

    async def _submit_entry_order(
        self, pair_tag, payload, pair_info, snapshot, directives,
        buy_qty, actual_cost_usdt, buy_price_idr,
    ) -> None:
        submission_started = False
        entry_coid = ""
        capacity_reservation_id = ""
        capacity_account_scope = ""
        try:
            logger.info(
                f"[State 1] Placing Maker Limit Buy on {pair_info.local_exchange} for {pair_info.local_symbol}: "
                f"qty={buy_qty}, price_idr={buy_price_idr}, cost_usd=${actual_cost_usdt:.2f}"
            )
            entry_seq = self._next_order_seq()
            entry_coid = build_client_order_id(pair_tag, "entry", entry_seq)
            placed_time = time.time()
            exchange_order_price = self._local_order_limit_price(
                pair_info, snapshot, buy_price_idr
            )
            recovery_context = self._entry_recovery_context(
                payload, pair_info, snapshot, directives, actual_cost_usdt
            )
            recovery_context["strategy_price_idr"] = buy_price_idr
            recovery_context["exchange_order_price"] = exchange_order_price
            recovery_context["exchange_quote_asset"] = str(
                pair_info.quote_asset or ""
            ).upper()
            if not bool(getattr(self.config, "mock_mode", False)):
                try:
                    capacity_account_scope = self._account_scope_for_exchange(
                        pair_info.local_exchange
                    )
                    capacity_reservation_id = await self._reserve_spot_capacity(
                        pair_tag, pair_info.local_exchange, entry_coid, actual_cost_usdt * 2.0
                    )
                except Exception as reserve_error:
                    # The reservation RPC may commit before its response is lost.
                    # No venue mutation has started, so release by logical order id.
                    try:
                        if capacity_account_scope and entry_coid:
                            await self._release_spot_capacity_reservation_for_order(
                                pair_info.local_exchange, entry_coid
                            )
                        else:
                            await self._release_spot_capacity_reservation(
                                pair_tag, pair_info.local_exchange
                            )
                    except Exception:
                        logger.exception(
                            "[State 1] failed to release uncertain pre-submit capacity reservation for %s",
                            pair_tag,
                        )
                    self._mark_failed(
                        pair_tag,
                        "SPOT_CAPACITY_RESERVATION_FAILED",
                        "Live entry blocked because atomic Postgres Spot slot/capital reservation failed.",
                        detail={"exchange": pair_info.local_exchange, "error": str(reserve_error)},
                    )
                    await self.publish_status()
                    return
                recovery_context["capacity_reservation_id"] = capacity_reservation_id
                recovery_context["capacity_account_scope"] = capacity_account_scope
                recovery_context["pair_capital_reservation_usdt"] = actual_cost_usdt * 2.0
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
                recovery_context=recovery_context,
            )
            submission_started = True
            try:
                order = await self.client.place_local_limit_buy(
                    pair_info.local_symbol,
                    buy_qty,
                    exchange_order_price,
                    client_order_id=entry_coid,
                    amount_spec=pair_info.local_amount_spec,
                )
            except Exception as submit_error:
                if self._is_definitive_order_rejection(submit_error):
                    submission_started = False
                    raise
                lookup = getattr(
                    self.client, "fetch_local_order_by_client_id", None
                )
                order = None
                if callable(lookup):
                    try:
                        order = await lookup(
                            entry_coid, pair_info.local_symbol,
                            pair_info.local_amount_spec,
                        )
                    except Exception:
                        logger.exception(
                            "[State 1] client-id lookup failed after ambiguous spot submit %s",
                            entry_coid,
                        )
                if not isinstance(order, dict) or not order.get("id"):
                    await self._mark_order_submission_ambiguous(
                        pair_tag, pair_info.local_exchange, entry_coid,
                        f"submit error: {type(submit_error).__name__}: {submit_error}; "
                        "venue lookup did not return a verifiable order",
                    )
                    return
                logger.warning(
                    "[State 1] recovered spot order %s by client id after lost acknowledgement",
                    entry_coid,
                )

            if not isinstance(order, dict) or not order.get("id"):
                await self._mark_order_submission_ambiguous(
                    pair_tag, pair_info.local_exchange, entry_coid,
                    "venue returned an order acknowledgement without an exchange order id",
                )
                return

            order_id = str(order["id"])
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
            registered = await self._register_order(
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
                recovery_context=recovery_context,
            )
            self.states[pair_tag] = PositionState.ENTRY_PENDING
            self.pending_entry_orders[pair_tag] = {
                "order_id": order_id,
                "client_order_id": entry_coid,
                "capacity_reservation_id": capacity_reservation_id,
                **self._pending_entry_record(
                    pair_info, snapshot, directives, payload,
                    buy_price_idr, buy_qty, actual_cost_usdt, placed_time,
                ),
            }
            await self._persist_pending_entry(
                pair_tag, self.pending_entry_orders[pair_tag]
            )
            if registered is None:
                await self._mark_order_submission_ambiguous(
                    pair_tag, pair_info.local_exchange, entry_coid,
                    "venue accepted the order but exchange acknowledgement could not "
                    "be committed to the universal order registry",
                )
        except Exception as exc:
            if not submission_started and capacity_reservation_id:
                try:
                    await self._release_spot_capacity_reservation(
                        pair_tag, pair_info.local_exchange
                    )
                except Exception:
                    logger.exception(
                        "[State 1] failed to release pre-submit Spot capacity reservation for %s",
                        pair_tag,
                    )
            if submission_started:
                await self._mark_order_submission_ambiguous(
                    pair_tag, pair_info.local_exchange, entry_coid,
                    f"post-submit persistence or processing failed: "
                    f"{type(exc).__name__}: {exc}",
                )
                return
            if entry_coid and self._is_definitive_order_rejection(exc):
                await self._mark_order_submission_rejected(
                    pair_info.local_exchange, entry_coid
                )
            await self._handle_entry_submission_failure(
                pair_tag, pair_info, actual_cost_usdt, exc
            )
