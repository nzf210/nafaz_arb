"""Futures hedge order submission and acknowledgement recovery."""

from typing import Any, Dict

from executor.models import OrderRole
from executor.quantity import AmountSpec
from executor.helpers import build_client_order_id
from executor.entry_common import logger
from executor.entry_hedge_common import AmbiguousOrderSubmission


class EntryHedgeSubmissionMixin:
    """Owns the ambiguous futures-hedge submission boundary."""

        async def _submit_entry_hedge_order(
            self, pair_tag: str, order_info: dict, global_symbol: str,
            unhedged_delta: float, expected_short_price: float,
        ):
            hedge_coid = str(
                order_info.get("pending_hedge_client_order_id") or ""
            ) or build_client_order_id(
                pair_tag, "hedge", self._next_order_seq()
            )
            order_info["pending_hedge_client_order_id"] = hedge_coid
            recovery_context = {
                "parent_spot_exchange": str(
                    order_info.get("local_exchange", "")
                ).lower(),
                "parent_spot_order_id": str(order_info.get("order_id", "")),
                "parent_spot_client_order_id": str(
                    order_info.get("client_order_id", "")
                ),
                "hedge_requested_for_spot_fill": float(unhedged_delta),
            }
            await self._register_order_intent(
                pair_tag,
                hedge_coid,
                exchange=order_info.get("global_exchange", ""),
                market_type="perpetual",
                symbol=global_symbol,
                role=OrderRole.HEDGE.value,
                side="sell",
                order_type="market",
                price=expected_short_price,
                quantity=unhedged_delta,
                global_amount_spec=order_info.get("global_amount_spec"),
                recovery_context=recovery_context,
                required=True,
            )
            order_info["hedge_submit_started"] = True
            try:
                short_order = await self.client.open_global_short_market(
                    global_symbol,
                    unhedged_delta,
                    price=expected_short_price,
                    client_order_id=hedge_coid,
                    amount_spec=AmountSpec.from_dict(
                        order_info.get("global_amount_spec")
                    ),
                )
            except Exception as submit_error:
                rejection_check = getattr(
                    self, "_is_definitive_order_rejection", None
                )
                if callable(rejection_check) and rejection_check(submit_error):
                    order_info["hedge_submit_started"] = False
                    await self._mark_order_submission_rejected(
                        str(order_info.get("global_exchange") or ""), hedge_coid
                    )
                    raise
                lookup = getattr(
                    self.client, "fetch_global_order_by_client_id", None
                )
                recovered = None
                if callable(lookup):
                    try:
                        recovered = await lookup(
                            hedge_coid, global_symbol,
                            order_info.get("global_amount_spec"),
                        )
                    except Exception as lookup_error:
                        raise AmbiguousOrderSubmission(
                            f"futures submit failed ({type(submit_error).__name__}: "
                            f"{submit_error}); client-id lookup failed "
                            f"({type(lookup_error).__name__}: {lookup_error})"
                        ) from submit_error
                if not isinstance(recovered, dict) or not recovered.get("id"):
                    raise AmbiguousOrderSubmission(
                        f"futures submit failed ({type(submit_error).__name__}: "
                        f"{submit_error}); no acknowledged order was found for "
                        f"client id {hedge_coid}"
                    ) from submit_error
                logger.warning(
                    "[State 2] recovered futures hedge %s by client id after lost acknowledgement",
                    hedge_coid,
                )
                short_order = recovered
            if not isinstance(short_order, dict) or not short_order.get("id"):
                raise AmbiguousOrderSubmission(
                    "futures hedge acknowledgement has no exchange order id; "
                    "reconciliation required"
                )
            hedge_status = str(short_order.get("status", "")).lower()
            if hedge_status not in ("closed", "filled"):
                raise ValueError(
                    f"futures hedge is not terminal (status={hedge_status})"
                )
            try:
                actual_hedged_qty = float(short_order.get("filled") or 0.0)
            except (TypeError, ValueError):
                actual_hedged_qty = 0.0
            qty_tolerance = max(1e-12, unhedged_delta * 1e-8)
            if actual_hedged_qty + qty_tolerance < unhedged_delta:
                raise ValueError(
                    f"futures hedge fill incomplete: requested={unhedged_delta}, "
                    f"filled={actual_hedged_qty}; spot rollback will be guarded by "
                    "an authoritative futures-position check"
                )
            if actual_hedged_qty > unhedged_delta + qty_tolerance:
                raise ValueError(
                    f"futures hedge overfilled: requested={unhedged_delta}, "
                    f"filled={actual_hedged_qty}; manual reconciliation required"
                )
    
            recovery_order = dict(short_order)
            recovery_order["status"] = "open"
            hedge_journal = await self._register_order(
                pair_tag,
                hedge_coid,
                recovery_order,
                exchange=order_info.get("global_exchange", ""),
                market_type="perpetual",
                symbol=global_symbol,
                role=OrderRole.HEDGE.value,
                side="sell",
                order_type="market",
                price=expected_short_price,
                quantity=unhedged_delta,
                global_amount_spec=order_info.get("global_amount_spec"),
                recovery_context=recovery_context,
            )
            if hedge_journal is None:
                raise RuntimeError(
                    "futures hedge was accepted but its fill could not be journaled; "
                    "rollback/reconciliation is required"
                )
            return hedge_coid, recovery_context, short_order, actual_hedged_qty
