"""entry hedge methods for entry execution."""

import json
import math
import time
from typing import Any, Dict

from executor.models import ActivePosition, OrderRole, PositionState
from executor.global_stop import GlobalStopError
from executor import helpers
from executor.quantity import (
    AmountSpec,
    calculate_order_size,
    canonical_hedge_target,
    validate_futures_sizing_metadata,
)
from executor.helpers import (
    HedgeSlippageExceeded,
    build_client_order_id,
    is_trading_permission_denied,
)
from executor.entry_common import logger
from executor.ownership import account_scope_id_for
from executor.risk_budget import RiskBudgetMetricsBuilder



class AmbiguousOrderSubmission(RuntimeError):
    """The venue may have accepted an order but its acknowledgement is unknown."""


class EntryHedgeMixin:
    """Focused entry lifecycle responsibilities."""

    async def _handle_entry_fills(
        self, pair_tag: str, order_info: dict, order: dict
    ) -> None:
        """Check order fill delta and trigger short hedge if threshold is met."""
        status = order.get("status", "")
        current_filled = float(order.get("filled", 0.0) or 0.0)
        previous_filled = float(order_info.get("filled", 0.0) or 0.0)
        order_info["filled"] = max(previous_filled, current_filled)
        order_info["filled_spot_amount"] = order_info["filled"]
        hedged_qty = float(order_info.get("hedged_short_qty", 0.0))
        # Fill quantity must be monotonic: an exchange snapshot that regresses
        # below a previously confirmed fill must never shrink the hedging
        # obligation. Compute the unhedged delta from the validated cumulative
        # value, not the raw snapshot.
        cumulative_spot_filled = max(previous_filled, current_filled)
        # Canonical hedge target/residual (single source of truth):
        #   target = cumulative_spot_filled × ratio
        #   residual = max(0, target − confirmed futures fills in base units)
        # hedged_short_qty stores confirmed futures fills in base-equivalent
        # units, so a non-1:1 ratio converges instead of re-hedging forever.
        _hedging_cfg = getattr(getattr(self, "config", None), "hedging", None)
        try:
            _hedge_ratio = float(
                getattr(_hedging_cfg, "futures_target_hedge_ratio", 1.0) or 1.0
            )
        except (TypeError, ValueError):
            _hedge_ratio = 1.0
        if not (_hedge_ratio > 0):
            _hedge_ratio = 1.0
        target_hedge_qty, unhedged_delta, _hedge_ratio = canonical_hedge_target(
            cumulative_spot_filled, hedged_qty, _hedge_ratio
        )

        # Extend Redis TTL on partial fill so pending order record does not expire.
        # Keep placed_time immutable so order age and status timestamps remain accurate.
        if current_filled > 0 and getattr(self, "redis", None):
            ttl = int(getattr(getattr(self, "config", None), "max_order_ttl_seconds", 120) or 120)
            if not order_info.get("placed_time"):
                order_info["placed_time"] = time.time()
            order_info["last_fill_time"] = time.time()
            key = f"arb:pending_entry:{pair_tag}"
            payload = json.dumps(order_info)
            if hasattr(self.redis, "setex"):
                await self.redis.setex(key, ttl, payload)
            else:
                await self.redis.set(key, payload, ex=ttl)

        if unhedged_delta <= 0:
            return

        try:
            usdt_rate = float(order_info.get("usdt_idr_rate", 0.0) or 0.0)
        except (TypeError, ValueError):
            usdt_rate = 0.0
        price_idr = float(order_info.get("price", 0.0) or 0.0)
        if not math.isfinite(usdt_rate) or usdt_rate <= 0:
            # A filled IDR order without its persisted live FX snapshot is
            # an unreconciled exposure. Do not fabricate USD value from a
            # hardcoded fallback or record a position on a guessed basis.
            self._mark_failed(
                pair_tag,
                "FILLED_ORDER_FX_RATE_MISSING",
                "Filled IDR order has no valid persisted live USDT/IDR rate; manual reconciliation required.",
                detail={"order_id": order_info.get("order_id"), "unhedged_qty": unhedged_delta},
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            await self.publish_status()
            return
        delta_val_usd = (unhedged_delta * price_idr) / usdt_rate

        is_complete = status in ("closed", "canceled", "cancelled", "rejected", "expired")
        # Cumulative spot fill percentage (spec §4.1): computed from confirmed
        # fills against the spot target quantity, never a single fill event.
        spot_target_qty = float(order_info.get("qty", 0.0) or 0.0)
        cumulative_filled = float(order_info.get("filled", 0.0) or 0.0)
        spot_fill_pct = (
            (cumulative_filled / spot_target_qty) * 100.0
            if spot_target_qty > 0 else 0.0
        )
        hedging = getattr(self.config, "hedging", None)
        hedging_enabled = hedging is not None and getattr(hedging, "enabled", False)
        threshold_pct = (
            float(hedging.spot_fill_trigger_pct) if hedging_enabled else 0.0
        )
        threshold_reached = hedging_enabled and spot_fill_pct >= threshold_pct
        # "wait_until_covered" policy: before the threshold activates hedging, a
        # partial fill stays unhedged regardless of its USD value. Once active,
        # a submission still waits until the delta covers a min-notional order.
        # Minimum order size for hedging is governed by the ACTUAL futures
        # venue metadata (min notional / amount spec), not by
        # min_partial_fill_usd (a legacy partial-fill knob). Using the wrong
        # floor either blocks a valid hedge (venue min < knob) or defers a
        # hedge that can never cover (venue min > knob).
        min_hedge_base = 0.0
        min_hedge_notional = float(
            order_info.get("futures_min_notional", 0.0) or 0.0
        )
        amount_spec_preview = AmountSpec.from_dict(
            order_info.get("global_amount_spec")
        )
        if amount_spec_preview is not None:
            min_hedge_base = float(
                amount_spec_preview.minimum * amount_spec_preview.base_per_unit
            )
        if min_hedge_notional <= 0:
            capabilities = getattr(
                self, "capabilities", None
            )
            if capabilities is not None and hasattr(capabilities, "get_profile"):
                prof = capabilities.get_profile(
                    str(order_info.get(
                        "global_exchange"
                    ) or ""), "futures"
                )
                if prof and getattr(prof, "min_notional_usdt", 0) > 0:
                    min_hedge_notional = float(prof.min_notional_usdt)
        # Futures reference price: must match the price used by the actual
        # sizing step (global_ask_usdt, falling back to the spot-derived
        # rate). Using a different price here than in sizing would make the
        # gate and the submission disagree around the minimum boundary.
        futures_ref_price = float(order_info.get("global_ask_usdt", 0.0) or 0.0)
        if not math.isfinite(futures_ref_price) or futures_ref_price <= 0:
            futures_ref_price = price_idr / usdt_rate if usdt_rate > 0 else 0.0
        min_hedge_base_by_notional = (
            min_hedge_notional / futures_ref_price
            if (min_hedge_notional > 0 and futures_ref_price > 0)
            else 0.0
        )
        min_hedge_qty = max(min_hedge_base, min_hedge_base_by_notional)
        hedgeable_delta = (
            unhedged_delta >= min_hedge_qty - 1e-12
            if min_hedge_qty > 0
            else delta_val_usd >= self.config.min_partial_fill_usd
        )
        # Structured hedge decision log (Phase 9 observability): every field
        # an operator needs to audit a hedge decision in one line.
        logger.info(
            "[Hedge Decision] %s: spot_order=%s cumulative_spot=%.8f ratio=%.4f "
            "target=%.8f confirmed_futures=%.8f residual=%.8f spot_fill_pct=%.2f%% "
            "threshold=%.2f%% min_hedge_qty=%.8f hedgeable=%s status=%s",
            pair_tag, order_info.get("order_id"), cumulative_spot_filled,
            _hedge_ratio, target_hedge_qty, hedged_qty, unhedged_delta,
            spot_fill_pct, threshold_pct, min_hedge_qty, hedgeable_delta, status,
        )
        if hedging_enabled:
            # A terminal order (closed/canceled/etc.) must not silently bypass
            # the activation threshold: hedging below the agreed trigger would
            # open a futures position outside policy. Route it to explicit
            # reconciliation instead.
            if is_complete and not threshold_reached:
                self._mark_failed(
                    pair_tag,
                    "TERMINAL_ORDER_BELOW_HEDGE_THRESHOLD",
                    (
                        f"Spot order reached terminal state '{status}' with cumulative fill "
                        f"{spot_fill_pct:.1f}% below the {threshold_pct:.1f}% hedge threshold; "
                        "hedge policy violated — manual reconciliation required."
                    ),
                    detail={
                        "order_id": order_info.get("order_id"),
                        "cumulative_filled": cumulative_filled,
                        "spot_target_qty": spot_target_qty,
                        "spot_fill_pct": spot_fill_pct,
                        "threshold_pct": threshold_pct,
                        "unhedged_qty": unhedged_delta,
                    },
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                await self._maybe_publish_status()
                return
            # A terminal order whose unhedged delta can never satisfy the
            # min-notional rule will never become covered; hedging it would
            # overhedge and leaving it would strand unhedged spot exposure.
            if is_complete and not hedgeable_delta:
                self._mark_failed(
                    pair_tag,
                    "TERMINAL_ORDER_BELOW_MIN_NOTIONAL",
                    (
                        f"Spot order terminal ('{status}') with unhedged delta "
                        f"{unhedged_delta:.8f} (${delta_val_usd:.2f}) below the "
                        f"futures minimum order size ({min_hedge_qty:.8f} base / "
                        f"${min_hedge_notional:.2f} notional) and hedge policy "
                        "'wait_until_covered'; manual reconciliation required."
                    ),
                    detail={
                        "order_id": order_info.get("order_id"),
                        "unhedged_qty": unhedged_delta,
                        "delta_val_usd": delta_val_usd,
                        "min_hedge_qty": min_hedge_qty,
                        "min_hedge_notional": min_hedge_notional,
                    },
                    state=PositionState.RECONCILIATION_REQUIRED,
                )
                await self._maybe_publish_status()
                return
            should_hedge = (threshold_reached and hedgeable_delta) or (
                is_complete and hedgeable_delta
            )
        else:
            should_hedge = hedgeable_delta or is_complete
        if should_hedge:
            logger.info(
                f"[State 2 Instant Short Hedge] Fill detected: delta_qty={unhedged_delta} "
                f"(${delta_val_usd:.2f}) on {pair_tag} "
                f"(cumulative fill {spot_fill_pct:.1f}% / threshold {threshold_pct:.1f}%). "
                "Opening Market Short on Global Futures!"
            )
            if getattr(self, "two_leg_coordinator", None) is not None:
                coord = self.two_leg_coordinator
                spot_leg = coord.spot_manager.legs.get(f"spot-{pair_tag}")
                if spot_leg is None:
                    spot_leg = coord.spot_manager.create_leg(
                        pair_tag,
                        venue=str(order_info.get("local_exchange") or ""),
                        symbol=str(order_info.get("symbol") or ""),
                        target_qty=float(order_info.get("qty", 0.0) or 0.0),
                    )
                coord.spot_manager.record_fill(
                    spot_leg.leg_id,
                    filled_qty=unhedged_delta,
                    price=price_idr / usdt_rate if usdt_rate > 0 else price_idr,
                )
            # Ledger the spot leg before the hedge so a hedge failure still
            # leaves the actual spot execution on record for reconciliation.
            spot_fill_price = helpers.extract_fill_price(order) or price_idr
            await self._record_spot_entry_fill(
                pair_tag, order_info, order, unhedged_delta, spot_fill_price, usdt_rate
            )
            await self._execute_entry_hedge(
                pair_tag, order_info,
                unhedged_delta, price_idr, usdt_rate,
                hedge_ratio=_hedge_ratio,
            )
            # Only clear the partial marker once the hedge actually went
            # through. A deferred hedge (wait_until_covered) keeps the pair in
            # ENTRY_PARTIAL: the unhedged exposure stays a visible obligation,
            # never silently promoted to HEDGED_ACTIVE.
            if (
                self.states.get(pair_tag) == PositionState.ENTRY_PARTIAL
                and not order_info.get("hedge_waiting_for_coverage")
            ):
                self.states[pair_tag] = PositionState.HEDGED_ACTIVE
        elif 0 < current_filled < float(order_info.get("qty", 0.0)):
            # Below the hedge threshold with the maker order still resting: the spot
            # leg is partially filled and unhedged. Surface that explicitly rather
            # than leaving the pair looking like a plain pending entry.
            if self.states.get(pair_tag) != PositionState.ENTRY_PARTIAL:
                logger.info(
                    f"[State 1 Partial Fill] {pair_tag} filled {current_filled}"
                    f"/{order_info.get('qty')} below hedge threshold (${delta_val_usd:.2f}); "
                    f"marking ENTRY_PARTIAL"
                )
                self.states[pair_tag] = PositionState.ENTRY_PARTIAL
                await self._maybe_publish_status()

    def _update_entry_hedge_markers(
        self, order_info: dict, quantity: float, hedge_order_id: str
    ) -> None:
        order_info["hedged_short_qty"] = max(
            float(order_info.get("hedged_short_qty", 0.0) or 0.0),
            quantity,
        )
        if not hedge_order_id:
            return
        hedge_fills = order_info.setdefault("hedge_fill_orders", {})
        if not isinstance(hedge_fills, dict):
            hedge_fills = {}
            order_info["hedge_fill_orders"] = hedge_fills
        hedge_fills.setdefault(str(hedge_order_id), quantity)

    async def _persist_registry_hedge_watermark(
        self, registry, pair_tag: str, order_info: dict,
        quantity: float, hedge_order_id: str,
    ) -> bool:
        exchange = str(order_info.get("local_exchange") or "").lower()
        order_id = str(order_info.get("order_id") or "")
        client_id = str(order_info.get("client_order_id") or "")
        record = None
        if exchange and order_id:
            record = await registry.find_by_exchange_id(exchange, order_id)
        if record is None and exchange and client_id:
            record = await registry.find_by_client_id(exchange, client_id)
        if record is None:
            logger.critical(
                "[Recovery] cannot locate originating spot order %s/%s "
                "to persist hedge watermark for %s",
                exchange, order_id or client_id, pair_tag,
            )
            return False
        record.hedged_quantity = max(
            float(record.hedged_quantity or 0.0), quantity
        )
        if hedge_order_id:
            context = dict(record.recovery_context or {})
            hedge_fills = context.get("hedge_fill_orders")
            if not isinstance(hedge_fills, dict):
                hedge_fills = {}
            hedge_fills.setdefault(str(hedge_order_id), quantity)
            context["hedge_fill_orders"] = hedge_fills
            record.recovery_context = context
        await registry.register(record)
        return True

    async def _retain_hedge_claim_for_recovery(
        self, pair_tag: str, exchange: str, client_order_id: str, reason: str
    ) -> bool:
        """Keep an acknowledged hedge indexed until its durable claim is resolved."""
        registry = getattr(self, "order_registry", None)
        if registry is None:
            logger.critical(
                "[Hedge Claim] cannot retain recovery record for %s: order registry missing",
                client_order_id,
            )
            return False
        try:
            record = await registry.find_by_client_id(exchange, client_order_id)
            if record is None:
                raise RuntimeError("acknowledged hedge order intent is missing from registry")
            # The venue fill is real, but the claim state is not yet durable.
            # Force the record back into the active recovery index so startup
            # recovery can fetch this exact venue order and retry mark_confirmed.
            record.status = "open"
            record.recovery_status = "UNVERIFIED"
            await registry.register(record)
            await registry.mark_pending_recovery(
                record.local_order_id, str(reason)[:1000]
            )
            return True
        except Exception:
            logger.exception(
                "[Hedge Claim] could not retain acknowledged hedge %s for recovery",
                client_order_id,
            )
            return False

    async def _persist_entry_hedge_watermark(
        self, pair_tag: str, order_info: dict, hedged_quantity: float,
        hedge_order_id: str = "",
    ) -> bool:
        """Persist cumulative hedge progress on the originating spot entry."""
        try:
            quantity = float(hedged_quantity or 0.0)
            if not math.isfinite(quantity) or quantity < 0:
                raise ValueError("invalid cumulative hedge quantity")
            self._update_entry_hedge_markers(
                order_info, quantity, hedge_order_id
            )
            registry = getattr(self, "order_registry", None)
            persisted = False
            if registry is not None:
                persisted = await self._persist_registry_hedge_watermark(
                    registry, pair_tag, order_info, quantity, hedge_order_id
                )
            persist = getattr(self, "_persist_pending_entry", None)
            if callable(persist):
                ttl = int(getattr(self.config, "max_order_ttl_seconds", 120)) if float(order_info.get("filled", 0.0) or 0.0) > 0 else None
                await persist(pair_tag, order_info, ttl=ttl)
            return persisted
        except Exception:
            # The hedge has already executed. Do not retry it here; the
            # caller keeps the position managed but blocks new entries.
            logger.exception(
                "[Recovery] failed to persist hedge watermark for %s", pair_tag
            )
            return False

    async def _submit_entry_hedge_order(
        self, pair_tag: str, order_info: dict, global_symbol: str,
        unhedged_delta: float, expected_short_price: float,
    ):
        if getattr(self, "global_stop", None) is None and not getattr(
            getattr(self, "config", None), "mock_mode", False
        ):
            raise GlobalStopError(
                "LIVE futures hedge requires initialized PostgreSQL PREPARED/SUBMITTING fencing"
            )
        # This flag means the venue create-order boundary was actually crossed,
        # not merely that an intent is being prepared. A PREPARED/STOP failure
        # must still allow the caller to unwind the already-filled spot leg.
        order_info["hedge_submit_started"] = False
        # Cross-instance claim: only one worker may hedge this (parent spot
        # order, cumulative fill) generation. The claim also yields the stable
        # deterministic client order id. In mock mode (no PostgreSQL) the
        # claim is skipped and the legacy id path is used.
        claim = None
        claims_service = getattr(self, "hedge_claims", None)
        if claims_service is not None and claims_service.enabled:
            _cfg0 = getattr(self, "config", None)
            _hed0 = getattr(_cfg0, "hedging", None)
            try:
                _ratio0 = float(getattr(_hed0, "futures_target_hedge_ratio", 1.0) or 1.0)
            except (TypeError, ValueError):
                _ratio0 = 1.0
            _target0, _residual0, _ratio0 = canonical_hedge_target(
                float(order_info.get("filled", 0.0) or 0.0),
                float(order_info.get("hedged_short_qty", 0.0) or 0.0),
                _ratio0,
            )
            claim = await claims_service.claim_generation(
                parent_spot_exchange=str(order_info.get("local_exchange", "")),
                parent_spot_order_id=str(order_info.get("order_id", "")),
                parent_spot_client_order_id=str(order_info.get("client_order_id", "")),
                cumulative_spot_filled=float(order_info.get("filled", 0.0) or 0.0),
                target_hedge_qty=_target0,
                residual_qty=unhedged_delta,
                hedge_ratio=_ratio0,
                futures_exchange=str(order_info.get("global_exchange", "")),
                futures_symbol=global_symbol,
                claimed_by=f"executor:{id(self):x}",
            )
            if claim is None:
                logger.info(
                    "[Hedge Claim] %s: generation already claimed by another "
                    "worker (or DB unavailable); skipping duplicate hedge.",
                    pair_tag,
                )
                order_info["hedge_waiting_for_coverage"] = True
                return None, {}, None, 0.0
            order_info["hedge_claim_key"] = claim["claim_key"]
        # A fresh durable claim owns its deterministic client id. Never
        # reuse the previous generation's pending id after a deferred fill;
        # doing so would make the claim row point at a different venue order.
        hedge_coid = (
            str(claim["hedge_client_order_id"])
            if claim else str(order_info.get("pending_hedge_client_order_id") or "")
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
            "hedge_claim_key": str(claim.get("claim_key") or "") if claim else "",
            "hedge_client_order_id": hedge_coid,
            "hedge_requested_for_spot_fill": float(unhedged_delta),
        }

        # Calculate futures hedge size and minimum notional constraints dynamically
        cfg = getattr(self, "config", None)
        futures_min_notional = float(
            order_info.get("futures_min_notional", 0.0)
            or getattr(cfg, "futures_min_notional", 0.0)
            or 0.0
        )
        if futures_min_notional <= 0:
            capabilities = getattr(self, "capabilities", None)
            if capabilities is not None and hasattr(capabilities, "get_profile"):
                prof = capabilities.get_profile(
                    str(order_info.get("global_exchange") or ""), "futures"
                )
                if prof and getattr(prof, "min_notional_usdt", 0) > 0:
                    futures_min_notional = float(prof.min_notional_usdt)

        amount_spec = AmountSpec.from_dict(
            order_info.get("global_amount_spec")
        )
        # Fail-closed metadata validation: sizing must not proceed with
        # missing/invalid spec, price, or notional metadata.
        metadata_reasons = validate_futures_sizing_metadata(
            amount_spec, expected_short_price, futures_min_notional
        )
        if metadata_reasons:
            raise ValueError(
                "futures sizing metadata invalid: " + "; ".join(metadata_reasons)
            )
        # Min-order policy is governed by the configured hedging policy
        # (hedging.futures.min_order_policy), not a legacy partial-fill safety
        # knob. "wait_until_covered" (the sanitized default) must never scale a
        # sub-minimum delta up to the venue minimum, which would overhedge.
        hedging_cfg = getattr(cfg, "hedging", None)
        if hedging_cfg is not None and getattr(hedging_cfg, "futures_min_order_policy", ""):
            min_order_policy = str(hedging_cfg.futures_min_order_policy)
        elif getattr(cfg, "futures_min_order_policy", ""):
            min_order_policy = str(cfg.futures_min_order_policy)
        else:
            min_order_policy = "wait_until_covered"
        # Map the policy onto calculate_order_size's safety policy. Strictly
        # wait (return 0 → caller keeps the unhedged delta pending) unless
        # explicit overhedge is allowed.
        allow_overhedge = bool(getattr(hedging_cfg, "futures_allow_min_notional_overhedge", False)) if (
            hedging_cfg is not None
        ) else bool(getattr(cfg, "futures_allow_min_notional_overhedge", False))
        safety_policy = (
            "scale_to_min"
            if (min_order_policy == "allow_min_notional" and allow_overhedge)
            else "offset_spot"
        )

        # unhedged_delta already arrives in futures-target units (computed as
        # cumulative_spot_filled × ratio − confirmed hedge in
        # _handle_entry_fills). Applying the ratio again would double-scale
        # non-1:1 configurations, so size directly from the residual.
        target_unhedged_delta = unhedged_delta

        futures_hedge_qty, target_notional, delta_imbalance = calculate_order_size(
            filled_spot_amount=target_unhedged_delta,
            expected_price_usdt=expected_short_price,
            futures_amount_spec=amount_spec,
            futures_min_notional=futures_min_notional,
            safety_policy=safety_policy,
        )

        # Enforce the configured overhedge ceiling on the scale-to-min path:
        # the scaled order must never exceed the target delta by more than
        # futures_max_overhedge_pct.
        if futures_hedge_qty > 0 and target_unhedged_delta > 0:
            max_overhedge_pct = (
                float(getattr(hedging_cfg, "futures_max_overhedge_pct", 0.0))
                if hedging_cfg is not None
                else float(getattr(cfg, "futures_max_overhedge_pct", 0.0))
            )
            max_allowed = target_unhedged_delta * (1.0 + max_overhedge_pct / 100.0)
            if futures_hedge_qty > max_allowed:
                logger.warning(
                    f"[Overhedge Limit] {pair_tag}: scaled hedge {futures_hedge_qty:.8f} exceeds "
                    f"{max_overhedge_pct:.1f}% overhedge ceiling ({max_allowed:.8f} max for target "
                    f"{target_unhedged_delta:.8f}); rejecting hedge."
                )
                raise ValueError(
                    f"scaled hedge {futures_hedge_qty} exceeds configured max overhedge "
                    f"{max_overhedge_pct}% (allowed {max_allowed}); overhedge policy violated"
                )

        if futures_hedge_qty <= 0:
            # "wait_until_covered": the delta cannot cover a min-notional
            # futures order yet. Keep the unhedged exposure pending so
            # subsequent spot fills can accumulate — do NOT roll back the
            # spot leg just because the minimum is not met.
            logger.info(
                f"[Wait Until Covered] {pair_tag}: unhedged delta {unhedged_delta:.8f} "
                f"(${unhedged_delta * expected_short_price:.2f}) below futures minimum "
                f"(${futures_min_notional:.2f}); holding unhedged delta for the next fill."
            )
            order_info["hedge_waiting_for_coverage"] = True
            # No venue request was started, so release this generation's
            # durable claim. The next cumulative Spot fill may then claim the
            # full outstanding residual without overlapping an older submit.
            claim_key = str(order_info.get("hedge_claim_key") or "")
            claims_service = getattr(self, "hedge_claims", None)
            if claim_key and claims_service is not None:
                if not await claims_service.mark_abandoned(
                    claim_key, hedge_coid,
                    "futures residual is below venue minimum; no submit started"
                ):
                    logger.error(
                        "[Hedge Claim] %s: could not release deferred pre-submit claim %s",
                        pair_tag, claim_key,
                    )
            return None, recovery_context, None, 0.0

        if abs(delta_imbalance) > 1e-12:
            logger.warning(
                f"[Delta Imbalance Logger] Imbalance detected on {pair_tag}: target hedge qty={target_unhedged_delta:.8f} "
                f"(${target_unhedged_delta * expected_short_price:.2f}) vs futures min notional (${futures_min_notional:.2f}). "
                f"Scaled futures short to {futures_hedge_qty:.8f} (${target_notional:.2f}). "
                f"Delta imbalance: {delta_imbalance:+.8f}"
            )
            order_info["delta_imbalance"] = delta_imbalance

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
            quantity=futures_hedge_qty,
            global_amount_spec=order_info.get("global_amount_spec"),
            recovery_context=recovery_context,
            required=True,
        )
        global_stop = getattr(self, "global_stop", None)
        hedge_account_scope_id = (
            account_scope_id_for(
                self.config, str(order_info.get("global_exchange") or "")
            )
            if (
                global_stop is not None
                and getattr(self, "config", None) is not None
                and not getattr(self.config, "mock_mode", False)
            )
            else None
        )
        if global_stop is not None:
            await global_stop.prepare_submit_attempt(
                client_order_id=hedge_coid,
                exchange=str(order_info.get("global_exchange") or ""),
                market_type="perpetual",
                symbol=global_symbol,
                side="sell",
                order_type="market",
                quantity=futures_hedge_qty,
                price=expected_short_price,
                pair_key=pair_tag,
                recovery_context=recovery_context,
                amount_spec=order_info.get("global_amount_spec"),
                account_scope_id=hedge_account_scope_id,
            )
        risk_metrics = RiskBudgetMetricsBuilder.build(
            self,
            venue=str(order_info.get("global_exchange") or ""),
            action_class="HEDGE",
            proposed_notional_usdt=target_notional,
            slippage_bps=0.0,
            funding_cost_pct=abs(
                float(order_info.get("current_funding_rate", 0.0) or 0.0)
            ) * 100.0,
            market_data_age_ms=(
                max(0, int(time.time() * 1000) - int(order_info["global_book_timestamp_ms"]))
                if order_info.get("global_book_timestamp_ms") else None
            ),
        )
        risk_limits = RiskBudgetMetricsBuilder.limits_from_config(getattr(self, "config", None))
        ip_ban_stop_persistence_failed = False
        try:
            if global_stop is None:
                order_info["hedge_submit_started"] = True
                short_order = await self.client.open_global_short_market(
                    global_symbol,
                    futures_hedge_qty,
                    price=expected_short_price,
                    client_order_id=hedge_coid,
                    amount_spec=amount_spec,
                )
            else:
                ip_ban_state = None
                try:
                    async with global_stop.order_submit_fence(
                        client_order_id=hedge_coid,
                        scope_exchange=str(order_info.get("global_exchange") or ""),
                        scope_symbol=global_symbol,
                        scope_market_type="perpetual",
                        account_scope_id=hedge_account_scope_id,
                        action_class=("HEDGE" if hedge_account_scope_id else "ENTRY"),
                        scope_pair_key=pair_tag,
                        risk_metrics=risk_metrics,
                        risk_limits=risk_limits,
                    ) as fence_conn:
                        order_info["hedge_submit_started"] = True
                        try:
                            short_order = await self.client.open_global_short_market(
                                global_symbol,
                                futures_hedge_qty,
                                price=expected_short_price,
                                client_order_id=hedge_coid,
                                amount_spec=amount_spec,
                            )
                        except Exception as fenced_error:
                            error_text = (
                                f"{type(fenced_error).__name__} {fenced_error}"
                            ).lower()
                            ip_ban_detected = self._is_explicit_ip_ban(error_text)
                            ip_ban_persisted = False
                            if ip_ban_detected:
                                self._halt_exposure_scope(
                                    "*",
                                    "CEX IP block: "
                                    + str(order_info.get("global_exchange") or ""),
                                )
                                self._refresh_entry_gate()
                                try:
                                    ip_ban_state = await global_stop.request_stop_in_transaction(
                                        fence_conn,
                                        "CEX_IP_BLOCK",
                                        source="exchange:"
                                        + str(order_info.get("global_exchange") or ""),
                                        metadata={
                                            "exchange": order_info.get("global_exchange"),
                                            "symbol": global_symbol,
                                            "error_type": type(fenced_error).__name__,
                                            "exchange_error": str(fenced_error)[:1000],
                                        },
                                    )
                                    ip_ban_persisted = True
                                except Exception:
                                    ip_ban_stop_persistence_failed = True
                                    logger.exception(
                                        "CRITICAL: failed to persist futures CEX IP-ban STOP "
                                        "inside submit fence"
                                    )
                            if ip_ban_detected and not ip_ban_persisted:
                                # Keep the durable attempt unresolved so peers cannot
                                # proceed if canonical STOP persistence failed.
                                await global_stop.mark_submit_unknown(
                                    hedge_coid,
                                    "confirmed futures venue IP ban; Global STOP persistence failed: "
                                    + str(fenced_error),
                                )
                                raise
                            rejection_check = getattr(
                                self, "_is_definitive_order_rejection", None
                            )
                            if callable(rejection_check) and rejection_check(fenced_error):
                                await global_stop.mark_submit_rejected(
                                    hedge_coid, str(fenced_error)
                                )
                                raise
                            await global_stop.mark_submit_unknown(
                                hedge_coid, str(fenced_error)
                            )
                            raise
                        if not isinstance(short_order, dict) or not short_order.get("id"):
                            await global_stop.mark_submit_unknown(
                                hedge_coid,
                                "futures hedge acknowledgement missing exchange order id",
                            )
                            raise AmbiguousOrderSubmission(
                                "futures hedge acknowledgement has no exchange order id; "
                                "reconciliation required"
                            )
                        await global_stop.mark_submit_acknowledged(
                            hedge_coid, order=short_order,
                        )
                finally:
                    if ip_ban_state is not None:
                        await global_stop.notify_state(ip_ban_state)
        except Exception as submit_error:
            if ip_ban_stop_persistence_failed:
                # Do not let an eventually-consistent lookup retire the only
                # durable cross-instance guard after canonical STOP failed.
                raise AmbiguousOrderSubmission(
                    "confirmed futures venue IP ban could not be persisted as Global STOP; "
                    "submit attempt remains unresolved"
                ) from submit_error
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
            if global_stop is not None:
                await global_stop.mark_submit_acknowledged(
                    hedge_coid, order=short_order
                )
        if not isinstance(short_order, dict) or not short_order.get("id"):
            raise AmbiguousOrderSubmission(
                "futures hedge acknowledgement has no exchange order id; "
                "reconciliation required"
            )
        # Record the durable hedge claim transition only after the venue
        # acknowledgement is known. If this write fails, keep the accepted
        # order ambiguous and let reconciliation resolve it; never release or
        # recreate the claim for a new Spot fill generation.
        claim_key = str(order_info.get("hedge_claim_key") or "")
        claims_service = getattr(self, "hedge_claims", None)
        if claim_key and claims_service is not None:
            marked_submitted = await claims_service.mark_submitted(claim_key, hedge_coid)
            if not marked_submitted:
                raise AmbiguousOrderSubmission(
                    "futures order was acknowledged but durable hedge claim "
                    "could not transition to SUBMITTED; reconciliation required"
                )
        hedge_status = str(short_order.get("status", "")).lower()
        if hedge_status not in ("closed", "filled"):
            order_id = str(short_order.get("id") or "")
            if order_id:
                try:
                    verified = await self.client.fetch_global_order(
                        order_id, global_symbol, order_info.get("global_amount_spec")
                    )
                    if isinstance(verified, dict) and verified.get("id"):
                        short_order = verified
                        hedge_status = str(short_order.get("status", "")).lower()
                except Exception as exc:
                    logger.debug("[State 2] follow-up futures hedge verification failed for %s: %s", order_id, exc)
        if hedge_status not in ("closed", "filled"):
            raise ValueError(
                f"futures hedge is not terminal (status={hedge_status})"
            )
        try:
            actual_hedged_qty = float(short_order.get("filled") or 0.0)
        except (TypeError, ValueError):
            actual_hedged_qty = 0.0
        qty_tolerance = max(1e-12, futures_hedge_qty * 1e-8)
        if actual_hedged_qty + qty_tolerance < futures_hedge_qty:
            raise ValueError(
                f"futures hedge fill incomplete: requested={futures_hedge_qty}, "
                f"filled={actual_hedged_qty}; spot rollback will be guarded by "
                "an authoritative futures-position check"
            )
        if actual_hedged_qty > futures_hedge_qty + qty_tolerance:
            raise ValueError(
                f"futures hedge overfilled: requested={futures_hedge_qty}, "
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
            quantity=futures_hedge_qty,
            global_amount_spec=order_info.get("global_amount_spec"),
            recovery_context=recovery_context,
        )
        if hedge_journal is None:
            raise RuntimeError(
                "futures hedge was accepted but its fill could not be journaled; "
                "rollback/reconciliation is required"
            )
        return hedge_coid, recovery_context, short_order, actual_hedged_qty

    def _halt_hedge_venue_permission(self, venue: str, error: Exception) -> None:
        """Stop new entries on a venue whose API key cannot open the hedge leg."""
        name = str(venue or "").strip().lower()
        if not name:
            return
        reason = str(error)[:400]
        denials = getattr(self, "_hedge_permission_denied", None)
        if not isinstance(denials, dict):
            self._hedge_permission_denied = denials = {}
        denials[name] = reason
        self._exchange_halted[name] = (
            f"hedge key trading permission denied ({reason[:120]})"
        )
        self._persist_risk_state(
            "exchange_halt", scope="exchange", exchange=name,
            reason=self._exchange_halted[name],
            value={"source": "hedge_permission_denied", "venue_error": reason},
        )
        self._refresh_entry_gate()
        logger.critical(
            "[Risk Control] Exchange %s HALTED: %s",
            name, self._exchange_halted[name],
        )

    async def _publish_entry_position_opened(self, pair_tag: str) -> None:
        pos = self.active_positions[pair_tag]
        open_msg = {
            "event": "POSITION_OPENED",
            "local_exchange": pos.local_exchange,
            "global_exchange": pos.global_exchange,
            "symbol": pos.symbol,
            "qty": pos.qty,
            "entry_price_idr": pos.entry_price_idr,
            "short_entry_usdt": pos.short_entry_usdt,
            "is_mock": getattr(self.config, "mock_mode", False),
            "timestamp_ms": int(time.time() * 1000),
        }
        await self.redis.publish(self.ARB_TRADE_EVENTS, json.dumps(open_msg))
        self.supabase.event(
            "POSITION_OPENED",
            pair_key=pair_tag,
            is_mock=open_msg["is_mock"],
            occurred_at_ms=open_msg["timestamp_ms"],
            payload=open_msg,
        )

    async def _persist_entry_hedge_success(
        self, pair_tag: str, order_info: dict, unhedged_delta: float,
        price_idr: float, usdt_rate: float, hedge_coid: str,
        recovery_context: dict, short_order: dict, actual_hedged_qty: float,
        expected_short_price: float,
    ) -> None:
        global_symbol = order_info["global_symbol"]
        order_info["hedged_short_qty"] += actual_hedged_qty
        # The deferred obligation is now (at least partially) covered.
        order_info["hedge_waiting_for_coverage"] = False
        watermark_persisted = await self._persist_entry_hedge_watermark(
            pair_tag,
            order_info,
            order_info["hedged_short_qty"],
            hedge_order_id=hedge_coid,
        )
        self._record_hedged_position(
            pair_tag, order_info, actual_hedged_qty,
            short_order, price_idr, usdt_rate,
        )
        # Immutable ledger: the spot leg that triggered this hedge was already
        # appended in _handle_entry_fills, so only the hedge leg is new here.
        await self._record_futures_entry_fill(
            pair_tag, order_info, short_order, actual_hedged_qty, usdt_rate
        )
        self.supabase.order(
            pair_tag,
            "futures_entry",
            order_info.get("global_exchange", ""),
            "perpetual",
            global_symbol,
            short_order,
            client_order_id=hedge_coid,
            reduce_only=False,
            is_mock=bool(getattr(self.config, "mock_mode", False)),
        )
        await self._register_order(
            pair_tag,
            hedge_coid,
            short_order,
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
        total_order_qty = float(order_info.get("qty", 0.0) or 0.0)
        is_partial = total_order_qty > 0 and float(order_info.get("hedged_short_qty", 0.0) or 0.0) < total_order_qty - 1e-9
        if is_partial:
            self.supabase.upsert_partial_position(
                pair_tag,
                self.active_positions[pair_tag],
                getattr(self, "_pair_infos", {}).get(pair_tag),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
                status="PARTIALLY_FILLED",
            )
        else:
            self.supabase.position_opened(
                pair_tag,
                self.active_positions[pair_tag],
                getattr(self, "_pair_infos", {}).get(pair_tag),
                is_mock=bool(getattr(self.config, "mock_mode", False)),
            )
        if getattr(self, "two_leg_coordinator", None) is not None:
            coord = self.two_leg_coordinator
            fut_leg = coord.futures_manager.legs.get(f"futures-{pair_tag}")
            if fut_leg is None:
                fut_leg = coord.futures_manager.create_leg(
                    pair_tag,
                    venue=str(order_info.get("global_exchange") or ""),
                    symbol=global_symbol,
                    contract_size=1.0,
                )
            fill_price = self._resolve_short_entry_usdt(short_order)
            coord.futures_manager.record_fill(
                fut_leg.leg_id, filled_contracts=actual_hedged_qty, price=fill_price
            )
            drift_report = coord.evaluate_hedge_drift(pair_tag)
            if drift_report.status.value == "HARD_DRIFT_BREACH":
                logger.critical(
                    "[Hedge Drift] Hard breach on %s: unhedged=%.6f drift_ratio=%.2f%% age=%.1fs",
                    pair_tag, drift_report.unhedged_qty, drift_report.drift_ratio * 100.0, drift_report.exposure_age_seconds
                )
        await self.redis.set(
            f"arb:active_pos:{pair_tag}",
            json.dumps(self.active_positions[pair_tag].to_dict()),
        )
        self.states[pair_tag] = PositionState.HEDGED_ACTIVE
        self._record_pair_success(pair_tag)
        if not watermark_persisted:
            self._halt_exposure_scope(
                pair_tag, "entry hedge watermark could not be persisted"
            )
            logger.critical(
                "[Recovery] %s is hedged on the venue but its originating "
                "spot-order watermark is not durable; pair entry remains halted",
                pair_tag,
            )
        order_info.pop("pending_hedge_client_order_id", None)
        order_info["hedge_submit_started"] = False
        await self._publish_entry_position_opened(pair_tag)

    async def _execute_entry_hedge(
        self,
        pair_tag: str,
        order_info: dict,
        unhedged_delta: float,
        price_idr: float,
        usdt_rate: float,
        hedge_ratio: float = 1.0,
    ) -> None:
        """Execute market short on global exchange and persist position state."""
        order_info["hedge_submit_started"] = False
        order_info.pop("pending_hedge_client_order_id", None)
        global_symbol = order_info["global_symbol"]
        try:
            expected_short_price = float(
                order_info.get("global_ask_usdt", 0.0) or 0.0
            )
            if expected_short_price <= 0 and usdt_rate > 0:
                expected_short_price = price_idr / usdt_rate
            await self._assert_hedge_slippage_within_limit(
                pair_tag, order_info, unhedged_delta, expected_short_price
            )
            hedge_coid, recovery_context, short_order, actual_hedged_qty = (
                await self._submit_entry_hedge_order(
                    pair_tag, order_info, global_symbol,
                    unhedged_delta, expected_short_price,
                )
            )
            if short_order is None:
                # "wait_until_covered": delta is below the futures minimum, the
                # hedge is intentionally deferred. Keep the pair pending so the
                # next fill can accumulate coverage — no rollback, no failure.
                logger.info(
                    f"[Wait Until Covered] {pair_tag}: hedge deferred; "
                    f"unhedged delta ${unhedged_delta * expected_short_price:.2f} "
                    "remains pending."
                )
                return
            await self._persist_entry_hedge_success(
                pair_tag, order_info, unhedged_delta, price_idr, usdt_rate,
                hedge_coid, recovery_context, short_order, actual_hedged_qty,
                expected_short_price,
            )
            claim_key = str(order_info.get("hedge_claim_key") or "")
            if claim_key:
                claims_service = getattr(self, "hedge_claims", None)
                if claims_service is not None:
                    claim_confirmed = await claims_service.mark_confirmed(
                        claim_key, actual_hedged_qty, hedge_coid
                    )
                    if not claim_confirmed:
                        # The venue hedge is already filled. Do not undo it or
                        # release the claim. Re-open the exact terminal order
                        # record in the recovery index so startup recovery can
                        # retry claim confirmation rather than silently skipping it.
                        retained = await self._retain_hedge_claim_for_recovery(
                            pair_tag,
                            str(order_info.get("global_exchange") or ""),
                            hedge_coid,
                            "venue hedge fill confirmed; durable claim confirmation requires retry",
                        )
                        self._halt_exposure_scope(
                            pair_tag,
                            "confirmed futures hedge claim was not persisted"
                            + ("" if retained else "; recovery record retention also failed"),
                        )
                        self._refresh_entry_gate()
                        self._mark_failed(
                            pair_tag,
                            "HEDGE_CLAIM_CONFIRMATION_PERSIST_FAILED",
                            "Futures hedge is filled, but its durable hedge claim "
                            "could not be marked CONFIRMED. The pair is blocked until "
                            "the claim and position are reconciled.",
                            detail={
                                "hedge_claim_key": claim_key,
                                "hedge_client_order_id": hedge_coid,
                                "confirmed_futures_qty": actual_hedged_qty,
                            },
                            state=PositionState.RECONCILIATION_REQUIRED,
                        )
                        await self._maybe_publish_status()
        except Exception as he:
            logger.critical(
                f"[State 2 CRITICAL] Failed to execute instant short hedge for {pair_tag}: {he}"
            )
            # A claim acquired before the venue boundary is safe to release:
            # no exchange order can exist for this generation. Once the venue
            # call has started, keep CLAIMED/SUBMITTED durable until recovery
            # proves the outcome; never release an ambiguous submission.
            claim_key = str(order_info.get("hedge_claim_key") or "")
            claims_service = getattr(self, "hedge_claims", None)
            if claim_key and claims_service is not None and not order_info.get(
                "hedge_submit_started"
            ):
                await claims_service.mark_abandoned(
                    claim_key, hedge_coid,
                    f"pre-submit hedge failure: {type(he).__name__}: {he}"
                )
            global_exchange = str(order_info.get("global_exchange", ""))
            # A key-permission refusal or trading restriction is a definitive venue response:
            # the short was never created, so the spot leg must be unwound immediately and
            # the venue stopped rather than retried on the next signal.
            if is_trading_permission_denied(he):
                if global_exchange:
                    self._halt_hedge_venue_permission(global_exchange, he)
                if order_info.get("hedge_submit_started"):
                    try:
                        await self._mark_order_submission_rejected(
                            global_exchange,
                            str(order_info.get("pending_hedge_client_order_id") or ""),
                        )
                    except Exception:
                        logger.exception(
                            "[Hedge Rollback] could not retire rejected hedge intent for %s",
                            pair_tag,
                        )
                    order_info["hedge_submit_started"] = False
                await self._rollback_hedge_failure(
                    pair_tag, order_info, unhedged_delta,
                    price_idr, usdt_rate, he,
                )
                return
            is_slippage_err = isinstance(he, HedgeSlippageExceeded)
            if global_exchange and not is_slippage_err:
                self._record_order_failure(
                    global_exchange, "hedge order placement failed"
                )
            if order_info.get("hedge_submit_started"):
                hedge_client_id = str(
                    order_info.get("pending_hedge_client_order_id") or ""
                )
                verified_absent = False
                verification_reason = "hedge absence has not been verified"
                verifier = getattr(self, "_verify_ambiguous_hedge_absent", None)
                if callable(verifier):
                    try:
                        verified_absent, verification_reason = await verifier(
                            pair_tag, order_info, hedge_client_id, he, unhedged_delta
                        )
                    except Exception as verification_error:
                        verification_reason = (
                            "hedge reconciliation failed: "
                            f"{type(verification_error).__name__}: {verification_error}"
                        )
                if verified_absent:
                    logger.critical(
                        "[Hedge Reconciliation] %s: %s; proceeding with guarded spot rollback",
                        pair_tag, verification_reason,
                    )
                    order_info["hedge_submit_started"] = False
                    try:
                        await self._mark_order_submission_rejected(
                            global_exchange, hedge_client_id
                        )
                    except Exception:
                        logger.debug(
                            "[Hedge Rollback] could not retire rejected hedge intent for %s: %s",
                            pair_tag, hedge_client_id,
                        )
                    await self._rollback_hedge_failure(
                        pair_tag, order_info, unhedged_delta,
                        price_idr, usdt_rate, he,
                    )
                    return
                await self._mark_order_submission_ambiguous(
                    pair_tag, global_exchange, hedge_client_id,
                    f"{type(he).__name__}: {he}; {verification_reason}",
                )
                return
            await self._rollback_hedge_failure(
                pair_tag, order_info, unhedged_delta, price_idr, usdt_rate, he
            )
        else:
            global_exchange = str(order_info.get("global_exchange", ""))
            if global_exchange:
                self._record_order_success(global_exchange)

    @staticmethod
    def _resolve_short_entry_usdt(short_order: Dict[str, Any]) -> float:
        """
        Resolves the actual futures short entry price.

        Never silently falls back to the spot price: if neither the exchange fill
        nor the expected futures ask is known, fail loudly so the position cannot
        be recorded with a wrong basis.
        """
        short_entry_usdt = helpers.extract_fill_price(short_order)
        if short_entry_usdt <= 0:
            raise ValueError(
                "short entry price unavailable from futures fill; refusing to "
                "record position using spot price basis"
            )
        return short_entry_usdt

# Risk controls (exposure caps, venue halts, balance drift) live in
# executor/risk.py as RiskMixin; hedge-failure rollback lives in
# executor/rollback.py as RollbackMixin. This module stays focused on entry flow.

    async def _assert_hedge_slippage_within_limit(
        self,
        pair_tag: str,
        order_info: dict,
        unhedged_delta: float,
        expected_short_price: float,
    ) -> None:
        """
        Rejects the hedge when the futures ask already moved past the expected
        short price by more than ``max_entry_slippage_pct``.

        The caller catches the failure and rolls the spot leg back, so a bad
        quote becomes a small, bounded loss instead of an unprofitable position.
        A ticker that cannot be read does not block the hedge: refusing to hedge
        at all is strictly riskier than hedging at an unverified price.
        """
        max_slippage = float(getattr(self.config, "max_entry_slippage_pct", 0.0) or 0.0)
        if max_slippage <= 0 or expected_short_price <= 0:
            return

        global_symbol = order_info["global_symbol"]
        try:
            ticker = await self.client.fetch_global_ticker(global_symbol)
        except Exception as exc:  # nosec BLE001 - fail open, see docstring
            logger.warning(
                f"[Slippage Guard] could not read {global_symbol} ticker for {pair_tag}: "
                f"{exc}. Proceeding with the hedge."
            )
            return

        try:
            raw_bid = (ticker or {}).get("bid")
            raw_ask = (ticker or {}).get("ask")
            live_bid = float(raw_bid if raw_bid is not None else (raw_ask or 0.0))
        except (TypeError, ValueError):
            live_bid = 0.0
        if live_bid <= 0:
            logger.info(
                f"[Slippage Guard] no live bid/quote for {global_symbol}; skipping guard on {pair_tag}"
            )
            return

        # Pilihan C (Asymmetric Guard):
        # 1) Ketat di bawah: Short hedge menjual ke bid Futures, sehingga penurunan harga
        #    melebihi max_slippage (mis. -0.3%) merusak spread dan harus diblokir.
        # 2) Longgar di atas: Kenaikan harga Futures hingga +5% (atau 10x max_slippage)
        #    adalah windfall profit untuk posisi Short dan tetap dieksekusi; di atas itu
        #    diblokir sebagai proteksi anomali ticker/fat-finger.
        floor_price = expected_short_price * (1.0 - max_slippage)
        sanity_ceiling_pct = max(0.05, max_slippage * 10.0)
        ceiling_price = expected_short_price * (1.0 + sanity_ceiling_pct)

        if live_bid < floor_price:
            deviation_pct = (1.0 - (live_bid / expected_short_price)) * 100.0
            raise HedgeSlippageExceeded(
                f"futures bid {live_bid} dropped {deviation_pct:.3f}% below expected "
                f"{expected_short_price} (limit -{max_slippage * 100:.3f}%, "
                f"qty {unhedged_delta})"
            )
        if live_bid > ceiling_price:
            deviation_pct = ((live_bid / expected_short_price) - 1.0) * 100.0
            raise HedgeSlippageExceeded(
                f"futures quote {live_bid} spiked {deviation_pct:.3f}% above expected "
                f"{expected_short_price} (sanity ceiling +{sanity_ceiling_pct * 100:.3f}%, "
                f"qty {unhedged_delta})"
            )


    def _record_hedged_position(
        self,
        pair_tag: str,
        order_info: dict,
        qty: float,
        short_order: dict,
        price_idr: float,
        usdt_rate: float,
    ) -> None:
        """Update or create active position record for the hedged fill."""
        short_entry_usdt = self._resolve_short_entry_usdt(short_order)
        spot_cost = (qty * price_idr) / usdt_rate
        fut_margin = qty * short_entry_usdt

        if pair_tag not in self.active_positions:
            self.active_positions[pair_tag] = ActivePosition(
                local_exchange=order_info["local_exchange"],
                global_exchange=order_info["global_exchange"],
                symbol=order_info["symbol"],
                base_asset=order_info.get("base_asset", ""),
                global_symbol=order_info.get("global_symbol", ""),
                entry_price_idr=price_idr,
                short_entry_usdt=short_entry_usdt,
                qty=qty,
                initial_qty=qty,
                opened_at_ms=int(order_info.get("opened_at_ms") or time.time() * 1000),
                accumulated_funding_usdt=0.0,
                next_funding_time_ms=order_info["next_funding_time_ms"],
                base_target_exit_spread_pct=order_info["base_target_exit_spread_pct"],
                current_funding_rate=order_info["current_funding_rate"],
                local_price_tick=float(order_info.get("tick_size", 1.0)),
                local_lot_size=float(order_info.get("local_lot_size", 0.0)),
                local_amount_spec=AmountSpec.from_dict(order_info.get("local_amount_spec")),
                global_amount_spec=AmountSpec.from_dict(order_info.get("global_amount_spec")),
                spot_cost_usdt=spot_cost,
                futures_margin_usdt=fut_margin,
                spot_entry_fee_pct=float(order_info.get("spot_entry_fee_pct", 0.0)),
                spot_exit_fee_pct=float(order_info.get("spot_exit_fee_pct", 0.0)),
                perp_entry_fee_pct=float(order_info.get("perp_entry_fee_pct", 0.0)),
                perp_exit_fee_pct=float(order_info.get("perp_exit_fee_pct", 0.0)),
                fee_source=str(order_info.get("fee_source", "LEGACY_UNVERIFIED")),
                fee_verified=bool(order_info.get("fee_verified", False)),
                fee_fetched_at_ms=int(order_info.get("fee_fetched_at_ms", 0)),
                fee_asset_surcharge_pct=float(order_info.get("fee_asset_surcharge_pct", 0.0)),
                expected_net_profit_usdt=float(order_info.get("expected_net_profit_usdt", 0.0) or 0.0),
                expected_fees_usdt=float(order_info.get("expected_fees_usdt", 0.0) or 0.0),
                expected_hold_hours=float(order_info.get("expected_hold_hours", 0.0) or 0.0),
            )
        else:
            self.active_positions[pair_tag].qty += qty
            self.active_positions[pair_tag].spot_cost_usdt += spot_cost
            self.active_positions[pair_tag].futures_margin_usdt += fut_margin

    async def _finalize_entry_order(self, pair_tag: str, status: str) -> None:
        """Remove finished entry order from pending list and transition state."""
        if status not in ("closed", "canceled", "cancelled", "rejected", "expired"):
            return

        order_info = self.pending_entry_orders.get(pair_tag, {})
        # If partial fill left unused reserved funds, release the remainder
        if hasattr(self.client, "unlock_funds"):
            pos = self.active_positions.get(pair_tag)
            used_spot = pos.spot_cost_usdt if pos else 0.0
            used_fut = pos.futures_margin_usdt if pos else 0.0
            reserved_spot = float(order_info.get("spot_cost_usdt", 0.0))
            reserved_fut = float(order_info.get("futures_margin_usdt", 0.0))
            unused_spot = max(0.0, reserved_spot - used_spot)
            unused_fut = max(0.0, reserved_fut - used_fut)
            if unused_spot > 0 or unused_fut > 0:
                self.client.unlock_funds(unused_spot, unused_fut)

        if getattr(self, "slot_manager", None) is not None:
            venue = str(order_info.get("local_exchange") or "").lower()
            coid = str(order_info.get("client_order_id") or "")
            if venue and coid:
                await self.slot_manager.release_slot(venue, client_order_id=coid, reason=status.upper())

        awaiting_coverage = bool(order_info.get("hedge_waiting_for_coverage"))
        hedged_qty = float(order_info.get("hedged_short_qty", 0.0) or 0.0)
        rolled_back_qty = float(order_info.get("rolled_back_qty", 0.0) or 0.0)
        cumulative_filled = float(order_info.get("filled", 0.0) or 0.0)
        # Canonical residual check: existence of active_positions alone is not
        # proof the hedge target is complete.
        _hed_cfg = getattr(getattr(self, "config", None), "hedging", None)
        try:
            _ratio = float(getattr(_hed_cfg, "futures_target_hedge_ratio", 1.0) or 1.0)
        except (TypeError, ValueError):
            _ratio = 1.0
        if not (_ratio > 0):
            _ratio = 1.0
        _target, residual, _ = canonical_hedge_target(
            cumulative_filled, hedged_qty + rolled_back_qty, _ratio
        )
        # Any unresolved (UNKNOWN) hedge submission for this entry also blocks
        # a clean finalize: the hedge may exist on the venue.
        has_unknown_hedge = bool(
            getattr(self, "global_stop", None) is not None
            and order_info.get("pending_hedge_client_order_id")
            and order_info.get("hedge_submit_started")
        )
        del self.pending_entry_orders[pair_tag]
        await self._clear_pending_entry(pair_tag)
        if self.states.get(pair_tag) == PositionState.RECONCILIATION_REQUIRED:
            # A reconciliation halt set while the order was terminal must not
            # be silently reset by cleanup; manual intervention is pending.
            self.pending_entry_orders.pop(pair_tag, None)
            await self._clear_pending_entry(pair_tag)
            return
        # Residual/UNKNOWN check MUST come before the active_positions check:
        # an active position can exist from a partial hedge while residual
        # exposure remains. active_positions alone is never proof the hedge
        # target is complete.
        if residual > 1e-12 or has_unknown_hedge:
            # Residual unhedged exposure (deferred or not) or an unresolved
            # hedge submission must surface as a reconciliation obligation,
            # never collapse into IDLE or a clean HEDGED_ACTIVE.
            self._mark_failed(
                pair_tag,
                "DEFERRED_HEDGE_UNCOVERED_ON_TERMINAL_SPOT"
                if awaiting_coverage else "HEDGE_RESIDUAL_ON_TERMINAL_SPOT",
                (
                    f"Spot order terminal ('{status}') with residual unhedged "
                    f"exposure {residual:.8f}"
                    + (" and an unresolved hedge submission" if has_unknown_hedge else "")
                    + " — manual reconciliation required."
                ),
                detail={
                    "order_id": order_info.get("order_id"),
                    "cumulative_filled": cumulative_filled,
                    "hedged_qty": hedged_qty,
                    "target_hedge_qty": _target,
                    "residual": residual,
                    "unknown_hedge_client_order_id": (
                        order_info.get("pending_hedge_client_order_id")
                        if has_unknown_hedge else None
                    ),
                },
                state=PositionState.RECONCILIATION_REQUIRED,
            )
            return
        if pair_tag in self.active_positions and self.active_positions[pair_tag].qty > 0:
            # Only reached when residual == 0 and no UNKNOWN submission: the
            # hedge target is fully covered.
            self.states[pair_tag] = PositionState.HEDGED_ACTIVE
            sync_entry = getattr(self, "_sync_entry_order_record", None)
            if callable(sync_entry):
                await sync_entry(
                    str(order_info.get("local_exchange") or ""),
                    str(order_info.get("order_id") or ""),
                    order_info.get("client_order_id"),
                    cumulative_filled,
                    status=status,
                    retire=True,
                )
            return
        self.states[pair_tag] = PositionState.IDLE
        sync_entry = getattr(self, "_sync_entry_order_record", None)
        if callable(sync_entry):
            await sync_entry(
                str(order_info.get("local_exchange") or ""),
                str(order_info.get("order_id") or ""),
                order_info.get("client_order_id"),
                cumulative_filled,
                status=status,
                retire=True,
            )
