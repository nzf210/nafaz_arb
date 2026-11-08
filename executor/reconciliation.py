"""ReconciliationMixin responsibilities for ArbitrageEngine."""

import logging
import math
from typing import Any, Dict

from executor.models import PositionState

logger = logging.getLogger("executor.engine")


class ReconciliationMixin:
    async def _verify_account_wide_exchange_state(self) -> None:
        """
        Fail closed on untracked futures exposure or unknown resting orders.

        Per-symbol recovery checks prove only the positions already present in
        Redis. This account-wide pass also discovers manual/foreign positions
        and orders that the executor does not own.
        """
        positions_reader = getattr(self.client, "fetch_account_positions_report", None)
        orders_reader = getattr(self.client, "fetch_account_open_orders_report", None)
        if not callable(positions_reader) or not callable(orders_reader):
            reason = "account-wide positions/open-orders readers are unavailable"
            perpetual_venues = set(getattr(self.client, "perpetual_clients", {}) or {})
            spot_venues = set(getattr(self.client, "spot_clients", {}) or {})
            if not perpetual_venues:
                perpetual_venues.add(str(getattr(self.config, "global_exchange_name", "") or "").lower())
            if not spot_venues:
                spot_venues.add(str(getattr(self.config, "local_exchange_name", "") or "").lower())
            for venue in perpetual_venues | spot_venues:
                if venue:
                    self._exchange_halted[venue] = reason
            logger.critical(f"[Reconciliation] {reason}; all live entries blocked")
            return

        def norm_symbol(value: str) -> str:
            return str(value or "").split(":")[0].replace("/", "").replace("-", "").replace("_", "").upper()

        expected_by_venue: Dict[str, Dict[str, float]] = {}
        tags_by_venue_symbol: Dict[tuple, list] = {}
        for pair_tag, pos in self.active_positions.items():
            venue = str(pos.global_exchange or "").lower()
            symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            key = (venue, norm_symbol(symbol))
            expected_by_venue.setdefault(venue, {})[norm_symbol(symbol)] = (
                expected_by_venue.setdefault(venue, {}).get(norm_symbol(symbol), 0.0)
                + float(pos.qty)
            )
            tags_by_venue_symbol.setdefault(key, []).append(pair_tag)

        try:
            position_reports = await positions_reader()
            order_reports = await orders_reader()
        except Exception as exc:
            reason = f"account-wide exchange state query failed: {exc}"
            for venue in set(getattr(self.client, "perpetual_clients", {})) | set(getattr(self.client, "spot_clients", {})):
                self._exchange_halted[venue] = reason
            logger.exception("[Reconciliation] account-wide exchange state unavailable")
            return

        for report in position_reports or []:
            venue = str(report.get("name") or "").lower()
            if report.get("status") != "ok":
                self._exchange_halted[venue] = f"cannot read account positions: {report.get('error', 'unknown error')}"
                continue
            actual: Dict[str, float] = {}
            malformed_reason = ""
            for position in report.get("positions") or []:
                if not isinstance(position, dict):
                    malformed_reason = "malformed futures position row"
                    break
                symbol = position.get("symbol")
                if not symbol:
                    malformed_reason = "futures position missing symbol"
                    break
                info = position.get("info") or {}
                if not isinstance(info, dict):
                    malformed_reason = f"malformed position metadata for {symbol}"
                    break
                side = str(
                    position.get("side")
                    or position.get("positionSide")
                    or info.get("posSide")
                    or info.get("positionSide")
                    or ""
                ).lower()
                try:
                    contracts = position.get("contracts")
                    if contracts is not None:
                        size = abs(float(contracts)) * abs(float(position.get("contractSize") or 1.0))
                    else:
                        raw_amount = position.get("positionAmt", info.get("positionAmt"))
                        if raw_amount is None:
                            malformed_reason = f"position size unavailable for {symbol}"
                            break
                        signed_amount = float(raw_amount)
                        size = abs(signed_amount)
                        if not side:
                            side = "short" if signed_amount < 0 else "long"
                    if not math.isfinite(size) or size < 0:
                        malformed_reason = f"invalid position size for {symbol}"
                        break
                except (TypeError, ValueError):
                    malformed_reason = f"invalid position size for {symbol}"
                    break
                if size <= 1e-12:
                    continue
                if side != "short":
                    malformed_reason = f"unexpected {side or 'unknown-side'} futures exposure on {symbol}"
                    break
                normalized = norm_symbol(symbol)
                actual[normalized] = actual.get(normalized, 0.0) + size

            if malformed_reason:
                self._exchange_halted[venue] = malformed_reason
                logger.critical(f"[Reconciliation] {venue}: {malformed_reason}")
                continue

            expected = expected_by_venue.get(venue, {})
            for symbol in set(expected) | set(actual):
                expected_qty = expected.get(symbol, 0.0)
                actual_qty = actual.get(symbol, 0.0)
                tolerance = max(1e-10, expected_qty * 1e-4)
                if abs(expected_qty - actual_qty) <= tolerance:
                    continue
                reason = (
                    f"account-wide futures mismatch for {symbol}: "
                    f"recorded={expected_qty}, exchange={actual_qty}"
                )
                self._exchange_halted[venue] = reason
                logger.critical(f"[Reconciliation] {venue}: {reason}")
                for pair_tag in tags_by_venue_symbol.get((venue, symbol), []):
                    pos = self.active_positions[pair_tag]
                    pos.state = "RECONCILIATION_REQUIRED"
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    self._mark_failed(
                        pair_tag,
                        "ACCOUNT_POSITION_MISMATCH",
                        reason,
                        detail={"exchange": venue, "symbol": symbol, "recorded_qty": expected_qty, "actual_qty": actual_qty},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )

        known_spot_order_ids = {
            (
                str(info.get("local_exchange") or "").lower(),
                str(info.get("order_id")),
            )
            for info in list(self.pending_entry_orders.values()) + list(self.pending_exit_orders.values())
            if info.get("order_id") and info.get("local_exchange")
        }
        for report in order_reports or []:
            venue = str(report.get("name") or "").lower()
            market = str(report.get("market") or "")
            if report.get("status") != "ok":
                self._exchange_halted[venue] = f"cannot read {market} open orders: {report.get('error', 'unknown error')}"
                continue
            for order in report.get("orders") or []:
                order_id = str(order.get("id") or "")
                # Scope known IDs to their venue: many exchanges issue overlapping
                # numeric order IDs, so an ID from Indodax must not whitelist an
                # unrelated Tokocrypto order with the same value.
                known = market == "spot" and (venue, order_id) in known_spot_order_ids
                if known:
                    continue
                reason = f"untracked {market} open order {order_id} on {venue}"
                self._exchange_halted[venue] = reason
                logger.critical(f"[Reconciliation] {reason}; new entries blocked")

        # Confirm that every recorded spot leg is still backed by the venue's
        # actual base-asset inventory. Extra inventory is not assumed to be bot
        # exposure, but a shortfall means the paired position is no longer backed.
        expected_spot: Dict[str, Dict[str, float]] = {}
        tags_by_spot_asset: Dict[tuple, list] = {}
        for pair_tag, pos in self.active_positions.items():
            venue = str(pos.local_exchange or "").lower()
            local_symbol = str(pos.symbol or "").upper()
            base_asset = local_symbol.split(":")[0].replace("/", "_").replace("-", "_").split("_")[0]
            if not venue or not base_asset:
                continue
            expected_spot.setdefault(venue, {})[base_asset] = (
                expected_spot.setdefault(venue, {}).get(base_asset, 0.0) + float(pos.qty)
            )
            tags_by_spot_asset.setdefault((venue, base_asset), []).append(pair_tag)

        if expected_spot:
            balance_reader = getattr(self.client, "fetch_balance_report", None)
            if not callable(balance_reader):
                for venue in expected_spot:
                    self._exchange_halted[venue] = "spot balance reader unavailable for active-position reconciliation"
            else:
                try:
                    balance_reports = await balance_reader()
                except Exception as exc:
                    balance_reports = []
                    for venue in expected_spot:
                        self._exchange_halted[venue] = f"cannot read spot balances: {exc}"
                by_venue = {
                    str(report.get("name") or "").lower(): report
                    for report in balance_reports or []
                    if report.get("market") == "spot"
                }
                for venue, assets in expected_spot.items():
                    report = by_venue.get(venue)
                    if not report or report.get("status") != "ok":
                        self._exchange_halted[venue] = (
                            f"cannot verify spot inventory: "
                            f"{(report or {}).get('error', 'balance report missing')}"
                        )
                        continue
                    totals = {
                        str(asset.get("asset") or "").upper(): float(asset.get("total") or 0.0)
                        for asset in report.get("assets") or []
                    }
                    for asset, expected_qty in assets.items():
                        actual_qty = totals.get(asset, 0.0)
                        tolerance = max(1e-10, expected_qty * 1e-4)
                        if actual_qty + tolerance >= expected_qty:
                            continue
                        reason = (
                            f"spot inventory shortfall for {asset}: "
                            f"recorded={expected_qty}, exchange_total={actual_qty}"
                        )
                        self._exchange_halted[venue] = reason
                        logger.critical(f"[Reconciliation] {venue}: {reason}")
                        for pair_tag in tags_by_spot_asset.get((venue, asset), []):
                            pos = self.active_positions[pair_tag]
                            pos.state = "RECONCILIATION_REQUIRED"
                            self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                            self._mark_failed(
                                pair_tag,
                                "SPOT_INVENTORY_MISMATCH",
                                reason,
                                detail={"exchange": venue, "asset": asset, "recorded_qty": expected_qty, "actual_qty": actual_qty},
                                state=PositionState.RECONCILIATION_REQUIRED,
                            )

    async def _verify_recovered_active_positions(self) -> None:
        """
        Verifies persisted live positions against the exchange after restart.
        Any unknown or mismatched exposure is retained and blocked for manual
        reconciliation; Redis snapshots alone are not proof of an open hedge.
        """
        if not self.active_positions:
            return
        if not hasattr(self.client, "fetch_positions"):
            logger.critical(
                "[Recovery] live client cannot fetch futures positions; "
                "all restored positions require manual reconciliation"
            )
            groups = {}
            for pair_tag, pos in self.active_positions.items():
                groups.setdefault(pos.global_symbol or self._resolve_global_symbol(pos.symbol), []).append((pair_tag, pos))
            for symbol, items in groups.items():
                for pair_tag, pos in items:
                    pos.state = "RECONCILIATION_REQUIRED"
                    self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                    await self.redis.set(
                        f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                    )
                    self._mark_failed(
                        pair_tag,
                        "RECOVERY_POSITION_STATE_UNKNOWN",
                        "Live futures position API is unavailable after restart.",
                        detail={"global_symbol": symbol, "expected_qty": pos.qty},
                        state=PositionState.RECONCILIATION_REQUIRED,
                    )
            return

        groups = {}
        for pair_tag, pos in self.active_positions.items():
            global_symbol = pos.global_symbol or self._resolve_global_symbol(pos.symbol)
            route_key = (pos.local_exchange, pos.global_exchange, global_symbol)
            groups.setdefault(route_key, []).append((pair_tag, pos))

        for (local_exchange, global_exchange, global_symbol), items in groups.items():
            expected_qty = sum(pos.qty for _, pos in items)
            try:
                if hasattr(self.client, "set_route"):
                    self.client.set_route(local_exchange, global_exchange)
                positions = await self.client.fetch_positions(global_symbol)
                if positions is None:
                    raise ValueError("fetch_positions returned None")
                actual_short_qty = 0.0
                for position in positions:
                    if not isinstance(position, dict):
                        raise ValueError("invalid position payload")
                    info = position.get("info") or {}
                    side = str(
                        position.get("side")
                        or position.get("positionSide")
                        or info.get("posSide")
                        or info.get("positionSide")
                        or ""
                    ).lower()
                    raw_contracts = position.get("contracts")
                    if raw_contracts is not None:
                        size = abs(float(raw_contracts)) * abs(
                            float(position.get("contractSize") or 1.0)
                        )
                        if size <= 1e-12:
                            continue
                        if side != "short":
                            raise ValueError(
                                f"unexpected non-short futures exposure (side={side or 'unknown'})"
                            )
                        actual_short_qty += size
                        continue

                    raw_amount = position.get("positionAmt", info.get("positionAmt"))
                    if raw_amount is None:
                        raise ValueError("position size unavailable")
                    signed_amount = float(raw_amount)
                    if abs(signed_amount) <= 1e-12:
                        continue
                    if signed_amount >= 0:
                        raise ValueError("unexpected long or unsigned futures exposure")
                    actual_short_qty += abs(signed_amount)
            except Exception as exc:
                actual_short_qty = None
                reason = f"cannot verify futures position after restart: {exc}"
            else:
                tolerance = max(1e-10, expected_qty * 1e-4)
                if abs(actual_short_qty - expected_qty) <= tolerance:
                    logger.info(
                        f"[Recovery] verified {global_symbol} short quantity "
                        f"{actual_short_qty} against Redis snapshot {expected_qty}"
                    )
                    continue
                reason = (
                    f"futures quantity mismatch after restart: expected={expected_qty}, "
                    f"exchange={actual_short_qty}"
                )

            logger.critical(
                f"[Recovery] {global_symbol}: {reason}; restored positions blocked"
            )
            for pair_tag, pos in items:
                pos.state = "RECONCILIATION_REQUIRED"
                self.states[pair_tag] = PositionState.RECONCILIATION_REQUIRED
                await self.redis.set(
                    f"arb:active_pos:{pair_tag}", json.dumps(pos.to_dict())
                )
                self._mark_failed(
                    pair_tag,
                    "RECOVERY_POSITION_MISMATCH",
                    reason,
                    detail={
                        "global_symbol": global_symbol,
                        "expected_qty": pos.qty,
                        "exchange_total_short_qty": actual_short_qty,
                    },
                    state=PositionState.RECONCILIATION_REQUIRED,
                )

