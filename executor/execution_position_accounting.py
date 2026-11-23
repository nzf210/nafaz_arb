"""Session-bound PnL snapshots and position projection persistence."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from executor.execution_repository_fencing import validate_coordinator_lease
from executor.pair_pnl_engine import PairPnLSnapshotV3
from executor.execution_pnl_common import PnLAccountingError, _d, _json


class ExecutionPositionAccountingMixin:
        async def record_snapshot(
            self, *, snapshot: PairPnLSnapshotV3, idempotency_key: str,
            coordinator_lease: Any, fencing_generation: int,
        ) -> dict:
            key = str(idempotency_key or "").strip()
            if not key or coordinator_lease is None:
                raise PnLAccountingError("snapshot idempotency key and coordinator lease are required")
            if snapshot.confidence not in {"VERIFIED", "PARTIAL", "UNKNOWN"}:
                raise PnLAccountingError("unsupported valuation confidence")
            if snapshot.confidence == "VERIFIED" and (
                snapshot.return_on_deployed_capital_pct is None
                or snapshot.total_deployed_capital_reporting is None
            ):
                raise PnLAccountingError("VERIFIED snapshot requires complete deployed-capital basis")
    
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    session = await conn.fetchrow(
                        """
                        SELECT pair_key, account_scope_id, coordinator_scope_key,
                               coordinator_owner_id
                        FROM public.execution_sessions
                        WHERE session_id=$1 FOR UPDATE
                        """,
                        snapshot.session_id,
                    )
                    if session is None or str(session["pair_key"]) != snapshot.pair_key:
                        raise PnLAccountingError("PnL snapshot does not match an existing session/pair")
                    await validate_coordinator_lease(
                        conn, coordinator_lease, session_id=snapshot.session_id,
                        account_scope_id=str(session["account_scope_id"]),
                        expected_generation=fencing_generation,
                        session_scope_key=session["coordinator_scope_key"],
                        session_owner_id=session["coordinator_owner_id"],
                    )
                    values = (
                        snapshot.session_id, snapshot.pair_key, snapshot.reporting_currency,
                        snapshot.confidence,
                        datetime.fromtimestamp(snapshot.evaluated_at_ms / 1000, tz=timezone.utc),
                        snapshot.spot_estimated_liquidation_pnl,
                        snapshot.futures_estimated_liquidation_pnl,
                        snapshot.funding_reporting_amount, snapshot.entry_fees_reporting,
                        snapshot.estimated_exit_fees_reporting, snapshot.estimated_tax_reporting,
                        snapshot.conversion_cost_reporting, snapshot.estimated_net_liquidation_pnl,
                        snapshot.strategy_roi_pct, snapshot.return_on_deployed_capital_pct,
                        snapshot.total_deployed_capital_reporting, snapshot.spot_exit_vwap,
                        snapshot.futures_exit_vwap, snapshot.spot_slippage_bps,
                        snapshot.futures_slippage_bps, snapshot.spot_executable_base_quantity,
                        snapshot.futures_executable_base_quantity,
                        snapshot.hedge_drift_base_quantity, _json(snapshot.provenance), key,
                    )
                    inserted = await conn.fetchrow(
                        """
                        INSERT INTO public.execution_pnl_snapshots (
                            session_id,pair_key,reporting_currency,confidence,evaluated_at,
                            spot_estimated_liquidation_pnl,futures_estimated_liquidation_pnl,
                            funding_reporting_amount,entry_fees_reporting,
                            estimated_exit_fees_reporting,estimated_tax_reporting,
                            conversion_cost_reporting,estimated_net_liquidation_pnl,
                            strategy_roi_pct,return_on_deployed_capital_pct,
                            total_deployed_capital_reporting,spot_exit_vwap,futures_exit_vwap,
                            spot_slippage_bps,futures_slippage_bps,
                            spot_executable_base_quantity,futures_executable_base_quantity,
                            hedge_drift_base_quantity,provenance,idempotency_key
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                            $16,$17,$18,$19,$20,$21,$22,$23,$24::jsonb,$25
                        )
                        ON CONFLICT (session_id,idempotency_key) DO NOTHING
                        RETURNING pnl_snapshot_id
                        """,
                        *values,
                    )
                    if inserted is not None:
                        return {"duplicate": False, "pnl_snapshot_id": str(inserted["pnl_snapshot_id"])}
                    prior = await conn.fetchrow(
                        """
                        SELECT pnl_snapshot_id, pair_key, reporting_currency, confidence,
                               evaluated_at, spot_estimated_liquidation_pnl,
                               futures_estimated_liquidation_pnl, funding_reporting_amount,
                               entry_fees_reporting, estimated_exit_fees_reporting,
                               estimated_tax_reporting, conversion_cost_reporting,
                               estimated_net_liquidation_pnl, strategy_roi_pct,
                               return_on_deployed_capital_pct, total_deployed_capital_reporting,
                               spot_exit_vwap, futures_exit_vwap, spot_slippage_bps,
                               futures_slippage_bps, spot_executable_base_quantity,
                               futures_executable_base_quantity, hedge_drift_base_quantity,
                               provenance
                        FROM public.execution_pnl_snapshots
                        WHERE session_id=$1 AND idempotency_key=$2
                        """,
                        snapshot.session_id, key,
                    )
                    expected_snapshot = {
                        "pair_key": snapshot.pair_key,
                        "reporting_currency": snapshot.reporting_currency,
                        "confidence": snapshot.confidence,
                        "evaluated_at": values[4],
                        "spot_estimated_liquidation_pnl": snapshot.spot_estimated_liquidation_pnl,
                        "futures_estimated_liquidation_pnl": snapshot.futures_estimated_liquidation_pnl,
                        "funding_reporting_amount": snapshot.funding_reporting_amount,
                        "entry_fees_reporting": snapshot.entry_fees_reporting,
                        "estimated_exit_fees_reporting": snapshot.estimated_exit_fees_reporting,
                        "estimated_tax_reporting": snapshot.estimated_tax_reporting,
                        "conversion_cost_reporting": snapshot.conversion_cost_reporting,
                        "estimated_net_liquidation_pnl": snapshot.estimated_net_liquidation_pnl,
                        "strategy_roi_pct": snapshot.strategy_roi_pct,
                        "return_on_deployed_capital_pct": snapshot.return_on_deployed_capital_pct,
                        "total_deployed_capital_reporting": snapshot.total_deployed_capital_reporting,
                        "spot_exit_vwap": snapshot.spot_exit_vwap,
                        "futures_exit_vwap": snapshot.futures_exit_vwap,
                        "spot_slippage_bps": snapshot.spot_slippage_bps,
                        "futures_slippage_bps": snapshot.futures_slippage_bps,
                        "spot_executable_base_quantity": snapshot.spot_executable_base_quantity,
                        "futures_executable_base_quantity": snapshot.futures_executable_base_quantity,
                        "hedge_drift_base_quantity": snapshot.hedge_drift_base_quantity,
                        "provenance": snapshot.provenance,
                    }
                    if prior is not None and isinstance(prior["provenance"], str):
                        prior_provenance = json.loads(prior["provenance"])
                    else:
                        prior_provenance = prior["provenance"] if prior is not None else None
                    if prior is None or any(
                        (
                            Decimal(str(prior[name])) != value
                            if isinstance(value, Decimal)
                            else (
                                prior[name] != value
                                if name == "evaluated_at"
                                else (
                                    prior_provenance != value
                                    if name == "provenance"
                                    else str(prior[name]) != str(value)
                                )
                            )
                        )
                        for name, value in expected_snapshot.items()
                    ):
                        raise PnLAccountingError("snapshot idempotency key was reused with changed valuation")
                    return {"duplicate": True, "pnl_snapshot_id": str(prior["pnl_snapshot_id"])}

        async def record_position_projection(
            self, *, session_id: str, reporting_currency: str,
            actual_futures_margin_reporting: Any | None,
            fee_reserve_reporting: Any,
            exchange_snapshot_source: str | None,
            exchange_snapshot_id: str | None,
            exchange_observed_at: datetime | None,
            spot_exchange_base_quantity: Any | None,
            futures_exchange_base_quantity: Any | None,
            expected_position_version: int | None,
            coordinator_lease: Any, fencing_generation: int,
            hedge_drift_tolerance: Any = "0.00000001",
        ) -> dict:
            """Rebuild one position projection from durable leg fills and a verified venue snapshot."""
            currency = str(reporting_currency or "").strip().upper()
            if not currency or coordinator_lease is None:
                raise PnLAccountingError("reporting currency and coordinator lease are required")
            tolerance = _d(hedge_drift_tolerance, "hedge_drift_tolerance")
            reserve = _d(fee_reserve_reporting, "fee_reserve_reporting")
            margin = (
                _d(actual_futures_margin_reporting, "actual_futures_margin_reporting")
                if actual_futures_margin_reporting is not None else None
            )
            actual_spot = (
                _d(spot_exchange_base_quantity, "spot_exchange_base_quantity")
                if spot_exchange_base_quantity is not None else None
            )
            actual_futures = (
                _d(futures_exchange_base_quantity, "futures_exchange_base_quantity")
                if futures_exchange_base_quantity is not None else None
            )
            source = str(exchange_snapshot_source or "").strip()
            snapshot_id = str(exchange_snapshot_id or "").strip()
            if any(value is not None for value in (actual_spot, actual_futures, exchange_observed_at)):
                if actual_spot is None or actual_futures is None or not source or not snapshot_id or not isinstance(exchange_observed_at, datetime):
                    raise PnLAccountingError("exchange exposure snapshot requires both legs, source, ID and timestamp")
                if exchange_observed_at.tzinfo is None:
                    raise PnLAccountingError("exchange position snapshot timestamp must be timezone-aware")
                snapshot_age = (datetime.now(timezone.utc) - exchange_observed_at).total_seconds()
                if snapshot_age < -5 or snapshot_age > 60:
                    raise PnLAccountingError("exchange position snapshot is stale or from the future")
            if expected_position_version is not None and (
                isinstance(expected_position_version, bool)
                or not isinstance(expected_position_version, int)
                or expected_position_version < 0
            ):
                raise PnLAccountingError("expected_position_version must be a non-negative integer")
    
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    session = await conn.fetchrow(
                        """
                        SELECT pair_key, account_scope_id, coordinator_scope_key, coordinator_owner_id
                        FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                        """,
                        session_id,
                    )
                    if session is None:
                        raise PnLAccountingError("position session does not exist")
                    await validate_coordinator_lease(
                        conn, coordinator_lease, session_id=session_id,
                        account_scope_id=str(session["account_scope_id"]),
                        expected_generation=fencing_generation,
                        session_scope_key=session["coordinator_scope_key"],
                        session_owner_id=session["coordinator_owner_id"],
                    )
                    leg_rows = await conn.fetch(
                        """
                        SELECT leg_type, base_asset,
                               COALESCE(SUM(confirmed_exposure_quantity),0) AS entry_base,
                               COALESCE(SUM(confirmed_exit_exposure_quantity),0) AS exit_base
                        FROM public.execution_legs
                        WHERE session_id=$1
                        GROUP BY leg_type, base_asset
                        """,
                        session_id,
                    )
                    by_type = {str(row["leg_type"]).upper(): row for row in leg_rows}
                    if "SPOT" not in by_type or "FUTURES" not in by_type:
                        raise PnLAccountingError("position requires both Spot and Futures leg projections")
                    spot_row, futures_row = by_type["SPOT"], by_type["FUTURES"]
                    if str(spot_row["base_asset"]).upper() != str(futures_row["base_asset"]).upper():
                        raise PnLAccountingError("Spot and Futures base assets do not match")
                    spot_entry = Decimal(str(spot_row["entry_base"]))
                    spot_exit = Decimal(str(spot_row["exit_base"]))
                    futures_entry = Decimal(str(futures_row["entry_base"]))
                    futures_exit = Decimal(str(futures_row["exit_base"]))
                    if spot_exit > spot_entry or futures_exit > futures_entry:
                        raise PnLAccountingError("exit fill projection exceeds confirmed entry exposure")
                    spot_remaining = spot_entry - spot_exit
                    futures_remaining = futures_entry - futures_exit
                    hedge_drift = abs(spot_remaining - futures_remaining)
                    reconciliation = "UNKNOWN"
                    if actual_spot is not None and actual_futures is not None:
                        reconciliation = (
                            "SYNCED"
                            if abs(actual_spot - spot_remaining) <= tolerance
                            and abs(actual_futures - futures_remaining) <= tolerance
                            else "MISMATCH"
                        )
                    if hedge_drift > tolerance and reconciliation == "SYNCED":
                        reconciliation = "MISMATCH"
                    if reconciliation != "SYNCED":
                        position_status = "RECONCILIATION_REQUIRED"
                    elif spot_remaining == 0 and futures_remaining == 0:
                        position_status = "CLOSED"
                    elif spot_exit > 0 or futures_exit > 0:
                        position_status = "PARTIALLY_CLOSED"
                    else:
                        position_status = "OPEN"
    
                    current = await conn.fetchrow(
                        """
                        SELECT position_id, state_version, last_verified_at,
                               exchange_snapshot_id, exchange_snapshot_source,
                               spot_exchange_base_quantity, futures_exchange_base_quantity,
                               reconciliation_status
                        FROM public.execution_positions WHERE session_id=$1 FOR UPDATE
                        """,
                        session_id,
                    )
                    if current is not None:
                        if snapshot_id and str(current["exchange_snapshot_id"] or "") == snapshot_id:
                            if (
                                current["spot_exchange_base_quantity"] is not None
                                and current["futures_exchange_base_quantity"] is not None
                                and Decimal(str(current["spot_exchange_base_quantity"])) == actual_spot
                                and Decimal(str(current["futures_exchange_base_quantity"])) == actual_futures
                                and str(current["reconciliation_status"]) == reconciliation
                                and str(current["exchange_snapshot_source"] or "") == source
                                and current["last_verified_at"] == exchange_observed_at
                            ):
                                return {
                                    "duplicate": True, "position_id": str(current["position_id"]),
                                    "state_version": int(current["state_version"]),
                                    "status": position_status, "reconciliation_status": reconciliation,
                                }
                            raise PnLAccountingError("exchange snapshot ID was replayed with changed exposure")
                        if expected_position_version is None or int(current["state_version"]) != int(expected_position_version):
                            raise PnLAccountingError("position projection state version is stale")
                        if (
                            exchange_observed_at is not None
                            and current["last_verified_at"] is not None
                            and exchange_observed_at < current["last_verified_at"]
                        ):
                            raise PnLAccountingError("stale exchange position snapshot cannot move verification time backward")
                        new_version = int(current["state_version"]) + 1
                    else:
                        if expected_position_version not in (None, 0):
                            raise PnLAccountingError("initial position projection expected version must be zero")
                        new_version = 1
    
                    row = await conn.fetchrow(
                        """
                        INSERT INTO public.execution_positions (
                            session_id,account_scope_id,pair_key,base_asset,reporting_currency,
                            spot_entry_base_quantity,spot_exit_base_quantity,spot_remaining_base_quantity,
                            futures_entry_base_quantity,futures_exit_base_quantity,futures_remaining_base_quantity,
                            hedge_drift_base_quantity,actual_futures_margin_reporting,fee_reserve_reporting,
                            status,state_version,last_verified_at,spot_exchange_base_quantity,
                            futures_exchange_base_quantity,reconciliation_status,
                            exchange_snapshot_source,exchange_snapshot_id,updated_at
                        ) VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                            $15,$16,$17,$18,$19,$20,$21,$22,clock_timestamp()
                        )
                        ON CONFLICT (session_id) DO UPDATE SET
                            account_scope_id=EXCLUDED.account_scope_id,
                            pair_key=EXCLUDED.pair_key,base_asset=EXCLUDED.base_asset,
                            reporting_currency=EXCLUDED.reporting_currency,
                            spot_entry_base_quantity=EXCLUDED.spot_entry_base_quantity,
                            spot_exit_base_quantity=EXCLUDED.spot_exit_base_quantity,
                            spot_remaining_base_quantity=EXCLUDED.spot_remaining_base_quantity,
                            futures_entry_base_quantity=EXCLUDED.futures_entry_base_quantity,
                            futures_exit_base_quantity=EXCLUDED.futures_exit_base_quantity,
                            futures_remaining_base_quantity=EXCLUDED.futures_remaining_base_quantity,
                            hedge_drift_base_quantity=EXCLUDED.hedge_drift_base_quantity,
                            actual_futures_margin_reporting=EXCLUDED.actual_futures_margin_reporting,
                            fee_reserve_reporting=EXCLUDED.fee_reserve_reporting,
                            status=EXCLUDED.status,state_version=EXCLUDED.state_version,
                            last_verified_at=COALESCE(EXCLUDED.last_verified_at,execution_positions.last_verified_at),
                            spot_exchange_base_quantity=EXCLUDED.spot_exchange_base_quantity,
                            futures_exchange_base_quantity=EXCLUDED.futures_exchange_base_quantity,
                            reconciliation_status=EXCLUDED.reconciliation_status,
                            exchange_snapshot_source=EXCLUDED.exchange_snapshot_source,
                            exchange_snapshot_id=EXCLUDED.exchange_snapshot_id,
                            updated_at=clock_timestamp()
                        RETURNING position_id,state_version,status,reconciliation_status
                        """,
                        session_id, str(session["account_scope_id"]), str(session["pair_key"]),
                        str(spot_row["base_asset"]).upper(), currency,
                        spot_entry, spot_exit, spot_remaining, futures_entry, futures_exit,
                        futures_remaining, hedge_drift, margin, reserve, position_status,
                        new_version, exchange_observed_at, actual_spot, actual_futures,
                        reconciliation, source or None, snapshot_id or None,
                    )
                    return {
                        "duplicate": False, "position_id": str(row["position_id"]),
                        "state_version": int(row["state_version"]), "status": str(row["status"]),
                        "reconciliation_status": str(row["reconciliation_status"]),
                        "spot_remaining_base_quantity": str(spot_remaining),
                        "futures_remaining_base_quantity": str(futures_remaining),
                        "hedge_drift_base_quantity": str(hedge_drift),
                    }
