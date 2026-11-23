"""V3 position valuation persistence and fill-matched realized PnL ledger."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from executor.execution_repository import ExecutionRepositoryError
from executor.execution_repository_fencing import validate_coordinator_lease
from executor.pair_pnl_engine import PairPnLSnapshotV3


class PnLAccountingError(ExecutionRepositoryError):
    """PnL evidence is incomplete, inconsistent, or not owned by this session."""


def _d(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PnLAccountingError(f"{name} must be a finite decimal") from exc
    if not result.is_finite() or (result <= 0 if positive else False):
        raise PnLAccountingError(f"{name} must be {'positive' if positive else 'finite'}")
    return result


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class ExecutionPnLAccountingService:
    """Persists immutable estimated valuations and verified realized fill matches."""

    def __init__(self, pool):
        if pool is None:
            raise ValueError("PostgreSQL pool is required")
        self.pool = pool

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
                           exchange_snapshot_id, spot_exchange_base_quantity,
                           futures_exchange_base_quantity, reconciliation_status
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
                        last_verified_at=COALESCE(EXCLUDED.last_verified_at,public.execution_positions.last_verified_at),
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

    async def record_funding_event(
        self, *, session_id: str, futures_leg_id: str, account_scope_id: str,
        venue: str, market_type: str, symbol: str, exchange_payment_id: str,
        amount: Any, asset: str, reporting_amount: Any,
        reporting_currency: str, fx_rate: Any, fx_source: str,
        fx_observed_at: Any, settled_at: Any, raw_payload: dict | None,
        coordinator_lease: Any, fencing_generation: int,
    ) -> dict:
        """Append a verified exchange funding settlement and its PnL ledger entry."""
        payment_id = str(exchange_payment_id or "").strip()
        account = str(account_scope_id or "").strip()
        venue_name = str(venue or "").strip().lower()
        market = str(market_type or "").strip().lower()
        instrument = str(symbol or "").strip()
        currency = str(asset or "").strip().upper()
        reporting = str(reporting_currency or "").strip().upper()
        source = str(fx_source or "").strip()
        if not all((payment_id, account, venue_name, market, instrument, currency, reporting, source)):
            raise PnLAccountingError("funding event identity and FX provenance are required")
        signed_amount = _d(amount, "funding.amount")
        valued_amount = _d(reporting_amount, "funding.reporting_amount")
        rate = _d(fx_rate, "funding.fx_rate", positive=True)
        if coordinator_lease is None:
            raise PnLAccountingError("funding event requires coordinator lease")
        if not isinstance(fx_observed_at, datetime) or not isinstance(settled_at, datetime):
            raise PnLAccountingError("funding FX observation and settlement timestamps are required")
        raw = _json(raw_payload or {})

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await conn.fetchrow(
                    """
                    SELECT account_scope_id, coordinator_scope_key, coordinator_owner_id
                    FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                if session is None or str(session["account_scope_id"]) != account:
                    raise PnLAccountingError("funding account scope does not match session")
                await validate_coordinator_lease(
                    conn, coordinator_lease, session_id=session_id,
                    account_scope_id=account, expected_generation=fencing_generation,
                    session_scope_key=session["coordinator_scope_key"],
                    session_owner_id=session["coordinator_owner_id"],
                )
                leg = await conn.fetchrow(
                    """
                    SELECT leg_type, venue, market_type, symbol
                    FROM public.execution_legs
                    WHERE session_id=$1 AND leg_id=$2
                    """,
                    session_id, futures_leg_id,
                )
                if leg is None or (
                    str(leg["leg_type"]).upper() != "FUTURES"
                    or str(leg["venue"]).lower() != venue_name
                    or str(leg["market_type"]).lower() != market
                    or str(leg["symbol"]) != instrument
                ):
                    raise PnLAccountingError("funding event does not match the session Futures leg")
                if abs((settled_at - fx_observed_at).total_seconds()) > 300:
                    raise PnLAccountingError("funding FX evidence is stale relative to settlement")

                event = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_funding_events (
                        session_id,futures_leg_id,account_scope_id,venue,market_type,
                        symbol,exchange_payment_id,amount,asset,reporting_amount,
                        reporting_currency,fx_rate,fx_source,fx_observed_at,
                        valuation_verified,settled_at,raw_payload
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                        TRUE,$15,$16::jsonb
                    )
                    ON CONFLICT (account_scope_id,venue,market_type,symbol,exchange_payment_id)
                    DO NOTHING
                    RETURNING funding_event_id
                    """,
                    session_id, futures_leg_id, account, venue_name, market,
                    instrument, payment_id, signed_amount, currency, valued_amount,
                    reporting, rate, source, fx_observed_at, settled_at, raw,
                )
                if event is None:
                    prior = await conn.fetchrow(
                        """
                        SELECT funding_event_id, session_id, amount, reporting_amount,
                               reporting_currency, fx_rate, fx_source
                        FROM public.execution_funding_events
                        WHERE account_scope_id=$1 AND venue=$2 AND market_type=$3
                          AND symbol=$4 AND exchange_payment_id=$5
                        """,
                        account, venue_name, market, instrument, payment_id,
                    )
                    if prior is None or (
                        str(prior["session_id"]) != session_id
                        or Decimal(str(prior["amount"])) != signed_amount
                        or Decimal(str(prior["reporting_amount"])) != valued_amount
                        or str(prior["reporting_currency"]).upper() != reporting
                        or Decimal(str(prior["fx_rate"])) != rate
                        or str(prior["fx_source"]) != source
                    ):
                        raise PnLAccountingError("funding payment identity was replayed with changed valuation")
                    return {"duplicate": True, "funding_event_id": str(prior["funding_event_id"])}

                ledger = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_pnl_ledger (
                        session_id,source_funding_event_id,entry_type,amount_reporting,
                        reporting_currency,evidence_source,idempotency_key,detail,occurred_at
                    ) VALUES (
                        $1,$2::uuid,'FUNDING',$3,$4,'VERIFIED_EXCHANGE_FUNDING',
                        $5,$6::jsonb,$7
                    )
                    ON CONFLICT (session_id,idempotency_key) DO NOTHING
                    RETURNING pnl_entry_id
                    """,
                    session_id, str(event["funding_event_id"]), valued_amount,
                    reporting, "funding:" + payment_id,
                    _json({
                        "exchange_payment_id": payment_id, "amount": str(signed_amount),
                        "asset": currency, "fx_rate": str(rate), "fx_source": source,
                        "fx_observed_at": fx_observed_at.isoformat(),
                    }),
                    settled_at,
                )
                if ledger is None:
                    raise PnLAccountingError("funding PnL append lost an idempotency race")
                return {
                    "duplicate": False, "funding_event_id": str(event["funding_event_id"]),
                    "pnl_entry_id": str(ledger["pnl_entry_id"]),
                    "amount_reporting": str(valued_amount), "reporting_currency": reporting,
                }

    async def record_realized_fill_match(
        self, *, session_id: str, entry_fill_id: str, exit_fill_id: str,
        matched_quantity: Any, reporting_currency: str, idempotency_key: str,
        actor_id: str, coordinator_lease: Any, fencing_generation: int,
        max_fx_age_seconds: int = 300,
    ) -> dict:
        """Match one exit-fill quantity against one entry fill and append realized PnL.

        Matching is quantity-reserved in the ledger: the sum already matched from
        either source fill is locked and cannot exceed that fill's native quantity.
        """
        key = str(idempotency_key or "").strip()
        actor = str(actor_id or "").strip()
        currency = str(reporting_currency or "").strip().upper()
        qty = _d(matched_quantity, "matched_quantity", positive=True)
        if not key or not actor or not currency or coordinator_lease is None:
            raise PnLAccountingError("actor, currency, idempotency key and coordinator lease are required")
        if isinstance(max_fx_age_seconds, bool) or not isinstance(max_fx_age_seconds, int) or max_fx_age_seconds <= 0:
            raise PnLAccountingError("max_fx_age_seconds must be a positive integer")

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                session = await conn.fetchrow(
                    """
                    SELECT account_scope_id, coordinator_scope_key, coordinator_owner_id
                    FROM public.execution_sessions WHERE session_id=$1 FOR UPDATE
                    """,
                    session_id,
                )
                if session is None:
                    raise PnLAccountingError("PnL session does not exist")
                await validate_coordinator_lease(
                    conn, coordinator_lease, session_id=session_id,
                    account_scope_id=str(session["account_scope_id"]),
                    expected_generation=fencing_generation,
                    session_scope_key=session["coordinator_scope_key"],
                    session_owner_id=session["coordinator_owner_id"],
                )
                entry = await conn.fetchrow(
                    """
                    SELECT * FROM public.execution_fills
                    WHERE session_id=$1 AND fill_id=$2::uuid FOR UPDATE
                    """,
                    session_id, entry_fill_id,
                )
                exit_fill = await conn.fetchrow(
                    """
                    SELECT * FROM public.execution_fills
                    WHERE session_id=$1 AND fill_id=$2::uuid FOR UPDATE
                    """,
                    session_id, exit_fill_id,
                )
                if entry is None or exit_fill is None:
                    raise PnLAccountingError("entry/exit fill is missing or belongs to another session")
                if (
                    str(entry["fill_role"]).upper() != "ENTRY"
                    or str(exit_fill["fill_role"]).upper() != "EXIT"
                    or str(entry["leg_id"]) != str(exit_fill["leg_id"])
                    or str(entry["quote_asset"]).upper() != str(exit_fill["quote_asset"]).upper()
                ):
                    raise PnLAccountingError("entry/exit fills do not belong to the same leg and quote asset")
                if exit_fill["filled_at"] < entry["filled_at"]:
                    raise PnLAccountingError("exit fill predates its entry fill")
                if not entry["quote_fx_verified"] or not exit_fill["quote_fx_verified"]:
                    raise PnLAccountingError("entry and exit quote FX must both be verified")
                if (
                    str(entry["fee_reporting_currency"] or "").upper() != currency
                    or str(exit_fill["fee_reporting_currency"] or "").upper() != currency
                ):
                    raise PnLAccountingError("entry and exit fee valuation currency must match reporting currency")
                for fill_name, fill in (("entry", entry), ("exit", exit_fill)):
                    if not fill["fee_verified"]:
                        raise PnLAccountingError(f"{fill_name} fee fact is unverified")
                    if Decimal(str(fill["fee_amount"])) > 0 and not fill["fee_valuation_verified"]:
                        raise PnLAccountingError(f"{fill_name} fee valuation is not verified")
                    if not fill["quote_fx_verified"] or not fill["quote_fx_source"] or not fill["quote_fx_observed_at"]:
                        raise PnLAccountingError(f"{fill_name} quote FX provenance is missing or unverified")
                    delta_seconds = abs((fill["filled_at"] - fill["quote_fx_observed_at"]).total_seconds())
                    if delta_seconds > max_fx_age_seconds:
                        raise PnLAccountingError(f"{fill_name} quote FX is too far from fill time")
                    if fill["fee_amount"] > 0:
                        if not fill["fee_fx_source"] or not fill["fee_fx_observed_at"]:
                            raise PnLAccountingError(f"{fill_name} fee FX provenance is missing")
                        fee_delta = abs((fill["filled_at"] - fill["fee_fx_observed_at"]).total_seconds())
                        if fee_delta > max_fx_age_seconds:
                            raise PnLAccountingError(f"{fill_name} fee FX is too far from fill time")
                if str(entry["fee_reporting_currency"] or "").upper() != currency:
                    raise PnLAccountingError("entry fee reporting currency mismatch")
                if str(exit_fill["fee_reporting_currency"] or "").upper() != currency:
                    raise PnLAccountingError("exit fee reporting currency mismatch")

                prior = await conn.fetchrow(
                    """
                    SELECT pnl_entry_id, amount_reporting, source_fill_id,
                           matched_entry_fill_id, matched_quantity
                    FROM public.execution_pnl_ledger
                    WHERE session_id=$1 AND idempotency_key=$2
                    """,
                    session_id, key,
                )
                if prior is not None:
                    if (
                        str(prior["source_fill_id"]) != str(exit_fill_id)
                        or str(prior["matched_entry_fill_id"]) != str(entry_fill_id)
                        or Decimal(str(prior["matched_quantity"])) != qty
                    ):
                        raise PnLAccountingError("PnL idempotency key was reused with a different fill match")
                    return {"duplicate": True, "pnl_entry_id": str(prior["pnl_entry_id"]),
                            "net_realized_pnl": str(prior["amount_reporting"])}

                entry_matched = await conn.fetchval(
                    """
                    SELECT COALESCE(SUM(matched_quantity),0)
                    FROM public.execution_pnl_ledger
                    WHERE matched_entry_fill_id=$1::uuid
                    """,
                    entry_fill_id,
                )
                exit_matched = await conn.fetchval(
                    """
                    SELECT COALESCE(SUM(matched_quantity),0)
                    FROM public.execution_pnl_ledger
                    WHERE source_fill_id=$1::uuid
                      AND entry_type IN ('REALIZED_SPOT_PNL','REALIZED_FUTURES_PNL')
                    """,
                    exit_fill_id,
                )
                if Decimal(str(entry_matched)) + qty > Decimal(str(entry["quantity"])):
                    raise PnLAccountingError("matched quantity exceeds remaining entry fill quantity")
                if Decimal(str(exit_matched)) + qty > Decimal(str(exit_fill["quantity"])):
                    raise PnLAccountingError("matched quantity exceeds remaining exit fill quantity")

                leg = await conn.fetchrow(
                    """
                    SELECT leg_type, base_per_unit, position_side
                    FROM public.execution_legs
                    WHERE session_id=$1 AND leg_id=$2
                    """,
                    session_id, entry["leg_id"],
                )
                if leg is None:
                    raise PnLAccountingError("matched fill leg is missing")
                leg_type = str(leg["leg_type"]).upper()
                entry_side = str(entry["side"]).lower()
                exit_side = str(exit_fill["side"]).lower()
                if (
                    (leg_type == "SPOT" and (entry_side, exit_side) != ("buy", "sell"))
                    or (leg_type == "FUTURES" and (entry_side, exit_side) != ("sell", "buy"))
                    or leg_type not in {"SPOT", "FUTURES"}
                ):
                    raise PnLAccountingError("fill sides do not represent a valid long Spot/short Futures close")

                entry_ratio = qty / Decimal(str(entry["quantity"]))
                exit_ratio = qty / Decimal(str(exit_fill["quantity"]))
                entry_quote = Decimal(str(entry["quote_quantity"])) * entry_ratio
                exit_quote = Decimal(str(exit_fill["quote_quantity"])) * exit_ratio
                entry_fx = Decimal(str(entry["quote_to_reporting_rate"]))
                exit_fx = Decimal(str(exit_fill["quote_to_reporting_rate"]))
                if leg_type == "SPOT":
                    gross = exit_quote * exit_fx - entry_quote * entry_fx
                    entry_type = "REALIZED_SPOT_PNL"
                else:
                    gross = entry_quote * entry_fx - exit_quote * exit_fx
                    entry_type = "REALIZED_FUTURES_PNL"
                entry_fee = (
                    Decimal(str(entry["fee_reporting_amount"] or 0)) * entry_ratio
                )
                exit_fee = (
                    Decimal(str(exit_fill["fee_reporting_amount"] or 0)) * exit_ratio
                )
                net = gross - entry_fee - exit_fee
                detail = {
                    "actor_id": actor, "entry_fill_id": str(entry_fill_id),
                    "exit_fill_id": str(exit_fill_id), "matched_quantity": str(qty),
                    "matched_base_quantity": str(qty * Decimal(str(leg["base_per_unit"]))),
                    "gross_realized_pnl_reporting": str(gross),
                    "entry_fee_reporting": str(entry_fee),
                    "exit_fee_reporting": str(exit_fee),
                    "entry_quote_fx_rate": str(entry_fx),
                    "entry_quote_fx_source": entry["quote_fx_source"],
                    "exit_quote_fx_rate": str(exit_fx),
                    "exit_quote_fx_source": exit_fill["quote_fx_source"],
                    "entry_fee_source": entry["fee_source"],
                    "exit_fee_source": exit_fill["fee_source"],
                }
                inserted = await conn.fetchrow(
                    """
                    INSERT INTO public.execution_pnl_ledger (
                        session_id, source_fill_id, matched_entry_fill_id,
                        matched_quantity, entry_type, amount_reporting,
                        reporting_currency, evidence_source, idempotency_key,
                        detail, occurred_at
                    ) VALUES (
                        $1,$2::uuid,$3::uuid,$4,$5,$6,$7,
                        'VERIFIED_ENTRY_EXIT_FILL_MATCH',$8,$9::jsonb,$10
                    )
                    ON CONFLICT (session_id,idempotency_key) DO NOTHING
                    RETURNING pnl_entry_id
                    """,
                    session_id, exit_fill_id, entry_fill_id, qty, entry_type,
                    net, currency, key, _json(detail), exit_fill["filled_at"],
                )
                if inserted is None:
                    raise PnLAccountingError("realized PnL append lost an idempotency race")
                return {
                    "duplicate": False, "pnl_entry_id": str(inserted["pnl_entry_id"]),
                    "entry_type": entry_type, "gross_realized_pnl": str(gross),
                    "net_realized_pnl": str(net), "matched_quantity": str(qty),
                    "reporting_currency": currency,
                }
