"""Verified funding and matched-fill realized PnL persistence."""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from executor.execution_repository_fencing import validate_coordinator_lease
from executor.execution_pnl_common import PnLAccountingError, _d, _signed_d, _json


class ExecutionPnLLedgerMixin:
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
        signed_amount = _signed_d(amount, "funding.amount")
        valued_amount = _signed_d(reporting_amount, "funding.reporting_amount")
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
