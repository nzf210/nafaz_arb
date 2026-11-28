"""Regression tests for pre-submit hedge claim cleanup and spot rollback."""

from types import SimpleNamespace

import pytest

from executor.entry_hedge import EntryHedgeMixin
from executor.global_stop import GlobalStopError
from executor.hedge_claims import HedgeClaimUnavailableError
from executor.models import PositionState


class _Claims:
    def __init__(self, *, result=True, error=None):
        self.result = result
        self.error = error
        self.abandoned = []

    async def mark_abandoned(self, claim_key, client_order_id, reason):
        self.abandoned.append((claim_key, client_order_id, reason))
        if self.error is not None:
            raise self.error
        return self.result


class _PreSubmitFailureEngine(EntryHedgeMixin):
    def __init__(self, claims, *, claim_on_submit=True):
        self.config = SimpleNamespace(mock_mode=False)
        self.hedge_claims = claims
        self.claim_on_submit = claim_on_submit
        self.rollback_calls = []
        self.order_failures = []
        self.halted_scopes = []
        self.failed_states = []
        self.entry_gate_refreshed = False
        self.status_publish_count = 0

    async def _assert_hedge_slippage_within_limit(
        self, pair_tag, order_info, unhedged_delta, expected_short_price
    ):
        return None

    async def _submit_entry_hedge_order(
        self, pair_tag, order_info, global_symbol, unhedged_delta, expected_short_price
    ):
        if self.claim_on_submit:
            order_info["hedge_claim_key"] = "claim-current-generation"
            order_info["pending_hedge_client_order_id"] = "hdg-current-generation"
        # Simulate STOP/DB/PREPARED failure before the futures venue boundary.
        order_info["hedge_submit_started"] = False
        raise GlobalStopError("global STOP rejected before futures venue request")

    def _record_order_failure(self, exchange, reason):
        self.order_failures.append((exchange, reason))

    async def _rollback_hedge_failure(self, *args):
        self.rollback_calls.append(args)

    def _halt_exposure_scope(self, pair_tag, reason):
        self.halted_scopes.append((pair_tag, reason))

    def _refresh_entry_gate(self):
        self.entry_gate_refreshed = True

    def _mark_failed(self, pair_tag, code, message, *, detail=None, state=None):
        self.failed_states.append((pair_tag, code, message, detail or {}, state))

    async def _maybe_publish_status(self):
        self.status_publish_count += 1


def _order_info(**overrides):
    result = {
        "global_symbol": "BTC/USDT:USDT",
        "global_exchange": "okx",
        "hedge_submit_started": False,
    }
    result.update(overrides)
    return result


@pytest.mark.asyncio
async def test_pre_submit_failure_abandons_claim_by_persisted_client_id_and_rolls_back():
    claims = _Claims()
    engine = _PreSubmitFailureEngine(claims)
    order_info = _order_info()

    await engine._execute_entry_hedge(
        "tokocrypto:BTC_IDR",
        order_info,
        0.01,
        1_000_000_000.0,
        16_000.0,
    )

    assert len(claims.abandoned) == 1
    claim_key, client_order_id, reason = claims.abandoned[0]
    assert claim_key == "claim-current-generation"
    assert client_order_id == "hdg-current-generation"
    assert "GlobalStopError" in reason
    assert len(engine.rollback_calls) == 1
    assert engine.rollback_calls[0][0] == "tokocrypto:BTC_IDR"
    assert order_info["hedge_submit_started"] is False


@pytest.mark.asyncio
async def test_claim_abandon_database_failure_does_not_block_spot_rollback():
    claims = _Claims(error=ConnectionError("PostgreSQL disconnected"))
    engine = _PreSubmitFailureEngine(claims)
    order_info = _order_info()

    await engine._execute_entry_hedge(
        "tokocrypto:BTC_IDR",
        order_info,
        0.01,
        1_000_000_000.0,
        16_000.0,
    )

    assert len(claims.abandoned) == 1
    assert len(engine.rollback_calls) == 1


@pytest.mark.asyncio
async def test_pre_submit_failure_does_not_abandon_stale_previous_generation_claim():
    claims = _Claims()
    engine = _PreSubmitFailureEngine(claims, claim_on_submit=False)
    order_info = _order_info(
        hedge_claim_key="stale-claim",
        pending_hedge_client_order_id="stale-client-id",
    )

    await engine._execute_entry_hedge(
        "tokocrypto:BTC_IDR",
        order_info,
        0.01,
        1_000_000_000.0,
        16_000.0,
    )

    assert claims.abandoned == []
    assert len(engine.rollback_calls) == 1



class _ClaimUnavailable:
    enabled = True

    async def claim_generation(self, **kwargs):
        raise HedgeClaimUnavailableError(
            "PostgreSQL claim connection lost",
            claim_key="tokocrypto:spot-123:10000000",
        )


class _ClaimContended:
    enabled = True

    async def claim_generation(self, **kwargs):
        return None


class _NeverSubmitVenue:
    def __init__(self):
        self.calls = 0

    async def open_global_short_market(self, *args, **kwargs):
        self.calls += 1
        raise AssertionError("venue create-order must not run without a durable claim")


class _ClaimAdmissionEngine(EntryHedgeMixin):
    def __init__(self, claims):
        self.config = SimpleNamespace(mock_mode=False)
        self.global_stop = SimpleNamespace(enabled=True)
        self.hedge_claims = claims
        self.client = _NeverSubmitVenue()
        self.rollback_calls = []
        self.order_failures = []

    async def _assert_hedge_slippage_within_limit(
        self, pair_tag, order_info, unhedged_delta, expected_short_price
    ):
        return None

    def _record_order_failure(self, exchange, reason):
        self.order_failures.append((exchange, reason))

    async def _rollback_hedge_failure(self, *args):
        self.rollback_calls.append(args)


@pytest.mark.asyncio
async def test_claim_database_outage_halts_pair_for_reconciliation_without_racing_rollback():
    engine = _ClaimAdmissionEngine(_ClaimUnavailable())
    order_info = _order_info(
        local_exchange="tokocrypto",
        order_id="spot-123",
        client_order_id="spot-client-123",
        filled=0.01,
        hedged_short_qty=0.0,
    )

    await engine._execute_entry_hedge(
        "tokocrypto:BTC_IDR",
        order_info,
        0.01,
        1_000_000_000.0,
        16_000.0,
    )

    assert engine.client.calls == 0
    # The claim commit may be ambiguous; an automatic rollback could race a
    # peer that already owns/submitted the futures hedge.
    assert engine.rollback_calls == []
    assert engine.halted_scopes
    assert engine.entry_gate_refreshed is True
    assert order_info["hedge_claim_key"] == "tokocrypto:spot-123:10000000"
    assert engine.failed_states[0][1] == "HEDGE_CLAIM_AUTHORITY_UNAVAILABLE"
    assert engine.failed_states[0][4] == PositionState.RECONCILIATION_REQUIRED
    assert engine.status_publish_count == 1
    assert order_info["hedge_submit_started"] is False


@pytest.mark.asyncio
async def test_peer_owned_claim_waits_without_duplicate_submit_or_rollback():
    engine = _ClaimAdmissionEngine(_ClaimContended())
    order_info = _order_info(
        local_exchange="tokocrypto",
        order_id="spot-456",
        client_order_id="spot-client-456",
        filled=0.01,
        hedged_short_qty=0.0,
    )

    await engine._execute_entry_hedge(
        "tokocrypto:BTC_IDR",
        order_info,
        0.01,
        1_000_000_000.0,
        16_000.0,
    )

    assert engine.client.calls == 0
    assert engine.rollback_calls == []
    assert order_info["hedge_waiting_for_claim"] is True
