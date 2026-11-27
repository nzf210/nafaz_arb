"""Regression tests for pre-submit hedge claim cleanup and spot rollback."""

from types import SimpleNamespace

import pytest

from executor.entry_hedge import EntryHedgeMixin
from executor.global_stop import GlobalStopError


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
