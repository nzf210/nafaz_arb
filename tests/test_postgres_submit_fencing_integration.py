"""Real PostgreSQL, multi-process crash test for the durable submit fence.

The fake venue runs in a separate process. Killing the submitting executor drops
its PostgreSQL session while the venue request remains in flight. A peer must
still be blocked by the committed SUBMITTING ledger row.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import multiprocessing as mp
import os
import time
import uuid

import pytest

from executor.global_stop import GlobalStopError, GlobalStopService


def _fake_venue(request_received, active, release, accepted):
    if not request_received.wait(30):
        return
    active.set()
    if not release.wait(30):
        return
    # This represents the venue accepting the request after the executor died.
    accepted.set()


def _submitter(dsn, client_id, venue_request, venue_active, venue_release):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        await service.prepare_submit_attempt(
            client_order_id=client_id, exchange="mexc", market_type="spot",
            symbol="BTC/USDT", side="buy", order_type="limit",
            quantity=0.001, price=60000, pair_key="mexc:BTC_USDT",
        )
        async with service.order_submit_fence(client_order_id=client_id):
            venue_request.set()
            if not venue_active.wait(10):
                raise RuntimeError("fake venue did not receive request")
            # Keep the session lock and HTTP call active until the parent kills us.
            await asyncio.to_thread(venue_release.wait, 30)
        await service.close()
    asyncio.run(run())


def _peer_submitter(dsn, client_id, result_queue):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        await service.prepare_submit_attempt(
            client_order_id=client_id, exchange="mexc", market_type="spot",
            symbol="ETH/USDT", side="buy", order_type="limit",
            quantity=0.01, price=3000, pair_key="mexc:ETH_USDT",
        )
        try:
            async with service.order_submit_fence(client_order_id=client_id):
                result_queue.put("UNEXPECTEDLY_ADMITTED")
        except GlobalStopError as exc:
            result_queue.put("BLOCKED:" + str(exc))
        finally:
            await service.close()
    asyncio.run(run())


@pytest.mark.integration
def test_process_death_during_live_venue_request_keeps_peer_fenced():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    ctx = mp.get_context("spawn")
    suffix = uuid.uuid4().hex[:16]
    first_id = "pg-crash-" + suffix
    second_id = "pg-peer-" + suffix
    venue_request = ctx.Event()
    venue_active = ctx.Event()
    venue_release = ctx.Event()
    venue_accepted = ctx.Event()
    result_queue = ctx.Queue()

    # A separate venue process remains alive after the executor process dies.
    venue = ctx.Process(
        target=_fake_venue, args=(venue_request, venue_active, venue_release, venue_accepted)
    )
    venue.start()
    submitter = ctx.Process(
        target=_submitter, args=(dsn, first_id, venue_request, venue_active, venue_release)
    )
    submitter.start()
    assert venue_active.wait(10), "fake venue did not receive the in-flight request"
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if not submitter.is_alive():
            pytest.fail("submitter exited before fault injection")
        # The test waits until the durable row has crossed the HTTP boundary.
        async def read_status():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                return await conn.fetchval(
                    "select status from public.executor_submit_attempts where client_order_id=$1",
                    first_id,
                )
            finally:
                await conn.close()
        if asyncio.run(read_status()) == "SUBMITTING":
            break
        time.sleep(0.1)
    else:
        pytest.fail("submitter never committed SUBMITTING")

    # Drop the executor process/connection while the independent venue remains active.
    submitter.terminate()
    submitter.join(10)
    assert submitter.exitcode is not None
    assert venue.is_alive()
    assert not venue_accepted.is_set()

    peer = ctx.Process(
        target=_peer_submitter, args=(dsn, second_id, result_queue)
    )
    peer.start()
    peer.join(15)
    assert peer.exitcode == 0
    outcome = result_queue.get(timeout=3)
    assert outcome.startswith("BLOCKED:")
    assert "unresolved in-flight submit" in outcome

    # The venue can still accept the request after executor death; no peer submit
    # was admitted while that side effect was outstanding.
    venue_release.set()
    venue.join(10)
    assert venue_accepted.is_set()

    async def final_status():
        import asyncpg
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetchval(
                "select status from public.executor_submit_attempts where client_order_id=$1",
                first_id,
            )
        finally:
            await conn.close()
    assert asyncio.run(final_status()) == "SUBMITTING"


@pytest.mark.integration
def test_signed_operator_resolution_requires_auth_and_independent_evidence():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        client_id = "pg-manual-" + uuid.uuid4().hex[:16]
        secret = "test-only-hmac-secret-with-at-least-32-bytes"
        try:
            await service.prepare_submit_attempt(
                client_order_id=client_id, exchange="okx", market_type="perpetual",
                symbol="BTC/USDT:USDT", side="sell", order_type="market",
                quantity=0.001, price=0, pair_key="okx:BTC_USDT",
            )
            async with service.order_submit_fence(client_order_id=client_id):
                pass
            await service.mark_submit_unknown(client_id, "simulated lost acknowledgement")

            payload = {
                "client_order_id": client_id,
                "operator_id": "ci-operator",
                "decision": "PROVEN_NOT_SUBMITTED",
                "venue_evidence_ref": "venue-case:confirmed-no-order",
                "independent_evidence_ref": "support-ticket:independent-confirmation",
                "rationale": "venue support and account ledger independently confirm no order",
                "nonce": uuid.uuid4().hex,
                "issued_at": int(time.time()),
            }
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            signature = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
            bad = {"payload": payload, "signature": "0" * 64}
            with pytest.raises(GlobalStopError, match="signature is invalid"):
                await service.resolve_submit_attempt_signed(bad, secret=secret)

            resolved = await service.resolve_submit_attempt_signed(
                {"payload": payload, "signature": signature}, secret=secret
            )
            assert resolved["status"] == "ABORTED"
            assert resolved["operator_id"] == "ci-operator"

            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                audit = await conn.fetchrow(
                    "select operator_id, decision, venue_evidence_ref, independent_evidence_ref "
                    "from public.executor_submit_resolution_audit where client_order_id=$1",
                    client_id,
                )
                assert audit["operator_id"] == "ci-operator"
                assert audit["decision"] == "PROVEN_NOT_SUBMITTED"
                assert audit["venue_evidence_ref"] != audit["independent_evidence_ref"]
            finally:
                await conn.close()
        finally:
            await service.close()

    asyncio.run(run())
