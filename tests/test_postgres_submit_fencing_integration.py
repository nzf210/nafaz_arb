"""Real PostgreSQL, multi-process crash test for the durable submit fence.

The executor and peer run in separate processes. The test process simulates an
active venue request, terminates the executor's PostgreSQL backend mid-request,
and then releases a simulated venue acceptance. A peer must
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


def _submitter(dsn, client_id, venue_active, venue_release, backend_queue):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        try:
            await service.prepare_submit_attempt(
                client_order_id=client_id, exchange="mexc", market_type="spot",
                symbol="BTC/USDT", side="buy", order_type="limit",
                quantity=0.001, price=60000, pair_key="mexc:BTC_USDT",
            )
            async with service.order_submit_fence(client_order_id=client_id) as conn:
                # The parent process acts as the fake venue and keeps this
                # request open until after it terminates our PostgreSQL backend.
                backend_queue.put(await conn.fetchval("select pg_backend_pid()"))
                venue_active.set()
                await asyncio.to_thread(venue_release.wait, 30)
        except Exception:
            # PostgreSQL backend termination during the in-flight venue call is expected.
            pass
        finally:
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
def test_postgres_disconnect_during_active_venue_request_keeps_peer_fenced():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    ctx = mp.get_context("fork")
    suffix = uuid.uuid4().hex[:16]
    first_id = "pg-crash-" + suffix
    second_id = "pg-peer-" + suffix
    venue_active = ctx.Event()
    venue_release = ctx.Event()
    backend_queue = ctx.Queue()
    result_queue = ctx.Queue()
    submitter = ctx.Process(
        target=_submitter,
        args=(dsn, first_id, venue_active, venue_release, backend_queue),
    )
    peer = None
    submitter.start()
    try:
        assert venue_active.wait(15), "executor never entered the fake venue request"
        backend_pid = backend_queue.get(timeout=5)

        async def terminate_backend():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                return await conn.fetchval("select pg_terminate_backend($1)", backend_pid)
            finally:
                await conn.close()

        assert asyncio.run(terminate_backend()) is True

        async def wait_backend_exit():
            import asyncpg
            deadline = time.monotonic() + 10
            conn = await asyncpg.connect(dsn)
            try:
                while time.monotonic() < deadline:
                    active = await conn.fetchval(
                        "select exists(select 1 from pg_stat_activity where pid=$1)",
                        backend_pid,
                    )
                    if not active:
                        return True
                    await asyncio.sleep(0.1)
                return False
            finally:
                await conn.close()

        assert asyncio.run(wait_backend_exit()), "PostgreSQL backend did not disconnect"
        assert submitter.is_alive(), "executor must remain inside the venue request"

        peer = ctx.Process(
            target=_peer_submitter, args=(dsn, second_id, result_queue)
        )
        peer.start()
        peer.join(15)
        if peer.is_alive():
            peer.terminate()
            peer.join(5)
            pytest.fail("peer did not finish its fenced admission attempt")
        assert peer.exitcode == 0
        outcome = result_queue.get(timeout=3)
        assert outcome.startswith("BLOCKED:")
        assert "unresolved in-flight submit" in outcome

        # The fake venue returns acceptance only after the DB connection is gone.
        venue_release.set()
        submitter.join(15)
        if submitter.is_alive():
            submitter.terminate()
            submitter.join(5)
            pytest.fail("executor did not leave the simulated venue request")
        assert submitter.exitcode == 0
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

        async def cleanup_attempt():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    "delete from public.executor_submit_attempts where client_order_id=$1",
                    first_id,
                )
            finally:
                await conn.close()
        asyncio.run(cleanup_attempt())
    finally:
        venue_release.set()
        if peer is not None and peer.is_alive():
            peer.terminate()
            peer.join(5)
        if submitter.is_alive():
            submitter.terminate()
            submitter.join(5)


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
