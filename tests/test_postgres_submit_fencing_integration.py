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

from executor.global_stop import FENCE_LOCK_KEY, GlobalStopError, GlobalStopService
from executor.hedge_claims import HedgeIntentClaimService


def _submitter(
    dsn, client_id, venue_active, venue_release, backend_queue,
    *, exchange="mexc", symbol="BTC/USDT",
    scope_exchange=None, scope_symbol=None,
):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        try:
            await service.prepare_submit_attempt(
                client_order_id=client_id, exchange=exchange, market_type="spot",
                symbol=symbol, side="buy", order_type="limit",
                quantity=0.001, price=60000, pair_key=f"{exchange}:{symbol}",
            )
            async with service.order_submit_fence(
                client_order_id=client_id,
                scope_exchange=scope_exchange,
                scope_symbol=scope_symbol,
            ) as conn:
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




def _derisk_holder(dsn, active_event, release_event, result_queue):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        try:
            async with service.derisk_submit_fence(
                client_order_id="pg-derisk-" + uuid.uuid4().hex[:12]
            ):
                result_queue.put("ACQUIRED")
                active_event.set()
                await asyncio.to_thread(release_event.wait, 30)
        except Exception as exc:
            result_queue.put("ERROR:" + type(exc).__name__ + ":" + str(exc))
        finally:
            await service.close()
    asyncio.run(run())



def _peer_submitter(
    dsn, client_id, result_queue, *,
    exchange="mexc", symbol="ETH/USDT",
    scope_exchange=None, scope_symbol=None,
):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        await service.prepare_submit_attempt(
            client_order_id=client_id, exchange=exchange, market_type="spot",
            symbol=symbol, side="buy", order_type="limit",
            quantity=0.01, price=3000, pair_key=f"{exchange}:{symbol}",
        )
        try:
            async with service.order_submit_fence(
                client_order_id=client_id,
                scope_exchange=scope_exchange,
                scope_symbol=scope_symbol,
            ):
                pass
            await service.mark_submit_acknowledged(
                client_id, {"id": "fake-peer-order", "status": "open"}
            )
            result_queue.put("ADMITTED")
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


def _pair_guard_submitter(
    dsn, client_id, pair_key, shared_claims, entered, release, result_queue
):
    async def run():
        service = GlobalStopService(dsn, min_size=1, max_size=2)
        await service.start()
        try:
            await service.prepare_submit_attempt(
                client_order_id=client_id, exchange="tokocrypto", market_type="spot",
                symbol="BTC/IDR", side="buy", order_type="limit",
                quantity=0.001, price=1_000_000_000, pair_key=pair_key,
            )

            async def pair_guard():
                return pair_key not in shared_claims

            async with service.order_submit_fence(
                client_order_id=client_id,
                pre_submit_guard=pair_guard,
            ):
                # Simulated fake-venue create-order boundary. The shared marker
                # is persisted before the first process releases the PG fence.
                shared_claims[pair_key] = client_id
                result_queue.put("FAKE_VENUE_CREATE_ORDER")
                entered.set()
                await asyncio.to_thread(release.wait, 30)
            await service.mark_submit_acknowledged(
                client_id, {"id": "fake-venue-order", "status": "open"}
            )
            result_queue.put("FIRST_ACKNOWLEDGED")
        except GlobalStopError as exc:
            result_queue.put("BLOCKED:" + str(exc))
        finally:
            await service.close()
    asyncio.run(run())


@pytest.mark.integration
def test_postgres_pair_guard_blocks_second_replica_after_first_entry_ack():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    ctx = mp.get_context("fork")
    manager = ctx.Manager()
    claims = manager.dict()
    pair_key = "tokocrypto:BTC_IDR:" + uuid.uuid4().hex[:8]
    first_id = "pg-pair-first-" + uuid.uuid4().hex[:12]
    second_id = "pg-pair-second-" + uuid.uuid4().hex[:12]
    first_entered = ctx.Event()
    release_first = ctx.Event()
    first_queue = ctx.Queue()
    second_queue = ctx.Queue()
    first = ctx.Process(
        target=_pair_guard_submitter,
        args=(dsn, first_id, pair_key, claims, first_entered, release_first, first_queue),
    )
    second = ctx.Process(
        target=_pair_guard_submitter,
        args=(dsn, second_id, pair_key, claims, ctx.Event(), ctx.Event(), second_queue),
    )
    guard_retry = None
    try:
        first.start()
        assert first_entered.wait(15), "first replica never reached fake venue"
        assert first_queue.get(timeout=3) == "FAKE_VENUE_CREATE_ORDER"

        # A second replica on the same pair may share the global STOP lock,
        # but the durable pair admission guard must reject duplicate exposure.
        second.start()
        second.join(5)
        if second.is_alive():
            second.terminate()
            second.join(5)
            pytest.fail("second replica did not fail fast on the busy submit fence")
        assert second.exitcode == 0
        busy_outcome = second_queue.get(timeout=3)
        assert busy_outcome.startswith("BLOCKED:")
        assert "pre-submit admission guard" in busy_outcome

        release_first.set()
        first.join(15)
        if first.is_alive():
            first.terminate()
            first.join(5)
            pytest.fail("first replica did not leave fake venue boundary")
        assert first.exitcode == 0
        assert first_queue.get(timeout=3) == "FIRST_ACKNOWLEDGED"

        # Once the first entry is acknowledged, a second attempt can acquire
        # the global fence but must still be rejected by the pair-level guard.
        guard_retry = ctx.Process(
            target=_pair_guard_submitter,
            args=(dsn, second_id, pair_key, claims, ctx.Event(), ctx.Event(), second_queue),
        )
        guard_retry.start()
        guard_retry.join(15)
        if guard_retry.is_alive():
            guard_retry.terminate()
            guard_retry.join(5)
            pytest.fail("pair guard retry did not finish after first acknowledgement")
        assert guard_retry.exitcode == 0
        guard_outcome = second_queue.get(timeout=3)
        assert guard_outcome.startswith("BLOCKED:")
        assert "pre-submit admission guard" in guard_outcome

        async def statuses():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                return await conn.fetch(
                    "select client_order_id, status from public.executor_submit_attempts "
                    "where client_order_id = any($1::text[])",
                    [first_id, second_id],
                )
            finally:
                await conn.close()

        rows = asyncio.run(statuses())
        by_id = {row["client_order_id"]: row["status"] for row in rows}
        assert by_id[first_id] == "ACKNOWLEDGED"
        assert by_id[second_id] == "PREPARED"
    finally:
        release_first.set()
        for process in (guard_retry, second, first):
            if process is not None and process.is_alive():
                process.terminate()
                process.join(5)

        async def cleanup():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    "delete from public.executor_submit_attempts "
                    "where client_order_id = any($1::text[])",
                    [first_id, second_id],
                )
            finally:
                await conn.close()
        asyncio.run(cleanup())
        manager.shutdown()




def _hedge_claim_worker(dsn, parent_order_id, barrier, result_queue):
    async def run():
        stop = GlobalStopService(dsn, min_size=1, max_size=1)
        await stop.start()
        try:
            claims = HedgeIntentClaimService(global_stop=stop)
            await asyncio.to_thread(barrier.wait, 10)
            claim = await claims.claim_generation(
                parent_spot_exchange="indodax",
                parent_spot_order_id=parent_order_id,
                parent_spot_client_order_id="spot-client-" + parent_order_id,
                cumulative_spot_filled=0.004,
                target_hedge_qty=0.004,
                residual_qty=0.004,
                hedge_ratio=1.0,
                futures_exchange="okx",
                futures_symbol="BTC/USDT:USDT",
                claimed_by="postgres-integration-worker",
            )
            result_queue.put(claim["claim_key"] if claim else None)
        finally:
            await stop.close()
    asyncio.run(run())


@pytest.mark.integration
def test_postgres_hedge_claim_is_atomic_across_processes():
    """Two independent processes must not claim one Spot-fill generation."""
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    ctx = mp.get_context("fork")
    parent_order_id = "pg-hedge-claim-" + uuid.uuid4().hex[:16]
    barrier = ctx.Barrier(2)
    results = ctx.Queue()
    workers = [
        ctx.Process(
            target=_hedge_claim_worker,
            args=(dsn, parent_order_id, barrier, results),
        )
        for _ in range(2)
    ]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(15)
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
                pytest.fail("hedge claim worker did not finish")
            assert worker.exitcode == 0

        outcomes = [results.get(timeout=3), results.get(timeout=3)]
        winners = [key for key in outcomes if key]
        assert len(winners) == 1, f"exactly one PostgreSQL claim must win: {outcomes!r}"

    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)

        async def cleanup():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    "delete from public.executor_hedge_intent_claims "
                    "where parent_spot_order_id=$1",
                    parent_order_id,
                )
            finally:
                await conn.close()
        asyncio.run(cleanup())

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


@pytest.mark.integration
def test_postgres_scoped_unresolved_submit_blocks_only_same_venue_symbol_after_disconnect():
    """A real PG disconnect preserves same-scope blocking without freezing unrelated scopes."""
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    ctx = mp.get_context("fork")
    suffix = uuid.uuid4().hex[:12]
    first_id = "pg-scoped-first-" + suffix
    other_id = "pg-scoped-other-" + suffix
    same_id = "pg-scoped-same-" + suffix
    venue_active = ctx.Event()
    venue_release = ctx.Event()
    backend_queue = ctx.Queue()
    other_queue = ctx.Queue()
    same_queue = ctx.Queue()
    submitter = ctx.Process(
        target=_submitter,
        args=(dsn, first_id, venue_active, venue_release, backend_queue),
        kwargs={
            "exchange": "mexc",
            "symbol": "DODO_USDT",
            "scope_exchange": "mexc",
            "scope_symbol": "DODO_USDT",
        },
    )
    other_peer = None
    same_peer = None
    try:
        submitter.start()
        assert venue_active.wait(15), "scoped executor never entered fake venue request"
        backend_pid = backend_queue.get(timeout=5)

        async def terminate_backend():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                return await conn.fetchval("select pg_terminate_backend($1)", backend_pid)
            finally:
                await conn.close()

        assert asyncio.run(terminate_backend()) is True
        assert submitter.is_alive(), "executor must remain inside fake venue request"

        other_peer = ctx.Process(
            target=_peer_submitter,
            args=(dsn, other_id, other_queue),
            kwargs={
                "exchange": "okx",
                "symbol": "BTC_USDT",
                "scope_exchange": "okx",
                "scope_symbol": "BTC_USDT",
            },
        )
        other_peer.start()
        other_peer.join(15)
        if other_peer.is_alive():
            other_peer.terminate()
            other_peer.join(5)
            pytest.fail("unrelated scoped peer did not finish")
        assert other_peer.exitcode == 0
        assert other_queue.get(timeout=3) == "ADMITTED"

        same_peer = ctx.Process(
            target=_peer_submitter,
            args=(dsn, same_id, same_queue),
            kwargs={
                "exchange": "mexc",
                "symbol": "DODO_USDT",
                "scope_exchange": "mexc",
                "scope_symbol": "DODO_USDT",
            },
        )
        same_peer.start()
        same_peer.join(15)
        if same_peer.is_alive():
            same_peer.terminate()
            same_peer.join(5)
            pytest.fail("same-scope peer did not finish")
        assert same_peer.exitcode == 0
        outcome = same_queue.get(timeout=3)
        assert outcome.startswith("BLOCKED:")
        assert "unresolved in-flight submit" in outcome

        venue_release.set()
        submitter.join(15)
        if submitter.is_alive():
            submitter.terminate()
            submitter.join(5)
            pytest.fail("scoped submitter did not leave fake venue request")
        assert submitter.exitcode == 0

        async def statuses():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                return await conn.fetch(
                    "select client_order_id, status from public.executor_submit_attempts "
                    "where client_order_id = any($1::text[])",
                    [first_id, other_id, same_id],
                )
            finally:
                await conn.close()

        rows = {row["client_order_id"]: row["status"] for row in asyncio.run(statuses())}
        assert rows[first_id] == "SUBMITTING"
        assert rows[other_id] == "ACKNOWLEDGED"
        assert rows[same_id] == "PREPARED"
    finally:
        venue_release.set()
        for process in (same_peer, other_peer, submitter):
            if process is not None and process.is_alive():
                process.terminate()
                process.join(5)

        async def cleanup():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    "delete from public.executor_submit_attempts "
                    "where client_order_id = any($1::text[])",
                    [first_id, other_id, same_id],
                )
            finally:
                await conn.close()
        asyncio.run(cleanup())


def test_spot_slot_advisory_key_is_stable_and_scope_specific():
    from executor.order_slots import OrderSlotManager

    first = OrderSlotManager._slot_lock_key("indodax", "idr")
    second = OrderSlotManager._slot_lock_key("indodax", "idr")
    other_account = OrderSlotManager._slot_lock_key("indodax", "usdt")
    other_venue = OrderSlotManager._slot_lock_key("tokocrypto", "idr")

    assert first == second
    assert first != other_account
    assert first != other_venue


@pytest.mark.integration
def test_postgres_module_slot_override_is_shared_between_independent_managers():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    async def run():
        import asyncpg
        from executor.order_slots import OrderSlotManager, SlotExhaustedError

        suffix = uuid.uuid4().hex[:12]
        venue = "audit-slot-" + suffix
        account = "idr"
        client_ids = ["slot-a-" + suffix, "slot-b-" + suffix]
        pool_a = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1)
        pool_b = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1)
        manager_a = OrderSlotManager(pool_a)
        manager_b = OrderSlotManager(pool_b)

        async def reserve(manager, client_id):
            try:
                slot = await manager.reserve_slot(
                    venue,
                    account=account,
                    client_order_id=client_id,
                    max_slots_override=1,
                )
                return ("RESERVED", slot)
            except SlotExhaustedError:
                return ("EXHAUSTED", None)

        try:
            outcomes = await asyncio.gather(
                reserve(manager_a, client_ids[0]),
                reserve(manager_b, client_ids[1]),
            )
            assert sorted(item[0] for item in outcomes) == ["EXHAUSTED", "RESERVED"]
            assert next(item[1] for item in outcomes if item[0] == "RESERVED") == 0
        finally:
            try:
                async with pool_a.acquire() as conn:
                    await conn.execute(
                        "delete from public.order_slot_reservations where client_order_id = any($1::text[])",
                        client_ids,
                    )
            finally:
                await pool_a.close()
                await pool_b.close()

    asyncio.run(run())


@pytest.mark.integration
def test_postgres_scoped_submit_rejects_scope_not_matching_durable_intent():
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    async def run():
        import asyncpg

        service = GlobalStopService(dsn, min_size=1, max_size=1)
        await service.start()
        suffix = uuid.uuid4().hex[:12]
        unresolved_id = "pg-scope-intent-" + suffix
        wrong_scope_id = "pg-scope-wrong-" + suffix
        try:
            await service.prepare_submit_attempt(
                client_order_id=unresolved_id,
                exchange="mexc",
                market_type="spot",
                symbol="DODO_USDT",
                side="buy",
                order_type="limit",
                quantity=1.0,
                price=0.1,
                pair_key="mexc:DODO_USDT",
            )
            async with service.order_submit_fence(
                client_order_id=unresolved_id,
                scope_exchange="mexc",
                scope_symbol="DODO_USDT",
            ):
                pass

            await service.prepare_submit_attempt(
                client_order_id=wrong_scope_id,
                exchange="mexc",
                market_type="spot",
                symbol="DODO_USDT",
                side="buy",
                order_type="limit",
                quantity=2.0,
                price=0.1,
                pair_key="mexc:DODO_USDT:second",
            )
            with pytest.raises(GlobalStopError, match="scope does not match"):
                async with service.order_submit_fence(
                    client_order_id=wrong_scope_id,
                    scope_exchange="mexc",
                    scope_symbol="BTC_USDT",
                ):
                    pytest.fail("mismatched scope must never reach the venue boundary")

            conn = await asyncpg.connect(dsn)
            try:
                statuses = await conn.fetch(
                    "select client_order_id, status from public.executor_submit_attempts "
                    "where client_order_id = any($1::text[])",
                    [unresolved_id, wrong_scope_id],
                )
                status_map = {row["client_order_id"]: row["status"] for row in statuses}
                assert status_map[unresolved_id] == "SUBMITTING"
                assert status_map[wrong_scope_id] == "PREPARED"
            finally:
                await conn.close()
        finally:
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    "delete from public.executor_submit_attempts "
                    "where client_order_id = any($1::text[])",
                    [unresolved_id, wrong_scope_id],
                )
            finally:
                await conn.close()
            await service.close()

    asyncio.run(run())


@pytest.mark.integration
def test_postgres_derisk_lane_is_not_blocked_by_active_entry_fence():
    """An open entry HTTP request must not hold up a risk-reducing submit lane."""
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    ctx = mp.get_context("fork")
    suffix = uuid.uuid4().hex[:12]
    entry_id = "pg-entry-held-" + suffix
    venue_active = ctx.Event()
    venue_release = ctx.Event()
    derisk_active = ctx.Event()
    derisk_release = ctx.Event()
    backend_queue = ctx.Queue()
    derisk_queue = ctx.Queue()
    entry = ctx.Process(
        target=_submitter,
        args=(dsn, entry_id, venue_active, venue_release, backend_queue),
        kwargs={"exchange": "mexc", "symbol": "DODO_USDT_" + suffix},
    )
    derisk = ctx.Process(
        target=_derisk_holder,
        args=(dsn, derisk_active, derisk_release, derisk_queue),
    )
    try:
        entry.start()
        assert venue_active.wait(15), "entry process never entered its fake venue request"
        derisk.start()
        assert derisk_active.wait(5), "de-risk lane waited behind the active entry fence"
        assert derisk_queue.get(timeout=3) == "ACQUIRED"
    finally:
        derisk_release.set()
        venue_release.set()
        for process in (derisk, entry):
            if process.is_alive():
                process.join(10)
            if process.is_alive():
                process.terminate()
                process.join(5)

        async def cleanup():
            import asyncpg
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    "delete from public.executor_submit_attempts where client_order_id=$1",
                    entry_id,
                )
            finally:
                await conn.close()
        asyncio.run(cleanup())

    assert entry.exitcode == 0
    assert derisk.exitcode == 0


@pytest.mark.integration
def test_postgres_venue_slot_override_is_shared_across_quote_accounts():
    """A per-venue cap must span IDR and USDT reservations across managers."""
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    async def run():
        import asyncpg
        from executor.order_slots import OrderSlotManager, SlotExhaustedError

        suffix = uuid.uuid4().hex[:12]
        venue = "audit-venue-slot-" + suffix
        client_ids = ["venue-slot-idr-" + suffix, "venue-slot-usdt-" + suffix]
        pool_a = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1)
        pool_b = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1)
        manager_a = OrderSlotManager(pool_a)
        manager_b = OrderSlotManager(pool_b)
        try:
            await manager_a.reserve_slot(
                venue,
                account="idr",
                client_order_id=client_ids[0],
                max_slots_override=2,
                venue_max_slots_override=1,
            )
            with pytest.raises(SlotExhaustedError):
                await manager_b.reserve_slot(
                    venue,
                    account="usdt",
                    client_order_id=client_ids[1],
                    max_slots_override=2,
                    venue_max_slots_override=1,
                )
        finally:
            try:
                async with pool_a.acquire() as conn:
                    await conn.execute(
                        "delete from public.order_slot_reservations where client_order_id = any($1::text[])",
                        client_ids,
                    )
            finally:
                await pool_a.close()
                await pool_b.close()

    asyncio.run(run())


@pytest.mark.integration
def test_postgres_slot_sweep_preserves_live_and_unresolved_reservations():
    """The stale sweep cannot free a live submit slot or strand stale PREPARED slots."""
    dsn = os.getenv("TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_POSTGRES_DSN is not configured")

    async def run():
        import asyncpg
        from executor.order_slots import OrderSlotManager
        from executor.global_stop import FENCE_LOCK_KEY, GlobalStopService

        suffix = uuid.uuid4().hex[:12]
        venue = "audit-slot-ledger-" + suffix
        client_ids = [
            "slot-prepared-" + suffix,
            "slot-submitting-" + suffix,
            "slot-active-" + suffix,
        ]
        service = GlobalStopService(dsn, min_size=1, max_size=2)
        await service.start()
        manager = OrderSlotManager(service.pool)
        lock_conn = None
        try:
            await manager.set_max_slots(venue, 3, account="default")
            for index, client_id in enumerate(client_ids[:2]):
                await service.prepare_submit_attempt(
                    client_order_id=client_id,
                    exchange=venue,
                    market_type="spot",
                    symbol=f"COIN{index}/USDT",
                    side="buy",
                    order_type="limit",
                    quantity=1.0,
                    price=1.0,
                    pair_key=f"{venue}:COIN{index}/USDT",
                )
            for client_id in client_ids:
                await manager.reserve_slot(
                    venue,
                    account="idr",
                    client_order_id=client_id,
                    max_slots_override=4,
                    venue_max_slots_override=4,
                )

            async with service.order_submit_fence(
                client_order_id=client_ids[1],
                scope_exchange=venue,
                scope_symbol="COIN1/USDT",
            ):
                pass

            async with service.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE public.order_slot_reservations "
                    "SET created_at = now() - interval '2 minutes' "
                    "WHERE client_order_id = any($1::text[])",
                    client_ids,
                )
                await conn.execute(
                    "UPDATE public.executor_submit_attempts "
                    "SET created_at = now() - interval '2 minutes' "
                    "WHERE client_order_id = any($1::text[])",
                    client_ids[:2],
                )

            # A PREPARED intent may still belong to a live admission that has
            # acquired the shared fence but has not crossed into SUBMITTING yet.
            lock_conn = await service.pool.acquire()
            await lock_conn.execute("select pg_advisory_lock($1)", FENCE_LOCK_KEY)
            await manager.reconcile_active_slots(
                {client_ids[2]}, max_reserved_age_seconds=1
            )
            async with service.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT client_order_id, status FROM public.order_slot_reservations "
                    "WHERE client_order_id = any($1::text[]) AND account = 'default'",
                    client_ids,
                )
            status = {row["client_order_id"]: row["status"] for row in rows}
            assert status == {client_id: "RESERVED" for client_id in client_ids}

            await lock_conn.execute("select pg_advisory_unlock($1)", FENCE_LOCK_KEY)
            await service.pool.release(lock_conn)
            lock_conn = None

            # With the submit fence free, stale PREPARED is provably pre-submit
            # and may be aborted; SUBMITTING and active caller IDs stay reserved.
            await manager.reconcile_active_slots(
                {client_ids[2]}, max_reserved_age_seconds=1
            )
            async with service.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT client_order_id, status FROM public.order_slot_reservations "
                    "WHERE client_order_id = any($1::text[]) AND account = 'default'",
                    client_ids,
                )
                attempts = await conn.fetch(
                    "SELECT client_order_id, status FROM public.executor_submit_attempts "
                    "WHERE client_order_id = any($1::text[])",
                    client_ids[:2],
                )
            status = {row["client_order_id"]: row["status"] for row in rows}
            attempt_status = {row["client_order_id"]: row["status"] for row in attempts}
            assert status[client_ids[0]] == "RELEASED"
            assert status[client_ids[1]] == "RESERVED"
            assert status[client_ids[2]] == "RESERVED"
            assert attempt_status[client_ids[0]] == "ABORTED"
            assert attempt_status[client_ids[1]] == "SUBMITTING"

            # The third reservation has no ledger row; the active caller ID is
            # its protection. Once the caller no longer owns it, it can expire.
            await manager.reconcile_active_slots(set(), max_reserved_age_seconds=1)
            async with service.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT client_order_id, status FROM public.order_slot_reservations "
                    "WHERE client_order_id = any($1::text[]) AND account = 'default'",
                    client_ids,
                )
            status = {row["client_order_id"]: row["status"] for row in rows}
            assert status[client_ids[1]] == "RESERVED"
            assert status[client_ids[2]] == "RELEASED"
        finally:
            if lock_conn is not None:
                try:
                    await lock_conn.execute("select pg_advisory_unlock($1)", FENCE_LOCK_KEY)
                finally:
                    await service.pool.release(lock_conn)
            async with service.pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM public.order_slot_reservations "
                    "WHERE client_order_id = any($1::text[])",
                    client_ids,
                )
                await conn.execute(
                    "DELETE FROM public.executor_submit_attempts "
                    "WHERE client_order_id = any($1::text[])",
                    client_ids[:2],
                )
                await conn.execute(
                    "DELETE FROM public.spot_order_slot_config WHERE venue = $1",
                    venue,
                )
            await service.close()

    asyncio.run(run())
