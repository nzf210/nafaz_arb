"""Durable Supabase recovery query tests."""

import pytest

from executor.supabase_queries import SupabaseQueryMixin


class _Response:
    status = 200

    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self):
        return self._rows


class _Session:
    closed = False

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def get(self, url, params, headers):
        self.calls.append((url, dict(params)))
        return _Response(self.pages.get(int(params.get("offset", "0")), []))


class _Outbox:
    def __init__(self, rows=None):
        self.rows = rows or {}

    def pending_table(self, table):
        return self.rows.get(table, [])


class _Queries(SupabaseQueryMixin):
    enabled = True
    url = "https://supabase.invalid"
    key = "service-role-test-key"

    def __init__(self, pages, outbox):
        self._session = _Session(pages)
        self._outbox = outbox


@pytest.mark.asyncio
async def test_open_order_query_paginates_and_overlays_pending_outbox_intents():
    db_order = {
        "local_order_id": "db-order",
        "status": "open",
        "recovery_status": "MANAGED",
        "is_mock": False,
    }
    pending_new = {
        "local_order_id": "pending-order",
        "status": "submitting",
        "recovery_status": "ADOPTED",
        "is_mock": False,
        "raw_payload": {"recovery_context": {"usdt_idr_rate": 16000}},
    }
    pending_closed = {
        "local_order_id": "db-order",
        "status": "closed",
        "recovery_status": "PHANTOM",
        "is_mock": False,
    }
    pending_mock = {
        "local_order_id": "mock-order",
        "status": "open",
        "recovery_status": "MANAGED",
        "is_mock": True,
    }
    outbox = _Outbox({
        "order_registry": [
            (1, "order_registry", "local_order_id", pending_new),
            (2, "order_registry", "local_order_id", pending_closed),
            (3, "order_registry", "local_order_id", pending_mock),
        ]
    })
    queries = _Queries({0: [db_order], 1: []}, outbox)

    rows = await queries.fetch_open_order_records(limit=1)

    assert {row["local_order_id"] for row in rows} == {"pending-order"}
    assert [params["offset"] for _url, params in queries._session.calls] == ["0", "1"]
    assert all(params["recovery_status"].endswith("PHANTOM,ORPHAN_ORDER)") for _url, params in queries._session.calls)


@pytest.mark.asyncio
async def test_open_position_query_overlays_pending_close_without_resurrection():
    db_position = {
        "pair_key": "indodax:btc_idr__okx:btc_usdt_swap",
        "status": "HEDGED_ACTIVE",
        "is_mock": False,
        "state_snapshot": {"qty": 0.01},
        "updated_at": "2026-11-12T01:00:00+00:00",
    }
    closed = {
        "pair_key": db_position["pair_key"],
        "status": "CLOSED",
        "is_mock": False,
        "state_snapshot": {"qty": 0.0, "state": "CLOSED"},
    }
    outbox = _Outbox({
        "positions": [(4, "positions", "id", closed)]
    })
    queries = _Queries({0: [db_position]}, outbox)

    rows = await queries.fetch_open_position_records()

    assert rows == []
    assert queries._session.calls[0][1]["is_mock"] == "eq.false"
    assert "FAILED" in queries._session.calls[0][1]["status"]
