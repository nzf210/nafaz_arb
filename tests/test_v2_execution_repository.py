from decimal import Decimal

import pytest

from executor.v2_execution_repository import (
    ExecutionIdempotencyConflict,
    ExecutionIntent,
    ExecutionRepositoryError,
    PostgresExecutionRepository,
    _json_payload,
    _validate_intent,
)


def _intent(**overrides):
    values = {
        "idempotency_key": "route-a:btc:entry:123",
        "route_id": "route_a",
        "venue_id": "spot_a",
        "account_id": "account_a",
        "market_id": "btc_idr",
        "canonical_symbol": "BTC",
        "order_role": "ENTRY",
        "side": "buy",
        "order_type": "limit",
        "client_order_id": "nafaz-entry-123",
        "requested_qty": Decimal("0.001"),
        "limit_price": Decimal("500000000"),
        "is_mock": False,
    }
    values.update(overrides)
    return ExecutionIntent(**values)


class _Transaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.connection.transaction_started += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.connection.committed += 1
        else:
            self.connection.rolled_back += 1
        return False


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return False


class _Connection:
    def __init__(self, *, existing_intent=None, current_state=None, duplicate_event=None):
        self.existing_intent = existing_intent
        self.current_state = current_state
        self.duplicate_event = duplicate_event
        self.transaction_started = 0
        self.committed = 0
        self.rolled_back = 0
        self.journal_inserts = []
        self.outbox_inserts = []
        self.intent_inserted = False

    def transaction(self):
        return _Transaction(self)

    async def fetchrow(self, query, *args):
        if "insert into public.v2_execution_intents" in query:
            if self.existing_intent is not None:
                return None
            self.intent_inserted = True
            return {"intent_id": "11111111-1111-1111-1111-111111111111", "state_version": 0}
        if "select intent_id::text, route_id" in query:
            return self.existing_intent
        if "select journal_id, intent_id::text, event_type, from_state, to_state" in query:
            return self.duplicate_event
        if "insert into public.v2_execution_journal" in query:
            self.journal_inserts.append(args)
            return {"journal_id": 77}
        if "select state, state_version" in query:
            return self.current_state
        if "update public.v2_execution_intents" in query:
            return {"state_version": args[2]}
        raise AssertionError(f"unexpected fetchrow query: {query[:100]}")

    async def execute(self, query, *args):
        if "insert into public.outbox_events" in query:
            self.outbox_inserts.append(args)
            return "INSERT 0 1"
        raise AssertionError(f"unexpected execute query: {query[:100]}")


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


def test_execution_intent_defaults_to_live_admission_gate():
    assert _intent().is_mock is False


def test_intent_validation_requires_decimal_precision_and_limit_price():
    with pytest.raises(ValueError, match="not float"):
        _validate_intent(_intent(requested_qty=0.001))

    with pytest.raises(ValueError, match="require a positive limit_price"):
        _validate_intent(_intent(limit_price=None))


def test_journal_payload_rejects_secrets_and_non_finite_values():
    with pytest.raises(ValueError, match="secret-like"):
        _json_payload({"api_secret": "never-store"})
    with pytest.raises(ValueError, match="finite JSON"):
        _json_payload({"bad": float("nan")})
    assert _json_payload({"quantity": Decimal("0.001")}) == '{"quantity":"0.001"}'


@pytest.mark.asyncio
async def test_create_intent_commits_intent_journal_and_outbox_atomically():
    connection = _Connection()
    repository = PostgresExecutionRepository(_Pool(connection))

    result = await repository.create_intent(
        _intent(),
        payload={"source": "unit-test", "scanner_event_id": "scan-1"},
    )

    assert result["created"] is True
    assert result["state_version"] == 0
    assert result["journal_id"] == 77
    assert connection.transaction_started == 1
    assert connection.committed == 1
    assert connection.rolled_back == 0
    assert len(connection.journal_inserts) == 1
    assert len(connection.outbox_inserts) == 1
    assert connection.outbox_inserts[0][0] == "execution:intent:route-a:btc:entry:123:received"


@pytest.mark.asyncio
async def test_create_intent_replay_returns_existing_intent_without_duplicate_events():
    existing = {
        "intent_id": "11111111-1111-1111-1111-111111111111",
        "route_id": "route_a",
        "venue_id": "spot_a",
        "account_id": "account_a",
        "market_id": "btc_idr",
        "canonical_symbol": "BTC",
        "order_role": "ENTRY",
        "side": "buy",
        "order_type": "limit",
        "client_order_id": "nafaz-entry-123",
        "requested_qty": Decimal("0.001"),
        "limit_price": Decimal("500000000"),
        "is_mock": False,
        "state_version": 0,
    }
    connection = _Connection(existing_intent=existing)
    result = await PostgresExecutionRepository(_Pool(connection)).create_intent(_intent())

    assert result == {
        "intent_id": existing["intent_id"],
        "created": False,
        "state_version": 0,
    }
    assert connection.journal_inserts == []
    assert connection.outbox_inserts == []


@pytest.mark.asyncio
async def test_idempotency_key_reuse_with_different_data_is_rejected():
    existing = {
        "intent_id": "11111111-1111-1111-1111-111111111111",
        "route_id": "route_other",
        "venue_id": "spot_a",
        "account_id": "account_a",
        "market_id": "btc_idr",
        "canonical_symbol": "BTC",
        "order_role": "ENTRY",
        "side": "buy",
        "order_type": "limit",
        "client_order_id": "nafaz-entry-123",
        "requested_qty": Decimal("0.001"),
        "limit_price": Decimal("500000000"),
        "is_mock": False,
        "state_version": 0,
    }
    connection = _Connection(existing_intent=existing)

    with pytest.raises(ExecutionIdempotencyConflict):
        await PostgresExecutionRepository(_Pool(connection)).create_intent(_intent())


@pytest.mark.asyncio
async def test_transition_uses_state_compare_and_swap_and_journals_numeric_fill():
    connection = _Connection(
        current_state={"state": "SUBMITTING", "state_version": 3}
    )
    result = await PostgresExecutionRepository(_Pool(connection)).transition_intent(
        "11111111-1111-1111-1111-111111111111",
        expected_state="SUBMITTING",
        next_state="ACKNOWLEDGED",
        event_key="intent:123:ack",
        event_type="order.acknowledged",
        payload={"exchange": "okx", "order_id": "venue-order-1"},
        quantity=Decimal("0.001"),
        price=Decimal("50000"),
        quote_amount=Decimal("50"),
        fee_amount=Decimal("0.025"),
        asset="USDT",
    )

    assert result["duplicate"] is False
    assert result["state_version"] == 4
    assert result["journal_id"] == 77
    assert connection.committed == 1
    assert len(connection.journal_inserts) == 1
    assert len(connection.outbox_inserts) == 1


@pytest.mark.asyncio
async def test_transition_replay_is_idempotent_and_invalid_transition_is_rejected():
    duplicate = {
        "journal_id": 88,
        "intent_id": "11111111-1111-1111-1111-111111111111",
        "event_type": "order.acknowledged",
        "from_state": "SUBMITTING",
        "to_state": "ACKNOWLEDGED",
        "quantity": None,
        "price": None,
        "quote_amount": None,
        "fee_amount": None,
        "realized_pnl": None,
        "asset": None,
        "exchange_event_at": None,
        "payload": "{}",
    }
    connection = _Connection(duplicate_event=duplicate)
    result = await PostgresExecutionRepository(_Pool(connection)).transition_intent(
        "11111111-1111-1111-1111-111111111111",
        expected_state="SUBMITTING",
        next_state="ACKNOWLEDGED",
        event_key="intent:123:ack",
        event_type="order.acknowledged",
    )
    assert result["duplicate"] is True
    assert result["journal_id"] == 88
    assert connection.current_state is None
    assert connection.outbox_inserts == []

    with pytest.raises(ExecutionIdempotencyConflict, match="different event data"):
        await PostgresExecutionRepository(_Pool(connection)).transition_intent(
            "11111111-1111-1111-1111-111111111111",
            expected_state="SUBMITTING",
            next_state="ACKNOWLEDGED",
            event_key="intent:123:ack",
            event_type="order.rejected",
        )

    with pytest.raises(ExecutionIdempotencyConflict, match="different event data"):
        await PostgresExecutionRepository(_Pool(connection)).transition_intent(
            "11111111-1111-1111-1111-111111111111",
            expected_state="SUBMITTING",
            next_state="ACKNOWLEDGED",
            event_key="intent:123:ack",
            event_type="order.acknowledged",
            payload={"different": "payload"},
        )

    with pytest.raises(ValueError, match="invalid execution state transition"):
        await PostgresExecutionRepository(_Pool(_Connection())).transition_intent(
            "11111111-1111-1111-1111-111111111111",
            expected_state="RECEIVED",
            next_state="FILLED",
            event_key="intent:123:filled",
            event_type="order.filled",
        )


@pytest.mark.asyncio
async def test_transition_state_mismatch_fails_without_writing_journal():
    connection = _Connection(current_state={"state": "FILLED", "state_version": 9})

    with pytest.raises(ExecutionRepositoryError, match="state conflict"):
        await PostgresExecutionRepository(_Pool(connection)).transition_intent(
            "11111111-1111-1111-1111-111111111111",
            expected_state="SUBMITTING",
            next_state="ACKNOWLEDGED",
            event_key="intent:state-conflict",
            event_type="order.acknowledged",
        )

    assert connection.journal_inserts == []
    assert connection.outbox_inserts == []
    assert connection.rolled_back == 1
