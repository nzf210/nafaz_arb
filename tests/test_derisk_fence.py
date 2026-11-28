"""Regression tests for de-risk fencing fallback semantics."""

from contextlib import asynccontextmanager

import pytest

from executor.derisk_fence import derisk_fence
from executor.global_stop import GlobalStopError


class _BrokenFence:
    def __init__(self, error):
        self.error = error

    async def __aenter__(self):
        raise self.error

    async def __aexit__(self, *exc):
        return False


class _Service:
    def __init__(self, error):
        self.error = error

    def derisk_submit_fence(self, *, client_order_id):
        return _BrokenFence(self.error)


class _Engine:
    def __init__(self, error):
        self.global_stop = _Service(error)


@pytest.mark.asyncio
async def test_missing_global_control_row_does_not_block_risk_reduction():
    engine = _Engine(GlobalStopError("global control row is missing"))

    async with derisk_fence(engine, client_order_id="rollback-missing-control") as conn:
        assert conn is None


class _UndefinedTableError(Exception):
    __module__ = "asyncpg.exceptions"


@pytest.mark.asyncio
async def test_postgres_schema_error_does_not_block_risk_reduction():
    engine = _Engine(_UndefinedTableError("executor_global_control is missing"))

    async with derisk_fence(engine, client_order_id="exit-missing-schema") as conn:
        assert conn is None


class _HealthyService:
    @asynccontextmanager
    async def derisk_submit_fence(self, *, client_order_id):
        yield "shared-derisk-connection"


@pytest.mark.asyncio
async def test_venue_error_inside_derisk_scope_is_not_swallowed():
    engine = _Engine.__new__(_Engine)
    engine.global_stop = _HealthyService()

    with pytest.raises(TimeoutError, match="venue response lost"):
        async with derisk_fence(engine, client_order_id="close-ambiguous"):
            raise TimeoutError("venue response lost")


@pytest.mark.asyncio
async def test_invalid_client_order_id_is_still_rejected_before_fallback():
    engine = _Engine(GlobalStopError("global control row is missing"))

    with pytest.raises(GlobalStopError, match="valid stable client_order_id"):
        async with derisk_fence(engine, client_order_id=""):
            pytest.fail("invalid de-risk identity must not reach venue")
