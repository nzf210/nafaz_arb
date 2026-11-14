from types import SimpleNamespace

import pytest

from executor.engine import ArbitrageEngine
from executor.v2_config import ConfigSnapshot, ConfigSnapshotError
from executor.v2_config_runtime import (
    AtomicConfigSnapshot,
    PostgresEntryAdmissionGate,
)


class _Gate:
    def __init__(self, error=None):
        self.error = error
        self.calls = 0

    async def require_entry_allowed(self):
        self.calls += 1
        if self.error:
            raise self.error


@pytest.mark.asyncio
async def test_live_entry_denied_when_control_plane_gate_is_not_wired():
    engine = object.__new__(ArbitrageEngine)
    engine.config = SimpleNamespace(mock_mode=False)
    engine.v2_admission_gate = None

    assert await engine._v2_entry_admission_allowed() is False


@pytest.mark.asyncio
async def test_mock_entry_does_not_require_live_control_plane_gate():
    engine = object.__new__(ArbitrageEngine)
    engine.config = SimpleNamespace(mock_mode=True)
    engine.v2_admission_gate = None

    assert await engine._v2_entry_admission_allowed() is True


@pytest.mark.asyncio
async def test_live_entry_requires_gate_success():
    engine = object.__new__(ArbitrageEngine)
    engine.config = SimpleNamespace(mock_mode=False)
    gate = _Gate()
    engine.v2_admission_gate = gate

    assert await engine._v2_entry_admission_allowed() is True
    assert gate.calls == 1


@pytest.mark.asyncio
async def test_live_entry_fails_closed_when_gate_raises():
    engine = object.__new__(ArbitrageEngine)
    engine.config = SimpleNamespace(mock_mode=False)
    gate = _Gate(ConfigSnapshotError("database unavailable"))
    engine.v2_admission_gate = gate

    assert await engine._v2_entry_admission_allowed() is False


class _Connection:
    def __init__(self, row):
        self.row = row

    async def fetchrow(self, _query):
        return self.row


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return False


class _Pool:
    def __init__(self, row):
        self.connection = _Connection(row)

    def acquire(self):
        return _Acquire(self.connection)


class _Cache:
    def __init__(self, error=None):
        self.error = error
        self.calls = 0

    async def verify(self, config):
        self.calls += 1
        if self.error:
            raise self.error


class _Registry:
    def __init__(self, report=None, error=None):
        self.report = report or SimpleNamespace(converged=True)
        self.error = error
        self.calls = 0

    async def require_converged(self, config):
        self.calls += 1
        if self.error:
            raise self.error
        return self.report


def _config(*, signals_enabled=True):
    return ConfigSnapshot(
        version=21,
        schema_version=2,
        content_sha256="f" * 64,
        snapshot={"execution": {"signals_enabled": signals_enabled}},
    )


def _operator_row(**overrides):
    row = {
        "state": "RUNNING",
        "trading_enabled": True,
        "config_version": 21,
        "active_version": 21,
        "active_sha256": "f" * 64,
    }
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_postgres_admission_gate_requires_running_matching_active_config_and_convergence():
    config = _config()
    registry = _Registry()
    gate = PostgresEntryAdmissionGate(
        _Pool(_operator_row()), AtomicConfigSnapshot(config), registry, cache=_Cache()
    )

    report = await gate.require_entry_allowed()

    assert report.converged is True
    assert registry.calls == 1


@pytest.mark.asyncio
async def test_postgres_admission_gate_rejects_stopped_operator():
    gate = PostgresEntryAdmissionGate(
        _Pool(_operator_row(state="STOPPED", trading_enabled=False)),
        AtomicConfigSnapshot(_config()),
        _Registry(),
        cache=_Cache(),
    )
    with pytest.raises(ConfigSnapshotError, match="not RUNNING"):
        await gate.require_entry_allowed()


@pytest.mark.asyncio
async def test_postgres_admission_gate_rejects_stale_snapshot_and_hash():
    gate = PostgresEntryAdmissionGate(
        _Pool(_operator_row(active_version=20)),
        AtomicConfigSnapshot(_config()),
        _Registry(),
        cache=_Cache(),
    )
    with pytest.raises(ConfigSnapshotError, match="active operator config"):
        await gate.require_entry_allowed()

    gate = PostgresEntryAdmissionGate(
        _Pool(_operator_row(active_sha256="0" * 64)),
        AtomicConfigSnapshot(_config()),
        _Registry(),
        cache=_Cache(),
    )
    with pytest.raises(ConfigSnapshotError, match="hash differs"):
        await gate.require_entry_allowed()


@pytest.mark.asyncio
async def test_postgres_admission_gate_rejects_disabled_signals_or_divergent_consumers():
    disabled = PostgresEntryAdmissionGate(
        _Pool(_operator_row()),
        AtomicConfigSnapshot(_config(signals_enabled=False)),
        _Registry(),
        cache=_Cache(),
    )
    with pytest.raises(ConfigSnapshotError, match="signals are disabled"):
        await disabled.require_entry_allowed()

    divergent = PostgresEntryAdmissionGate(
        _Pool(_operator_row()),
        AtomicConfigSnapshot(_config()),
        _Registry(error=ConfigSnapshotError("consumer divergence")),
        cache=_Cache(),
    )
    with pytest.raises(ConfigSnapshotError, match="consumer divergence"):
        await divergent.require_entry_allowed()



@pytest.mark.asyncio
async def test_postgres_admission_gate_fails_closed_when_redis_snapshot_is_unavailable():
    cache = _Cache(ConfigSnapshotError("Redis unavailable"))
    gate = PostgresEntryAdmissionGate(
        _Pool(_operator_row()),
        AtomicConfigSnapshot(_config()),
        _Registry(),
        cache=cache,
    )

    with pytest.raises(ConfigSnapshotError, match="Redis unavailable"):
        await gate.require_entry_allowed()
    assert cache.calls == 1
