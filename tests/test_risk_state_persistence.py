import asyncio
from types import SimpleNamespace

import pytest

from executor.risk_state import RiskStateMixin
from executor.system_readiness import ActionPermission, SystemReadinessGate


class _RiskOutbox:
    enabled = True

    def __init__(self):
        self.failed = False
        self.events = []

    def risk_state(self, **kwargs):
        self.events.append(kwargs)
        return True

    def mark_risk_persistence_failure(self):
        self.failed = True

    def risk_persistence_failed(self):
        return self.failed

    def clear_risk_persistence_failure(self):
        self.failed = False


class _AsyncPostgresRisk:
    enabled = True

    def __init__(self, *, gate=None, result=True):
        self.gate = gate
        self.result = result
        self.failed = False

    async def persist_risk_state(self, **kwargs):
        if self.gate is not None:
            await self.gate.wait()
        return self.result

    def mark_risk_persistence_failure(self):
        self.failed = True

    def risk_persistence_failed(self):
        return self.failed

    def clear_risk_persistence_failure(self):
        self.failed = False


class _RiskHarness(RiskStateMixin):
    def __init__(self, *, postgres_risk, supabase=None):
        self.config = SimpleNamespace(mock_mode=False, max_daily_loss_usdt=0.0)
        self.postgres_risk = postgres_risk
        self.supabase = supabase
        self._risk_persistence_failed = False
        self._risk_persistence_pending = False
        self._risk_persistence_pending_count = 0
        self._risk_persistence_tasks = set()
        self._entry_blocked = False
        self._trading_halted = False
        self._halt_reason = ""
        self._daily_realized_loss_usdt = 0.0
        self._loss_day = "2026-11-22"
        self._risk_state_restored = True
        self._recovery_complete = True
        self._exchange_halted = False
        self.restored = []

    def _utc_day(self):
        return "2026-11-22"

    def _roll_loss_day_if_needed(self):
        return None

    def _restore_risk_record(self, row):
        self.restored.append(row)

    def _restore_daily_loss_record(self, value):
        return None


@pytest.mark.asyncio
async def test_pending_postgres_risk_write_blocks_entry_until_durable_completion():
    gate = asyncio.Event()
    outbox = _RiskOutbox()
    engine = _RiskHarness(
        postgres_risk=_AsyncPostgresRisk(gate=gate),
        supabase=outbox,
    )

    assert engine._persist_risk_state(
        "exchange_halt", scope="exchange", exchange="okx", reason="rate limit"
    ) is True
    assert engine._risk_persistence_pending is True
    assert len(outbox.events) == 1

    report = SystemReadinessGate().evaluate_readiness(engine)
    assert report.permission == ActionPermission.READ_AND_PROTECT_ONLY
    assert "RISK_PERSISTENCE_PENDING" in report.blocking_reasons

    gate.set()
    assert await engine._flush_risk_state_persistence() is True
    assert engine._risk_persistence_pending is False
    assert engine._risk_persistence_failed is False


@pytest.mark.asyncio
async def test_failed_postgres_risk_write_persists_failure_latch_and_blocks_entry():
    outbox = _RiskOutbox()
    engine = _RiskHarness(
        postgres_risk=_AsyncPostgresRisk(result=False),
        supabase=outbox,
    )

    engine._persist_risk_state("trading_halt", scope="global", reason="test")
    assert await engine._flush_risk_state_persistence() is False
    assert engine._risk_persistence_pending is False
    assert engine._risk_persistence_failed is True
    assert engine._entry_blocked is True
    assert outbox.failed is True

    report = SystemReadinessGate().evaluate_readiness(engine)
    assert report.permission == ActionPermission.READ_AND_PROTECT_ONLY
    assert "RISK_PERSISTENCE_UNAVAILABLE" in report.blocking_reasons


@pytest.mark.asyncio
async def test_postgres_restore_failure_uses_supabase_and_starts_protect_only():
    class _UnavailablePostgres(_AsyncPostgresRisk):
        async def fetch_active_risk_states(self, *, is_mock=True):
            raise RuntimeError("postgres unavailable")

    class _SupabaseFallback(_RiskOutbox):
        async def fetch_active_risk_states(self, *, is_mock=True):
            return [{
                "state_id": "risk-state-1",
                "state_type": "exchange_halt",
                "scope": "exchange",
                "exchange": "okx",
                "pair_key": None,
                "reason": "restored from durable fallback",
                "value": {"source": "test"},
                "updated_at": "2026-11-22T00:00:00+00:00",
            }]

        async def fetch_daily_realized_loss_usdt(self, loss_day, *, is_mock=True):
            return 12.5

    outbox = _SupabaseFallback()
    engine = _RiskHarness(
        postgres_risk=_UnavailablePostgres(result=True),
        supabase=outbox,
    )

    await engine._restore_risk_state()

    assert engine._risk_state_restored is True
    assert engine._risk_persistence_failed is True
    assert engine._trading_halted is True
    assert engine._entry_blocked is True
    assert engine._daily_realized_loss_usdt == 12.5
    assert len(engine.restored) == 1
    assert engine.restored[0]["state_id"] == "risk-state-1"


def test_risk_persistence_failure_latch_survives_process_restart(tmp_path):
    latch = tmp_path / "risk-persistence-failed.json"
    first = _RiskHarness(
        postgres_risk=_AsyncPostgresRisk(),
        supabase=_RiskOutbox(),
    )
    first.config.risk_persistence_latch_path = str(latch)
    first._mark_risk_persistence_failed(
        "trading_halt", RuntimeError("database write failed")
    )
    assert latch.exists()

    restarted = _RiskHarness(
        postgres_risk=_AsyncPostgresRisk(),
        supabase=_RiskOutbox(),
    )
    restarted.config.risk_persistence_latch_path = str(latch)
    restarted._restore_risk_persistence_failure()
    assert restarted._risk_persistence_failed is True
    assert restarted._trading_halted is True
    assert restarted._entry_blocked is True
    assert restarted._clear_risk_persistence_latch() is True
    assert not latch.exists()
