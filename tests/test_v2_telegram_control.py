from types import SimpleNamespace

import pytest

from executor.v2_config import ConfigSnapshot
from executor.v2_config_runtime import AtomicConfigSnapshot, PostgresEntryAdmissionGate
from executor.v2_operator_control import (
    OperatorControlError,
    PostgresOperatorControlRepository,
    StartReadinessReport,
    TelegramIdentity,
    TelegramPolicy,
)
from executor.v2_telegram_bot import TelegramControlService


def _policy():
    return TelegramPolicy.from_snapshot({
        "telegram": {
            "enabled": True,
            "bot_token_env": "NAFAZ_TELEGRAM_TOKEN",
            "allowed_user_ids": [101],
            "allowed_chat_ids": [-10012345],
            "start_confirmation_ttl_seconds": 300,
        }
    })


def _config(*, signals_enabled=False):
    return ConfigSnapshot(
        version=12,
        schema_version=2,
        content_sha256="a" * 64,
        snapshot={"execution": {"signals_enabled": signals_enabled}},
    )


def _update(update_id, text, *, user_id=101, chat_id=-10012345):
    return {
        "update_id": update_id,
        "message": {
            "text": text,
            "from": {"id": user_id},
            "chat": {"id": chat_id},
        },
    }


def test_telegram_policy_requires_both_allowlists_and_accepts_group_chat_ids():
    policy = _policy()
    assert policy.authorize(TelegramIdentity(101, -10012345, 1))
    assert not policy.authorize(TelegramIdentity(101, 55, 1))
    assert not policy.authorize(TelegramIdentity(102, -10012345, 1))

    with pytest.raises(OperatorControlError, match="allowed_chat_ids"):
        TelegramPolicy.from_snapshot({
            "telegram": {
                "enabled": True,
                "bot_token_env": "NAFAZ_TELEGRAM_TOKEN",
                "allowed_user_ids": [101],
                "allowed_chat_ids": [],
            }
        })


def test_telegram_policy_rejects_zero_ids_and_unbounded_confirmation_ttl():
    base = {
        "enabled": True,
        "bot_token_env": "NAFAZ_TELEGRAM_TOKEN",
        "allowed_user_ids": [101],
        "allowed_chat_ids": [-10012345],
    }
    invalid_id = dict(base, allowed_chat_ids=[0])
    with pytest.raises(OperatorControlError, match="invalid ID"):
        TelegramPolicy.from_snapshot({"telegram": invalid_id})
    invalid_ttl = dict(base, start_confirmation_ttl_seconds=10)
    with pytest.raises(OperatorControlError, match="TTL"):
        TelegramPolicy.from_snapshot({"telegram": invalid_ttl})


def test_start_readiness_requires_every_explicit_gate():
    report = StartReadinessReport.build({
        "database": True,
        "config_version": True,
        "redis_snapshot": True,
        "credentials": True,
        "market_metadata": True,
        "live_fx": True,
        "reconciliation": True,
        "risk": True,
        "kill_switch": True,
        "python_sole_consumer": True,
        "signals_enabled": True,
    })
    assert report.ready
    assert not StartReadinessReport.build({"database": True}).ready
    assert "python_sole_consumer" in StartReadinessReport.build({}).failed


class _Repository:
    def __init__(self):
        self.rejected = []
        self.start_request = None
        self.stop_request = None
        self.confirm_request = None
        self.status = {
            "state": "STOPPED",
            "trading_enabled": False,
            "state_version": 0,
            "config_version": None,
            "active_config_version": 12,
            "open_order_stop_policy": "keep_and_manage",
            "last_reason": "safe initial state",
            "live_executor_processes": 1,
        }

    async def record_rejected(self, identity, command, reason, error):
        self.rejected.append((identity, command, reason, error))

    async def record_status_command(self, identity):
        return self.status

    async def request_start(self, identity, **kwargs):
        self.start_request = (identity, kwargs)
        return {"duplicate": False, "status": "PENDING_CONFIRMATION"}

    async def stop(self, identity, **kwargs):
        self.stop_request = (identity, kwargs)
        return {"duplicate": False, "state": "STOPPED"}

    async def confirm_start(self, identity, **kwargs):
        self.confirm_request = (identity, kwargs)
        return {"duplicate": False, "status": "FAILED", "error": "START_READINESS_FAILED",
                "readiness": kwargs["readiness"].as_dict()}


class _ConfigRepository:
    async def fetch_active(self):
        return _config()


@pytest.mark.asyncio
async def test_unauthorized_user_or_chat_is_audited_and_never_runs_command():
    repository = _Repository()
    service = TelegramControlService(repository, _ConfigRepository())
    reply = await service.handle_update(
        _update(1, "/trade_stop unauthorized", user_id=999),
        _policy(),
        "test-token",
    )
    assert "ditolak" in reply.lower()
    assert repository.rejected[0][3] == "TELEGRAM_ALLOWLIST_DENIED"
    assert repository.stop_request is None


@pytest.mark.asyncio
async def test_trade_start_creates_actor_bound_confirmation_and_does_not_enable_trading():
    repository = _Repository()
    service = TelegramControlService(repository, _ConfigRepository())
    reply = await service.handle_update(
        _update(2, "/trade_start reviewed maintenance window"),
        _policy(),
        "test-token",
    )
    assert "/trade_confirm " in reply
    identity, arguments = repository.start_request
    assert identity == TelegramIdentity(101, -10012345, 2)
    assert arguments["config_version"] == 12
    assert arguments["ttl_seconds"] == 300
    assert len(arguments["challenge_code"]) >= 24


@pytest.mark.asyncio
async def test_trade_stop_uses_keep_and_manage_and_never_closes_positions():
    repository = _Repository()
    service = TelegramControlService(repository, _ConfigRepository())
    reply = await service.handle_update(
        _update(3, "/trade_stop operator requested pause"),
        _policy(),
        "test-token",
    )
    assert "STOPPED" in reply
    assert "tidak ditutup otomatis" in reply
    assert repository.stop_request[1] == {
        "reason": "operator requested pause",
        "open_order_policy": "keep_and_manage",
    }


@pytest.mark.asyncio
async def test_trade_confirm_is_fail_closed_when_readiness_provider_is_not_wired():
    repository = _Repository()
    service = TelegramControlService(repository, _ConfigRepository())
    reply = await service.handle_update(
        _update(4, "/trade_confirm " + "x" * 40),
        _policy(),
        "test-token",
    )
    assert "gagal secara fail-closed" in reply
    assert repository.confirm_request is not None
    readiness = repository.confirm_request[1]["readiness"]
    assert not readiness.ready
    assert repository.confirm_request[1]["signals_enabled"] is False


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_):
        return False


class _Cache:
    def __init__(self, error=None):
        self.error = error
        self.calls = 0

    async def verify(self, config):
        self.calls += 1
        if self.error:
            raise self.error


class _FenceConnection:
    def __init__(self, *, consumers=None, state="RUNNING"):
        self.consumers = consumers or [{
            "instance_id": "executor-1",
            "status": "ACTIVE",
            "acknowledged_version": 12,
            "acknowledged_sha256": "a" * 64,
        }]
        self.state = state
        self.state_version = 8
        self.lock_acquired = False
        self.executed = []

    def transaction(self):
        return _Transaction()

    async def execute(self, query, *args):
        self.executed.append((query, args))
        if "pg_advisory_xact_lock" in query:
            self.lock_acquired = True
        if "update public.operator_control_state" in query:
            self.state = "STOPPED"
            self.state_version = args[0]

    async def fetchrow(self, query, *args):
        return {
            "state": self.state,
            "trading_enabled": self.state == "RUNNING",
            "state_version": self.state_version,
            "config_version": 12,
            "active_version": 12,
            "active_sha256": "a" * 64,
        }

    async def fetch(self, query, *args):
        return self.consumers


class _FencePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


@pytest.mark.asyncio
async def test_entry_submission_fence_holds_shared_database_lock_until_exit():
    connection = _FenceConnection()
    config = _config(signals_enabled=True)
    gate = PostgresEntryAdmissionGate(
        _FencePool(connection),
        AtomicConfigSnapshot(config),
        SimpleNamespace(),
        cache=_Cache(),
    )

    async with gate.entry_submission_fence() as report:
        assert report.converged
        assert connection.lock_acquired


@pytest.mark.asyncio
async def test_entry_submission_fence_rejects_stopped_or_stale_consumer():
    stopped = PostgresEntryAdmissionGate(
        _FencePool(_FenceConnection(state="STOPPED")),
        AtomicConfigSnapshot(_config(signals_enabled=True)),
        SimpleNamespace(),
        cache=_Cache(),
    )
    with pytest.raises(Exception, match="not RUNNING"):
        async with stopped.entry_submission_fence():
            pytest.fail("stopped executor must not enter")

    stale_connection = _FenceConnection(consumers=[{
        "instance_id": "executor-2",
        "status": "ACTIVE",
        "acknowledged_version": 11,
        "acknowledged_sha256": "b" * 64,
    }])
    stale = PostgresEntryAdmissionGate(
        _FencePool(stale_connection),
        AtomicConfigSnapshot(_config(signals_enabled=True)),
        SimpleNamespace(),
        cache=_Cache(),
    )
    with pytest.raises(Exception, match="convergence"):
        async with stale.entry_submission_fence():
            pytest.fail("stale executor must not enter")



@pytest.mark.asyncio
async def test_executor_restart_durably_resets_trading_to_stopped():
    connection = _FenceConnection(state="RUNNING")
    repository = PostgresOperatorControlRepository(_FencePool(connection))

    result = await repository.enforce_stopped_on_startup("executor-restarted")

    assert result["state"] == "STOPPED"
    assert result["trading_enabled"] is False
    assert result["actor"] == "system"
    assert "executor-restarted" in result["reason"]
    assert connection.state == "STOPPED"
    assert connection.state_version == 9
    assert connection.lock_acquired
    assert any(
        "update public.operator_actions" in query
        and "EXECUTOR_RESTART_REQUIRES_OPERATOR_START" in query
        for query, _ in connection.executed
    )
    assert any(
        "operator_control.startup_stopped" in query
        for query, _ in connection.executed
    )
