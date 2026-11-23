import pytest

from executor.risk_control import _validate_scope_identity


@pytest.mark.parametrize(
    ("scope_type", "identity", "expected"),
    [
        ("GLOBAL", {}, "global"),
        ("ACCOUNT", {"account_scope_id": "acct-1"}, "account:acct-1"),
        (
            "VENUE",
            {"account_scope_id": "acct-1", "venue": "OKX"},
            "venue:acct-1:okx",
        ),
        (
            "INSTRUMENT",
            {
                "account_scope_id": "acct-1",
                "venue": "OKX",
                "market_type": "SWAP",
                "instrument": "BTC-USDT-SWAP",
            },
            "instrument:acct-1:okx:swap:BTC-USDT-SWAP",
        ),
        ("PAIR", {"pair_key": "tokocrypto:btc_idr"}, "pair:tokocrypto:btc_idr"),
        ("SESSION", {"session_id": "session-1"}, "session:session-1"),
    ],
)
def test_risk_scope_key_is_canonical(scope_type, identity, expected):
    _validate_scope_identity(
        scope_key=expected,
        scope_type=scope_type,
        account_scope_id=identity.get("account_scope_id"),
        venue=identity.get("venue"),
        market_type=identity.get("market_type"),
        instrument=identity.get("instrument"),
        pair_key=identity.get("pair_key"),
        session_id=identity.get("session_id"),
    )


@pytest.mark.parametrize(
    ("scope_type", "identity", "wrong_key"),
    [
        ("GLOBAL", {}, "global:wrong"),
        ("ACCOUNT", {"account_scope_id": "acct-1"}, "account:acct-2"),
        (
            "VENUE",
            {"account_scope_id": "acct-1", "venue": "okx"},
            "venue:acct-1:mexc",
        ),
        (
            "INSTRUMENT",
            {
                "account_scope_id": "acct-1",
                "venue": "okx",
                "market_type": "swap",
                "instrument": "BTC-USDT-SWAP",
            },
            "instrument:acct-1:okx:spot:BTC-USDT-SWAP",
        ),
        ("PAIR", {"pair_key": "tokocrypto:btc_idr"}, "pair:other-pair"),
        ("SESSION", {"session_id": "session-1"}, "session:session-2"),
    ],
)
def test_mismatched_risk_scope_key_is_rejected(scope_type, identity, wrong_key):
    with pytest.raises(ValueError, match="does not match"):
        _validate_scope_identity(
            scope_key=wrong_key,
            scope_type=scope_type,
            account_scope_id=identity.get("account_scope_id"),
            venue=identity.get("venue"),
            market_type=identity.get("market_type"),
            instrument=identity.get("instrument"),
            pair_key=identity.get("pair_key"),
            session_id=identity.get("session_id"),
        )


def test_incomplete_risk_scope_identity_is_rejected():
    with pytest.raises(ValueError, match="incomplete identity"):
        _validate_scope_identity(
            scope_key="instrument:acct-1:okx:swap:BTC-USDT-SWAP",
            scope_type="INSTRUMENT",
            account_scope_id="acct-1",
            venue="okx",
            market_type="",
            instrument="BTC-USDT-SWAP",
            pair_key=None,
            session_id=None,
        )
