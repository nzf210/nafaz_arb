import pytest

from scripts.resolve_ai_position import (
    _asset_total,
    _order_fully_filled,
    _position_contracts,
)


def test_spot_balance_total_includes_locked_used_asset():
    assert _asset_total({"free": {"AI": 0.0}, "used": {"AI": 2.5}}, "AI") == pytest.approx(2.5)


def test_terminal_full_fill_is_required_before_cleanup():
    assert _order_fully_filled({"status": "closed", "filled": 1.0}, 1.0)
    assert not _order_fully_filled({"status": "open", "filled": 1.0}, 1.0)
    assert not _order_fully_filled({"status": "closed", "filled": 0.99}, 1.0)
    assert not _order_fully_filled({"status": "closed", "filled": "invalid"}, 1.0)


def test_position_side_is_not_inferred_from_positive_contract_count():
    long_qty, short_qty = _position_contracts([
        {"side": "long", "contracts": 3.0},
        {"side": "short", "contracts": 2.0},
    ])
    assert long_qty == pytest.approx(3.0)
    assert short_qty == pytest.approx(2.0)


def test_unknown_nonzero_position_side_fails_closed():
    with pytest.raises(ValueError, match="side is missing"):
        _position_contracts([{"contracts": 1.0}])
