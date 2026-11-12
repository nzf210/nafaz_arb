import argparse

import pytest

from scripts.live_readonly_preflight import _spot_symbol_arg


def test_readonly_preflight_parses_exchange_scoped_spot_symbol():
    assert _spot_symbol_arg("tokocrypto=BTC/IDR") == ("tokocrypto", "BTC/IDR")


@pytest.mark.parametrize("value", ["BTC/IDR", "=BTC/IDR", "tokocrypto=", "tokocrypto"])
def test_readonly_preflight_rejects_ambiguous_spot_symbol(value):
    with pytest.raises(argparse.ArgumentTypeError):
        _spot_symbol_arg(value)
