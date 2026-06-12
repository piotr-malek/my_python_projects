from util.formatting import (
    format_delta_minutes_pm,
    format_minutes_hm,
    format_minutes_pair_hm,
    format_recharge_delta,
    format_recharge_rate,
)


def test_format_minutes_hm_examples():
    assert format_minutes_hm(250) == "4h10m"
    assert format_minutes_hm(45) == "45m"
    assert format_minutes_hm(120) == "2h"
    assert format_minutes_hm(0) == "0m"
    assert format_minutes_hm(411) == "6h51m"
    assert format_minutes_hm(458) == "7h38m"


def test_format_delta_minutes_pm():
    assert format_delta_minutes_pm(-47) == "-47m"
    assert format_delta_minutes_pm(90) == "+1h30m"
    assert format_delta_minutes_pm(0) == "0m"
    assert format_delta_minutes_pm(None) is None


def test_format_minutes_pair_hm():
    assert format_minutes_pair_hm([420, 490]) == ["7h", "8h10m"]
    assert format_minutes_pair_hm(None) is None


def test_format_recharge_rate():
    assert format_recharge_rate(7.728) == "7.7 pts/h"
    assert format_recharge_rate(1.7283950617283952) == "1.7 pts/h"
    assert format_recharge_rate(None) is None


def test_format_recharge_delta():
    assert format_recharge_delta(-5.99) == "-6.0 pts/h"
    assert format_recharge_delta(0.4) == "+0.4 pts/h"
