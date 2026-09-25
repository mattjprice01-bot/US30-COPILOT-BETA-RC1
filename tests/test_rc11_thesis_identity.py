from server import _market_thesis_id


def _result(*, ts, side="LONG", entry_low=52434.8, entry_high=52438.0, stop=52421.8, tp2=52480.0):
    return {
        "strategy": "scalp",
        "symbol": "OANDA:US30USD",
        "signal": side,
        "ts": ts,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "tp2": tp2,
    }


def test_same_directional_thesis_survives_time_and_level_drift():
    first = _result(ts=178? if False else 1800000000000)
    later = _result(
        ts=1800000540000,
        entry_low=52233.0,
        entry_high=52238.5,
        stop=52210.9,
        tp2=52290.0,
    )
    assert _market_thesis_id(first) == _market_thesis_id(later)


def test_direction_change_is_a_different_thesis():
    long_id = _market_thesis_id(_result(ts=1800000000000, side="LONG"))
    short_id = _market_thesis_id(_result(ts=1800000000000, side="SHORT"))
    assert long_id != short_id


def test_wait_has_no_thesis_id():
    result = _result(ts=1800000000000)
    result["signal"] = "WAIT"
    assert _market_thesis_id(result) is None
