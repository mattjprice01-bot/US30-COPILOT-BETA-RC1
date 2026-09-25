import copy

from scoring import aggregate


def _frame(tf="1m", c=100.0):
    return {
        "tf": tf, "o": c - 1, "h": c + 2, "l": c - 2, "c": c,
        "ema20": c - 1, "ema50": c - 2, "ema200": c - 5,
        "rsi": 60, "atr": 10, "vwap": c - 1,
        "swing_hi": c + 20, "swing_lo": c - 20, "c3": c - 2,
        "prev_o": c - 1, "prev_c": c - 0.5,
    }


def _ym_frame(tf="1m", c=42000.0):
    return {
        "tf": tf, "c": c, "ema20": c - 5, "ema50": c - 10,
        "rsi": 60, "atr": 20, "c3": c - 10, "vol": 150, "vma20": 100,
    }


def test_ym_shadow_does_not_change_live_signal_math():
    base = {
        "symbol": "OANDA:US30USD", "exchange": "OANDA", "ts": 1_800_000_000_000,
        "frames": [_frame("1m"), _frame("5m"), _frame("15m"), _frame("1h"), _frame("4h")],
    }
    with_futures = copy.deepcopy(base)
    with_futures["futures"] = {
        "symbol": "CBOT_MINI:YM1!",
        "frames": [_ym_frame("1m"), _ym_frame("5m"), _ym_frame("15m"), _ym_frame("1h"), _ym_frame("4h")],
    }

    a = aggregate(base, strategy="scalp")
    b = aggregate(with_futures, strategy="scalp")

    for key in ("signal", "confidence", "weighted_score", "entry_low", "entry_high", "stop", "tp1", "tp2"):
        assert a[key] == b[key]

    assert a["ym_futures_shadow"]["available"] is False
    assert b["ym_futures_shadow"]["available"] is True
    assert b["ym_futures_shadow"]["shadow"] is True
    assert b["ym_futures_shadow"]["symbol"] == "CBOT_MINI:YM1!"


def test_intraday_ym_shadow_is_also_non_executing():
    base = {
        "symbol": "OANDA:US30USD", "exchange": "OANDA", "ts": 1_800_000_000_000,
        "frames": [_frame("5m"), _frame("15m"), _frame("30m"), _frame("1h"), _frame("4h")],
    }
    with_futures = copy.deepcopy(base)
    with_futures["futures"] = {
        "symbol": "CBOT_MINI:YM1!",
        "frames": [_ym_frame("5m"), _ym_frame("15m"), _ym_frame("30m"), _ym_frame("1h"), _ym_frame("4h")],
    }
    a = aggregate(base, strategy="intraday")
    b = aggregate(with_futures, strategy="intraday")
    assert (a["signal"], a["confidence"], a["weighted_score"]) == (
        b["signal"], b["confidence"], b["weighted_score"]
    )
