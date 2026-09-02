from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import math

DEFAULT_WEIGHTS = {"4h": 0.25, "1h": 0.25, "15m": 0.22, "5m": 0.18, "1m": 0.10}


@dataclass(frozen=True)
class ScoreConfig:
    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    long_threshold: float = 1.25
    short_threshold: float = -1.25
    confidence_cap: int = 95
    strength_full_scale: float = 5.5


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def score_frame(f: dict[str, Any]) -> dict[str, Any]:
    close = _f(f.get("c"))
    open_ = _f(f.get("o"), close)
    high = _f(f.get("h"), close)
    low = _f(f.get("l"), close)
    ema20 = _f(f.get("ema20"), close)
    ema50 = _f(f.get("ema50"), close)
    ema200 = _f(f.get("ema200"), close)
    rsi = _f(f.get("rsi"), 50)
    atr = max(_f(f.get("atr"), 1), 1e-9)
    vwap = _f(f.get("vwap"), close)
    swing_hi = _f(f.get("swing_hi"), high)
    swing_lo = _f(f.get("swing_lo"), low)
    c3 = _f(f.get("c3"), close)
    prev_o = _f(f.get("prev_o"), open_)
    prev_c = _f(f.get("prev_c"), close)

    score = 0.0
    reasons: list[str] = []

    if close > ema20 > ema50:
        score += 2.0; reasons.append("bullish EMA stack")
    elif close < ema20 < ema50:
        score -= 2.0; reasons.append("bearish EMA stack")

    if close > ema200:
        score += 0.8; reasons.append("above EMA200")
    elif close < ema200:
        score -= 0.8; reasons.append("below EMA200")

    if close > vwap:
        score += 0.8; reasons.append("above VWAP")
    elif close < vwap:
        score -= 0.8; reasons.append("below VWAP")

    if 52 <= rsi <= 72:
        score += 0.8; reasons.append(f"RSI supportive ({rsi:.0f})")
    elif 28 <= rsi <= 48:
        score -= 0.8; reasons.append(f"RSI weak ({rsi:.0f})")

    if close > swing_hi:
        score += 1.6; reasons.append("20-bar bullish break")
    elif close < swing_lo:
        score -= 1.6; reasons.append("20-bar bearish break")

    if low < swing_lo and close > swing_lo:
        score += 1.4; reasons.append("sell-side sweep/reclaim")
    if high > swing_hi and close < swing_hi:
        score -= 1.4; reasons.append("buy-side sweep/rejection")

    body = abs(close - open_)
    rng = max(high - low, 1e-9)
    if body > 0.9 * atr and body / rng > 0.62:
        if close > open_:
            score += 1.2; reasons.append("bullish displacement")
        else:
            score -= 1.2; reasons.append("bearish displacement")

    mom = max(-1.0, min(1.0, (close - c3) / atr))
    score += mom * 0.7
    if abs(mom) > 0.45:
        reasons.append("positive 3-bar momentum" if mom > 0 else "negative 3-bar momentum")

    if close > open_ and prev_c < prev_o and close > prev_o and open_ <= prev_c:
        score += 0.5; reasons.append("bullish engulfing")
    elif close < open_ and prev_c > prev_o and open_ >= prev_c and close < prev_o:
        score -= 0.5; reasons.append("bearish engulfing")

    return {
        "tf": f.get("tf", "?"),
        "score": round(max(-8.0, min(8.0, score)), 3),
        "price": close,
        "high": high,
        "low": low,
        "atr": atr,
        "rsi": rsi,
        "reasons": reasons,
    }


def aggregate(payload: dict[str, Any], news_block: bool = False, config: ScoreConfig | None = None) -> dict[str, Any]:
    config = config or ScoreConfig()
    frames = payload.get("frames") or []
    scored = [score_frame(x) for x in frames if isinstance(x, dict)]
    by_tf = {x["tf"]: x for x in scored}

    weighted = 0.0
    active_weight = 0.0
    alignment_long = alignment_short = 0
    for tf, w in config.weights.items():
        if tf in by_tf:
            s = by_tf[tf]["score"]
            weighted += s * w
            active_weight += w
            alignment_long += int(s > 0.5)
            alignment_short += int(s < -0.5)
    weighted = weighted / active_weight if active_weight else 0.0

    directional_strength = min(abs(weighted) / config.strength_full_scale, 1.0)
    confidence = round(50 + directional_strength * (config.confidence_cap - 50))

    if news_block:
        signal = "WAIT"
        confidence = 0
    elif weighted >= config.long_threshold:
        signal = "LONG"
    elif weighted <= config.short_threshold:
        signal = "SHORT"
    else:
        signal = "WAIT"

    base = by_tf.get("1m") or by_tf.get("5m") or (scored[0] if scored else {"price": 0.0, "atr": 1.0})
    price = float(base["price"])
    atr = max(float(base["atr"]), 1.0)

    if signal == "LONG":
        entry_lo, entry_hi = price - 0.12 * atr, price + 0.08 * atr
        stop = price - 0.85 * atr
        risk = max(price - stop, 1e-9)
        tp1, tp2 = price + 1.5 * risk, price + 2.5 * risk
    elif signal == "SHORT":
        entry_lo, entry_hi = price - 0.08 * atr, price + 0.12 * atr
        stop = price + 0.85 * atr
        risk = max(stop - price, 1e-9)
        tp1, tp2 = price - 1.5 * risk, price - 2.5 * risk
    else:
        entry_lo = entry_hi = stop = tp1 = tp2 = None

    return {
        "version": 4,
        "symbol": payload.get("symbol", "US30"),
        "exchange": payload.get("exchange", ""),
        "ts": payload.get("ts"),
        "signal": signal,
        "confidence": confidence,
        "weighted_score": round(weighted, 3),
        "alignment": max(alignment_long, alignment_short),
        "alignment_long": alignment_long,
        "alignment_short": alignment_short,
        "news_block": bool(news_block),
        "price": price,
        "entry_low": entry_lo,
        "entry_high": entry_hi,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "frames": scored,
    }
