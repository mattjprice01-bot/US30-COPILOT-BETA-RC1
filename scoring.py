from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import math


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    label: str
    horizon: str
    weights: dict[str, float] = field(default_factory=dict)
    long_threshold: float = 1.25
    short_threshold: float = -1.25
    confidence_cap: int = 95
    strength_full_scale: float = 5.5
    entry_atr_low: float = 0.12
    entry_atr_high: float = 0.08
    stop_atr: float = 0.85
    tp1_r: float = 1.5
    tp2_r: float = 2.5
    ym_weight: float = 0.60
    orderflow_weight: float = 0.55


STRATEGIES: dict[str, StrategyConfig] = {
    "scalp": StrategyConfig(
        name="scalp",
        label="Scalp",
        horizon="minutes–1h",
        weights={"1m": 0.22, "5m": 0.28, "15m": 0.25, "1h": 0.17, "4h": 0.08},
        long_threshold=1.20,
        short_threshold=-1.20,
        strength_full_scale=5.2,
        entry_atr_low=0.10,
        entry_atr_high=0.07,
        stop_atr=0.78,
        tp1_r=1.5,
        tp2_r=2.4,
        ym_weight=0.75,
        orderflow_weight=0.90,
    ),
    "intraday": StrategyConfig(
        name="intraday",
        label="Intraday",
        horizon="1–8h",
        weights={"5m": 0.16, "15m": 0.25, "30m": 0.20, "1h": 0.24, "4h": 0.15},
        long_threshold=1.25,
        short_threshold=-1.25,
        strength_full_scale=5.5,
        entry_atr_low=0.12,
        entry_atr_high=0.08,
        stop_atr=0.85,
        tp1_r=1.5,
        tp2_r=2.5,
        ym_weight=0.60,
        orderflow_weight=0.55,
    ),
    "swing": StrategyConfig(
        name="swing",
        label="Swing",
        horizon="days",
        weights={"1h": 0.18, "4h": 0.32, "1d": 0.35, "1w": 0.15},
        long_threshold=1.20,
        short_threshold=-1.20,
        strength_full_scale=5.2,
        entry_atr_low=0.15,
        entry_atr_high=0.10,
        stop_atr=1.05,
        tp1_r=1.6,
        tp2_r=2.8,
        ym_weight=0.35,
        orderflow_weight=0.15,
    ),
}


def get_strategy(name: str | None) -> StrategyConfig:
    return STRATEGIES.get(str(name or "scalp").lower(), STRATEGIES["scalp"])


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


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

    mom = _clip((close - c3) / atr, -1.0, 1.0)
    score += mom * 0.7
    if abs(mom) > 0.45:
        reasons.append("positive 3-bar momentum" if mom > 0 else "negative 3-bar momentum")

    if close > open_ and prev_c < prev_o and close > prev_o and open_ <= prev_c:
        score += 0.5; reasons.append("bullish engulfing")
    elif close < open_ and prev_c > prev_o and open_ >= prev_c and close < prev_o:
        score -= 0.5; reasons.append("bearish engulfing")

    return {
        "tf": str(f.get("tf", "?")),
        "score": round(_clip(score, -8.0, 8.0), 3),
        "price": close,
        "open": open_,
        "high": high,
        "low": low,
        "atr": atr,
        "rsi": rsi,
        "reasons": reasons,
    }


def _ym_futures_shadow(payload: dict[str, Any], cfg: StrategyConfig) -> dict[str, Any]:
    """Score TradingView CME YM multi-timeframe frames in shadow mode only.

    RC1.1 records this score for forward validation. It is deliberately excluded
    from the live signal calculation until enough forward evidence exists.
    """
    futures = payload.get("futures")
    if not isinstance(futures, dict):
        return {"available": False, "shadow": True, "score": 0.0, "weighted": 0.0,
                "symbol": None, "frames": [], "reason": "futures payload unavailable"}

    frames = futures.get("frames")
    if not isinstance(frames, list):
        frames = []

    by_tf: dict[str, dict[str, Any]] = {}
    scored_frames: list[dict[str, Any]] = []
    for frame in frames:
        if not isinstance(frame, dict):
            continue
        tf = str(frame.get("tf", ""))
        if not tf:
            continue

        close = _f(frame.get("c"))
        ema20 = _f(frame.get("ema20"), close)
        ema50 = _f(frame.get("ema50"), close)
        rsi = _f(frame.get("rsi"), 50.0)
        atr = max(_f(frame.get("atr"), 1.0), 1e-9)
        c3 = _f(frame.get("c3"), close)
        vol = max(_f(frame.get("vol")), 0.0)
        vma20 = max(_f(frame.get("vma20")), 0.0)

        raw = 0.0
        reasons: list[str] = []
        if close > ema20 > ema50:
            raw += 1.0; reasons.append("bullish EMA structure")
        elif close < ema20 < ema50:
            raw -= 1.0; reasons.append("bearish EMA structure")
        if rsi >= 55:
            raw += 0.45; reasons.append("RSI bullish")
        elif rsi <= 45:
            raw -= 0.45; reasons.append("RSI bearish")

        momentum = _clip((close - c3) / atr, -1.0, 1.0)
        raw += 0.55 * momentum
        if abs(momentum) >= 0.35:
            reasons.append("positive momentum" if momentum > 0 else "negative momentum")

        rvol = (vol / vma20) if vma20 > 0 and vol > 0 else None
        if rvol is not None and rvol >= 1.25:
            raw *= 1.10
            reasons.append("volume expansion")

        score = _clip(raw, -2.0, 2.0)
        item = {"tf": tf, "score": round(score, 3), "rsi": round(rsi, 2),
                "momentum": round(momentum, 3),
                "rvol": round(rvol, 3) if rvol is not None else None,
                "reasons": reasons}
        by_tf[tf] = item
        scored_frames.append(item)

    weighted_sum = 0.0
    active_weight = 0.0
    for tf, weight in cfg.weights.items():
        item = by_tf.get(tf)
        if item is None:
            continue
        weighted_sum += _f(item.get("score")) * weight
        active_weight += weight

    score = _clip(weighted_sum / active_weight if active_weight else 0.0, -2.0, 2.0)
    return {"available": bool(active_weight), "shadow": True,
            "score": round(score, 3), "weighted": round(score * cfg.ym_weight, 3),
            "symbol": futures.get("symbol"), "frames": scored_frames,
            "reason": "shadow only; excluded from live signal calculation"}


def _ym_context(payload: dict[str, Any], cfg: StrategyConfig) -> dict[str, Any]:
    ym = payload.get("ym")
    if not isinstance(ym, dict):
        return {"available": False, "score": 0.0, "weighted": 0.0}

    raw = ym.get("score")
    if raw is None:
        raw = ym.get("bias")
    if isinstance(raw, str):
        s = raw.upper()
        raw = 1.0 if s in {"LONG", "BULL", "BULLISH", "UP"} else -1.0 if s in {"SHORT", "BEAR", "BEARISH", "DOWN"} else 0.0
    score = _clip(_f(raw), -2.0, 2.0)
    return {
        **ym,
        "available": True,
        "score": round(score, 3),
        "weighted": round(score * cfg.ym_weight, 3),
    }


def _orderflow_context(ctx: dict[str, Any] | None, cfg: StrategyConfig) -> dict[str, Any]:
    if not isinstance(ctx, dict):
        return {"available": False, "score": 0.0, "weighted": 0.0}

    data = ctx.get("data") if isinstance(ctx.get("data"), dict) else ctx
    live = bool(ctx.get("fresh", ctx.get("live", ctx.get("connected", True))))
    if not live:
        return {**data, "available": False, "score": 0.0, "weighted": 0.0}

    # L1 / MBP-1 confirmation. Accept the field names used by our Databento manager.
    imbalance = _f(data.get("imbalance", data.get("size_imbalance", data.get("book_imbalance", 0.0))))
    delta = _f(data.get("delta", data.get("normalized_delta", data.get("trade_delta", 0.0))))
    liquidity = _f(data.get("liquidity_shift", data.get("liquidity_score", 0.0)))
    absorption = _f(data.get("absorption", data.get("absorption_score", 0.0)))

    # Normalize defensively because connector versions may express these as ratios or percentages.
    def norm(v: float) -> float:
        if abs(v) > 2.0:
            v /= 100.0
        return _clip(v, -1.0, 1.0)

    imbalance, delta, liquidity, absorption = map(norm, (imbalance, delta, liquidity, absorption))
    score = 0.42 * imbalance + 0.36 * delta + 0.14 * liquidity + 0.08 * absorption
    score = _clip(score, -1.0, 1.0)

    return {
        **data,
        "available": True,
        "score": round(score, 3),
        "weighted": round(score * cfg.orderflow_weight, 3),
    }


def aggregate(
    payload: dict[str, Any],
    strategy: str = "scalp",
    news_ctx: dict[str, Any] | None = None,
    orderflow_ctx: dict[str, Any] | None = None,
    manual_news_block: bool = False,
    **_: Any,
) -> dict[str, Any]:
    cfg = get_strategy(strategy)
    frames = payload.get("frames") or []
    scored = [score_frame(x) for x in frames if isinstance(x, dict)]
    by_tf = {x["tf"]: x for x in scored}

    weighted = 0.0
    active_weight = 0.0
    alignment_long = 0
    alignment_short = 0
    active_frames = 0

    for tf, w in cfg.weights.items():
        f = by_tf.get(tf)
        if not f:
            continue
        s = _f(f.get("score"))
        weighted += s * w
        active_weight += w
        active_frames += 1
        alignment_long += int(s > 0.5)
        alignment_short += int(s < -0.5)

    technical_score = weighted / active_weight if active_weight else 0.0

    ym = _ym_context(payload, cfg)
    ym_futures_shadow = _ym_futures_shadow(payload, cfg)
    orderflow = _orderflow_context(orderflow_ctx, cfg)

    # RC1.1 safety invariant: the multi-timeframe YM futures score is shadow
    # telemetry only. Live RC1 signal calculations remain unchanged.
    combined = technical_score + _f(ym.get("weighted")) + _f(orderflow.get("weighted"))

    news = news_ctx if isinstance(news_ctx, dict) else {}
    high_impact_near = bool(news.get("high_impact_near"))
    news_block = bool(manual_news_block or news.get("block") or news.get("news_block") or high_impact_near)

    if news_block:
        signal = "WAIT"
        confidence = 0
    elif combined >= cfg.long_threshold:
        signal = "LONG"
        strength = min(abs(combined) / cfg.strength_full_scale, 1.0)
        confidence = round(50 + strength * (cfg.confidence_cap - 50))
    elif combined <= cfg.short_threshold:
        signal = "SHORT"
        strength = min(abs(combined) / cfg.strength_full_scale, 1.0)
        confidence = round(50 + strength * (cfg.confidence_cap - 50))
    else:
        signal = "WAIT"
        strength = min(abs(combined) / cfg.strength_full_scale, 1.0)
        confidence = round(50 + strength * (cfg.confidence_cap - 50))

    # Use the fastest timeframe available for executable levels.
    base = None
    for tf in ("1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"):
        if tf in by_tf:
            base = by_tf[tf]
            break
    if base is None:
        base = {"price": 0.0, "atr": 1.0, "high": 0.0, "low": 0.0}

    price = _f(base.get("price"))
    atr = max(_f(base.get("atr"), 1.0), 1.0)

    if signal == "LONG":
        entry_low = price - cfg.entry_atr_low * atr
        entry_high = price + cfg.entry_atr_high * atr
        stop = price - cfg.stop_atr * atr
        risk = max(price - stop, 1e-9)
        tp1 = price + cfg.tp1_r * risk
        tp2 = price + cfg.tp2_r * risk
    elif signal == "SHORT":
        entry_low = price - cfg.entry_atr_high * atr
        entry_high = price + cfg.entry_atr_low * atr
        stop = price + cfg.stop_atr * atr
        risk = max(stop - price, 1e-9)
        tp1 = price - cfg.tp1_r * risk
        tp2 = price - cfg.tp2_r * risk
    else:
        entry_low = entry_high = stop = tp1 = tp2 = None

    alignment = max(alignment_long, alignment_short)

    return {
        "version": 6.4,
        "strategy": cfg.name,
        "strategy_label": cfg.label,
        "horizon": cfg.horizon,
        "symbol": payload.get("symbol", "US30"),
        "exchange": payload.get("exchange", ""),
        "ts": payload.get("ts"),
        "signal": signal,
        "confidence": int(confidence),
        "weighted_score": round(combined, 3),
        "technical_score": round(technical_score, 3),
        "alignment": alignment,
        "alignment_total": active_frames,
        "alignment_long": alignment_long,
        "alignment_short": alignment_short,
        "news_block": news_block,
        "news": news,
        "ym": ym,
        "ym_futures_shadow": ym_futures_shadow,
        "orderflow": orderflow,
        "price": price,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "frames": scored,
    }
