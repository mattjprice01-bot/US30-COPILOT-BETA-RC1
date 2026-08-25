from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any
import math


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    label: str
    horizon: str
    weights: dict[str, float]
    base_tf: str
    long_threshold: float
    short_threshold: float
    stop_atr: float
    tp1_r: float
    tp2_r: float
    min_alignment: int
    ym_weight: float
    orderflow_weight: float
    news_confidence_penalty: int
    block_high_impact_news: bool


STRATEGIES: dict[str, StrategyConfig] = {
    "scalp": StrategyConfig(
        name="scalp", label="Scalp", horizon="Minutes to ~1 hour",
        weights={"1m": .18, "5m": .30, "15m": .30, "1h": .22},
        base_tf="1m", long_threshold=1.30, short_threshold=-1.30,
        stop_atr=.82, tp1_r=1.35, tp2_r=2.15, min_alignment=3,
        ym_weight=.75, orderflow_weight=.90, news_confidence_penalty=20,
        block_high_impact_news=True,
    ),
    "intraday": StrategyConfig(
        name="intraday", label="Intraday", horizon="~1 to 8 hours",
        weights={"5m": .10, "15m": .24, "30m": .18, "1h": .28, "4h": .20},
        base_tf="15m", long_threshold=1.20, short_threshold=-1.20,
        stop_atr=1.05, tp1_r=1.60, tp2_r=2.60, min_alignment=3,
        ym_weight=.60, orderflow_weight=.55, news_confidence_penalty=14,
        block_high_impact_news=True,
    ),
    "swing": StrategyConfig(
        name="swing", label="Swing", horizon="~1 to several days",
        weights={"1h": .12, "4h": .28, "1d": .38, "1w": .22},
        base_tf="4h", long_threshold=1.05, short_threshold=-1.05,
        stop_atr=1.45, tp1_r=1.80, tp2_r=3.20, min_alignment=3,
        ym_weight=.35, orderflow_weight=.15, news_confidence_penalty=8,
        block_high_impact_news=False,
    ),
}


def get_strategy(name: str | None) -> StrategyConfig:
    return STRATEGIES.get((name or "scalp").lower(), STRATEGIES["scalp"])


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
    vol = max(_f(f.get("vol"), 0), 0)
    vma20 = max(_f(f.get("vma20"), vol), 1e-9)

    score = 0.0
    reasons: list[str] = []

    if close > ema20 > ema50:
        score += 2.0; reasons.append("bullish EMA stack")
    elif close < ema20 < ema50:
        score -= 2.0; reasons.append("bearish EMA stack")

    if close > ema200:
        score += .8; reasons.append("above EMA200")
    elif close < ema200:
        score -= .8; reasons.append("below EMA200")

    if close > vwap:
        score += .75; reasons.append("above VWAP")
    elif close < vwap:
        score -= .75; reasons.append("below VWAP")

    if 52 <= rsi <= 72:
        score += .8; reasons.append(f"RSI supportive ({rsi:.0f})")
    elif 28 <= rsi <= 48:
        score -= .8; reasons.append(f"RSI weak ({rsi:.0f})")

    if close > swing_hi:
        score += 1.55; reasons.append("20-bar bullish break")
    elif close < swing_lo:
        score -= 1.55; reasons.append("20-bar bearish break")

    if low < swing_lo and close > swing_lo:
        score += 1.35; reasons.append("sell-side sweep/reclaim")
    if high > swing_hi and close < swing_hi:
        score -= 1.35; reasons.append("buy-side sweep/rejection")

    body = abs(close - open_)
    rng = max(high - low, 1e-9)
    if body > .9 * atr and body / rng > .62:
        if close > open_:
            score += 1.15; reasons.append("bullish displacement")
        else:
            score -= 1.15; reasons.append("bearish displacement")

    mom = max(-1.0, min(1.0, (close - c3) / atr))
    score += mom * .7
    if abs(mom) > .45:
        reasons.append("positive 3-bar momentum" if mom > 0 else "negative 3-bar momentum")

    if close > open_ and prev_c < prev_o and close > prev_o and open_ <= prev_c:
        score += .5; reasons.append("bullish engulfing")
    elif close < open_ and prev_c > prev_o and open_ >= prev_c and close < prev_o:
        score -= .5; reasons.append("bearish engulfing")

    rvol = vol / vma20 if vma20 else 1.0
    if rvol >= 1.5 and abs(close-open_) / atr > .35:
        score += .35 if close > open_ else -.35
        reasons.append(f"volume expansion {rvol:.1f}x")

    return {
        "tf": f.get("tf", "?"), "score": round(max(-8., min(8., score)), 3),
        "price": close, "high": high, "low": low, "atr": atr, "rsi": rsi,
        "rvol": round(rvol, 2), "reasons": reasons,
    }


def score_ym_futures(payload: dict[str, Any], config: StrategyConfig) -> dict[str, Any]:
    fut = payload.get("futures") if isinstance(payload, dict) else None
    frames = (fut or {}).get("frames") if isinstance(fut, dict) else None
    if not isinstance(frames, list):
        return {"available": False, "score": 0.0, "reasons": [], "symbol": None}
    by_tf = {str(x.get("tf")): x for x in frames if isinstance(x, dict)}
    weights = config.weights
    total = active = 0.0
    reasons: list[str] = []
    for tf, w in weights.items():
        f = by_tf.get(tf)
        if not f:
            continue
        c = _f(f.get("c")); e20 = _f(f.get("ema20"), c); e50 = _f(f.get("ema50"), c)
        rsi = _f(f.get("rsi"), 50); atr = max(_f(f.get("atr"), 1), 1e-9)
        c3 = _f(f.get("c3"), c); vol = max(_f(f.get("vol"), 0), 0); vma = max(_f(f.get("vma20"), vol), 1e-9)
        s = 0.0
        if c > e20 > e50: s += 1.2
        elif c < e20 < e50: s -= 1.2
        if rsi > 55: s += .35
        elif rsi < 45: s -= .35
        rvol = max(.25, min(3., vol/vma if vma else 1.))
        impulse = max(-1., min(1., (c-c3)/atr))
        s += impulse * min(.8, .35*rvol)
        total += s*w; active += w
    score = total/active if active else 0.0
    if score > .5: reasons.append("YM futures confirm bullish pressure")
    elif score < -.5: reasons.append("YM futures confirm bearish pressure")
    else: reasons.append("YM futures mixed/neutral")
    return {
        "available": bool(active), "score": round(score, 3), "reasons": reasons,
        "symbol": (fut or {}).get("symbol"), "source": "TradingView YM futures volume/price proxy"
    }


def score_orderflow(ctx: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(ctx, dict) or not ctx.get("fresh"):
        return {"available": False, "score": 0.0, "reasons": []}
    imb = max(-1., min(1., _f(ctx.get("depth_imbalance"))))
    delta = max(-1., min(1., _f(ctx.get("delta_norm"))))
    absorption = max(-1., min(1., _f(ctx.get("absorption"))))
    score = 1.1*imb + .9*delta + .35*absorption
    reasons=[]
    if score > .35: reasons.append("L2 order flow favours buyers")
    elif score < -.35: reasons.append("L2 order flow favours sellers")
    else: reasons.append("L2 order flow balanced")
    return {"available": True, "score": round(score,3), "reasons":reasons, **{k:ctx.get(k) for k in ("depth_imbalance","delta_norm","absorption","source","age_seconds")}}


def aggregate(payload: dict[str, Any], strategy: str = "scalp", news_ctx: dict[str, Any] | None = None,
              orderflow_ctx: dict[str, Any] | None = None, manual_news_block: bool = False) -> dict[str, Any]:
    cfg = get_strategy(strategy)
    frames = payload.get("frames") or []
    scored = [score_frame(x) for x in frames if isinstance(x, dict)]
    by_tf = {x["tf"]: x for x in scored}

    weighted = active_weight = 0.0
    alignment_long = alignment_short = 0
    used_tfs=[]
    for tf,w in cfg.weights.items():
        if tf in by_tf:
            s=by_tf[tf]["score"]; weighted += s*w; active_weight += w; used_tfs.append(tf)
            alignment_long += int(s>.5); alignment_short += int(s<-.5)
    weighted = weighted/active_weight if active_weight else 0.0

    ym = score_ym_futures(payload, cfg)
    if ym["available"]:
        weighted += ym["score"] * cfg.ym_weight
    of = score_orderflow(orderflow_ctx)
    if of["available"]:
        weighted += of["score"] * cfg.orderflow_weight

    directional_strength = min(abs(weighted)/5.5, 1.)
    confidence = round(50 + directional_strength*45)

    news_ctx = news_ctx or {"available": False, "block": False, "risk": "unknown", "events": []}
    news_block = bool(manual_news_block or (news_ctx.get("block") and cfg.block_high_impact_news))
    if news_ctx.get("high_impact_near") and not news_block:
        confidence = max(0, confidence-cfg.news_confidence_penalty)

    if news_block:
        signal="WAIT"; confidence=0
    elif weighted >= cfg.long_threshold: signal="LONG"
    elif weighted <= cfg.short_threshold: signal="SHORT"
    else: signal="WAIT"

    base = by_tf.get(cfg.base_tf) or by_tf.get("1m") or (scored[0] if scored else {"price":0.,"atr":1.})
    price=float(base["price"]); atr=max(float(base["atr"]),1e-9)
    if signal=="LONG":
        entry_lo,entry_hi=price-.10*atr,price+.06*atr; stop=price-cfg.stop_atr*atr; risk=max(price-stop,1e-9)
        tp1,tp2=price+cfg.tp1_r*risk,price+cfg.tp2_r*risk
    elif signal=="SHORT":
        entry_lo,entry_hi=price-.06*atr,price+.10*atr; stop=price+cfg.stop_atr*atr; risk=max(stop-price,1e-9)
        tp1,tp2=price-cfg.tp1_r*risk,price-cfg.tp2_r*risk
    else:
        entry_lo=entry_hi=stop=tp1=tp2=None

    return {
        "version":6, "strategy":cfg.name, "strategy_label":cfg.label, "horizon":cfg.horizon,
        "symbol":payload.get("symbol","US30"), "exchange":payload.get("exchange",""), "ts":payload.get("ts"),
        "signal":signal, "confidence":confidence, "weighted_score":round(weighted,3),
        "alignment":max(alignment_long,alignment_short), "alignment_long":alignment_long, "alignment_short":alignment_short,
        "alignment_total":len(used_tfs), "used_timeframes":used_tfs,
        "news_block":news_block, "manual_news_block":bool(manual_news_block), "news":news_ctx,
        "ym_futures":ym, "orderflow":of,
        "price":price, "entry_low":entry_lo, "entry_high":entry_hi, "stop":stop, "tp1":tp1, "tp2":tp2,
        "tp1_r":cfg.tp1_r, "tp2_r":cfg.tp2_r, "frames":scored,
    }
