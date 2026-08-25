from __future__ import annotations
import calendar, os, re, time
from datetime import datetime, timezone, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

FRED_KEY = os.getenv("FRED_API_KEY", "").strip()
NY = ZoneInfo("America/New_York")

# Official Federal Reserve sources. No paid subscription is required.
FED_FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
FED_CAL_URL = "https://www.federalreserve.gov/newsevents/calendar.htm"
FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

HIGH_IMPACT_WORDS = (
    "fomc", "press conference", "monetary policy", "minutes", "chair", "powell",
    "testimony", "semiannual monetary policy report"
)


class EconomicCalendar:
    """Free-first US macro context.

    * Federal Reserve FOMC schedule is fetched from the official Fed website with no key.
    * FRED macro-regime data is optional and only needs a free FRED API key.
    * The module fails open: if a source is temporarily unavailable, technical scoring
      continues and the UI reports the missing context rather than inventing data.
    """

    def __init__(self, fred_key: str | None = None):
        self.fred_key = (fred_key if fred_key is not None else FRED_KEY).strip()
        self._events: list[dict[str, Any]] = []
        self._fetched_at = 0.0
        self._fred_fetched_at = 0.0
        self._error: str | None = None
        self._fred_error: str | None = None
        self._macro: dict[str, Any] = {}

    @staticmethod
    def _strategy_windows(strategy: str) -> tuple[int, int]:
        if strategy == "scalp":
            return 30, 20
        if strategy == "intraday":
            return 60, 30
        return 240, 60

    @staticmethod
    def _event_dt(year: int, month: int, day: int, hour: int = 14, minute: int = 0) -> datetime:
        # Scheduled FOMC policy statements are normally released at 2pm New York time.
        return datetime(year, month, day, hour, minute, tzinfo=NY).astimezone(timezone.utc)

    @staticmethod
    def _extract_fomc_events(html: str) -> list[dict[str, Any]]:
        text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
        lines = [re.sub(r"\s+", " ", x).strip() for x in text.splitlines() if x.strip()]
        events: list[dict[str, Any]] = []
        current_year: int | None = None
        month_num: int | None = None
        month_map = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
        for line in lines:
            y = re.fullmatch(r"(20\d{2}) FOMC Meetings", line)
            if y:
                current_year = int(y.group(1)); month_num = None; continue
            if current_year is None:
                continue
            low = line.lower()
            for name, num in month_map.items():
                if low == name or low.startswith(name + " "):
                    month_num = num
                    rest = line[len(name):].strip()
                    if rest:
                        m = re.search(r"(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?", rest)
                        if m:
                            end_day = int(m.group(2) or m.group(1))
                            dt = EconomicCalendar._event_dt(current_year, month_num, end_day)
                            events.append({"name": "FOMC rate decision / statement", "date": dt.isoformat(), "importance": 3, "source": "Federal Reserve", "category": "FOMC"})
                    break
            else:
                if month_num:
                    m = re.fullmatch(r"(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\*?", line)
                    if m:
                        end_day = int(m.group(2) or m.group(1))
                        try:
                            dt = EconomicCalendar._event_dt(current_year, month_num, end_day)
                            events.append({"name": "FOMC rate decision / statement", "date": dt.isoformat(), "importance": 3, "source": "Federal Reserve", "category": "FOMC"})
                        except ValueError:
                            pass
        # Deduplicate if the page layout caused both inline and next-line parsing.
        unique = {(e["date"], e["name"]): e for e in events}
        return sorted(unique.values(), key=lambda x: x["date"])

    async def _refresh_fed(self, force: bool = False) -> None:
        if not force and time.time() - self._fetched_at < 6 * 3600:
            return
        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers={"User-Agent": "US30-Signal-Lab/6"}) as client:
                r = await client.get(FED_FOMC_URL)
                r.raise_for_status()
            self._events = self._extract_fomc_events(r.text)
            self._error = None
        except Exception as exc:
            self._error = f"{type(exc).__name__}: {exc}"
        self._fetched_at = time.time()

    async def _fred_series(self, client: httpx.AsyncClient, series_id: str) -> list[float]:
        r = await client.get(FRED_URL, params={
            "series_id": series_id, "api_key": self.fred_key, "file_type": "json",
            "sort_order": "desc", "limit": 8,
        })
        r.raise_for_status()
        vals: list[float] = []
        for row in r.json().get("observations", []):
            try:
                v = float(row.get("value"))
                vals.append(v)
            except (TypeError, ValueError):
                pass
        return vals

    async def _refresh_fred(self, force: bool = False) -> None:
        if not self.fred_key:
            self._macro = {"available": False, "provider": "FRED", "reason": "FRED_API_KEY not configured"}
            return
        if not force and time.time() - self._fred_fetched_at < 30 * 60:
            return
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                dgs2, dgs10, vix, fedfunds = await __import__('asyncio').gather(
                    self._fred_series(client, "DGS2"),
                    self._fred_series(client, "DGS10"),
                    self._fred_series(client, "VIXCLS"),
                    self._fred_series(client, "DFF"),
                )
            def latest(xs): return xs[0] if xs else None
            def change(xs): return (xs[0] - xs[min(4, len(xs)-1)]) if len(xs) > 1 else None
            vx = latest(vix)
            if vx is None: regime = "UNKNOWN"
            elif vx < 15: regime = "LOW"
            elif vx < 22: regime = "NORMAL"
            elif vx < 30: regime = "ELEVATED"
            else: regime = "EXTREME"
            self._macro = {
                "available": True, "provider": "FRED", "vix": vx, "volatility_regime": regime,
                "us2y": latest(dgs2), "us10y": latest(dgs10),
                "curve_2s10s": (latest(dgs10) - latest(dgs2)) if latest(dgs10) is not None and latest(dgs2) is not None else None,
                "us2y_change": change(dgs2), "us10y_change": change(dgs10), "fed_funds": latest(fedfunds),
                "updated_epoch": time.time(),
            }
            self._fred_error = None
        except Exception as exc:
            self._fred_error = f"{type(exc).__name__}: {exc}"
            self._macro = {"available": False, "provider": "FRED", "reason": self._fred_error}
        self._fred_fetched_at = time.time()

    async def context(self, strategy: str) -> dict[str, Any]:
        await self._refresh_fed()
        await self._refresh_fred()
        now = datetime.now(timezone.utc)
        before, after = self._strategy_windows(strategy)
        near: list[dict[str, Any]] = []
        upcoming: list[dict[str, Any]] = []
        for e in self._events:
            try:
                dt = datetime.fromisoformat(str(e["date"]).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                continue
            mins = (dt - now).total_seconds() / 60
            x = {**e, "minutes": round(mins)}
            if -after <= mins <= before:
                near.append(x)
            if 0 <= mins <= 14 * 24 * 60:
                upcoming.append(x)
        upcoming.sort(key=lambda x: x["minutes"])
        near.sort(key=lambda x: abs(x["minutes"]))
        risk = "HIGH" if near else ("WATCH" if upcoming and upcoming[0]["minutes"] < 360 else "LOW")
        return {
            "available": True,
            "provider": "Federal Reserve" + (" + FRED" if self._macro.get("available") else ""),
            "high_impact_near": bool(near), "block": bool(near), "risk": risk,
            "events": near[:5], "upcoming": upcoming[:5], "error": self._error,
            "window_before_min": before, "window_after_min": after,
            "macro": self._macro, "fred_error": self._fred_error,
            "sources": ["Federal Reserve FOMC calendar"] + (["FRED macro series"] if self._macro.get("available") else []),
        }
