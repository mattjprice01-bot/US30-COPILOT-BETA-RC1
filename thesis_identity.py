from __future__ import annotations

import hashlib
from typing import Any


def market_thesis_id(result: dict[str, Any]) -> str | None:
    """Return a stable directional market-thesis identity for RC1.1 shadow telemetry.

    Price levels and timestamps are deliberately excluded: they drift as a setup
    re-arms and were fragmenting one continuing failed thesis into many IDs.
    Lifecycle reset/termination is handled separately by shadow events.
    """
    side = str(result.get("signal") or "").upper()
    if side not in ("LONG", "SHORT"):
        return None
    strategy = str(result.get("strategy") or "scalp").lower()
    symbol = str(result.get("symbol") or "US30").upper()
    canonical = "|".join((strategy, symbol, side))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]
