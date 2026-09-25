from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.request
from typing import Any

SHADOW_MIRROR_URL = os.getenv("SHADOW_MIRROR_URL", "").strip().rstrip("/")
SHADOW_MIRROR_SECRET = os.getenv("SHADOW_MIRROR_SECRET", "").strip()
SHADOW_MIRROR_TIMEOUT = max(0.25, float(os.getenv("SHADOW_MIRROR_TIMEOUT", "2.0")))


def _post_shadow(payload: dict[str, Any]) -> None:
    if not SHADOW_MIRROR_URL or not SHADOW_MIRROR_SECRET:
        return
    try:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(
            f"{SHADOW_MIRROR_URL}/internal/shadow/tradingview",
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Shadow-Mirror-Secret": SHADOW_MIRROR_SECRET,
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=SHADOW_MIRROR_TIMEOUT) as response:
            response.read(1)
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        # Fail-open by design: Shadow must never affect production ingest.
        print(f"[SHADOW-MIRROR-WARN] {type(exc).__name__}: {exc}", flush=True)
    except Exception as exc:
        print(f"[SHADOW-MIRROR-WARN] unexpected {type(exc).__name__}: {exc}", flush=True)


def mirror_packet(payload: dict[str, Any]) -> bool:
    """Queue a best-effort packet copy to RC1.1 Shadow and return immediately."""
    if not SHADOW_MIRROR_URL or not SHADOW_MIRROR_SECRET:
        return False
    packet = dict(payload)
    thread = threading.Thread(target=_post_shadow, args=(packet,), daemon=True, name="rc1-shadow-mirror")
    thread.start()
    return True
