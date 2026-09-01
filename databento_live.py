"""Per-user Databento live YM L1 connector for US30 Copilot 6.4.

Each US30 Copilot account may store its own encrypted Databento API key.
The server decrypts the key only in memory and starts a private YM.FUT MBP-1
stream for that user.

Subscription:
    dataset = GLBX.MDP3
    schema  = mbp-1
    stype   = parent
    symbol  = YM.FUT

No API key is logged or returned to the browser.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable


class DatabentoOrderflowManager:
    def __init__(
        self,
        api_key: str,
        publish: Callable[[dict[str, Any]], None],
        status_publish: Callable[[dict[str, Any]], None],
        *,
        dataset: str = "GLBX.MDP3",
        schema: str = "mbp-1",
        stype: str = "parent",
        symbol: str = "YM.FUT",
        emit_seconds: float = 1.0,
    ):
        self.key = (api_key or "").strip()
        self.publish = publish
        self.status_publish = status_publish
        self.dataset = dataset
        self.schema = schema
        self.stype = stype
        self.symbol = symbol
        self.emit_seconds = max(0.25, float(emit_seconds))

        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._client: Any = None

        self._last_emit = 0.0
        self._bid_depth = 0.0
        self._ask_depth = 0.0
        self._prev_bid_depth = 0.0
        self._prev_ask_depth = 0.0
        self._best_bid: float | None = None
        self._best_ask: float | None = None
        self._buy_vol = 0.0
        self._sell_vol = 0.0
        self._events = 0

    @property
    def configured(self) -> bool:
        return bool(self.key)

    def _status(self, **extra: Any) -> None:
        self.status_publish({
            "configured": self.configured,
            "dataset": self.dataset,
            "schema": self.schema,
            "stype": self.stype,
            "symbol": self.symbol,
            "source": "Databento YM · GLBX.MDP3 · L1 (MBP-1)",
            **extra,
        })

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        if not self.key:
            self._status(connected=False, status="NOT_CONFIGURED", error=None)
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="databento-ym-mbp1",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        client = self._client
        if client is not None:
            try:
                client.stop()
            except Exception:
                pass

    @staticmethod
    def _enum_text(value: Any) -> str:
        try:
            value = getattr(value, "value", value)
            if isinstance(value, bytes):
                value = value.decode("ascii", "ignore")
            return str(value).strip().upper()
        except Exception:
            return ""

    @staticmethod
    def _price(value: Any) -> float | None:
        try:
            x = float(value)
        except Exception:
            return None
        # DBN fixed prices can arrive in 1e-9 units.
        if abs(x) > 10_000_000:
            x /= 1_000_000_000.0
        return x

    @classmethod
    def _is_trade(cls, action: Any) -> bool:
        s = cls._enum_text(action)
        return s in {"T", "TRADE"} or s.endswith(".TRADE")

    @classmethod
    def _aggressor(cls, side: Any) -> str:
        s = cls._enum_text(side)
        if s in {"B", "BID"} or s.endswith(".BID"):
            return "BUY"
        if s in {"A", "ASK"} or s.endswith(".ASK"):
            return "SELL"
        return "UNKNOWN"

    def _handle_record(self, record: Any) -> None:
        if type(record).__name__ == "ErrorMsg" or hasattr(record, "err"):
            self._status(
                connected=False,
                status="ERROR",
                error=str(getattr(record, "err", record))[:500],
            )
            return

        levels = getattr(record, "levels", None)
        if levels is None:
            return

        try:
            levels_list = list(levels)[:1]
        except Exception:
            return

        bid_depth = ask_depth = 0.0
        best_bid = best_ask = None

        for i, level in enumerate(levels_list):
            try:
                bid_depth += float(getattr(level, "bid_sz", 0) or 0)
                ask_depth += float(getattr(level, "ask_sz", 0) or 0)
            except Exception:
                pass
            if i == 0:
                best_bid = self._price(getattr(level, "bid_px", None))
                best_ask = self._price(getattr(level, "ask_px", None))

        try:
            size = float(getattr(record, "size", 0) or 0)
        except Exception:
            size = 0.0

        now_mono = time.monotonic()
        with self._lock:
            if bid_depth + ask_depth > 0:
                self._bid_depth = bid_depth
                self._ask_depth = ask_depth
                self._best_bid = best_bid
                self._best_ask = best_ask

            if self._is_trade(getattr(record, "action", "")) and size > 0:
                aggr = self._aggressor(getattr(record, "side", ""))
                if aggr == "BUY":
                    self._buy_vol += size
                elif aggr == "SELL":
                    self._sell_vol += size

            self._events += 1
            if now_mono - self._last_emit < self.emit_seconds:
                return

            self._last_emit = now_mono
            bdepth, adepth = self._bid_depth, self._ask_depth
            prev_bid, prev_ask = self._prev_bid_depth, self._prev_ask_depth
            self._prev_bid_depth, self._prev_ask_depth = bdepth, adepth

            buy, sell, events = self._buy_vol, self._sell_vol, self._events
            bb, ba = self._best_bid, self._best_ask
            self._buy_vol = self._sell_vol = 0.0
            self._events = 0

        depth_total = bdepth + adepth
        depth_imbalance = (bdepth - adepth) / depth_total if depth_total else 0.0

        trade_total = buy + sell
        delta_norm = (buy - sell) / trade_total if trade_total else 0.0

        bid_change = bdepth - prev_bid if prev_bid else 0.0
        ask_change = adepth - prev_ask if prev_ask else 0.0
        denom = max(abs(bid_change) + abs(ask_change), 1.0)
        liquidity_shift = (bid_change - ask_change) / denom

        absorption = 0.0
        if delta_norm > 0.20 and depth_imbalance < -0.15:
            absorption = max(-1.0, depth_imbalance - delta_norm)
        elif delta_norm < -0.20 and depth_imbalance > 0.15:
            absorption = min(1.0, depth_imbalance - delta_norm)

        spread = mid = microprice = None
        if bb is not None and ba is not None and ba >= bb:
            spread = ba - bb
            mid = (ba + bb) / 2.0
            if depth_total:
                microprice = (ba * bdepth + bb * adepth) / depth_total

        self.publish({
            "source": "Databento YM · GLBX.MDP3 · L1 (MBP-1)",
            "dataset": self.dataset,
            "schema": self.schema,
            "stype": self.stype,
            "symbol": self.symbol,
            "depth_imbalance": round(depth_imbalance, 4),
            "delta_norm": round(delta_norm, 4),
            "absorption": round(absorption, 4),
            "liquidity_shift": round(max(-1.0, min(1.0, liquidity_shift)), 4),
            "bid_size_l1": round(bdepth, 2),
            "ask_size_l1": round(adepth, 2),
            "bid_size_change": round(bid_change, 2),
            "ask_size_change": round(ask_change, 2),
            "buy_volume_window": round(buy, 2),
            "sell_volume_window": round(sell, 2),
            "events_window": int(events),
            "best_bid": bb,
            "best_ask": ba,
            "mid": round(mid, 6) if mid is not None else None,
            "microprice": round(microprice, 6) if microprice is not None else None,
            "spread": round(spread, 6) if spread is not None else None,
        })
        self._status(
            connected=True,
            status="LIVE",
            error=None,
            last_data_epoch=time.time(),
        )

    def _callback_error(self, exc: Exception) -> None:
        self._status(
            connected=False,
            status="CALLBACK_ERROR",
            error=f"{type(exc).__name__}: {exc}"[:500],
        )

    def _run(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            try:
                import databento as db

                self._status(connected=False, status="CONNECTING", error=None)
                client = db.Live(key=self.key, reconnect_policy="reconnect")
                self._client = client
                client.subscribe(
                    dataset=self.dataset,
                    schema=self.schema,
                    stype_in=self.stype,
                    symbols=self.symbol,
                )
                client.add_callback(
                    self._handle_record,
                    exception_callback=self._callback_error,
                )
                client.start()
                self._status(
                    connected=True,
                    status="CONNECTED_WAITING_FOR_DATA",
                    error=None,
                )
                client.block_for_close()

                if not self._stop.is_set():
                    self._status(
                        connected=False,
                        status="DISCONNECTED",
                        error="Databento live session closed",
                    )
            except Exception as exc:
                self._status(
                    connected=False,
                    status="ERROR",
                    error=f"{type(exc).__name__}: {exc}"[:500],
                )
            finally:
                self._client = None

            if self._stop.wait(backoff):
                break
            backoff = min(30.0, backoff * 1.6)
