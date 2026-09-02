from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.errors import UniqueViolation
except ImportError:
    psycopg = None
    dict_row = None
    UniqueViolation = Exception
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
from pywebpush import webpush, WebPushException
from fastapi import Cookie, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, EmailStr
import stripe

from market_context import EconomicCalendar
from scoring import STRATEGIES, aggregate, get_strategy
from databento_live import DatabentoOrderflowManager

BASE = Path(__file__).resolve().parent
DB = Path(os.getenv("US30_DB", BASE / "data" / "commercial.db"))
DB.parent.mkdir(parents=True, exist_ok=True)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_BACKEND = "postgresql" if DATABASE_URL else "sqlite"
APP_TITLE = "US30 Copilot Data Engine"
BUILD_ID = "2026-09-02-stripe-beta-definitive-1"
APP_SECRET = os.getenv("APP_SECRET", "CHANGE-ME-COMMERCIAL-ALPHA")
ALLOW_REGISTRATION = os.getenv("ALLOW_REGISTRATION", "true").lower() in {"1", "true", "yes", "on"}
SESSION_DAYS = int(os.getenv("SESSION_DAYS", "30"))
MIN_TRACK_CONFIDENCE = int(os.getenv("MIN_TRACK_CONFIDENCE", "68"))
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:admin@example.com").strip()
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_BETA_PRICE_ID = os.getenv("STRIPE_BETA_PRICE_ID", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
stripe.api_key = STRIPE_SECRET_KEY or None

app = FastAPI(title=APP_TITLE)

# Per-user Databento state. API keys remain encrypted in SQLite and are only
# decrypted in memory long enough to create that user's live connector.
DATABENTO_MAX_AGE_SECONDS = max(1.0, float(os.getenv("DATABENTO_MAX_AGE_SECONDS", "5.0")))
_DATABENTO_LOCK = threading.RLock()
_DATABENTO_MANAGERS: dict[int, DatabentoOrderflowManager] = {}
_DATABENTO_STATE: dict[int, dict[str, Any]] = {}


def _db_state(uid: int) -> dict[str, Any]:
    with _DATABENTO_LOCK:
        return _DATABENTO_STATE.setdefault(uid, {
            "data": None,
            "status": {
                "configured": False,
                "connected": False,
                "status": "NOT_CONFIGURED",
                "source": "Databento YM · GLBX.MDP3 · L1 (MBP-1)",
            },
        })


def _db_publish(uid: int, data: dict[str, Any]) -> None:
    with _DATABENTO_LOCK:
        st = _db_state(uid)
        st["data"] = {**data, "received_epoch": time.time()}


def _db_publish_status(uid: int, data: dict[str, Any]) -> None:
    with _DATABENTO_LOCK:
        st = _db_state(uid)
        st["status"] = {**data, "updated_epoch": time.time()}


def _fresh_orderflow(uid: int) -> dict[str, Any]:
    with _DATABENTO_LOCK:
        st = _db_state(uid)
        status = dict(st.get("status") or {})
        data = dict(st.get("data") or {})
    received = data.get("received_epoch")
    age = (time.time() - float(received)) if received else None
    fresh = bool(received and age is not None and age <= DATABENTO_MAX_AGE_SECONDS)
    if data:
        return {
            **status,
            **data,
            "fresh": fresh,
            "age_seconds": round(age, 3) if age is not None else None,
        }
    return {
        **status,
        "fresh": False,
        "age_seconds": None,
    }


def _start_user_databento(uid: int, api_key: str) -> None:
    key = (api_key or "").strip()
    with _DATABENTO_LOCK:
        old = _DATABENTO_MANAGERS.pop(uid, None)
    if old is not None:
        old.stop()

    if not key:
        _db_publish_status(uid, {
            "configured": False,
            "connected": False,
            "status": "NOT_CONFIGURED",
            "error": None,
            "source": "Databento YM · GLBX.MDP3 · L1 (MBP-1)",
        })
        return

    mgr = DatabentoOrderflowManager(
        key,
        lambda data, _uid=uid: _db_publish(_uid, data),
        lambda status, _uid=uid: _db_publish_status(_uid, status),
    )
    with _DATABENTO_LOCK:
        _DATABENTO_MANAGERS[uid] = mgr
    mgr.start()


def _start_saved_databento_connections() -> None:
    with db() as con:
        rows = con.execute(
            "SELECT user_id, databento_key_enc FROM user_connections WHERE databento_key_enc IS NOT NULL"
        ).fetchall()
    for row in rows:
        key = _dec(row["databento_key_enc"])
        if key:
            _start_user_databento(int(row["user_id"]), key)


@app.on_event("startup")
def _startup_databento() -> None:
    _start_saved_databento_connections()


@app.on_event("shutdown")
def _shutdown_databento() -> None:
    with _DATABENTO_LOCK:
        managers = list(_DATABENTO_MANAGERS.values())
        _DATABENTO_MANAGERS.clear()
    for mgr in managers:
        mgr.stop()

DEFAULT_SETTINGS = {
    "strategy": "scalp",
    "browser_notifications": True,
    "notify_setup": True,
    "notify_entry_ready": True,
    "notify_trade_update": True,
    "notify_exit_warning": True,
    "notify_tp1": True,
    "notify_tp2": True,
    "notify_stop": True,
    "notify_news": True,
    "auto_arm": True,
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_PG_SCHEMA_READY = False
_PG_SCHEMA_LOCK = threading.Lock()

PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  id BIGSERIAL PRIMARY KEY,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,password_salt TEXT NOT NULL,
  display_name TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,plan TEXT NOT NULL DEFAULT 'BETA',is_active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS auth_sessions(
  token_hash TEXT PRIMARY KEY,user_id BIGINT NOT NULL REFERENCES users(id),created_at TEXT NOT NULL,expires_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS user_settings(user_id BIGINT NOT NULL,k TEXT NOT NULL,v TEXT NOT NULL,PRIMARY KEY(user_id,k));
CREATE TABLE IF NOT EXISTS user_connections(
  user_id BIGINT PRIMARY KEY REFERENCES users(id),tradingview_token TEXT UNIQUE NOT NULL,fred_key_enc TEXT,databento_key_enc TEXT,
  created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots(
  id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL,received_at TEXT NOT NULL,symbol TEXT,raw_json TEXT NOT NULL,result_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS candles_1m(
  user_id BIGINT NOT NULL,ts BIGINT NOT NULL,symbol TEXT,o DOUBLE PRECISION NOT NULL,h DOUBLE PRECISION NOT NULL,l DOUBLE PRECISION NOT NULL,c DOUBLE PRECISION NOT NULL,vol DOUBLE PRECISION,
  PRIMARY KEY(user_id,ts));
CREATE INDEX IF NOT EXISTS idx_candles_1m_user_ts ON candles_1m(user_id,ts);
CREATE TABLE IF NOT EXISTS notifications(
  id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL,created_at TEXT NOT NULL,event_type TEXT NOT NULL,title TEXT NOT NULL,body TEXT NOT NULL,dedupe_key TEXT NOT NULL,
  UNIQUE(user_id,dedupe_key));
CREATE TABLE IF NOT EXISTS push_subscriptions(
  id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL,endpoint TEXT NOT NULL,subscription_json TEXT NOT NULL,created_at TEXT NOT NULL,last_ok_at TEXT,
  UNIQUE(user_id,endpoint));
CREATE TABLE IF NOT EXISTS push_config(k TEXT PRIMARY KEY,v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS push_delivery_log(
  id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL,created_at TEXT NOT NULL,event_type TEXT NOT NULL,status TEXT NOT NULL,http_status INTEGER,error TEXT,endpoint_tail TEXT);
CREATE TABLE IF NOT EXISTS copilot_sessions(
  id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'IDLE',
  strategy TEXT NOT NULL DEFAULT 'scalp',symbol TEXT,side TEXT,planned_entry_low DOUBLE PRECISION,planned_entry_high DOUBLE PRECISION,actual_entry DOUBLE PRECISION,stop DOUBLE PRECISION,tp1 DOUBLE PRECISION,tp2 DOUBLE PRECISION,size DOUBLE PRECISION,
  opened_at TEXT,closed_at TEXT,close_price DOUBLE PRECISION,close_reason TEXT,last_price DOUBLE PRECISION,last_r DOUBLE PRECISION DEFAULT 0,setup_confidence INTEGER,setup_score DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS trades(
  id BIGSERIAL PRIMARY KEY,user_id BIGINT NOT NULL,opened_at TEXT NOT NULL,opened_ts BIGINT,symbol TEXT NOT NULL,side TEXT NOT NULL,
  confidence INTEGER NOT NULL,weighted_score DOUBLE PRECISION NOT NULL,alignment INTEGER NOT NULL,entry DOUBLE PRECISION NOT NULL,stop DOUBLE PRECISION NOT NULL,tp1 DOUBLE PRECISION NOT NULL,tp2 DOUBLE PRECISION NOT NULL,risk DOUBLE PRECISION NOT NULL,
  status TEXT NOT NULL DEFAULT 'OPEN',tp1_hit INTEGER NOT NULL DEFAULT 0,closed_at TEXT,close_reason TEXT,close_price DOUBLE PRECISION,bars_open INTEGER NOT NULL DEFAULT 0,
  max_favourable_r DOUBLE PRECISION NOT NULL DEFAULT 0,max_adverse_r DOUBLE PRECISION NOT NULL DEFAULT 0,strategy TEXT NOT NULL DEFAULT 'scalp',copilot_session_id BIGINT,realised_r DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS user_subscriptions(
  user_id BIGINT PRIMARY KEY REFERENCES users(id),plan TEXT NOT NULL DEFAULT 'BETA',status TEXT NOT NULL DEFAULT 'inactive',
  stripe_customer_id TEXT UNIQUE,stripe_subscription_id TEXT UNIQUE,stripe_price_id TEXT,current_period_end TEXT,cancel_at_period_end INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS stripe_events(
  event_id TEXT PRIMARY KEY,event_type TEXT NOT NULL,created_at TEXT NOT NULL,processed_at TEXT NOT NULL);
"""

class PGCompatConnection:
    def __init__(self, con):
        self._con = con
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._con.commit()
        else:
            self._con.rollback()
        self._con.close()
    @staticmethod
    def _sql(sql: str) -> str:
        # Existing application SQL uses sqlite qmark placeholders.
        return sql.replace("?", "%s")
    def execute(self, sql: str, params=()):
        return self._con.execute(self._sql(sql), params)
    def executemany(self, sql: str, params_seq):
        return self._con.executemany(self._sql(sql), params_seq)
    def commit(self):
        self._con.commit()
    def rollback(self):
        self._con.rollback()
    def close(self):
        self._con.close()



def _migrate_sqlite_to_postgres_if_needed(pg) -> None:
    """One-time bootstrap from the existing Railway SQLite volume.

    Migration only runs when PostgreSQL has no users, so it is safe on later
    deploys. Provider keys stay encrypted; no plaintext credentials are copied.
    """
    if not DB.exists():
        return
    try:
        if int(pg.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]) > 0:
            return
        src = sqlite3.connect(DB)
        src.row_factory = sqlite3.Row
        if int(src.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]) == 0:
            src.close()
            return
        tables = [
            "users", "auth_sessions", "user_settings", "user_connections",
            "snapshots", "candles_1m", "notifications", "push_subscriptions",
            "push_config", "push_delivery_log", "copilot_sessions", "trades",
        ]
        pg.execute("SET CONSTRAINTS ALL DEFERRED")
        copied = {}
        for table in tables:
            try:
                rows = src.execute(f"SELECT * FROM {table}").fetchall()
            except sqlite3.OperationalError:
                continue
            if not rows:
                copied[table] = 0
                continue
            cols = list(rows[0].keys())
            col_sql = ",".join(cols)
            marks = ",".join(["%s"] * len(cols))
            stmt = f"INSERT INTO {table} ({col_sql}) VALUES ({marks}) ON CONFLICT DO NOTHING"
            for row in rows:
                pg.execute(stmt, tuple(row[c] for c in cols))
            copied[table] = len(rows)
        for table in ("users","snapshots","notifications","push_subscriptions","push_delivery_log","copilot_sessions","trades"):
            pg.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}','id'), COALESCE((SELECT MAX(id) FROM {table}),1), COALESCE((SELECT MAX(id) FROM {table}),0) > 0)"
            )
        pg.commit()
        src.close()
        print(f"[DB-MIGRATION] SQLite -> PostgreSQL complete: {copied}", flush=True)
    except Exception as exc:
        pg.rollback()
        print(f"[DB-MIGRATION-ERROR] {type(exc).__name__}: {exc}", flush=True)
        raise

def _ensure_pg_schema(con) -> None:
    global _PG_SCHEMA_READY
    if _PG_SCHEMA_READY:
        return
    with _PG_SCHEMA_LOCK:
        if _PG_SCHEMA_READY:
            return
        con.execute(PG_SCHEMA)
        con.execute("ALTER TABLE users ALTER COLUMN plan SET DEFAULT 'BETA'")
        con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS copilot_session_id BIGINT")
        con.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS realised_r DOUBLE PRECISION")
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_session_unique ON trades(copilot_session_id) WHERE copilot_session_id IS NOT NULL")
        con.commit()
        _migrate_sqlite_to_postgres_if_needed(con)
        _PG_SCHEMA_READY = True


def db():
    if DATABASE_URL:
        if psycopg is None:
            raise RuntimeError("DATABASE_URL is configured but psycopg is not installed")
        raw = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        _ensure_pg_schema(raw)
        return PGCompatConnection(raw)

    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    con.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS users(
      id INTEGER PRIMARY KEY AUTOINCREMENT,email TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,password_salt TEXT NOT NULL,
      display_name TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,plan TEXT NOT NULL DEFAULT 'BETA',is_active INTEGER NOT NULL DEFAULT 1);
    CREATE TABLE IF NOT EXISTS auth_sessions(
      token_hash TEXT PRIMARY KEY,user_id INTEGER NOT NULL,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id));
    CREATE TABLE IF NOT EXISTS user_settings(user_id INTEGER NOT NULL,k TEXT NOT NULL,v TEXT NOT NULL,PRIMARY KEY(user_id,k));
    CREATE TABLE IF NOT EXISTS user_connections(
      user_id INTEGER PRIMARY KEY,tradingview_token TEXT UNIQUE NOT NULL,fred_key_enc TEXT,databento_key_enc TEXT,
      created_at TEXT NOT NULL,updated_at TEXT NOT NULL,FOREIGN KEY(user_id) REFERENCES users(id));
    CREATE TABLE IF NOT EXISTS snapshots(
      id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,received_at TEXT NOT NULL,symbol TEXT,raw_json TEXT NOT NULL,result_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS candles_1m(
      user_id INTEGER NOT NULL,ts INTEGER NOT NULL,symbol TEXT,o REAL NOT NULL,h REAL NOT NULL,l REAL NOT NULL,c REAL NOT NULL,vol REAL,
      PRIMARY KEY(user_id,ts));
    CREATE INDEX IF NOT EXISTS idx_candles_1m_user_ts ON candles_1m(user_id,ts);
    CREATE TABLE IF NOT EXISTS notifications(
      id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,created_at TEXT NOT NULL,event_type TEXT NOT NULL,title TEXT NOT NULL,body TEXT NOT NULL,dedupe_key TEXT NOT NULL,
      UNIQUE(user_id,dedupe_key));
    CREATE TABLE IF NOT EXISTS push_subscriptions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,endpoint TEXT NOT NULL,subscription_json TEXT NOT NULL,created_at TEXT NOT NULL,last_ok_at TEXT,
      UNIQUE(user_id,endpoint));
    CREATE TABLE IF NOT EXISTS push_config(k TEXT PRIMARY KEY,v TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS push_delivery_log(
      id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,created_at TEXT NOT NULL,
      event_type TEXT NOT NULL,status TEXT NOT NULL,http_status INTEGER,error TEXT,
      endpoint_tail TEXT
    );
    CREATE TABLE IF NOT EXISTS copilot_sessions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'IDLE',
      strategy TEXT NOT NULL DEFAULT 'scalp',symbol TEXT,side TEXT,planned_entry_low REAL,planned_entry_high REAL,actual_entry REAL,stop REAL,tp1 REAL,tp2 REAL,size REAL,
      opened_at TEXT,closed_at TEXT,close_price REAL,close_reason TEXT,last_price REAL,last_r REAL DEFAULT 0,setup_confidence INTEGER,setup_score REAL);
    CREATE TABLE IF NOT EXISTS trades(
      id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,opened_at TEXT NOT NULL,opened_ts INTEGER,symbol TEXT NOT NULL,side TEXT NOT NULL,
      confidence INTEGER NOT NULL,weighted_score REAL NOT NULL,alignment INTEGER NOT NULL,entry REAL NOT NULL,stop REAL NOT NULL,tp1 REAL NOT NULL,tp2 REAL NOT NULL,risk REAL NOT NULL,
      status TEXT NOT NULL DEFAULT 'OPEN',tp1_hit INTEGER NOT NULL DEFAULT 0,closed_at TEXT,close_reason TEXT,close_price REAL,bars_open INTEGER NOT NULL DEFAULT 0,
      max_favourable_r REAL NOT NULL DEFAULT 0,max_adverse_r REAL NOT NULL DEFAULT 0,strategy TEXT NOT NULL DEFAULT 'scalp');
    CREATE TABLE IF NOT EXISTS user_subscriptions(
      user_id INTEGER PRIMARY KEY,plan TEXT NOT NULL DEFAULT 'BETA',status TEXT NOT NULL DEFAULT 'inactive',
      stripe_customer_id TEXT UNIQUE,stripe_subscription_id TEXT UNIQUE,stripe_price_id TEXT,current_period_end TEXT,cancel_at_period_end INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS stripe_events(
      event_id TEXT PRIMARY KEY,event_type TEXT NOT NULL,created_at TEXT NOT NULL,processed_at TEXT NOT NULL);
    """)
    con.commit()
    return con


def _subscription_row(user_id: int, con: Any | None = None) -> dict[str, Any] | None:
    own = con is None
    con = con or db()
    r = con.execute("SELECT * FROM user_subscriptions WHERE user_id=?", (user_id,)).fetchone()
    if own:
        con.close()
    return dict(r) if r else None


def _is_entitled(user: dict[str, Any], sub: dict[str, Any] | None = None) -> bool:
    # Existing ALPHA accounts are founder/grandfathered accounts and remain enabled.
    if (user.get("plan") or "").upper() == "ALPHA":
        return True
    status = (sub or {}).get("status", "inactive")
    return status in {"active", "trialing", "past_due"}


def _billing_status(user: dict[str, Any]) -> dict[str, Any]:
    sub = _subscription_row(int(user["id"]))
    return {
        "plan": (sub or {}).get("plan") or user.get("plan") or "BETA",
        "status": (sub or {}).get("status") or ("grandfathered" if (user.get("plan") or "").upper() == "ALPHA" else "inactive"),
        "entitled": _is_entitled(user, sub),
        "cancel_at_period_end": bool((sub or {}).get("cancel_at_period_end")),
        "current_period_end": (sub or {}).get("current_period_end"),
        "stripe_configured": bool(STRIPE_SECRET_KEY and STRIPE_BETA_PRICE_ID),
        "beta_price_gbp": 29.99,
        "access_message": "Founder ALPHA access" if (user.get("plan") or "").upper() == "ALPHA" else ("Subscription active" if _is_entitled(user, sub) else "Subscribe to unlock the live decision engine"),
        "data_disclosure": "Third-party data is not included. Beta requires your own compatible TradingView access and your own Databento subscription/API key. FRED can use a free API key.",
    }


def _stripe_dt(epoch: Any) -> str | None:
    try:
        return datetime.fromtimestamp(int(epoch), timezone.utc).isoformat() if epoch else None
    except Exception:
        return None


def _upsert_subscription(con: Any, user_id: int, *, plan: str = "BETA", status: str | None = None, customer_id: str | None = None, subscription_id: str | None = None, price_id: str | None = None, current_period_end: str | None = None, cancel_at_period_end: bool | None = None) -> None:
    existing = _subscription_row(user_id, con) or {}
    stamp = now_iso()
    con.execute(
        """INSERT INTO user_subscriptions(user_id,plan,status,stripe_customer_id,stripe_subscription_id,stripe_price_id,current_period_end,cancel_at_period_end,created_at,updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET plan=excluded.plan,status=excluded.status,stripe_customer_id=COALESCE(excluded.stripe_customer_id,user_subscriptions.stripe_customer_id),
             stripe_subscription_id=COALESCE(excluded.stripe_subscription_id,user_subscriptions.stripe_subscription_id),stripe_price_id=COALESCE(excluded.stripe_price_id,user_subscriptions.stripe_price_id),
             current_period_end=COALESCE(excluded.current_period_end,user_subscriptions.current_period_end),cancel_at_period_end=excluded.cancel_at_period_end,updated_at=excluded.updated_at""",
        (user_id, plan, status or existing.get("status") or "inactive", customer_id, subscription_id, price_id, current_period_end, int(cancel_at_period_end if cancel_at_period_end is not None else bool(existing.get("cancel_at_period_end"))), existing.get("created_at") or stamp, stamp),
    )


def _stripe_user_id(con: Any, obj: dict[str, Any]) -> int | None:
    md = obj.get("metadata") or {}
    raw = md.get("user_id") or obj.get("client_reference_id")
    try:
        if raw:
            return int(raw)
    except Exception:
        pass
    customer = obj.get("customer")
    subscription = obj.get("subscription") or obj.get("id") if str(obj.get("object", "")) == "subscription" else obj.get("subscription")
    row = None
    if customer:
        row = con.execute("SELECT user_id FROM user_subscriptions WHERE stripe_customer_id=?", (str(customer),)).fetchone()
    if not row and subscription:
        row = con.execute("SELECT user_id FROM user_subscriptions WHERE stripe_subscription_id=?", (str(subscription),)).fetchone()
    return int(row["user_id"]) if row else None


def _pwd_hash(password: str, salt: bytes) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000).hex()


def _fernet() -> Fernet:
    key = base64.urlsafe_b64encode(hashlib.sha256(APP_SECRET.encode()).digest())
    return Fernet(key)


def _enc(value: str | None) -> str | None:
    if not value:
        return None
    return _fernet().encrypt(value.strip().encode()).decode()


def _dec(value: str | None) -> str:
    if not value:
        return ""
    try:
        return _fernet().decrypt(value.encode()).decode()
    except InvalidToken:
        return ""


def _session_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _user_from_session(token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    with db() as con:
        row = con.execute("""SELECT u.* FROM auth_sessions s JOIN users u ON u.id=s.user_id
                           WHERE s.token_hash=? AND s.expires_at>? AND u.is_active=1""", (_session_hash(token), now_iso())).fetchone()
    return dict(row) if row else None


def require_user(session: str | None) -> dict[str, Any]:
    u = _user_from_session(session)
    if not u:
        raise HTTPException(401, "Login required")
    return u


def require_entitled_user(session: str | None) -> dict[str, Any]:
    """Require login plus commercial access; ALPHA stays grandfathered."""
    u = require_user(session)
    sub = _subscription_row(int(u["id"]))
    if not _is_entitled(u, sub):
        raise HTTPException(402, "Active US30 Copilot BETA subscription required")
    return u


def _settings(user_id: int, con: Any | None = None) -> dict[str, Any]:
    own = con is None
    con = con or db()
    out = dict(DEFAULT_SETTINGS)
    for r in con.execute("SELECT k,v FROM user_settings WHERE user_id=?", (user_id,)):
        try:
            out[r["k"]] = json.loads(r["v"])
        except Exception:
            pass
    if own:
        con.close()
    return out


def _save_settings(user_id: int, values: dict[str, Any]) -> dict[str, Any]:
    with db() as con:
        for k, v in values.items():
            if k not in DEFAULT_SETTINGS:
                continue
            if k == "strategy" and v not in STRATEGIES:
                continue
            con.execute("""INSERT INTO user_settings(user_id,k,v) VALUES(?,?,?) ON CONFLICT(user_id,k) DO UPDATE SET v=excluded.v""", (user_id, k, json.dumps(v)))
        con.commit()
    return _settings(user_id)


def _connection(user_id: int, con: Any | None = None) -> dict[str, Any]:
    own = con is None
    con = con or db()
    r = con.execute("SELECT * FROM user_connections WHERE user_id=?", (user_id,)).fetchone()
    if own:
        con.close()
    if not r:
        return {}
    return dict(r)


def _calendar_for_user(user_id: int) -> EconomicCalendar:
    c = _connection(user_id)
    return EconomicCalendar(fred_key=_dec(c.get("fred_key_enc")))


def _latest_result(user_id: int, con: Any | None = None) -> dict[str, Any] | None:
    own = con is None
    con = con or db()
    r = con.execute("SELECT result_json FROM snapshots WHERE user_id=? ORDER BY id DESC LIMIT 1", (user_id,)).fetchone()
    if own:
        con.close()
    if not r:
        return None
    try:
        return json.loads(r["result_json"])
    except Exception:
        return None


def _active_copilot(user_id: int, con: Any | None = None) -> dict[str, Any] | None:
    own = con is None
    con = con or db()
    r = con.execute("SELECT * FROM copilot_sessions WHERE user_id=? AND status!='CLOSED' ORDER BY id DESC LIMIT 1", (user_id,)).fetchone()
    if own:
        con.close()
    return dict(r) if r else None


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


_P256_ORDER = int("FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551", 16)


def _vapid_keys(con: Any | None = None) -> tuple[str, str, str]:
    """Return (private PEM, public P-256 base64url, source).

    Railway VAPID_* variables win. Otherwise a stable key is derived from
    APP_SECRET so redeploys do not invalidate browser subscriptions.
    """
    env_priv = os.getenv("VAPID_PRIVATE_KEY", "").strip().replace("\\n", "\n")
    env_pub = os.getenv("VAPID_PUBLIC_KEY", "").strip()
    if env_priv and env_pub:
        # pywebpush treats a string value as base64url DER/raw, not as inline PEM.
        # Convert an inline PEM Railway variable to the 32-byte raw private scalar.
        if "-----BEGIN" in env_priv:
            env_key = serialization.load_pem_private_key(env_priv.encode("utf-8"), password=None)
            env_priv = _b64url(env_key.private_numbers().private_value.to_bytes(32, "big"))
        return env_priv, env_pub, "env"
    seed = hashlib.sha256(("US30-COPILOT-VAPID|" + APP_SECRET).encode()).digest()
    private_value = (int.from_bytes(seed, "big") % (_P256_ORDER - 1)) + 1
    key = ec.derive_private_key(private_value, ec.SECP256R1())
    # IMPORTANT: pywebpush passes non-file strings to py_vapid.from_string().
    # A 32-byte base64url raw scalar is the unambiguous supported representation.
    priv = _b64url(private_value.to_bytes(32, "big"))
    nums = key.public_key().public_numbers()
    pub = _b64url(b"\x04" + nums.x.to_bytes(32, "big") + nums.y.to_bytes(32, "big"))
    return priv, pub, "app_secret"


def _push_key_id() -> str:
    _, pub, _ = _vapid_keys()
    return hashlib.sha256(pub.encode()).hexdigest()[:12]


def _log_push(con, user_id, event_type, status, endpoint="", http_status=None, error=None):
    con.execute("""INSERT INTO push_delivery_log(user_id,created_at,event_type,status,http_status,error,endpoint_tail)
                   VALUES(?,?,?,?,?,?,?)""",
                (user_id, now_iso(), event_type, status, http_status, (error or "")[:800] or None, endpoint[-28:] if endpoint else None))


def _send_web_push(con: Any, user_id: int, title: str, body: str, event_type: str) -> dict[str, Any]:
    rows = con.execute("SELECT id,endpoint,subscription_json FROM push_subscriptions WHERE user_id=?", (user_id,)).fetchall()
    if not rows:
        _log_push(con,user_id,event_type,"NO_SUBSCRIPTION",error="No device push subscription registered")
        return {"ok":False,"attempted":0,"sent":0,"failed":0,"error":"No device subscription registered"}
    try:
        priv, _, source = _vapid_keys(con)
    except Exception as exc:
        msg=f"VAPID key error: {type(exc).__name__}: {exc}"
        _log_push(con,user_id,event_type,"VAPID_ERROR",error=msg)
        return {"ok":False,"attempted":len(rows),"sent":0,"failed":len(rows),"error":msg}
    payload=json.dumps({"title":title,"body":body,"event_type":event_type,"url":"/","sent_at":now_iso()})
    sent=0; failed=0; errors=[]
    for r in rows:
        endpoint=r["endpoint"] or ""
        try:
            webpush(subscription_info=json.loads(r["subscription_json"]),data=payload,vapid_private_key=priv,vapid_claims={"sub":VAPID_SUBJECT},ttl=300)
            sent+=1
            con.execute("UPDATE push_subscriptions SET last_ok_at=? WHERE id=?",(now_iso(),r["id"]))
            _log_push(con,user_id,event_type,"SENT",endpoint=endpoint)
        except WebPushException as exc:
            failed+=1
            response=getattr(exc,"response",None)
            code=getattr(response,"status_code",None)
            detail=str(exc)
            try:
                if response is not None and getattr(response,"text",None): detail += " | "+str(response.text)[:400]
            except Exception: pass
            errors.append(f"{code or 'push'}: {detail}"[:500])
            _log_push(con,user_id,event_type,"FAILED",endpoint,code,detail)
            if code in (404,410): con.execute("DELETE FROM push_subscriptions WHERE id=?",(r["id"],))
        except Exception as exc:
            failed+=1
            detail=f"{type(exc).__name__}: {exc}"
            errors.append(detail[:500])
            _log_push(con,user_id,event_type,"FAILED",endpoint,None,detail)
    return {"ok":sent>0,"attempted":len(rows),"sent":sent,"failed":failed,"vapid_source":source,"vapid_key_id":_push_key_id(),"errors":errors[:3]}


def _notify(con: Any, user_id: int, event_type: str, title: str, body: str, dedupe: str) -> None:
    s = _settings(user_id, con)
    map_key = {"SETUP":"notify_setup","ENTRY_READY":"notify_entry_ready","TRADE_UPDATE":"notify_trade_update","EXIT_WARNING":"notify_exit_warning","TP1":"notify_tp1","TP2":"notify_tp2","STOP":"notify_stop","NEWS":"notify_news"}
    key = map_key.get(event_type)
    if key and not s.get(key, True):
        return
    try:
        con.execute("INSERT INTO notifications(user_id,created_at,event_type,title,body,dedupe_key) VALUES(?,?,?,?,?,?)", (user_id, now_iso(), event_type, title, body, dedupe))
    except (sqlite3.IntegrityError, UniqueViolation):
        return
    if s.get("browser_notifications", True):
        _send_web_push(con, user_id, title, body, event_type)


def _one_minute_bar(payload: dict[str, Any]) -> dict[str, float] | None:
    for f in payload.get("frames") or []:
        if isinstance(f, dict) and f.get("tf") == "1m":
            try:
                return {k: float(f[k]) for k in ("o", "h", "l", "c")}
            except Exception:
                return None
    return None


def _entry_assessment(result: dict[str, Any], sess: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = get_strategy(result.get("strategy"))
    signal = result.get("signal", "WAIT")
    conf = int(result.get("confidence") or 0)
    alignment = int(result.get("alignment") or 0)
    total = max(1, int(result.get("alignment_total") or len(cfg.weights)))
    score = 0
    reasons: list[str] = []
    threshold = {"scalp":72,"intraday":70,"swing":68}.get(cfg.name, 70)
    if signal in ("LONG", "SHORT"):
        score += 30; reasons.append("directional structure present")
    if conf >= threshold:
        score += 30; reasons.append(f"engine score {conf}% meets threshold")
    elif conf >= threshold - 5:
        score += 18; reasons.append("engine score near threshold")
    score += round(25 * alignment / total)

    # Once a setup is ARMED, readiness must reference that immutable setup,
    # not a newly recalculated entry zone from a later snapshot.
    use_armed_plan = bool(sess and sess.get("status") in ("ARMED", "LIVE") and sess.get("planned_entry_low") is not None and sess.get("planned_entry_high") is not None)
    lo = sess.get("planned_entry_low") if use_armed_plan else result.get("entry_low")
    hi = sess.get("planned_entry_high") if use_armed_plan else result.get("entry_high")
    px = float(result.get("price") or 0)
    inside = False
    if lo is not None and hi is not None and px:
        low, high = sorted((float(lo), float(hi)))
        inside = low <= px <= high
        if inside:
            score += 15
            reasons.append("price inside armed entry zone" if use_armed_plan else "price inside planned entry zone")
    if result.get("news_block"):
        score = 0; reasons.append("event-risk block active")
    quality = "BLOCKED" if result.get("news_block") else "READY" if score >= 80 and signal in ("LONG","SHORT") else "NEAR" if score >= 65 and signal in ("LONG","SHORT") else "WAIT"
    return {
        "score": min(100, score), "quality": quality, "inside_entry": inside, "reasons": reasons,
        "entry_low": float(lo) if lo is not None else None,
        "entry_high": float(hi) if hi is not None else None,
        "entry_source": "ARMED_PLAN" if use_armed_plan else "LIVE_ENGINE",
    }



def _alignment_count(result: dict[str, Any]) -> int:
    a = result.get("alignment")
    if isinstance(a, dict):
        for k in ("aligned", "count", "agreement"):
            if a.get(k) is not None:
                try: return int(a[k])
                except Exception: pass
    try: return int(result.get("alignment_count") or result.get("aligned_timeframes") or 0)
    except Exception: return 0


def _ensure_validation_trade(con: Any, user_id: int, sess: dict[str, Any], result: dict[str, Any], entry: float):
    existing = con.execute("SELECT * FROM trades WHERE copilot_session_id=? LIMIT 1", (sess["id"],)).fetchone()
    if existing: return dict(existing)
    side = (sess.get("side") or result.get("signal") or "").upper()
    stop=float(sess.get("stop") or result.get("stop") or 0); tp1=float(sess.get("tp1") or result.get("tp1") or 0); tp2=float(sess.get("tp2") or result.get("tp2") or 0); entry=float(entry or 0)
    if side not in ("LONG","SHORT") or min(entry,stop,tp1,tp2)<=0: return None
    risk=abs(entry-stop)
    if risk<=0: return None
    ts=int(result.get("ts") or int(time.time()*1000))
    con.execute("""INSERT INTO trades(user_id,opened_at,opened_ts,symbol,side,confidence,weighted_score,alignment,entry,stop,tp1,tp2,risk,status,tp1_hit,bars_open,max_favourable_r,max_adverse_r,strategy,copilot_session_id)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',0,0,0,0,?,?)""",
                (user_id,now_iso(),ts,sess.get("symbol") or result.get("symbol") or "US30",side,int(sess.get("setup_confidence") or result.get("confidence") or 0),float(sess.get("setup_score") or result.get("weighted_score") or 0),_alignment_count(result),entry,stop,tp1,tp2,risk,sess.get("strategy") or result.get("strategy") or "scalp",sess["id"]))
    row=con.execute("SELECT * FROM trades WHERE copilot_session_id=? LIMIT 1",(sess["id"],)).fetchone()
    return dict(row) if row else None


def _advance_validation_trade(con: Any, trade: dict[str, Any], bar: dict[str, Any], px: float) -> None:
    if not trade or trade.get("status")!="OPEN": return
    side=trade["side"]; entry=float(trade["entry"]); stop=float(trade["stop"]); tp1=float(trade["tp1"]); tp2=float(trade["tp2"]); risk=max(float(trade["risk"]),1e-9)
    high=float(bar.get("h",px)); low=float(bar.get("l",px))
    fav=(high-entry)/risk if side=="LONG" else (entry-low)/risk
    adv=(entry-low)/risk if side=="LONG" else (high-entry)/risk
    mfe=max(float(trade.get("max_favourable_r") or 0),fav); mae=max(float(trade.get("max_adverse_r") or 0),adv)
    tp1_hit=bool(trade.get("tp1_hit")) or (high>=tp1 if side=="LONG" else low<=tp1)
    stop_hit=low<=stop if side=="LONG" else high>=stop
    tp2_hit=high>=tp2 if side=="LONG" else low<=tp2
    # Conservative same-bar rule: unknown intrabar sequence is scored as STOP.
    if stop_hit:
        con.execute("UPDATE trades SET status='CLOSED',tp1_hit=?,closed_at=?,close_reason='STOP',close_price=?,bars_open=bars_open+1,max_favourable_r=?,max_adverse_r=?,realised_r=-1.0 WHERE id=?",(1 if tp1_hit else 0,now_iso(),stop,mfe,mae,trade["id"]))
    elif tp2_hit:
        rr=(tp2-entry)/risk if side=="LONG" else (entry-tp2)/risk
        con.execute("UPDATE trades SET status='CLOSED',tp1_hit=1,closed_at=?,close_reason='TP2',close_price=?,bars_open=bars_open+1,max_favourable_r=?,max_adverse_r=?,realised_r=? WHERE id=?",(now_iso(),tp2,mfe,mae,rr,trade["id"]))
    else:
        con.execute("UPDATE trades SET tp1_hit=?,bars_open=bars_open+1,max_favourable_r=?,max_adverse_r=? WHERE id=?",(1 if tp1_hit else 0,mfe,mae,trade["id"]))


def _advance_open_validation_trades(con: Any, user_id: int, bar: dict[str, Any], px: float) -> None:
    """Engine validation is independent of the user's manual Live Trade state."""
    rows = con.execute("SELECT * FROM trades WHERE user_id=? AND status='OPEN' ORDER BY id", (user_id,)).fetchall()
    for row in rows:
        _advance_validation_trade(con, dict(row), bar, px)



def _process_copilot(con: Any, user_id: int, payload: dict[str, Any], result: dict[str, Any]) -> None:
    px = float(result.get("price") or 0)
    bar = _one_minute_bar(payload) or {"h":px,"l":px}
    # Engine-validation positions continue to their objective outcome even if
    # the user never presses Live Trade, changes state, or closes manual tracking.
    _advance_open_validation_trades(con, user_id, bar, px)

    sess = _active_copilot(user_id, con)
    if not sess or sess["status"] == "IDLE":
        return
    assess = _entry_assessment(result, sess)
    cfg = get_strategy(result.get("strategy"))
    ts_bucket = int(result.get("ts") or 0) // 60000
    if sess["status"] == "LOOKING" and assess["quality"] in ("READY", "NEAR") and result.get("signal") in ("LONG", "SHORT"):
        _notify(con, user_id, "SETUP", f"{cfg.label} opportunity detected", f"{result['signal']} · readiness {assess['score']}/100 · engine score {result['confidence']}%", f"setup:{sess['id']}:{result['signal']}:{int(result['confidence'])//5}")
        if _settings(user_id, con).get("auto_arm", True) and assess["score"] >= 72:
            con.execute("""UPDATE copilot_sessions SET status='ARMED',updated_at=?,strategy=?,symbol=?,side=?,planned_entry_low=?,planned_entry_high=?,stop=?,tp1=?,tp2=?,setup_confidence=?,setup_score=? WHERE id=?""",
                        (now_iso(), cfg.name, result.get("symbol"), result.get("signal"), result.get("entry_low"), result.get("entry_high"), result.get("stop"), result.get("tp1"), result.get("tp2"), result.get("confidence"), result.get("weighted_score"), sess["id"]))
            sess = dict(con.execute("SELECT * FROM copilot_sessions WHERE id=?", (sess["id"],)).fetchone())
    if sess["status"] == "ARMED" and result.get("signal") == sess.get("side") and assess["quality"] == "READY" and assess["inside_entry"]:
        lo, hi = sorted((float(sess["planned_entry_low"]), float(sess["planned_entry_high"])))
        _notify(
            con, user_id, "ENTRY_READY", f"Entry conditions confirmed · {sess['side']}",
            f"Price {px:,.1f} is inside ARMED zone {lo:,.1f}–{hi:,.1f}. Stop {sess['stop']:,.1f} · TP1 {sess['tp1']:,.1f} · TP2 {sess['tp2']:,.1f}",
            f"ready:{sess['id']}:{ts_bucket}"
        )
        # Automatic ENGINE validation starts here. Manual live tracking does not.
        _ensure_validation_trade(con, user_id, sess, result, px)
    if sess["status"] != "LIVE":
        return
    side = sess.get("side")
    entry, stop = float(sess.get("actual_entry") or 0), float(sess.get("stop") or 0)
    if not side or not entry or not stop:
        return
    tp1, tp2 = float(sess.get("tp1") or 0), float(sess.get("tp2") or 0)
    risk = max(abs(entry - stop), 1e-9)
    r_now = (px-entry)/risk if side == "LONG" else (entry-px)/risk
    stop_hit = bar["l"] <= stop if side == "LONG" else bar["h"] >= stop
    tp1_hit = bar["h"] >= tp1 if side == "LONG" else bar["l"] <= tp1
    tp2_hit = bar["h"] >= tp2 if side == "LONG" else bar["l"] <= tp2
    con.execute("UPDATE copilot_sessions SET updated_at=?,last_price=?,last_r=? WHERE id=?", (now_iso(), px, r_now, sess["id"]))
    if stop_hit:
        con.execute("UPDATE copilot_sessions SET status='CLOSED',closed_at=?,close_price=?,close_reason='STOP' WHERE id=?", (now_iso(), stop, sess["id"]))
        _notify(con, user_id, "STOP", "Trade invalidation reached", f"{side} position reached stop {stop:,.1f}", f"stop:{sess['id']}")
    elif tp2_hit:
        con.execute("UPDATE copilot_sessions SET status='CLOSED',closed_at=?,close_price=?,close_reason='TP2' WHERE id=?", (now_iso(), tp2, sess["id"]))
        _notify(con, user_id, "TP2", "Target 2 reached", f"{side} position reached {tp2:,.1f}", f"tp2:{sess['id']}")
    elif tp1_hit:
        _notify(con, user_id, "TP1", "Target 1 reached", f"{side} position reached {tp1:,.1f} · current approx {r_now:+.2f}R", f"tp1:{sess['id']}")
    else:
        adverse = float(result.get("weighted_score") or 0) if side == "SHORT" else -float(result.get("weighted_score") or 0)
        threshold = {"scalp":.7,"intraday":1.0,"swing":1.3}[cfg.name]
        if adverse >= threshold:
            _notify(con, user_id, "EXIT_WARNING", "Trade health deteriorating", f"{cfg.label} structure materially reversed · current {r_now:+.2f}R", f"exit:{sess['id']}:{ts_bucket//15}")


class RegisterBody(BaseModel):
    email: EmailStr
    password: str
    display_name: str = ""

class LoginBody(BaseModel):
    email: EmailStr
    password: str

class SettingsBody(BaseModel):
    strategy: str | None = None
    browser_notifications: bool | None = None
    notify_setup: bool | None = None
    notify_entry_ready: bool | None = None
    notify_trade_update: bool | None = None
    notify_exit_warning: bool | None = None
    notify_tp1: bool | None = None
    notify_tp2: bool | None = None
    notify_stop: bool | None = None
    notify_news: bool | None = None
    auto_arm: bool | None = None

class ConnectionsBody(BaseModel):
    fred_api_key: str | None = None
    databento_api_key: str | None = None
    rotate_tradingview_webhook: bool = False

class StateBody(BaseModel):
    state: str

class LiveTradeBody(BaseModel):
    side: str | None = None
    entry: float | None = None
    stop: float | None = None
    tp1: float | None = None
    tp2: float | None = None
    size: float | None = None

class PushSubscriptionBody(BaseModel):
    endpoint: str
    expirationTime: float | None = None
    keys: dict[str, str]


def _create_login_response(user: dict[str, Any]) -> Response:
    token = secrets.token_urlsafe(32)
    exp = datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS)
    with db() as con:
        con.execute("INSERT INTO auth_sessions(token_hash,user_id,created_at,expires_at) VALUES(?,?,?,?)", (_session_hash(token), user["id"], now_iso(), exp.isoformat()))
        con.commit()
    resp = Response(content=json.dumps({"ok":True,"user":{"id":user["id"],"email":user["email"],"display_name":user["display_name"],"plan":user["plan"]}}), media_type="application/json")
    resp.set_cookie("session", token, httponly=True, secure=True, samesite="lax", max_age=SESSION_DAYS*86400, path="/")
    return resp


@app.get("/health")
def health():
    try:
        kid=_push_key_id(); source=_vapid_keys()[2]
    except Exception:
        kid=None; source="error"
    with _DATABENTO_LOCK:
        db_states = [dict((v.get("status") or {})) for v in _DATABENTO_STATE.values()]
    return {
        "ok":True,
        "service":APP_TITLE,
        "build_id":BUILD_ID,
        "commercial":True,
        "registration":ALLOW_REGISTRATION,
        "database":{"backend":DB_BACKEND,"path":str(DB) if DB_BACKEND=="sqlite" else None,"exists":DB.exists() if DB_BACKEND=="sqlite" else True},
        "app_secret_configured":APP_SECRET != "CHANGE-ME-COMMERCIAL-ALPHA",
        "push":{"vapid_ready":bool(kid),"key_id":kid,"source":source,"subject_configured":bool(VAPID_SUBJECT)},
        "stripe":{"configured":bool(STRIPE_SECRET_KEY and STRIPE_BETA_PRICE_ID),"webhook_secret_configured":bool(STRIPE_WEBHOOK_SECRET),"beta_price_configured":bool(STRIPE_BETA_PRICE_ID)},
        "databento":{
            "configured_users":sum(bool(x.get("configured")) for x in db_states),
            "live_users":sum(x.get("status")=="LIVE" for x in db_states),
        },
    }

@app.post("/auth/register")
def register(body: RegisterBody):
    if not ALLOW_REGISTRATION:
        raise HTTPException(403, "Registration closed")
    if len(body.password) < 10:
        raise HTTPException(422, "Password must be at least 10 characters")
    salt = os.urandom(16)
    email = body.email.lower().strip()
    with db() as con:
        try:
            cur = con.execute("INSERT INTO users(email,password_hash,password_salt,display_name,created_at,plan) VALUES(?,?,?,?,?,?) RETURNING id", (email, _pwd_hash(body.password, salt), salt.hex(), body.display_name.strip(), now_iso(), "BETA"))
        except (sqlite3.IntegrityError, UniqueViolation):
            raise HTTPException(409, "Email already registered")
        uid = int(cur.fetchone()["id"])
        token = secrets.token_hex(24)
        con.execute("INSERT INTO user_connections(user_id,tradingview_token,created_at,updated_at) VALUES(?,?,?,?)", (uid, token, now_iso(), now_iso()))
        # Commercial invariant: every account created through /auth/register is BETA.
        # Existing ALPHA rows are never touched; they remain grandfathered.
        forced = con.execute("UPDATE users SET plan=? WHERE id=? RETURNING *", ("BETA", uid)).fetchone()
        if not forced or str(forced["plan"]).upper() != "BETA":
            raise RuntimeError(f"Registration plan invariant failed for uid={uid}")
        _upsert_subscription(con, uid, plan="BETA", status="inactive")
        for k,v in DEFAULT_SETTINGS.items():
            con.execute("INSERT INTO user_settings(user_id,k,v) VALUES(?,?,?)", (uid,k,json.dumps(v)))
        con.commit()
        user = dict(con.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone())
        if str(user.get("plan") or "").upper() != "BETA":
            raise RuntimeError(f"Registration committed with unexpected plan for uid={uid}: {user.get('plan')}")
    print(f"[REGISTER] uid={uid} plan={user.get('plan')} email_domain={email.rsplit('@',1)[-1] if '@' in email else 'unknown'} build={BUILD_ID}", flush=True)
    return _create_login_response(user)

@app.post("/auth/login")
def login(body: LoginBody):
    with db() as con:
        r = con.execute("SELECT * FROM users WHERE email=? AND is_active=1", (body.email.lower().strip(),)).fetchone()
    if not r:
        raise HTTPException(401, "Invalid email or password")
    salt = bytes.fromhex(r["password_salt"])
    if not hmac.compare_digest(_pwd_hash(body.password, salt), r["password_hash"]):
        raise HTTPException(401, "Invalid email or password")
    return _create_login_response(dict(r))

@app.post("/auth/logout")
def logout(session: str | None = Cookie(default=None)):
    if session:
        with db() as con:
            con.execute("DELETE FROM auth_sessions WHERE token_hash=?", (_session_hash(session),)); con.commit()
    resp = Response(content='{"ok":true}', media_type="application/json")
    resp.delete_cookie("session", path="/")
    return resp

@app.get("/auth/me")
def me(session: str | None = Cookie(default=None)):
    u = require_user(session)
    return {"ok":True,"user":{"id":u["id"],"email":u["email"],"display_name":u["display_name"],"plan":u["plan"]},"billing":_billing_status(u)}

@app.get("/api/billing/status")
def billing_status(session: str | None = Cookie(default=None)):
    u = require_user(session)
    return {"ok": True, "billing": _billing_status(u)}

@app.post("/api/billing/checkout")
def billing_checkout(request: Request, session: str | None = Cookie(default=None)):
    u = require_user(session)
    if (u.get("plan") or "").upper() == "ALPHA":
        raise HTTPException(409, "This account already has grandfathered ALPHA access")
    if not STRIPE_SECRET_KEY or not STRIPE_BETA_PRICE_ID:
        raise HTTPException(503, "Stripe billing is not configured yet")
    sub = _subscription_row(int(u["id"]))
    if _is_entitled(u, sub):
        raise HTTPException(409, "Subscription is already active")
    base = PUBLIC_URL or str(request.base_url).rstrip("/")
    kwargs = {
        "mode": "subscription",
        "line_items": [{"price": STRIPE_BETA_PRICE_ID, "quantity": 1}],
        "success_url": f"{base}/?billing=success",
        "cancel_url": f"{base}/?billing=cancelled",
        "client_reference_id": str(u["id"]),
        "customer_email": u["email"],
        "metadata": {"user_id": str(u["id"]), "plan": "BETA"},
        "subscription_data": {"metadata": {"user_id": str(u["id"]), "plan": "BETA"}},
        "allow_promotion_codes": True,
    }
    try:
        cs = stripe.checkout.Session.create(**kwargs)
    except Exception as exc:
        raise HTTPException(502, f"Stripe Checkout error: {type(exc).__name__}") from exc
    return {"ok": True, "url": cs.url}

@app.post("/api/billing/portal")
def billing_portal(request: Request, session: str | None = Cookie(default=None)):
    u = require_user(session)
    if not STRIPE_SECRET_KEY:
        raise HTTPException(503, "Stripe billing is not configured yet")
    sub = _subscription_row(int(u["id"]))
    customer = (sub or {}).get("stripe_customer_id")
    if not customer:
        raise HTTPException(409, "No Stripe customer is linked to this account yet")
    base = PUBLIC_URL or str(request.base_url).rstrip("/")
    try:
        ps = stripe.billing_portal.Session.create(customer=customer, return_url=f"{base}/")
    except Exception as exc:
        raise HTTPException(502, f"Stripe portal error: {type(exc).__name__}") from exc
    return {"ok": True, "url": ps.url}

@app.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(503, "Stripe webhook secret is not configured")
    payload = await request.body()
    signature = request.headers.get("stripe-signature", "")
    try:
        event = stripe.Webhook.construct_event(payload, signature, STRIPE_WEBHOOK_SECRET)
    except ValueError as exc:
        raise HTTPException(400, "Invalid Stripe payload") from exc
    except stripe.error.SignatureVerificationError as exc:
        raise HTTPException(400, "Invalid Stripe signature") from exc

    event_id = str(event.get("id") or "")
    event_type = str(event.get("type") or "")
    obj = dict((event.get("data") or {}).get("object") or {})
    with db() as con:
        if event_id and con.execute("SELECT event_id FROM stripe_events WHERE event_id=?", (event_id,)).fetchone():
            return {"ok": True, "duplicate": True}
        uid = _stripe_user_id(con, obj)
        if event_type == "checkout.session.completed" and uid:
            customer = obj.get("customer")
            subscription_id = obj.get("subscription")
            _upsert_subscription(con, uid, plan="BETA", status="active" if subscription_id else "inactive", customer_id=str(customer) if customer else None, subscription_id=str(subscription_id) if subscription_id else None, price_id=STRIPE_BETA_PRICE_ID)
        elif event_type.startswith("customer.subscription.") and uid:
            items = (((obj.get("items") or {}).get("data")) or [])
            price_id = None
            if items:
                price_id = ((items[0].get("price") or {}).get("id"))
            _upsert_subscription(con, uid, plan=(obj.get("metadata") or {}).get("plan") or "BETA", status=str(obj.get("status") or ("canceled" if event_type.endswith("deleted") else "inactive")), customer_id=str(obj.get("customer")) if obj.get("customer") else None, subscription_id=str(obj.get("id")) if obj.get("id") else None, price_id=price_id, current_period_end=_stripe_dt(obj.get("current_period_end")), cancel_at_period_end=bool(obj.get("cancel_at_period_end")))
        elif event_type in {"invoice.paid", "invoice.payment_failed"} and uid:
            status = "active" if event_type == "invoice.paid" else "past_due"
            _upsert_subscription(con, uid, status=status, customer_id=str(obj.get("customer")) if obj.get("customer") else None, subscription_id=str(obj.get("subscription")) if obj.get("subscription") else None)
        if event_id:
            con.execute("INSERT INTO stripe_events(event_id,event_type,created_at,processed_at) VALUES(?,?,?,?)", (event_id, event_type, _stripe_dt(event.get("created")) or now_iso(), now_iso()))
        con.commit()
    return {"ok": True}

@app.get("/api/connections")
def connections(request: Request, session: str | None = Cookie(default=None)):
    u = require_user(session); c = _connection(u["id"])
    base = PUBLIC_URL or str(request.base_url).rstrip("/")
    token = c.get("tradingview_token")
    # TradingView-safe migration: personal webhook tokens are hex-only.
    # Existing URL-safe-base64 tokens are rotated automatically once here.
    if token and not re.fullmatch(r"[0-9a-fA-F]+", token):
        token = secrets.token_hex(24)
        with db() as con:
            con.execute(
                """INSERT INTO user_connections(user_id,tradingview_token,created_at,updated_at)
                   VALUES(?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                     tradingview_token=excluded.tradingview_token,
                     updated_at=excluded.updated_at""",
                (u["id"], token, now_iso(), now_iso()),
            )
            con.commit()
    if not token:
        # Defensive migration for older/migrated accounts. The connection row
        # itself may be missing, so UPDATE alone is not sufficient.
        token = secrets.token_hex(24)
        stamp = now_iso()
        with db() as con:
            con.execute(
                """INSERT INTO user_connections(
                       user_id,tradingview_token,fred_key_enc,databento_key_enc,created_at,updated_at
                   ) VALUES(?,?,?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                       tradingview_token=excluded.tradingview_token,
                       updated_at=excluded.updated_at""",
                (
                    u["id"],
                    token,
                    c.get("fred_key_enc"),
                    c.get("databento_key_enc"),
                    c.get("created_at") or stamp,
                    stamp,
                ),
            )
            con.commit()
        c = _connection(u["id"])
    print(
        f"[TV-CONNECTIONS] backend={DB_BACKEND} uid={u['id']} "
        f"row={'yes' if c else 'no'} token_tail={token[-6:] if token else 'NONE'}",
        flush=True,
    )
    return {"ok":True,"connections":{
        "tradingview":{
            "configured":True,
            "webhook_url":f"{base}/webhook/tradingview/{token}",
            "token_tail":token[-6:],
            "personal":True
        },
        "fred":{"configured":bool(_dec(c.get("fred_key_enc"))),"masked":"••••••••" if c.get("fred_key_enc") else ""},
        "databento":{
            "configured":bool(_dec(c.get("databento_key_enc"))),
            "masked":"••••••••" if c.get("databento_key_enc") else "",
            "live":_fresh_orderflow(u["id"]).get("fresh", False),
            "status":_fresh_orderflow(u["id"]),
            "note":"Live per-account YM.FUT GLBX.MDP3 L1 (MBP-1) order flow"
        },
    }}

@app.post("/api/connections")
def save_connections(body: ConnectionsBody, session: str | None = Cookie(default=None)):
    u = require_user(session)
    with db() as con:
        c = _connection(u["id"], con)
        tv = secrets.token_hex(24) if body.rotate_tradingview_webhook else c.get("tradingview_token") or secrets.token_hex(24)
        fred = c.get("fred_key_enc") if body.fred_api_key is None else _enc(body.fred_api_key)
        dbento = c.get("databento_key_enc") if body.databento_api_key is None else _enc(body.databento_api_key)
        con.execute("""INSERT INTO user_connections(user_id,tradingview_token,fred_key_enc,databento_key_enc,created_at,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET tradingview_token=excluded.tradingview_token,fred_key_enc=excluded.fred_key_enc,databento_key_enc=excluded.databento_key_enc,updated_at=excluded.updated_at""", (u["id"],tv,fred,dbento,c.get("created_at") or now_iso(),now_iso()))
        con.commit()
    # If a Databento key was newly supplied, apply it immediately. If the key
    # was already stored, leave the running stream untouched.
    if body.databento_api_key is not None:
        _start_user_databento(u["id"], body.databento_api_key)
    elif bool(_dec(dbento)) and u["id"] not in _DATABENTO_MANAGERS:
        _start_user_databento(u["id"], _dec(dbento))
    return {"ok":True,"databento":_fresh_orderflow(u["id"])}

@app.get("/api/settings")
def get_settings(session: str | None = Cookie(default=None)):
    u = require_user(session)
    return {"ok":True,"settings":_settings(u["id"]),"strategies":{k:{"label":v.label,"horizon":v.horizon} for k,v in STRATEGIES.items()}}

@app.post("/api/settings")
def set_settings(body: SettingsBody, session: str | None = Cookie(default=None)):
    u = require_user(session)
    vals = {k:v for k,v in body.model_dump().items() if v is not None}
    return {"ok":True,"settings":_save_settings(u["id"], vals)}

async def _ingest_tradingview_for_user(uid: int, request: Request):
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(400, "Webhook body must be valid JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("frames"), list):
        raise HTTPException(422, "Expected object containing frames[]")
    settings = _settings(uid)
    cal = _calendar_for_user(uid)
    news = await cal.context(settings["strategy"])
    result = aggregate(payload, strategy=settings["strategy"], news_ctx=news, orderflow_ctx=_fresh_orderflow(uid), manual_news_block=False)
    with db() as con:
        _process_copilot(con, uid, payload, result)
        if news.get("high_impact_near") and news.get("events"):
            e = news["events"][0]
            _notify(con, uid, "NEWS", "High-impact US event risk", f"{e.get('name','US event')} in {e.get('minutes',0)} min", f"news:{e.get('date')}:{e.get('name')}")
        con.execute("INSERT INTO snapshots(user_id,received_at,symbol,raw_json,result_json) VALUES(?,?,?,?,?)", (uid,now_iso(),str(payload.get("symbol","US30")),json.dumps(payload),json.dumps(result)))
        # Keep a compact persistent 1-minute OHLC store for the in-app chart.
        bar = _one_minute_bar(payload)
        if bar:
            try:
                ts_ms = int(payload.get("ts") or 0)
                frame_1m = next((f for f in (payload.get("frames") or []) if isinstance(f, dict) and f.get("tf") == "1m"), {})
                vol = frame_1m.get("vol")
                if ts_ms > 0:
                    con.execute(
                        """INSERT INTO candles_1m(user_id,ts,symbol,o,h,l,c,vol) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,ts) DO UPDATE SET symbol=excluded.symbol,o=excluded.o,h=excluded.h,l=excluded.l,c=excluded.c,vol=excluded.vol""",
                        (uid, ts_ms, str(payload.get("symbol","US30")), bar["o"], bar["h"], bar["l"], bar["c"], float(vol) if vol is not None else None),
                    )
            except Exception as exc:
                print(f"[CHART-BAR-WARN] uid={uid} {type(exc).__name__}: {exc}", flush=True)
        con.commit()
    return {"ok":True,"result":{"signal":result.get("signal"),"confidence":result.get("confidence")}}

@app.post("/webhook/tradingview")
async def tradingview_unsecured(request: Request):
    # Legacy single-user compatibility route only.
    # Commercial/multi-user accounts MUST use /webhook/tradingview/{token}.
    # We never guess which account should receive an unsecured packet.
    with db() as con:
        users = con.execute("SELECT id FROM users WHERE is_active=1 ORDER BY id").fetchall()
    if not users:
        raise HTTPException(404, "No active user configured")
    if len(users) != 1:
        raise HTTPException(
            409,
            "Shared TradingView webhook disabled in multi-user mode. Use the personal webhook shown in Connections."
        )
    return await _ingest_tradingview_for_user(int(users[0]["id"]), request)

@app.post("/webhook/tradingview/{token}")
async def tradingview(token: str, request: Request):
    # Personal multi-user webhook. Diagnostics intentionally log only token
    # tails, never complete webhook tokens or provider credentials.
    incoming = (token or "").strip()
    with db() as con:
        c = con.execute(
            "SELECT user_id FROM user_connections WHERE tradingview_token=?",
            (incoming,),
        ).fetchone()
        if not c:
            rows = con.execute(
                "SELECT user_id,tradingview_token FROM user_connections ORDER BY user_id"
            ).fetchall()
            safe_rows = [
                {
                    "uid": int(r["user_id"]),
                    "tail": (r["tradingview_token"] or "")[-6:],
                    "len": len(r["tradingview_token"] or ""),
                }
                for r in rows
            ]
            print(
                f"[TV-WEBHOOK-MISS] backend={DB_BACKEND} "
                f"incoming_tail={incoming[-6:]} incoming_len={len(incoming)} "
                f"connections={safe_rows}",
                flush=True,
            )
            raise HTTPException(404, "Unknown webhook")
        uid = int(c["user_id"])
        print(
            f"[TV-WEBHOOK-HIT] backend={DB_BACKEND} uid={uid} incoming_tail={incoming[-6:]}",
            flush=True,
        )
    return await _ingest_tradingview_for_user(uid, request)

@app.get("/api/latest")
async def latest(session: str | None = Cookie(default=None)):
    u = require_entitled_user(session); uid = u["id"]
    with db() as con:
        r = con.execute("SELECT * FROM snapshots WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,)).fetchone()
        cp = _active_copilot(uid, con)
    if not r:
        return {"ok":True,"data":None,"settings":_settings(uid),"copilot":cp}
    raw = json.loads(r["raw_json"])
    settings = _settings(uid)
    news = await _calendar_for_user(uid).context(settings["strategy"])
    result = aggregate(raw, strategy=settings["strategy"], news_ctx=news, orderflow_ctx=_fresh_orderflow(uid), manual_news_block=False)
    return {"ok":True,"data":result,"received_at":r["received_at"],"settings":settings,"copilot":cp,"entry_assessment":_entry_assessment(result, cp)}

@app.post("/api/copilot/state")
def copilot_state(body: StateBody, session: str | None = Cookie(default=None)):
    u = require_entitled_user(session); uid = u["id"]
    st = body.state.upper().strip()
    if st not in {"IDLE","LOOKING","ARMED"}:
        raise HTTPException(422, "Invalid state")
    with db() as con:
        now = now_iso(); strat = _settings(uid, con)["strategy"]
        con.execute("UPDATE copilot_sessions SET status='CLOSED',closed_at=?,close_reason='RESET' WHERE user_id=? AND status!='CLOSED'", (now,uid))
        latest_result = _latest_result(uid, con)
        side = latest_result.get("signal") if latest_result else None
        if st == "ARMED" and side not in ("LONG","SHORT"):
            raise HTTPException(409, "No directional setup available")
        cur = con.execute("""INSERT INTO copilot_sessions(user_id,created_at,updated_at,status,strategy,symbol,side,planned_entry_low,planned_entry_high,stop,tp1,tp2,setup_confidence,setup_score)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id""", (uid,now,now,st,strat,(latest_result or {}).get("symbol"),side,(latest_result or {}).get("entry_low"),(latest_result or {}).get("entry_high"),(latest_result or {}).get("stop"),(latest_result or {}).get("tp1"),(latest_result or {}).get("tp2"),(latest_result or {}).get("confidence"),(latest_result or {}).get("weighted_score")))
        con.commit(); out=dict(con.execute("SELECT * FROM copilot_sessions WHERE id=?", (cur.lastrowid,)).fetchone())
    return {"ok":True,"session":out}

@app.post("/api/copilot/live")
def copilot_live(body: LiveTradeBody, session: str | None = Cookie(default=None)):
    u = require_entitled_user(session); uid=u["id"]
    with db() as con:
        cp = _active_copilot(uid, con)
        lr = _latest_result(uid, con)
        if not cp:
            now=now_iso();cur=con.execute("INSERT INTO copilot_sessions(user_id,created_at,updated_at,status,strategy) VALUES(?,?,?,?,?) RETURNING id", (uid,now,now,"LOOKING",_settings(uid,con)["strategy"])); new_id=int(cur.fetchone()["id"]); con.commit(); cp=dict(con.execute("SELECT * FROM copilot_sessions WHERE id=?",(new_id,)).fetchone())
        side=(body.side or cp.get("side") or (lr or {}).get("signal") or "").upper()
        if side not in ("LONG","SHORT"):
            raise HTTPException(422,"Side required")
        entry=float(body.entry if body.entry is not None else (lr or {}).get("price") or 0)
        stop=float(body.stop if body.stop is not None else cp.get("stop") or (lr or {}).get("stop") or 0)
        tp1=float(body.tp1 if body.tp1 is not None else cp.get("tp1") or (lr or {}).get("tp1") or 0)
        tp2=float(body.tp2 if body.tp2 is not None else cp.get("tp2") or (lr or {}).get("tp2") or 0)
        if min(entry,stop,tp1,tp2)<=0:
            raise HTTPException(422,"Entry/stop/targets required")
        now=now_iso(); con.execute("""UPDATE copilot_sessions SET status='LIVE',updated_at=?,side=?,actual_entry=?,stop=?,tp1=?,tp2=?,size=?,opened_at=?,last_price=? WHERE id=?""", (now,side,entry,stop,tp1,tp2,body.size,now,entry,cp["id"]))
        cp_live=dict(con.execute("SELECT * FROM copilot_sessions WHERE id=?",(cp["id"],)).fetchone())
        if lr: _ensure_validation_trade(con,uid,cp_live,lr,entry)
        _notify(con,uid,"SETUP","Live trade monitoring active",f"{side} entry {entry:,.1f} · stop {stop:,.1f}",f"live:{cp['id']}")
        con.commit(); out=dict(con.execute("SELECT * FROM copilot_sessions WHERE id=?",(cp["id"],)).fetchone())
    return {"ok":True,"session":out}

@app.get("/api/notifications")
def notifications(after: int = 0, session: str | None = Cookie(default=None)):
    u=require_entitled_user(session)
    with db() as con:
        rows=con.execute("SELECT * FROM notifications WHERE user_id=? AND id>? ORDER BY id ASC LIMIT 50",(u["id"],max(0,after))).fetchall()
    return {"ok":True,"items":[dict(r) for r in rows]}

@app.get("/api/push/public-key")
def push_public_key(session: str | None = Cookie(default=None)):
    require_user(session)
    _, pub, source = _vapid_keys()
    return {"ok": True, "public_key": pub, "key_id": _push_key_id(), "source": source}


@app.post("/api/push/subscribe")
def push_subscribe(body: PushSubscriptionBody, session: str | None = Cookie(default=None)):
    u = require_user(session)
    if not body.endpoint.startswith("https://"):
        raise HTTPException(422, "Invalid push endpoint")
    if not body.keys.get("p256dh") or not body.keys.get("auth"):
        raise HTTPException(422, "Push subscription keys missing")
    sub = body.model_dump()
    with db() as con:
        con.execute("""INSERT INTO push_subscriptions(user_id,endpoint,subscription_json,created_at,last_ok_at) VALUES(?,?,?,?,COALESCE((SELECT last_ok_at FROM push_subscriptions WHERE user_id=? AND endpoint=?),NULL)) ON CONFLICT(user_id,endpoint) DO UPDATE SET subscription_json=excluded.subscription_json,created_at=excluded.created_at,last_ok_at=COALESCE(push_subscriptions.last_ok_at,excluded.last_ok_at)""",
                    (u["id"], body.endpoint, json.dumps(sub), now_iso(), u["id"], body.endpoint))
        con.commit()
        count=con.execute("SELECT COUNT(*) n FROM push_subscriptions WHERE user_id=?",(u["id"],)).fetchone()["n"]
    return {"ok": True, "subscriptions": count, "vapid_key_id": _push_key_id()}


@app.post("/api/push/unsubscribe")
def push_unsubscribe(body: PushSubscriptionBody, session: str | None = Cookie(default=None)):
    u = require_user(session)
    with db() as con:
        con.execute("DELETE FROM push_subscriptions WHERE user_id=? AND endpoint=?", (u["id"], body.endpoint))
        con.commit()
    return {"ok": True}


@app.post("/api/push/test")
def push_test(session: str | None = Cookie(default=None)):
    u = require_user(session)
    with db() as con:
        result=_send_web_push(con,u["id"],"US30 Copilot push test","Background push notifications are working on this device.","TEST")
        con.commit()
    if not result.get("ok"):
        return Response(content=json.dumps({"ok":False,**result}),media_type="application/json",status_code=502)
    return {"ok":True,**result}


@app.get("/api/push/status")
def push_status(session: str | None = Cookie(default=None)):
    u=require_user(session)
    with db() as con:
        count=con.execute("SELECT COUNT(*) n FROM push_subscriptions WHERE user_id=?",(u["id"],)).fetchone()["n"]
        last=con.execute("SELECT * FROM push_delivery_log WHERE user_id=? ORDER BY id DESC LIMIT 1",(u["id"],)).fetchone()
    _,_,source=_vapid_keys()
    return {"ok":True,"subscriptions":count,"vapid_key_id":_push_key_id(),"vapid_source":source,"vapid_subject":VAPID_SUBJECT,"last_delivery":dict(last) if last else None}


@app.get("/api/orderflow")
def orderflow_status(session: str | None = Cookie(default=None)):
    u = require_entitled_user(session)
    return {"ok":True,"orderflow":_fresh_orderflow(u["id"])}



@app.get("/api/candles")
def candles(tf: str = "1m", limit: int = 140, session: str | None = Cookie(default=None)):
    u = require_entitled_user(session); uid = int(u["id"])
    tf_map = {"1m":1, "5m":5, "15m":15, "30m":30, "1h":60, "4h":240}
    if tf not in tf_map:
        raise HTTPException(422, "Unsupported chart timeframe")
    limit = max(30, min(int(limit), 180))
    minutes = tf_map[tf]
    with db() as con:
        count = int(con.execute("SELECT COUNT(*) n FROM candles_1m WHERE user_id=?", (uid,)).fetchone()["n"])
        if count < 30:
            rows = con.execute(
                "SELECT symbol,raw_json FROM snapshots WHERE user_id=? ORDER BY id DESC LIMIT 30000",
                (uid,),
            ).fetchall()
            batch = []
            for r in reversed(rows):
                try:
                    p = json.loads(r["raw_json"])
                    b = _one_minute_bar(p)
                    ts_ms = int(p.get("ts") or 0)
                    if not b or ts_ms <= 0:
                        continue
                    f1 = next((f for f in (p.get("frames") or []) if isinstance(f, dict) and f.get("tf") == "1m"), {})
                    v = f1.get("vol")
                    batch.append((uid,ts_ms,str(p.get("symbol") or r["symbol"] or "US30"),b["o"],b["h"],b["l"],b["c"],float(v) if v is not None else None))
                except Exception:
                    continue
            if batch:
                con.executemany(
                    """INSERT INTO candles_1m(user_id,ts,symbol,o,h,l,c,vol) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(user_id,ts) DO UPDATE SET symbol=excluded.symbol,o=excluded.o,h=excluded.h,l=excluded.l,c=excluded.c,vol=excluded.vol""",
                    batch,
                )
                con.commit()
        need = min(32000, max(600, limit * minutes + minutes * 2))
        rows = con.execute(
            "SELECT ts,symbol,o,h,l,c,vol FROM candles_1m WHERE user_id=? ORDER BY ts DESC LIMIT ?",
            (uid, need),
        ).fetchall()
    rows = list(reversed(rows))
    bucket_ms = minutes * 60_000
    buckets = {}
    symbol = "US30"
    for r in rows:
        symbol = r["symbol"] or symbol
        ts_ms = int(r["ts"])
        key = (ts_ms // bucket_ms) * bucket_ms
        b = buckets.get(key)
        if b is None:
            buckets[key] = {
                "t": key, "o": float(r["o"]), "h": float(r["h"]), "l": float(r["l"]), "c": float(r["c"]),
                "v": float(r["vol"] or 0.0),
            }
        else:
            b["h"] = max(b["h"], float(r["h"]))
            b["l"] = min(b["l"], float(r["l"]))
            b["c"] = float(r["c"])
            b["v"] += float(r["vol"] or 0.0)
    out = [buckets[k] for k in sorted(buckets)][-limit:]
    return {"ok":True,"tf":tf,"minutes":minutes,"symbol":symbol,"candles":out}

@app.get("/api/performance")
def performance(session: str | None = Cookie(default=None)):
    u=require_entitled_user(session); strategy=_settings(u["id"])["strategy"]
    with db() as con:
        rows=[dict(r) for r in con.execute("SELECT * FROM trades WHERE user_id=? AND strategy=? ORDER BY id DESC",(u["id"],strategy)).fetchall()]
    closed=[r for r in rows if r.get("status")=="CLOSED" and r.get("realised_r") is not None]
    tp1_rate=round(100*sum(1 for r in closed if r.get("tp1_hit"))/len(closed),1) if closed else None
    rs=[float(r["realised_r"]) for r in closed]
    expectancy=round(sum(rs)/len(rs),2) if rs else None
    gross_win=sum(r for r in rs if r>0); gross_loss=abs(sum(r for r in rs if r<0))
    profit_factor=round(gross_win/gross_loss,2) if gross_loss>0 else (None if not gross_win else 999.0)
    with db() as con:
        manual_rows=[dict(r) for r in con.execute(
            "SELECT * FROM copilot_sessions WHERE user_id=? AND strategy=? AND opened_at IS NOT NULL ORDER BY id DESC",
            (u["id"], strategy)
        ).fetchall()]
    manual_closed=[r for r in manual_rows if r.get("status")=="CLOSED" and r.get("close_reason") not in (None,"RESET")]
    return {
        "ok":True,"strategy":strategy,
        "tracked":len(rows),"closed":len(closed),"tp1_hit_rate":tp1_rate,
        "profit_factor":profit_factor,"expectancy_r":expectancy,
        "manual_live_trades":len(manual_rows),"manual_closed":len(manual_closed)
    }

@app.get("/promo.png")
def promo():
    return FileResponse(BASE/"US30_COPILOT_PROMO.png", media_type="image/png")

@app.get("/downloads/tradingview-pine")
def download_tradingview_pine(feed: str = "standard"):
    if feed.lower() == "cme":
        path = BASE / "TRADINGVIEW_PINE_V6_CME_YM.txt"
        name = "US30_COPILOT_TRADINGVIEW_CME_YM.txt"
    else:
        path = BASE / "TRADINGVIEW_PINE_V6_FREE.txt"
        name = "US30_COPILOT_6_4_LIVE_FEED.txt"
    if not path.exists():
        raise HTTPException(404, "Pine script not found")
    return FileResponse(path, media_type="text/plain", filename=name)


HTML = r'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><meta name="theme-color" content="#030914"><link rel="manifest" href="/manifest.webmanifest"><title>US30 Copilot — Data Engine</title>
<style>
:root{color-scheme:dark;font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,Arial;background:#02060d;color:#f3f8ff;--bg:#02060d;--panel:#07111e;--panel2:#0b1727;--line:rgba(80,151,218,.22);--blue:#22b8ff;--blue2:#087fe0;--green:#31e6a0;--red:#ff667a;--amber:#ffc85c;--muted:#8198b2;--text:#f4f9ff}*{box-sizing:border-box}body{margin:0;min-height:100vh;background:radial-gradient(circle at 52% -12%,#153f70 0,#071525 29%,#02060d 67%);overflow-x:hidden}body:before{content:"";position:fixed;inset:0;pointer-events:none;background-image:linear-gradient(rgba(53,128,190,.025) 1px,transparent 1px),linear-gradient(90deg,rgba(53,128,190,.025) 1px,transparent 1px);background-size:36px 36px;mask-image:linear-gradient(to bottom,black,transparent 75%)}button,input,select{font:inherit}.hidden{display:none!important}.shell{max-width:1240px;margin:auto;padding:18px 18px 102px}.glass{background:linear-gradient(180deg,rgba(12,29,48,.94),rgba(4,12,22,.96));border:1px solid var(--line);box-shadow:0 24px 70px rgba(0,0,0,.34),inset 0 1px rgba(255,255,255,.03);border-radius:24px}.top{display:flex;align-items:center;justify-content:space-between;gap:14px}.logoRow{display:flex;align-items:center;gap:11px}.miniLogo{width:38px;height:38px;border-radius:12px;border:1px solid rgba(57,181,255,.35);background:radial-gradient(circle at 50% 50%,#126fb9 0,#06101e 47%,#02060d 70%);position:relative;box-shadow:0 0 24px rgba(24,168,255,.18)}.miniLogo:after{content:"C";position:absolute;inset:0;display:grid;place-items:center;font-size:24px;font-weight:1000;color:#dff4ff}.brand{font-weight:1000;letter-spacing:.7px;font-size:19px}.brand b{color:var(--blue)}.brandSub{font-size:9px;color:#6685a5;letter-spacing:1.8px;font-weight:850;margin-top:1px}.statusWrap{display:flex;gap:8px;align-items:center}.pill{font-size:10px;font-weight:950;padding:7px 10px;border-radius:999px;background:#152439;color:#93a9c0;border:1px solid rgba(92,149,202,.16)}.pill.live{background:#073f2c;color:#6ef1bc;border-color:#16825d;box-shadow:0 0 16px rgba(49,230,160,.12)}.nav{display:flex;gap:6px;margin:14px 0 18px;overflow:auto}.nav button{border:1px solid transparent;background:#06121f;color:#91a7bd;padding:10px 13px;border-radius:12px;font-weight:850;cursor:pointer;white-space:nowrap}.nav button.on{background:linear-gradient(135deg,#0b6fc4,#14b1ff);color:white;border-color:#22b8ff;box-shadow:0 0 20px rgba(21,164,245,.16)}.eyebrow{font-size:10px;color:#63c6ff;font-weight:950;text-transform:uppercase;letter-spacing:1.35px}.grid{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(300px,.72fr);gap:16px}.hero{padding:21px;position:relative;overflow:hidden}.hero:before{content:"";position:absolute;width:390px;height:390px;border:1px solid rgba(42,182,255,.12);border-radius:50%;right:-210px;top:-230px;box-shadow:0 0 90px rgba(29,156,238,.08)}.heroTop{display:flex;justify-content:space-between;gap:18px;position:relative}.sig{font-size:78px;font-weight:1000;line-height:.92;margin:15px 0 6px;letter-spacing:-3px}.LONG{color:var(--green);text-shadow:0 0 30px rgba(49,230,160,.13)}.SHORT{color:#ff7586;text-shadow:0 0 30px rgba(255,102,122,.12)}.WAIT{color:var(--amber)}.confidence{font-size:20px;font-weight:950}.sub{color:var(--muted);margin-top:5px;font-size:13px}.scoreWrap{display:flex;align-items:center;flex-direction:column;gap:7px}.scoreRing{--score:0;width:136px;height:136px;border-radius:50%;display:grid;place-items:center;background:conic-gradient(var(--green) calc(var(--score)*1%),#14243a 0);position:relative;box-shadow:0 0 36px rgba(49,230,160,.08)}.scoreRing:after{content:"";position:absolute;inset:10px;background:radial-gradient(circle at 50% 40%,#0c2035,#06101b 65%);border-radius:50%;border:1px solid rgba(116,178,228,.12)}.scoreRing span{position:relative;z-index:2;font-size:32px;font-weight:1000}.scoreLabel{font-size:9px;text-transform:uppercase;color:#6d87a1;letter-spacing:1px;font-weight:900}.scanline{height:3px;border-radius:99px;background:linear-gradient(90deg,transparent,#28bfff,transparent);opacity:.55;animation:scan 3.2s ease-in-out infinite;margin:17px 0 4px}@keyframes scan{0%,100%{transform:translateX(-50%);opacity:.15}50%{transform:translateX(50%);opacity:.7}}.levels{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-top:16px}.metric{padding:13px;border:1px solid rgba(83,146,202,.15);background:rgba(6,17,30,.58);border-radius:16px}.lab{font-size:9px;text-transform:uppercase;letter-spacing:.9px;color:#7890a9;font-weight:900}.val{font-weight:950;font-size:18px;margin-top:5px}.contextGrid{display:grid;grid-template-columns:repeat(5,1fr);gap:9px;margin-top:14px}.context{padding:14px}.context .val{font-size:15px}.good{color:var(--green)}.bad{color:var(--red)}.warn{color:var(--amber)}.neutral{color:#b7c7d8}.engineWhy{margin-top:12px;padding:14px;border-radius:16px;background:linear-gradient(90deg,rgba(10,40,67,.7),rgba(5,17,30,.45));border:1px solid rgba(44,144,209,.16)}.whyTitle{font-size:11px;font-weight:950;color:#bde8ff;margin-bottom:7px}.whyText{font-size:12px;line-height:1.5;color:#91a8c0}.statebar{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;padding:8px;margin-top:14px}.statebar button{border:1px solid #1d3a58;background:#06121f;color:#9fb5ca;border-radius:12px;padding:11px;font-weight:950;cursor:pointer}.statebar button.active{background:linear-gradient(135deg,#087dd8,#14afff);color:white;border-color:#23baff;box-shadow:0 0 18px rgba(20,175,255,.14)}.side{padding:18px;display:flex;flex-direction:column;gap:14px}.radar{height:175px;display:grid;place-items:center;position:relative;overflow:hidden;border-radius:20px;background:radial-gradient(circle,#0a2844 0,#071525 43%,#04101c 70%);border:1px solid rgba(64,154,218,.17)}.radar:before,.radar:after{content:"";position:absolute;border:1px solid rgba(62,186,255,.17);border-radius:50%}.radar:before{width:130px;height:130px}.radar:after{width:78px;height:78px}.radarSweep{position:absolute;width:90px;height:90px;border-left:2px solid #28c3ff;transform-origin:0 100%;left:50%;top:0;animation:sweep 2.8s linear infinite;filter:drop-shadow(0 0 8px #20baff)}@keyframes sweep{to{transform:rotate(360deg)}}.radarCore{z-index:2;text-align:center}.readiness{font-size:30px;font-weight:1000}.notice{font-size:12px;color:var(--muted);line-height:1.5}.field{width:100%;padding:12px 13px;border:1px solid #21425f;border-radius:12px;background:#05101c;color:#fff;margin-top:7px;outline:none}.field:focus{border-color:#1baeff;box-shadow:0 0 0 3px rgba(24,168,255,.08)}.section{margin-top:16px}.sectionHead{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin-bottom:12px}.section h1{font-size:27px;margin:5px 0 2px}.section h2{font-size:15px}.connections{display:grid;grid-template-columns:repeat(3,1fr);gap:11px}.conn{padding:17px}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;background:#43576c}.dot.on{background:var(--green);box-shadow:0 0 12px #35e49a}.btn{border:0;border-radius:13px;padding:12px 15px;font-weight:950;background:linear-gradient(135deg,#087bdc,#20b1ff);color:white;cursor:pointer}.btn.ghost{background:#132238}.btn.red{background:#742b3b}.btn.slim{padding:9px 12px;font-size:11px}.copy{font-family:ui-monospace,monospace;font-size:10px;word-break:break-all;background:#030d18;padding:12px;border-radius:12px;margin-top:8px;color:#b8dbf5;border:1px solid rgba(54,130,188,.15)}.pageGrid{display:grid;grid-template-columns:1.15fr .85fr;gap:14px}.premiumCard{padding:18px}.bigStat{font-size:34px;font-weight:1000}.eventList{display:grid;gap:8px;margin-top:12px}.event{display:flex;justify-content:space-between;gap:12px;padding:12px;border-radius:14px;background:#061321;border:1px solid rgba(65,127,184,.14)}.event strong{font-size:12px}.event span{font-size:11px;color:#7f96ae}.tradePanel{padding:20px}.tradeHero{display:flex;justify-content:space-between;gap:18px;align-items:flex-start}.tradeSide{font-size:48px;font-weight:1000}.rBig{font-size:38px;font-weight:1000}.riskBar{height:9px;background:#112238;border-radius:99px;overflow:hidden;margin-top:9px}.riskFill{height:100%;width:50%;background:linear-gradient(90deg,#25e6a0,#21baff);border-radius:99px}.perfGrid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.perfStat{padding:18px}.perfStat .bigStat{font-size:27px}.settingsGrid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.toggleLine{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:13px 0;border-bottom:1px solid rgba(70,127,182,.12)}.toggleLine:last-child{border:0}.toggleLine label{font-size:12px;font-weight:850}.wizard{margin-top:16px;padding:18px}.wizardHead{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}.wizardSteps{display:grid;gap:10px;margin-top:14px}.wizardStep{display:grid;grid-template-columns:34px 1fr;gap:12px;padding:13px;border:1px solid rgba(63,146,209,.17);border-radius:16px;background:rgba(4,15,27,.55)}.stepNo{width:34px;height:34px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,#087dd8,#14afff);font-weight:1000;color:white;box-shadow:0 0 16px rgba(20,175,255,.15)}.stepTitle{font-weight:950;margin:1px 0 4px}.stepText{font-size:12px;line-height:1.5;color:#91a8c0}.setupCode{margin-top:8px;padding:10px 12px;background:#020914;border:1px solid rgba(69,145,204,.2);border-radius:11px;font:600 11px ui-monospace,SFMono-Regular,Consolas,monospace;color:#c9e9ff;word-break:break-all}.wizardActions{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}.feedChoice{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}.feedCard{border:1px solid rgba(75,145,201,.2);background:#06121f;border-radius:12px;padding:10px;cursor:pointer}.feedCard.on{border-color:#22b8ff;box-shadow:0 0 18px rgba(34,184,255,.12)}.verifyBox{margin-top:10px;padding:11px;border-radius:12px;background:#06131f;border:1px solid rgba(65,141,198,.18)}.login{max-width:1030px;margin:5vh auto;padding:0;overflow:hidden}.loginGrid{display:grid;grid-template-columns:1.15fr .85fr;min-height:620px}.loginArt{background:url('/promo.png') center/cover no-repeat;position:relative}.loginArt:after{content:"";position:absolute;inset:0;background:linear-gradient(90deg,transparent 55%,rgba(3,9,17,.62))}.loginForm{padding:42px;display:flex;flex-direction:column;justify-content:center}.loginForm h1{font-size:38px;line-height:1.02;margin:7px 0 4px}.loginForm p{color:var(--muted)}.tabs{display:flex;gap:6px;margin:18px 0}.tabs button{flex:1}.error{color:#ff8290;font-size:12px;min-height:18px}.small{font-size:10px;color:#667f98;line-height:1.4}.footerNav{display:none}.toast{position:fixed;right:18px;bottom:18px;z-index:99;background:#0b1d30;border:1px solid #1d6fa8;color:#dff5ff;padding:12px 15px;border-radius:14px;box-shadow:0 18px 45px #0008;font-size:12px;opacity:0;transform:translateY(20px);transition:.25s}.toast.show{opacity:1;transform:none}

.chartCard{margin-top:16px;padding:18px;overflow:hidden}.chartTop{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px}.chartTitle{font-size:18px;font-weight:1000;margin-top:3px}.chartTf{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}.chartTf button{border:1px solid #1d3a58;background:#06121f;color:#8fa9c3;border-radius:9px;padding:7px 9px;font-size:10px;font-weight:950;cursor:pointer}.chartTf button.on{background:#0b75c9;color:white;border-color:#20b8ff}.chartWrap{height:460px;position:relative;border:1px solid rgba(65,137,196,.16);border-radius:16px;overflow:hidden;background:linear-gradient(180deg,rgba(3,12,22,.88),rgba(2,8,15,.98))}.chartWrap canvas{display:block;width:100%;height:100%}.chartLegend{display:flex;gap:13px;flex-wrap:wrap;margin-top:10px;font-size:10px;color:#7f98b1}.chartLegend b{color:#cbe8fa}.chartEmpty{position:absolute;inset:0;display:grid;place-items:center;color:#6f89a4;font-size:12px;pointer-events:none}.chartMeta{font-size:10px;color:#6f89a4;margin-top:4px}

@media(max-width:880px){.grid,.pageGrid,.loginGrid,.settingsGrid{grid-template-columns:1fr}.loginArt{min-height:290px}.levels{grid-template-columns:repeat(2,1fr)}.contextGrid,.connections,.perfGrid{grid-template-columns:1fr 1fr}.sig{font-size:58px}.scoreRing{width:105px;height:105px}.nav{display:none}.footerNav{display:grid;position:fixed;left:8px;right:8px;bottom:8px;grid-template-columns:repeat(5,1fr);gap:4px;background:#06111ef2;border:1px solid #1a3652;border-radius:18px;padding:7px;z-index:90;backdrop-filter:blur(15px)}.footerNav button{border:0;background:transparent;color:#859db5;font-size:9px;padding:7px 2px;font-weight:850}.footerNav button.on{color:#43c1ff}.shell{padding-bottom:92px}.tradeHero{flex-direction:column}.chartWrap{height:390px}.chartTop{align-items:flex-start}}@media(max-width:520px){.shell{padding:12px 11px 92px}.hero,.side,.premiumCard,.tradePanel{border-radius:19px}.sig{font-size:50px}.heroTop{align-items:center}.scoreRing{width:88px;height:88px}.scoreRing span{font-size:24px}.contextGrid,.connections,.perfGrid{grid-template-columns:1fr}.login{margin:0;min-height:100vh;border:0;border-radius:0}.loginArt{min-height:230px}.loginForm{padding:27px}.loginForm h1{font-size:32px}.levels{grid-template-columns:1fr 1fr}.statebar button{font-size:10px;padding:10px 4px}.chartWrap{height:340px}.chartTop{display:block}.chartTf{justify-content:flex-start;margin-top:10px}.top{align-items:flex-start}.statusWrap{flex-direction:column;align-items:flex-end}}
.tourOverlay{position:fixed;inset:0;z-index:9998;background:rgba(1,5,12,.82);backdrop-filter:blur(8px);display:grid;place-items:center;padding:18px}.tourCard{width:min(720px,100%);max-height:90vh;overflow:auto;padding:24px;border:1px solid rgba(52,183,255,.35);box-shadow:0 30px 100px rgba(0,0,0,.62),0 0 60px rgba(24,168,255,.10)}.tourTop{display:flex;justify-content:space-between;gap:14px;align-items:flex-start}.tourProgress{height:7px;background:#102238;border-radius:99px;overflow:hidden;margin:18px 0}.tourProgress>div{height:100%;background:linear-gradient(90deg,#087bdc,#31e6a0);transition:width .25s ease}.tourIcon{width:52px;height:52px;border-radius:16px;display:grid;place-items:center;font-size:25px;font-weight:1000;background:linear-gradient(145deg,#0a78ca,#12b6ff);box-shadow:0 0 25px rgba(20,175,255,.2)}.tourTitle{font-size:26px;font-weight:1000;margin:8px 0 5px}.tourBody{font-size:14px;line-height:1.65;color:#a9bdd1}.tourCheck{margin-top:14px;padding:13px 14px;border:1px solid rgba(76,147,205,.2);background:#061321;border-radius:14px}.tourCheck.good{border-color:rgba(49,230,160,.4);background:rgba(7,63,44,.25)}.tourActions{display:flex;justify-content:space-between;gap:9px;flex-wrap:wrap;margin-top:20px}.tourActions .right{display:flex;gap:8px;margin-left:auto}.tourSkip{background:transparent;border:0;color:#7992aa;cursor:pointer;font-weight:800}.helpBtn{border:1px solid rgba(69,153,218,.25);background:#081725;color:#9fdcff;border-radius:10px;padding:7px 10px;font-size:10px;font-weight:950;cursor:pointer}.tourMini{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px}.tourMini>div{padding:11px;border-radius:12px;background:#061321;border:1px solid rgba(65,127,184,.14);font-size:11px;color:#9eb4c9}.tourMini b{display:block;color:#eef8ff;margin-bottom:4px}@media(max-width:650px){.tourCard{padding:18px}.tourTitle{font-size:22px}.tourMini{grid-template-columns:1fr}.tourActions{display:block}.tourActions .right{margin-top:10px}.tourActions button{width:100%;margin-top:7px}}</style></head><body>
<div id="login" class="login glass hidden"><div class="loginGrid"><div class="loginArt"></div><div class="loginForm"><div class="eyebrow">US30 COPILOT · DATA ENGINE</div><h1>Market intelligence.<br>Not another signal app.</h1><p>One decision engine combining your live technical feed, macro context and optional futures/order-flow confirmation.</p><div class="tabs"><button class="btn" onclick="mode='login';drawAuth()">Sign in</button><button class="btn ghost" onclick="mode='register';drawAuth()">Create account</button></div><input id="name" class="field" placeholder="Display name"><input id="email" class="field" placeholder="Email"><input id="password" class="field" type="password" placeholder="Password (10+ characters)"><button id="authBtn" class="btn" style="margin-top:10px" onclick="authSubmit()">Sign in</button><div id="authErr" class="error"></div><div class="small">Decision-support software · no automatic trade execution · confidence is an engine score, not a guaranteed probability.</div></div></div></div>
<div id="app" class="shell hidden"><div class="top"><div class="logoRow"><div class="miniLogo"></div><div><div class="brand">US30 <b>COPILOT</b></div><div class="brandSub">REAL-TIME DATA ENGINE</div></div></div><div class="statusWrap"><button class="helpBtn" onclick="openTour(0)">? TUTORIAL</button><div id="plan" class="pill">BETA</div><div id="feed" class="pill">NOT CONNECTED</div></div></div><div class="nav"><button class="on" data-page="dashboard">Dashboard</button><button data-page="opportunities">Opportunities</button><button data-page="live">Live Trade</button><button data-page="performance">Performance</button><button data-page="connections">Connections</button><button data-page="settings">Settings</button></div>
<div id="dashboardPage"><div class="grid"><div class="hero glass"><div class="heroTop"><div><div id="symbol" class="eyebrow">WAITING FOR YOUR DATA FEED</div><div id="signal" class="sig WAIT">WAIT</div><div id="confidence" class="confidence">Data engine standby</div><div id="summary" class="sub">Connect TradingView to begin continuous analysis.</div></div><div class="scoreWrap"><div id="ring" class="scoreRing" style="--score:0"><span id="ringScore">0</span></div><div class="scoreLabel">ENGINE SCORE</div></div></div><div class="scanline"></div><div class="levels"><div class="metric"><div class="lab">Price</div><div id="price" class="val">—</div></div><div class="metric"><div class="lab">Entry zone</div><div id="entry" class="val">—</div></div><div class="metric"><div class="lab">Invalidation</div><div id="stop" class="val">—</div></div><div class="metric"><div class="lab">Target 1</div><div id="tp1" class="val">—</div></div><div class="metric"><div class="lab">Target 2</div><div id="tp2" class="val">—</div></div></div><div class="contextGrid"><div class="context glass"><div class="lab">Market structure</div><div id="structure" class="val neutral">STANDBY</div></div><div class="context glass"><div class="lab">Macro / event risk</div><div id="macro" class="val neutral">STANDBY</div><div id="macrod" class="notice"></div></div><div class="context glass"><div class="lab">YM confirmation</div><div id="ym" class="val neutral">STANDBY</div></div><div class="context glass"><div class="lab">YM L1 order flow</div><div id="l2" class="val neutral">STANDBY</div><div id="l2d" class="notice"></div></div><div class="context glass"><div class="lab">Timeframe agreement</div><div id="alignment" class="val neutral">—</div><div id="alignDetail" class="notice"></div></div></div><div class="engineWhy"><div class="whyTitle">WHY THE ENGINE IS HERE</div><div id="why" class="whyText">Waiting for live market structure, momentum, volatility and context.</div></div><div class="statebar glass"><button id="sIDLE" onclick="setState('IDLE')">IDLE</button><button id="sLOOKING" onclick="setState('LOOKING')">LOOKING</button><button id="sARMED" onclick="setState('ARMED')">ARMED</button><button id="sLIVE" onclick="startLive()">LIVE TRADE</button></div></div><div class="side glass"><div class="radar"><div class="radarSweep"></div><div class="radarCore"><div class="eyebrow">COPILOT READINESS</div><div id="readiness" class="readiness warn">WAIT</div><div id="readyScore" class="notice">0 / 100</div></div></div><div id="readyDetail" class="notice">The engine will explain why a setup is or is not ready.</div><div><div class="lab">Active strategy</div><select id="strategy" class="field" onchange="saveStrategy()"><option value="scalp">Scalp · minutes–1h</option><option value="intraday">Intraday · 1–8h</option><option value="swing">Swing · days</option></select></div><div><div class="lab">Current trade</div><div id="tradeState" class="val">IDLE</div><div id="tradeDetail" class="notice">No live trade.</div></div><div><div class="lab">Live P/L</div><div id="liveR" class="val">—</div></div></div></div><div class="chartCard glass"><div class="chartTop"><div><div class="eyebrow">LIVE MARKET MAP</div><div class="chartTitle">US30 price action + Copilot levels</div><div id="chartMeta" class="chartMeta">Waiting for candle history…</div></div><div class="chartTf"><button class="on" data-tf="1m" onclick="setChartTf('1m')">1m</button><button data-tf="5m" onclick="setChartTf('5m')">5m</button><button data-tf="15m" onclick="setChartTf('15m')">15m</button><button data-tf="30m" onclick="setChartTf('30m')">30m</button><button data-tf="1h" onclick="setChartTf('1h')">1h</button><button data-tf="4h" onclick="setChartTf('4h')">4h</button></div></div><div class="chartWrap"><canvas id="candleCanvas"></canvas><div id="chartEmpty" class="chartEmpty">Waiting for TradingView candle history…</div></div><div class="chartLegend"><span><b>●</b> EMA 20</span><span><b>●</b> EMA 50</span><span>Copilot overlays appear only when a directional setup exists: entry zone · invalidation · TP1 · TP2.</span></div></div></div>
<div id="opportunitiesPage" class="hidden"><div class="section"><div class="sectionHead"><div><div class="eyebrow">OPPORTUNITY ENGINE</div><h1>Qualified setups, not noise.</h1><div class="notice">Copilot separates direction, readiness and execution timing.</div></div><button class="btn slim" onclick="setState('LOOKING')">Start looking</button></div><div class="pageGrid"><div class="premiumCard glass"><div class="lab">Current opportunity</div><div id="oppSignal" class="bigStat warn">WAIT</div><div id="oppText" class="notice">No live opportunity yet.</div><div class="eventList"><div class="event"><div><strong>STRUCTURE</strong><br><span>Multi-timeframe market direction</span></div><b id="oppStructure">—</b></div><div class="event"><div><strong>CONTEXT</strong><br><span>Macro / economic event risk</span></div><b id="oppMacro">—</b></div><div class="event"><div><strong>FUTURES CONFIRMATION</strong><br><span>YM cross-market confirmation</span></div><b id="oppYm">—</b></div><div class="event"><div><strong>ENTRY READINESS</strong><br><span>Is the setup ready now?</span></div><b id="oppReady">—</b></div></div></div><div class="premiumCard glass"><div class="eyebrow">COPILOT PRINCIPLE</div><h2>Direction is not entry timing.</h2><p class="notice">The engine may hold a directional bias while still telling you to WAIT. That separation is deliberate: it reduces chasing and turns the dashboard into decision support rather than a stream of raw signals.</p><div class="engineWhy"><div class="whyTitle">CURRENT ENGINE EXPLANATION</div><div id="oppWhy" class="whyText">Connect your data feed to populate this panel.</div></div></div></div></div></div>
<div id="livePage" class="hidden"><div class="section"><div class="sectionHead"><div><div class="eyebrow">LIVE TRADE COPILOT</div><h1>From entry to exit.</h1><div class="notice">Once you're in, Copilot stops hunting and starts protecting the position.</div></div><button class="btn slim" onclick="startLive()">I'm in</button></div><div class="tradePanel glass"><div class="tradeHero"><div><div class="lab">Position state</div><div id="liveSide" class="tradeSide warn">IDLE</div><div id="liveMeta" class="notice">No active trade.</div></div><div><div class="lab">R multiple</div><div id="livePageR" class="rBig">—</div></div></div><div class="levels" style="margin-top:22px"><div class="metric"><div class="lab">Entry</div><div id="ltEntry" class="val">—</div></div><div class="metric"><div class="lab">Current</div><div id="ltPrice" class="val">—</div></div><div class="metric"><div class="lab">Stop</div><div id="ltStop" class="val">—</div></div><div class="metric"><div class="lab">TP1</div><div id="ltTp1" class="val">—</div></div><div class="metric"><div class="lab">TP2</div><div id="ltTp2" class="val">—</div></div></div><div style="margin-top:18px"><div class="lab">Trade health</div><div class="riskBar"><div id="healthFill" class="riskFill"></div></div><div id="healthText" class="notice" style="margin-top:8px">No position being monitored.</div></div></div></div></div>
<div id="performancePage" class="hidden"><div class="section"><div class="eyebrow">ENGINE VALIDATION</div><h1>Measure the engine, don't believe the marketing.</h1><p class="notice">Engine validation is automatic and independent of whether you personally take a trade. Results are separated by strategy.</p><div class="perfGrid"><div class="perfStat glass"><div class="lab">Engine tracked</div><div id="pTracked" class="bigStat">0</div></div><div class="perfStat glass"><div class="lab">Engine closed</div><div id="pClosed" class="bigStat">0</div></div><div class="perfStat glass"><div class="lab">TP1 hit rate</div><div id="pTp1" class="bigStat">—</div></div><div class="perfStat glass"><div class="lab">Expectancy</div><div id="pExp" class="bigStat">—</div></div></div><div class="premiumCard glass" style="margin-top:12px"><div class="lab">Manual live trades</div><div style="display:flex;gap:22px;margin-top:8px"><div><b id="pManualLive" style="font-size:24px">0</b><div class="small">Accepted with LIVE TRADE</div></div><div><b id="pManualClosed" style="font-size:24px">0</b><div class="small">Manually tracked closed</div></div></div><p class="notice">Manual live-trade tracking is separate from engine validation and begins only when you press LIVE TRADE and confirm your actual entry.</p></div><div class="premiumCard glass" style="margin-top:12px"><div class="lab">Validation status</div><div id="validationStatus" class="val warn">BUILDING SAMPLE</div><p class="notice">Confidence scores become meaningful only after enough closed engine setups. Sample size is shown before any performance claims.</p></div></div></div>
<div id="connectionsPage" class="hidden"><div class="section"><div class="sectionHead"><div><div class="eyebrow">DATA CONNECTIONS</div><h1>Your data. One intelligence layer.</h1><p class="notice">Each account gets a private TradingView webhook. Optional provider keys are encrypted at rest using your server APP_SECRET.</p></div></div><div class="connections"><div class="conn glass"><div class="lab"><span id="tvDot" class="dot on"></span>TradingView</div><div class="val">PERSONAL WEBHOOK</div><div id="webhook" class="copy">Loading…</div><button class="btn" onclick="copyWebhook()" style="margin-top:8px">Copy webhook</button><button class="btn ghost" onclick="rotateWebhook()" style="margin-top:8px">Rotate URL</button><div class="notice" style="margin-top:9px">Primary live technical / multi-timeframe feed.</div></div><div class="conn glass"><div class="lab"><span id="fredDot" class="dot"></span>FRED Macro</div><div class="val">OPTIONAL · FREE KEY</div><input id="fredKey" class="field" type="password" placeholder="Paste FRED API key"><button class="btn" onclick="saveConnections()" style="margin-top:8px">Save securely</button><div class="notice">Rates and macro-regime context.</div></div><div class="conn glass"><div class="lab"><span id="dbDot" class="dot"></span>Databento</div><div class="val">OPTIONAL · LICENSED L1</div><input id="dbKey" class="field" type="password" placeholder="Paste Databento API key"><button class="btn" onclick="saveConnections()" style="margin-top:8px">Save securely</button><div class="notice">Prepared for licensed CME L1 order-flow confirmation.</div></div></div><div class="wizard glass"><div class="wizardHead"><div><div class="eyebrow">TRADINGVIEW SETUP WIZARD</div><h2 style="margin:5px 0 0">Connect in five steps</h2><p class="notice">The webhook is the destination. The Pine feed calculates and sends the actual market-data packet.</p></div><div id="wizardStatus" class="pill">NOT VERIFIED</div></div><div class="wizardSteps"><div class="wizardStep"><div class="stepNo">1</div><div><div class="stepTitle">Choose your TradingView feed</div><div class="stepText">Standard works without a CME subscription. CME Enhanced includes live CBOT E-mini Dow (YM) confirmation when the customer has TradingView CME data.</div><div class="feedChoice"><div id="feedStandard" class="feedCard on" onclick="chooseFeed('standard')"><b>Standard</b><div class="stepText">US30 multi-timeframe feed</div></div><div id="feedCme" class="feedCard" onclick="chooseFeed('cme')"><b>CME Enhanced</b><div class="stepText">US30 + YM futures confirmation</div></div></div><div class="wizardActions"><a id="pineDownload" class="btn" href="/downloads/tradingview-pine?feed=standard">Download Pine code</a></div></div></div><div class="wizardStep"><div class="stepNo">2</div><div><div class="stepTitle">Install the Pine feed in TradingView</div><div class="stepText"><b>The download does not auto-install into TradingView.</b> Open the exact US30/Dow chart, set it to <b>1 minute</b>, open Pine Editor → New blank indicator. Open the downloaded Pine file in a text editor, copy the <b>entire file from line 1</b> (it must start with <code>//@version=6</code>), paste it into Pine Editor, Save, then Add to chart.</div><div class="wizardActions"><button class="btn ghost" onclick="copyPineCode()">Copy selected Pine code</button></div><div id="pineCopyStatus" class="notice" style="margin-top:7px">Tip: use this button to avoid broken or partial copy/paste.</div></div></div><div class="wizardStep"><div class="stepNo">3</div><div><div class="stepTitle">Create one TradingView alert</div><div class="stepText">Condition: select the Copilot Pine script. Underneath select <b>Any alert() function call</b>. Frequency: <b>Once Per Bar Close</b>.</div><div id="conditionHint" class="setupCode">US30 COPILOT 6.4 LIVE FEED → Any alert() function call</div></div></div><div class="wizardStep"><div class="stepNo">4</div><div><div class="stepTitle">Paste your personal webhook</div><div class="stepText">Under Notifications enable <b>Webhook URL</b>, paste this private address, and leave TradingView's Message field alone because Pine builds the JSON payload automatically.</div><div id="wizardWebhook" class="setupCode">Loading…</div><div class="wizardActions"><button class="btn" onclick="copyWebhook()">Copy webhook</button></div></div></div><div class="wizardStep"><div class="stepNo">5</div><div><div class="stepTitle">Verify the connection</div><div class="stepText">Create the alert and wait for a completed 1-minute candle. Copilot should then receive a packet automatically.</div><div id="verifyBox" class="verifyBox notice">Waiting for a TradingView packet.</div><div class="wizardActions"><button class="btn ghost" onclick="verifyTradingView()">Check connection</button></div></div></div></div></div></div></div><div id="settingsPage" class="hidden"><div class="section"><div class="eyebrow">PERSONALISE COPILOT</div><h1>Only notify what matters.</h1><div class="settingsGrid"><div class="premiumCard glass"><div class="lab">Trading mode</div><select id="strategy2" class="field" onchange="syncStrategy()"><option value="scalp">Scalp · minutes–1h</option><option value="intraday">Intraday · 1–8h</option><option value="swing">Swing · days</option></select><div class="engineWhy" style="margin-top:12px"><div class="whyTitle">PHONE / BACKGROUND PUSH</div><div id="pushStatus" class="whyText">Not enabled on this device.</div><div class="wizardActions"><button class="btn" onclick="enablePush()">Enable background push</button><button class="btn ghost" onclick="testPush()">Send test</button></div></div><div class="toggleLine"><label>Setup developing</label><input id="nsetup" type="checkbox" onchange="saveAlerts()"></div><div class="toggleLine"><label>Entry ready</label><input id="nready" type="checkbox" onchange="saveAlerts()"></div><div class="toggleLine"><label>Live trade updates</label><input id="nupdate" type="checkbox" onchange="saveAlerts()"></div><div class="toggleLine"><label>Exit-risk warning</label><input id="nexit" type="checkbox" onchange="saveAlerts()"></div><div class="toggleLine"><label>Macro / event warning</label><input id="nnews" type="checkbox" onchange="saveAlerts()"></div><div class="toggleLine"><label>Auto-arm qualified setups</label><input id="autoarm" type="checkbox" onchange="saveAlerts()"></div></div><div class="premiumCard glass"><div class="eyebrow">ACCOUNT</div><div id="accountName" class="val">—</div><p class="notice">Your personal webhook and provider credentials belong to this account only.</p><div class="engineWhy" style="margin-top:12px"><div class="whyTitle">BILLING & ACCESS</div><div id="billingStatus" class="whyText">Checking subscription…</div><div class="wizardActions"><button id="subscribeBtn" class="btn" onclick="startCheckout()">Subscribe · £29.99/month</button><button id="portalBtn" class="btn ghost hidden" onclick="openBillingPortal()">Manage subscription</button></div><div class="small" style="margin-top:9px"><b>Beta data requirement:</b> Third-party data is not included. You need your own compatible TradingView access and your own Databento subscription/API key. FRED can use a free API key.</div></div><button class="btn ghost" style="margin-top:12px" onclick="logout()">Sign out</button><div class="engineWhy" style="margin-top:14px"><div class="whyTitle">PRODUCT POSITIONING</div><div class="whyText">US30 Copilot is a data-engine decision-support product. It explains the inputs and reasoning behind a setup rather than presenting itself as a guaranteed signal service.</div></div></div></div></div></div>
<div class="footerNav"><button class="on" data-page="dashboard">◉<br>Dashboard</button><button data-page="opportunities">⌁<br>Setups</button><button data-page="live">↗<br>Live</button><button data-page="connections">◌<br>Connect</button><button data-page="settings">⚙<br>Settings</button></div></div><div id="tourOverlay" class="tourOverlay hidden" role="dialog" aria-modal="true" aria-labelledby="tourTitle"><div class="tourCard glass"><div class="tourTop"><div><div class="eyebrow">COPILOT QUICK START</div><div id="tourTitle" class="tourTitle">Welcome to US30 Copilot</div></div><div id="tourIcon" class="tourIcon">C</div></div><div class="tourProgress"><div id="tourProgress"></div></div><div id="tourBody" class="tourBody"></div><div id="tourCheck" class="tourCheck"></div><div class="tourActions"><button class="tourSkip" onclick="finishTour()">Skip tutorial</button><div class="right"><button id="tourBack" class="btn ghost" onclick="tourMove(-1)">Back</button><button id="tourAction" class="btn ghost hidden" onclick="tourAction()">Do this now</button><button id="tourNext" class="btn" onclick="tourMove(1)">Next</button></div></div></div></div><div id="toast" class="toast"></div>
<script>
const $=id=>document.getElementById(id);let mode='login',user=null,settings={},latest=null,copilot=null,lastN=Number(localStorage.getItem('lastN')||0),conn=null,setupFeed=localStorage.getItem('setupFeed')||'standard';let tourStep=0;let loadTimer=null,notifTimer=null;const fmt=x=>x==null?'—':Number(x).toLocaleString(undefined,{maximumFractionDigits:1});
let chartTf=localStorage.getItem('chartTf')||'1m',chartCandles=[],chartBusy=false,lastChartLoad=0;

function setChartTf(tf){chartTf=tf;localStorage.setItem('chartTf',tf);document.querySelectorAll('.chartTf button').forEach(b=>b.classList.toggle('on',b.dataset.tf===tf));loadCandles(true)}
async function loadCandles(force=false){let now=Date.now();if(chartBusy||(!force&&now-lastChartLoad<12000))return;chartBusy=true;try{let r=await fetch('/api/candles?tf='+encodeURIComponent(chartTf)+'&limit=140',{cache:'no-store'}),j=await r.json();if(!r.ok||!j.ok)throw new Error(j.detail||'chart unavailable');chartCandles=j.candles||[];lastChartLoad=now;let m=$('chartMeta');if(m)m.textContent=(j.symbol||'US30')+' · '+chartTf+' · '+chartCandles.length+' candles · TradingView technical feed';drawCandles()}catch(e){let m=$('chartMeta');if(m)m.textContent='Chart unavailable · '+e.message}finally{chartBusy=false}}
function emaSeries(candles,period){let out=[],k=2/(period+1),v=null;for(let b of candles){v=v==null?b.c:(b.c*k+v*(1-k));out.push(v)}return out}
function drawCandles(){
let cv=$('candleCanvas'),empty=$('chartEmpty');if(!cv)return;let bars=chartCandles||[];if(empty)empty.classList.toggle('hidden',bars.length>1);if(bars.length<2)return;
let rect=cv.getBoundingClientRect(),dpr=Math.max(1,Math.min(2,window.devicePixelRatio||1)),w=Math.max(320,rect.width),h=Math.max(260,rect.height);cv.width=Math.round(w*dpr);cv.height=Math.round(h*dpr);let x=cv.getContext('2d');x.setTransform(dpr,0,0,dpr,0,0);x.clearRect(0,0,w,h);
let pad={l:12,r:116,t:22,b:28},pw=w-pad.l-pad.r,ph=h-pad.t-pad.b,d=latest||{},directional=d.signal==='LONG'||d.signal==='SHORT';
let plan=copilot||{};let armed=(plan.status==='ARMED'||plan.status==='LIVE')&&plan.planned_entry_low!=null&&plan.planned_entry_high!=null;
let overlay={entry_low:armed?plan.planned_entry_low:d.entry_low,entry_high:armed?plan.planned_entry_high:d.entry_high,stop:armed?plan.stop:d.stop,tp1:armed?plan.tp1:d.tp1,tp2:armed?plan.tp2:d.tp2,side:armed?(plan.side||d.signal):d.signal};
let levels=[];if(directional||armed){['entry_low','entry_high','stop','tp1','tp2'].forEach(k=>{let v=Number(overlay[k]);if(Number.isFinite(v)&&v>0)levels.push(v)})}
let lo=Math.min(...bars.map(b=>b.l),...(levels.length?levels:[Infinity])),hi=Math.max(...bars.map(b=>b.h),...(levels.length?levels:[-Infinity]));let span=Math.max(1,hi-lo);lo-=span*.12;hi+=span*.12;span=hi-lo;let py=v=>pad.t+(hi-v)/span*ph,px=i=>pad.l+(i+.5)/bars.length*pw;
x.strokeStyle='rgba(91,142,184,.10)';x.lineWidth=1;x.font='10px system-ui';x.fillStyle='#718ba5';x.textAlign='left';for(let i=0;i<=5;i++){let yy=pad.t+i*ph/5,v=hi-i*span/5;x.beginPath();x.moveTo(pad.l,yy);x.lineTo(w-pad.r,yy);x.stroke();x.fillText(v.toLocaleString(undefined,{maximumFractionDigits:1}),w-pad.r+8,yy+3)}
let bw=Math.max(2,Math.min(9,pw/bars.length*.62));bars.forEach((b,i)=>{let xx=px(i),yo=py(b.o),yc=py(b.c),yh=py(b.h),yl=py(b.l),up=b.c>=b.o;x.strokeStyle=up?'#35e6a1':'#ff697b';x.fillStyle=up?'#20c98a':'#e65367';x.beginPath();x.moveTo(xx,yh);x.lineTo(xx,yl);x.stroke();x.fillRect(xx-bw/2,Math.min(yo,yc),bw,Math.max(1,Math.abs(yc-yo)))});
function lineSeries(vals,color,width){x.strokeStyle=color;x.lineWidth=width;x.beginPath();vals.forEach((v,i)=>{let xx=px(i),yy=py(v);if(i===0)x.moveTo(xx,yy);else x.lineTo(xx,yy)});x.stroke()}lineSeries(emaSeries(bars,20),'rgba(36,190,255,.70)',1.25);lineSeries(emaSeries(bars,50),'rgba(255,190,79,.65)',1.2);
let labels=[];function level(v,label,color,dash=[],weight=1.5){v=Number(v);if(!Number.isFinite(v)||v<=0)return;let yy=py(v);if(yy<pad.t-2||yy>h-pad.b+2)return;x.save();x.setLineDash(dash);x.strokeStyle=color;x.lineWidth=weight;x.beginPath();x.moveTo(pad.l,yy);x.lineTo(w-pad.r,yy);x.stroke();x.restore();labels.push({v,label,color,y:yy})}
if((directional||armed)&&Number.isFinite(Number(overlay.entry_low))&&Number.isFinite(Number(overlay.entry_high))){let el=Number(overlay.entry_low),eh=Number(overlay.entry_high),top=py(Math.max(el,eh)),bot=py(Math.min(el,eh));x.fillStyle=overlay.side==='LONG'?'rgba(49,230,160,.12)':'rgba(255,102,122,.12)';x.fillRect(pad.l,top,pw,Math.max(4,bot-top));level(el,'ENTRY LOW','rgba(77,199,255,.95)',[4,4]);level(eh,'ENTRY HIGH','rgba(77,199,255,.95)',[4,4]);level(overlay.stop,'STOP / INVALID','rgba(255,102,122,.98)',[8,4],1.8);level(overlay.tp1,'TP1','rgba(49,230,160,.98)',[5,4],1.7);level(overlay.tp2,'TP2','rgba(49,230,160,.98)',[5,4],1.7);let i=bars.length-1,xx=px(i),yy=py(bars[i].c);x.fillStyle=overlay.side==='LONG'?'#31e6a0':'#ff667a';x.font='1000 11px system-ui';x.textAlign='right';x.fillText(overlay.side,xx-8,Math.max(pad.t+12,yy-12))}
let cp=Number(bars[bars.length-1].c);level(cp,'PRICE','rgba(224,240,255,.90)',[2,4],1.25);
// Separate right-edge badges vertically so ENTRY / PRICE / TP labels never sit on top of each other.
labels.sort((a,b)=>a.y-b.y);let minGap=23,topLim=pad.t+10,botLim=h-pad.b-10;for(let i=0;i<labels.length;i++){labels[i].ly=Math.max(labels[i].y,i?labels[i-1].ly+minGap:topLim)}for(let i=labels.length-1;i>=0;i--){let maxY=i===labels.length-1?botLim:labels[i+1].ly-minGap;labels[i].ly=Math.min(labels[i].ly,maxY)}
labels.forEach(q=>{let text=q.label+'  '+q.v.toLocaleString(undefined,{maximumFractionDigits:1});x.save();x.strokeStyle='rgba(102,156,202,.38)';x.lineWidth=1;x.beginPath();x.moveTo(w-pad.r,q.y);x.lineTo(w-pad.r+8,q.ly);x.lineTo(w-pad.r+12,q.ly);x.stroke();x.font='900 9px system-ui';let tw=Math.min(pad.r-17,x.measureText(text).width+12);x.fillStyle='rgba(3,12,22,.97)';x.fillRect(w-pad.r+12,q.ly-9,tw,18);x.strokeStyle=q.color;x.strokeRect(w-pad.r+12,q.ly-9,tw,18);x.fillStyle=q.color;x.textAlign='left';x.fillText(text,w-pad.r+17,q.ly+3);x.restore()});
x.fillStyle='#637f99';x.font='9px system-ui';x.textAlign='center';let step=Math.max(1,Math.floor(bars.length/5));for(let i=0;i<bars.length;i+=step){let dt=new Date(bars[i].t),lab=chartTf==='1m'||chartTf==='5m'||chartTf==='15m'||chartTf==='30m'?dt.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}):dt.toLocaleDateString([],{day:'2-digit',month:'short'});x.fillText(lab,px(i),h-8)}
}
window.addEventListener('resize',()=>drawCandles());

function toast(t){let e=$('toast');e.textContent=t;e.classList.add('show');setTimeout(()=>e.classList.remove('show'),1800)}
function drawAuth(){$('name').style.display=mode==='register'?'block':'none';$('authBtn').textContent=mode==='register'?'Create account':'Sign in';$('authErr').textContent=''}
async function authSubmit(){let url=mode==='register'?'/auth/register':'/auth/login',body={email:$('email').value,password:$('password').value,display_name:$('name').value};let r=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});let j=await r.json();if(!r.ok){$('authErr').textContent=j.detail||'Unable to continue';return}await boot()}
async function logout(){await fetch('/auth/logout',{method:'POST'});location.reload()}
const pages=['dashboard','opportunities','live','performance','connections','settings'];const TOUR_STEPS=[
{icon:'C',title:'Welcome to US30 Copilot',body:'Copilot watches the live US30 technical feed, macro context and YM L1 order flow, then scores a potential setup. It is decision support: you remain in control of every real trade.',check:'This quick tour takes about two minutes.',action:null},
{icon:'◎',title:'Engine validation runs automatically',body:'When an ARMED setup becomes entry-ready, the engine records that setup for objective performance validation automatically. This happens whether you personally take the trade or not.',check:'Performance → Engine Tracked measures the engine. It is separate from your own trading results.',action:'performance'},
{icon:'↗',title:'Your real trades are always manual',body:'Copilot can auto-arm a qualified setup, but it does NOT put you into a live trade. Only press LIVE TRADE / “I’m in” after you have actually entered. You then confirm your real entry price and Copilot tracks that position separately.',check:'Manual Live Trades only increases after you press LIVE TRADE. Engine validation and manual tracking never get mixed together.',action:'dashboard'},
{icon:'⌁',title:'Read the setup in this order',body:'Start with direction and Data Engine Score, then Copilot Readiness. Check the planned Entry Zone, Invalidation, TP1 and TP2. YM confirmation, YM L1 pressure, macro risk and timeframe agreement explain the context behind the score.',check:'READY means the engine conditions are aligned now — not that a profit is guaranteed.',action:'dashboard'},
{icon:'◌',title:'Connect the live data feed',body:'TradingView is the primary technical feed. Your account has its own private webhook. The Connections page already contains a five-step setup wizard with the current V6 Pine feed, personal webhook and connection verifier. Databento YM L1 and FRED are optional provider connections.',check:'Open Connections to install the Pine feed or verify that packets are arriving.',action:'connections'},
{icon:'🔔',title:'Enable phone and background alerts',body:'Push alerts can tell you when a setup develops, entry conditions are confirmed, targets are reached, structure deteriorates or event risk changes. Notification preferences remain under Settings.',check:'Enable push separately on each device where you want alerts.',action:'settings'},
{icon:'✓',title:'You’re ready',body:'Leave Copilot in LOOKING when you want it scanning. Qualified setups can move to ARMED automatically if Auto-arm is enabled. Wait for the entry-ready alert, make your own trade decision, and press LIVE TRADE only if you actually enter.',check:'The engine learns its statistical performance automatically; your manual live-trade history remains a separate record.',action:null}
];
function tourKey(){return 'us30TourV1:'+(user?.id||user?.email||'guest')}
function tourCheckText(step){let t=TOUR_STEPS[step];if(step===4){let live=!!(latest&&conn);return {text:live?'TradingView data is already reaching this account.':t.check,good:live}}if(step===5){let ok=localStorage.getItem('pushEnabled')==='1';return {text:ok?'Background push is registered on this device.':t.check,good:ok}}return {text:t.check,good:step===6}}
function renderTour(){let t=TOUR_STEPS[tourStep],o=$('tourOverlay');if(!o)return;$('tourIcon').textContent=t.icon;$('tourTitle').textContent=t.title;$('tourBody').textContent=t.body;$('tourProgress').style.width=((tourStep+1)/TOUR_STEPS.length*100)+'%';let c=tourCheckText(tourStep),box=$('tourCheck');box.textContent=c.text;box.className='tourCheck'+(c.good?' good':'');$('tourBack').classList.toggle('hidden',tourStep===0);$('tourNext').textContent=tourStep===TOUR_STEPS.length-1?'Finish':'Next';let a=$('tourAction');a.classList.toggle('hidden',!t.action);a.textContent=t.action==='connections'?'Open Connections':t.action==='settings'?'Open Settings':t.action==='performance'?'Open Performance':'Show Dashboard'}
function openTour(step=0){tourStep=Math.max(0,Math.min(TOUR_STEPS.length-1,step));$('tourOverlay').classList.remove('hidden');renderTour()}
function finishTour(){localStorage.setItem(tourKey(),'done');$('tourOverlay').classList.add('hidden');showPage('dashboard');toast('Tutorial complete · Copilot ready')}
function tourMove(n){if(n>0&&tourStep===TOUR_STEPS.length-1){finishTour();return}tourStep=Math.max(0,Math.min(TOUR_STEPS.length-1,tourStep+n));renderTour()}
function tourAction(){let a=TOUR_STEPS[tourStep].action;if(!a)return;$('tourOverlay').classList.add('hidden');showPage(a);toast('Tutorial paused · use ? TUTORIAL to continue')}
function maybeStartTour(){if(!localStorage.getItem(tourKey()))setTimeout(()=>openTour(0),450)}
function showPage(p){pages.forEach(x=>$(x+'Page').classList.toggle('hidden',x!==p));document.querySelectorAll('[data-page]').forEach(b=>b.classList.toggle('on',b.dataset.page===p));if(p==='connections')loadConnections();if(p==='performance')loadPerformance();if(p==='dashboard'){loadCandles(true);setTimeout(drawCandles,30)}renderAux()}
document.addEventListener('click',e=>{let b=e.target.closest('[data-page]');if(b)showPage(b.dataset.page)});
function renderBilling(b){if(!b)return;let st=$('billingStatus'),sub=$('subscribeBtn'),portal=$('portalBtn');let label=(b.plan||'BETA')+' · '+String(b.status||'inactive').toUpperCase();if(st)st.textContent=label+(b.current_period_end?' · current period to '+new Date(b.current_period_end).toLocaleDateString():'');if(sub)sub.classList.toggle('hidden',!!b.entitled);if(portal)portal.classList.toggle('hidden',!b.entitled||b.status==='grandfathered');$('plan').textContent=(b.plan||user?.plan||'BETA')+(b.entitled?' · ACTIVE':'')}
async function refreshBilling(){try{let r=await fetch('/api/billing/status',{cache:'no-store'}),j=await r.json();if(r.ok&&j.ok)renderBilling(j.billing)}catch(e){}}
async function startCheckout(){try{let r=await fetch('/api/billing/checkout',{method:'POST'}),j=await r.json();if(!r.ok)throw new Error(j.detail||'checkout unavailable');location.href=j.url}catch(e){alert(e.message)}}
async function openBillingPortal(){try{let r=await fetch('/api/billing/portal',{method:'POST'}),j=await r.json();if(!r.ok)throw new Error(j.detail||'billing portal unavailable');location.href=j.url}catch(e){alert(e.message)}}
async function boot(){let r=await fetch('/auth/me');if(!r.ok){$('login').classList.remove('hidden');$('app').classList.add('hidden');drawAuth();return}let j=await r.json();user=j.user;$('login').classList.add('hidden');$('app').classList.remove('hidden');$('plan').textContent=user.plan||'BETA';$('accountName').textContent=(user.display_name||user.email)+' · '+(user.plan||'BETA');renderBilling(j.billing);if(j.billing&&!j.billing.entitled){document.querySelectorAll('.nav button').forEach(b=>{if(!['settings','connections'].includes(b.dataset.page))b.disabled=true});setPage('settings');setTimeout(()=>toast('Subscribe to unlock the live decision engine'),250)}await load();await loadConnections();refreshPushStatus();refreshBilling();let qp=new URLSearchParams(location.search);if(qp.get('billing')==='success'){toast('Subscription checkout completed');history.replaceState({},'',location.pathname);setTimeout(refreshBilling,1200)}else if(qp.get('billing')==='cancelled'){toast('Checkout cancelled');history.replaceState({},'',location.pathname)}maybeStartTour();clearInterval(loadTimer);clearInterval(notifTimer);loadTimer=setInterval(load,5000);notifTimer=setInterval(pollNotifications,5000)}
function feed(received){if(!received){$('feed').textContent='NOT CONNECTED';$('feed').className='pill';return}let a=(Date.now()-new Date(received).getTime())/1000;if(a<150){$('feed').textContent='ENGINE LIVE';$('feed').className='pill live'}else{$('feed').textContent=a<600?'STALE':'OFFLINE';$('feed').className='pill'}}
function reasonsText(d,a){let r=[];if(d?.signal&&d.signal!=='WAIT')r.push(d.signal+' directional structure');if(d?.alignment!=null)r.push(d.alignment+'/'+d.alignment_total+' timeframes aligned');let n=d?.news||{};if(n.risk)r.push('event risk '+String(n.risk).toLowerCase());let y=d?.ym||{};if(y.available)r.push('YM '+(y.score>0?'bullish':y.score<0?'bearish':'neutral'));let o=d?.orderflow||{};if(o.available)r.push('YM L1 '+(o.score>.35?'buy pressure':o.score<-.35?'sell pressure':'balanced'));if(a?.reasons?.length)r.push(...a.reasons.slice(0,2));return r.length?r.join(' · '):'Waiting for live market structure, momentum, volatility and context.'}
function render(d,received,a){latest=d;feed(received);$('symbol').textContent=(d.symbol||'US30')+' · '+d.strategy_label+' · '+d.horizon;$('signal').textContent=d.signal;$('signal').className='sig '+d.signal;$('confidence').textContent=d.signal==='WAIT'?'Engine waiting for structure':'Data Engine Score '+d.confidence+'/100';$('summary').textContent='Alignment '+d.alignment+'/'+d.alignment_total+' · weighted model '+d.weighted_score;$('ring').style.setProperty('--score',d.signal==='WAIT'?Math.min(60,d.confidence||0):d.confidence);$('ringScore').textContent=(d.confidence||0);$('price').textContent=fmt(d.price);$('entry').textContent=d.entry_low==null?'—':fmt(d.entry_low)+'–'+fmt(d.entry_high);$('stop').textContent=fmt(d.stop);$('tp1').textContent=fmt(d.tp1);$('tp2').textContent=fmt(d.tp2);$('structure').textContent=d.signal==='WAIT'?'MIXED':d.signal+' BIAS';$('structure').className='val '+(d.signal==='LONG'?'good':d.signal==='SHORT'?'bad':'warn');let n=d.news||{};$('macro').textContent=n.risk||'LOW';$('macro').className='val '+(n.risk==='HIGH'?'bad':n.risk==='WATCH'?'warn':'good');$('macrod').textContent=(n.macro?.available?'FRED regime active':'Federal Reserve calendar active');let y=d.ym||{};$('ym').textContent=y.available?(y.score>0?'BULLISH CONFIRM':y.score<0?'BEARISH CONFIRM':'NEUTRAL'):'NOT PRESENT';$('ym').className='val '+(y.score>0?'good':y.score<0?'bad':'warn');let o=d.orderflow||{};$('l2').textContent=o.available?(o.score>.35?'BUY PRESSURE':o.score<-.35?'SELL PRESSURE':'BALANCED'):(o.configured?'WAITING':'NOT CONNECTED');$('l2').className='val '+(o.available?(o.score>.35?'good':o.score<-.35?'bad':'warn'):'warn');$('l2d').textContent=o.available?('L1 imbalance '+Number(o.depth_imbalance||0).toFixed(2)+' · trade delta '+Number(o.delta_norm||0).toFixed(2)+' · '+Number(o.age_seconds||0).toFixed(1)+'s'):(o.error||o.status||'');$('alignment').textContent=(d.alignment||0)+' / '+(d.alignment_total||0);$('alignDetail').textContent=d.alignment_total?'multi-timeframe directional agreement':'';$('why').textContent=reasonsText(d,a);if(a){$('readiness').textContent=a.quality;$('readyScore').textContent=a.score+'/100';$('readiness').className='readiness '+(a.quality==='READY'?'good':a.quality==='BLOCKED'?'bad':'warn');$('readyDetail').textContent=(a.reasons||[]).slice(0,4).join(' · ')||'No readiness blockers reported.'}renderAux()}
function renderCopilot(c){copilot=c;let st=c?.status||'IDLE';['IDLE','LOOKING','ARMED','LIVE'].forEach(x=>$('s'+x).className=''+(x===st?'active':''));$('tradeState').textContent=st;$('tradeDetail').textContent=c?(c.side?c.side+' · ':'')+(c.actual_entry?'entry '+fmt(c.actual_entry):c.planned_entry_low?'planned '+fmt(c.planned_entry_low)+'–'+fmt(c.planned_entry_high):''):'No live trade.';$('liveR').textContent=st==='LIVE'?(Number(c.last_r||0)>=0?'+':'')+Number(c.last_r||0).toFixed(2)+'R':'—';renderAux()}
function renderAux(){let d=latest||{},n=d.news||{},y=d.ym||{},c=copilot||{},st=c.status||'IDLE';$('oppSignal').textContent=d.signal||'WAIT';$('oppSignal').className='bigStat '+(d.signal==='LONG'?'good':d.signal==='SHORT'?'bad':'warn');$('oppText').textContent=d.signal&&d.signal!=='WAIT'?'Directional setup detected. Readiness determines whether it is actionable now.':'Engine is not currently seeing a qualified directional setup.';$('oppStructure').textContent=d.signal&&d.signal!=='WAIT'?d.signal:'MIXED';$('oppMacro').textContent=n.risk||'—';$('oppYm').textContent=y.available?(y.score>0?'BULLISH':y.score<0?'BEARISH':'NEUTRAL'):'—';$('oppReady').textContent=$('readiness').textContent||'—';$('oppWhy').textContent=$('why').textContent||'—';$('liveSide').textContent=st==='LIVE'?(c.side||'LIVE'):'IDLE';$('liveSide').className='tradeSide '+(c.side==='LONG'?'good':c.side==='SHORT'?'bad':'warn');$('liveMeta').textContent=st==='LIVE'?'Copilot is monitoring structure, risk and targets.':'Press “I’m in” when you actually enter a trade.';$('livePageR').textContent=st==='LIVE'?((Number(c.last_r||0)>=0?'+':'')+Number(c.last_r||0).toFixed(2)+'R'):'—';$('livePageR').className='rBig '+(Number(c.last_r||0)>=0?'good':'bad');$('ltEntry').textContent=fmt(c.actual_entry);$('ltPrice').textContent=fmt(c.last_price||d.price);$('ltStop').textContent=fmt(c.stop);$('ltTp1').textContent=fmt(c.tp1);$('ltTp2').textContent=fmt(c.tp2);let health=st==='LIVE'?Math.max(8,Math.min(100,50+Number(c.last_r||0)*22)):0;$('healthFill').style.width=health+'%';$('healthText').textContent=st==='LIVE'?(Number(c.last_r||0)>=1?'Trade progressing favourably. Continue monitoring structure and event risk.':Number(c.last_r||0)<-.35?'Trade under pressure. Re-check invalidation and structure.':'Position active. No major trade-health conclusion yet.'):'No position being monitored.'}
function applySettings(s){settings=s;$('strategy').value=s.strategy;$('strategy2').value=s.strategy;['nsetup','nready','nupdate','nexit','nnews','autoarm'].forEach(id=>$(id).checked=s[{nsetup:'notify_setup',nready:'notify_entry_ready',nupdate:'notify_trade_update',nexit:'notify_exit_warning',nnews:'notify_news',autoarm:'auto_arm'}[id]])}
async function load(){try{let j=await(await fetch('/api/latest',{cache:'no-store'})).json();if(!j.ok)return;applySettings(j.settings);renderCopilot(j.copilot);if(j.data)render(j.data,j.received_at,j.entry_assessment);else{latest=null;feed(null);renderAux()}document.querySelectorAll('.chartTf button').forEach(b=>b.classList.toggle('on',b.dataset.tf===chartTf));await loadCandles(false)}catch(e){}}
async function loadPerformance(){try{let p=await(await fetch('/api/performance',{cache:'no-store'})).json();$('pTracked').textContent=p.tracked??0;$('pClosed').textContent=p.closed??0;$('pTp1').textContent=p.tp1_hit_rate==null?'—':p.tp1_hit_rate+'%';$('pExp').textContent=p.expectancy_r==null?'—':p.expectancy_r+'R';if($('pManualLive'))$('pManualLive').textContent=p.manual_live_trades??0;if($('pManualClosed'))$('pManualClosed').textContent=p.manual_closed??0;$('validationStatus').textContent=(p.closed||0)>=100?'MEANINGFUL SAMPLE':(p.closed||0)>=30?'EARLY SIGNAL':'BUILDING SAMPLE';$('validationStatus').className='val '+((p.closed||0)>=100?'good':'warn')}catch(e){}}
async function saveStrategy(){let j=await(await fetch('/api/settings',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({strategy:$('strategy').value})})).json();applySettings(j.settings);toast('Strategy updated');load()}
async function syncStrategy(){$('strategy').value=$('strategy2').value;await saveStrategy()}
function urlB64ToUint8Array(s){let p='='.repeat((4-s.length%4)%4),b=(s+p).replace(/-/g,'+').replace(/_/g,'/'),r=atob(b),a=new Uint8Array(r.length);for(let i=0;i<r.length;i++)a[i]=r.charCodeAt(i);return a}
function b64urlBytes(a){if(!a)return'';let s='';new Uint8Array(a).forEach(x=>s+=String.fromCharCode(x));return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'')}
async function enablePush(){let st=$('pushStatus');if(!('Notification'in window)||!('serviceWorker'in navigator)||!('PushManager'in window)){st.textContent='Background push is not supported in this browser.';return}let p=await Notification.requestPermission();if(p!=='granted'){st.textContent='Notifications are blocked for this site. Allow them in browser/site settings, then try again.';return}try{st.textContent='Registering this device…';let reg=await navigator.serviceWorker.ready;await reg.update().catch(()=>{});let kr=await fetch('/api/push/public-key',{cache:'no-store'}),k=await kr.json();if(!kr.ok||!k.public_key)throw new Error(k.detail||'could not load push key');let sub=await reg.pushManager.getSubscription();if(sub&&sub.options&&sub.options.applicationServerKey&&b64urlBytes(sub.options.applicationServerKey)!==k.public_key){await sub.unsubscribe();sub=null;localStorage.removeItem('pushEnabled')}if(!sub)sub=await reg.pushManager.subscribe({userVisibleOnly:true,applicationServerKey:urlB64ToUint8Array(k.public_key)});let r=await fetch('/api/push/subscribe',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(sub.toJSON())}),j=await r.json();if(!r.ok||!j.ok)throw new Error(j.detail||'subscription failed');localStorage.setItem('pushEnabled','1');st.textContent='Device registered · sending test notification…';await testPush()}catch(e){console.error(e);localStorage.removeItem('pushEnabled');st.textContent='Push setup failed: '+e.message}}
async function testPush(){let st=$('pushStatus');try{let r=await fetch('/api/push/test',{method:'POST'}),j=await r.json();if(!r.ok||!j.ok)throw new Error((j.errors&&j.errors[0])||j.error||'delivery failed');toast('Push delivered');if(st)st.textContent='Push test delivered · '+(j.sent||1)+' device'+((j.sent||1)===1?'':'s')}catch(e){toast('Push test failed');if(st)st.textContent='Push test failed: '+e.message}}
async function refreshPushStatus(){let st=$('pushStatus');if(!st)return;if(!('Notification'in window)||!('serviceWorker'in navigator)||!('PushManager'in window)){st.textContent='Background push not supported in this browser.';return}if(Notification.permission==='denied'){st.textContent='Notifications blocked in browser/site settings.';return}if(Notification.permission!=='granted'){st.textContent='Not enabled on this device.';return}try{let reg=await navigator.serviceWorker.ready,sub=await reg.pushManager.getSubscription(),r=await fetch('/api/push/status',{cache:'no-store'}),j=await r.json();if(sub&&j.subscriptions>0){localStorage.setItem('pushEnabled','1');let ld=j.last_delivery;st.textContent='Background push registered'+(ld?' · last server result '+ld.status:'')+'.'}else{localStorage.removeItem('pushEnabled');st.textContent='Permission granted — press Enable background push to register this device.'}}catch(e){st.textContent='Push status check failed: '+e.message}}
async function copyPineCode(){try{let r=await fetch('/downloads/tradingview-pine?feed='+setupFeed,{cache:'no-store'});if(!r.ok)throw new Error('download failed');let t=await r.text();if(!t.trim().startsWith('//@version=6'))throw new Error('Pine file failed integrity check');await navigator.clipboard.writeText(t);$('pineCopyStatus').textContent='Copied complete Pine file to clipboard — first line verified as //@version=6';toast('Pine code copied')}catch(e){$('pineCopyStatus').textContent='Copy failed: '+e.message}}
async function saveAlerts(){let body={notify_setup:$('nsetup').checked,notify_entry_ready:$('nready').checked,notify_trade_update:$('nupdate').checked,notify_exit_warning:$('nexit').checked,notify_news:$('nnews').checked,auto_arm:$('autoarm').checked};let j=await(await fetch('/api/settings',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)})).json();applySettings(j.settings);toast('Notification preferences saved')}
async function setState(state){let r=await fetch('/api/copilot/state',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({state})});let j=await r.json();if(!r.ok){alert(j.detail);return}renderCopilot(j.session);toast('Copilot '+state.toLowerCase())}
async function startLive(){if(!latest){alert('Connect TradingView first');return}let entry=prompt('Actual entry price',latest.price||'');if(!entry)return;let r=await fetch('/api/copilot/live',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({entry:Number(entry)})});let j=await r.json();if(!r.ok){alert(j.detail);return}renderCopilot(j.session);showPage('live');toast('Live trade monitoring active')}
function chooseFeed(feed){setupFeed=feed;localStorage.setItem('setupFeed',feed);let a=$('pineDownload');if(a)a.href='/downloads/tradingview-pine?feed='+feed;let st=$('feedStandard'),cm=$('feedCme');if(st)st.className='feedCard '+(feed==='standard'?'on':'');if(cm)cm.className='feedCard '+(feed==='cme'?'on':'');let h=$('conditionHint');if(h)h.textContent=(feed==='cme'?'US30 LIVE DATA FEED V6 + CME YM':'US30 COPILOT 6.4 LIVE FEED')+' → Any alert() function call'}
async function loadConnections(){let j=await(await fetch('/api/connections',{cache:'no-store'})).json();conn=j.connections;$('webhook').textContent=conn.tradingview.webhook_url;let ww=$('wizardWebhook');if(ww)ww.textContent=conn.tradingview.webhook_url;$('fredDot').className='dot '+(conn.fred.configured?'on':'');$('dbDot').className='dot '+(conn.databento.live?'on':'');let ds=$('dbStatus');if(ds){let s=conn.databento.status||{};ds.textContent=conn.databento.live?'LIVE · YM.FUT · L1 (MBP-1)':(conn.databento.configured?((s.status||'CONNECTING')+' · YM.FUT · L1 (MBP-1)'):'Not configured');}chooseFeed(setupFeed);verifyTradingView(false)}
async function saveConnections(){let body={fred_api_key:$('fredKey').value||null,databento_api_key:$('dbKey').value||null};await fetch('/api/connections',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});$('fredKey').value='';$('dbKey').value='';loadConnections();toast('Data connection saved securely')}
async function rotateWebhook(){if(!confirm('Rotate webhook URL? Your old TradingView alert will stop working until updated.'))return;await fetch('/api/connections',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({rotate_tradingview_webhook:true})});loadConnections();toast('Webhook rotated')}
async function copyWebhook(){try{let raw=(conn?.tradingview?.webhook_url||$('webhook').textContent||'');let w=String(raw).trim().replace(/[\u0000-\u001F\u007F-\u009F\u200B-\u200D\u2060\uFEFF]/g,'');let u=new URL(w);if(u.protocol!=='https:'||!u.hostname||!/^\/webhook\/tradingview\/[0-9a-fA-F]+$/.test(u.pathname))throw new Error('invalid webhook URL');w=u.origin+u.pathname;await navigator.clipboard.writeText(w);$('webhook').textContent=w;let ww=$('wizardWebhook');if(ww)ww.textContent=w;toast('Clean webhook copied')}catch(e){console.error(e);toast('Webhook copy failed: '+e.message)}}
async function verifyTradingView(showToast=true){try{let j=await(await fetch('/api/latest',{cache:'no-store'})).json();let box=$('verifyBox'),st=$('wizardStatus');if(!box||!st)return;if(j.data&&j.received_at){let age=Math.max(0,Math.round((Date.now()-new Date(j.received_at).getTime())/1000));box.textContent='Connected · last TradingView packet '+age+'s ago · '+(j.data.symbol||'US30');box.className='verifyBox good';st.textContent=age<180?'CONNECTED':'STALE';st.className='pill '+(age<180?'live':'');if(showToast)toast('TradingView connection found')}else{box.textContent='No TradingView packet received yet. Create the alert and wait for the next completed 1-minute bar.';box.className='verifyBox notice';st.textContent='NOT VERIFIED';st.className='pill';if(showToast)toast('No TradingView packet yet')}}catch(e){if(showToast)toast('Connection check failed')}}
async function pollNotifications(){try{let j=await(await fetch('/api/notifications?after='+lastN,{cache:'no-store'})).json();for(let n of j.items){lastN=Math.max(lastN,n.id);localStorage.setItem('lastN',lastN);if(localStorage.getItem('pushEnabled')!=='1'&&Notification.permission==='granted')new Notification(n.title,{body:n.body,icon:'/icon.svg'});toast(n.title)}}catch(e){}}
if('serviceWorker'in navigator)navigator.serviceWorker.register('/sw.js').catch(()=>{});boot();
</script></body></html>'''
@app.get("/", response_class=HTMLResponse)
def index():
    return HTMLResponse(content=HTML, headers={"Cache-Control":"no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/manifest.webmanifest")
def manifest():
    return Response(content=json.dumps({"name":"US30 Copilot Data Engine","short_name":"US30 Copilot","start_url":"/","display":"standalone","background_color":"#02060d","theme_color":"#050912","icons":[{"src":"/icon.svg","sizes":"any","type":"image/svg+xml","purpose":"any maskable"}]}), media_type="application/manifest+json")

@app.get("/sw.js")
def sw():
    js=r"""const C='us30-commercial-push-v3';
self.addEventListener('install',e=>{self.skipWaiting()});
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
self.addEventListener('push',e=>{let d={};try{d=e.data?e.data.json():{}}catch(_){d={body:e.data?e.data.text():''}};e.waitUntil(self.registration.showNotification(d.title||'US30 Copilot',{body:d.body||'New Copilot alert',icon:'/icon.svg',badge:'/icon.svg',tag:'copilot-'+(d.event_type||'alert')+'-'+Date.now(),renotify:true,vibrate:[180,80,180],data:{url:d.url||'/'}}))});
self.addEventListener('notificationclick',e=>{e.notification.close();const url=e.notification.data&&e.notification.data.url?e.notification.data.url:'/';e.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(cs=>{for(const c of cs){if('navigate'in c)c.navigate(url);if('focus'in c)return c.focus()}return clients.openWindow(url)}))});
"""
    return Response(content=js,media_type="application/javascript",headers={"Service-Worker-Allowed":"/","Cache-Control":"no-store, no-cache, must-revalidate"})

@app.get("/icon.svg")
def icon():
    svg='''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><defs><radialGradient id="g"><stop offset="0" stop-color="#1cc3ff"/><stop offset="1" stop-color="#07111f"/></radialGradient></defs><rect width="512" height="512" rx="110" fill="#030811"/><circle cx="256" cy="240" r="170" fill="url(#g)" opacity=".35"/><path d="M368 145c-30-31-69-48-112-48-88 0-159 70-159 157s71 157 159 157c46 0 87-19 116-52" fill="none" stroke="#e8f2fb" stroke-width="49" stroke-linecap="round"/><circle cx="256" cy="241" r="28" fill="#18a8ff"/><path d="M256 241l94-68" stroke="#18a8ff" stroke-width="11" stroke-linecap="round"/></svg>'''
    return Response(content=svg,media_type="image/svg+xml")
