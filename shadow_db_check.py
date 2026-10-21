"""RC1.1 isolated Shadow database diagnostic.

Runs as a Railway pre-deploy gate. It never prints credentials or customer data.
A non-zero exit blocks deployment if the isolated PostgreSQL connection/schema is not ready.
"""
from __future__ import annotations

import os
import sys

import psycopg


def main() -> int:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        print("[RC1.1-SHADOW-DB] FAIL: DATABASE_URL is not configured", flush=True)
        return 2

    try:
        with psycopg.connect(database_url) as con:
            with con.cursor() as cur:
                cur.execute("SELECT 1")
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("database ping returned unexpected result")
                cur.execute(
                    """CREATE TABLE IF NOT EXISTS thesis_shadow_events(
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        created_at TEXT NOT NULL,
                        session_id BIGINT,
                        thesis_id TEXT,
                        strategy TEXT NOT NULL,
                        side TEXT,
                        event_type TEXT NOT NULL,
                        signal TEXT,
                        confidence INTEGER,
                        weighted_score DOUBLE PRECISION,
                        price DOUBLE PRECISION,
                        details_json TEXT NOT NULL DEFAULT '{}'
                    )"""
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_thesis_shadow_user_created "
                    "ON thesis_shadow_events(user_id,created_at)"
                )
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema='public' AND table_name='thesis_shadow_events')"
                )
                exists = bool(cur.fetchone()[0])
            con.commit()
        if not exists:
            raise RuntimeError("thesis_shadow_events was not found after schema check")
        print("[RC1.1-SHADOW-DB] PASS: connection OK; thesis_shadow_events ready", flush=True)
        return 0
    except Exception as exc:
        print(f"[RC1.1-SHADOW-DB] FAIL: {type(exc).__name__}: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
