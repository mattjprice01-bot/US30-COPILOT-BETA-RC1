-- RC1 production PostgreSQL audit, 25 September 2026 through execution time.
-- Run only in production Postgres, not the shadow database.
-- Aggregate output only: no raw payloads, customer details or credentials.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT current_timestamp AS checked_at,
       current_setting('transaction_read_only') AS read_only;

SELECT 'snapshots' AS dataset, count(*) AS rows, min(received_at) AS first_at,
       max(received_at) AS last_at FROM snapshots
UNION ALL
SELECT 'trades', count(*), min(opened_at), max(opened_at) FROM trades
UNION ALL
SELECT 'copilot_sessions', count(*), min(created_at), max(created_at) FROM copilot_sessions;

SELECT user_id, symbol, count(*) AS snapshots,
       min(received_at) AS first_at, max(received_at) AS last_at
FROM snapshots WHERE received_at::timestamptz >= '2026-09-25 00:00:00+00'
GROUP BY user_id, symbol ORDER BY user_id, symbol;

-- Duplicate / non-increasing market timestamps must be scoped to each feed user.
WITH packets AS (
  SELECT id, user_id, symbol, received_at,
         CASE WHEN raw_json::jsonb->>'ts' ~ '^[0-9]+$'
              THEN (raw_json::jsonb->>'ts')::bigint END AS market_ts
  FROM snapshots WHERE received_at::timestamptz >= '2026-09-25 00:00:00+00'
), ordered AS (
  SELECT *, lag(market_ts) OVER (PARTITION BY user_id, symbol ORDER BY id) AS previous_ts
  FROM packets
)
SELECT user_id, symbol, count(*) AS deliveries,
       count(DISTINCT market_ts) AS unique_market_times,
       count(*) FILTER (WHERE market_ts IS NULL) AS missing_market_time,
       count(*) FILTER (WHERE market_ts = previous_ts) AS repeated_previous_time,
       count(*) FILTER (WHERE market_ts < previous_ts) AS out_of_order
FROM ordered GROUP BY user_id, symbol ORDER BY user_id, symbol;

-- Gaps are reported, not automatically called feed failures: weekends/sessions matter.
WITH bars AS (
  SELECT user_id, symbol, ts,
         lag(ts) OVER (PARTITION BY user_id, symbol ORDER BY ts) AS previous_ts
  FROM candles_1m WHERE ts >= 1790294400000
)
SELECT user_id, symbol, count(*) AS unique_bars,
       to_timestamp(min(ts)/1000.0) AS first_market_time,
       to_timestamp(max(ts)/1000.0) AS last_market_time,
       count(*) FILTER (WHERE ts-previous_ts > 90000) AS gaps_over_90_seconds,
       max(ts-previous_ts)/1000.0 AS largest_gap_seconds
FROM bars GROUP BY user_id, symbol ORDER BY user_id, symbol;

SELECT user_id, strategy, status, close_reason, count(*) AS trades,
       count(*) FILTER (WHERE tp1_hit=1) AS tp1_flags,
       count(*) FILTER (WHERE status='CLOSED' AND realised_r IS NULL) AS missing_realised_r,
       avg(realised_r) FILTER (WHERE status='CLOSED') AS mean_recorded_r
FROM trades WHERE opened_at::timestamptz >= '2026-09-25 00:00:00+00'
GROUP BY user_id, strategy, status, close_reason ORDER BY user_id, strategy, status, close_reason;

SELECT count(*) FILTER (WHERE side='LONG' AND NOT(stop<entry AND entry<tp1 AND tp1<=tp2)
                        OR side='SHORT' AND NOT(tp2<=tp1 AND tp1<entry AND entry<stop)) AS invalid_geometry,
       count(*) FILTER (WHERE risk<=0 OR abs(risk-abs(entry-stop))>0.000001) AS invalid_risk,
       count(*) FILTER (WHERE status='CLOSED' AND closed_at::timestamptz<opened_at::timestamptz) AS closed_before_open
FROM trades WHERE opened_at::timestamptz >= '2026-09-25 00:00:00+00';

SELECT user_id, copilot_session_id, count(*) AS records
FROM trades WHERE copilot_session_id IS NOT NULL
GROUP BY user_id, copilot_session_id HAVING count(*)>1;
ROLLBACK;

-- SHADOW: run the following separately in the isolated shadow Postgres.
-- First verify tables exist, then run counts only for returned tables.
-- BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
-- SET LOCAL statement_timeout = '30s';
-- SELECT table_name FROM information_schema.tables
-- WHERE table_schema='public' AND table_name IN
-- ('snapshots','candles_1m','trades','copilot_sessions','thesis_shadow_events');
-- SELECT count(*),min(created_at),max(created_at) FROM thesis_shadow_events;
-- SELECT event_type,count(*) FROM thesis_shadow_events GROUP BY event_type;
-- SELECT count(*),min(received_at),max(received_at) FROM snapshots;
-- ROLLBACK;
