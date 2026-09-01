US30 COPILOT 6.4 — DATABENTO L1 MULTI-USER V5

Purpose
- Switches YM Databento confirmation from MBP-10/L2 to MBP-1/L1.
- Keeps the per-user encrypted Databento API-key architecture.
- Keeps TradingView personal webhooks, FRED, push notifications and user isolation from V4.

Live Databento subscription
- dataset: GLBX.MDP3
- schema: mbp-1
- stype_in: parent
- symbol: YM.FUT

L1 features fed into scoring
- best bid / best ask
- spread / midpoint
- top-of-book bid-vs-ask size imbalance
- aggressive trade buy/sell window and normalized delta when present in MBP-1 records
- short-term top-of-book liquidity shift
- L1 absorption proxy

UI
- Card now reads YM L1 ORDER FLOW.
- Connection status reports L1 (MBP-1), not MBP-10.

Deployment
Replace server.py, scoring.py, databento_live.py and requirements.txt with these files, then redeploy Railway. APP_SECRET and existing Railway variables must remain unchanged. Existing saved Databento keys remain encrypted in the database and are reused after restart.
