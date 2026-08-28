US30 COPILOT 6.4 - MULTI-USER PERSONAL WEBHOOK FIX V3

Replace ONLY server.py.

What this changes
- Every account now receives its own TradingView webhook URL:
  /webhook/tradingview/<private-token>
- Incoming TradingView packets are routed only to the account that owns that token.
- Your friend's account gets a different token and therefore a different feed.
- The old shared /webhook/tradingview route remains only as single-user legacy
  compatibility and deliberately refuses traffic when more than one active user exists.
- Existing working push-notification V2 code is preserved.
- FRED and Databento keys remain account-scoped/encrypted exactly as before.

IMPORTANT AFTER DEPLOY
1. Log into YOUR US30 Copilot account.
2. Connections -> copy the Personal Webhook.
3. Edit YOUR TradingView alert and replace the old webhook URL with this new personal URL.
4. Save the alert.
5. Wait for the next completed 1-minute bar.
6. Railway should show POST /webhook/tradingview/<token> ... 200 OK.
7. Your dashboard should change from OFFLINE/STALE to live/connected.

Your friend must do the same from THEIR account using THEIR personal webhook.
Do not share personal webhook URLs between accounts.
Do not change APP_SECRET.
