US30 COPILOT 6.4 - PUSH FIX V2

Replace ONLY server.py in the current co-pilot-6.4 repository.

V2 corrects the VAPID private-key representation passed to pywebpush.
The stable APP_SECRET-derived P-256 private key is now a 32-byte base64url
raw scalar, the representation py_vapid explicitly accepts.

Keep APP_SECRET unchanged.
Do not add VAPID_PRIVATE_KEY or VAPID_PUBLIC_KEY.
No TradingView or Databento files are changed.

After Railway redeploy:
1. Open /health and confirm push.vapid_ready=true and source=app_secret.
2. Hard refresh the app.
3. Settings -> Enable background push.
4. The app automatically sends a real push test.
