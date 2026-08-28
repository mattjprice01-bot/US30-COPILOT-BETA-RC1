US30 COPILOT 6.4 - PUSH NOTIFICATION FIX

Replace ONLY server.py in the current working co-pilot-6.4 GitHub repo.
Do not change TradingView, scoring.py, run.py, railway.json or the webhook.

Railway variables to keep:
APP_SECRET=<existing secret>
VAPID_SUBJECT=mailto:<your email>

No VAPID_PRIVATE_KEY or VAPID_PUBLIC_KEY is required for this patch.
If they are absent, the app derives a stable VAPID key from APP_SECRET so redeploys do not invalidate device subscriptions.

After deploy:
1. Open /health and confirm push.vapid_ready is true.
2. Open app -> Settings.
3. Press Enable background push.
4. Allow browser notification permission.
5. The app will detect an old/mismatched subscription, replace it, register the device, and send a real test.
6. The status now reports the actual delivery result instead of always saying success.

New diagnostic endpoint (while logged in):
/api/push/status

Important:
- Keep APP_SECRET unchanged after users subscribe; changing it changes the derived VAPID key.
- On Android, allow notifications for the browser/PWA in phone settings.
