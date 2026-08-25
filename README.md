# US30 Copilot Fresh Commercial Test Build

Clean deployment package for a new GitHub repository and new Railway service.

## Deploy
1. Upload every file/folder in this package to the ROOT of a blank GitHub repository.
2. Create a NEW Railway service from that repository.
3. Add variables from `.env.example` (generate a strong APP_SECRET).
4. Set PUBLIC_URL to the new Railway public domain after Railway creates it.
5. Confirm `/health` returns HTTP 200.
6. In the app, copy the fixed TradingView webhook. It must end exactly `/webhook/tradingview`.
7. Send a TradingView alert and confirm Railway logs show `POST /webhook/tradingview ... 200`.

Do not reuse the old Railway service while validating this clean build.
