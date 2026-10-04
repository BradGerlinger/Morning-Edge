15-MINUTE EDGE V10.8 — SERVER REPAIR
=====================================

WHAT THIS ZIP IS
This is a replacement-file update for the current GitHub project.
It does NOT replace your icons, Dockerfile, manifest, service worker, or index.html.
Those stay exactly as they are.

FILE TO REPLACE
1. Replace the current server.py in GitHub with the server.py in this ZIP.
2. Commit the change.
3. Let Render redeploy.

WHAT V10.8 FIXES
- Cuts the Kalshi request load substantially.
- Adds one global request pacing gate.
- Honors HTTP 429 backoff instead of continuing to hammer the feed.
- Reduces BRTI polling to every 5 seconds.
- Reduces market/orderbook refresh to every 10 seconds.
- Reduces official settlement checks to every 60 seconds, two pending tickers at a time.
- Keeps all model weights and trading thresholds unchanged.
- Fixes the 7:30 capture race:
  live qualification remains 8:30 -> 7:30, but the final recorder has a small
  execution tolerance so a delayed poll cannot permanently record a false PASS.
- PASS remains excluded from WIN/LOSS.
- WIN/LOSS still comes only from Kalshi's official result for the exact ticker.
- Push test now SAVES the iPhone subscription before sending the test.
  This repairs the "SUBSCRIBED on phone / 0 saved on server" condition.

AFTER RENDER SAYS LIVE
1. Open the installed 15 Edge app.
2. Settings -> ENABLE / REPAIR NOTIFICATIONS.
3. Tap SEND TEST NOTIFICATION.
4. Tap REFRESH STATUS.
5. Confirm:
   - Server push: READY · 1 saved
   - Collector: RUNNING
   - Feed error: none
   - Version: V10.8

IMPORTANT
Do not change the model thresholds yet. This repair is infrastructure/timing only.
