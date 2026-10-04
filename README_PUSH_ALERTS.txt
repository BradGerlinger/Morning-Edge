15-Minute Edge — Push Alert Update

UPLOAD/REPLACE:
- server.py
- index.html
- sw.js
- requirements.txt (new/replace)

Then deploy latest commit in Render.

ON IPHONE:
1. Open 15-Minute Edge from the HOME SCREEN icon (not a normal Safari tab).
2. Tap ENABLE 7:30 PUSH ALERTS.
3. Tap Allow when iPhone asks for notification permission.
4. The status should change to: 7:30 alerts are ON for this phone.

The server sends a push only when the frozen 7:30 final decision is UP or DOWN. PASS does not push.
The notification contains side, frozen ask, model probability, and model edge.

Automatic real-money orders are intentionally NOT enabled in this build.
