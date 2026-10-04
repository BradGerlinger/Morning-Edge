15-Minute Edge V10.6

Changes:
- PASS notifications disabled; only qualified UP/DOWN trades send push alerts.
- PASS calls are never scored as wins/losses and are excluded from win rate and paper P/L.
- $10 paper P/L is calculated from the actual trigger-entry ask (WIN = $10*(1/ask-1), LOSS = -$10).
- Historical qualified V10 calls are backfilled with paper P/L when possible.
- Qualified Signal Tracker restored in the V9-style layout, including W/L, win rate, paper P/L, last settled, UP/DOWN split stats, and trigger-level result table.
- Current model bias is shown even when filters produce PASS, making DOWN evaluation visible.
- Existing V10.5 Settings, push repair/test, cache fixes, and persistent warm-up remain intact.
