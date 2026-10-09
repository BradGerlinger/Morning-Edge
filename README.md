# Morning Edge V1.2 (Repair)

A phone-friendly, paper-only U.S. stock movement scanner.

## What it does

1. **Overnight / premarket scan**
   - Scans a liquid U.S. stock universe rather than forcing every listed microcap into the model.
   - Looks at gap size, premarket movement, premarket volume, normal liquidity, sector action, U.S. futures, Asia, and Europe.
   - Optionally adds recent company news with a Finnhub API key and recent SEC filing context.
   - Produces a ranked watchlist. This is **not** an entry signal.

2. **8:45 AM Central confirmation**
   - Waits through the first 15 minutes after the U.S. cash open.
   - Re-ranks using opening-range position, VWAP, first-15-minute relative volume, and short-term direction.
   - Labels each candidate **LONG**, **SHORT**, or **PASS**.
   - It can reverse the overnight bias if the open clearly says the opposite.
   - It will return fewer than five setups if fewer than five qualify.

3. **Market-based stop and target**
   - Stop is placed beyond nearby support/resistance with an ATR buffer.
   - Target uses the next meaningful market level when that level gives enough reward.
   - If the nearby level is too close, the model uses a 2R / ATR expansion target.
   - Setup passes when R:R is below 1.5, risk is too wide, or the stop is unrealistically tight.

4. **$100 paper trade**
   - Every qualifying setup uses $100 notional for clean comparison.
   - It logs entry, stop, target, shares, result, and dollar P/L.
   - If stop and target both appear inside the same 1-minute candle, the logic is intentionally conservative: it counts the stop first.

## Data source notes

The zero-key price feed uses Yahoo Finance chart endpoints. It is useful for research and paper testing but it is not an exchange-grade execution feed. For production trading, replace it with your broker or a licensed real-time market-data feed.

News scoring is optional and uses Finnhub if `FINNHUB_API_KEY` is set. SEC checks use official SEC submissions data and a descriptive `SEC_USER_AGENT`.

## Diagnostic improvements (V1.1)

The new **SCANNER STATUS** panel reports running/completed/failed state, attempted
symbols, successful quotes, sample provider failures, and database storage mode.
A missing watchlist is no longer represented as a completed scan with zero stocks.
The manual scan is non-blocking and status refreshes every five seconds.

**External-feed limitation:** Yahoo Finance's unofficial chart endpoint may return
HTTP 429/403, especially from cloud hosting. That cannot be resolved reliably by
changing scoring filters or buying more RAM. A suitable licensed stock-data
provider may be necessary. Scans are research/paper-only.

## Deploy on Render

1. Unzip this folder and upload all files to a new GitHub repository.
2. Create a new **Web Service** in Render from that repository.
3. Build command: `pip install -r requirements.txt`
4. Start command: `uvicorn server:app --host 0.0.0.0 --port $PORT`
5. Check the new scanner status panel and API `/api/health` after deployment.
6. For data across restarts, attach a persistent disk at `/var/data` and set
   `MORNING_EDGE_DB=/var/data/morning_edge.db` (check pricing and back up old data).
   A paid always-on instance alone does not make the local SQLite DB persistent.
7. Optional environment variables are shown in `.env.example`.
8. See `DEPLOY_STEPS.txt` for step-by-step repair installation.

## Timing

- Background scan window: roughly 4:00–8:25 AM Central on weekdays.
- Automatic confirmation: 8:45 AM Central.
- Paper positions update through the day.

## Important design choice

The app intentionally does **not** promise to scan literally every U.S. ticker with a free feed. That would add thousands of illiquid/microcap names and rate-limit the data source. V1 starts with a liquid universe because the goal is tradable movement and clean paper-test results. You can override the list with `SCAN_TICKERS`.


V1.2 - SERVER CRASH ISOLATION (OCT 9, 2026)
- Runs quote scanning in a separate subprocess. If scanner hits a native-library
  segmentation fault, it should no longer terminate the web server process.
- Writes scan progress to SQLite regularly and reports isolated scanner exit codes.
- Scans STOCK TICKERS FIRST so the 0/74 indicator means the app really has attempted
  quote requests; global and sector references no longer block ticker scanning.
- Performs a one-symbol Yahoo access test before launching 74 per-symbol fetches;
  if Yahoo is blocked (403/429/DNS), displays that failure instead of hanging.
- Dashboard no longer prints Render's huge HTML 502 response as raw text;
  it displays a concise HTTP status message.
- Enables Python faulthandler tracebacks for lower-level Python/native crashes.
- Does not solve vendor feed restrictions, nor ensure strategy profitability.
- Still uses temporary disk by default; configure a Render persistent disk
  after stabilizing, or historical trade data may vanish on a deploy/restart.

VALIDATION: Eight offline unit tests passed. A local server integration check
confirmed that the dashboard API remained healthy after the scan worker failed
on an intentionally inaccessible upstream. Yahoo live data was NOT validated
in Render; the feed may require replacement with a licensed API.
