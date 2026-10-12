# Morning Edge V1.3 — Forward-only paper scalp scanner

**Mode:** Local paper simulator only. No live brokerage execution code. No order-submission code to Alpaca paper brokerage yet. **Do not infer the app is trading in an Alpaca paper account**: it is performing internal paper simulation against retrieved market candles.

## What changed from V1.2

- Added `edge_engine.py`: an independent Nasdaq / stock scan loop using Alpaca market-data REST endpoints instead of Yahoo Finance for the active scanner. The old Yahoo-based functions are retained for historical compatibility, but **not scheduled by default**.
- Uses completed 1-minute candles from current trading day only and rejects stale data.
- Computes a 15-minute opening range (9:30–9:44 ET, 8:30–8:44 CT).
- Looks for a *fresh* break above/below the range after 10:00 ET (9:00 CT), with VWAP, relative minute volume, QQQ direction and at least 4/7 Mag7 stocks agreeing.
- At the first valid signal, creates a `PENDING` simulated order, **not an automatic fill at the old bar close**. Next bars may trigger an entry. Orders expire within roughly 4 minutes if never triggered.
- Simulates a structural stop beyond the opening range and a 2R take profit, using a configurable 5 bps per-side fill-friction assumption by default. On an entry candle that also reaches the stop, counts a stop. If a later candle touches stop and target, counts a stop first.
- Records simulated entries/exits and P/L in SQLite with one signal per symbol per trading date and caps on positions, trades/day, realized daily loss.
- Rebuilt the mobile dashboard showing feed state and individual PENDING, OPEN, TARGET, STOP, EXPIRED paper-trade statuses.
- Explicitly marks that VIX/VXN is not integrated, and does not pretend it contributes to any entry.

## Caveats

- This is a **prototype**, not a validated strategy. It has not been tested against an Alpaca account or live market-data provider from this development environment. It passes deterministic offline tests.
- Alpaca Basic real-time IEX feed is a **single-exchange subset**, not a consolidated tape. Signals using IEX bars may differ materially from full-market SIP price, volume or VWAP. **Do not promote this strategy to live trading without consolidated licensed quotes, proper position reconciliation, and out-of-sample testing**.
- Paper fills use 1-minute OHLC bars; ordering inside one minute is unknown and results can be wrong even with conservative handling. Spread, market impact, short locate/borrow availability, broker rejects, halts, and real execution differences are not fully modeled. Five basis points is an illustrative setting, not a measurement of actual friction.
- Local paper stops and targets are **not actual protective broker orders**. If the process stops or the data feed fails, an open simulated position may not be updated until quotes return. Real broker bracket orders are a **future milestone**, requiring separate verification and authorization.
- Simulator permits fractional shares to keep positions at $100. Real broker advanced orders for fractional positions may be restricted; never assume these would be accepted as bracket orders.
- This one-minute REST polling version is a testbed, not a low-latency scalper. Request/processing delays can miss breakouts; future production design should use streaming quotes and actual bid/ask spreads.
- Dashboard is **read-only**, but publicly deployed Render URLs can expose simulated trade records if left public. Add authentication before connecting any actual brokerage system.
- Run **one server worker/Render replica only** and use a persistent SQLite volume or a durable database. Multiple replicas may create competing schedulers and shared-database contention. Without a persistent disk, trade history disappears after Render instance replacement.

## Setup

1. Deploy to the existing Render web service from the repo root. Keep Python `3.12.10`. Build command: `pip install -r requirements.txt`.
2. Start command: `uvicorn server:app --host 0.0.0.0 --port $PORT --loop asyncio --http h11`.
3. In Render > Environment, add **ALPACA_KEY_ID** and **ALPACA_SECRET_KEY**. Prefer paper credentials. Do **not** send these keys to chat, place them in GitHub, or embed them in HTML.
4. Leave `ALPACA_DATA_FEED=iex` unless entitled to SIP coverage. This feed is for development, not a proven execution-grade consolidated scanner.
5. `MORNING_EDGE_DB=/var/data/morning_edge.db` only if `/var/data` is actually a mounted Render persistent disk. Back up the old database first. SQLite migration adds a separate `edge_trades` table; does not wipe historic records.
6. Visit the deployed URL: dashboard must display `V1.3` and `PAPER SIMULATOR ONLY`. Check the market-data state during regular market hours (8:30 AM to 3 PM CT weekdays). If keys fail, it will show `SETUP NEEDED` or `FEED NOT READY` and **not invent any trade**.
7. Monitor multiple weeks of forward paper data and export/query SQLite before considering a broker API for simulated bracket orders. Unit tests: `python -m pytest -q test_edge_engine.py test_repair.py` (pytest is a development-only dependency).

## Default controls

```
ALPACA_DATA_FEED=iex
EDGE_SYMBOLS=NVDA,TSLA,AAPL,MSFT,AMZN,META,GOOGL,AMD,QQQ
EDGE_PAPER_NOTIONAL=100
EDGE_MAX_TRADES_DAY=5
EDGE_MAX_OPEN=3
EDGE_DAILY_LOSS_CUTOFF=10
EDGE_MIN_VOLUME_MULT=1.2
EDGE_SLIPPAGE_BPS=5
EDGE_LEGACY_SCANNER_ENABLED=0
```

## Trade engine overview

- Fetches 1-minute completed bars from Alpaca's documented data endpoint once/minute, using a small bounded thread pool.
- No signal before 9:00 CT, no new entry signal at/after 10:30 CT; paper open positions are checked through 3:10 PM CT while feed works. Intended EOD liquidation is 3:00 PM CT if price data is available.
- Never fabricate signals when API keys, fresh QQQ data, or a minimum number of Mag7 confirmations are missing.
- Backend endpoints: `GET /api/edge` for current paper summary and `GET /api/health` for server health. Previous V1.2 diagnostic API remains in the codebase; it does not run automatically unless `EDGE_LEGACY_SCANNER_ENABLED=1`.

## Next milestone before any broker automation

- Establish that the data provider is stable on Render (freshness monitoring and reconnect behavior).
- Compare this trial strategy with a base opening-range rule using multiple market regimes and out-of-sample forward tests; look at net P/L, profit factor, max drawdown, per-symbol performance and execution assumptions.
- Integrate a **paper brokerage** with supported whole-share bracket orders. Verify API order acknowledgments, fills, conditional exits, cancel/replace, orphan positions, duplicate prevention, trading halts and network failures. Do not use live credentials.
- Only after separate user approval, verified broker constraints and account risk limits should any live execution feature be considered.
