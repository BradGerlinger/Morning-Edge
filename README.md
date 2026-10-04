# 15-Minute Edge Secure — iPhone App + BRTI Backend

This build keeps your Kalshi private key on the server. The iPhone receives only market data.

## What it does
- Reads the active public `KXBTC15M` Kalshi market and order book.
- Authenticates server-side to Kalshi using RSA-PSS/SHA-256.
- Calls Kalshi's CF Benchmarks passthrough for `BRTI`.
- Polls BRTI once per second (within the documented Basic-tier passthrough budget).
- Calculates short BRTI momentum, target distance, order-book imbalance, a heuristic score, model probability, and value edge.
- Shows UP / DOWN / PASS and a fixed $10 test stake.
- Does **not** submit orders.

## Required secrets
Create a Kalshi API key in **Account & security → API Keys**.

Set:
- `KALSHI_API_KEY_ID` = your API Key ID
- `KALSHI_PRIVATE_KEY_PEM` = the full contents of the downloaded `.key` file

Do not put either secret into `static/index.html`, GitHub source code, or iPhone local storage.

Kalshi's CF Benchmarks passthrough is entitlement-gated. If `/api/brti` returns an authorization error, the key can still be valid while the account lacks CF Benchmarks passthrough entitlement.

## Easiest cloud deployment: Render
1. Put this folder in a **private GitHub repository**.
2. In Render, create a Web Service from that repo. This project includes `render.yaml` and a `Dockerfile`.
3. Add `KALSHI_API_KEY_ID` as a secret environment variable.
4. Open your downloaded `.key` file in a text editor, copy the entire PEM including the BEGIN/END lines, and add it as `KALSHI_PRIVATE_KEY_PEM`.
5. Deploy.
6. Open `https://YOUR-SERVICE.onrender.com/api/health`.
7. Then open `/api/brti`. You should see a numeric BRTI value.
8. Open the root URL in Safari on iPhone → Share → **Add to Home Screen**.

## Local computer test
Create a `.env` only for your own shell or export the variables, then:
```
pip install -r requirements.txt
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_FILE="/full/path/to/your-key.key"
uvicorn server:app --host 0.0.0.0 --port 8080
```
Open `http://127.0.0.1:8080/api/health`, then `/api/brti`.

## Security rules
- Never paste the private key into a public repo.
- Use a private GitHub repo if using GitHub.
- Rotate/delete the API key in Kalshi if it is ever exposed.
- This app does not need order placement; if Kalshi offers scoped API permissions, choose the least privilege that still permits the required data endpoint.

## V10.10 Multi-Crypto Profit Lab
V10.10 preserves the V10.9 BTC Value decision engine and adds a research-only collector for BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, NEAR and ZEC. Spot prices are sampled together from Coinbase's public USD exchange-rate endpoint. Kalshi 15-minute series are rotated through the existing request pacing gate. BTC impulse/follower-lag candidates are paper-traded only and settled from the exact Kalshi ticker. `/api/research/stats` ranks settled paper trades by P/L; `/api/research/export.csv` exports the research paper trades.

Defaults are intentionally conservative and configurable with `EDGE_BTC_IMPULSE_30S`, `EDGE_FOLLOWER_MAX_MOVE_30S`, `EDGE_PAPER_MIN_ASK`, `EDGE_PAPER_MAX_ASK`, `EDGE_RESEARCH_SPOT_INTERVAL`, and `EDGE_RESEARCH_MARKET_INTERVAL`. No live order endpoint is implemented.


## V10.11 Full-Signal Settlement / Backtest Logger
V10.11 leaves the BTC Value qualification logic and push-alert gate unchanged. Every recorded BTC 15-minute signal, including PASS, is now revisited after market settlement and tied to the exact Kalshi ticker result. Qualified signals continue to populate `result` and `pnl` as paper trades. Every signal additionally receives `outcome`, `hypothetical_result`, `hypothetical_pnl`, `settlement_source`, `kalshi_result`, and `settled_ts`, so rejected signals can be analyzed without contaminating live trade statistics. Existing PASS history is backfilled progressively through the same paced Kalshi request gate. `/api/stats` now exposes `all_signals_settled`, `passes_settled`, and `backtest_settlement_backlog`.
