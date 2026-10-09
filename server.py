from __future__ import annotations

import csv
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import math
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

APP_NAME = "Morning Edge V1"
CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent
# Use a persistent Render disk when one has been attached. Never package live trade data.
DEFAULT_DB = "/var/data/morning_edge.db" if Path("/var/data").is_dir() else "/tmp/morning_edge.db"
DB = Path(os.getenv("MORNING_EDGE_DB", DEFAULT_DB))
DB.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("morning-edge")

PAPER_DOLLARS = float(os.getenv("PAPER_DOLLARS", "100"))
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
MAX_SETUPS = int(os.getenv("MAX_SETUPS", "5"))
SCAN_INTERVAL_MIN = int(os.getenv("SCAN_INTERVAL_MIN", "15"))
CONFIRM_HOUR_CT = int(os.getenv("CONFIRM_HOUR_CT", "8"))
CONFIRM_MINUTE_CT = int(os.getenv("CONFIRM_MINUTE_CT", "45"))
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "MorningEdge research app contact@example.com")

# Liquid U.S. fallback universe. Users can override with SCAN_TICKERS.
DEFAULT_TICKERS = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AMD","AVGO","NFLX","PLTR",
    "INTC","MU","ARM","QCOM","SMCI","ORCL","CRM","ADBE","UBER","SHOP","COIN","MSTR",
    "JPM","BAC","WFC","GS","MS","C","V","MA","PYPL","AXP",
    "XOM","CVX","COP","OXY","SLB","HAL",
    "LLY","UNH","JNJ","PFE","MRK","ABBV","TMO","AMGN","GILD","BMY",
    "WMT","COST","HD","LOW","TGT","NKE","SBUX","MCD",
    "BA","CAT","GE","DE","HON","UPS","FDX",
    "F","GM","RIVN","LCID",
    "DIS","CMCSA","T","VZ","TMUS",
    "SPY","QQQ","IWM","DIA","XLK","XLF","XLE","XLV","XLY","XLI","XLP","XLU","XLB","XLRE"
]

GLOBAL_SYMBOLS = {
    "S&P futures": "ES=F",
    "Nasdaq futures": "NQ=F",
    "Russell futures": "RTY=F",
    "Nikkei": "^N225",
    "Hang Seng": "^HSI",
    "Shanghai": "000001.SS",
    "FTSE 100": "^FTSE",
    "DAX": "^GDAXI",
    "Crude": "CL=F",
    "Gold": "GC=F",
    "Dollar": "DX-Y.NYB",
}

SECTOR_ETF = {
    "Technology": "XLK", "Financials": "XLF", "Energy": "XLE", "Health Care": "XLV",
    "Consumer Discretionary": "XLY", "Industrials": "XLI", "Consumer Staples": "XLP",
    "Utilities": "XLU", "Materials": "XLB", "Real Estate": "XLRE", "Communication": "XLC",
}

TICKER_SECTOR = {
    "AAPL":"Technology","MSFT":"Technology","NVDA":"Technology","AMD":"Technology","AVGO":"Technology","INTC":"Technology","MU":"Technology","ARM":"Technology","QCOM":"Technology","SMCI":"Technology","ORCL":"Technology","CRM":"Technology","ADBE":"Technology",
    "META":"Communication","GOOGL":"Communication","GOOG":"Communication","NFLX":"Communication","DIS":"Communication","CMCSA":"Communication","T":"Communication","VZ":"Communication","TMUS":"Communication",
    "AMZN":"Consumer Discretionary","TSLA":"Consumer Discretionary","HD":"Consumer Discretionary","LOW":"Consumer Discretionary","NKE":"Consumer Discretionary","SBUX":"Consumer Discretionary","MCD":"Consumer Discretionary","F":"Consumer Discretionary","GM":"Consumer Discretionary","RIVN":"Consumer Discretionary","LCID":"Consumer Discretionary",
    "JPM":"Financials","BAC":"Financials","WFC":"Financials","GS":"Financials","MS":"Financials","C":"Financials","V":"Financials","MA":"Financials","PYPL":"Financials","AXP":"Financials",
    "XOM":"Energy","CVX":"Energy","COP":"Energy","OXY":"Energy","SLB":"Energy","HAL":"Energy",
    "LLY":"Health Care","UNH":"Health Care","JNJ":"Health Care","PFE":"Health Care","MRK":"Health Care","ABBV":"Health Care","TMO":"Health Care","AMGN":"Health Care","GILD":"Health Care","BMY":"Health Care",
    "WMT":"Consumer Staples","COST":"Consumer Staples","TGT":"Consumer Staples",
    "BA":"Industrials","CAT":"Industrials","GE":"Industrials","DE":"Industrials","HON":"Industrials","UPS":"Industrials","FDX":"Industrials",
}

_session = requests.Session()
_user_agent = "Mozilla/5.0 (compatible; MorningEdge/1.1; research; noncommercial)"
_session.headers.update({"User-Agent": _user_agent, "Accept": "application/json"})
_request_local = threading.local()


def _feed_session() -> requests.Session:
    # Each worker thread owns its HTTP Session (requests.Session is not thread-safe).
    if not hasattr(_request_local, "session"):
        session = requests.Session()
        session.headers.update({"User-Agent": _user_agent, "Accept": "application/json"})
        _request_local.session = session
    return _request_local.session


_db_lock = threading.Lock()
_worker_started = False
_job_lock = threading.RLock()
_scan_job: Dict[str, Any] = {"state": "idle", "started_at": None, "finished_at": None, "total": 0, "scanned": 0, "processed": 0, "error": None, "errors": []}
_last_auto_bucket = None
MAX_FETCH_WORKERS = max(1, min(5, int(os.getenv("FETCH_WORKERS", "3"))))


def now_ct() -> datetime:
    return datetime.now(timezone.utc).astimezone(CT)


def scan_universe() -> List[str]:
    raw = os.getenv("SCAN_TICKERS", "").strip()
    if raw:
        vals = [x.strip().upper() for x in raw.replace("\n", ",").split(",") if x.strip()]
        return list(dict.fromkeys(vals))
    return DEFAULT_TICKERS.copy()


def db_conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, timeout=20)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _db_lock:
        c = db_conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS scans (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          scan_ts TEXT NOT NULL,
          scan_date TEXT NOT NULL,
          ticker TEXT NOT NULL,
          sector TEXT,
          price REAL,
          prev_close REAL,
          gap_pct REAL,
          pm_change_pct REAL,
          pm_volume REAL,
          avg_daily_volume REAL,
          dollar_volume REAL,
          catalyst_score REAL,
          catalyst_text TEXT,
          sec_forms TEXT,
          global_score REAL,
          sector_score REAL,
          liquidity_score REAL,
          movement_score REAL,
          prelim_bias TEXT,
          prelim_score REAL,
          UNIQUE(scan_ts,ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_scans_date_score ON scans(scan_date, prelim_score DESC);

        CREATE TABLE IF NOT EXISTS confirmations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          confirm_ts TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          ticker TEXT NOT NULL,
          overnight_bias TEXT,
          decision TEXT NOT NULL,
          score REAL,
          entry REAL,
          stop REAL,
          target REAL,
          rr REAL,
          opening_high REAL,
          opening_low REAL,
          vwap REAL,
          first15_volume REAL,
          rel_volume REAL,
          reason TEXT,
          paper_dollars REAL,
          shares REAL,
          status TEXT DEFAULT 'OPEN',
          exit_price REAL,
          exit_reason TEXT,
          pnl_dollars REAL,
          settled_ts TEXT,
          UNIQUE(trade_date,ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_conf_date ON confirmations(trade_date, score DESC);

        CREATE TABLE IF NOT EXISTS globals (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          label TEXT NOT NULL,
          symbol TEXT NOT NULL,
          price REAL,
          change_pct REAL
        );
        CREATE TABLE IF NOT EXISTS scan_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          status TEXT NOT NULL,
          universe_count INTEGER NOT NULL DEFAULT 0,
          processed INTEGER NOT NULL DEFAULT 0,
          scanned INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          errors_json TEXT
        );
        """)
        c.commit(); c.close()


def _yf_chart(symbol: str, period: str = "5d", interval: str = "5m", prepost: bool = True) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": period, "interval": interval, "includePrePost": "true" if prepost else "false", "events": "div,splits"}
    # Yahoo's no-key chart API may deny requests from hosted IPs. Report that explicitly.
    # Do not silently substitute yesterday's close as current premarket data.
    r = _feed_session().get(url, params=params, timeout=(4, 7))
    if not r.ok:
        raise RuntimeError(f"Yahoo Finance HTTP {r.status_code} for {symbol}")
    payload = r.json().get("chart") or {}
    if payload.get("error"):
        raise RuntimeError(f"Yahoo Finance {symbol}: {payload['error']}")
    data = payload.get("result")
    if not data:
        return pd.DataFrame()
    res = data[0]
    ts = res.get("timestamp") or []
    q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    adj = ((res.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose")
    idx = pd.to_datetime(ts, unit="s", utc=True)
    df = pd.DataFrame({
        "open": q.get("open", []), "high": q.get("high", []), "low": q.get("low", []),
        "close": q.get("close", []), "volume": q.get("volume", []),
    }, index=idx)
    if adj and len(adj) == len(df):
        df["adjclose"] = adj
    df = df.apply(pd.to_numeric, errors="coerce")
    return df.dropna(subset=["close"])


def _daily(symbol: str, period: str = "3mo") -> pd.DataFrame:
    return _yf_chart(symbol, period=period, interval="1d", prepost=False)


def _headline_catalyst(ticker: str) -> Tuple[float, str]:
    if not FINNHUB_API_KEY:
        return 0.0, "No news API key; price/volume scan active"
    end = now_ct().date()
    start = end - timedelta(days=2)
    url = "https://finnhub.io/api/v1/company-news"
    try:
        r = _session.get(url, params={"symbol": ticker, "from": start.isoformat(), "to": end.isoformat(), "token": FINNHUB_API_KEY}, timeout=8)
        items = r.json() if r.ok else []
    except Exception:
        items = []
    if not isinstance(items, list) or not items:
        return 0.0, "No major headline found"
    keywords_hi = ["earnings", "guidance", "approval", "fda", "merger", "acquisition", "contract", "offering", "investigation", "lawsuit", "upgrade", "downgrade", "forecast", "layoff", "bankruptcy"]
    best = None; best_score = 0.0
    for it in items[:30]:
        h = str(it.get("headline") or "")
        score = sum(4 for k in keywords_hi if k in h.lower())
        age_h = max(0.0, (time.time() - float(it.get("datetime") or time.time())) / 3600)
        score += max(0.0, 8.0 - min(age_h, 8.0))
        if score > best_score:
            best_score = score; best = h
    return min(25.0, best_score), (best or "Headline activity")[:240]


def _sec_recent_forms(ticker: str) -> Tuple[float, str]:
    # SEC check is intentionally light and only used after a ticker ranks highly.
    try:
        mp = _session.get("https://www.sec.gov/files/company_tickers.json", headers={"User-Agent": SEC_USER_AGENT}, timeout=10)
        if not mp.ok:
            return 0.0, ""
        mapping = mp.json()
        cik = None
        for v in mapping.values():
            if str(v.get("ticker", "")).upper() == ticker.upper():
                cik = int(v["cik_str"]); break
        if cik is None:
            return 0.0, ""
        sub = _session.get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json", headers={"User-Agent": SEC_USER_AGENT}, timeout=10)
        if not sub.ok:
            return 0.0, ""
        recent = sub.json().get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        today = now_ct().date()
        found = []
        for form, d in zip(forms[:30], dates[:30]):
            try: fd = datetime.fromisoformat(d).date()
            except Exception: continue
            if (today - fd).days <= 1 and form in {"8-K","10-Q","10-K","S-3","S-1","424B5","SC 13D","SC 13G"}:
                found.append(form)
        if not found:
            return 0.0, ""
        score = min(15.0, 6.0 + 3.0 * len(set(found)))
        return score, ",".join(sorted(set(found)))
    except Exception:
        return 0.0, ""


def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0): return None
    return (a / b - 1.0) * 100.0


def _global_snapshot() -> List[Dict[str, Any]]:
    out=[]
    ts=now_ct().isoformat()
    def fetch(label: str, sym: str):
        try:
            d=_daily(sym,"5d")
            if len(d)<2: return None
            p=float(d["close"].iloc[-1]); pc=float(d["close"].iloc[-2])
            return {"label":label,"symbol":sym,"price":p,"change_pct":_pct(p,pc)}
        except Exception as exc:
            logger.warning("Global reference %s unavailable: %s", sym, exc)
            return None
    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
        futs=[pool.submit(fetch,label,sym) for label,sym in GLOBAL_SYMBOLS.items()]
        for f in as_completed(futs):
            val=f.result()
            if val:out.append(val)
    with _db_lock:
        c=db_conn()
        for x in out:
            c.execute("INSERT INTO globals(ts,label,symbol,price,change_pct) VALUES(?,?,?,?,?)", (ts,x["label"],x["symbol"],x["price"],x["change_pct"]))
        c.commit(); c.close()
    return out


def _market_context(globals_: List[Dict[str, Any]]) -> float:
    vals = {x["label"]: x.get("change_pct") for x in globals_}
    weighted = 0.0
    for k,w in [("S&P futures",0.35),("Nasdaq futures",0.25),("Nikkei",0.1),("Hang Seng",0.08),("FTSE 100",0.12),("DAX",0.10)]:
        v = vals.get(k)
        if v is not None and math.isfinite(v): weighted += w * max(-2.0,min(2.0,float(v)))
    return max(-10.0,min(10.0, weighted * 5.0))


def _ticker_metrics(ticker: str, errors: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    try:
        intr = _yf_chart(ticker, "5d", "5m", True)
        daily = _daily(ticker, "3mo")
        if intr.empty or len(daily) < 20:
            raise RuntimeError("missing intraday or 20-day historical candles")
        local = intr.tz_convert(ET)
        today = now_ct().astimezone(ET).date()
        tdf = local[local.index.date == today]
        if tdf.empty:
            raise RuntimeError("no current-day candles (feed may be delayed or market closed)")
        pm = tdf.between_time("04:00", "09:29")
        if pm.empty or pm["close"].dropna().empty:
            raise RuntimeError("no current-day premarket candles")
        price = float(pm["close"].dropna().iloc[-1])
        prev_close = float(daily["close"].dropna().iloc[-2] if daily.index[-1].date() >= today else daily["close"].dropna().iloc[-1])
        pm_first = float(pm["open"].dropna().iloc[0]) if not pm.empty and not pm["open"].dropna().empty else price
        pm_vol = float(pm["volume"].fillna(0).sum()) if not pm.empty else 0.0
        avg_vol = float(daily["volume"].tail(20).mean())
        adv_dollars = float((daily["close"].tail(20) * daily["volume"].tail(20)).mean())
        gap = _pct(pm_first, prev_close) or 0.0
        pm_chg = _pct(price, pm_first) or 0.0
        return {"ticker":ticker,"price":price,"prev_close":prev_close,"gap_pct":gap,"pm_change_pct":pm_chg,"pm_volume":pm_vol,"avg_daily_volume":avg_vol,"dollar_volume":adv_dollars}
    except Exception as exc:
        if errors is not None:
            errors.append(f"{ticker}: {str(exc)[:150]}")
        logger.warning("Ticker %s data unavailable: %s", ticker, exc)
        return None


def _score_prelim(m: Dict[str, Any], global_bias: float, sector_change: float, catalyst: float) -> Tuple[float,str,Dict[str,float]]:
    gap = float(m.get("gap_pct") or 0)
    pmc = float(m.get("pm_change_pct") or 0)
    pmv = float(m.get("pm_volume") or 0)
    av = max(1.0,float(m.get("avg_daily_volume") or 1))
    adv = float(m.get("dollar_volume") or 0)

    movement = min(22.0, abs(gap)*2.2 + abs(pmc)*2.5)
    # Premarket volume vs normal daily volume. 5% of ADV before open is meaningful for many liquid names.
    pm_share = pmv / av
    vol_score = min(20.0, pm_share * 220.0)
    liquidity = 15.0 if adv >= 1e9 else 12.0 if adv >= 2e8 else 8.0 if adv >= 5e7 else 3.0
    sector_score = min(10.0, abs(sector_change)*5.0)
    global_score = min(8.0, abs(global_bias))
    total = min(100.0, movement + vol_score + liquidity + sector_score + global_score + catalyst)

    directional = gap*1.0 + pmc*1.4 + sector_change*0.5 + global_bias*0.15
    bias = "LONG" if directional > 0.25 else "SHORT" if directional < -0.25 else "NEUTRAL"
    pieces = {"movement":movement,"volume":vol_score,"liquidity":liquidity,"sector":sector_score,"global":global_score,"catalyst":catalyst}
    return total,bias,pieces


def _update_scan_progress(**changes: Any) -> None:
    with _job_lock:
        _scan_job.update(changes)


def scan_state() -> Dict[str, Any]:
    with _job_lock:
        current = dict(_scan_job)
    with _db_lock:
        c = db_conn()
        recent = c.execute("SELECT * FROM scan_runs ORDER BY id DESC LIMIT 1").fetchone()
        c.close()
    if recent:
        recent = dict(recent)
        recent["errors"] = json.loads(recent.pop("errors_json") or "[]")
    return {"job": current, "last_run": recent,
            "storage": "disk path /var/data (verify Render persistent disk is attached)" if str(DB).startswith("/var/data/") else "ephemeral (not durable)",
            "market_feed": "Yahoo Finance public chart API (unofficial; may rate-limit or block hosted servers)"}


def run_overnight_scan() -> Dict[str, Any]:
    init_db()
    started = now_ct().isoformat()
    universe = [t for t in scan_universe() if t not in set(SECTOR_ETF.values()) | {"SPY", "QQQ", "IWM", "DIA"}]
    _update_scan_progress(state="running", started_at=started, finished_at=None,
                          total=len(universe), processed=0, scanned=0, error=None, errors=[])
    with _db_lock:
        c = db_conn()
        cur = c.execute("INSERT INTO scan_runs(started_at,status,universe_count) VALUES(?,?,?)",
                        (started, "running", len(universe)))
        run_id = cur.lastrowid
        c.commit(); c.close()
    errors: List[str] = []
    scanned = 0
    processed = 0
    try:
        # Independent quote requests are bounded; the HTTP API stays responsive.
        globals_ = _global_snapshot()
        gbias = _market_context(globals_)
        sector_moves: Dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
            work = {pool.submit(_daily, sym, "5d"): sec for sec, sym in SECTOR_ETF.items()}
            for future in as_completed(work):
                sec = work[future]
                try:
                    d = future.result()
                    sector_moves[sec] = (_pct(float(d.close.iloc[-1]), float(d.close.iloc[-2])) or 0.0) if len(d) >= 2 else 0.0
                except Exception as exc:
                    sector_moves[sec] = 0.0
                    if len(errors) < 20: errors.append(f"Sector {sec}: {str(exc)[:120]}")

        # This is still an unofficial upstream; avoid firing every request at once.
        # Collect the errors rather than returning an apparently successful empty list.
        results: List[Dict[str, Any]] = []
        def fetch_one(ticker: str):
            problems = []
            m = _ticker_metrics(ticker, problems)
            return ticker, m, problems
        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as pool:
            work = {pool.submit(fetch_one, ticker): ticker for ticker in universe}
            for future in as_completed(work):
                processed += 1
                try:
                    ticker, m, problems = future.result()
                    if problems and len(errors) < 20: errors.extend(problems[:max(0,20-len(errors))])
                    if m:
                        results.append(m)
                        scanned += 1
                except Exception as exc:
                    if len(errors) < 20: errors.append(f"{work[future]}: {str(exc)[:150]}")
                _update_scan_progress(processed=processed, scanned=scanned, errors=errors[-10:])

        if not results:
            msg = ("No current-day premarket quotes returned. The Yahoo feed may be blocked, "
                   "rate-limited, delayed, or the market may be closed. "
                   "Check scan details and Render logs; do not treat this as a no-trade signal.")
            raise RuntimeError(msg)

        rows = []
        for m in results:
            ticker = m["ticker"]
            sec = TICKER_SECTOR.get(ticker, "Unknown")
            sector_change = sector_moves.get(sec, 0.0)
            score, bias, pieces = _score_prelim(m, gbias, sector_change, 0.0)
            rows.append({**m,"sector":sec,"catalyst_score":0.0,
                         "catalyst_text":"No headline checked", "sec_forms":"",
                         "global_score":pieces["global"],"sector_score":pieces["sector"],
                         "liquidity_score":pieces["liquidity"],"movement_score":pieces["movement"],
                         "prelim_bias":bias,"prelim_score":round(score,2)})
        rows.sort(key=lambda x: x["prelim_score"],reverse=True)
        # Enrich only the most active names. The old version fetched SEC metadata
        # individually for dozens of tickers, making a single scan run for minutes.
        for r in rows[:min(10, len(rows))]:
            ticker = r["ticker"]
            hscore, headline = _headline_catalyst(ticker)
            # Only query SEC when a real contact user-agent has been configured.
            sec_score, forms = _sec_recent_forms(ticker) if "contact@example.com" not in SEC_USER_AGENT else (0.0, "")
            combined = min(25.0, hscore + sec_score)
            score, bias, pieces = _score_prelim(r, gbias, sector_moves.get(r["sector"],0.0), combined)
            r.update(catalyst_score=combined,catalyst_text=headline,sec_forms=forms,
                     prelim_score=round(score,2),prelim_bias=bias)
        rows.sort(key=lambda x: x["prelim_score"],reverse=True)
        ts = now_ct().isoformat()
        date = now_ct().date().isoformat()
        with _db_lock:
            c = db_conn()
            for r in rows:
                c.execute("""INSERT OR REPLACE INTO scans(scan_ts,scan_date,ticker,sector,price,prev_close,gap_pct,pm_change_pct,pm_volume,avg_daily_volume,dollar_volume,catalyst_score,catalyst_text,sec_forms,global_score,sector_score,liquidity_score,movement_score,prelim_bias,prelim_score)
                             VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                          (ts,date,r["ticker"],r["sector"],r["price"],r["prev_close"],r["gap_pct"],r["pm_change_pct"],r["pm_volume"],r["avg_daily_volume"],r["dollar_volume"],r["catalyst_score"],r["catalyst_text"],r["sec_forms"],r["global_score"],r["sector_score"],r["liquidity_score"],r["movement_score"],r["prelim_bias"],r["prelim_score"]))
            c.commit(); c.close()
        _update_scan_progress(state="completed", finished_at=now_ct().isoformat(),
                              scanned=len(rows), processed=processed, errors=errors[-10:])
        result = {"scan_ts":ts,"market_bias_score":round(gbias,2),"globals":globals_,
                  "candidates":rows[:15],"universe_count":len(universe),"scanned":len(rows),
                  "diagnostic_errors":errors[-10:]}
        with _db_lock:
            c=db_conn()
            c.execute("UPDATE scan_runs SET finished_at=?, status='completed', processed=?, scanned=?, errors_json=? WHERE id=?",
                      (now_ct().isoformat(), processed, len(rows),json.dumps(errors[-20:]),run_id))
            c.commit();c.close()
        logger.info("Morning Edge scan completed: %s/%s tickers; %s sample errors",len(rows),len(universe),len(errors))
        return result
    except Exception as exc:
        logger.exception("Morning Edge scan failed")
        _update_scan_progress(state="failed", finished_at=now_ct().isoformat(),
                              error=str(exc),processed=processed,scanned=scanned,errors=errors[-10:])
        with _db_lock:
            c=db_conn()
            c.execute("UPDATE scan_runs SET finished_at=?, status='failed', error=?, processed=?, scanned=?, errors_json=? WHERE id=?",
                      (now_ct().isoformat(),str(exc),processed,scanned,json.dumps(errors[-20:]),run_id))
            c.commit(); c.close()
        raise


def launch_scan(source: str = "manual") -> Dict[str,Any]:
    with _job_lock:
        if _scan_job["state"] == "running":
            return {"accepted":False,"message":"A scan is already running","job":dict(_scan_job)}
        _scan_job.update(state="running",started_at=now_ct().isoformat(),finished_at=None,
                         processed=0,scanned=0,error=None,errors=[])
    def work():
        try:
            run_overnight_scan()
        except Exception:
            logger.exception("%s scan ended with an error",source)
    threading.Thread(target=work,daemon=True,name=f"morning-edge-scan-{source}").start()
    return {"accepted":True,"message":"Scan started in background"}


def _vwap(df: pd.DataFrame) -> float:
    vol = df["volume"].fillna(0).astype(float)
    tp = (df["high"]+df["low"]+df["close"])/3.0
    if vol.sum() <= 0: return float(df["close"].iloc[-1])
    return float((tp*vol).sum()/vol.sum())


def _atr14(daily: pd.DataFrame) -> float:
    h,l,c = daily.high, daily.low, daily.close
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(),(h-pc).abs(),(l-pc).abs()],axis=1).max(axis=1)
    return float(tr.tail(14).mean())


def _market_levels(ticker: str, entry: float, side: str, opening: pd.DataFrame, premarket: pd.DataFrame, daily: pd.DataFrame) -> Tuple[float,float,float,str]:
    atr = max(0.01,_atr14(daily))
    buffer = max(0.01, min(atr*0.12, entry*0.006))
    vw = _vwap(opening)
    orh = float(opening.high.max()); orl = float(opening.low.min())
    pmh = float(premarket.high.max()) if not premarket.empty else float("nan")
    pml = float(premarket.low.min()) if not premarket.empty else float("nan")
    yhigh = float(daily.high.iloc[-2] if len(daily)>=2 else daily.high.iloc[-1])
    ylow = float(daily.low.iloc[-2] if len(daily)>=2 else daily.low.iloc[-1])

    if side == "LONG":
        supports = [x for x in [vw,orl,pml,ylow] if math.isfinite(x) and x < entry]
        support = max(supports) if supports else entry-atr*0.5
        stop = support-buffer
        risk = entry-stop
        resist = sorted([x for x in [pmh,yhigh] if math.isfinite(x) and x > entry])
        structural = resist[0] if resist else entry + max(2.0*risk, 0.65*atr)
        if structural-entry < MIN_RR*risk:
            target = entry + 2.0*risk
            reason = "nearest resistance too close; ATR/2R target used"
        else:
            target = structural; reason = "next market resistance"
    else:
        resistances = [x for x in [vw,orh,pmh,yhigh] if math.isfinite(x) and x > entry]
        resistance = min(resistances) if resistances else entry+atr*0.5
        stop = resistance+buffer
        risk = stop-entry
        supports = sorted([x for x in [pml,ylow] if math.isfinite(x) and x < entry], reverse=True)
        structural = supports[0] if supports else entry - max(2.0*risk,0.65*atr)
        if entry-structural < MIN_RR*risk:
            target = entry - 2.0*risk
            reason = "nearest support too close; ATR/2R target used"
        else:
            target = structural; reason = "next market support"
    rr = abs(target-entry)/max(1e-9,abs(entry-stop))
    return float(stop),float(target),float(rr),reason


def _confirm_ticker(scanrow: sqlite3.Row) -> Dict[str, Any]:
    ticker=scanrow["ticker"]
    intr=_yf_chart(ticker,"5d","1m",True).tz_convert(ET)
    daily=_daily(ticker,"3mo")
    today=now_ct().astimezone(ET).date()
    td=intr[intr.index.date==today]
    if td.empty:
        raise RuntimeError("No current-day data")
    pm=td.between_time("04:00","09:29")
    # The 09:45 ET candle is not complete at 08:45 CT. Avoid look-ahead.
    opening=td.between_time("09:30","09:44")
    if len(opening)<3:
        raise RuntimeError("Opening 15-minute data incomplete")
    entry=float(opening.close.iloc[-1]); vw=_vwap(opening); orh=float(opening.high.max()); orl=float(opening.low.min())
    first15vol=float(opening.volume.fillna(0).sum())
    # Baseline: average volume during first 15 minutes of prior available days.
    prior=[]
    for d in sorted(set(intr.index.date))[-6:-1]:
        w=intr[intr.index.date==d].between_time("09:30","09:44")
        if not w.empty: prior.append(float(w.volume.fillna(0).sum()))
    base=sum(prior)/len(prior) if prior else max(1.0,first15vol)
    rel=float(first15vol/max(1.0,base))
    pos=(entry-orl)/max(1e-9,orh-orl)
    above=entry>vw

    long_score=0.0; short_score=0.0; reasons=[]
    if above: long_score+=14; reasons.append("above VWAP")
    else: short_score+=14; reasons.append("below VWAP")
    if pos>=0.68: long_score+=14; reasons.append("upper opening range")
    elif pos<=0.32: short_score+=14; reasons.append("lower opening range")
    if rel>=1.5:
        if above: long_score+=12
        else: short_score+=12
        reasons.append(f"{rel:.1f}x opening volume")
    elif rel>=1.1:
        if above: long_score+=7
        else: short_score+=7
    # Strong reversal is allowed; overnight bias is context, not a veto.
    overnight=str(scanrow["prelim_bias"])
    if overnight=="LONG": long_score+=6
    elif overnight=="SHORT": short_score+=6
    # Opening breakout/breakdown confirmation using last 5 minutes.
    last5=opening.tail(5)
    if len(last5)>=3:
        if float(last5.close.iloc[-1])>float(last5.close.iloc[0]): long_score+=8
        elif float(last5.close.iloc[-1])<float(last5.close.iloc[0]): short_score+=8

    if max(long_score,short_score)<27 or abs(long_score-short_score)<7:
        return {"ticker":ticker,"decision":"PASS","score":round(max(long_score,short_score)+float(scanrow["prelim_score"])*0.25,1),"entry":entry,"opening_high":orh,"opening_low":orl,"vwap":vw,"first15_volume":first15vol,"rel_volume":rel,"reason":"; ".join(reasons)+"; confirmation not decisive"}
    side="LONG" if long_score>short_score else "SHORT"
    stop,target,rr,level_reason=_market_levels(ticker,entry,side,opening,pm,daily)
    risk_pct=abs(entry-stop)/entry*100
    if rr<MIN_RR or risk_pct>3.0 or risk_pct<0.15:
        return {"ticker":ticker,"decision":"PASS","score":round(max(long_score,short_score)+float(scanrow["prelim_score"])*0.25,1),"entry":entry,"opening_high":orh,"opening_low":orl,"vwap":vw,"first15_volume":first15vol,"rel_volume":rel,"reason":f"market structure failed risk test (RR {rr:.2f}, risk {risk_pct:.2f}%)"}
    total=min(100.0,float(scanrow["prelim_score"])*0.55+max(long_score,short_score)*0.9)
    return {"ticker":ticker,"decision":side,"score":round(total,1),"entry":entry,"stop":stop,"target":target,"rr":rr,"opening_high":orh,"opening_low":orl,"vwap":vw,"first15_volume":first15vol,"rel_volume":rel,"reason":"; ".join(reasons)+"; "+level_reason}


def confirm_opening_range() -> Dict[str, Any]:
    n = now_ct()
    if n.hour * 60 + n.minute < CONFIRM_HOUR_CT * 60 + CONFIRM_MINUTE_CT:
        raise RuntimeError("8:45 CT confirmation is not available before 8:45 AM Central")
    init_db(); d=n.date().isoformat()
    with _db_lock:
        c=db_conn()
        # latest scan row per ticker today
        scans=c.execute("""SELECT s.* FROM scans s JOIN (SELECT ticker,MAX(scan_ts) mx FROM scans WHERE scan_date=? GROUP BY ticker) x ON s.ticker=x.ticker AND s.scan_ts=x.mx WHERE s.scan_date=? ORDER BY s.prelim_score DESC LIMIT 20""",(d,d)).fetchall(); c.close()
    if not scans:
        raise RuntimeError("No overnight watchlist for today. Complete a successful overnight scan before confirmation.")
    results=[]
    for sr in scans:
        try: results.append((_confirm_ticker(sr),sr))
        except Exception as e: results.append(({"ticker":sr["ticker"],"decision":"PASS","score":0,"reason":str(e)},sr))
    actionable=[x for x in results if x[0]["decision"] in {"LONG","SHORT"}]
    actionable.sort(key=lambda x:x[0]["score"], reverse=True)
    allowed={x[0]["ticker"] for x in actionable[:MAX_SETUPS]}
    final=[]; ts=now_ct().isoformat()
    with _db_lock:
        c=db_conn()
        for r,sr in results:
            if r["decision"] in {"LONG","SHORT"} and r["ticker"] not in allowed:
                r={**r,"decision":"PASS","reason":r.get("reason","")+"; outside top setup limit"}
            entry=r.get("entry"); shares=(PAPER_DOLLARS/entry) if entry and r["decision"] in {"LONG","SHORT"} else None
            c.execute("""INSERT OR REPLACE INTO confirmations(confirm_ts,trade_date,ticker,overnight_bias,decision,score,entry,stop,target,rr,opening_high,opening_low,vwap,first15_volume,rel_volume,reason,paper_dollars,shares,status)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (ts,d,r["ticker"],sr["prelim_bias"],r["decision"],r.get("score"),entry,r.get("stop"),r.get("target"),r.get("rr"),r.get("opening_high"),r.get("opening_low"),r.get("vwap"),r.get("first15_volume"),r.get("rel_volume"),r.get("reason"),PAPER_DOLLARS,shares,"OPEN" if r["decision"] in {"LONG","SHORT"} else "PASS"))
            final.append(r)
        c.commit(); c.close()
    final.sort(key=lambda x:x.get("score",0), reverse=True)
    return {"confirm_ts":ts,"paper_dollars":PAPER_DOLLARS,"setups":[x for x in final if x["decision"] in {"LONG","SHORT"}],"passes":[x for x in final if x["decision"]=="PASS"]}


def settle_paper_trades() -> int:
    d=now_ct().date().isoformat(); settled=0
    with _db_lock:
        c=db_conn(); opens=c.execute("SELECT * FROM confirmations WHERE trade_date=? AND status='OPEN' AND decision IN ('LONG','SHORT')",(d,)).fetchall(); c.close()
    for tr in opens:
        try:
            intr=_yf_chart(tr["ticker"],"1d","1m",False).tz_convert(ET)
            today=now_ct().astimezone(ET).date(); df=intr[intr.index.date==today].between_time("09:46","16:00")
            if df.empty: continue
            exit_price=None; why=None
            for _,bar in df.iterrows():
                if tr["decision"]=="LONG":
                    if float(bar.low)<=float(tr["stop"]): exit_price=float(tr["stop"]); why="STOP"; break
                    if float(bar.high)>=float(tr["target"]): exit_price=float(tr["target"]); why="TARGET"; break
                else:
                    if float(bar.high)>=float(tr["stop"]): exit_price=float(tr["stop"]); why="STOP"; break
                    if float(bar.low)<=float(tr["target"]): exit_price=float(tr["target"]); why="TARGET"; break
            if exit_price is None and now_ct().time() >= datetime.strptime("15:05","%H:%M").time():
                exit_price=float(df.close.iloc[-1]); why="CLOSE"
            if exit_price is None: continue
            pnl=(exit_price-float(tr["entry"]))*float(tr["shares"])
            if tr["decision"]=="SHORT": pnl=-pnl
            with _db_lock:
                c=db_conn(); c.execute("UPDATE confirmations SET status='SETTLED',exit_price=?,exit_reason=?,pnl_dollars=?,settled_ts=? WHERE id=?",(exit_price,why,pnl,now_ct().isoformat(),tr["id"])); c.commit(); c.close()
            settled+=1
        except Exception:
            continue
    return settled


def latest_candidates() -> Dict[str,Any]:
    d=now_ct().date().isoformat()
    with _db_lock:
        c=db_conn()
        scan=[dict(r) for r in c.execute("""SELECT s.* FROM scans s JOIN (SELECT ticker,MAX(scan_ts) mx FROM scans WHERE scan_date=? GROUP BY ticker) x ON s.ticker=x.ticker AND s.scan_ts=x.mx WHERE s.scan_date=? ORDER BY s.prelim_score DESC LIMIT 15""",(d,d)).fetchall()]
        conf=[dict(r) for r in c.execute("SELECT * FROM confirmations WHERE trade_date=? ORDER BY score DESC",(d,)).fetchall()]
        glob=[dict(r) for r in c.execute("SELECT * FROM globals WHERE ts=(SELECT MAX(ts) FROM globals) ORDER BY id",()).fetchall()]
        c.close()
    return {"date":d,"overnight":scan,"confirmations":conf,"globals":glob}


def _worker() -> None:
    global _last_auto_bucket
    while True:
        try:
            n=now_ct()
            if n.weekday()<5:
                mins=n.hour*60+n.minute
                if 4*60 <= mins <= 8*60+25:
                    bucket=(n.date().isoformat(), mins // max(1,SCAN_INTERVAL_MIN))
                    if bucket != _last_auto_bucket:
                        with _job_lock:
                            running = _scan_job["state"] == "running"
                        if not running:
                            _last_auto_bucket = bucket
                            launch_scan("automatic")
                if CONFIRM_HOUR_CT*60+CONFIRM_MINUTE_CT <= mins <= 9*60+5:
                    with _db_lock:
                        c=db_conn()
                        cnt=c.execute("SELECT COUNT(*) FROM confirmations WHERE trade_date=?",(n.date().isoformat(),)).fetchone()[0]
                        latest=c.execute("SELECT COUNT(*) FROM scans WHERE scan_date=?",(n.date().isoformat(),)).fetchone()[0]
                        c.close()
                    if cnt==0 and latest:
                        try:confirm_opening_range()
                        except Exception:logger.exception("Automatic confirmation failed")
                if 8*60+46 <= mins <= 15*60+10:
                    settle_paper_trades()
        except Exception:
            logger.exception("Morning Edge scheduler failure")
        time.sleep(60)


def start_worker() -> None:
    global _worker_started
    if _worker_started: return
    _worker_started=True
    threading.Thread(target=_worker,daemon=True,name="morning-edge-worker").start()


app=FastAPI(title=APP_NAME)

@app.on_event("startup")
def startup():
    init_db()
    with _db_lock:
        c=db_conn()
        c.execute("UPDATE scan_runs SET status='interrupted',finished_at=?,error='Server restarted before scan completed' WHERE status='running'", (now_ct().isoformat(),))
        c.commit();c.close()
    start_worker()

@app.get("/")
def home(): return FileResponse(ROOT/"index.html")

@app.get("/api/status")
def status():
    data=latest_candidates(); n=now_ct()
    return {"ok":True,"app":APP_NAME,"now_ct":n.isoformat(),"paper_dollars":PAPER_DOLLARS,"min_rr":MIN_RR,"max_setups":MAX_SETUPS,"universe":len(scan_universe()),"news_enabled":bool(FINNHUB_API_KEY),"latest":data,"scanner":scan_state()}

@app.post("/api/run-scan", status_code=202)
def api_scan():
    return launch_scan("manual")

@app.get("/api/health")
def health():
    return {"ok":True,"app":APP_NAME,"scanner":scan_state()}

@app.post("/api/confirm")
def api_confirm():
    try:return confirm_opening_range()
    except Exception as e: raise HTTPException(500,str(e))

@app.post("/api/settle")
def api_settle(): return {"settled":settle_paper_trades()}

@app.get("/api/data")
def api_data(): return latest_candidates()

@app.get("/api/export.csv")
def export_csv():
    with _db_lock:
        c=db_conn(); cur=c.execute("SELECT * FROM confirmations ORDER BY trade_date DESC,score DESC"); names=[d[0] for d in cur.description]; rows=cur.fetchall(); c.close()
    out=io.StringIO(); w=csv.writer(out); w.writerow(names); w.writerows(rows)
    return StreamingResponse(iter([out.getvalue()]),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=morning-edge-paper-trades.csv"})
