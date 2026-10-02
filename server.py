from __future__ import annotations

import csv
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

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

APP_NAME = "Morning Edge V1"
CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent
DB = Path(os.getenv("MORNING_EDGE_DB", str(ROOT / "morning_edge.db")))

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
_session.headers.update({"User-Agent": "Mozilla/5.0 MorningEdge/1.0"})
_db_lock = threading.Lock()
_worker_started = False


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
        """)
        c.commit(); c.close()


def _yf_chart(symbol: str, period: str = "5d", interval: str = "5m", prepost: bool = True) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": period, "interval": interval, "includePrePost": "true" if prepost else "false", "events": "div,splits"}
    r = _session.get(url, params=params, timeout=10)
    if not r.ok:
        raise RuntimeError(f"Yahoo {r.status_code} for {symbol}")
    data = r.json()["chart"]["result"]
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
    out = []
    ts = now_ct().isoformat()
    for label, sym in GLOBAL_SYMBOLS.items():
        try:
            d = _daily(sym, "5d")
            if len(d) < 2: continue
            p = float(d["close"].iloc[-1]); pc = float(d["close"].iloc[-2])
            out.append({"label":label,"symbol":sym,"price":p,"change_pct":_pct(p,pc)})
        except Exception:
            continue
    with _db_lock:
        c = db_conn()
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


def _ticker_metrics(ticker: str) -> Optional[Dict[str, Any]]:
    try:
        intr = _yf_chart(ticker, "5d", "5m", True)
        daily = _daily(ticker, "3mo")
        if intr.empty or len(daily) < 20:
            return None
        local = intr.tz_convert(ET)
        today = now_ct().astimezone(ET).date()
        tdf = local[local.index.date == today]
        if tdf.empty:
            # before Yahoo has today's bar, use latest available intraday day
            latest_date = local.index[-1].date(); tdf = local[local.index.date == latest_date]
        pm = tdf.between_time("04:00", "09:29")
        price = float((pm if not pm.empty else tdf)["close"].dropna().iloc[-1])
        prev_close = float(daily["close"].dropna().iloc[-2] if daily.index[-1].date() >= today else daily["close"].dropna().iloc[-1])
        pm_first = float(pm["open"].dropna().iloc[0]) if not pm.empty and not pm["open"].dropna().empty else price
        pm_vol = float(pm["volume"].fillna(0).sum()) if not pm.empty else 0.0
        avg_vol = float(daily["volume"].tail(20).mean())
        adv_dollars = float((daily["close"].tail(20) * daily["volume"].tail(20)).mean())
        gap = _pct(pm_first, prev_close) or 0.0
        pm_chg = _pct(price, pm_first) or 0.0
        return {"ticker":ticker,"price":price,"prev_close":prev_close,"gap_pct":gap,"pm_change_pct":pm_chg,"pm_volume":pm_vol,"avg_daily_volume":avg_vol,"dollar_volume":adv_dollars}
    except Exception:
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


def run_overnight_scan() -> Dict[str, Any]:
    init_db()
    globals_ = _global_snapshot()
    gbias = _market_context(globals_)
    # Sector moves from ETFs.
    sector_moves: Dict[str,float] = {}
    for sec,sym in SECTOR_ETF.items():
        try:
            d = _daily(sym,"5d")
            sector_moves[sec] = _pct(float(d.close.iloc[-1]), float(d.close.iloc[-2])) or 0.0
        except Exception:
            sector_moves[sec] = 0.0

    rows = []
    for ticker in scan_universe():
        if ticker in set(SECTOR_ETF.values()) or ticker in {"SPY","QQQ","IWM","DIA"}:
            continue
        m = _ticker_metrics(ticker)
        if not m: continue
        sec = TICKER_SECTOR.get(ticker, "Unknown")
        sec_chg = sector_moves.get(sec,0.0)
        # Fast first pass with price/volume. News is queried only for candidates that already move.
        provisional,_bias,_ = _score_prelim(m,gbias,sec_chg,0.0)
        catalyst = 0.0; catalyst_text = "No major headline found"; sec_forms=""
        if provisional >= 22 or abs(float(m.get("gap_pct") or 0)) >= 1.5:
            hscore, catalyst_text = _headline_catalyst(ticker)
            sscore, sec_forms = _sec_recent_forms(ticker)
            catalyst = min(25.0, hscore + sscore)
        score,bias,pieces = _score_prelim(m,gbias,sec_chg,catalyst)
        row = {**m,"sector":sec,"catalyst_score":catalyst,"catalyst_text":catalyst_text,"sec_forms":sec_forms,"global_score":pieces["global"],"sector_score":pieces["sector"],"liquidity_score":pieces["liquidity"],"movement_score":pieces["movement"],"prelim_bias":bias,"prelim_score":round(score,2)}
        rows.append(row)

    rows.sort(key=lambda x:x["prelim_score"], reverse=True)
    ts = now_ct().isoformat(); date = now_ct().date().isoformat()
    with _db_lock:
        c=db_conn()
        for r in rows:
            c.execute("""INSERT OR REPLACE INTO scans(scan_ts,scan_date,ticker,sector,price,prev_close,gap_pct,pm_change_pct,pm_volume,avg_daily_volume,dollar_volume,catalyst_score,catalyst_text,sec_forms,global_score,sector_score,liquidity_score,movement_score,prelim_bias,prelim_score)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (ts,date,r["ticker"],r["sector"],r["price"],r["prev_close"],r["gap_pct"],r["pm_change_pct"],r["pm_volume"],r["avg_daily_volume"],r["dollar_volume"],r["catalyst_score"],r["catalyst_text"],r["sec_forms"],r["global_score"],r["sector_score"],r["liquidity_score"],r["movement_score"],r["prelim_bias"],r["prelim_score"]))
        c.commit(); c.close()
    return {"scan_ts":ts,"market_bias_score":round(gbias,2),"globals":globals_,"candidates":rows[:15],"universe_count":len(scan_universe()),"scanned":len(rows)}


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
    opening=td.between_time("09:30","09:45")
    if len(opening)<3:
        raise RuntimeError("Opening 15-minute data incomplete")
    entry=float(opening.close.iloc[-1]); vw=_vwap(opening); orh=float(opening.high.max()); orl=float(opening.low.min())
    first15vol=float(opening.volume.fillna(0).sum())
    # Baseline: average volume during first 15 minutes of prior available days.
    prior=[]
    for d in sorted(set(intr.index.date))[-6:-1]:
        w=intr[intr.index.date==d].between_time("09:30","09:45")
        if not w.empty: prior.append(float(w.volume.fillna(0).sum()))
    base=np.mean(prior) if prior else max(1.0,first15vol)
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
    init_db(); d=now_ct().date().isoformat()
    with _db_lock:
        c=db_conn()
        # latest scan row per ticker today
        scans=c.execute("""SELECT s.* FROM scans s JOIN (SELECT ticker,MAX(scan_ts) mx FROM scans WHERE scan_date=? GROUP BY ticker) x ON s.ticker=x.ticker AND s.scan_ts=x.mx WHERE s.scan_date=? ORDER BY s.prelim_score DESC LIMIT 20""",(d,d)).fetchall(); c.close()
    if not scans:
        scan=run_overnight_scan()
        with _db_lock:
            c=db_conn(); scans=c.execute("""SELECT s.* FROM scans s JOIN (SELECT ticker,MAX(scan_ts) mx FROM scans WHERE scan_date=? GROUP BY ticker) x ON s.ticker=x.ticker AND s.scan_ts=x.mx WHERE s.scan_date=? ORDER BY s.prelim_score DESC LIMIT 20""",(d,d)).fetchall(); c.close()
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
    global _worker_started
    while True:
        try:
            n=now_ct(); weekday=n.weekday()<5
            if weekday:
                mins=n.hour*60+n.minute
                # overnight/premarket refresh 04:00-08:25 CT every interval
                if 4*60 <= mins <= 8*60+25 and n.minute % max(1,SCAN_INTERVAL_MIN) == 0:
                    run_overnight_scan()
                # opening confirmation at / after 8:45 if none exists
                if mins>=CONFIRM_HOUR_CT*60+CONFIRM_MINUTE_CT and mins<=9*60+5:
                    with _db_lock:
                        c=db_conn(); cnt=c.execute("SELECT COUNT(*) n FROM confirmations WHERE trade_date=?",(n.date().isoformat(),)).fetchone()[0]; c.close()
                    if cnt==0: confirm_opening_range()
                if 8*60+46 <= mins <= 15*60+10:
                    settle_paper_trades()
        except Exception:
            pass
        time.sleep(60)


def start_worker() -> None:
    global _worker_started
    if _worker_started: return
    _worker_started=True
    threading.Thread(target=_worker,daemon=True,name="morning-edge-worker").start()


app=FastAPI(title=APP_NAME)
app.mount("/assets",StaticFiles(directory=str(ROOT)),name="assets")

@app.on_event("startup")
def startup():
    init_db(); start_worker()

@app.get("/")
def home(): return FileResponse(ROOT/"index.html")

@app.get("/api/status")
def status():
    data=latest_candidates(); n=now_ct()
    return {"ok":True,"app":APP_NAME,"now_ct":n.isoformat(),"paper_dollars":PAPER_DOLLARS,"min_rr":MIN_RR,"max_setups":MAX_SETUPS,"universe":len(scan_universe()),"news_enabled":bool(FINNHUB_API_KEY),"latest":data}

@app.post("/api/run-scan")
def api_scan():
    try:return run_overnight_scan()
    except Exception as e: raise HTTPException(500,str(e))

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
