"""Morning Edge V1.3 — paper-only, forward-only, fail-closed intraday engine.

Not a broker. No live trading URL, credentials, or order-submission function.
This is a deliberately simple one-minute OHLC paper simulator that explicitly
models next-bar fills, stop-first ambiguity and estimated execution friction.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

LOG = logging.getLogger('morning-edge.edge')
CT = ZoneInfo('America/Chicago')
ET = ZoneInfo('America/New_York')
BASE = 'https://data.alpaca.markets'  # market data, NOT the order-execution API
MAG7 = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL', 'TSLA']
DEFAULT_SCAN = ['NVDA', 'TSLA', 'AAPL', 'MSFT', 'AMZN', 'META', 'GOOGL', 'AMD', 'QQQ']
PAPER_NOTIONAL = max(1.0, float(os.getenv('EDGE_PAPER_NOTIONAL', '100')))
MAX_TRADES_DAY = max(1, min(15, int(os.getenv('EDGE_MAX_TRADES_DAY', '5'))))
MAX_OPEN = max(1, min(5, int(os.getenv('EDGE_MAX_OPEN', '3'))))
DAILY_LOSS_CUTOFF = max(0.01, float(os.getenv('EDGE_DAILY_LOSS_CUTOFF', '10')))
SLIPPAGE_BPS = max(0.0, float(os.getenv('EDGE_SLIPPAGE_BPS', '5')))  # applies to every fill and exit
MIN_VOLUME_MULT = max(1.0, float(os.getenv('EDGE_MIN_VOLUME_MULT', '1.2')))
DATA_FEED = os.getenv('ALPACA_DATA_FEED', 'iex').lower()
if DATA_FEED not in ('iex', 'sip'):
    DATA_FEED = 'iex'
DB = Path(os.getenv('MORNING_EDGE_DB', '/var/data/morning_edge.db' if Path('/var/data').is_dir() else '/tmp/morning_edge.db'))
DB.parent.mkdir(parents=True, exist_ok=True)
LOCK = threading.RLock()
POLL_LOCK = threading.Lock()
LAST: Dict[str, Any] = {'status': 'not_started', 'last_poll': None, 'last_success': None, 'last_error': None, 'provider': 'Alpaca '+DATA_FEED.upper(), 'latest_bar': None, 'fetched': 0}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def credentials_ready() -> bool:
    return bool(os.getenv('ALPACA_KEY_ID') and os.getenv('ALPACA_SECRET_KEY'))


def scanner_symbols():
    raw = os.getenv('EDGE_SYMBOLS', ','.join(DEFAULT_SCAN))
    names = [x.strip().upper() for x in raw.split(',') if x.strip()]
    # MAG7 and QQQ always included for confirmation. Maximum 12 tradable symbols.
    trade = list(dict.fromkeys(names))[:12]
    context = list(dict.fromkeys(['QQQ']+MAG7+trade))
    return trade, context


def connect():
    c=sqlite3.connect(DB, timeout=20)
    c.row_factory=sqlite3.Row
    return c


def init_db():
    with LOCK:
        c=connect()
        c.executescript('''
        CREATE TABLE IF NOT EXISTS edge_trades (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          trade_date TEXT NOT NULL,
          ticker TEXT NOT NULL,
          side TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
          signal_bar_ts TEXT NOT NULL,
          signal_at TEXT NOT NULL,
          trigger_price REAL NOT NULL,
          stop_price REAL NOT NULL,
          target_price REAL NOT NULL,
          shares REAL NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('PENDING','OPEN','TARGET','STOP','TIME_EXIT','EXPIRED','STALE_REVIEW')),
          entry REAL,
          entry_bar_ts TEXT,
          exit REAL,
          exit_bar_ts TEXT,
          pnl REAL,
          rr REAL,
          reasoning TEXT,
          provider TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(trade_date,ticker)
        );
        CREATE INDEX IF NOT EXISTS edge_date_idx ON edge_trades (trade_date, status);
        ''')
        c.commit();c.close()


def _safe_float(x):
    z=float(x)
    if not math.isfinite(z):
        raise ValueError('non-finite price or volume')
    return z


def fetch_bars(symbol: str, now: datetime) -> pd.DataFrame:
    """Alpaca stock bars from today's 9:30 ET through last completed minute.

    Unlike Yahoo's unofficial endpoint, a failed provider call fails closed.
    We never use the current, potentially incomplete minute.
    """
    if not credentials_ready():
        raise RuntimeError('Alpaca market-data API keys are not configured')
    day=now.astimezone(ET).date()
    start=datetime(day.year,day.month,day.day,9,30,tzinfo=ET)
    if now.astimezone(ET) < start:
        return pd.DataFrame()
    url=f'{BASE}/v2/stocks/{symbol}/bars'
    headers={'APCA-API-KEY-ID':os.environ['ALPACA_KEY_ID'],'APCA-API-SECRET-KEY':os.environ['ALPACA_SECRET_KEY']}
    params={'start':start.isoformat(), 'end':now.astimezone(ET).isoformat(), 'timeframe':'1Min','adjustment':'raw','feed':DATA_FEED,'limit':1000}
    r=requests.get(url,headers=headers,params=params,timeout=12)
    if not r.ok:
        # never include the request headers or vendor response text that may contain account data
        raise RuntimeError(f'Alpaca {symbol}: HTTP {r.status_code} (check subscription and API credentials)')
    out=[]
    for b in r.json().get('bars', []):
        t=datetime.fromisoformat(b['t'].replace('Z','+00:00')).astimezone(timezone.utc)
        if t+timedelta(minutes=1) > now-timedelta(seconds=2):
            continue
        if t.astimezone(ET).date()!=day:
            continue
        out.append((t,_safe_float(b['o']),_safe_float(b['h']),_safe_float(b['l']),_safe_float(b['c']),_safe_float(b['v'])))
    if not out:
        return pd.DataFrame()
    f=pd.DataFrame(out,columns=['ts','open','high','low','close','volume']).set_index('ts').sort_index()
    if f.index.duplicated().any(): f=f[~f.index.duplicated(keep='last')]
    if (f[['open','high','low','close']]<=0).any().any() or (f['volume']<0).any():
        raise RuntimeError(f'Alpaca {symbol}: invalid price or volume')
    return f


def vwap(df):
    volume=df['volume'].clip(lower=0)
    if volume.sum()<=0: return float('nan')
    return float(((df['high']+df['low']+df['close'])/3*volume).sum()/volume.sum())


def _complete_and_fresh(df: pd.DataFrame, now: datetime):
    return (not df.empty and (now-df.index[-1].to_pydatetime()).total_seconds()<=180)


def signal_for(ticker: str, bars: Dict[str,pd.DataFrame], now: datetime) -> Optional[Dict[str,Any]]:
    """A new, confirmed post-opening-range crossing; never an earlier-close entry."""
    df=bars.get(ticker)
    if df is None or len(df)<31 or not _complete_and_fresh(df,now):return None
    et_idx=df.index.tz_convert(ET)
    day=now.astimezone(ET).date()
    mask=(et_idx.date==day)&(et_idx.hour==9)&(et_idx.minute>=30)&(et_idx.minute<=44)
    opening=df[mask]
    if len(opening)<12: return None
    last=df.iloc[-1];prev=df.iloc[-2]
    last_et=df.index[-1].astimezone(ET)
    if last_et.hour < 10: return None  # no false breakout at the same candle used to define range
    orh=float(opening.high.max());orl=float(opening.low.min());width=orh-orl
    if width<=0:return None
    price=float(last.close)
    above=price>orh*1.0003 and float(prev.close)<=orh*1.0003
    below=price<orl*0.9997 and float(prev.close)>=orl*0.9997
    if not (above or below):return None
    side='LONG' if above else 'SHORT'
    vw=vwap(df)
    if not math.isfinite(vw) or (side=='LONG' and price<=vw) or (side=='SHORT' and price>=vw):return None
    previous=df.iloc[-21:-1] if len(df)>=22 else df.iloc[:-1]
    if len(previous)<8: return None
    vol_base=float(previous.volume.mean())
    if vol_base<=0 or float(last.volume)<max(1.0,vol_base*MIN_VOLUME_MULT):return None
    # Broad Nasdaq confirmation: QQQ VWAP + short-term trend.
    qq=bars.get('QQQ')
    if qq is None or len(qq)<8 or not _complete_and_fresh(qq,now):return None
    qq_price=float(qq.close.iloc[-1]);qq_vw=vwap(qq)
    qq_slope=float(qq.close.iloc[-1]-qq.close.iloc[-6])
    if side=='LONG' and (qq_price<=qq_vw or qq_slope<=0):return None
    if side=='SHORT' and (qq_price>=qq_vw or qq_slope>=0):return None
    # Mag7 price relative to VWAP. Missing stocks must NOT count as supporting votes.
    votes=0
    for sym in MAG7:
        peer=bars.get(sym)
        if peer is None or len(peer)<8 or not _complete_and_fresh(peer,now):continue
        pv=vwap(peer)
        if side=='LONG' and float(peer.close.iloc[-1])>pv: votes+=1
        if side=='SHORT' and float(peer.close.iloc[-1])<pv: votes+=1
    if votes<4:return None
    low5=float(df.low.tail(5).min());high5=float(df.high.tail(5).max())
    if side=='LONG':
        stop=min(orl,low5)-max(.01,width*.08)
        risk=price-stop
        target=price+2*risk
    else:
        stop=max(orh,high5)+max(.01,width*.08)
        risk=stop-price
        target=price-2*risk
    risk_pct=risk/price*100
    if risk<=0 or risk_pct<0.15 or risk_pct>2.0 or target<=0: return None
    return {'ticker':ticker,'side':side,'signal_bar_ts':df.index[-1].isoformat(),
            'trigger_price':round(price,4),'stop_price':round(stop,4),
            'target_price':round(target,4),'shares':round(PAPER_NOTIONAL/price,7),
            'reasoning':f'Opening-range {"breakout" if above else "breakdown"}; VWAP; '+
                 f'{float(last.volume)/vol_base:.2f}x volume; QQQ confirmed; Mag7 breadth {votes}/7',
            'rr':2.0}


def _apply_slippage(price: float, buy: bool):
    return round(price*(1+(SLIPPAGE_BPS if buy else -SLIPPAGE_BPS)/10000),6)


def _signal_age_minutes(ts: str, now_ts):
    return (now_ts - datetime.fromisoformat(ts)).total_seconds()/60


def update_one(c, tr: sqlite3.Row, df: pd.DataFrame, now: datetime) -> None:
    if df.empty:return
    date=now.astimezone(CT).date().isoformat()
    if tr['trade_date']!=date:
        if tr['status'] in ('PENDING','OPEN'):
            c.execute("UPDATE edge_trades SET status='STALE_REVIEW',updated_at=? WHERE id=?",(now.isoformat(),tr['id']))
        return
    # Only evaluate bars AFTER the signal/fill. No backward-filled trades.
    start_ts=datetime.fromisoformat(tr['signal_bar_ts'])
    later=df[df.index>start_ts]
    if tr['status']=='PENDING':
        for ts,bar in later.iterrows():
            if _signal_age_minutes(tr['signal_bar_ts'],ts.to_pydatetime())>3:break
            entry=float(tr['trigger_price'])
            if tr['side']=='LONG' and float(bar.high)>=entry:
                entry=_apply_slippage(max(entry,float(bar.open)),True)
            elif tr['side']=='SHORT' and float(bar.low)<=entry:
                entry=_apply_slippage(min(entry,float(bar.open)),False)
            else:continue
            # reject gaps larger than half the structural risk; paper order would be canceled
            original_risk=abs(float(tr['trigger_price'])-float(tr['stop_price']))
            if abs(entry-float(tr['trigger_price']))>0.5*original_risk:break
            # The OHLC ordering within a minute is unknown. If the fill candle
            # also touches the stop, assume the adverse sequence (a stop-out).
            if (tr['side']=='LONG' and float(bar.low)<=float(tr['stop_price'])) or (tr['side']=='SHORT' and float(bar.high)>=float(tr['stop_price'])):
                stop=float(tr['stop_price'])
                exit_price=_apply_slippage(min(stop,float(bar.open)),False) if tr['side']=='LONG' else _apply_slippage(max(stop,float(bar.open)),True)
                pnl=(exit_price-entry)*float(tr['shares'])*(1 if tr['side']=='LONG' else -1)
                c.execute('''UPDATE edge_trades SET status='STOP',entry=?,entry_bar_ts=?,exit=?,exit_bar_ts=?,pnl=?,updated_at=? WHERE id=?''',
                          (entry,ts.isoformat(),exit_price,ts.isoformat(),round(pnl,5),now.isoformat(),tr['id']))
            else:
                c.execute('''UPDATE edge_trades SET status='OPEN',entry=?,entry_bar_ts=?,updated_at=? WHERE id=?''',
                          (entry,ts.isoformat(),now.isoformat(),tr['id']))
            return
        if _signal_age_minutes(tr['signal_bar_ts'],now)>=4:
            c.execute("UPDATE edge_trades SET status='EXPIRED',updated_at=? WHERE id=?",(now.isoformat(),tr['id']))
        return
    if tr['status']!='OPEN':return
    fill_ts=datetime.fromisoformat(tr['entry_bar_ts'])
    later=df[df.index>fill_ts]
    for ts,bar in later.iterrows():
        stop=float(tr['stop_price']);target=float(tr['target_price'])
        if tr['side']=='LONG':
            # A gap can fill below the intended stop; stop-first when both touched.
            stop_hit=float(bar.low)<=stop
            target_hit=float(bar.high)>=target
            if stop_hit: exit_price=_apply_slippage(min(stop,float(bar.open)),False); status='STOP'
            elif target_hit: exit_price=_apply_slippage(target,False);status='TARGET'
        else:
            stop_hit=float(bar.high)>=stop
            target_hit=float(bar.low)<=target
            if stop_hit: exit_price=_apply_slippage(max(stop,float(bar.open)),True);status='STOP'
            elif target_hit: exit_price=_apply_slippage(target,True);status='TARGET'
        if stop_hit or target_hit:
            pnl=(exit_price-float(tr['entry']))*float(tr['shares'])*(1 if tr['side']=='LONG' else -1)
            c.execute('''UPDATE edge_trades SET status=?,exit=?,exit_bar_ts=?,pnl=?,updated_at=? WHERE id=?''',
                      (status,exit_price,ts.isoformat(),round(pnl,5),now.isoformat(),tr['id']))
            return
    # Intraday positions don't silently stay open overnight. A shutdown/holiday may prevent time exit.
    nowct=now.astimezone(CT)
    if (nowct.hour,nowct.minute)>=(15,0) and len(later):
        last=later.iloc[-1];ts=later.index[-1]
        exit_price=_apply_slippage(float(last.close),tr['side']=='SHORT')
        pnl=(exit_price-float(tr['entry']))*float(tr['shares'])*(1 if tr['side']=='LONG' else -1)
        c.execute('''UPDATE edge_trades SET status='TIME_EXIT',exit=?,exit_bar_ts=?,pnl=?,updated_at=? WHERE id=?''',
                  (exit_price,ts.isoformat(),round(pnl,5),now.isoformat(),tr['id']))


def process(bars: Dict[str,pd.DataFrame], now: datetime):
    """One atomic simulation step. Call with market data fetched outside DB lock."""
    init_db()
    day=now.astimezone(CT).date().isoformat()
    trade_symbols,_=scanner_symbols()
    with LOCK:
        c=connect()
        try:
            # Mutate known trades first; never reuse the same historical signal.
            rows=c.execute("SELECT * FROM edge_trades WHERE status IN ('PENDING','OPEN')").fetchall()
            for r in rows:
                update_one(c,r,bars.get(r['ticker'],pd.DataFrame()),now)
            # No opening of a new trade until at least 9:00 CT, no entries after 10:30 CT.
            ct=now.astimezone(CT)
            active_start=(ct.hour,ct.minute)>=(9,0)
            active_end=(ct.hour,ct.minute)<(10,30)
            if ct.weekday()<5 and active_start and active_end:
                existing=c.execute("SELECT COUNT(*) FROM edge_trades WHERE trade_date=?",(day,)).fetchone()[0]
                live_count=c.execute("SELECT COUNT(*) FROM edge_trades WHERE trade_date=? AND status IN ('OPEN','PENDING')",(day,)).fetchone()[0]
                pnl=c.execute("SELECT COALESCE(SUM(pnl),0) FROM edge_trades WHERE trade_date=?",(day,)).fetchone()[0]
                if existing<MAX_TRADES_DAY and live_count<MAX_OPEN and pnl>-DAILY_LOSS_CUTOFF:
                    for sym in trade_symbols:
                        if existing>=MAX_TRADES_DAY or live_count>=MAX_OPEN:break
                        if c.execute("SELECT 1 FROM edge_trades WHERE trade_date=? AND ticker=?",(day,sym)).fetchone():continue
                        s=signal_for(sym,bars,now)
                        if not s:continue
                        # Signal must correspond to the latest closed bar, not stale prior session.
                        age=_signal_age_minutes(s['signal_bar_ts'],now)
                        if not (1<=age<=3):continue
                        c.execute('''INSERT INTO edge_trades(trade_date,ticker,side,signal_bar_ts,signal_at,trigger_price,stop_price,target_price,shares,status,rr,reasoning,provider,created_at,updated_at)
                         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                         (day,sym,s['side'],s['signal_bar_ts'],now.isoformat(),s['trigger_price'],s['stop_price'],s['target_price'],s['shares'],'PENDING',s['rr'],s['reasoning'],'Alpaca '+DATA_FEED.upper(),now.isoformat(),now.isoformat()))
                        existing+=1;live_count+=1
            c.commit()
        finally:c.close()


def poll():
    if not POLL_LOCK.acquire(blocking=False):
        return
    try:
        now=now_utc();LAST['last_poll']=now.isoformat()
        if not credentials_ready():
            LAST.update(status='setup_required',last_error='Add ALPACA_KEY_ID and ALPACA_SECRET_KEY in Render Environment',fetched=0)
            return
        # Skip provider requests outside RTH, but preserve last known status for dashboard.
        ct=now.astimezone(CT)
        if ct.weekday()>4 or not ((ct.hour,ct.minute)>=(8,30) and (ct.hour,ct.minute)<=(15,10)):
            LAST.update(status='market_closed',last_error=None)
            return
        syms,context=scanner_symbols()
        bars={};errors=[]
        with ThreadPoolExecutor(max_workers=4,thread_name_prefix='alpaca-data') as pool:
            futures={pool.submit(fetch_bars,sym,now):sym for sym in context}
            for future in as_completed(futures):
                sym=futures[future]
                try:
                    df=future.result()
                    if not df.empty and _complete_and_fresh(df,now):bars[sym]=df
                    else:errors.append(f'{sym}: no recent completed bars')
                except Exception as e:
                    errors.append(str(e))
        # Without QQQ and 4 Mag7 confirmations, no new trades can be created.
        if not bars or 'QQQ' not in bars or sum(x in bars for x in MAG7)<4:
            LAST.update(status='data_error',last_error=' | '.join(errors[:4]) or 'Insufficient fresh market data',fetched=len(bars))
            return
        process(bars,now)
        LAST.update(status='running',last_success=now.isoformat(),last_error=' | '.join(errors[:3]) if errors else None,
                    fetched=len(bars),latest_bar=max(d.index[-1].isoformat() for d in bars.values()))
    except Exception as e:
        LAST.update(status='error',last_error=str(e))
        LOG.exception('Edge simulation poll failed')
    finally:
        POLL_LOCK.release()


def summary():
    init_db()
    day=now_utc().astimezone(CT).date().isoformat()
    with LOCK:
        c=connect()
        rows=[dict(r) for r in c.execute('SELECT * FROM edge_trades WHERE trade_date=? ORDER BY id DESC',(day,)).fetchall()]
        all_rows=[dict(r) for r in c.execute('SELECT * FROM edge_trades ORDER BY id DESC LIMIT 30').fetchall()]
        # Last 30 trades can include prior sessions if market closed.
        c.close()
    settled=[x for x in rows if x['pnl'] is not None]
    return {'mode':'LOCAL_PAPER_ONLY','live_trading_enabled':False,'paper_order_route':'LOCAL_SIMULATOR',
      'provider':LAST.copy(),'keys_configured':credentials_ready(),'market_data_feed':DATA_FEED.upper(),
      'paper_notional':PAPER_NOTIONAL,'slippage_bps_each_side':SLIPPAGE_BPS,
      'max_trades_day':MAX_TRADES_DAY,'max_open':MAX_OPEN,'daily_loss_cutoff':DAILY_LOSS_CUTOFF,
      'tracked_symbols':scanner_symbols()[0],'recent_trades':all_rows,'today':rows,
      'completed_count':len(settled),'wins':sum(x['pnl']>0 for x in settled),
      'realized_pnl':round(sum(x['pnl'] for x in settled),2),'open_count':sum(x['status']=='OPEN' for x in rows),
      'pending_count':sum(x['status']=='PENDING' for x in rows),
      'now_ct':now_utc().astimezone(CT).isoformat(),
      'notes':['VIX/VXN is not yet integrated (no licensed volatility feed).',
               'One-minute OHLC simulated fills can differ materially from executable quotes.',
               'No broker orders are submitted; simulated positions are fractional where needed.']}
