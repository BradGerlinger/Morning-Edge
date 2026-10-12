"""Offline deterministic tests: forward-only fills and no possible live orders."""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import edge_engine as edge

ET=ZoneInfo('America/New_York')


def make_bars(symbol, last='2026-10-12T10:00:00-04:00'):
    idx=pd.date_range(pd.Timestamp('2026-10-12 09:30').tz_localize(ET),pd.Timestamp(last).tz_convert(ET),freq='1min').tz_convert('UTC')
    n=len(idx)
    df=pd.DataFrame({'open':[100.]*n,'high':[100.4]*n,'low':[99.6]*n,
                     'close':[100.0]*n,'volume':[1000.]*n},index=idx)
    # last regular candle breaks OR high with above-average volume.
    if symbol=='NVDA':
        df.iloc[-2,df.columns.get_loc('close')]=100.2
        for col,value in [('open',100.35),('high',101.1),('low',100.25),('close',101.0),('volume',5000)]:
            df.iloc[-1,df.columns.get_loc(col)]=value
    else:
        for i in range(15,n):
            df.iloc[i,df.columns.get_loc('close')]=100.1+(i-15)*.02
            df.iloc[i,df.columns.get_loc('high')]=100.45+(i-15)*.02
            df.iloc[i,df.columns.get_loc('low')]=99.7+(i-15)*.01
    return df


def fixture_bars():
    return {t:make_bars(t) for t in ['NVDA','QQQ']+edge.MAG7}


def setup_temp(tmp_path,monkeypatch):
    monkeypatch.setattr(edge,'DB',tmp_path/'test.sqlite')
    monkeypatch.setattr(edge,'now_utc',now)
    edge.init_db()
    monkeypatch.setenv('ALPACA_KEY_ID','DUMMY_TEST_KEY')
    monkeypatch.setenv('ALPACA_SECRET_KEY','DUMMY_TEST_SECRET')
    monkeypatch.setenv('EDGE_SYMBOLS','NVDA')


def now():return datetime(2026,10,12,14,1,15,tzinfo=timezone.utc)


def test_breakout_signal_requires_fresh_quote_and_breadth():
    bars=fixture_bars()
    a=edge.signal_for('NVDA',bars,now())
    assert a is not None and a['side']=='LONG' and a['target_price']>a['trigger_price']>a['stop_price']
    assert edge.signal_for('NVDA',bars,now()+timedelta(minutes=6)) is None
    bars.pop('QQQ')
    assert edge.signal_for('NVDA',bars,now()) is None


def test_forward_entry_then_stop_priority(tmp_path,monkeypatch):
    setup_temp(tmp_path,monkeypatch)
    bars=fixture_bars()
    edge.process(bars,now())
    trade=edge.summary()['today'][0]
    assert trade['status']=='PENDING' and trade['entry'] is None
    # Same signal candle's high MUST NOT retroactively fill the trade.
    assert trade['trigger_price']==101.
    # first bar *after* signal touches entry, without hitting exits
    later_time=pd.Timestamp('2026-10-12T10:01:00-04:00').tz_convert('UTC')
    new=pd.DataFrame([dict(open=101.,high=101.1,low=100.9,close=101.05,volume=2000.)],index=[later_time])
    bars['NVDA']=pd.concat([bars['NVDA'],new]); edge.process(bars,now()+timedelta(minutes=1))
    trade=edge.summary()['today'][0]
    assert trade['status']=='OPEN' and trade['entry']>101.
    # next minute hits both stop and target: count STOP (conservative OHLC assumption)
    t2=pd.Timestamp('2026-10-12T10:02:00-04:00').tz_convert('UTC')
    row=pd.DataFrame([dict(open=100.3,high=106.,low=98.,close=101.,volume=2200.)],index=[t2])
    bars['NVDA']=pd.concat([bars['NVDA'],row]);edge.process(bars,now()+timedelta(minutes=2))
    trade=edge.summary()['today'][0]
    assert trade['status']=='STOP' and trade['pnl']<0
    assert trade['exit']<=trade['stop_price']


def test_expired_if_no_next_bar_fill(tmp_path,monkeypatch):
    setup_temp(tmp_path,monkeypatch)
    bars=fixture_bars()
    edge.process(bars,now())
    for m in range(1,6):
        newtime=pd.Timestamp('2026-10-12T10:00:00-04:00').tz_convert('UTC')+timedelta(minutes=m)
        row=pd.DataFrame([dict(open=100.2,high=100.3,low=100.0,close=100.1,volume=1000.)],index=[newtime])
        bars['NVDA']=pd.concat([bars['NVDA'],row])
    edge.process(bars,now()+timedelta(minutes=5))
    trade=edge.summary()['today'][0]
    assert trade['status']=='EXPIRED' and trade['entry'] is None


def test_data_error_no_fabricated_trades(tmp_path,monkeypatch):
    setup_temp(tmp_path,monkeypatch)
    monkeypatch.setattr(edge,'now_utc',now)
    monkeypatch.setattr(edge,'fetch_bars',lambda symbol,time: pd.DataFrame())
    edge.poll()
    assert edge.LAST['status']=='data_error'
    assert edge.summary()['today']==[]


def test_server_route_readonly():
    import server
    paths=[(r.path,sorted(getattr(r,'methods',[]))) for r in server.app.routes]
    assert ('/api/edge',['GET']) in paths
    assert 'https://api.alpaca.markets' not in open(edge.__file__).read()
    assert 'https://paper-api.alpaca.markets' not in open(edge.__file__).read()
