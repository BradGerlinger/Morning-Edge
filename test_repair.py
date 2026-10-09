"""Offline tests for scan observability and safe handling of missing market quotes."""
from datetime import datetime, timedelta
import threading
import time

import pandas as pd
import pytest
import server
from fastapi.testclient import TestClient


def test_silent_yesterday_fallback_removed(monkeypatch):
    now = server.now_ct().astimezone(server.ET)
    old = (now - timedelta(days=1)).replace(hour=9, minute=30, second=0, microsecond=0)
    idx = pd.date_range(old, periods=15, freq="5min", tz=server.ET).tz_convert('UTC')
    intr = pd.DataFrame(dict(open=[100.] * 15, high=[101.] * 15, low=[99.] * 15, close=[100.] * 15, volume=[10.] * 15), index=idx)
    dts = pd.date_range((old - timedelta(days=35)).astimezone(server.CT).astimezone(__import__('datetime').timezone.utc), periods=30, freq="D")
    daily = pd.DataFrame(dict(open=[100.] * 30, high=[101.] * 30, low=[99.] * 30, close=[100.] * 30, volume=[1e6] * 30), index=dts)
    monkeypatch.setattr(server, "_yf_chart", lambda *args, **kwargs: intr)
    monkeypatch.setattr(server, "_daily", lambda *args, **kwargs: daily)
    issues = []
    assert server._ticker_metrics("AAPL", issues) is None
    assert "no current-day candles" in issues[0]


def test_scan_failure_persisted_and_exposed(tmp_path,monkeypatch):
    monkeypatch.setattr(server, "DB",tmp_path/'scan.db')
    monkeypatch.setattr(server, "scan_universe",lambda: ["AAPL", "MSFT"])
    monkeypatch.setattr(server, "_global_snapshot",lambda: [])
    monkeypatch.setattr(server, "_daily",lambda *args,**kwargs: pd.DataFrame())
    monkeypatch.setattr(server, "_ticker_metrics",lambda t,errors=None: None)
    monkeypatch.setattr(server, "_yf_chart",lambda *args,**kwargs: pd.DataFrame())
    with pytest.raises(RuntimeError,match="No current-day premarket quotes"):
        server.run_overnight_scan()
    state=server.scan_state()
    assert state['last_run']['status']=='failed'
    assert state['last_run']['scanned']==0
    assert state['last_run']['processed']==2
    assert "Yahoo" in state['last_run']['error']


def test_scan_success_populates_api(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "DB",tmp_path/'ok.db')
    monkeypatch.setattr(server,"scan_universe",lambda: ["AAPL"])
    monkeypatch.setattr(server,"_global_snapshot",lambda: [])
    monkeypatch.setattr(server,"_daily",lambda *args,**kwargs: pd.DataFrame())
    monkeypatch.setattr(server,"_yf_chart",lambda *args,**kwargs: pd.DataFrame())
    monkeypatch.setattr(server,"_ticker_metrics",lambda t,errors=None: {
        "ticker":t,"price":108.,"prev_close":100.,"gap_pct":6.,"pm_change_pct":2.,
        "pm_volume":1000000.,"avg_daily_volume":5000000.,"dollar_volume":1e9})
    monkeypatch.setattr(server,"_headline_catalyst",lambda t:(0.,"Not configured"))
    outcome=server.run_overnight_scan()
    assert outcome['scanned']==1
    monkeypatch.setattr(server,"start_worker",lambda:None)
    with TestClient(server.app) as client:
        d=client.get('/api/status').json()
        assert d['latest']['overnight'][0]['ticker']=='AAPL'
        assert d['scanner']['last_run']['status']=='completed'
        assert d['scanner']['last_run']['scanned']==1
        assert client.get('/api/health').status_code==200


def test_manual_scan_nonblocking(tmp_path,monkeypatch):
    monkeypatch.setattr(server, "DB", tmp_path/'async.db')
    server.init_db()
    start = threading.Event()
    finish = threading.Event()
    class FakeChild:
        pid = 1234
        def __init__(self, *args, **kwargs):
            start.set()
        def wait(self):
            assert finish.wait(3)
            return 0
    monkeypatch.setattr(server.subprocess, "Popen", FakeChild)
    monkeypatch.setattr(server, "start_worker", lambda: None)
    server._update_scan_progress(state='idle')
    with TestClient(server.app) as client:
        a = client.post('/api/run-scan')
        assert a.status_code == 202 and a.json()['accepted']
        assert start.wait(1)
        b = client.post('/api/run-scan')
        assert b.json()['accepted'] is False
        with server._db_lock:
            c=server.db_conn()
            c.execute("UPDATE scan_runs SET status='completed' WHERE id=?",(a.json()['run_id'],))
            c.commit();c.close()
        finish.set()
        for _ in range(100):
            if server._scan_job['state']=='completed':break
            time.sleep(.01)
        assert server.scan_state()['last_run']['status']=='completed'


def test_native_worker_crash_does_not_stop_api(tmp_path,monkeypatch):
    monkeypatch.setattr(server, 'DB', tmp_path/'crash.db')
    server.init_db()
    class FailingChild:
        pid=2222
        def __init__(self,*args,**kwargs): pass
        def wait(self): return -11
    monkeypatch.setattr(server.subprocess, 'Popen', FailingChild)
    monkeypatch.setattr(server,'start_worker',lambda:None)
    server._update_scan_progress(state='idle')
    with TestClient(server.app) as client:
        resp=client.post('/api/run-scan')
        assert resp.status_code==202
        for _ in range(150):
            if server.scan_state()['last_run']['status']=='failed':break
            time.sleep(.01)
        assert 'Signal 11' in server.scan_state()['last_run']['error']
        assert client.get('/api/status').status_code==200
