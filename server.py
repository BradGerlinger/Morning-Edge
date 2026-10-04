import os
import time
import base64
import re
import csv
import io
import sqlite3
import threading
import json
import asyncio
from collections import defaultdict, deque
from datetime import datetime, timezone

import requests
import websockets
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

try:
    from pywebpush import webpush, WebPushException
except Exception:
    webpush = None
    WebPushException = Exception

APP_NAME = "15-Minute Edge Value V10.12 Lead-Lag Scalp Research"
KALSHI_BASE = "https://external-api.kalshi.com"
TRADE_ROOT = "/trade-api/v2"
SERIES = os.getenv("KALSHI_SERIES", "KXBTC15M")
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "").strip()
PRIVATE_KEY_PEM = os.getenv("KALSHI_PRIVATE_KEY_PEM", "").replace("\\n", "\n").strip()
PRIVATE_KEY_FILE = os.getenv("KALSHI_PRIVATE_KEY_FILE", "").strip()
DB = os.getenv("EDGE_DB", os.getenv("EDGE_DB_PATH", "/data/edge.db" if os.path.isdir("/data") else "edge-v10.db"))

VAPID_PUBLIC_KEY_ENV = os.getenv("VAPID_PUBLIC_KEY", "").strip()
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "").replace("\\n", "\n").strip()
VAPID_EMAIL = os.getenv(
    "VAPID_EMAIL",
    os.getenv("VAPID_CLAIMS_EMAIL", "mailto:edge@example.com"),
).strip()
if VAPID_EMAIL and not VAPID_EMAIL.startswith("mailto:"):
    VAPID_EMAIL = "mailto:" + VAPID_EMAIL

# API pacing. These values keep the app responsive while preventing the old
# request storm that caused Kalshi HTTP 429 errors.
BRTI_INTERVAL = float(os.getenv("EDGE_BRTI_INTERVAL", "5"))
MARKET_INTERVAL = float(os.getenv("EDGE_MARKET_INTERVAL", "10"))
SETTLEMENT_INTERVAL = float(os.getenv("EDGE_SETTLEMENT_INTERVAL", "60"))
SETTLEMENT_BATCH = int(os.getenv("EDGE_SETTLEMENT_BATCH", "2"))
KALSHI_MIN_GAP = float(os.getenv("EDGE_KALSHI_MIN_GAP", "1.25"))
DEFAULT_429_BACKOFF = float(os.getenv("EDGE_429_BACKOFF", "15"))

# V10.10 research collector. BTC Value logic above is unchanged.
RESEARCH_ASSETS = [x.strip().upper() for x in os.getenv(
    "EDGE_RESEARCH_ASSETS", "BTC,ETH,SOL,XRP,DOGE,BNB,HYPE,NEAR,ZEC"
).split(",") if x.strip()]
RESEARCH_SERIES = {
    "BTC":"KXBTC15M", "ETH":"KXETH15M", "SOL":"KXSOL15M",
    "XRP":"KXXRP15M", "DOGE":"KXDOGE15M", "BNB":"KXBNB15M",
    "HYPE":"KXHYPE15M", "NEAR":"KXNEAR15M", "ZEC":"KXZEC15M",
}
SPOT_INTERVAL = float(os.getenv("EDGE_RESEARCH_SPOT_INTERVAL", "5"))
RESEARCH_MARKET_INTERVAL = float(os.getenv("EDGE_RESEARCH_MARKET_INTERVAL", "4"))
RESEARCH_KEEP_SECONDS = int(os.getenv("EDGE_RESEARCH_KEEP_SECONDS", "21600"))
BTC_IMPULSE_30S = float(os.getenv("EDGE_BTC_IMPULSE_30S", "0.0010"))
FOLLOWER_MAX_MOVE_30S = float(os.getenv("EDGE_FOLLOWER_MAX_MOVE_30S", "0.00055"))
PAPER_MIN_ASK = float(os.getenv("EDGE_PAPER_MIN_ASK", "0.20"))
PAPER_MAX_ASK = float(os.getenv("EDGE_PAPER_MAX_ASK", "0.60"))

# V10.12 lead/lag scalp research.  This is observation + paper analysis only.
# It never submits an order.  Quotes are derived from the live Kalshi order book:
# buy-side entry uses the displayed ask; a hypothetical exit uses the executable bid.
LEADLAG_ENABLED = os.getenv("EDGE_LEADLAG_ENABLED", "1").strip().lower() not in ("0","false","no","off")
KALSHI_WS_URL = os.getenv("KALSHI_WS_URL", "wss://external-api-ws.kalshi.com/trade-api/ws/v2")
KALSHI_WS_PATH = "/trade-api/ws/v2"
LEADLAG_LOOKBACK = float(os.getenv("EDGE_LEADLAG_LOOKBACK", "3"))
LEADLAG_TRIGGER_CENTS = float(os.getenv("EDGE_LEADLAG_TRIGGER_CENTS", "8"))
LEADLAG_FOLLOWER_MAX_CENTS = float(os.getenv("EDGE_LEADLAG_FOLLOWER_MAX_CENTS", "3"))
LEADLAG_EVENT_COOLDOWN = float(os.getenv("EDGE_LEADLAG_EVENT_COOLDOWN", "8"))
LEADLAG_CHECKPOINTS = tuple(int(x.strip()) for x in os.getenv("EDGE_LEADLAG_CHECKPOINTS", "1,3,5,10,15,20,30,45,60,90,120").split(",") if x.strip())
LEADLAG_MAX_HOLD = float(os.getenv("EDGE_LEADLAG_MAX_HOLD", str(max(LEADLAG_CHECKPOINTS or (120,)))))
LEADLAG_KEEP_HOURS = float(os.getenv("EDGE_LEADLAG_KEEP_HOURS", "336"))
LEADLAG_ENTRY_MIN = float(os.getenv("EDGE_LEADLAG_ENTRY_MIN", "0.05"))
LEADLAG_ENTRY_MAX = float(os.getenv("EDGE_LEADLAG_ENTRY_MAX", "0.90"))

_session = requests.Session()
_key = None

_kalshi_gate = threading.Lock()
_next_kalshi_request_at = 0.0
_backoff_until = 0.0

_collector_guard = threading.Lock()
_collector_started = False
_leadlag_guard = threading.Lock()
_leadlag_started = False
_leadlag_db_lock = threading.Lock()
_leadlag_books = {}
_leadlag_last_top = {}
_leadlag_hist = defaultdict(lambda: deque(maxlen=1200))
_leadlag_last_candidate = {}
_leadlag_last_purge = 0.0

state = {
    "market": None,
    "hist": [],
    "last_call": None,
    "error": None,
    "push_error": None,
    "last_sample_at": None,
    "last_market_at": None,
    "last_429_at": None,
    "collector": "STARTING",
    "research": {"spot": {}, "markets": {}, "last_spot_at": None, "last_market_at": None, "error": None},
    "leadlag": {
        "enabled": LEADLAG_ENABLED, "connected": False, "status": "STARTING",
        "subscribed": [], "last_message_at": None, "last_quote_at": None,
        "reconnects": 0, "error": None, "tops": {},
    },
}


def _b64u_dec(s):
    return base64.urlsafe_b64decode(s + "=" * ((4 - len(s) % 4) % 4))


def _b64u_enc(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def derive_vapid_public(private_value):
    try:
        from cryptography.hazmat.primitives.asymmetric import ec

        raw = private_value.strip()
        if raw.startswith("-----BEGIN"):
            key = serialization.load_pem_private_key(raw.encode(), password=None)
        else:
            d = int.from_bytes(_b64u_dec(raw), "big")
            key = ec.derive_private_key(d, ec.SECP256R1())
        nums = key.public_key().public_numbers()
        return _b64u_enc(
            b"\x04" + nums.x.to_bytes(32, "big") + nums.y.to_bytes(32, "big")
        )
    except Exception:
        return ""


VAPID_DERIVED_PUBLIC_KEY = (
    derive_vapid_public(VAPID_PRIVATE_KEY) if VAPID_PRIVATE_KEY else ""
)
VAPID_PUBLIC_KEY = VAPID_DERIVED_PUBLIC_KEY or VAPID_PUBLIC_KEY_ENV
VAPID_KEY_MATCH = (
    (not VAPID_PUBLIC_KEY_ENV)
    or (not VAPID_DERIVED_PUBLIC_KEY)
    or VAPID_PUBLIC_KEY_ENV == VAPID_DERIVED_PUBLIC_KEY
)

app = FastAPI(title=APP_NAME)
app.mount("/assets", StaticFiles(directory="."), name="assets")


def get_key():
    global _key
    if _key:
        return _key
    raw = (
        PRIVATE_KEY_PEM.encode()
        if PRIVATE_KEY_PEM
        else (open(PRIVATE_KEY_FILE, "rb").read() if PRIVATE_KEY_FILE else None)
    )
    if not raw:
        raise RuntimeError("Kalshi private key is not configured")
    _key = serialization.load_pem_private_key(raw, password=None)
    return _key


def auth_headers(method, path):
    ts = str(int(time.time() * 1000))
    msg = f"{ts}{method.upper()}{path.split('?')[0]}".encode()
    sig = get_key().sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


def _retry_after_seconds(response):
    raw = response.headers.get("Retry-After", "").strip()
    try:
        return max(1.0, min(120.0, float(raw)))
    except Exception:
        return DEFAULT_429_BACKOFF


def kget(path, params=None, auth=False, timeout=8):
    """
    All Kalshi GETs pass through one pacing gate.
    - Prevents multiple loops from hammering Kalshi at the same instant.
    - Enforces a minimum gap between requests.
    - Honors 429 Retry-After/backoff instead of immediately retrying.
    """
    global _next_kalshi_request_at, _backoff_until

    with _kalshi_gate:
        now = time.monotonic()
        wait_for = max(_next_kalshi_request_at, _backoff_until) - now
        if wait_for > 0:
            time.sleep(wait_for)

        started = time.monotonic()
        r = _session.get(
            KALSHI_BASE + path,
            params=params,
            headers=auth_headers("GET", path) if auth else {},
            timeout=timeout,
        )
        _next_kalshi_request_at = max(
            _next_kalshi_request_at, started + KALSHI_MIN_GAP
        )

        if r.status_code == 429:
            retry_after = _retry_after_seconds(r)
            _backoff_until = time.monotonic() + retry_after
            state["last_429_at"] = datetime.now(timezone.utc).isoformat()
            raise HTTPException(
                429, f"Kalshi rate limit; backing off {retry_after:.0f}s"
            )

        if not r.ok:
            raise HTTPException(r.status_code, r.text[:500])

        return r.json()


def first_number(obj):
    out = []

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                if k in ("value", "price", "indexValue", "index_value", "rate"):
                    try:
                        n = float(str(v).replace(",", ""))
                        if 1000 < n < 10000000:
                            out.append(n)
                    except Exception:
                        pass
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    return out[0] if out else None


def parse_target(m):
    for k in ("floor_strike", "cap_strike", "functional_strike"):
        try:
            v = float(m.get(k))
            if v > 1000:
                return v
        except Exception:
            pass

    text = " ".join(
        str(m.get(k, ""))
        for k in ("title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary")
    )
    vals = re.findall(r"\$?\s*([0-9]{2,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)", text)
    return float(vals[0].replace(",", "")) if vals else None


def ask_price(m, side):
    for k, scale in (
        (("yes_ask_dollars" if side == "UP" else "no_ask_dollars"), 1),
        (("yes_ask" if side == "UP" else "no_ask"), 100),
    ):
        try:
            v = float(m.get(k)) / scale
            if 0 <= v <= 1:
                return v
        except Exception:
            pass
    return None


def get_market():
    d = kget(
        f"{TRADE_ROOT}/markets",
        {"series_ticker": SERIES, "status": "open"},
    )
    now = datetime.now(timezone.utc)

    def et(m):
        for k in ("close_time", "expiration_time", "expected_expiration_time"):
            if m.get(k):
                try:
                    return datetime.fromisoformat(m[k].replace("Z", "+00:00"))
                except Exception:
                    pass
        return datetime.max.replace(tzinfo=timezone.utc)

    ms = [m for m in d.get("markets", []) if et(m) > now]
    ms.sort(key=et)
    if not ms:
        return None

    m = ms[0]
    imb = 0
    try:
        ob = kget(f"{TRADE_ROOT}/markets/{m['ticker']}/orderbook")
        b = ob.get("orderbook_fp") or ob.get("orderbook") or {}
        yes = b.get("yes_dollars") or b.get("yes") or []
        no = b.get("no_dollars") or b.get("no") or []

        def q(a):
            return sum(float(x[1]) for x in a[:5])

        y, n = q(yes), q(no)
        imb = (y - n) / (y + n) if y + n else 0
    except HTTPException as e:
        # A temporary order-book failure should not throw away the whole market.
        if e.status_code == 429:
            raise
    except Exception:
        pass

    return {
        "ticker": m.get("ticker"),
        "title": m.get("title"),
        "target": parse_target(m),
        "close_time": m.get("close_time") or m.get("expiration_time"),
        "up_ask": ask_price(m, "UP"),
        "down_ask": ask_price(m, "DOWN"),
        "book_imbalance": imb,
    }


def get_series_market(series):
    """Lightweight current-market lookup for research; no orderbook request."""
    d = kget(f"{TRADE_ROOT}/markets", {"series_ticker": series, "status": "open"})
    now = datetime.now(timezone.utc)
    def et(m):
        for k in ("close_time", "expiration_time", "expected_expiration_time"):
            if m.get(k):
                try: return datetime.fromisoformat(m[k].replace("Z", "+00:00"))
                except Exception: pass
        return datetime.max.replace(tzinfo=timezone.utc)
    ms = [m for m in d.get("markets", []) if et(m) > now]
    ms.sort(key=et)
    if not ms: return None
    m = ms[0]
    return {"ticker":m.get("ticker"), "title":m.get("title"), "target":parse_target(m),
            "close_time":m.get("close_time") or m.get("expiration_time"),
            "up_ask":ask_price(m,"UP"), "down_ask":ask_price(m,"DOWN")}


def coinbase_spot_snapshot():
    """One public request returns USD exchange rates for all research assets."""
    r = _session.get("https://api.coinbase.com/v2/exchange-rates", params={"currency":"USD"}, timeout=8)
    if not r.ok: raise RuntimeError(f"Coinbase spot HTTP {r.status_code}")
    rates = ((r.json().get("data") or {}).get("rates") or {})
    out = {}
    for asset in RESEARCH_ASSETS:
        try:
            rate = float(rates.get(asset))
            if rate > 0: out[asset] = 1.0 / rate
        except Exception: pass
    return out


def research_return(asset, seconds, now=None):
    now = now or time.time()
    c = sqlite3.connect(DB)
    row = c.execute("SELECT price FROM crypto_spot WHERE asset=? AND ts<=? ORDER BY ts DESC LIMIT 1", (asset, now-seconds)).fetchone()
    last = c.execute("SELECT price FROM crypto_spot WHERE asset=? ORDER BY ts DESC LIMIT 1", (asset,)).fetchone()
    c.close()
    if not row or not last or not row[0]: return None
    return float(last[0])/float(row[0])-1.0


def save_spot_snapshot(ts, prices):
    c=sqlite3.connect(DB)
    c.executemany("INSERT OR REPLACE INTO crypto_spot(ts,asset,price) VALUES(?,?,?)", [(ts,a,p) for a,p in prices.items()])
    c.execute("DELETE FROM crypto_spot WHERE ts<?", (ts-RESEARCH_KEEP_SECONDS,))
    c.commit(); c.close()


def save_research_market(ts, asset, m):
    if not m: return
    c=sqlite3.connect(DB)
    c.execute("""INSERT INTO crypto_market_snapshots(ts,asset,series,ticker,seconds,target,up_ask,down_ask)
                 VALUES(?,?,?,?,?,?,?,?)""", (ts,asset,RESEARCH_SERIES.get(asset),m.get("ticker"),left(m),m.get("target"),m.get("up_ask"),m.get("down_ask")))
    c.execute("DELETE FROM crypto_market_snapshots WHERE ts<?", (ts-RESEARCH_KEEP_SECONDS,))
    c.commit(); c.close()


def maybe_paper_followers(ts):
    """Research-only BTC->follower lag entries. Never sends an order."""
    btc30=research_return("BTC",30,ts)
    if btc30 is None or abs(btc30) < BTC_IMPULSE_30S: return
    side="UP" if btc30>0 else "DOWN"
    c=sqlite3.connect(DB)
    for asset in RESEARCH_ASSETS:
        if asset=="BTC": continue
        f30=research_return(asset,30,ts)
        if f30 is None or abs(f30) > FOLLOWER_MAX_MOVE_30S: continue
        m=(state.get("research") or {}).get("markets",{}).get(asset) or {}
        ask=m.get("up_ask") if side=="UP" else m.get("down_ask")
        ticker=m.get("ticker")
        sec=left(m) if m else None
        if not ticker or ask is None or not (PAPER_MIN_ASK <= ask <= PAPER_MAX_ASK): continue
        # One paper entry per ticker/leader/asset/side.
        c.execute("""INSERT OR IGNORE INTO crypto_paper_trades(
            ts,leader,asset,ticker,side,ask,seconds,leader_move_30s,follower_move_30s,status)
            VALUES(?,?,?,?,?,?,?,?,?,?)""", (ts,"BTC",asset,ticker,side,ask,sec,btc30,f30,"PENDING"))
    c.commit(); c.close()


def settle_research_paper(limit=2):
    c=sqlite3.connect(DB)
    rows=c.execute("SELECT id,ticker,side,ask FROM crypto_paper_trades WHERE status='PENDING' ORDER BY id LIMIT ?",(limit,)).fetchall()
    c.close()
    for rid,ticker,side,ask in rows:
        try:
            d=kget(f"{TRADE_ROOT}/markets/{ticker}")
            m=d.get("market") or {}; kr=str(m.get("result") or "").lower()
            if kr not in ("yes","no"): continue
            outcome="UP" if kr=="yes" else "DOWN"; result="WIN" if side==outcome else "LOSS"
            pnl=10*((1/float(ask))-1) if result=="WIN" and ask else -10.0
            c=sqlite3.connect(DB); c.execute("UPDATE crypto_paper_trades SET outcome=?,result=?,pnl=?,status='SETTLED',settled_ts=? WHERE id=?",(outcome,result,pnl,datetime.now(timezone.utc).isoformat(),rid)); c.commit(); c.close()
        except HTTPException as e:
            if e.status_code==429: break
        except Exception: pass




def _ll_price(v):
    try:
        x = float(v)
        if x > 1.0001:
            x /= 100.0
        return x if 0 <= x <= 1 else None
    except Exception:
        return None


def _ll_qty(v):
    try:
        return max(0.0, float(v))
    except Exception:
        return 0.0


def _ll_exchange_ts_ms(msg, fallback_ms):
    try:
        if msg.get("ts_ms") is not None:
            return int(msg.get("ts_ms"))
    except Exception:
        pass
    raw = msg.get("ts")
    if raw:
        try:
            return int(datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            pass
    return fallback_ms


def _leadlag_desired_tickers():
    out = {}
    markets = (state.get("research") or {}).get("markets") or {}
    for asset in RESEARCH_ASSETS:
        m = markets.get(asset) or {}
        ticker = m.get("ticker")
        if ticker:
            out[str(ticker)] = asset
    return out


def _leadlag_seconds(asset):
    try:
        m = ((state.get("research") or {}).get("markets") or {}).get(asset) or {}
        return left(m)
    except Exception:
        return None


def _leadlag_book_top(book):
    yes = book.get("yes") or {}
    no = book.get("no") or {}
    yes_live = [(p, q) for p, q in yes.items() if q > 0]
    no_live = [(p, q) for p, q in no.items() if q > 0]
    yes_bid = max((p for p, _ in yes_live), default=None)
    no_bid = max((p for p, _ in no_live), default=None)
    if yes_bid is None or no_bid is None:
        return None
    yes_qty = yes.get(yes_bid, 0.0)
    no_qty = no.get(no_bid, 0.0)
    up_ask = 1.0 - no_bid
    down_ask = 1.0 - yes_bid
    if up_ask < yes_bid or down_ask < no_bid:
        # A transient crossed book is not safe enough for executable scalp research.
        return None
    return {
        "up_bid": yes_bid,
        "up_ask": up_ask,
        "down_bid": no_bid,
        "down_ask": down_ask,
        "up_bid_size": yes_qty,
        "down_bid_size": no_qty,
        "up_mid": (yes_bid + up_ask) / 2.0,
    }


def _leadlag_history_base(asset, now_s, lookback_s):
    h = _leadlag_hist.get(asset)
    if not h:
        return None
    cutoff = now_s - lookback_s
    base = None
    for ts, mid, ticker in h:
        if ts <= cutoff:
            base = (ts, mid, ticker)
        else:
            break
    return base


def _leadlag_checkpoint_bid(c, asset, side, due_ts_ms):
    """Return the last executable bid available at or before a checkpoint."""
    col = "up_bid" if side == "UP" else "down_bid"
    row = c.execute(
        f"""SELECT recv_ts_ms,{col} AS bid
            FROM leadlag_quotes
            WHERE asset=? AND recv_ts_ms<=? AND {col} IS NOT NULL
            ORDER BY recv_ts_ms DESC LIMIT 1""",
        (asset, int(due_ts_ms)),
    ).fetchone()
    if not row or row["bid"] is None:
        return None, None
    return int(row["recv_ts_ms"]), float(row["bid"])


def _leadlag_update_open_events(asset, now_ms, top):
    if not top:
        return
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT id,created_ts_ms,side,entry_ask,expires_ts_ms,max_exit_bid,
                      hit_2c_ts_ms,hit_5c_ts_ms,hit_10c_ts_ms
               FROM leadlag_events WHERE status='OPEN' AND follower_asset=?""",
            (asset,),
        ).fetchall()
        for r in rows:
            rid = int(r["id"])
            side = r["side"]
            entry = float(r["entry_ask"] or 0)
            expires = int(r["expires_ts_ms"] or 0)

            bid = top.get("up_bid") if side == "UP" else top.get("down_bid")
            if bid is not None:
                old_max = r["max_exit_bid"]
                best = max(float(old_max) if old_max is not None else -1.0, float(bid))
                best_ts = now_ms if old_max is None or float(bid) > float(old_max) else None
                pnl10 = 10.0 * (best / entry - 1.0) if entry > 0 else None
                gain = float(bid) - entry
                h2 = r["hit_2c_ts_ms"] or (now_ms if gain >= 0.02 else None)
                h5 = r["hit_5c_ts_ms"] or (now_ms if gain >= 0.05 else None)
                h10 = r["hit_10c_ts_ms"] or (now_ms if gain >= 0.10 else None)
                if best_ts is not None:
                    c.execute(
                        """UPDATE leadlag_events SET max_exit_bid=?,max_exit_ts_ms=?,max_gross_pnl_10=?,
                           hit_2c_ts_ms=?,hit_5c_ts_ms=?,hit_10c_ts_ms=?,last_update_ts_ms=? WHERE id=?""",
                        (best, best_ts, pnl10, h2, h5, h10, now_ms, rid),
                    )
                else:
                    c.execute(
                        """UPDATE leadlag_events SET hit_2c_ts_ms=?,hit_5c_ts_ms=?,hit_10c_ts_ms=?,
                           last_update_ts_ms=? WHERE id=?""",
                        (h2, h5, h10, now_ms, rid),
                    )

            cps = c.execute(
                """SELECT checkpoint_seconds,due_ts_ms
                   FROM leadlag_checkpoints
                   WHERE event_id=? AND exit_bid IS NULL AND due_ts_ms<=?
                   ORDER BY checkpoint_seconds""",
                (rid, int(now_ms)),
            ).fetchall()
            for cp in cps:
                qts, cp_bid = _leadlag_checkpoint_bid(c, asset, side, int(cp["due_ts_ms"]))
                if cp_bid is None:
                    continue
                cp_pnl = 10.0 * (cp_bid / entry - 1.0) if entry > 0 else None
                c.execute(
                    """UPDATE leadlag_checkpoints
                       SET quote_ts_ms=?,exit_bid=?,gross_pnl_10=?,recorded_ts_ms=?
                       WHERE event_id=? AND checkpoint_seconds=?""",
                    (qts, cp_bid, cp_pnl, int(now_ms), rid, int(cp["checkpoint_seconds"])),
                )

            if now_ms >= expires:
                c.execute(
                    "UPDATE leadlag_events SET status='COMPLETE',last_update_ts_ms=? WHERE id=?",
                    (int(now_ms), rid),
                )
        c.commit()
        c.close()

def _leadlag_maybe_candidates(leader_asset, now_s):
    base = _leadlag_history_base(leader_asset, now_s, LEADLAG_LOOKBACK)
    h = _leadlag_hist.get(leader_asset)
    if not base or not h:
        return
    _, base_mid, base_ticker = base
    _, leader_mid, leader_ticker = h[-1]
    leader_move = (leader_mid - base_mid) * 100.0
    if abs(leader_move) < LEADLAG_TRIGGER_CENTS:
        return
    side = "UP" if leader_move > 0 else "DOWN"
    now_ms = int(now_s * 1000)
    for follower in RESEARCH_ASSETS:
        if follower == leader_asset:
            continue
        fh = _leadlag_hist.get(follower)
        fbase = _leadlag_history_base(follower, now_s, LEADLAG_LOOKBACK)
        if not fh or not fbase:
            continue
        _, follower_base_mid, _ = fbase
        _, follower_mid, follower_ticker = fh[-1]
        follower_move = (follower_mid - follower_base_mid) * 100.0
        if abs(follower_move) > LEADLAG_FOLLOWER_MAX_CENTS:
            continue
        top = (state.get("leadlag") or {}).get("tops", {}).get(follower) or {}
        entry_ask = top.get("up_ask") if side == "UP" else top.get("down_ask")
        entry_bid = top.get("up_bid") if side == "UP" else top.get("down_bid")
        if entry_ask is None or entry_bid is None or not (LEADLAG_ENTRY_MIN <= float(entry_ask) <= LEADLAG_ENTRY_MAX):
            continue
        k = (leader_asset, follower, side)
        last = _leadlag_last_candidate.get(k, 0.0)
        if now_s - last < LEADLAG_EVENT_COOLDOWN:
            continue
        _leadlag_last_candidate[k] = now_s
        spread_cents = (float(entry_ask) - float(entry_bid)) * 100.0
        with _leadlag_db_lock:
            c = sqlite3.connect(DB, timeout=5)
            cur = c.execute(
                """INSERT INTO leadlag_events(
                    created_ts_ms,created_ts,leader_asset,follower_asset,side,leader_ticker,follower_ticker,
                    leader_move_cents,follower_move_cents,entry_ask,entry_bid,entry_spread_cents,expires_ts_ms,
                    max_exit_bid,max_exit_ts_ms,max_gross_pnl_10,status,last_update_ts_ms)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    now_ms, datetime.fromtimestamp(now_s, timezone.utc).isoformat(),
                    leader_asset, follower, side, leader_ticker or base_ticker, follower_ticker,
                    leader_move, follower_move, float(entry_ask), float(entry_bid), spread_cents,
                    now_ms + int(LEADLAG_MAX_HOLD * 1000), float(entry_bid), now_ms,
                    10.0 * (float(entry_bid) / float(entry_ask) - 1.0), "OPEN", now_ms,
                ),
            )
            event_id = int(cur.lastrowid)
            for seconds in LEADLAG_CHECKPOINTS:
                c.execute(
                    """INSERT OR IGNORE INTO leadlag_checkpoints(event_id,checkpoint_seconds,due_ts_ms)
                       VALUES(?,?,?)""",
                    (event_id, int(seconds), now_ms + int(seconds * 1000)),
                )
            c.commit(); c.close()


def _leadlag_purge(now_s):
    global _leadlag_last_purge
    if now_s - _leadlag_last_purge < 60:
        return
    _leadlag_last_purge = now_s
    cutoff_ms = int((now_s - LEADLAG_KEEP_HOURS * 3600.0) * 1000)
    now_ms = int(now_s * 1000)
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        c.execute("UPDATE leadlag_events SET status='COMPLETE',last_update_ts_ms=? WHERE status='OPEN' AND expires_ts_ms<?", (now_ms, now_ms))
        c.execute("DELETE FROM leadlag_quotes WHERE recv_ts_ms<?", (cutoff_ms,))
        c.execute("DELETE FROM leadlag_events WHERE created_ts_ms<?", (cutoff_ms,))
        c.commit(); c.close()


def _leadlag_store_quote(asset, ticker, seq, exchange_ts_ms, recv_ts_ms, top):
    if not top:
        return
    key = (ticker, round(top["up_bid"], 4), round(top["up_ask"], 4), round(top["down_bid"], 4), round(top["down_ask"], 4))
    if _leadlag_last_top.get(asset) == key:
        return
    _leadlag_last_top[asset] = key
    now_s = recv_ts_ms / 1000.0
    _leadlag_hist[asset].append((now_s, top["up_mid"], ticker))
    state["leadlag"]["tops"][asset] = {
        "ticker": ticker, "up_bid": top["up_bid"], "up_ask": top["up_ask"],
        "down_bid": top["down_bid"], "down_ask": top["down_ask"], "recv_ts_ms": recv_ts_ms,
    }
    state["leadlag"]["last_quote_at"] = now_s
    seconds = _leadlag_seconds(asset)
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        c.execute(
            """INSERT INTO leadlag_quotes(recv_ts_ms,exchange_ts_ms,asset,ticker,seconds,up_bid,up_ask,down_bid,down_ask,up_bid_size,down_bid_size,seq)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (recv_ts_ms, exchange_ts_ms, asset, ticker, seconds, top["up_bid"], top["up_ask"], top["down_bid"], top["down_ask"], top["up_bid_size"], top["down_bid_size"], seq),
        )
        c.commit(); c.close()
    _leadlag_update_open_events(asset, recv_ts_ms, top)
    _leadlag_maybe_candidates(asset, now_s)
    _leadlag_purge(now_s)


def _leadlag_apply_snapshot(asset, ticker, msg, seq, recv_ts_ms):
    book = {"yes": {}, "no": {}, "seq": seq}
    for side, keys in (("yes", ("yes_dollars_fp","yes_dollars","yes")), ("no", ("no_dollars_fp","no_dollars","no"))):
        levels = []
        for k in keys:
            if msg.get(k) is not None:
                levels = msg.get(k) or []
                break
        for level in levels:
            try:
                p = _ll_price(level[0]); q = _ll_qty(level[1])
                if p is not None and q > 0:
                    book[side][p] = q
            except Exception:
                pass
    _leadlag_books[ticker] = book
    top = _leadlag_book_top(book)
    _leadlag_store_quote(asset, ticker, seq, _ll_exchange_ts_ms(msg, recv_ts_ms), recv_ts_ms, top)


def _leadlag_apply_delta(asset, ticker, msg, seq, recv_ts_ms):
    book = _leadlag_books.get(ticker)
    if not book:
        raise RuntimeError(f"delta before snapshot for {ticker}")
    side = str(msg.get("side") or "").lower()
    p = _ll_price(msg.get("price_dollars") if msg.get("price_dollars") is not None else msg.get("price"))
    if side not in ("yes","no") or p is None:
        return
    delta = _ll_qty(abs(float(msg.get("delta_fp") if msg.get("delta_fp") is not None else msg.get("delta") or 0)))
    raw_delta = float(msg.get("delta_fp") if msg.get("delta_fp") is not None else msg.get("delta") or 0)
    old = float(book[side].get(p, 0.0))
    new = old - delta if raw_delta < 0 else old + delta
    if new <= 1e-12:
        book[side].pop(p, None)
    else:
        book[side][p] = new
    book["seq"] = seq
    top = _leadlag_book_top(book)
    _leadlag_store_quote(asset, ticker, seq, _ll_exchange_ts_ms(msg, recv_ts_ms), recv_ts_ms, top)


async def _leadlag_ws_session(ticker_to_asset):
    # Only retain books for the currently active 15-minute markets. Without this,
    # old order books accumulate forever as tickers roll and eventually exhaust RAM.
    active = set(ticker_to_asset)
    for old_ticker in list(_leadlag_books):
        if old_ticker not in active:
            _leadlag_books.pop(old_ticker, None)
    for asset, key in list(_leadlag_last_top.items()):
        if key and key[0] not in active:
            _leadlag_last_top.pop(asset, None)

    headers = auth_headers("GET", KALSHI_WS_PATH)
    async with websockets.connect(
        KALSHI_WS_URL,
        additional_headers=headers,
        ping_interval=10,
        ping_timeout=20,
        close_timeout=5,
        # Keep the inbound queue bounded. A large queue can consume substantial RAM
        # during bursts and contributed to Render memory pressure.
        max_queue=256,
    ) as ws:
        tickers = sorted(ticker_to_asset)
        state["leadlag"].update({"connected": True, "status": "STREAMING", "subscribed": tickers, "error": None})
        await ws.send(json.dumps({
            "id": 1, "cmd": "subscribe",
            "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
        }))
        last_seq = None
        while True:
            desired = _leadlag_desired_tickers()
            if set(desired) != set(ticker_to_asset):
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            recv_ts_ms = int(time.time() * 1000)
            data = json.loads(raw)
            typ = data.get("type")
            state["leadlag"]["last_message_at"] = recv_ts_ms / 1000.0
            if typ == "error":
                raise RuntimeError(str(data.get("msg") or data)[:300])
            if typ not in ("orderbook_snapshot", "orderbook_delta"):
                continue
            seq = data.get("seq")
            if seq is not None:
                if last_seq is not None and int(seq) != int(last_seq) + 1:
                    raise RuntimeError(f"orderbook sequence gap: {last_seq}->{seq}")
                last_seq = int(seq)
            msg = data.get("msg") or {}
            ticker = msg.get("market_ticker")
            asset = ticker_to_asset.get(ticker)
            if not ticker or not asset:
                continue
            if typ == "orderbook_snapshot":
                _leadlag_apply_snapshot(asset, ticker, msg, seq, recv_ts_ms)
            else:
                _leadlag_apply_delta(asset, ticker, msg, seq, recv_ts_ms)


async def _leadlag_ws_loop():
    while LEADLAG_ENABLED:
        desired = _leadlag_desired_tickers()
        if len(desired) < 2:
            state["leadlag"].update({"connected": False, "status": "WAITING FOR MARKET TICKERS", "subscribed": []})
            await asyncio.sleep(2)
            continue
        try:
            await _leadlag_ws_session(desired)
        except Exception as e:
            state["leadlag"].update({"connected": False, "status": "RECONNECTING", "error": str(e)[:300]})
            state["leadlag"]["reconnects"] = int(state["leadlag"].get("reconnects") or 0) + 1
            await asyncio.sleep(min(15, 1 + state["leadlag"]["reconnects"] ** 0.5))
        finally:
            state["leadlag"]["connected"] = False


def leadlag_ws_thread():
    if not LEADLAG_ENABLED:
        state["leadlag"]["status"] = "DISABLED"
        return
    try:
        asyncio.run(_leadlag_ws_loop())
    except Exception as e:
        state["leadlag"].update({"connected": False, "status": "STOPPED", "error": str(e)[:300]})


def brti_value():
    return first_number(
        kget(
            f"{TRADE_ROOT}/cfbenchmarks/values",
            {"id": "BRTI"},
            auth=True,
        )
    )


def left(m):
    try:
        return max(
            0,
            (
                datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
                - datetime.now(timezone.utc)
            ).total_seconds(),
        )
    except Exception:
        return None


def calc(m, h, final_call=False):
    if not m or len(h) < 10:
        return {"ready": False, "signal": "WARMING UP"}

    now = time.time()
    vals = [p for t, p in h]

    def prior(sec):
        target = now - sec
        a = min(h, key=lambda x: abs(x[0] - target))
        return a[1] if abs(a[0] - target) <= max(5, sec * 0.12) else None

    def ret(sec):
        p = prior(sec)
        return vals[-1] / p - 1 if p else None

    recent = [p for t, p in h if t > now - 60]
    rg = max(recent) - min(recent) if len(recent) > 3 else None
    r1, r3, r5 = ret(60), ret(180), ret(300)

    ready = (
        h[-1][0] - h[0][0] >= 270
        and all(x is not None for x in (r1, r3, r5, rg))
        and rg > 0
        and m.get("target")
    )
    if not ready:
        return {"ready": False, "signal": "WARMING UP"}

    def clamp(x, a, b):
        return max(a, min(b, x))

    def ns(v, d):
        return clamp(v / d * 50, -100, 100)

    d = vals[-1] - m["target"]
    nd = clamp(d / rg * 40, -100, 100)
    book = clamp((m.get("book_imbalance") or 0) * 100, -100, 100)
    s = clamp(
        0.52 * nd
        + 0.14 * ns(r1, 0.0025)
        + 0.10 * ns(r3, 0.005)
        + 0.08 * ns(r5, 0.0075)
        + 0.16 * book,
        -100,
        100,
    )

    pu = clamp(0.5 + s * 0.0035, 0.15, 0.85)
    side = "UP" if s >= 0 else "DOWN"
    pside = pu if side == "UP" else 1 - pu
    ask = m["up_ask"] if side == "UP" else m["down_ask"]
    edge = pside - ask if ask is not None else None
    sec = left(m)

    # Strategy thresholds are unchanged.
    # Live model: 8:30 -> 7:30.
    # Final-call mode gives the scheduler a 10-second execution tolerance AFTER
    # 7:30 so a delayed collector tick cannot permanently record a false PASS.
    if final_call:
        in_window = sec is not None and 440 <= sec <= 510
    else:
        in_window = sec is not None and 450 <= sec <= 510

    # V10.9 profit-first forward-test rule.
    # Direction still comes from the existing BTC model, but qualification is
    # based on the actual Kalshi ask for that selected side. Historical
    # settled-trade analysis identified 34–42 cents as the candidate value
    # band. Probability, edge, score and strength remain logged for research
    # but DO NOT veto a trade during this forward test.
    value_price_ok = ask is not None and 0.34 <= ask <= 0.42
    qual = bool(in_window and value_price_ok)

    strength = "LOW" if pside < 0.60 else ("MEDIUM" if pside < 0.65 else "STRONG")
    sig = f"VALUE {side}" if qual else "PASS"

    reason = (
        "VALUE 34–42¢"
        if qual
        else (
            "OUTSIDE 8:30–7:30"
            if not in_window
            else (
                "ASK BELOW 34¢"
                if ask is not None and ask < 0.34
                else (
                    "ASK ABOVE 42¢"
                    if ask is not None and ask > 0.42
                    else "NO LIVE ASK"
                )
            )
        )
    )

    return {
        "ready": True,
        "signal": sig,
        "side": side,
        "strength": strength,
        "qualified": qual,
        "reason": reason,
        "score": s,
        "prob": pside,
        "edge": edge,
        "ask": ask,
        "sec": sec,
        "r1": r1,
        "r3": r3,
        "r5": r5,
        "range": rg,
    }


def initdb():
    c = sqlite3.connect(DB)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute(
        """CREATE TABLE IF NOT EXISTS calls (
            id INTEGER PRIMARY KEY,
            ts TEXT,
            ticker TEXT UNIQUE,
            side TEXT,
            strength TEXT,
            signal TEXT,
            qualified INTEGER,
            reason TEXT,
            seconds REAL,
            brti REAL,
            target REAL,
            ask REAL,
            probability REAL,
            edge REAL,
            score REAL,
            outcome TEXT,
            result TEXT,
            pnl REAL,
            settlement_source TEXT,
            kalshi_result TEXT,
            settled_ts TEXT,
            hypothetical_result TEXT,
            hypothetical_pnl REAL
        )"""
    )

    cols = {r[1] for r in c.execute("PRAGMA table_info(calls)").fetchall()}
    for name, typ in [
        ("pnl", "REAL"),
        ("settlement_source", "TEXT"),
        ("kalshi_result", "TEXT"),
        ("settled_ts", "TEXT"),
        ("hypothetical_result", "TEXT"),
        ("hypothetical_pnl", "REAL"),
    ]:
        if name not in cols:
            c.execute(f"ALTER TABLE calls ADD COLUMN {name} {typ}")

    c.execute(
        "CREATE TABLE IF NOT EXISTS samples (ts REAL PRIMARY KEY, brti REAL)"
    )
    c.execute("""CREATE TABLE IF NOT EXISTS crypto_spot (ts REAL, asset TEXT, price REAL, PRIMARY KEY(ts,asset))""")
    c.execute("""CREATE TABLE IF NOT EXISTS crypto_market_snapshots (id INTEGER PRIMARY KEY, ts REAL, asset TEXT, series TEXT, ticker TEXT, seconds REAL, target REAL, up_ask REAL, down_ask REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS crypto_paper_trades (id INTEGER PRIMARY KEY, ts REAL, leader TEXT, asset TEXT, ticker TEXT, side TEXT, ask REAL, seconds REAL, leader_move_30s REAL, follower_move_30s REAL, status TEXT, outcome TEXT, result TEXT, pnl REAL, settled_ts TEXT, UNIQUE(ticker,leader,asset,side))""")
    c.execute("""CREATE TABLE IF NOT EXISTS leadlag_quotes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        recv_ts_ms INTEGER NOT NULL, exchange_ts_ms INTEGER, asset TEXT NOT NULL,
        ticker TEXT NOT NULL, seconds REAL, up_bid REAL, up_ask REAL,
        down_bid REAL, down_ask REAL, up_bid_size REAL, down_bid_size REAL, seq INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_quotes_asset_ts ON leadlag_quotes(asset, recv_ts_ms)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_quotes_ticker_ts ON leadlag_quotes(ticker, recv_ts_ms)")
    c.execute("""CREATE TABLE IF NOT EXISTS leadlag_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_ts_ms INTEGER NOT NULL, created_ts TEXT NOT NULL,
        leader_asset TEXT NOT NULL, follower_asset TEXT NOT NULL, side TEXT NOT NULL,
        leader_ticker TEXT, follower_ticker TEXT, leader_move_cents REAL,
        follower_move_cents REAL, entry_ask REAL, entry_bid REAL, entry_spread_cents REAL,
        expires_ts_ms INTEGER, max_exit_bid REAL, max_exit_ts_ms INTEGER,
        max_gross_pnl_10 REAL, hit_2c_ts_ms INTEGER, hit_5c_ts_ms INTEGER,
        hit_10c_ts_ms INTEGER, status TEXT, last_update_ts_ms INTEGER
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_events_pair ON leadlag_events(leader_asset,follower_asset,side,created_ts_ms)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_events_open ON leadlag_events(status,follower_asset,expires_ts_ms)")
    c.execute("""CREATE TABLE IF NOT EXISTS leadlag_checkpoints (
        event_id INTEGER NOT NULL, checkpoint_seconds INTEGER NOT NULL,
        due_ts_ms INTEGER NOT NULL, quote_ts_ms INTEGER, exit_bid REAL,
        gross_pnl_10 REAL, recorded_ts_ms INTEGER,
        PRIMARY KEY(event_id,checkpoint_seconds)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_leadlag_checkpoints_due ON leadlag_checkpoints(due_ts_ms,exit_bid)")
    c.execute(
        """CREATE TABLE IF NOT EXISTS push_subscriptions (
            endpoint TEXT PRIMARY KEY,
            subscription TEXT,
            updated_at TEXT
        )"""
    )

    # PASS rows are research observations, not trades.  Preserve their official
    # market outcome/hypothetical result, but never let them count as actual
    # WIN/LOSS trades in the live dashboard.
    c.execute(
        """UPDATE calls
           SET result=NULL,pnl=NULL
           WHERE qualified=0"""
    )

    # Existing officially settled qualified trades can populate the new
    # counterfactual columns locally without another API request.
    c.execute(
        """UPDATE calls
           SET hypothetical_result=result, hypothetical_pnl=pnl
           WHERE qualified=1
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'
             AND result IN ('WIN','LOSS')
             AND hypothetical_result IS NULL"""
    )

    # Only Kalshi official exact-ticker grades survive for actual trades.
    c.execute(
        """UPDATE calls
           SET outcome=NULL,result=NULL,pnl=NULL,settlement_source=NULL,
               kalshi_result=NULL,settled_ts=NULL
           WHERE qualified=1
             AND COALESCE(settlement_source,'')!='KALSHI_OFFICIAL_EXACT_TICKER'"""
    )
    c.commit()
    c.close()


def load_samples():
    c = sqlite3.connect(DB)
    cutoff = time.time() - 900
    rows = c.execute(
        "SELECT ts,brti FROM samples WHERE ts>? ORDER BY ts", (cutoff,)
    ).fetchall()
    c.close()
    state["hist"] = [(float(t), float(v)) for t, v in rows]


def save_sample(ts, b):
    c = sqlite3.connect(DB)
    c.execute(
        "INSERT OR REPLACE INTO samples(ts,brti) VALUES(?,?)",
        (ts, b),
    )
    c.execute("DELETE FROM samples WHERE ts<?", (ts - 1800,))
    c.commit()
    c.close()


def _save_push_subscription(sub):
    if not isinstance(sub, dict):
        raise HTTPException(400, "Invalid push subscription")

    endpoint = sub.get("endpoint")
    keys = sub.get("keys") or {}
    if not endpoint:
        raise HTTPException(400, "Missing push endpoint")
    if not keys.get("p256dh") or not keys.get("auth"):
        raise HTTPException(400, "Push subscription is missing keys")

    c = sqlite3.connect(DB)
    c.execute(
        """INSERT OR REPLACE INTO push_subscriptions
           (endpoint,subscription,updated_at)
           VALUES(?,?,?)""",
        (
            endpoint,
            json.dumps(sub, separators=(",", ":")),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    n = c.execute("SELECT COUNT(*) FROM push_subscriptions").fetchone()[0]
    c.commit()
    c.close()
    return n


def send_push(m, x):
    if not x.get("qualified"):
        return
    if not (webpush and VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY):
        state["push_error"] = "Push is not configured"
        return

    c = sqlite3.connect(DB)
    rows = c.execute(
        "SELECT endpoint,subscription FROM push_subscriptions"
    ).fetchall()
    c.close()

    if not rows:
        state["push_error"] = "No saved push subscriptions"
        return

    title = "15 Minute Edge — " + (x.get("signal") or "PASS")
    body = (
        f"{x.get('side','')} VALUE TRADE · "
        f"{round((x.get('ask') or 0) * 100)}¢ ask · "
        f"{round((x.get('prob') or 0) * 100)}% model probability"
    )
    data = json.dumps(
        {
            "title": title,
            "body": body,
            "url": "/",
            "ticker": m.get("ticker"),
        }
    )

    dead = []
    errors = []

    for endpoint, raw in rows:
        try:
            webpush(
                subscription_info=json.loads(raw),
                data=data,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_EMAIL},
                ttl=120,
            )
        except Exception as e:
            msg = str(e)
            if "410" in msg or "404" in msg:
                dead.append(endpoint)
            else:
                errors.append(msg[:180])

    if dead:
        c = sqlite3.connect(DB)
        c.executemany(
            "DELETE FROM push_subscriptions WHERE endpoint=?",
            [(x,) for x in dead],
        )
        c.commit()
        c.close()

    state["push_error"] = errors[0] if errors else None


def save_call(m, b, x):
    c = sqlite3.connect(DB)
    cur = c.execute(
        """INSERT OR IGNORE INTO calls(
            ts,ticker,side,strength,signal,qualified,reason,seconds,
            brti,target,ask,probability,edge,score
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            datetime.now(timezone.utc).isoformat(),
            m["ticker"],
            x.get("side"),
            x.get("strength"),
            x.get("signal"),
            int(x.get("qualified", False)),
            x.get("reason"),
            x.get("sec"),
            b,
            m.get("target"),
            x.get("ask"),
            x.get("prob"),
            x.get("edge"),
            x.get("score"),
        ),
    )
    inserted = cur.rowcount == 1
    c.commit()
    c.close()

    if inserted:
        send_push(m, x)
    return inserted


def get_official_market_result(ticker):
    if not ticker or not ticker.startswith(SERIES):
        return None
    try:
        d = kget(f"{TRADE_ROOT}/markets/{ticker}")
        m = d.get("market") or {}

        if m.get("ticker") != ticker:
            return None

        kr = str(m.get("result") or "").strip().lower()
        if kr not in ("yes", "no"):
            return None

        return {
            "outcome": "UP" if kr == "yes" else "DOWN",
            "kalshi_result": kr,
            "settled_ts": m.get("settlement_ts")
            or datetime.now(timezone.utc).isoformat(),
            "settlement_source": "KALSHI_OFFICIAL_EXACT_TICKER",
        }
    except HTTPException:
        raise
    except Exception:
        return None


def settle_official(ticker):
    official = get_official_market_result(ticker)
    if not official:
        return False

    c = sqlite3.connect(DB)
    row = c.execute(
        "SELECT side,qualified,ask FROM calls WHERE ticker=?",
        (ticker,),
    ).fetchone()

    if not row:
        c.close()
        return False

    side, qualified, ask = row
    hypothetical_result = "WIN" if side == official["outcome"] else "LOSS"
    hypothetical_pnl = (
        10.0 * ((1.0 / float(ask)) - 1.0)
        if hypothetical_result == "WIN" and ask and ask > 0
        else (-10.0 if ask and ask > 0 else None)
    )

    # Only qualified signals are actual paper trades.  PASS rows still receive
    # the exact Kalshi outcome plus separate hypothetical fields for backtests.
    trade_result = hypothetical_result if qualified else None
    trade_pnl = hypothetical_pnl if qualified else None

    c.execute(
        """UPDATE calls
           SET outcome=?,result=?,pnl=?,settlement_source=?,
               kalshi_result=?,settled_ts=?,
               hypothetical_result=?,hypothetical_pnl=?
           WHERE ticker=?""",
        (
            official["outcome"],
            trade_result,
            trade_pnl,
            official["settlement_source"],
            official["kalshi_result"],
            official["settled_ts"],
            hypothetical_result,
            hypothetical_pnl,
            ticker,
        ),
    )
    c.commit()
    c.close()
    return True


def settle_pending_official(limit=SETTLEMENT_BATCH):
    c = sqlite3.connect(DB)
    rows = c.execute(
        """SELECT ticker
           FROM calls
           WHERE COALESCE(settlement_source,'')!='KALSHI_OFFICIAL_EXACT_TICKER'
              OR hypothetical_result IS NULL
              OR (qualified=1 AND result IS NULL)
           ORDER BY qualified DESC, id ASC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    c.close()

    for (ticker,) in rows:
        try:
            settle_official(ticker)
        except HTTPException as e:
            if e.status_code == 429:
                break


def collector():
    initdb()
    load_samples()

    last_market = 0.0
    last_sample = 0.0
    last_settlement_check = 0.0
    last_research_spot = 0.0
    last_research_market = 0.0
    research_index = 0
    last_research_settle = 0.0
    state["collector"] = "RUNNING"

    while True:
        loop_started = time.time()

        try:
            now = time.time()

            if now - last_market >= MARKET_INTERVAL:
                m = get_market()
                last_market = time.time()
                if m:
                    state["market"] = m
                    state["last_market_at"] = time.time()

            if now - last_sample >= BRTI_INTERVAL:
                b = brti_value()
                last_sample = time.time()

                if b:
                    ts = time.time()
                    state["last_sample_at"] = ts
                    state["hist"].append((ts, b))
                    state["hist"] = state["hist"][-900:]
                    save_sample(ts, b)

                    x = calc(state["market"], state["hist"])
                    state["last_call"] = x

                    # Final call is stored once as the clock crosses 7:30.
                    # We allow 10 seconds of scheduler tolerance so a 429/backoff
                    # or network delay cannot accidentally create a permanent PASS.
                    if (
                        x.get("ready")
                        and x.get("sec") is not None
                        and 440 <= x["sec"] <= 450
                        and state.get("market")
                    ):
                        final_x = calc(
                            state["market"],
                            state["hist"],
                            final_call=True,
                        )
                        save_call(state["market"], b, final_x)

            # V10.10 multi-crypto research runs beside, never inside, BTC Value logic.
            if now - last_research_spot >= SPOT_INTERVAL:
                try:
                    prices = coinbase_spot_snapshot(); rts=time.time(); save_spot_snapshot(rts, prices)
                    state["research"]["spot"] = prices; state["research"]["last_spot_at"] = rts
                    maybe_paper_followers(rts); state["research"]["error"] = None
                except Exception as e:
                    state["research"]["error"] = str(e)[:220]
                last_research_spot = time.time()

            if now - last_research_market >= RESEARCH_MARKET_INTERVAL and RESEARCH_ASSETS:
                asset = RESEARCH_ASSETS[research_index % len(RESEARCH_ASSETS)]; research_index += 1
                series = RESEARCH_SERIES.get(asset)
                if series:
                    try:
                        rm=get_series_market(series); rts=time.time()
                        if rm:
                            state["research"]["markets"][asset]=rm; state["research"]["last_market_at"]=rts; save_research_market(rts,asset,rm)
                    except HTTPException as e:
                        if e.status_code != 429: state["research"]["error"] = f"{e.status_code}: {e.detail}"
                last_research_market = time.time()

            if now - last_research_settle >= 90:
                settle_research_paper(2); last_research_settle=time.time()

            if now - last_settlement_check >= SETTLEMENT_INTERVAL:
                settle_pending_official()
                last_settlement_check = time.time()

            state["error"] = None

        except HTTPException as e:
            state["error"] = f"{e.status_code}: {e.detail}"
        except Exception as e:
            state["error"] = str(e)[:240]

        # This loop itself is lightweight. Actual Kalshi calls are separately paced.
        elapsed = time.time() - loop_started
        time.sleep(max(0.5, 1.0 - elapsed))


@app.on_event("startup")
def startup():
    global _collector_started, _leadlag_started
    initdb()
    with _collector_guard:
        if not _collector_started:
            _collector_started = True
            threading.Thread(target=collector, daemon=True).start()
    with _leadlag_guard:
        if LEADLAG_ENABLED and not _leadlag_started:
            _leadlag_started = True
            threading.Thread(target=leadlag_ws_thread, daemon=True).start()


@app.get("/")
def home():
    return FileResponse(
        "index.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/manifest.webmanifest")
def manifest():
    return FileResponse(
        "manifest.webmanifest",
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.get("/sw.js")
def sw():
    return FileResponse(
        "sw.js",
        media_type="application/javascript",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Service-Worker-Allowed": "/",
        },
    )


@app.get("/api/live")
def live():
    m = state.get("market")
    h = state.get("hist") or []
    b = h[-1][1] if h else None
    x = calc(m, h) if m else {"ready": False, "signal": "WAIT"}
    span = (h[-1][0] - h[0][0]) if len(h) > 1 else 0

    return {
        "market": m,
        "brti": b,
        "model": x,
        "collector": state.get("collector", "RUNNING"),
        "samples": len(h),
        "history_seconds": round(span, 1),
        "warmup_remaining": max(0, round(270 - span, 1)),
        "last_sample_at": state.get("last_sample_at"),
        "last_market_at": state.get("last_market_at"),
        "error": state.get("error"),
    }


@app.get("/api/history")
def history(actionable: bool = True):
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    if actionable:
        rows = [
            dict(r)
            for r in c.execute(
                "SELECT * FROM calls WHERE qualified=1 ORDER BY id DESC LIMIT 100"
            )
        ]
    else:
        rows = [
            dict(r)
            for r in c.execute("SELECT * FROM calls ORDER BY id DESC LIMIT 100")
        ]
    c.close()
    return rows


@app.get("/api/stats")
def stats():
    c = sqlite3.connect(DB)

    recorded = c.execute(
        "SELECT COUNT(*) FROM calls WHERE qualified=1"
    ).fetchone()[0]
    settled = c.execute(
        """SELECT COUNT(*) FROM calls
           WHERE qualified=1
             AND result IN ('WIN','LOSS')
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'"""
    ).fetchone()[0]
    wins = c.execute(
        """SELECT COUNT(*) FROM calls
           WHERE qualified=1
             AND result='WIN'
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'"""
    ).fetchone()[0]
    losses = c.execute(
        """SELECT COUNT(*) FROM calls
           WHERE qualified=1
             AND result='LOSS'
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'"""
    ).fetchone()[0]
    paper = float(
        c.execute(
            """SELECT COALESCE(SUM(pnl),0) FROM calls
               WHERE qualified=1
                 AND result IN ('WIN','LOSS')
                 AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'"""
        ).fetchone()[0]
        or 0
    )
    passes = c.execute(
        "SELECT COUNT(*) FROM calls WHERE qualified=0"
    ).fetchone()[0]
    logged = c.execute("SELECT COUNT(*) FROM calls").fetchone()[0]

    last = c.execute(
        """SELECT side,result,pnl FROM calls
           WHERE qualified=1
             AND result IN ('WIN','LOSS')
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'
           ORDER BY id DESC LIMIT 1"""
    ).fetchone()

    def side_stats(side):
        w = c.execute(
            """SELECT COUNT(*) FROM calls
               WHERE qualified=1 AND side=? AND result='WIN'
                 AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'""",
            (side,),
        ).fetchone()[0]
        l = c.execute(
            """SELECT COUNT(*) FROM calls
               WHERE qualified=1 AND side=? AND result='LOSS'
                 AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'""",
            (side,),
        ).fetchone()[0]
        n = w + l
        return {
            "wins": w,
            "losses": l,
            "trades": n,
            "win_rate": round(100 * w / n, 1) if n else None,
        }

    pending = max(0, recorded - settled)
    all_settled = c.execute(
        """SELECT COUNT(*) FROM calls
           WHERE hypothetical_result IN ('WIN','LOSS')
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'"""
    ).fetchone()[0]
    passes_settled = c.execute(
        """SELECT COUNT(*) FROM calls
           WHERE qualified=0
             AND hypothetical_result IN ('WIN','LOSS')
             AND settlement_source='KALSHI_OFFICIAL_EXACT_TICKER'"""
    ).fetchone()[0]
    backtest_backlog = max(0, logged - all_settled)

    out = {
        "recorded_signals": recorded,
        "settled": settled,
        "pending": pending,
        "wins": wins,
        "losses": losses,
        "win_rate": round(100 * wins / settled, 1) if settled else None,
        "paper_pl_10": round(paper, 2),
        "passes": passes,
        "calls_logged": logged,
        "all_signals_settled": all_settled,
        "passes_settled": passes_settled,
        "backtest_settlement_backlog": backtest_backlog,
        "last_settled": (
            {"side": last[0], "result": last[1], "pnl": last[2]}
            if last
            else None
        ),
        "up": side_stats("UP"),
        "down": side_stats("DOWN"),
    }

    c.close()
    return out


@app.get("/api/export.csv")
def export_csv():
    c = sqlite3.connect(DB)
    cur = c.execute("SELECT * FROM calls ORDER BY id")
    names = [d[0] for d in cur.description]
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(names)
    w.writerows(cur.fetchall())
    c.close()

    return StreamingResponse(
        iter([out.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=15-minute-edge-v10-signals.csv"
        },
    )


@app.get("/api/push/public-key")
def push_public_key():
    return {
        "publicKey": VAPID_PUBLIC_KEY,
        "configured": bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY and webpush),
        "keyMatch": VAPID_KEY_MATCH,
        "usingDerivedPublicKey": bool(
            VAPID_DERIVED_PUBLIC_KEY
            and VAPID_DERIVED_PUBLIC_KEY != VAPID_PUBLIC_KEY_ENV
        ),
    }


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    sub = await request.json()
    n = _save_push_subscription(sub)
    return {"ok": True, "subscriptions": n}


@app.get("/api/push/status")
def push_status():
    c = sqlite3.connect(DB)
    n = c.execute("SELECT COUNT(*) FROM push_subscriptions").fetchone()[0]
    c.close()

    return {
        "configured": bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY and webpush),
        "subscriptions": n,
        "keyMatch": VAPID_KEY_MATCH,
        "usingDerivedPublicKey": bool(
            VAPID_DERIVED_PUBLIC_KEY
            and VAPID_DERIVED_PUBLIC_KEY != VAPID_PUBLIC_KEY_ENV
        ),
        "lastPushError": state.get("push_error"),
    }


@app.post("/api/push/test")
async def push_test(request: Request):
    if not (webpush and VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY):
        raise HTTPException(503, "Push is not configured on the server")

    sub = await request.json()
    if not isinstance(sub, dict) or not sub.get("endpoint"):
        raise HTTPException(400, "Missing push subscription")

    # Important V10.8 repair:
    # A test notification also persists the phone subscription. This means an
    # existing iPhone subscription can self-repair the old "0 saved" condition.
    n = _save_push_subscription(sub)

    data = json.dumps(
        {
            "title": "15 Minute Edge — TEST",
            "body": "Push notifications are working.",
            "url": "/",
            "ticker": "edge-v10-test",
        }
    )

    try:
        webpush(
            subscription_info=sub,
            data=data,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_EMAIL},
            ttl=120,
        )
        state["push_error"] = None
    except Exception as e:
        state["push_error"] = str(e)[:220]
        raise HTTPException(
            502,
            "Push provider rejected test: " + str(e)[:220],
        )

    return {"ok": True, "subscriptions": n}


@app.get("/api/research/live")
def research_live():
    now=time.time(); moves={}
    for a in RESEARCH_ASSETS:
        moves[a]={"r5s":research_return(a,5,now),"r15s":research_return(a,15,now),"r30s":research_return(a,30,now),"r60s":research_return(a,60,now),"r120s":research_return(a,120,now)}
    return {"assets":RESEARCH_ASSETS,"spot":state["research"].get("spot",{}),"moves":moves,"markets":state["research"].get("markets",{}),"last_spot_at":state["research"].get("last_spot_at"),"last_market_at":state["research"].get("last_market_at"),"error":state["research"].get("error")}

@app.get("/api/research/stats")
def research_stats():
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row
    rows=[dict(r) for r in c.execute("""SELECT asset,COUNT(*) trades,SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) wins,SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) losses,ROUND(COALESCE(SUM(pnl),0),2) pnl FROM crypto_paper_trades WHERE status='SETTLED' GROUP BY asset ORDER BY pnl DESC""")]
    for r in rows:
        n=(r['wins'] or 0)+(r['losses'] or 0); r['win_rate']=round(100*(r['wins'] or 0)/n,1) if n else None; r['roi_on_10_risk']=round(100*(r['pnl'] or 0)/(10*n),1) if n else None
    pending=c.execute("SELECT COUNT(*) FROM crypto_paper_trades WHERE status='PENDING'").fetchone()[0]; c.close()
    return {"objective":"NET_PROFIT_FIRST","paper_bet_dollars":10,"pending":pending,"by_asset":rows}

@app.get("/api/research/export.csv")
def research_export():
    c=sqlite3.connect(DB); cur=c.execute("SELECT * FROM crypto_paper_trades ORDER BY id"); names=[d[0] for d in cur.description]
    out=io.StringIO(); w=csv.writer(out); w.writerow(names); w.writerows(cur.fetchall()); c.close()
    return StreamingResponse(iter([out.getvalue()]),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=v10.10-multicrypto-paper-trades.csv"})


@app.get("/api/leadlag/status")
def leadlag_status():
    _leadlag_purge(time.time())
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        quotes = c.execute("SELECT COUNT(*) FROM leadlag_quotes").fetchone()[0]
        events = c.execute("SELECT COUNT(*) FROM leadlag_events").fetchone()[0]
        open_events = c.execute("SELECT COUNT(*) FROM leadlag_events WHERE status='OPEN'").fetchone()[0]
        last = c.execute(
            """SELECT leader_asset,follower_asset,side,leader_move_cents,follower_move_cents,
                      entry_ask,max_exit_bid,max_gross_pnl_10,created_ts
               FROM leadlag_events ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        c.close()
    last_event = None
    if last:
        last_event = {
            "leader": last[0], "follower": last[1], "side": last[2],
            "leader_move_cents": last[3], "follower_move_cents": last[4],
            "entry_ask": last[5], "max_exit_bid": last[6],
            "max_gross_pnl_10": last[7], "created_ts": last[8],
        }
    ll = state.get("leadlag") or {}
    return {
        "enabled": LEADLAG_ENABLED,
        "connected": bool(ll.get("connected")),
        "status": ll.get("status"),
        "subscribed": ll.get("subscribed") or [],
        "last_message_at": ll.get("last_message_at"),
        "last_quote_at": ll.get("last_quote_at"),
        "reconnects": ll.get("reconnects") or 0,
        "error": ll.get("error"),
        "quotes_stored": quotes,
        "recording": bool(ll.get("connected") and ll.get("last_quote_at") and time.time() - float(ll.get("last_quote_at")) < 30),
        "db_path": DB,
        "candidate_events": events,
        "open_events": open_events,
        "last_event": last_event,
        "tops": ll.get("tops") or {},
        "settings": {
            "lookback_seconds": LEADLAG_LOOKBACK,
            "leader_trigger_cents": LEADLAG_TRIGGER_CENTS,
            "follower_max_move_cents": LEADLAG_FOLLOWER_MAX_CENTS,
            "max_hold_seconds": LEADLAG_MAX_HOLD,
            "checkpoints_seconds": list(LEADLAG_CHECKPOINTS),
            "keep_hours": LEADLAG_KEEP_HOURS,
            "entry_ask_range": [LEADLAG_ENTRY_MIN, LEADLAG_ENTRY_MAX],
        },
        "note": "Research only. Entry uses executable ask; hypothetical exit uses executable bid. Gross P/L excludes fees and fill uncertainty.",
    }


@app.get("/api/leadlag/stats")
def leadlag_stats():
    _leadlag_purge(time.time())
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        c.row_factory = sqlite3.Row
        rows = [dict(r) for r in c.execute(
            """SELECT leader_asset AS leader, follower_asset AS follower, side,
                      COUNT(*) AS events,
                      SUM(CASE WHEN hit_2c_ts_ms IS NOT NULL THEN 1 ELSE 0 END) AS hit_2c,
                      SUM(CASE WHEN hit_5c_ts_ms IS NOT NULL THEN 1 ELSE 0 END) AS hit_5c,
                      SUM(CASE WHEN hit_10c_ts_ms IS NOT NULL THEN 1 ELSE 0 END) AS hit_10c,
                      ROUND(AVG(max_gross_pnl_10),3) AS avg_best_gross_pnl_10,
                      ROUND(MAX(max_gross_pnl_10),3) AS max_best_gross_pnl_10,
                      ROUND(AVG(CASE WHEN hit_5c_ts_ms IS NOT NULL THEN hit_5c_ts_ms-created_ts_ms END),0) AS avg_ms_to_5c
               FROM leadlag_events
               GROUP BY leader_asset,follower_asset,side
               ORDER BY avg_best_gross_pnl_10 DESC, events DESC"""
        )]
        c.close()
    for r in rows:
        n = r.get("events") or 0
        for k in ("hit_2c","hit_5c","hit_10c"):
            r[k+"_rate"] = round(100.0*(r.get(k) or 0)/n, 1) if n else None
    return {
        "objective": "PROVE_EXECUTABLE_LEAD_LAG_SCALP",
        "pairs": rows,
        "warning": "avg_best_gross_pnl_10 is the best executable bid seen within the observation window, not a guaranteed realized exit. Fees are not deducted.",
    }


@app.get("/api/leadlag/export-quotes.csv")
def leadlag_export_quotes():
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        cur = c.execute("SELECT * FROM leadlag_quotes ORDER BY id")
        names = [d[0] for d in cur.description]
        out = io.StringIO(); w = csv.writer(out); w.writerow(names); w.writerows(cur.fetchall()); c.close()
    return StreamingResponse(iter([out.getvalue()]), media_type="text/csv", headers={"Content-Disposition":"attachment; filename=v10.12-leadlag-quotes.csv"})


@app.get("/api/leadlag/export-events.csv")
def leadlag_export_events():
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        cur = c.execute("SELECT * FROM leadlag_events ORDER BY id")
        names = [d[0] for d in cur.description]
        out = io.StringIO(); w = csv.writer(out); w.writerow(names); w.writerows(cur.fetchall()); c.close()
    return StreamingResponse(iter([out.getvalue()]), media_type="text/csv", headers={"Content-Disposition":"attachment; filename=v10.12-leadlag-events.csv"})


@app.get("/api/leadlag/checkpoint-stats")
def leadlag_checkpoint_stats():
    _leadlag_purge(time.time())
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        c.row_factory = sqlite3.Row
        overall = [dict(r) for r in c.execute(
            """SELECT checkpoint_seconds,COUNT(*) AS observations,
                      ROUND(AVG(gross_pnl_10),3) AS avg_gross_pnl_10,
                      ROUND(SUM(gross_pnl_10),3) AS total_gross_pnl_10,
                      SUM(CASE WHEN gross_pnl_10>0 THEN 1 ELSE 0 END) AS profitable,
                      ROUND(MIN(gross_pnl_10),3) AS worst_gross_pnl_10,
                      ROUND(MAX(gross_pnl_10),3) AS best_gross_pnl_10
               FROM leadlag_checkpoints WHERE exit_bid IS NOT NULL
               GROUP BY checkpoint_seconds ORDER BY checkpoint_seconds"""
        )]
        by_pair = [dict(r) for r in c.execute(
            """SELECT e.leader_asset AS leader,e.follower_asset AS follower,e.side,
                      cp.checkpoint_seconds,COUNT(*) AS observations,
                      ROUND(AVG(cp.gross_pnl_10),3) AS avg_gross_pnl_10,
                      ROUND(SUM(cp.gross_pnl_10),3) AS total_gross_pnl_10,
                      SUM(CASE WHEN cp.gross_pnl_10>0 THEN 1 ELSE 0 END) AS profitable
               FROM leadlag_checkpoints cp JOIN leadlag_events e ON e.id=cp.event_id
               WHERE cp.exit_bid IS NOT NULL
               GROUP BY e.leader_asset,e.follower_asset,e.side,cp.checkpoint_seconds
               ORDER BY total_gross_pnl_10 DESC,observations DESC"""
        )]
        c.close()
    for row in overall + by_pair:
        n = int(row.get("observations") or 0)
        row["profitable_rate"] = round(100.0 * int(row.get("profitable") or 0) / n, 1) if n else None
    return {
        "paper_bet_dollars": 10,
        "checkpoints_seconds": list(LEADLAG_CHECKPOINTS),
        "overall": overall,
        "by_pair": by_pair,
        "warning": "Gross paper P/L uses ask-in and bid-out and does not deduct fees or model queue/fill uncertainty.",
    }


@app.get("/api/leadlag/export-checkpoints.csv")
def leadlag_export_checkpoints():
    with _leadlag_db_lock:
        c = sqlite3.connect(DB, timeout=5)
        cur = c.execute(
            """SELECT cp.event_id,e.created_ts,e.leader_asset,e.follower_asset,e.side,
                      e.entry_ask,e.entry_bid,e.entry_spread_cents,cp.checkpoint_seconds,
                      cp.due_ts_ms,cp.quote_ts_ms,cp.exit_bid,cp.gross_pnl_10,cp.recorded_ts_ms
               FROM leadlag_checkpoints cp JOIN leadlag_events e ON e.id=cp.event_id
               ORDER BY cp.event_id,cp.checkpoint_seconds"""
        )
        names = [d[0] for d in cur.description]
        out = io.StringIO(); w = csv.writer(out); w.writerow(names); w.writerows(cur.fetchall()); c.close()
    return StreamingResponse(iter([out.getvalue()]), media_type="text/csv", headers={"Content-Disposition":"attachment; filename=v10.12-leadlag-checkpoints.csv"})


@app.get("/api/health")
def health():
    h = state.get("hist") or []
    span = (h[-1][0] - h[0][0]) if len(h) > 1 else 0

    c = sqlite3.connect(DB)
    n = c.execute("SELECT COUNT(*) FROM push_subscriptions").fetchone()[0]
    c.close()

    return {
        "ok": True,
        "version": "V10.12",
        "series": SERIES,
        "collector": state.get("collector", "RUNNING"),
        "db": DB,
        "samples": len(h),
        "history_seconds": round(span, 1),
        "warmup_remaining": max(0, round(270 - span, 1)),
        "push_configured": bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY and webpush),
        "push_subscriptions": n,
        "push_error": state.get("push_error"),
        "vapid_key_match": VAPID_KEY_MATCH,
        "using_derived_public_key": bool(
            VAPID_DERIVED_PUBLIC_KEY
            and VAPID_DERIVED_PUBLIC_KEY != VAPID_PUBLIC_KEY_ENV
        ),
        "last_429_at": state.get("last_429_at"),
        "leadlag": {
            "enabled": LEADLAG_ENABLED,
            "connected": bool((state.get("leadlag") or {}).get("connected")),
            "status": (state.get("leadlag") or {}).get("status"),
            "subscribed_count": len((state.get("leadlag") or {}).get("subscribed") or []),
            "last_quote_at": (state.get("leadlag") or {}).get("last_quote_at"),
            "error": (state.get("leadlag") or {}).get("error"),
        },
        "api_pacing": {
            "brti_seconds": BRTI_INTERVAL,
            "market_seconds": MARKET_INTERVAL,
            "settlement_seconds": SETTLEMENT_INTERVAL,
            "settlement_batch": SETTLEMENT_BATCH,
            "minimum_request_gap_seconds": KALSHI_MIN_GAP,
        },
        "error": state.get("error"),
    }
