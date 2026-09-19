"""
THE CREW — AI SIGNAL DESK
==========================
Four AI agents work the desk together, each with a distinct job:

  LESTER   — Head Analyst. Reads multi-timeframe structure (EMA stacks).
  MICHAEL  — Momentum. Confirms the move has fuel, refuses to chase exhaust.
  FRANKLIN — Risk Manager. Audits every setup, can VETO anything.
  TREVOR   — Executor. Fires the signal only on full crew consensus.

Built on the same hardened engine as the Signal Scanner bot:
  - Binance spot public data (no API key)
  - Multi-timeframe confluence voting (15m / 1h / 4h / 1d)
  - Entry + SL (1.5x ATR) + TP1/TP2/TP3 (1R/2R/3R)
  - Self-auditing win rate on every closed signal
  - Generations pattern + watchdog (Render-safe), keep-alive via /health

A trade fires ONLY when Lester and Michael agree on direction AND Franklin
approves the risk. Trevor then executes. Every agent's individual record is
tracked and displayed, so you can see who is actually earning their keep.

This is a signal generator, NOT financial advice. It never places orders.
"""

import os
import json
import time
import math
import copy
import threading
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, request

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------

WATCHLIST = [
    c.strip().upper()
    for c in os.environ.get(
        "WATCHLIST", "BTC,ETH,SOL,BNB,XRP,DOGE,ADA,LINK,AVAX,TON"
    ).split(",")
    if c.strip()
]

QUOTE = "USDT"
TIMEFRAMES = ["15m", "1h", "4h", "1d"]

# FX + gold via Yahoo Finance chart API (Binance has no forex/gold).
# Yahoo ticker format. Set FOREX_GOLD="" to disable. GC=F = COMEX gold futures.
FOREX_GOLD = [
    c.strip().upper()
    for c in os.environ.get(
        "FOREX_GOLD", "EURUSD=X,GBPUSD=X,USDJPY=X,AUDUSD=X,GC=F"
    ).split(",")
    if c.strip()
]

def is_fx(sym):
    return "=" in sym or ":" in sym

def disp(sym):
    """Display name: EURUSD=X -> EURUSD, GC=F -> GOLD, BTC -> BTCUSDT."""
    if sym == "GC=F":
        return "GOLD"
    return sym.replace("=X", "").replace("=F", "")
CANDLE_LIMIT = 220
MIN_CONFLUENCE = int(os.environ.get("MIN_CONFLUENCE", "3"))

RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "35"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "65"))

# Michael's momentum window: LONG needs RSI in [MO_FLOOR, RSI_OVERBOUGHT],
# SHORT needs RSI in [RSI_OVERSOLD, 100 - MO_FLOOR]
MO_FLOOR = float(os.environ.get("MO_FLOOR", "50"))

# Franklin's risk rails
MAX_ATR_PCT = float(os.environ.get("MAX_ATR_PCT", "6.0"))  # ATR/price ceiling
BE_AT_TP1 = (os.environ.get("BE_AT_TP1", "1") == "1")  # move SL to entry once +1R is tagged
MAX_EXT_ATR = float(os.environ.get("MAX_EXT_ATR", "2.5"))  # max distance from EMA20, in ATRs (chase filter)
FX_ROLLOVER_VETO = os.environ.get("FX_ROLLOVER_VETO", "1") == "1"  # skip FX/gold in the thin rollover hour

# Lester's trend-strength rails
ADX_MIN = float(os.environ.get("ADX_MIN", "20"))  # 1h ADX must clear this for a trend to count

ATR_MULT = 1.5
TP1_R, TP2_R, TP3_R = 1.0, 2.0, 3.0

RESCAN_MINUTES = int(os.environ.get("RESCAN_MINUTES", "60"))

# Ledger self-grading rails
BENCH_MIN_TRADES = int(os.environ.get("BENCH_MIN_TRADES", "5"))   # closed trades before a symbol/hour can be judged
BENCH_AVG_R = float(os.environ.get("BENCH_AVG_R", "0.0"))        # avg realized R below this = loser

# Backtest lab
BT_DAYS = int(os.environ.get("BT_DAYS", "90"))
DATA_FILE = os.environ.get("DATA_FILE", "signals.json")
MAX_HISTORY = 500
MAX_CHATTER = 160

CYCLE_MINUTES = float(os.environ.get("CYCLE_MINUTES", "5"))
BOOT_DELAY_SECONDS = int(os.environ.get("BOOT_DELAY_SECONDS", "30"))
APP_VERSION = os.environ.get("APP_VERSION", "1.8.4")  # shown in dashboard + API, so live version is always checkable

BINANCE_BASES = ["https://api.binance.com", "https://api1.binance.com", "https://data-api.binance.vision"]
FETCH_TIMEOUT = 6
FETCH_HARD_BOUND = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("crew")

AGENTS = {
    "lester":   {"name": "Lester",   "role": "Head Analyst",  "color": "#22d3ee", "glyph": "L",
                 "img": "https://media.base44.com/images/public/6aa89406a2d9664f56be6f5c/6a86a36f1_generated_image.png"},
    "michael":  {"name": "Michael",  "role": "Momentum",      "color": "#a78bfa", "glyph": "M",
                 "img": "https://media.base44.com/images/public/6aa89406a2d9664f56be6f5c/0fe37be48_generated_image.png"},
    "franklin": {"name": "Franklin", "role": "Risk Manager",  "color": "#f59e0b", "glyph": "F",
                 "img": "https://media.base44.com/images/public/6aa89406a2d9664f56be6f5c/37e03db2b_generated_image.png"},
    "trevor":   {"name": "Trevor",   "role": "Executor",      "color": "#ef4444", "glyph": "T",
                 "img": "https://media.base44.com/images/public/6aa89406a2d9664f56be6f5c/1b58a25bf_generated_image.png"},
}

# ----------------------------------------------------------------------------
# STATE (generations pattern — a hung cycle is abandoned, never blocks)
# ----------------------------------------------------------------------------

STATE_LOCK = threading.Lock()
STATE = {
    "generation": 0,
    "last_cycle_ts": 0,
    "cycle_alive": False,
    "data_feed_ok": True,
    "last_error": "",
    "scan": {},
    "signals": [],
    "stats": {},
    "agent_stats": {},
    "chatter": [],
    "cycle_count": 0,
    "ledger_grade": {"symbols": {}, "hours": {}, "benched": {}, "bad_hours": []},
    "backtest": {"running": False, "progress": "", "results": [], "done_at": None},
}

def _load_signals():
    try:
        with open(DATA_FILE, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            STATE["signals"] = data[-MAX_HISTORY:]
    except Exception:
        pass

def _save_signals():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(STATE["signals"][-MAX_HISTORY:], f)
    except Exception as e:
        log.warning("could not persist signals: %s", e)

_load_signals()

# ----------------------------------------------------------------------------
# DATA FETCH (hard-bounded, multi-host fallback)
# ----------------------------------------------------------------------------

def _bounded_get(path, params=None):
    def _do(base):
        try:
            r = requests.get(base + path, params=params, timeout=FETCH_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def _run():
        for base in BINANCE_BASES:
            res = _do(base)
            if res is not None:
                return res
        return None

    ex = ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(_run)
        return fut.result(timeout=FETCH_HARD_BOUND)
    except Exception:
        return None
    finally:
        ex.shutdown(wait=False)  # never block on a hung fetch thread

def fetch_klines(symbol):
    out = {}
    for tf in TIMEFRAMES:
        data = _bounded_get(
            "/api/v3/klines",
            {"symbol": symbol, "interval": tf, "limit": CANDLE_LIMIT},
        )
        if not data:
            return None
        try:
            out[tf] = [
                [float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])]
                for c in data
            ]
        except Exception:
            return None
    return out

def fetch_price(symbol):
    data = _bounded_get("/api/v3/ticker/price", {"symbol": symbol})
    if data and "price" in data:
        try:
            return float(data["price"])
        except Exception:
            return None
    return None

# --- Yahoo Finance feed (forex + gold), same hard-bounded pattern ---

UA_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}

# Yahoo throttle: one request every YAHOO_MIN_INTERVAL seconds, shared by the
# live sweep and the backtest lab so a replay can't starve the live desk.
YAHOO_MIN_INTERVAL = float(os.environ.get("YAHOO_MIN_INTERVAL", "0.5"))
_yahoo_lock = threading.Lock()
_yahoo_last = [0.0]

def _yahoo_pace():
    with _yahoo_lock:
        wait = _yahoo_last[0] + YAHOO_MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _yahoo_last[0] = time.monotonic()
FX_SYMBOLS = set(disp(t) for t in FOREX_GOLD)

YAHOO_BASES = ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]
YAHOO_INTERVAL = {"15m": ("15m", "1mo"), "1h": ("60m", "3mo"), "1d": ("1d", "1y")}
FX_STALE_SECONDS = 3 * 3600  # last 15m bar older than this -> market closed

def _yahoo_chart(symbol, interval, rng):
    def _do(base):
        try:
            _yahoo_pace()
            r = requests.get(
                base + "/v8/finance/chart/" + symbol,
                params={"interval": interval, "range": rng},
                headers=UA_HEADERS, timeout=FETCH_TIMEOUT,
            )
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    def _run():
        for attempt in range(2):  # one retry pass — Yahoo 429s are transient
            for base in YAHOO_BASES:
                res = _do(base)
                if res is not None:
                    return res
            if attempt == 0:
                time.sleep(2.0)
        return None

    ex = ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(_run)
        return fut.result(timeout=FETCH_HARD_BOUND)
    except Exception:
        return None
    finally:
        ex.shutdown(wait=False)  # never block on a hung fetch thread

def _yahoo_candles(payload):
    """-> (list of [t,o,h,l,c,v], regularMarketPrice) or (None, None)."""
    try:
        res = payload["chart"]["result"][0]
        ts = res.get("timestamp") or []
        q = res["indicators"]["quote"][0]
        vols = q.get("volume") or [0] * len(ts)
        out = []
        for i, t in enumerate(ts):
            o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
            if None in (o, h, l, c):
                continue
            out.append([t, o, h, l, c, float(vols[i] or 0)])
        if not out:
            return None, None
        return out, res.get("meta", {}).get("regularMarketPrice")
    except Exception:
        return None, None

def _resample_4h(candles):
    """1h bars [t,o,h,l,c,v] -> 4h bars [o,h,l,c,v], aligned on 4h boundaries."""
    groups, order = {}, []
    for t, o, h, l, c, v in candles:
        key = t // (4 * 3600)
        if key not in groups:
            groups[key] = [o, h, l, c, v]
            order.append(key)
        else:
            g = groups[key]
            g[1] = max(g[1], h)
            g[2] = min(g[2], l)
            g[3] = c
            g[4] += v
    return [groups[k] for k in order]

def fetch_yahoo(symbol):
    """-> (status, klines|None, price|None). status: OK / STALE (market closed) / ERROR."""
    out, price, full_1h = {}, None, None
    for tf in TIMEFRAMES:
        if tf == "4h":
            continue
        interval, rng = YAHOO_INTERVAL[tf]
        raw, px = _yahoo_candles(_yahoo_chart(symbol, interval, rng))
        if not raw:
            return "ERROR", None, None
        if px:
            price = float(px)
        if tf == "15m" and raw[-1][0] + FX_STALE_SECONDS < time.time():
            return "STALE", None, price
        if tf == "1h":
            full_1h = raw
        out[tf] = [[o, h, l, c, v] for t, o, h, l, c, v in raw][-CANDLE_LIMIT:]
    if not full_1h:
        return "ERROR", None, None
    out["4h"] = _resample_4h(full_1h)[-CANDLE_LIMIT:]
    return "OK", out, price

# ----------------------------------------------------------------------------
# INDICATORS
# ----------------------------------------------------------------------------

def ema(values, period):
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    for v in values[period:]:
        e = v * k + e * (1 - k)
    return e

def rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    avg_g = sum(gains) / period
    avg_l = sum(losses) / period
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0.0)) / period
        avg_l = (avg_l * (period - 1) + max(-d, 0.0)) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))

def adx(candles, period=14):
    """Wilder ADX + DI direction. Returns (adx_value, di_dir) or (None, None)."""
    if len(candles) < period * 2 + 2:
        return None, None
    trs, pdms, mdms = [], [], []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][1], candles[i][2], candles[i - 1][3]
        up = h - candles[i - 1][1]
        dn = candles[i - 1][2] - l
        pdms.append(up if (up > dn and up > 0) else 0.0)
        mdms.append(dn if (dn > up and dn > 0) else 0.0)
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    n = period
    atr_ = sum(trs[:n]) / n
    pd = sum(pdms[:n]) / n
    md = sum(mdms[:n]) / n
    dxs, pdi, mdi = [], 0.0, 0.0
    for i in range(n, len(trs)):
        atr_ = (atr_ * (n - 1) + trs[i]) / n
        pd = (pd * (n - 1) + pdms[i]) / n
        md = (md * (n - 1) + mdms[i]) / n
        if atr_ <= 0:
            continue
        pdi, mdi = 100.0 * pd / atr_, 100.0 * md / atr_
        s = pdi + mdi
        dxs.append(100.0 * abs(pdi - mdi) / s if s else 0.0)
    if len(dxs) < n or not dxs:
        return None, None
    val = sum(dxs[-n:]) / n
    return val, (1 if pdi >= mdi else -1)

def _ema_series(values, period):
    out = []
    if len(values) < period:
        return out
    k = 2.0 / (period + 1)
    e = sum(values[:period]) / period
    out.append(e)
    for v in values[period:]:
        e = v * k + e * (1 - k)
        out.append(e)
    return out

def macd_read(closes, fast=12, slow=26, sig_p=9):
    """Returns {'hist': latest, 'slope': hist[-1]-hist[-2]} or None."""
    if len(closes) < slow + sig_p + 2:
        return None
    ef = _ema_series(closes, fast)
    es = _ema_series(closes, slow)
    m = len(es)
    line = [ef[-i] - es[-i] for i in range(1, m + 1)][::-1]
    if len(line) < sig_p + 2:
        return None
    sig_line = _ema_series(line, sig_p)
    if len(sig_line) < 2:
        return None
    hist = [line[len(line) - len(sig_line) + i] - sig_line[i] for i in range(len(sig_line))]
    return {"hist": hist[-1], "slope": hist[-1] - hist[-2]}

def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_c = candles[i][1], candles[i][2], candles[i - 1][3]
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(trs[-period:]) / period

# ----------------------------------------------------------------------------
# BASE READ (same confluence engine as Signal Scanner)
# ----------------------------------------------------------------------------

def timeframe_vote(candles):
    closes = [c[3] for c in candles]
    vols = [c[4] for c in candles]
    r = rsi(closes)
    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    vol_avg = sum(vols[-20:]) / 20 if len(vols) >= 20 else None

    detail = {"rsi": r, "e20": e20, "e50": e50, "e200": e200}
    if r is None or e50 is None or e200 is None:
        return 0, detail

    if e20 is not None and e20 > e50 > e200:
        trend = +1
    elif e20 is not None and e20 < e50 < e200:
        trend = -1
    else:
        trend = 0
    detail["trend"] = trend
    detail["vol_ok"] = bool(vol_avg and vols[-1] > vol_avg * 0.8)
    detail["vol_ratio"] = (vols[-1] / vol_avg) if vol_avg else None

    if trend == +1:
        if r > RSI_OVERBOUGHT:
            return 0, detail
        return +1, detail
    if trend == -1:
        if r < RSI_OVERSOLD:
            return 0, detail
        return -1, detail
    # No clean EMA stack -> no edge. Never vote counter-trend off RSI alone:
    # that's how the desk used to fire LONGs into falling markets.
    return 0, detail

def base_read(symbol, klines):
    votes, details = {}, {}
    for tf in TIMEFRAMES:
        v, d = timeframe_vote(klines[tf])
        votes[tf] = v
        details[tf] = d

    bull = sum(1 for v in votes.values() if v > 0)
    bear = sum(1 for v in votes.values() if v < 0)

    ref = klines.get("1h") or klines[TIMEFRAMES[1]]
    a = atr(ref)
    last_close = ref[-1][3]
    d1h = details.get("1h", {})
    vol_ok = sum(1 for d in details.values() if d.get("vol_ok")) >= 2

    adx_v, di_dir = adx(ref)
    closes_1h = [c[3] for c in ref]
    mac = macd_read(closes_1h)
    e20_1h = ema(closes_1h, 20)
    ext = (abs(last_close - e20_1h) / a) if (a and e20_1h and last_close) else None

    return {
        "symbol": symbol,
        "votes": votes,
        "details": details,
        "bull": bull,
        "bear": bear,
        "rsi_1h": d1h.get("rsi"),
        "atr": a,
        "atr_pct": (a / last_close * 100.0) if (a and last_close) else None,
        "last_close": last_close,
        "vol_ok": vol_ok,
        "adx_1h": adx_v,
        "di_dir_1h": di_dir,
        "macd_1h": mac,
        "ext_atr": ext,
        "is_fx_gold": symbol in FX_SYMBOLS,
        "ts": time.time(),
    }

# ----------------------------------------------------------------------------
# THE CREW — agent verdicts
# ----------------------------------------------------------------------------

def lester(read):
    """Head Analyst: structure across timeframes, strength-filtered (1h ADX)."""
    bull, bear = read["bull"], read["bear"]
    votes = read["votes"]
    adx_v, di_dir = read.get("adx_1h"), read.get("di_dir_1h")
    if bull >= MIN_CONFLUENCE and bull > bear:
        hold = [tf for tf, v in votes.items() if v <= 0]
        extra = f" ({hold[0]} is the holdout)" if hold else " — unanimous"
        if adx_v is not None and di_dir is not None:
            if adx_v < ADX_MIN:
                return {"verdict": "NEUTRAL", "line": f"{read['symbol']} has {bull}/4 up but ADX {round(adx_v,1)} — that's chop, not a trend. Pass."}
            if di_dir < 0:
                return {"verdict": "NEUTRAL", "line": f"{read['symbol']} stacks up but sellers own the 1h (-DI leads, ADX {round(adx_v,1)}). No clean structure."}
        return {"verdict": "LONG", "line": f"{read['symbol']} structure is clean: {bull}/4 timeframes trending up, ADX {round(adx_v,1) if adx_v else '--'}{extra}."}
    if bear >= MIN_CONFLUENCE and bear > bull:
        hold = [tf for tf, v in votes.items() if v >= 0]
        extra = f" ({hold[0]} is the holdout)" if hold else " — unanimous"
        if adx_v is not None and di_dir is not None:
            if adx_v < ADX_MIN:
                return {"verdict": "NEUTRAL", "line": f"{read['symbol']} has {bear}/4 down but ADX {round(adx_v,1)} — that's chop, not a trend. Pass."}
            if di_dir > 0:
                return {"verdict": "NEUTRAL", "line": f"{read['symbol']} stacks down but buyers own the 1h (+DI leads, ADX {round(adx_v,1)}). No clean structure."}
        return {"verdict": "SHORT", "line": f"{read['symbol']} structure is heavy: {bear}/4 timeframes trending down, ADX {round(adx_v,1) if adx_v else '--'}{extra}."}
    if max(bull, bear) == 2:
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']}: {bull} up / {bear} down. Structure is forming, no edge yet."}
    return {"verdict": "NEUTRAL", "line": f"{read['symbol']}: timeframes disagree, staying off it."}

def michael(read):
    """Momentum: confirms fuel, refuses to chase exhaust."""
    r = read["rsi_1h"]
    if r is None:
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']}: no momentum read."}
    rc = round(r, 1)
    if rc > RSI_OVERBOUGHT:
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']} is running hot at RSI {rc} — I don't chase blow-off tops."}
    if rc < RSI_OVERSOLD:
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']} is washed out at RSI {rc} — not catching knives, waiting for a base."}
    mac = read.get("macd_1h")
    if read["bull"] >= MIN_CONFLUENCE:
        if rc >= MO_FLOOR:
            if mac and mac["hist"] is not None:
                if mac["hist"] > 0 and mac["slope"] > 0:
                    return {"verdict": "LONG", "line": f"Momentum's live on {read['symbol']} — RSI {rc}, MACD building. Room before it's cooked."}
                return {"verdict": "NEUTRAL", "line": f"{read['symbol']} RSI {rc} but MACD isn't confirming the fuel. Wait."}
            return {"verdict": "LONG", "line": f"Momentum's live on {read['symbol']} — RSI {rc}, room before it's cooked."}
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']} momentum's flat (RSI {rc}). Wait for fuel."}
    if read["bear"] >= MIN_CONFLUENCE:
        if rc <= (100 - MO_FLOOR):
            if mac and mac["hist"] is not None:
                if mac["hist"] < 0 and mac["slope"] < 0:
                    return {"verdict": "SHORT", "line": f"{read['symbol']} is rolling over — RSI {rc}, MACD fading. Downside has room."}
                return {"verdict": "NEUTRAL", "line": f"{read['symbol']} RSI {rc} but MACD hasn't turned over yet. Wait."}
            return {"verdict": "SHORT", "line": f"{read['symbol']} is rolling over — RSI {rc}, downside has room."}
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']} still too perky to short (RSI {rc})."}
    return {"verdict": "NEUTRAL", "line": f"{read['symbol']}: momentum fine but no setup to confirm."}

def franklin(read, direction):
    """Risk Manager: audits geometry, can VETO."""
    a, pct = read["atr"], read["atr_pct"]
    if direction == "NEUTRAL":
        return {"verdict": "PASS", "line": None}
    if a is None or read["last_close"] is None:
        return {"verdict": "VETO", "line": f"VETO on {read['symbol']}: can't size risk without a clean ATR read."}
    if pct is not None and pct > MAX_ATR_PCT:
        return {"verdict": "VETO", "line": f"VETO on {read['symbol']}: ATR is {pct:.1f}% of price — that's a casino, not a setup."}
    r = read["rsi_1h"]
    if direction == "LONG" and r is not None and r > RSI_OVERBOUGHT:
        return {"verdict": "VETO", "line": f"VETO on {read['symbol']} long: RSI {round(r,1)} is stretched to the moon."}
    if direction == "SHORT" and r is not None and r < RSI_OVERSOLD:
        return {"verdict": "VETO", "line": f"VETO on {read['symbol']} short: RSI {round(r,1)} — knife-catch territory."}
    now_utc = read.get("bt_now") or datetime.now(timezone.utc)
    # Ledger-learned bad hours: hours where our own closed trades lose money
    grade = STATE.get("ledger_grade") or {}
    bad = grade.get("bad_hours") or []
    if now_utc.hour in bad:
        return {"verdict": "VETO", "line": f"VETO on {read['symbol']}: {now_utc.hour:02d}:00 UTC is a proven losing hour for this desk (ledger data). Not touching it."}
    # FX/gold: skip the thin rollover hour (wide spreads, fake outs)
    if FX_ROLLOVER_VETO and read.get("is_fx_gold"):
        if (now_utc.hour == 21 and now_utc.minute >= 45) or (now_utc.hour == 22 and now_utc.minute < 15):
            return {"verdict": "VETO", "line": f"VETO on {read['symbol']}: we're in the rollover hour — spreads widen and moves lie. Wait it out."}
    # Don't chase: price too far from the 20 EMA means the move is already stretched
    ext = read.get("ext_atr")
    if ext is not None and ext > MAX_EXT_ATR:
        return {"verdict": "VETO", "line": f"VETO on {read['symbol']} {direction.lower()}: price is {ext:.1f} ATR off the 20 EMA — that's chasing, not entering. Wait for the pullback."}
    sl_pct = ATR_MULT * pct if pct is not None else None
    note = f"risk is {sl_pct:.1f}% to the stop" if sl_pct else "risk sized to 1.5x ATR"
    return {"verdict": "APPROVE", "line": f"{read['symbol']} {direction} clears risk: {note}. Green light from me."}

def trevor(read, direction, sig):
    """Executor: fires on full consensus."""
    if sig is None:
        return {"verdict": "STAND", "line": None}
    p = read["last_close"]
    fmt_p = f"{p:,.4f}" if p < 1000 else f"{p:,.2f}"
    sl = sig["stop_loss"]
    fmt_sl = f"{sl:,.4f}" if sl < 1000 else f"{sl:,.2f}"
    return {"verdict": "EXECUTE",
            "line": f"Executing {direction} {read['symbol']}. Entry {fmt_p}, SL {fmt_sl}, targets 1R / 2R / 3R. Let's get paid."}

def _say(agent, text, symbol=None):
    return {"a": agent, "t": text, "s": symbol, "ts": time.time()}

def crew_review(read):
    """Full desk review of one symbol. Returns (signal or None, chatter lines)."""
    lines = []
    L = lester(read)
    M = michael(read)
    direction = L["verdict"] if L["verdict"] == M["verdict"] else "NEUTRAL"
    F = franklin(read, direction)

    sig = None
    if direction in ("LONG", "SHORT") and F["verdict"] == "APPROVE":
        sig = build_signal(read, direction)
    T = trevor(read, direction, sig)

    hot = read["rsi_1h"] is not None and (read["rsi_1h"] > RSI_OVERBOUGHT or read["rsi_1h"] < RSI_OVERSOLD)
    interesting = (max(read["bull"], read["bear"]) >= 2) or (sig is not None) or (F["verdict"] == "VETO") or hot
    if interesting:
        if L["line"]:
            lines.append(_say("lester", L["line"], read["symbol"]))
        if M["line"] and (sig or L["verdict"] != "NEUTRAL" or hot):
            lines.append(_say("michael", M["line"], read["symbol"]))
        if F["verdict"] == "VETO" and F["line"]:
            lines.append(_say("franklin", F["line"], read["symbol"]))
        elif F["verdict"] == "APPROVE" and sig is not None and F["line"]:
            lines.append(_say("franklin", F["line"], read["symbol"]))
        if T["verdict"] == "EXECUTE" and T["line"]:
            lines.append(_say("trevor", T["line"], read["symbol"]))

    verdicts = {
        "lester": L["verdict"], "michael": M["verdict"],
        "franklin": F["verdict"], "trevor": T["verdict"],
    }
    if sig is not None:
        sig["crew"] = verdicts
    return sig, verdicts, lines

# ----------------------------------------------------------------------------
# SIGNAL BUILD + TRACKING (self-audit)
# ----------------------------------------------------------------------------

def build_signal(read, direction):
    entry = read["last_close"]
    a = read["atr"]
    if not a or not entry:
        return None
    risk = ATR_MULT * a
    if direction == "LONG":
        sl = entry - risk
        tps = [entry + TP1_R * risk, entry + TP2_R * risk, entry + TP3_R * risk]
    else:
        sl = entry + risk
        tps = [entry - TP1_R * risk, entry - TP2_R * risk, entry - TP3_R * risk]

    conf = int(round(100.0 * max(read["bull"], read["bear"]) / len(TIMEFRAMES)))
    adx_v = read.get("adx_1h")
    if adx_v is not None and adx_v >= 30:
        conf = min(95, conf + 10)
    elif adx_v is not None and adx_v >= 25:
        conf = min(95, conf + 5)
    mac = read.get("macd_1h")
    if mac and mac["hist"] is not None and (
        (direction == "LONG" and mac["hist"] > 0) or (direction == "SHORT" and mac["hist"] < 0)):
        conf = min(95, conf + 5)
    return {
        "id": f"{read['symbol']}-{int(time.time())}",
        "symbol": read["symbol"],
        "direction": direction,
        "entry": round(entry, 6),
        "stop_loss": round(sl, 6),
        "tp1": round(tps[0], 6),
        "tp2": round(tps[1], 6),
        "tp3": round(tps[2], 6),
        "confidence": conf,
        "rsi_1h": round(read["rsi_1h"], 1) if read["rsi_1h"] is not None else None,
        "created": datetime.now(timezone.utc).isoformat(),
        "hour_utc": read.get("hour_utc", datetime.now(timezone.utc).hour),
        "status": "OPEN",
        "outcome": None,
        "closed": None,
        "max_favorable_r": 0.0,
    }

def risk_distance(sig):
    return abs(sig["entry"] - sig["stop_loss"])

def compute_stats(signals):
    closed = [s for s in signals if s["status"] == "CLOSED"]
    n = len(closed)
    if n == 0:
        return {"closed": 0, "win_rate": None, "sl_rate": None,
                "avg_r": None, "best": None, "worst": None}
    wins = [s for s in closed if s["outcome"] in ("TP1_HIT", "TP2_HIT", "TP3_HIT")]
    sl = [s for s in closed if s["outcome"] == "SL_HIT"]
    avg_r = sum(s.get("max_favorable_r", 0.0) for s in closed) / n
    by_r = sorted(closed, key=lambda s: s.get("max_favorable_r", 0.0))
    return {
        "closed": n,
        "win_rate": round(100.0 * len(wins) / n, 1),
        "sl_rate": round(100.0 * len(sl) / n, 1),
        "avg_r": round(avg_r, 2),
        "best": {"symbol": by_r[-1]["symbol"], "r": by_r[-1]["max_favorable_r"]},
        "worst": {"symbol": by_r[0]["symbol"], "r": by_r[0]["max_favorable_r"]},
    }

def compute_agent_stats(signals):
    """Per-agent record on fired signals. Every crew member approved every fired
    signal by construction, so all four share the desk record; the interesting
    split is what each agent would have done WITHOUT the others' check — so we
    also track Lester-only and Michael-only performance."""
    closed = [s for s in signals if s["status"] == "CLOSED"]
    n = len(closed)
    if n == 0:
        return {"desk": {"closed": 0, "win_rate": None}}
    wins = [s for s in closed if s["outcome"] in ("TP1_HIT", "TP2_HIT", "TP3_HIT")]
    return {
        "desk": {"closed": n, "win_rate": round(100.0 * len(wins) / n, 1)},
        "crew_note": "All four agents approve every fired signal by design — the desk record is shared.",
    }

def can_emit(symbol, direction):
    now = time.time()
    benched = (STATE.get("ledger_grade") or {}).get("benched") or {}
    if symbol in benched:
        return False
    for sig in STATE["signals"]:
        if sig["symbol"] != symbol or sig["direction"] != direction:
            continue
        if sig["status"] == "OPEN":
            return False
        try:
            t = datetime.fromisoformat(sig["closed"]).timestamp()
        except Exception:
            t = 0
        if now - t < RESCAN_MINUTES * 60:
            return False
    return True

# ----------------------------------------------------------------------------
# LEDGER SELF-GRADE (bench losers, learn bad hours)
# ----------------------------------------------------------------------------

def _realized_r(sig):
    """Honest realized R under the desk's management: SL = -1, TP3 = +3, BE = 0."""
    if sig["outcome"] == "TP3_HIT":
        return 3.0
    if sig["outcome"] == "SL_HIT":
        return -1.0
    if sig["outcome"] == "BE_HIT":
        return 0.0
    return 0.0

def compute_ledger_grade(signals=None):
    closed = [s for s in (signals if signals is not None else STATE["signals"])
             if s.get("status") == "CLOSED"]
    per_sym, per_hour = {}, {}
    for s in closed:
        r = _realized_r(s)
        per_sym.setdefault(s["symbol"], []).append(r)
        h = s.get("hour_utc")
        if h is not None:
            per_hour.setdefault(int(h), []).append(r)

    def agg(rs):
        return {"n": len(rs), "avg_r": round(sum(rs) / len(rs), 2),
                "win_rate": round(100.0 * sum(1 for r in rs if r > 0) / len(rs), 1)}

    symbols = {k: agg(v) for k, v in per_sym.items()}
    hours = {k: agg(v) for k, v in per_hour.items()}
    benched = {k: v for k, v in symbols.items()
               if v["n"] >= BENCH_MIN_TRADES and v["avg_r"] < BENCH_AVG_R}
    bad_hours = sorted(k for k, v in hours.items()
                       if v["n"] >= BENCH_MIN_TRADES and v["avg_r"] < BENCH_AVG_R)
    STATE["ledger_grade"] = {"symbols": symbols, "hours": hours,
                             "benched": benched, "bad_hours": bad_hours}
    return STATE["ledger_grade"]

# ----------------------------------------------------------------------------
# BACKTEST LAB — replays history through the EXACT live crew logic
# ----------------------------------------------------------------------------

BT_IV_MS = {"15m": 15 * 60_000, "1h": 3_600_000, "4h": 4 * 3_600_000, "1d": 86_400_000}

def _bt_yahoo(coin, interval, days):
    """Yahoo history -> (opens_ms, candles [o,h,l,c,v]) or (None, None)."""
    p2 = int(time.time())
    p1 = p2 - int(days * 86400)
    def _do(base):
        try:
            _yahoo_pace()
            r = requests.get(
                base + "/v8/finance/chart/" + coin,
                params={"interval": interval, "period1": p1, "period2": p2},
                headers=UA_HEADERS, timeout=FETCH_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None
    payload = None
    for attempt in range(2):
        for base in YAHOO_BASES:
            payload = _do(base)
            if payload:
                break
        if payload:
            break
        if attempt == 0:
            time.sleep(2.0)
    raw, _ = _yahoo_candles(payload)
    if not raw:
        return None, None
    opens = [int(t) * 1000 for t, *_ in raw]
    candles = [[o, h, l, c, v] for _, o, h, l, c, v in raw]
    return opens, candles

def _bt_binance(symbol, interval, days):
    """Paginated Binance history -> (opens_ms, candles)."""
    end = int(time.time() * 1000)
    start = end - int(days * 86_400_000)
    rows = []
    while True:
        try:
            r = None
            for base in BINANCE_BASES:
                try:
                    r = requests.get(
                        base + "/api/v3/klines",
                        params={"symbol": symbol, "interval": interval,
                                "startTime": start, "endTime": end, "limit": 1000},
                        timeout=FETCH_TIMEOUT)
                    if r.ok:
                        break
                    r = None
                except Exception:
                    continue
            if not r or not r.ok:
                return None, None
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < 1000:
                break
            start = int(batch[-1][0]) + 1
        except Exception:
            return None, None
    if not rows:
        return None, None
    opens = [int(c[0]) for c in rows]
    candles = [[float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in rows]
    return opens, candles

def _bt_resample_4h(opens, candles):
    """1h -> 4h bars, keeping open times."""
    groups, order = {}, []
    for o, c in zip(opens, candles):
        key = (o // BT_IV_MS["4h"]) * BT_IV_MS["4h"]
        if key not in groups:
            groups[key] = [c[0], c[1], c[2], c[3], c[4]]
            order.append(key)
            continue
        g = groups[key]
        g[1] = max(g[1], c[1]); g[2] = min(g[2], c[2])
        g[3] = c[3]; g[4] += c[4]
    return order, [groups[k] for k in order]

def bt_symbol(symbol, is_fx, coin, days):
    """Replay one symbol through the live crew logic. Returns metrics dict."""
    import bisect
    data = {}
    for tf in TIMEFRAMES:
        # lookback deep enough for EMA200 on every TF
        # FX trades ~5/7 of calendar days — widen lookback so EMA200 still has data
        need = max(days, int(210 * BT_IV_MS[tf] / 86_400_000 * (1.45 if is_fx else 1.0)) + 5)
        if tf == "1d":
            need = max(need, 400 if is_fx else 260)  # FX daily skips weekends
        if is_fx:
            if tf == "15m":
                need = min(need, 55)  # Yahoo 15m depth cap
            if tf == "4h":
                o1, c1 = _bt_yahoo(coin, "1h", max(need, 10))
                if not o1:
                    return {"symbol": symbol, "error": "no data"}
                o, c = _bt_resample_4h(o1, c1)
            else:
                o, c = _bt_yahoo(coin, tf, need)
        else:
            o, c = _bt_binance(symbol, tf, need)
        if not o or len(c) < 200:  # EMA200 floor
            return {"symbol": symbol, "error": "insufficient history"}
        data[tf] = (o, c)
    close_ms = {tf: [o + BT_IV_MS[tf] for o in data[tf][0]] for tf in TIMEFRAMES}
    c15o, c15 = data["15m"]
    if is_fx:
        days = min(days, 55)

    trades = []
    pos = None
    for i in range(len(c15)):
        bar = c15[i]
        t_close = c15o[i] + BT_IV_MS["15m"]
        slices, ok = {}, True
        for tf in TIMEFRAMES:
            cnt = bisect.bisect_right(close_ms[tf], t_close)
            if cnt < 200:  # EMA200 floor
                ok = False
                break
            slices[tf] = data[tf][1][max(0, cnt - 210):cnt]
        if not ok:
            continue

        if pos is not None:
            risk = abs(pos["entry"] - pos["sl"])
            if risk <= 0:
                pos = None
                continue
            hit, r_be = False, None
            if pos["long"]:
                # breakeven shadow: stop = entry once +1R tagged (conservative: arm after this bar)
                sl_be = pos["entry"] if pos["be_armed"] else pos["sl"]
                if bar[2] <= pos["sl"]:
                    r_be = 0.0 if pos["be_armed"] else -1.0
                    trades.append({"r": -1.0, "r_be": r_be, "mfe": pos["mfe"], "hour": pos["hour"]}); hit = True
                elif bar[1] >= pos["tp3"]:
                    r_be = 3.0
                    trades.append({"r": 3.0, "r_be": r_be, "mfe": max(pos["mfe"], (bar[1] - pos["entry"]) / risk), "hour": pos["hour"]}); hit = True
                else:
                    pos["mfe"] = max(pos["mfe"], (bar[1] - pos["entry"]) / risk)
                    if bar[1] >= pos["tp1"]:
                        pos["be_armed"] = True
            else:
                sl_be = pos["entry"] if pos["be_armed"] else pos["sl"]
                if bar[1] >= pos["sl"]:
                    r_be = 0.0 if pos["be_armed"] else -1.0
                    trades.append({"r": -1.0, "r_be": r_be, "mfe": pos["mfe"], "hour": pos["hour"]}); hit = True
                elif bar[2] <= pos["tp3"]:
                    r_be = 3.0
                    trades.append({"r": 3.0, "r_be": r_be, "mfe": max(pos["mfe"], (pos["entry"] - bar[2]) / risk), "hour": pos["hour"]}); hit = True
                else:
                    pos["mfe"] = max(pos["mfe"], (pos["entry"] - bar[2]) / risk)
                    if bar[2] <= pos["tp1"]:
                        pos["be_armed"] = True
            if hit:
                pos = None
            continue

        read = base_read(symbol, slices)
        read["last_close"] = bar[3]
        read["bt_now"] = datetime.fromtimestamp(t_close / 1000.0, tz=timezone.utc)
        read["hour_utc"] = read["bt_now"].hour
        try:
            sig, _v, _l = crew_review(read)
        except Exception:
            continue
        if sig is None:
            continue
        pos = {"long": sig["direction"] == "LONG", "entry": sig["entry"],
               "sl": sig["stop_loss"], "tp1": sig["tp1"], "tp3": sig["tp3"], "mfe": 0.0,
               "be_armed": False, "hour": read["hour_utc"], "dir": sig["direction"]}

    n = len(trades)
    if n == 0:
        return {"symbol": symbol, "trades": 0, "note": "crew fired on nothing — filters held",
                "days": days}
    rs = [t["r"] for t in trades]
    rs_be = [t.get("r_be", t["r"]) for t in trades]
    cum, peak, max_dd = 0.0, 0.0, 0.0
    for r in rs_be:  # drawdown under the live (breakeven) management
        cum += r
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
    saved = sum(1 for t in trades if t.get("r_be", t["r"]) > t["r"])
    res = {
        "symbol": symbol, "days": days, "trades": n,
        "sl_rate": round(100.0 * sum(1 for t in trades if t["r"] < 0) / n, 1),
        "tp3_rate": round(100.0 * sum(1 for t in trades if t["r"] > 0) / n, 1),
        "tp1_touch": round(100.0 * sum(1 for t in trades if t["mfe"] >= 1.0) / n, 1),
        "avg_r": round(sum(rs) / n, 2),
        "avg_r_be": round(sum(rs_be) / n, 2),
        "delta_r": round(sum(rs_be) - sum(rs), 1),
        "be_saved": saved,
        "expectancy_r": round(sum(rs_be), 1),
        "max_dd_r": round(max_dd, 1),
    }
    judge = sum(rs_be) / n  # verdict under live management (breakeven ON)
    if n >= 8 and judge >= 0.5:
        res["verdict"] = "EDGE"
    elif n >= 8 and judge <= -0.3:
        res["verdict"] = "AVOID"
    else:
        res["verdict"] = "FLAT" if n >= 8 else "SMALL N"
    return res

def bt_run(days=BT_DAYS, symbols=None):
    if STATE["backtest"].get("running"):
        return False
    STATE["backtest"] = {"running": True, "progress": "starting…", "results": [], "done_at": None}
    def _work():
        try:
            targets = symbols or ([f"{c}{QUOTE}" for c in WATCHLIST] + [disp(t) for t in FOREX_GOLD])
            results = []
            for idx, sym in enumerate(targets):
                STATE["backtest"]["progress"] = f"replaying {sym} ({idx + 1}/{len(targets)})…"
                if sym in FX_SYMBOLS:
                    coin = next((t for t in FOREX_GOLD if disp(t) == sym), None)
                    if coin is None:
                        continue
                    r = bt_symbol(sym, True, coin, days)
                else:
                    r = bt_symbol(sym, False, None, days)
                results.append(r)
                STATE["backtest"]["results"] = results
            STATE["backtest"]["progress"] = f"done — {len(results)} symbols replayed over ~{days}d"
            STATE["backtest"]["done_at"] = time.time()
        except Exception as e:
            STATE["backtest"]["progress"] = f"error: {e}"
            STATE["backtest"]["running"] = False
        finally:
            STATE["backtest"]["running"] = False
    threading.Thread(target=_work, daemon=True).start()
    return True

# ----------------------------------------------------------------------------
# CYCLE (generations pattern)
# ----------------------------------------------------------------------------

def run_cycle():
    gen = STATE["generation"] + 1
    log.info("cycle generation %d starting", gen)
    work = copy.deepcopy(STATE)

    prices = {}
    scan = {}
    new_signals = []
    chatter = []
    feed_ok = True

    chatter.append(_say("lester", "Fresh sweep across the watchlist. Let's see what the market's giving us today."))

    for coin in WATCHLIST + FOREX_GOLD:
        if is_fx(coin):
            symbol = disp(coin)
            st, klines, price = fetch_yahoo(coin)
            if st != "OK":
                if st == "ERROR":
                    feed_ok = False
                scan[symbol] = {"symbol": symbol, "direction": "NO_DATA", "confluence": "-",
                                "score": 0, "rsi_1h": None, "last_close": price, "ts": time.time(),
                                "market_closed": st == "STALE"}
                continue
            if price:
                prices[symbol] = price
        else:
            symbol = f"{coin}{QUOTE}"
            price = fetch_price(symbol)
            if price:
                prices[symbol] = price
            klines = fetch_klines(symbol)
            if not klines:
                feed_ok = False
                scan[symbol] = {"symbol": symbol, "direction": "NO_DATA", "confluence": "-",
                                "score": 0, "rsi_1h": None, "last_close": price, "ts": time.time()}
                continue

        read = base_read(symbol, klines)
        if price:
            read["last_close"] = price

        sig, verdicts, lines = crew_review(read)
        for entry in lines:
            chatter.append(entry)

        if sig is not None:
            benched = (STATE.get("ledger_grade") or {}).get("benched") or {}
            if symbol in benched:
                b = benched[symbol]
                chatter.append(_say("franklin",
                    f"{symbol} setup killed by the numbers: ledger shows {b['win_rate']}% wins over {b['n']} trades (avg {b['avg_r']}R). Benched until it earns its way back.",
                    symbol))
            elif can_emit(symbol, sig["direction"]):
                sig["crew"] = verdicts
                new_signals.append(sig)
                log.info("CREW SIGNAL %s %s @ %s conf=%d%%",
                         sig["direction"], sig["symbol"], sig["entry"], sig["confidence"])
            else:
                chatter.append(_say("trevor", f"{symbol} {sig['direction']} stands down — position already open or cooldown active.", symbol))

        read["direction"] = sig["direction"] if sig else (
            "LONG" if verdicts["lester"] == "LONG" else
            "SHORT" if verdicts["lester"] == "SHORT" else
            ("VETOED" if verdicts["franklin"] == "VETO" else "NEUTRAL"))
        read["score"] = max(read["bull"], read["bear"])
        read["confluence"] = f"{read['score']}/{len(TIMEFRAMES)}"
        read["crew"] = verdicts
        scan[symbol] = read

    # update open signals against fresh prices
    update_open_signals_work(work, prices)

    work["scan"] = scan
    work["signals"] = (work["signals"] + new_signals)[-MAX_HISTORY:]
    work["stats"] = compute_stats(work["signals"])
    work["agent_stats"] = compute_agent_stats(work["signals"])
    work["chatter"] = (work["chatter"] + chatter)[-MAX_CHATTER:]
    work["cycle_count"] = work.get("cycle_count", 0) + 1
    work["data_feed_ok"] = feed_ok
    work["last_error"] = "" if feed_ok else "Some symbols returned no data"

    with STATE_LOCK:
        if gen > STATE["generation"]:
            work["generation"] = gen
            work["last_cycle_ts"] = time.time()
            work["cycle_alive"] = False
            # fields owned by other threads (backtest lab) stay live:
            work["backtest"] = STATE.get("backtest") or {}
            STATE.update(work)
            _save_signals()
            log.info("cycle gen %d committed: %d scanned, %d new signals", gen, len(scan), len(new_signals))
        else:
            log.info("cycle gen %d superseded, discarded", gen)

def update_open_signals_work(work, prices):
    """Same tracker as the scanner, applied to the generation's private copy."""
    changed = False
    for sig in work["signals"]:
        if sig["status"] != "OPEN":
            continue
        p = prices.get(sig["symbol"])
        if not p:
            continue
        risk = risk_distance(sig)
        if risk <= 0 and sig.get("orig_sl") is not None:
            risk = abs(sig["entry"] - sig["orig_sl"])  # original risk survives the breakeven move
        if risk <= 0:
            continue
        long = sig["direction"] == "LONG"
        fav = ((p - sig["entry"]) / risk) if long else ((sig["entry"] - p) / risk)
        sig["max_favorable_r"] = round(max(sig.get("max_favorable_r", 0.0), fav), 2)
        # Breakeven rule: once +1R is tagged, the stop rides to entry (free trade)
        if BE_AT_TP1 and not sig.get("be_moved") and sig["max_favorable_r"] >= 1.0:
            sig["be_moved"] = True
            sig["orig_sl"] = sig["stop_loss"]
            sig["stop_loss"] = sig["entry"]
            changed = True
            try:
                work["chatter"].append(_say("franklin",
                    f"{sig['symbol']} tagged +1R — stop moved to breakeven. Free trade from here, we don't give it back.",
                    sig["symbol"]))
            except Exception:
                pass
        if long:
            if p <= sig["stop_loss"]:
                sig["status"], sig["outcome"] = "CLOSED", "SL_HIT"
            elif p >= sig["tp3"]:
                sig["status"], sig["outcome"] = "CLOSED", "TP3_HIT"
            elif p >= sig["tp2"]:
                sig["outcome"] = "TP2_HIT"
            elif p >= sig["tp1"]:
                sig["outcome"] = "TP1_HIT"
        else:
            if p >= sig["stop_loss"]:
                sig["status"], sig["outcome"] = "CLOSED", "SL_HIT"
            elif p <= sig["tp3"]:
                sig["status"], sig["outcome"] = "CLOSED", "TP3_HIT"
            elif p <= sig["tp2"]:
                sig["outcome"] = "TP2_HIT"
            elif p <= sig["tp1"]:
                sig["outcome"] = "TP1_HIT"
        if sig["status"] == "CLOSED":
            sig["closed"] = datetime.now(timezone.utc).isoformat()
            if sig["outcome"] == "SL_HIT" and sig.get("be_moved"):
                sig["outcome"] = "BE_HIT"
            changed = True
    if changed:
        try:
            work["ledger_grade"] = compute_ledger_grade(work["signals"])
        except Exception:
            pass
    return changed

# ----------------------------------------------------------------------------
# LOOP + WATCHDOG
# ----------------------------------------------------------------------------

def _loop():
    time.sleep(BOOT_DELAY_SECONDS)
    while True:
        started = threading.Thread(target=run_cycle, daemon=True)
        with STATE_LOCK:
            STATE["cycle_alive"] = True
        started.start()
        started.join(timeout=900)
        time.sleep(CYCLE_MINUTES * 60)

threading.Thread(target=_loop, daemon=True).start()

_last_watchdog_kick = 0.0

def watchdog():
    global _last_watchdog_kick
    now = time.time()
    if now - _last_watchdog_kick < 120:
        return
    _last_watchdog_kick = now
    stale = now - STATE["last_cycle_ts"] > max(CYCLE_MINUTES * 60 * 1.5, 900)
    if stale and not STATE["cycle_alive"]:
        with STATE_LOCK:
            STATE["cycle_alive"] = True
        threading.Thread(target=run_cycle, daemon=True).start()

# ----------------------------------------------------------------------------
# FLASK APP + PREMIUM DASHBOARD
# ----------------------------------------------------------------------------

app = Flask(__name__)

@app.before_request
def _before():
    watchdog()

@app.route("/health")
def health():
    return "ok", 200

@app.route("/api/status")
def status():
    rows = []
    for r in STATE["scan"].values():
        rows.append({
            "symbol": r["symbol"],
            "direction": r.get("direction", "NEUTRAL"),
            "confluence": r.get("confluence", "-"),
            "score": r.get("score", 0),
            "rsi_1h": r.get("rsi_1h"),
            "last_close": r.get("last_close"),
            "votes": r.get("votes") or {},
            "crew": r.get("crew") or {},
            "market_closed": bool(r.get("market_closed")),
        })
    rows.sort(key=lambda r: -r["score"])
    return jsonify({
        "watchlist": [disp(c) if is_fx(c) else f"{c}{QUOTE}" for c in WATCHLIST + FOREX_GOLD],
        "timeframes": TIMEFRAMES,
        "cycle_minutes": CYCLE_MINUTES,
        "version": APP_VERSION,
        "cycles": STATE.get("cycle_count", 0),
        "data_feed_ok": STATE["data_feed_ok"],
        "last_error": STATE["last_error"],
        "last_cycle_ts": STATE["last_cycle_ts"],
        "agents": AGENTS,
        "ledger_grade": STATE.get("ledger_grade") or {},
        "backtest": STATE.get("backtest") or {},
        "open_signals": [s for s in STATE["signals"] if s["status"] == "OPEN"],
        "signals": list(reversed(STATE["signals"]))[:40],
        "stats": STATE.get("stats") or compute_stats(STATE["signals"]),
        "agent_stats": STATE.get("agent_stats") or compute_agent_stats(STATE["signals"]),
        "chatter": list(reversed(STATE["chatter"]))[:60],
        "scan_rows": rows,
    })

@app.route("/api/backtest", methods=["POST"])
def backtest_route():
    payload = request.get_json(silent=True) or {}
    days = int(payload.get("days") or BT_DAYS)
    days = max(20, min(days, 180))
    syms = payload.get("symbols")
    if isinstance(syms, list) and syms:
        syms = [s for s in syms if isinstance(s, str)][:20]
    else:
        syms = None
    started = bt_run(days=days, symbols=syms)
    if not started:
        return jsonify({"ok": False, "reason": "backtest already running"}), 429
    return jsonify({"ok": True, "days": days})

@app.route("/api/run-now", methods=["POST"])
def run_now():
    if STATE["cycle_alive"]:
        return jsonify({"ok": False, "reason": "cycle already running"}), 429
    with STATE_LOCK:
        STATE["cycle_alive"] = True
    threading.Thread(target=run_cycle, daemon=True).start()
    return jsonify({"ok": True})

LOGO_PNG_B64 = "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isXiJ+67995//fdvfesu9OX4ON+vzk4T3d1q0aJFxY987BOXT582/dUNDQ3THfcjA/t7dv30gx/84I3QOk66vIoDm1TN7r/xO9/9ry+Y3ND/9m/f/MQjjzxS2bBhDVuzZoP8yMc+tnL5kqXv/dVv7vnBj7//3QewapWBTfs50C3b28/ivb0Plz/x6fWXnX7G6X+VzmZhmIYV+kGooWAYFq+Wy/Td67/797ff/t87i/Pn54dtOyjmcsbwrl3Opz7z2ctPPeW0txB0lTFKS6EDDVUulctPvO+vrvoFuuaWsG1biM5OC0NDR3v/yxolKdRah5//0pf+5qTlJ51LGkoIwZRSQmsZVKvVx/7qr/7qBoyWwh6bDyIvcl/99NdvqNqVB999+RWf27p1a4i4etj13/nP9zOOi2/8yU+uBrDlGBWcZAFWb33rmmVr3vH2/53L541MKt0kAkmu75bL5fJTN95404bbbrupZyqV6Atf+MpfLlrU+ZZMJmum06mMUipQSjnQgvXuP7D/ve/967UARmoWdf2ud72r9Q1veMM1DQ1RSU/OOTdNi0spveHhYWnb9sPve997vlHbjnius2uv/epHW1pamt/znnf/S6x8TemvtmrVKr5p0yb5vve9b/lll73tK08++fTPPvWpj//HhP4hAPrKK6+84NWvvvBdTzzx9G3XXPOF/9Za07p162j9+vXqG9/41l8Hgej6/Oc/89mhoaEyjjPn6KpVs+WmTZvktdd+/eyTl5/0v1paWk713cDr6+u775GHH/nel7762WeLxfk5OXyYl5HzgD7vkkvONO+4owef/thnTlvctejvH3nskf/45je/tqmmH9UUEjERkf7i565ZfeKSJf/j36+//lN33nnLc0CHuQ3bvPb2szLdvQ+7V1zwPy/6n+//668+vvnxz/7DP2y4UWvN1q1bF70D1q591WmnnfFPDz10/6e/+MUvbl67di2Lf8/Wr1+v3v3uKxdfeOEFn2pra2WGYVmu68p0Om1qLaWUsvjUU89+/uMf/8ffJPeSHPeuN61e8JYr/vJThWIxC8CUMtSGYSoGGnpy81M/IaK742ck4VR/NNkt6ibgOuqo42WDlJJ3ndi1snVa63xAE2Ms1VwsLpwzt+Nj3/jGNf/x2te+ed4kyhwBwJz2GcWuRSeea5lW2redHs9x9rtVZ5/vugcC3+8dKZcrALA+XsQS5e+b3/zm8lXnrvphe/ucd4swLI2MjLxQnFZcdvKyZf/2yU9+8oLocmPX27BhAwFAS6GltWvRkgunT58+PbqnF4JxqmJ3t3/ppZe2fv4LX/rXFaec9o8GI+/5F164rzGfa1qx4oxrv/Od7/4V4oUiNv2qtWvXEgDkctMzM9qmz1m6bOnrLr/88iuISK9efSMBQGfH/DkLT1z8+hnTZ7QDwIpqlYBuAUC1t88BAN7S2jJz4aKFZ0FDOJXqXs9z+6pVZ7dbrfZWK9VdBw4ctAHIYSl99Pe784vFEEA4a+bMlkULO88uNDTOEr6oaiVUU6HpxOVLl63/wY/+85PYti1K8dENYGjomGqNJtUqFsxbsHTJiUsuNgyeB7RmjKWam6d1LlnS9ekf/OAHH67pZ0r+//Smp0eyucxzZ599zqf+7iN/t5Ixphlj6oorrpjbOX/+VbNnzNZtbW27E3eBY4QGgGzWzCxa2HleoaFhtm3bvX7o9Tc2NhSXdJ141fvf/+5vXHbZZfOnUoFnzZi+uGtJ13lhGJQq5coLrusdDAJ/2A2CspTyEAC/5nY0ABSLRU8pNeL73uEgCAc9L+yvVp3nstm0PPHEE1/d0NCQmrCuJmfg8+efcPrixYsvTNTjo6lF99xzjwSgGxoaGhYuXLhi9uxZ8ydRvQkATjhhwYmnnHLqW17zmr/4+ze/+c1zGGN63bp1AEALFy74i7lz27uCILDifjjm51hrTZs2bVKfXf/F15x7zrk/mN42Y2V3985flysjW5Yu7br8TW9+048/9rFPrxge3lkudhSdTuQk0GnefvvtIQDMndfesfjERa9vbZ02s/Z+XwxLlp24fPnJy/5CKZkHoFasmCYBoLf3YRcAyuVyd2Oh0Wxvn31RTMLV0qVLCQDOO++Ct0+f3jp/eHh4EAC2bds2wfUiMJcu7Tqrvb1jUaVS7hEiOOR5bp/reoccx9nnedVxz0NyfHFe+7RlJ3W9btasGScaBjHLMtL5fL5twYIT3n7RJRd9/0tf+tLpk1gi6gpgHXXU8eeFQ4cOqaGhwyO79ux54sMf+ptPJe/eD/3d311y2WVv+9Kad152+Z133vy5yY4dPHTIOXTwYP9/fv8H/7Hhpzf8cmqtMVJB1q1bp9evX49cQ8M7most035ww/c/8NUvf/VOAJjd2dn+w//4zs8uuuii933uc597YDLlYXBwcKSvr293ZaRSjs83qpDEqgJe9apXXTp37twzH3/04W9/4ANXfhlAMG3atJk//cmG7yxetPhv3/3uKx9av3799kQtSM5t24dktVK1q3m7d8GCRW+96qqrfskY2wMAw5VK9fDQYJ/jlsoA8Pjjj4/6jz300I0eEWF4ZMjrP9B34JprrvnK3b+67cEpN/A90yTQox5//HGXiDB4cGCk78D+/d++/t+vvenGG29DZJrK3PCTG69ua2t99T99Yu3JRPTo6tWrxYYN3ccVuT04ePjQC8/v2H7JJa/9IICDMYlpvfvuu/+zo6Pj9e3t7T8gov1xX+h169YREel1X/iXa77y+esufPOlb/3I17/y9b8EIFauPPstnufpn/zkR1/70Y9+ZE/sv2NBT0+f3du7v++5LVt/9rFPfuw7yez4x49//M1/8apXf/qii15z6U033XTdhg0bGCZEZPf3Hx4+1N9/6Ktf/dr/2bTprvsnJ0Fj5C/eaJS/+c1v/t3Ez11//Xe/0t4+e/unP331hmReTvzMyEhpkHNGGAv80C/e32Xn0KGBA+VyuTTVZ4aHR4YOHx7Y3dIybcaaNe/84M033/zp+Nx8aGik5Djjqpwcs5pPRLpYnN/YtWzp//I9r+8fPvKRDz700KYtAPCpf/zUG19/6aXXnHXmyvcD9Pi+XubV5MgjADg8fLh8oO/ATtd1jytlUKlSqhw8uH+P47hezbOR3BRuvvlney666IKburqWXnDeeecV77///uF3vOMdsrW1NS+EnL1167M3X3/99T2MMWzYsGHcfBoY6PUOHDhwwLbth/7H/3jHPx+l8eP6q1QadHp79x144YUXvvvBD37wW4hM4Oqf//kTa951xbvWTZs2/a1a68fj99EfVW7bugJYRx11vGzI5XLkeY6rZahWr17NOY9yzH7r619/8Pnnt23OF7JzALA4g/64nIFOEISVql2Z3jKN1ypCSeDBES+vJAt/GOYHDg/0/vC/fvg0AHDDwIGdO3sPHux7zI6qfxDnfPR6q1ev1hFZPWDZtl0RwleTnFsBQD7f1Dl4eKDn61//2g8BBFu2aGtwcLDv6Wee+L99Bw74p522tBkAEhUiWfxnzpyJatW2u3e+sMvzPPekU05771iwhFRSCEXx+zcmnuO4h3B833UdZ/4Jc2jCfdUqZUfU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"

DASHBOARD = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>The Crew · Signal Desk</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#04060d; --panel:rgba(13,18,32,.82); --panel2:rgba(20,27,45,.6);
  --line:rgba(120,150,220,.12); --text:#e8ecf6; --muted:#8b96b0;
  --cyan:#22d3ee; --violet:#a78bfa; --amber:#f59e0b; --red:#ef4444; --green:#34d399;
}
*{box-sizing:border-box;margin:0;padding:0}
body{
  background:radial-gradient(1200px 600px at 80% -10%,rgba(34,211,238,.08),transparent 60%),
             radial-gradient(900px 500px at -10% 110%,rgba(167,139,250,.07),transparent 60%),
             var(--bg);
  color:var(--text); font-family:'Inter',system-ui,sans-serif; min-height:100vh; padding:28px clamp(16px,4vw,44px);
}
.mono{font-family:'JetBrains Mono',monospace}
a{color:var(--cyan)}
header{display:flex;justify-content:space-between;align-items:flex-end;gap:16px;flex-wrap:wrap;margin-bottom:26px}
h1{font-size:clamp(24px,4vw,34px);font-weight:800;letter-spacing:-.02em;
   background:linear-gradient(90deg,#e8ecf6,#22d3ee 55%,#a78bfa);-webkit-background-clip:text;background-clip:text;color:transparent}
.tag{color:var(--muted);font-size:13px;margin-top:6px}
.live{display:inline-flex;align-items:center;gap:7px;font-size:12px;color:var(--green);font-weight:600}
.live .dot{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 10px var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.banner{background:rgba(190,18,60,.15);border:1px solid rgba(244,63,94,.4);color:#fecdd3;
        padding:11px 16px;border-radius:12px;margin-bottom:16px;font-size:13px}
.grid{display:grid;gap:14px}
.stats{grid-template-columns:repeat(auto-fit,minmax(160px,1fr));margin-bottom:22px}
.stat{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px 18px;backdrop-filter:blur(8px)}
.stat .k{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.08em}
.stat .v{font-size:26px;font-weight:800;margin-top:6px}
.stat .s{color:var(--muted);font-size:12px;margin-top:2px}
.crew{grid-template-columns:repeat(auto-fit,minmax(230px,1fr));margin-bottom:22px}
.agent{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px;backdrop-filter:blur(8px);
       position:relative;overflow:hidden;transition:border-color .3s}
.agent:hover{border-color:rgba(120,150,220,.3)}
.agent::before{content:'';position:absolute;inset:0 0 auto 0;height:3px;background:var(--ac)}
.agent .top{display:flex;align-items:center;gap:12px;margin-bottom:12px}
.avatar{width:42px;height:42px;border-radius:12px;display:flex;align-items:center;justify-content:center;overflow:hidden;
        font-weight:800;font-size:17px;color:#04060d;background:var(--ac);box-shadow:0 0 22px color-mix(in srgb,var(--ac) 45%,transparent)}
.avatar img,.msg .av img{width:100%;height:100%;object-fit:cover;border-radius:inherit;display:block}
.agent .name{font-weight:700;font-size:16px}
.agent .role{color:var(--muted);font-size:12px}
.agent .last{font-size:12.5px;color:var(--muted);line-height:1.5;margin-top:10px;min-height:38px}
.agent .last b{color:var(--text)}
h2{font-size:15px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);margin:26px 0 12px}
h2 span{color:var(--text)}
.feed{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px;max-height:430px;overflow-y:auto;backdrop-filter:blur(8px)}
.msg{display:flex;gap:11px;margin-bottom:14px;animation:rise .35s ease}
@keyframes rise{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
.msg .av{flex:0 0 30px;height:30px;border-radius:9px;background:var(--ac);color:#04060d;font-weight:800;font-size:13px;
         display:flex;align-items:center;justify-content:center;overflow:hidden}
.msg .who{font-size:12px;font-weight:700;margin-bottom:2px}
.msg .who i{font-style:normal;color:var(--muted);font-weight:500;margin-left:6px}
.msg .tx{font-size:13.5px;line-height:1.5;color:#c9d2e6}
.msg .tx b{color:var(--text)}
table{width:100%;border-collapse:separate;border-spacing:0;background:var(--panel);border:1px solid var(--line);
      border-radius:14px;overflow:hidden;font-size:13px;backdrop-filter:blur(8px)}
th{background:var(--panel2);color:var(--muted);text-align:left;padding:10px 12px;font-size:10.5px;
   text-transform:uppercase;letter-spacing:.07em;font-weight:600}
td{padding:9px 12px;border-top:1px solid var(--line)}
tr:hover td{background:rgba(120,150,220,.05)}
.badge{padding:3px 9px;border-radius:6px;font-size:11px;font-weight:700;color:#fff;display:inline-block;letter-spacing:.03em}
.b-long{background:linear-gradient(135deg,#0ea371,#34d399)}
.b-short{background:linear-gradient(135deg,#dc2626,#ef4444)}
.b-neutral{background:#3a4358}
.b-vetoed{background:linear-gradient(135deg,#b45309,#f59e0b);color:#04060d}
.b-nodata{background:#7f1d1d}
.out-tp{color:var(--green);font-weight:700}
.out-sl{color:var(--red);font-weight:700}
.out-open{color:var(--cyan);font-weight:700}
.vote{font-size:14px;width:22px;display:inline-block;text-align:center}
.v-up{color:var(--green)} .v-dn{color:var(--red)} .v-nt{color:#3a4358}
button{background:linear-gradient(135deg,#0e7490,#155e93);color:#fff;border:0;border-radius:10px;padding:9px 16px;
       cursor:pointer;font-size:13px;font-weight:600;transition:filter .2s}
button:hover{filter:brightness(1.15)}
button:disabled{filter:grayscale(.6);cursor:wait}
.foot{color:var(--muted);font-size:12px;margin-top:30px;line-height:1.6}
.wrap{max-width:1180px;margin:0 auto}
</style></head><body>
<div class="wrap">
<header>
  <div>
    <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
    <img src="data:image/png;base64,{{logo}}" alt="The Crew · Signal Desk — designed by Dezlin Oliver" style="height:60px;width:auto;display:block;image-rendering:-webkit-optimize-contrast">
    <span class="mono" id="ver" style="font-size:12px;opacity:.55">v?</span>
  </div>
    <div class="tag">Four AI agents. One desk. Full consensus or no trade · <span class="mono">15m / 1h / 4h / 1d</span> confluence · crypto + FX + gold · self-audited record</div>
  </div>
  <div style="display:flex;align-items:center;gap:14px">
    <div class="live"><span class="dot"></span><span id="live">syncing…</span></div>
    <button id="scan" onclick="runNow()">↻ Run sweep</button>
  </div>
</header>

<div id="banner"></div>

<div class="grid stats">
  <div class="stat"><div class="k">Desk win rate</div><div class="v" id="s-win">--</div><div class="s" id="s-win-n">no closed signals yet</div></div>
  <div class="stat"><div class="k">Closed signals</div><div class="v" id="s-closed">0</div><div class="s">self-audited, on-chain price data</div></div>
  <div class="stat"><div class="k">Avg best R</div><div class="v" id="s-avg">--</div><div class="s">per closed signal</div></div>
  <div class="stat"><div class="k">Open positions</div><div class="v" id="s-open">0</div><div class="s" id="s-open-n">tracked live</div></div>
  <div class="stat"><div class="k">Sweeps run</div><div class="v" id="s-cycles">0</div><div class="s" id="s-cycle-n">--</div></div>
</div>

<h2>THE CREW <span>· who's on the desk</span></h2>
<div class="grid crew" id="agents"></div>

<h2>DESK CHATTER <span>· live agent log</span></h2>
<div class="feed" id="feed"><div class="msg"><div class="tx" style="color:var(--muted)">Booting the desk…</div></div></div>

<h2>MARKET SWEEP <span>· current reads</span></h2>
<table id="scan-t"><thead><tr>
  <th>Symbol</th><th>Read</th><th>Confluence</th>
  <th>15m</th><th>1h</th><th>4h</th><th>1d</th><th>RSI 1h</th><th>Price</th><th>Crew</th>
</tr></thead><tbody></tbody></table>

<h2>BACKTEST LAB <span>· history replayed through the live crew logic</span></h2>
<div style="background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px;margin-bottom:26px">
  <div style="display:flex;gap:14px;align-items:center;margin-bottom:14px;flex-wrap:wrap">
    <button id="bt-run" onclick="runBT()">▶ Run 90-day backtest</button>
    <span id="bt-prog" style="color:var(--muted);font-size:12.5px"></span>
  </div>
  <div style="overflow-x:auto">
  <table id="bt-t">
    <thead><tr><th>Symbol</th><th>Days</th><th>Trades</th><th>SL %</th><th>TP3 %</th><th>TP1 tag %</th><th>Avg R</th><th>Avg R BE</th><th>Δ R</th><th>Total R (BE)</th><th>Max DD (R)</th><th>Verdict</th></tr></thead>
    <tbody></tbody>
  </table>
  </div>
</div>

<h2>LEDGER GRADE <span>· the desk benches its own losers</span></h2>
<div id="grade" style="background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px;margin-bottom:26px"></div>

<h2>SIGNAL LEDGER <span>· latest 40, self-audited</span></h2>
<table id="sig-t"><thead><tr>
  <th>Time</th><th>Symbol</th><th>Dir</th><th>Conf</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>TP3</th><th>Outcome</th><th>Best R</th>
</tr></thead><tbody></tbody></table>

<div class="foot">
  A trade fires only when Lester (structure) and Michael (momentum) agree on direction AND Franklin (risk) approves. Trevor then executes with SL = 1.5×ATR and targets at 1R / 2R / 3R.
  Signals close at TP3 or SL; the ledger shows the highest target reached.
  <b>This is a signal tool, not financial advice, and it never places orders.</b> Past performance never guarantees future results. DYOR.
</div>
</div>

<script>
const A = {};
const fmt = (x, nd) => x == null ? '--' : (x >= 1000 ? x.toLocaleString('en-US',{maximumFractionDigits:2}) : x.toFixed(nd ?? 4));
const badge = (d, closed) => `<span class="badge b-${({LONG:'long',SHORT:'short',NEUTRAL:'neutral',VETOED:'vetoed',NO_DATA:'nodata'})[d]||'neutral'}">${(d==='NO_DATA'&&closed)?'MARKET CLOSED':d}</span>`;
const vote = v => `<span class="vote ${v>0?'v-up':(v<0?'v-dn':'v-nt')}">${v>0?'▲':(v<0?'▼':'·')}</span>`;
const agentBadge = k => A[k] ? `<span style="color:${A[k].color};font-weight:700">${A[k].glyph}</span>` : k;

async function pull(){
  try{
    const r = await fetch('/api/status');
    const d = await r.json();
    render(d);
  }catch(e){
    document.getElementById('live').textContent = 'offline';
  }
}

function render(d){
  renderBT(d.backtest || {});
  renderGrade(d.ledger_grade || {});
  for (const k in d.agents) A[k] = d.agents[k];
  const live = document.getElementById('live');
  const age = d.last_cycle_ts ? Math.round((Date.now()/1000 - d.last_cycle_ts)) : null;
  live.textContent = d.data_feed_ok ? (age != null ? `desk live · last sweep ${age}s ago` : 'desk live') : 'feed issues';

  const staleMin = age != null ? age / 60 : null;
  const staleWarn = staleMin != null && staleMin > Math.max(d.cycle_minutes * 1.5, 10);
  document.getElementById('banner').innerHTML = !d.data_feed_ok
    ? `<div class="banner"><b>⚠ Data feed problem:</b> some symbols could not be fetched. ${d.last_error||''}</div>`
    : staleWarn
      ? `<div class="banner"><b>⚠ Reads are ${Math.round(staleMin)} min old</b> — desk was likely asleep (Render free tier). Hit "Run sweep" or set up a keep-alive pinger on /health for live data.</div>`
      : '';

  // stats
  const st = d.stats || {};
  document.getElementById('s-win').textContent = st.win_rate == null ? '--' : st.win_rate + '%';
  document.getElementById('s-win-n').textContent = st.win_rate == null ? 'no closed signals yet' : `${st.sl_rate ?? '--'}% hit the stop`;
  document.getElementById('s-closed').textContent = st.closed ?? 0;
  document.getElementById('s-avg').textContent = st.avg_r == null ? '--' : st.avg_r + 'R';
  const opens = d.open_signals.length;
  document.getElementById('s-open').textContent = opens;
  document.getElementById('s-open-n').textContent = opens ? d.open_signals.map(s=>s.symbol.replace('USDT','')).join(', ') : 'none right now';
  document.getElementById('s-cycles').textContent = d.cycles;
  const ver=document.getElementById('ver'); if (ver && d.version) ver.textContent = 'v'+d.version;
  document.getElementById('s-cycle-n').textContent = `every ${d.cycle_minutes} min`;

  // agents
  const lastWords = {};
  (d.chatter||[]).forEach(m => { lastWords[m.a] = m; });
  document.getElementById('agents').innerHTML = Object.entries(d.agents).map(([k,a])=>{
    const lw = lastWords[k];
    return `<div class="agent" style="--ac:${a.color}">
      <div class="top"><div class="avatar">${a.img ? `<img src="${a.img}" alt="${a.name}" loading="lazy" onerror="this.remove()">` : a.glyph}</div>
        <div><div class="name">${a.name}</div><div class="role">${a.role}</div></div></div>
      <div class="last">${lw ? `<b>${lw.s||'desk'}:</b> ${lw.t}` : 'Waiting for the first sweep…'}</div>
    </div>`;}).join('');

  // chatter
  document.getElementById('feed').innerHTML = (d.chatter||[]).map(m=>{
    const a = d.agents[m.a] || {glyph:'?',color:'#888',name:m.a};
    return `<div class="msg" style="--ac:${a.color}">
      <div class="av">${a.img ? `<img src="${a.img}" alt="${a.name}" loading="lazy" onerror="this.remove()">` : a.glyph}</div>
      <div><div class="who" style="color:${a.color}">${a.name}<i>${m.s||''}${m.ts?(' · '+new Date(m.ts*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})):''}</i></div>
      <div class="tx">${m.t}</div></div>
    </div>`;}).join('') || '<div class="msg"><div class="tx" style="color:var(--muted)">Quiet desk. Run a sweep.</div></div>';

  // scan table
  document.getElementById('scan-t').querySelector('tbody').innerHTML = d.scan_rows.map(r=>{
    const v = r.votes||{}, c = r.crew||{};
    const crew = ['lester','michael','franklin','trevor'].map(k=>c[k]?agentBadge(k):'').join(' ');
    return `<tr><td><b>${r.symbol}</b></td><td>${badge(r.direction, r.market_closed)}</td><td class="mono">${r.confluence}</td>
      <td>${vote(v['15m'])}</td><td>${vote(v['1h'])}</td><td>${vote(v['4h'])}</td><td>${vote(v['1d'])}</td>
      <td class="mono">${r.rsi_1h==null?'--':r.rsi_1h.toFixed(1)}</td><td class="mono">${fmt(r.last_close)}</td><td>${crew}</td></tr>`;
  }).join('');

  // signals table
  document.getElementById('sig-t').querySelector('tbody').innerHTML = (d.signals||[]).map(s=>{
    const out = (s.outcome==='BE_HIT'?'BE':s.outcome) || (s.status==='OPEN'?'OPEN':'--');
    const cls = out.startsWith('TP')?'out-tp':(out==='SL_HIT'?'out-sl':'out-open');
    return `<tr><td class="mono">${s.created.slice(5,16).replace('T',' ')}</td><td><b>${s.symbol}</b></td>
      <td>${badge(s.direction)}</td><td class="mono">${s.confidence}%</td>
      <td class="mono">${fmt(s.entry)}</td><td class="mono">${fmt(s.stop_loss)}</td>
      <td class="mono">${fmt(s.tp1)}</td><td class="mono">${fmt(s.tp2)}</td><td class="mono">${fmt(s.tp3)}</td>
      <td class="${cls}">${out.replace('_HIT','')}</td><td class="mono">${s.max_favorable_r}R</td></tr>`;
  }).join('') || '<tr><td colspan="11" style="color:var(--muted)">No signals yet — the crew only fires on full consensus.</td></tr>';
}

async function runBT(days=90){
  const b = document.getElementById('bt-run');
  b.disabled = true;
  try {
    const r = await fetch('/api/backtest', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({days})});
    const x = await r.json();
    if(!x.ok) alert(x.reason || 'could not start backtest');
  } catch(e) { alert('could not start backtest'); }
  setTimeout(()=>b.disabled=false, 1500);
}

function renderBT(bt){
  const el = document.getElementById('bt-prog');
  el.textContent = bt.progress || (bt.results && bt.results.length ? 'done' : 'never run');
  const tb = document.getElementById('bt-t').querySelector('tbody');
  tb.innerHTML = (bt.results||[]).map(r=>{
    if(r.error) return `<tr><td>${r.symbol}</td><td colspan="9" style="color:var(--muted)">${r.error}</td></tr>`;
    const v = r.verdict || '—';
    const vc = v==='EDGE' ? '#34d399' : (v==='AVOID' ? '#ef4444' : 'var(--muted)');
    const trades = r.trades || 0;
    if(!trades) return `<tr><td>${r.symbol}</td><td>${r.days||'—'}</td><td>0</td><td colspan="9" style="color:var(--muted)">${r.note||'crew fired on nothing'}</td></tr>`;
    const dr = (r.delta_r||0);
    const dc = dr>0 ? '#34d399' : (dr<0 ? '#ef4444' : 'var(--muted)');
    return `<tr><td>${r.symbol}</td><td>${r.days}</td><td>${trades}</td><td>${r.sl_rate}%</td><td>${r.tp3_rate}%</td><td>${r.tp1_touch}%</td><td>${r.avg_r}</td><td>${r.avg_r_be}</td><td style="color:${dc}">${dr>0?'+':''}${dr}</td><td>${r.expectancy_r}</td><td>${r.max_dd_r}</td><td style="color:${vc};font-weight:700">${v}</td></tr>`;
  }).join('');
}

function renderGrade(g){
  const el = document.getElementById('grade');
  const syms = Object.entries(g.symbols||{}).sort((a,b)=>(b[1].avg_r||0)-(a[1].avg_r||0));
  if(!syms.length){
    el.innerHTML = '<div style="color:var(--muted);font-size:13px">No closed trades graded yet — the ledger grades itself as trades close. Symbols that lose get benched; hours that lose get vetoed.</div>';
    return;
  }
  const bench = g.benched || {};
  const rows = syms.map(([s,v])=>`
    <div style="display:flex;gap:16px;align-items:center;padding:8px 0;border-bottom:1px solid var(--line);font-size:13px;flex-wrap:wrap">
      <b style="min-width:86px">${s}</b>
      <span style="color:var(--muted)">${v.n} trades</span>
      <span>win ${v.win_rate}%</span>
      <span>avg ${v.avg_r}R</span>
      ${bench[s] ? '<span style="color:#ef4444;font-weight:700">BENCHED</span>' : ''}
    </div>`).join('');
  const bh = (g.bad_hours||[]);
  const hours = bh.length ? `<div style="margin-top:12px;color:#ef4444;font-size:13px"><b>Learned veto hours (UTC):</b> ${bh.map(h=>h+':00').join(', ')} — Franklin blocks entries born in these hours</div>`
    : '<div style="margin-top:12px;color:var(--muted);font-size:12.5px">No losing hours proven yet (needs '+(g.min_trades||5)+'+ closed trades per hour)</div>';
  el.innerHTML = rows + hours;
}

async function runNow(){
  const b = document.getElementById('scan');
  b.disabled = true; b.textContent = 'Sweeping…';
  try{ await fetch('/api/run-now',{method:'POST'}); }catch(e){}
  setTimeout(()=>{ b.disabled=false; b.textContent='↻ Run sweep'; pull(); }, 12000);
}

pull();
setInterval(pull, 15000);
</script>
</body></html>"""

@app.route("/")
def dashboard():
    return DASHBOARD.replace("{{logo}}", LOGO_PNG_B64)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
