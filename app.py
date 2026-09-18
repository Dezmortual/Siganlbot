"""
TRADEBISE-STYLE SIGNAL SCANNER BOT
==================================
Scans a watchlist of markets across multiple timeframes (15m / 1h / 4h / 1d),
computes confluence-based signals with entry, stop-loss and TP1/TP2/TP3,
and SELF-AUDITS every closed signal so the dashboard shows the real win rate.

Data source: Binance spot public API (no key needed, works from Render regions).
Deploy pattern: same as your other bots (gunicorn 1 worker, generations + watchdog).

This generates analysis signals, NOT financial advice. It is a scanner, not a
trading bot — it never places orders.
"""

import os
import json
import time
import math
import copy
import threading
import logging
from datetime import datetime, timezone, timedelta
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

# Symbols the bot scans. Crypto only by default (Binance spot). Pair vs USDT.
QUOTE = "USDT"

# Timeframes used for confluence voting. Each timeframe casts one vote.
TIMEFRAMES = ["15m", "1h", "4h", "1d"]

# How many candles to fetch per timeframe
CANDLE_LIMIT = 220

# Minimum confluence score (out of 4 timeframes) required to emit a signal.
# 3 = only fire when 3+ timeframes agree. 4 = everything must agree (rare).
MIN_CONFLUENCE = 3

# RSI zones
RSI_OVERSOLD = float(os.environ.get("RSI_OVERSOLD", "35"))
RSI_OVERBOUGHT = float(os.environ.get("RSI_OVERBOUGHT", "65"))

# Risk geometry: SL = ATR_MULT * ATR. TPs at R-multiples of the risk distance.
ATR_MULT = 1.5
TP1_R = 1.0
TP2_R = 2.0
TP3_R = 3.0

# Re-signal cooldown: don't emit a new signal for a symbol while one is open
# or within N minutes of the last closed one for the same direction.
RESCAN_MINUTES = int(os.environ.get("RESCAN_MINUTES", "60"))

# Signal store persistence (flat file, survives restarts on same disk)
DATA_FILE = os.environ.get("DATA_FILE", "signals.json")

# Max tracked (closed) signals kept in history
MAX_HISTORY = 500

CYCLE_MINUTES = float(os.environ.get("CYCLE_MINUTES", "30"))
BOOT_DELAY_SECONDS = 30

BINANCE_BASES = ["https://api.binance.com", "https://api1.binance.com", "https://data-api.binance.vision"]
FETCH_TIMEOUT = 6
FETCH_HARD_BOUND = 20  # absolute max seconds any data fetch may take (covers DNS hangs)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scanner")

# ----------------------------------------------------------------------------
# STATE (generations pattern — a hung cycle is abandoned, never blocks)
# ----------------------------------------------------------------------------

STATE_LOCK = threading.Lock()
STATE = {
    "generation": 0,          # monotonically increasing cycle id
    "last_cycle_ts": 0,
    "last_signal_ts": 0,
    "cycle_alive": False,
    "data_feed_ok": True,
    "last_error": "",
    "scan": {},                # symbol -> latest read (row for dashboard)
    "signals": [],             # signal records (open + closed)
    "stats": {},
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
    """GET with hard time bound covering DNS hangs."""
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

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            return fut.result(timeout=FETCH_HARD_BOUND)
        except Exception:
            return None

def fetch_klines(symbol):
    """Fetch candles for all timeframes. Returns {tf: [[o,h,l,c,v],...]} or None."""
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
    """Latest price via ticker."""
    data = _bounded_get("/api/v3/ticker/price", {"symbol": symbol})
    if data and "price" in data:
        try:
            return float(data["price"])
        except Exception:
            return None
    return None

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

def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_c = candles[i][1], candles[i][2], candles[i - 1][3]
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(trs[-period:]) / period

# ----------------------------------------------------------------------------
# SIGNAL ENGINE
# ----------------------------------------------------------------------------

def timeframe_vote(candles):
    """One timeframe's read. Returns (direction, detail dict).
    direction: +1 bullish, -1 bearish, 0 neutral"""
    closes = [c[3] for c in candles]
    vols = [c[4] for c in candles]
    r = rsi(closes)
    e20, e50, e200 = ema(closes, 20), ema(closes, 50), ema(closes, 200)
    vol_avg = sum(vols[-20:]) / 20 if len(vols) >= 20 else None

    detail = {"rsi": r, "e20": e20, "e50": e50, "e200": e200}
    if r is None or e50 is None or e200 is None:
        return 0, detail

    # Trend structure
    if e20 is not None and e20 > e50 > e200:
        trend = +1
    elif e20 is not None and e20 < e50 < e200:
        trend = -1
    else:
        trend = 0
    detail["trend"] = trend

    # Volume confirmation on the last candle vs average
    detail["vol_ok"] = bool(vol_avg and vols[-1] > vol_avg * 0.8)
    detail["vol_ratio"] = (vols[-1] / vol_avg) if vol_avg else None

    # Vote = trend direction, strengthened when RSI agrees
    #  - bullish trend + RSI 50-70 (momentum, not exhausted)  -> strong +1
    #  - bullish trend + RSI > overbought -> stretched, weaken to 0
    #  - trend 0 but RSI < oversold -> mean-reversion long vote
    if trend == +1:
        if r > RSI_OVERBOUGHT:
            return 0, detail  # overextended, wait for pullback
        return +1, detail
    if trend == -1:
        if r < RSI_OVERSOLD:
            return 0, detail  # overextended down, wait
        return -1, detail
    # No trend: allow pure mean-reversion votes only from oversold/overbought
    if r < RSI_OVERSOLD:
        return +1, detail
    if r > RSI_OVERBOUGHT:
        return -1, detail
    return 0, detail

def score_symbol(symbol, klines):
    """Full multi-timeframe read for one symbol."""
    votes = {}
    details = {}
    for tf in TIMEFRAMES:
        v, d = timeframe_vote(klines[tf])
        votes[tf] = v
        details[tf] = d

    bull = sum(1 for v in votes.values() if v > 0)
    bear = sum(1 for v in votes.values() if v < 0)

    if bull >= MIN_CONFLUENCE and bull > bear:
        direction = "LONG"
        score = bull
    elif bear >= MIN_CONFLUENCE and bear > bull:
        direction = "SHORT"
        score = bear
    else:
        direction = "NEUTRAL"
        score = max(bull, bear)

    # Use the 1h ATR for risk geometry
    ref = klines.get("1h") or klines[TIMEFRAMES[1]]
    a = atr(ref)
    last_close = ref[-1][3]
    detail_1h = details.get("1h", {})

    read = {
        "symbol": symbol,
        "direction": direction,
        "confluence": f"{score}/{len(TIMEFRAMES)}",
        "score": score,
        "rsi_1h": round(detail_1h.get("rsi") or 0, 1),
        "trend_1h": detail_1h.get("trend", 0),
        "atr": a,
        "last_close": last_close,
        "votes": votes,
        "details": details,
        "ts": time.time(),
    }
    return read

def build_signal(read):
    """Turn a directional read into a full signal with entry/SL/TPs."""
    entry = read["last_close"]
    a = read["atr"]
    if not a or not entry:
        return None
    risk = ATR_MULT * a
    if read["direction"] == "LONG":
        sl = entry - risk
        tps = [entry + TP1_R * risk, entry + TP2_R * risk, entry + TP3_R * risk]
    elif read["direction"] == "SHORT":
        sl = entry + risk
        tps = [entry - TP1_R * risk, entry - TP2_R * risk, entry - TP3_R * risk]
    else:
        return None

    conf = int(round(100.0 * read["score"] / len(TIMEFRAMES)))
    return {
        "id": f"{read['symbol']}-{int(time.time())}",
        "symbol": read["symbol"],
        "direction": read["direction"],
        "entry": round(entry, 6),
        "stop_loss": round(sl, 6),
        "tp1": round(tps[0], 6),
        "tp2": round(tps[1], 6),
        "tp3": round(tps[2], 6),
        "confidence": conf,
        "rsi_1h": read["rsi_1h"],
        "votes": read["votes"],
        "created": datetime.now(timezone.utc).isoformat(),
        "status": "OPEN",
        "outcome": None,       # TP1_HIT / TP2_HIT / TP3_HIT / SL_HIT
        "closed": None,
        "max_favorable_r": 0.0,
    }

# ----------------------------------------------------------------------------
# SIGNAL TRACKING (self-audit)
# ----------------------------------------------------------------------------

def risk_distance(sig):
    return abs(sig["entry"] - sig["stop_loss"])

def update_open_signals(prices):
    """Check each open signal against current price. Close on TP3 or SL.
    TPs beyond TP1 ratchet the outcome upward."""
    changed = False
    for sig in STATE["signals"]:
        if sig["status"] != "OPEN":
            continue
        p = prices.get(sig["symbol"])
        if not p:
            continue
        risk = risk_distance(sig)
        if risk <= 0:
            continue
        long = sig["direction"] == "LONG"
        # favorable excursion in R
        fav = ((p - sig["entry"]) / risk) if long else ((sig["entry"] - p) / risk)
        sig["max_favorable_r"] = round(max(sig.get("max_favorable_r", 0.0), fav), 2)

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
            changed = True
    if changed:
        _save_signals()

def compute_stats():
    closed = [s for s in STATE["signals"] if s["status"] == "CLOSED"]
    n = len(closed)
    if n == 0:
        return {"closed": 0, "win_rate": None, "tp1_rate": None,
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

def can_emit(symbol, direction):
    """Cooldown: skip if same symbol+direction signal is open or recent."""
    now = time.time()
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
# CYCLE (runs in generations — private deepcopy, commit only if still newest)
# ----------------------------------------------------------------------------

def run_cycle():
    gen = STATE["generation"] + 1
    log.info("cycle generation %d starting", gen)
    work = copy.deepcopy(STATE)  # private workspace

    prices = {}
    scan = {}
    new_signals = []
    feed_ok = True

    for coin in WATCHLIST:
        symbol = f"{coin}{QUOTE}"
        price = fetch_price(symbol)
        if price:
            prices[symbol] = price
        klines = fetch_klines(symbol)
        if not klines:
            feed_ok = False
            scan[symbol] = {
                "symbol": symbol, "direction": "NO_DATA",
                "confluence": "-", "score": 0, "rsi_1h": None,
                "last_close": price, "ts": time.time(),
            }
            continue
        read = score_symbol(symbol, klines)
        if price:
            read["last_close"] = price
        scan[symbol] = read

        if read["direction"] != "NEUTRAL" and can_emit(symbol, read["direction"]):
            sig = build_signal(read)
            if sig:
                new_signals.append(sig)
                log.info("SIGNAL %s %s @ %s conf=%d%% SL=%s TP1=%s",
                         sig["direction"], sig["symbol"], sig["entry"],
                         sig["confidence"], sig["stop_loss"], sig["tp1"])

    work["scan"] = scan
    work["signals"] = (work["signals"] + new_signals)[-MAX_HISTORY:]
    work["stats"] = compute_stats_from(work["signals"])
    work["data_feed_ok"] = feed_ok
    work["last_error"] = "" if feed_ok else "Some symbols returned no data"

    # commit only if still the newest generation
    with STATE_LOCK:
        if gen > STATE["generation"]:
            STATE.update(work)
            STATE["generation"] = gen
            STATE["last_cycle_ts"] = time.time()
            STATE["last_signal_ts"] = time.time()
            STATE["cycle_alive"] = False
            _save_signals()
            log.info("cycle gen %d committed: %d scanned, %d new signals", gen, len(scan), len(new_signals))
        else:
            log.info("cycle gen %d superseded, discarded", gen)

def compute_stats_from(signals):
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

# ----------------------------------------------------------------------------
# LOOP + WATCHDOG (same pattern as Medallion v4)
# ----------------------------------------------------------------------------

def _loop():
    time.sleep(BOOT_DELAY_SECONDS)
    while True:
        started = threading.Thread(target=run_cycle, daemon=True)
        with STATE_LOCK:
            STATE["cycle_alive"] = True
        started.start()
        # join with generous cap; generations pattern makes hangs harmless
        started.join(timeout=900)
        time.sleep(CYCLE_MINUTES * 60)

threading.Thread(target=_loop, daemon=True).start()

_last_watchdog_kick = 0.0

def watchdog():
    """On any web request, kick a cycle if the last one is stale."""
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
# FLASK APP
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
    age = time.time() - STATE["last_cycle_ts"] if STATE["last_cycle_ts"] else None
    return jsonify({
        "watchlist": [f"{c}{QUOTE}" for c in WATCHLIST],
        "timeframes": TIMEFRAMES,
        "min_confluence": MIN_CONFLUENCE,
        "cycle_minutes": CYCLE_MINUTES,
        "last_cycle_age_s": round(age, 0) if age else None,
        "data_feed_ok": STATE["data_feed_ok"],
        "last_error": STATE["last_error"],
        "open_signals": [s for s in STATE["signals"] if s["status"] == "OPEN"],
        "stats": STATE.get("stats") or compute_stats(),
        "scan_rows": [
            {k: v.get(k) for k in ("symbol", "direction", "confluence", "score",
                                    "rsi_1h", "last_close")}
            for v in STATE["scan"].values()
        ],
    })

@app.route("/api/signals")
def signals():
    limit = int(request.args.get("limit", "50"))
    rows = list(reversed(STATE["signals"]))[:limit]
    return jsonify(rows)

@app.route("/api/run-now", methods=["POST"])
def run_now():
    if STATE["cycle_alive"]:
        return jsonify({"ok": False, "reason": "cycle already running"}), 429
    with STATE_LOCK:
        STATE["cycle_alive"] = True
    threading.Thread(target=run_cycle, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/")
def dashboard():
    st = STATE.get("stats") or compute_stats()
    rows = sorted(STATE["scan"].values(), key=lambda r: -r.get("score", 0))
    now_s = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def fmt(x, nd=4):
        if x is None:
            return "--"
        if x >= 1000:
            return f"{x:,.0f}"
        return f"{x:.{nd}f}"

    def dir_badge(d):
        color = {"LONG": "#16a34a", "SHORT": "#dc2626",
                 "NEUTRAL": "#6b7280", "NO_DATA": "#92400e"}.get(d, "#6b7280")
        return f'<span style="background:{color};color:#fff;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600">{d}</span>'

    scan_html = ""
    for r in rows:
        votes = r.get("votes") or {}
        vote_cells = "".join(
            f"<td>{'▲' if v > 0 else ('▼' if v < 0 else '·')}</td>"
            for v in (votes.get(tf, 0) for tf in TIMEFRAMES)
        )
        scan_html += f"""
        <tr>
          <td><b>{r['symbol']}</b></td>
          <td>{dir_badge(r['direction'])}</td>
          <td>{r.get('confluence','-')}</td>
          {vote_cells}
          <td>{r.get('rsi_1h') if r.get('rsi_1h') is not None else '--'}</td>
          <td>{fmt(r.get('last_close'))}</td>
        </tr>"""

    sig_rows = list(reversed(STATE["signals"]))[:40]
    sig_html = ""
    for s in sig_rows:
        outcome = s.get("outcome") or ("OPEN" if s["status"] == "OPEN" else "--")
        oc_color = "#16a34a" if outcome.startswith("TP") else ("#dc2626" if outcome == "SL_HIT" else "#2563eb")
        sig_html += f"""
        <tr>
          <td>{s['created'][:16].replace('T',' ')}</td>
          <td><b>{s['symbol']}</b></td>
          <td>{dir_badge(s['direction'])}</td>
          <td>{s['confidence']}%</td>
          <td>{fmt(s['entry'])}</td>
          <td>{fmt(s['stop_loss'])}</td>
          <td>{fmt(s['tp1'])}</td>
          <td>{fmt(s['tp2'])}</td>
          <td>{fmt(s['tp3'])}</td>
          <td style="color:{oc_color};font-weight:600">{outcome}</td>
          <td>{s.get('max_favorable_r','--')}R</td>
        </tr>"""

    wr = st.get("win_rate")
    feed = STATE["data_feed_ok"]
    feed_banner = "" if feed else '<div style="background:#7f1d1d;color:#fff;padding:10px;border-radius:6px;margin-bottom:12px"><b>⚠ Data feed problem:</b> some symbols could not be fetched. The Binance API may be unreachable from this region.</div>'

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Signal Scanner</title>
<style>
 body {{ background:#0b1220; color:#e5e7eb; font-family:-apple-system,system-ui,sans-serif; margin:0; padding:24px; }}
 h1 {{ font-size:22px; margin:0 0 4px; }}
 h2 {{ font-size:16px; margin:28px 0 8px; color:#93c5fd; }}
 .sub {{ color:#6b7280; font-size:13px; margin-bottom:20px; }}
 .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:8px; }}
 .card {{ background:#111a2e; border:1px solid #1f2b45; border-radius:10px; padding:14px; }}
 .card .label {{ color:#6b7280; font-size:11px; text-transform:uppercase; letter-spacing:.05em; }}
 .card .value {{ font-size:22px; font-weight:700; margin-top:4px; }}
 table {{ width:100%; border-collapse:collapse; background:#111a2e; border-radius:10px; overflow:hidden; font-size:13px; }}
 th {{ background:#18233c; color:#93c5fd; text-align:left; padding:8px 10px; font-size:11px; text-transform:uppercase; letter-spacing:.04em; }}
 td {{ padding:8px 10px; border-top:1px solid #1f2b45; }}
 .note {{ color:#6b7280; font-size:12px; margin-top:24px; }}
 button {{ background:#2563eb; color:#fff; border:0; border-radius:6px; padding:8px 14px; cursor:pointer; font-size:13px; }}
</style></head><body>
<h1>📡 Signal Scanner</h1>
<div class="sub">Multi-timeframe confluence · {', '.join(TIMEFRAMES)} · updated {now_s} · not financial advice</div>
{feed_banner}
<div class="grid">
  <div class="card"><div class="label">Win rate (closed)</div><div class="value">{'--' if wr is None else str(wr) + '%'}</div></div>
  <div class="card"><div class="label">Closed signals</div><div class="value">{st.get('closed', 0)}</div></div>
  <div class="card"><div class="label">Avg best R</div><div class="value">{'--' if st.get('avg_r') is None else str(st['avg_r']) + 'R'}</div></div>
  <div class="card"><div class="label">Open signals</div><div class="value">{sum(1 for s in STATE['signals'] if s['status']=='OPEN')}</div></div>
</div>
<p><button onclick="fetch('/api/run-now',{{method:'POST'}}).then(()=>location.reload())">↻ Scan now</button></p>
<h2>Market scan</h2>
<table><tr><th>Symbol</th><th>Read</th><th>Confluence</th>{''.join(f'<th>{tf} vote</th>' for tf in TIMEFRAMES)}<th>RSI 1h</th><th>Price</th></tr>{scan_html}</table>
<h2>Signals (latest 40) · self-audited results</h2>
<table><tr><th>Time</th><th>Symbol</th><th>Dir</th><th>Conf</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>TP3</th><th>Outcome</th><th>Best R</th></tr>{sig_html}</table>
<p class="note">Signals fire when {MIN_CONFLUENCE}+ of {len(TIMEFRAMES)} timeframes agree. Stop-loss = {ATR_MULT}×ATR(14); targets at {TP1_R}R / {TP2_R}R / {TP3_R}R. A signal closes at TP3 or SL; outcome shows the highest target reached. Past performance never guarantees future results. DYOR.</p>
</body></html>"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
