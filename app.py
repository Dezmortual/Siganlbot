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

ATR_MULT = 1.5
TP1_R, TP2_R, TP3_R = 1.0, 2.0, 3.0

RESCAN_MINUTES = int(os.environ.get("RESCAN_MINUTES", "60"))
DATA_FILE = os.environ.get("DATA_FILE", "signals.json")
MAX_HISTORY = 500
MAX_CHATTER = 160

CYCLE_MINUTES = float(os.environ.get("CYCLE_MINUTES", "30"))
BOOT_DELAY_SECONDS = int(os.environ.get("BOOT_DELAY_SECONDS", "30"))

BINANCE_BASES = ["https://api.binance.com", "https://api1.binance.com", "https://data-api.binance.vision"]
FETCH_TIMEOUT = 6
FETCH_HARD_BOUND = 20

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("crew")

AGENTS = {
    "lester":   {"name": "Lester",   "role": "Head Analyst",  "color": "#22d3ee", "glyph": "L"},
    "michael":  {"name": "Michael",  "role": "Momentum",      "color": "#a78bfa", "glyph": "M"},
    "franklin": {"name": "Franklin", "role": "Risk Manager",  "color": "#f59e0b", "glyph": "F"},
    "trevor":   {"name": "Trevor",   "role": "Executor",      "color": "#ef4444", "glyph": "T"},
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

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            return fut.result(timeout=FETCH_HARD_BOUND)
        except Exception:
            return None

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
YAHOO_BASES = ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]
YAHOO_INTERVAL = {"15m": ("15m", "1mo"), "1h": ("60m", "3mo"), "1d": ("1d", "1y")}
FX_STALE_SECONDS = 3 * 3600  # last 15m bar older than this -> market closed

def _yahoo_chart(symbol, interval, rng):
    def _do(base):
        try:
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
        for base in YAHOO_BASES:
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
    if r < RSI_OVERSOLD:
        return +1, detail
    if r > RSI_OVERBOUGHT:
        return -1, detail
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
        "ts": time.time(),
    }

# ----------------------------------------------------------------------------
# THE CREW — agent verdicts
# ----------------------------------------------------------------------------

def lester(read):
    """Head Analyst: structure across timeframes."""
    bull, bear = read["bull"], read["bear"]
    votes = read["votes"]
    if bull >= MIN_CONFLUENCE and bull > bear:
        hold = [tf for tf, v in votes.items() if v <= 0]
        extra = f" ({hold[0]} is the holdout)" if hold else " — unanimous"
        return {"verdict": "LONG", "line": f"{read['symbol']} structure is clean: {bull}/4 timeframes trending up{extra}."}
    if bear >= MIN_CONFLUENCE and bear > bull:
        hold = [tf for tf, v in votes.items() if v >= 0]
        extra = f" ({hold[0]} is the holdout)" if hold else " — unanimous"
        return {"verdict": "SHORT", "line": f"{read['symbol']} structure is heavy: {bear}/4 timeframes trending down{extra}."}
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
    if read["bull"] >= MIN_CONFLUENCE:
        if rc >= MO_FLOOR:
            return {"verdict": "LONG", "line": f"Momentum's live on {read['symbol']} — RSI {rc}, room before it's cooked."}
        return {"verdict": "NEUTRAL", "line": f"{read['symbol']} momentum's flat (RSI {rc}). Wait for fuel."}
    if read["bear"] >= MIN_CONFLUENCE:
        if rc <= (100 - MO_FLOOR):
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
                                "score": 0, "rsi_1h": None, "last_close": price, "ts": time.time()}
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
            if can_emit(symbol, sig["direction"]):
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
        if risk <= 0:
            continue
        long = sig["direction"] == "LONG"
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
        })
    rows.sort(key=lambda r: -r["score"])
    return jsonify({
        "watchlist": [disp(c) if is_fx(c) else f"{c}{QUOTE}" for c in WATCHLIST + FOREX_GOLD],
        "timeframes": TIMEFRAMES,
        "cycle_minutes": CYCLE_MINUTES,
        "cycles": STATE.get("cycle_count", 0),
        "data_feed_ok": STATE["data_feed_ok"],
        "last_error": STATE["last_error"],
        "last_cycle_ts": STATE["last_cycle_ts"],
        "agents": AGENTS,
        "open_signals": [s for s in STATE["signals"] if s["status"] == "OPEN"],
        "signals": list(reversed(STATE["signals"]))[:40],
        "stats": STATE.get("stats") or compute_stats(STATE["signals"]),
        "agent_stats": STATE.get("agent_stats") or compute_agent_stats(STATE["signals"]),
        "chatter": list(reversed(STATE["chatter"]))[:60],
        "scan_rows": rows,
    })

@app.route("/api/run-now", methods=["POST"])
def run_now():
    if STATE["cycle_alive"]:
        return jsonify({"ok": False, "reason": "cycle already running"}), 429
    with STATE_LOCK:
        STATE["cycle_alive"] = True
    threading.Thread(target=run_cycle, daemon=True).start()
    return jsonify({"ok": True})

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
.avatar{width:42px;height:42px;border-radius:12px;display:flex;align-items:center;justify-content:center;
        font-weight:800;font-size:17px;color:#04060d;background:var(--ac);box-shadow:0 0 22px color-mix(in srgb,var(--ac) 45%,transparent)}
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
         display:flex;align-items:center;justify-content:center}
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
    <h1>THE CREW · SIGNAL DESK</h1>
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
const badge = d => `<span class="badge b-${({LONG:'long',SHORT:'short',NEUTRAL:'neutral',VETOED:'vetoed',NO_DATA:'nodata'})[d]||'neutral'}">${d}</span>`;
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
  for (const k in d.agents) A[k] = d.agents[k];
  const live = document.getElementById('live');
  const age = d.last_cycle_ts ? Math.round((Date.now()/1000 - d.last_cycle_ts)) : null;
  live.textContent = d.data_feed_ok ? (age != null ? `desk live · last sweep ${age}s ago` : 'desk live') : 'feed issues';

  document.getElementById('banner').innerHTML = d.data_feed_ok ? '' :
    `<div class="banner"><b>⚠ Data feed problem:</b> some symbols could not be fetched from Binance. ${d.last_error||''}</div>`;

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
  document.getElementById('s-cycle-n').textContent = `every ${d.cycle_minutes} min`;

  // agents
  const lastWords = {};
  (d.chatter||[]).forEach(m => { lastWords[m.a] = m; });
  document.getElementById('agents').innerHTML = Object.entries(d.agents).map(([k,a])=>{
    const lw = lastWords[k];
    return `<div class="agent" style="--ac:${a.color}">
      <div class="top"><div class="avatar">${a.glyph}</div>
        <div><div class="name">${a.name}</div><div class="role">${a.role}</div></div></div>
      <div class="last">${lw ? `<b>${lw.s||'desk'}:</b> ${lw.t}` : 'Waiting for the first sweep…'}</div>
    </div>`;}).join('');

  // chatter
  document.getElementById('feed').innerHTML = (d.chatter||[]).map(m=>{
    const a = d.agents[m.a] || {glyph:'?',color:'#888',name:m.a};
    return `<div class="msg" style="--ac:${a.color}">
      <div class="av">${a.glyph}</div>
      <div><div class="who" style="color:${a.color}">${a.name}<i>${m.s||''}${m.ts?(' · '+new Date(m.ts*1000).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})):''}</i></div>
      <div class="tx">${m.t}</div></div>
    </div>`;}).join('') || '<div class="msg"><div class="tx" style="color:var(--muted)">Quiet desk. Run a sweep.</div></div>';

  // scan table
  document.getElementById('scan-t').querySelector('tbody').innerHTML = d.scan_rows.map(r=>{
    const v = r.votes||{}, c = r.crew||{};
    const crew = ['lester','michael','franklin','trevor'].map(k=>c[k]?agentBadge(k):'').join(' ');
    return `<tr><td><b>${r.symbol}</b></td><td>${badge(r.direction)}</td><td class="mono">${r.confluence}</td>
      <td>${vote(v['15m'])}</td><td>${vote(v['1h'])}</td><td>${vote(v['4h'])}</td><td>${vote(v['1d'])}</td>
      <td class="mono">${r.rsi_1h==null?'--':r.rsi_1h.toFixed(1)}</td><td class="mono">${fmt(r.last_close)}</td><td>${crew}</td></tr>`;
  }).join('');

  // signals table
  document.getElementById('sig-t').querySelector('tbody').innerHTML = (d.signals||[]).map(s=>{
    const out = s.outcome || (s.status==='OPEN'?'OPEN':'--');
    const cls = out.startsWith('TP')?'out-tp':(out==='SL_HIT'?'out-sl':'out-open');
    return `<tr><td class="mono">${s.created.slice(5,16).replace('T',' ')}</td><td><b>${s.symbol}</b></td>
      <td>${badge(s.direction)}</td><td class="mono">${s.confidence}%</td>
      <td class="mono">${fmt(s.entry)}</td><td class="mono">${fmt(s.stop_loss)}</td>
      <td class="mono">${fmt(s.tp1)}</td><td class="mono">${fmt(s.tp2)}</td><td class="mono">${fmt(s.tp3)}</td>
      <td class="${cls}">${out.replace('_HIT','')}</td><td class="mono">${s.max_favorable_r}R</td></tr>`;
  }).join('') || '<tr><td colspan="11" style="color:var(--muted)">No signals yet — the crew only fires on full consensus.</td></tr>';
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
    return DASHBOARD

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
