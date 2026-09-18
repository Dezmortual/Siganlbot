# THE CREW · Signal Desk

Four AI agents run one trading desk. A signal fires only on **full crew consensus**.

| Agent | Job | What they check |
|-------|-----|-----------------|
| **Lester** (analyst) | Structure | EMA 20/50/200 stack across 15m / 1h / 4h / 1d — needs 3+ timeframes agreeing |
| **Michael** (momentum) | Fuel | 1h RSI must have room to run — he vetoes blow-off tops and knife catches |
| **Franklin** (risk) | Risk rails | Audits ATR geometry — vetoes anything with crazy volatility or stretched RSI |
| **Trevor** (executor) | Execution | Fires only after Lester + Michael agree AND Franklin approves |

Every signal carries entry, stop-loss (1.5× ATR) and TP1/TP2/TP3 at 1R/2R/3R.
The ledger self-audits: each closed signal is tracked to TP or SL, and the
dashboard's win rate is computed from real closed trades — not marketing.

The desk chatter feed shows the agents discussing every sweep live, including
when they REFUSE a trade (that's the discipline working).

## Files

- `app.py` — everything (single file, no subfolders)
- `requirements.txt` — flask, requests, gunicorn
- `env_example.txt` — optional settings (watchlist, cycle time, RSI rails)
- `README.md` — this file

## Deploy on Render

1. Create a new **Web Service** from your GitHub repo (upload all 4 files to the repo ROOT — the GitHub web UI drops subfolders, so keep everything flat).
2. Environment: Python 3.
3. Build command: `pip install -r requirements.txt`
4. Start command: `gunicorn --workers 1 --threads 4 --timeout 60 --bind 0.0.0.0:$PORT app:app`
5. Add a free external pinger (UptimeRobot / cron-job.org) hitting `https://YOUR-APP.onrender.com/health` every 10 minutes so the watchdog keeps the desk trading on the free tier.

No API keys needed — it uses Binance spot public data.

## Notes

- Signals persist to `signals.json` (survives restarts on the same disk).
- A trade fires only when Lester AND Michael agree on direction AND Franklin approves. Trevor then executes.
- This is a signal tool, not financial advice. It never places orders.

## Forex + Gold feed (v1.1)

The desk now also scans **EURUSD, GBPUSD, USDJPY, AUDUSD and GOLD** (COMEX gold
futures via Yahoo Finance, which closely tracks spot XAU/USD). Crypto still comes
from Binance; FX/gold comes from Yahoo Finance's free chart API — no API key needed.

- Config: `FOREX_GOLD` env var (default `EURUSD=X,GBPUSD=X,USDJPY=X,AUDUSD=X,GC=F`).
  Set `FOREX_GOLD=""` to disable, or add pairs like `USDCAD=X`, `EURJPY=X`, `XAUEUR=X`.
- The 4h candles are resampled from Yahoo's 1h bars; all crew logic (EMA confluence,
  RSI momentum, Franklin's ATR rails) runs identically on every symbol.
- Forex/gold trade ~24/5: on weekends the desk detects stale candles (last 15m bar
  older than 3h) and marks those symbols NO_DATA without flagging a feed problem.

## Momentum filter tightened (v1.3) — fixes whipsaw shorts/longs

Root cause of "signals go the opposite way": Michael's momentum check only
rejected *overbought/oversold* RSI, so a SHORT could fire on any RSI as high
as 55 and a LONG on any RSI as low as 45 — a near-50 "coin flip" zone with
no real directional edge. That's exactly what happened to a GBPUSD SHORT that
fired at RSI 52.7 and went straight to its stop with zero favorable movement.

There was also a bug where the SHORT-side threshold was hardcoded to `55`
instead of respecting the `MO_FLOOR` env var, so tuning it via config never
actually worked.

Fix: `MO_FLOOR` now defaults to `50` (was `45`), and the SHORT side correctly
mirrors it as `100 - MO_FLOOR`. Momentum must now be clearly on the correct
side of neutral before Michael confirms it — no more entries in the dead zone.
Tune `MO_FLOOR` env var higher (e.g. 55) for even stricter confirmation.

## Faster sweeps + staleness warning (v1.4)

The "SHORT" reads you see can only be as fresh as the last sweep. At the old
30-minute cycle, a fast-moving forex reversal could flip the real market
completely before the desk caught up — that's the "wrong direction" you were
seeing; the read wasn't broken, it was up to 30+ min stale (worse if Render's
free tier had put the app to sleep between visits).

Fixes:
- `CYCLE_MINUTES` default dropped from 30 to **5** — reads catch up to the
  market much faster. Tune via env var if you want it even tighter/looser.
- The dashboard now shows a **loud red banner** directly on the page if the
  last sweep is stale (> max(1.5x cycle, 10 min) old), telling you to hit
  "Run sweep" or that the free-tier instance was asleep.
- This makes the **keep-alive pinger** (UptimeRobot -> /health every 10 min)
  even more important now: without it, "every 5 min" cycles only actually
  happen while someone's got the tab open.

## Character accuracy upgrades (v1.5)

Every agent got a professionally-standard second layer of confirmation
(grounded in standard technical-analysis practice: ADX 20-25+ strength
thresholds, RSI+MACD double momentum confirmation, liquidity-session
filtering, anti-chase entry discipline):

- **Lester (structure)** now requires 1h **ADX >= 20** (trend strength) and
  that the dominant DI matches direction — a 3/4 EMA stack in weak/choppy
  conditions is no longer enough. Configurable via `ADX_MIN`.
- **Michael (momentum)** now requires **MACD(12,26,9) confirmation** on the 1h
  alongside RSI: histogram positive and rising for LONGs, negative and
  falling for SHORTs. RSI alone can't confirm fuel anymore.
- **Franklin (risk)** adds two new vetoes: **rollover-hour veto** (no FX/gold
  entries 21:45-22:15 UTC, when spreads widen and moves lie — toggle
  `FX_ROLLOVER_VETO`) and an **anti-chase veto** (price more than
  `MAX_EXT_ATR`=2.5 ATRs from the 20 EMA = stretched move, wait for the
  pullback).
- **The vote engine** no longer casts counter-trend votes from RSI extremes
  when EMAs aren't stacked — this was the "opposite signal" class of bug
  (firing LONGs into falling markets because RSI was oversold).
- **Confidence scores** are now strength-weighted: ADX 25+/30+ and MACD
  alignment add to the score, capped at 95% (no more fake 100%).
