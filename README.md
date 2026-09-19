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

## Character faces (v1.6)

Each crew member now has an original face on the dashboard — cel-shaded
portraits with neon rim-lighting matched to each agent's accent color.
- Lester (cyan) — sharp-eyed strategist, glasses up on the forehead
- Michael (violet) — calm, arms crossed, momentum reader
- Franklin (amber) — heavyset risk manager with the no-nonsense stare
- Trevor (red) — the unhinged grin of the executor

Faces load from a public CDN and are served through the agents API; if an
image ever fails to load, the card gracefully falls back to the old letter
avatar. Same deploy pattern: replace app.py in GitHub, Render redeploys.

## Backtest Lab + Ledger Grade (v1.7)

**Backtest Lab** — the "▶ Run 90-day backtest" button replays history through the
EXACT live crew logic (same base_read / agent filters / crew_review code path —
not a re-implementation). Scorecard per symbol: trades, SL rate, TP3 rate,
TP1-tag rate, avg R, total R, max drawdown in R, and a verdict (EDGE / FLAT /
AVOID / SMALL N). Crypto replays off Binance history; forex/gold off Yahoo
(FX 15m depth is capped at ~55 days). Trades that never close are honest: SL
counts as -1R, TP3 as +3R, TP1/TP2 tags are tracked but the trade runs to SL
or TP3 — same management as the live desk.

**Ledger Grade** — after every sweep the desk re-grades its own closed ledger:
- Symbols with 5+ closed trades and negative avg R get BENCHED (can_emit
  blocks them; Franklin announces the bench in desk chatter).
- UTC hours with 5+ closed trades and negative avg R become learned VETO
  hours — Franklin blocks any entry born in a proven losing hour.
Both self-heal: when the numbers improve (or the ledger shifts), the bench
and veto lists recompute automatically on the next sweep.

Env knobs: BT_DAYS (default 90), BENCH_MIN_TRADES (5), BENCH_AVG_R (0.0).
POST /api/backtest {"days":90, "symbols":["BTCUSDT", ...]} for targeted runs.

## Breakeven at TP1 (v1.8)

Once a trade tags +1R, the stop rides to entry — a free trade from there. The
ledger shows BE as the outcome (0R). Env knob: BE_AT_TP1=0 disables it.

The backtester runs the rule as a shadow A/B on every replay: the scorecard
shows Avg R (raw) vs Avg R (BE), a ΔR column (what the rule added over the
window), and how many losses it saved. Verdicts are graded on the BE-managed
path — the same management the live desk uses. Verified: BTC 90d -1.0 → -0.33
avg R, EURUSD -1.0 → -0.5, winners (SOL) untouched (they run to TP3 anyway).

## Weekend behavior + Yahoo hardening (v1.8.1)

FX and gold trade ~24/5 — on weekends the desk correctly refuses to vote on
stale bars. Those rows now show "MARKET CLOSED" in the dashboard instead of
the alarming "NO_DATA" (a real feed failure still shows NO_DATA). Crypto
scans 24/7.

All Yahoo calls (live sweep + backtest lab) now share one pacer: a request
every YAHOO_MIN_INTERVAL (0.5s default) with a retry pass, so running a full
backtest no longer starves the live sweep into NO_DATA via throttling.

## Fake timeout fix (v1.8.2)

Root cause of persistent "reads went stale, watchdog + manual sweep both
stuck" incidents even with an external pinger running: every network fetch
used `with ThreadPoolExecutor(...) as ex:` to enforce a 20s hard bound via
`fut.result(timeout=...)`. That pattern is broken — Python's context manager
calls `shutdown(wait=True)` on exit, which blocks until the worker thread
actually finishes, regardless of whether `.result()` already raised a
timeout. A single genuinely-hung DNS/socket call (rare, but happens on
Render's network) could freeze that "bounded" fetch forever, which froze the
whole cycle, which left `cycle_alive` stuck True forever — so neither the
watchdog nor the "Run sweep" button could ever recover it. Only a manual
restart cleared it.

Fixed by dropping the blocking `with` and calling `ex.shutdown(wait=False)`
in a `finally`, so a hung fetch is abandoned (leaked, dies on its own later)
instead of blocking the caller. Verified with a simulated 60s hang: fetch now
returns in the intended 20s instead of blocking the full 60s.

## Version marker (v1.8.3)

The dashboard title now shows the running version (e.g. "v1.8.3") and
/api/status exposes `"version"`. Check what's live at a glance — no more
guessing which zip is deployed. Override with the APP_VERSION env var.
