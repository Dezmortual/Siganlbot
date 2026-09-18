# Signal Scanner Bot

A Tradebise-style signal scanner, built on free public market data (Binance spot
API — no key, no account, no paywall). It scans a watchlist across four
timeframes (15m / 1h / 4h / 1d) and only fires a signal when at least 3 of 4
timeframes agree (multi-timeframe confluence).

Every signal comes with:
- Entry price
- Stop-loss (1.5 × ATR)
- TP1 / TP2 / TP3 (1R / 2R / 3R)
- Confidence score

## Self-auditing

The bot tracks every signal it emits and checks it against live prices on each
cycle. A signal closes at TP3 or SL, and the dashboard shows the REAL win rate,
average R, and best/worst trades — like Tradebise's "we publish our losses next
to our wins", except you can verify it yourself.

## Deploy on Render (same as your other bots)

1. Create a new Web Service, connect your GitHub repo.
2. Upload the two files from this zip (app.py + requirements.txt) to the repo
   root (no subfolders — the GitHub web UI drops folders).
3. Build command: `pip install -r requirements.txt`
4. Start command:
   `gunicorn --workers 1 --threads 4 --timeout 60 --bind 0.0.0.0:$PORT app:app`
5. Optional: add environment variables from env_example.txt (WATCHLIST,
   CYCLE_MINUTES, etc.). Defaults work fine.

## Keep-alive

Like your other bots on Render free tier, the background loop needs help
surviving. The built-in watchdog restarts a scan if a web request arrives and
the last scan is stale, so point UptimeRobot (or cron-job.org) at `/health`
every 10 minutes.

## Endpoints

- `/` — dashboard (scan table, signals, win rate)
- `/health` — keep-alive ping
- `/api/status` — JSON status + open signals + stats
- `/api/signals?limit=50` — signal history JSON
- `/api/run-now` (POST) — trigger an immediate scan

## Honest disclaimer

No scanner gives "accurate" signals. This one only fires on strong confluence
and shows you its verified hit rate so you can judge it with data. Signals are
analysis, not financial advice. DYOR, never trade money you can't afford to lose.
