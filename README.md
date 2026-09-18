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
