# BTC System Monitor

Read-only Telegram monitoring bot for BTC ecosystem.

## Files
- `btc_monitor_bot.py`
- `requirements.txt`
- `Procfile`
- `.env.example`

## Railway Variables
```txt
MONITOR_BOT_TOKEN=
MONITOR_CHAT_ID=
BRAIN_OPS_URL=https://web-production-f47d4.up.railway.app
CHECK_INTERVAL_SEC=300
DAILY_SUMMARY_HOUR=8
DAILY_SUMMARY_MINUTE=0
```

## Commands
- `/status`
- `/summary`
- `/help`

## Notes
This bot is read-only. It does not trade, execute, or modify core systems.
