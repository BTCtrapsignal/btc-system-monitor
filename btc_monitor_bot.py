"""
BTC System Monitor — Warm Watchtower
Read-only Telegram monitoring bot for BTC ecosystem.

Commands:
  /status
  /help

Railway Variables:
  MONITOR_BOT_TOKEN
  MONITOR_CHAT_ID
  BRAIN_OPS_URL
  CHECK_INTERVAL_SEC
"""

import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from statistics import mean

import httpx


# ─────────────────────────────────────────────────────────────
# Time helpers must be defined before state initialization
# ─────────────────────────────────────────────────────────────

TH_TZ = timezone(timedelta(hours=7))

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def now_th() -> str:
    return datetime.now(TH_TZ).strftime("%d %b %Y %H:%M:%S")

def today_key() -> str:
    return datetime.now(TH_TZ).strftime("%Y-%m-%d")

def parse_iso(value):
    if not value:
        return None

    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None

def rel_time(value) -> str:
    dt = parse_iso(value)

    if not dt:
        return "—"

    diff = (datetime.now(timezone.utc) - dt).total_seconds()

    if diff < 0:
        return "just now"
    if diff < 60:
        return f"{int(diff)}s ago"
    if diff < 3600:
        return f"{int(diff // 60)}m ago"
    if diff < 86400:
        return f"{int(diff // 3600)}h ago"

    return f"{int(diff // 86400)}d ago"


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger("btc-system-monitor")


# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

MONITOR_VERSION = "v1.3.2-hotfix-real"

BOT_TOKEN = os.getenv("MONITOR_BOT_TOKEN")
ALLOWED_ID = str(os.getenv("MONITOR_CHAT_ID", "")).strip()

BRAIN_URL = os.getenv(
    "BRAIN_OPS_URL",
    "https://web-production-f47d4.up.railway.app"
).rstrip("/")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SEC", "300"))
STATE_FILE = Path(os.getenv("MONITOR_STATE_FILE", "monitor_state.json"))

if not BOT_TOKEN:
    raise RuntimeError("MONITOR_BOT_TOKEN missing")

if not ALLOWED_ID:
    raise RuntimeError("MONITOR_CHAT_ID missing")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"


# ─────────────────────────────────────────────────────────────
# Persistent lightweight state
# ─────────────────────────────────────────────────────────────

def default_state():
    return {
        "previous": {
            "brain_ops": "unknown",
            "layer_a": "unknown",
        },
        "metrics": {
            "alerts_today": 0,
            "recoveries_today": 0,
            "last_reset_date": today_key(),
            "latencies": [],
            "checks_total": 0,
            "brain_ok_total": 0,
        },
        "timestamps": {
            "last_monitor_check": None,
            "last_brain_ok": None,
            "last_startup_sent": None,
        }
    }

def deep_merge(base: dict, override: dict) -> dict:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            base[k] = deep_merge(base[k], v)
        else:
            base[k] = v
    return base

class StateManager:
    def __init__(self, path: Path):
        self.path = path
        self.state = default_state()
        self.load()

    def load(self):
        try:
            if self.path.exists():
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                self.state = deep_merge(default_state(), loaded)
        except Exception as e:
            log.warning(f"state load failed: {e}")
            self.state = default_state()

    def save(self):
        try:
            self.path.write_text(
                json.dumps(self.state, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            log.warning(f"state save failed: {e}")

    def reset_daily_if_needed(self):
        if self.state["metrics"].get("last_reset_date") != today_key():
            self.state["metrics"]["alerts_today"] = 0
            self.state["metrics"]["recoveries_today"] = 0
            self.state["metrics"]["last_reset_date"] = today_key()
            self.save()

    def record_latency(self, value):
        try:
            latency = int(value)
        except Exception:
            return

        arr = self.state["metrics"].setdefault("latencies", [])
        arr.append(latency)
        self.state["metrics"]["latencies"] = arr[-60:]

    def latency_stats(self):
        arr = self.state["metrics"].get("latencies", [])

        if not arr:
            return {"current": 0, "avg": 0, "peak": 0, "status": "unknown"}

        current = int(arr[-1])
        avg = int(mean(arr))
        peak = int(max(arr))

        if avg <= 600 and peak <= 1500:
            status = "stable"
        elif avg <= 1200 and peak <= 3000:
            status = "elevated"
        else:
            status = "degraded"

        return {"current": current, "avg": avg, "peak": peak, "status": status}


state_mgr = StateManager(STATE_FILE)


# ─────────────────────────────────────────────────────────────
# Telegram helpers
# ─────────────────────────────────────────────────────────────

async def send(client: httpx.AsyncClient, text: str):
    try:
        await client.post(
            f"{TELEGRAM_API}/sendMessage",
            json={
                "chat_id": ALLOWED_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10
        )
    except Exception as e:
        log.warning(f"send failed: {e}")

async def get_updates(client: httpx.AsyncClient, offset: int):
    try:
        r = await client.get(
            f"{TELEGRAM_API}/getUpdates",
            params={
                "offset": offset,
                "timeout": 5,
                "allowed_updates": ["message"],
            },
            timeout=12
        )
        return r.json().get("result", [])

    except Exception as e:
        log.warning(f"getUpdates failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# Read-only monitor fetcher
# ─────────────────────────────────────────────────────────────

async def fetch_monitor(client: httpx.AsyncClient) -> dict:
    result = {
        "brain_ok": False,
        "brain_lat": 0,
        "brain_version": "?",
        "brain_uptime": "?",
        "brain_signals": 0,
        "overall": "unknown",

        "layer_a_status": "no_data",
        "layer_a_last": None,
        "layer_a_open": 0,
        "signals_today": None,
        "signals_last_hour": None,

        "layer_c_status": "observer_mode",
        "layer_c_last": None,
        "layer_c_version": None,

        "db_status": "unknown",
        "resources": {
            "cpu": None,
            "ram": None,
            "disk": None,
        },
    }

    try:
        r = await client.get(f"{BRAIN_URL}/health", timeout=8)

        if r.status_code == 200:
            result["brain_ok"] = True
            result["brain_lat"] = int(r.elapsed.total_seconds() * 1000)

            try:
                h = r.json()
            except Exception:
                h = {}

            result["brain_version"] = (
                h.get("version")
                or h.get("app_version")
                or h.get("release")
                or "?"
            )

            result["brain_uptime"] = (
                h.get("uptime_human")
                or h.get("uptime")
                or result["brain_uptime"]
            )

            db = h.get("db") or h.get("database")
            if isinstance(db, dict):
                result["db_status"] = db.get("status") or db.get("health") or result["db_status"]
            elif isinstance(db, str):
                result["db_status"] = db

    except Exception as e:
        log.warning(f"health fetch failed: {e}")

    if not result["brain_ok"]:
        return result

    try:
        r2 = await client.get(f"{BRAIN_URL}/monitor/status", timeout=8)

        if r2.status_code == 200:
            try:
                m = r2.json()
            except Exception:
                m = {}

            result["overall"] = m.get("overall", result["overall"])

            a = m.get("layer_a", {}) or {}
            b = m.get("layer_b", {}) or {}
            c = m.get("layer_c", {}) or {}
            db = m.get("db") or m.get("database")

            result["layer_a_status"] = a.get("status", result["layer_a_status"])
            result["layer_a_last"] = a.get("last_signal_ts") or a.get("last_signal") or result["layer_a_last"]
            result["layer_a_open"] = a.get("open_trades_in_db", result["layer_a_open"])
            result["signals_today"] = a.get("signals_today") or a.get("today_signals")
            result["signals_last_hour"] = a.get("signals_last_hour") or a.get("last_hour_signals")

            result["brain_uptime"] = b.get("uptime_human", result["brain_uptime"])
            result["brain_signals"] = b.get("total_signals_stored", result["brain_signals"])
            result["brain_version"] = b.get("version", result["brain_version"])

            result["layer_c_status"] = normalize_reflex(c.get("status", result["layer_c_status"]))
            result["layer_c_last"] = c.get("last_heartbeat") or c.get("last_sync") or c.get("updated_at")
            result["layer_c_version"] = c.get("version")

            if isinstance(db, dict):
                result["db_status"] = db.get("status") or db.get("health") or result["db_status"]
            elif isinstance(db, str):
                result["db_status"] = db

    except Exception as e:
        log.warning(f"monitor status fetch failed: {e}")

    return result


# ─────────────────────────────────────────────────────────────
# Format helpers
# ─────────────────────────────────────────────────────────────

def normalize_reflex(status):
    if not status:
        return "observer_mode"

    s = str(status).lower()

    if s in ("unconfirmed", "observer", "observer_mode"):
        return "observer_mode"

    return s

def normalize_db(status):
    if not status:
        return "unknown"

    s = str(status).lower()

    if s in ("ok", "healthy", "connected", "online", "up"):
        return "connected"
    if s in ("warn", "warning", "degraded", "slow"):
        return "degraded"
    if s in ("down", "error", "failed", "disconnected", "offline"):
        return "disconnected"

    return s

def layer_a_icon(status):
    if status == "active":
        return "🟢"
    if status == "idle":
        return "🟡"
    if status == "likely_down":
        return "🔴"
    return "⚪"

def layer_a_text(status):
    if status == "active":
        return "Active"
    if status == "idle":
        return "Idle"
    if status == "likely_down":
        return "Inactivity detected"
    if status == "no_data":
        return "Waiting for first signal"
    return str(status).replace("_", " ").title()

def reflex_icon(status):
    if normalize_reflex(status) == "active":
        return "🟢"
    return "🟡"

def reflex_text(status):
    if normalize_reflex(status) == "observer_mode":
        return "Observer Mode"
    return str(status).replace("_", " ").title()

def db_icon(status):
    s = normalize_db(status)
    if s == "connected":
        return "🟢"
    if s == "degraded":
        return "🟡"
    if s == "disconnected":
        return "🔴"
    return "⚪"

def db_text(status):
    return normalize_db(status).replace("_", " ").title()

def latency_icon(status):
    if status == "stable":
        return "🟢"
    if status == "elevated":
        return "🟡"
    if status == "degraded":
        return "🔴"
    return "⚪"

def safe(value, fallback="not reported"):
    return fallback if value is None else value

def build_status_message(d: dict) -> str:
    latency = state_mgr.latency_stats()

    brain_icon = "🟢" if d["brain_ok"] else "🔴"
    brain_text = (
        f"online · {d['brain_lat']}ms · uptime {d['brain_uptime']}"
        if d["brain_ok"]
        else "unavailable"
    )

    return (
        "🛡️ <b>BTC System Monitor</b>\n"
        "<i>Warm Watchtower · Read-only monitoring</i>\n\n"

        "<b>Nodes</b>\n"
        f"{brain_icon} Brain Ops — {brain_text}\n"
        f"{layer_a_icon(d['layer_a_status'])} Signal Bot — {layer_a_text(d['layer_a_status'])}\n"
        f"{reflex_icon(d['layer_c_status'])} Reflex Engine — {reflex_text(d['layer_c_status'])}\n"
        "🟢 Monitor Node — online\n"
        f"{db_icon(d['db_status'])} Database — {db_text(d['db_status'])}\n\n"

        "<b>Signals</b>\n"
        f"• today: {safe(d.get('signals_today'))}\n"
        f"• last 1h: {safe(d.get('signals_last_hour'))}\n"
        f"• last signal: {rel_time(d.get('layer_a_last'))}\n"
        f"• open trades: {d.get('layer_a_open', 0)}\n\n"

        "<b>Latency</b>\n"
        f"{latency_icon(latency['status'])} {latency['status']}\n"
        f"• current: {latency['current']}ms\n"
        f"• avg: {latency['avg']}ms\n"
        f"• peak: {latency['peak']}ms\n\n"

        "<b>Heartbeat</b>\n"
        f"• monitor check: {rel_time(state_mgr.state['timestamps'].get('last_monitor_check'))}\n"
        f"• Brain API: {rel_time(state_mgr.state['timestamps'].get('last_brain_ok'))}\n"
        f"• Reflex sync: {rel_time(d.get('layer_c_last'))}\n\n"

        "<b>Versions</b>\n"
        f"• Brain Ops: {d.get('brain_version') or '?'}\n"
        f"• Monitor: {MONITOR_VERSION}\n"
        f"• Reflex: {d.get('layer_c_version') or reflex_text(d.get('layer_c_status'))}\n\n"

        f"🕐 {now_th()}"
    )

def build_help_message() -> str:
    return (
        "🛡️ <b>BTC System Monitor</b>\n\n"
        "<b>Commands</b>\n"
        "/status — full ecosystem status\n"
        "/help — help\n\n"
        "<b>Role</b>\n"
        "Read-only watchdog for BTC ecosystem.\n"
        "No trading. No execution. No modification of core systems."
    )


# ─────────────────────────────────────────────────────────────
# Alerts
# ─────────────────────────────────────────────────────────────

async def process_alerts(client: httpx.AsyncClient, d: dict):
    state_mgr.reset_daily_if_needed()

    prev = state_mgr.state["previous"]
    metrics = state_mgr.state["metrics"]

    brain_now = "ok" if d["brain_ok"] else "down"

    if brain_now == "down" and prev.get("brain_ops") != "down":
        metrics["alerts_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "⚠️ Brain Ops unavailable\n"
            f"🕐 {now_th()}"
        )

    elif brain_now == "ok" and prev.get("brain_ops") == "down":
        metrics["recoveries_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "✅ Brain Ops recovered\n"
            f"Latency: {d['brain_lat']}ms\n"
            f"🕐 {now_th()}"
        )

    prev["brain_ops"] = brain_now

    a_now = d.get("layer_a_status", "no_data")

    if a_now == "likely_down" and prev.get("layer_a") != "likely_down":
        metrics["alerts_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "⚠️ Signal Bot inactivity detected\n"
            f"Last signal: {rel_time(d.get('layer_a_last'))}\n"
            f"🕐 {now_th()}"
        )

    elif a_now in ("active", "idle") and prev.get("layer_a") == "likely_down":
        metrics["recoveries_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "✅ Signal Bot activity recovered\n"
            f"🕐 {now_th()}"
        )

    prev["layer_a"] = a_now

    state_mgr.save()


# ─────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────

async def auto_check(client: httpx.AsyncClient):
    d = await fetch_monitor(client)

    state_mgr.state["timestamps"]["last_monitor_check"] = now_iso()
    state_mgr.state["metrics"]["checks_total"] += 1

    if d["brain_ok"]:
        state_mgr.state["timestamps"]["last_brain_ok"] = now_iso()
        state_mgr.state["metrics"]["brain_ok_total"] += 1
        state_mgr.record_latency(d["brain_lat"])

    state_mgr.save()

    await process_alerts(client, d)

    return d

async def main():
    log.info(f"BTC System Monitor starting — interval={CHECK_INTERVAL}s")

    offset = 0
    last_check = 0.0

    async with httpx.AsyncClient() as client:
        await send(
            client,
            "🛡️ <b>BTC System Monitor online</b>\n\n"
            f"Version: {MONITOR_VERSION}\n"
            f"Auto-check every {CHECK_INTERVAL // 60} minute(s)"
        )

        while True:
            try:
                loop_now = asyncio.get_event_loop().time()

                if loop_now - last_check >= CHECK_INTERVAL:
                    await auto_check(client)
                    last_check = loop_now

                updates = await get_updates(client, offset)

                for update in updates:
                    offset = update["update_id"] + 1

                    msg = update.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id", ""))
                    text = msg.get("text", "").strip()

                    if chat_id != ALLOWED_ID:
                        log.warning(f"unauthorized chat_id={chat_id}")
                        continue

                    if text.startswith("/status"):
                        await send(client, "⏳ Checking ecosystem status...")

                        d = await fetch_monitor(client)

                        state_mgr.state["timestamps"]["last_monitor_check"] = now_iso()

                        if d["brain_ok"]:
                            state_mgr.state["timestamps"]["last_brain_ok"] = now_iso()
                            state_mgr.record_latency(d["brain_lat"])

                        state_mgr.save()

                        await send(client, build_status_message(d))

                    elif text.startswith(("/help", "/start")):
                        await send(client, build_help_message())

                await asyncio.sleep(1)

            except Exception as e:
                log.exception(f"main loop recovered from error: {e}")
                await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
