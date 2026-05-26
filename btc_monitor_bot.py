"""
BTC System Monitor — Production Watchtower Mode
Read-only Telegram monitoring bot for BTC ecosystem.

Commands:
  /status  - show full ecosystem status
  /help    - show commands
  /summary - show current runtime summary

Railway Variables:
  MONITOR_BOT_TOKEN
  MONITOR_CHAT_ID
  BRAIN_OPS_URL
  CHECK_INTERVAL_SEC
  DAILY_SUMMARY_HOUR
  DAILY_SUMMARY_MINUTE
"""

import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from collections import deque
from statistics import mean

import httpx


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

MONITOR_VERSION = "v1.3.0-watchtower"

BOT_TOKEN = os.getenv("MONITOR_BOT_TOKEN")
ALLOWED_ID = str(os.getenv("MONITOR_CHAT_ID", "")).strip()

BRAIN_URL = os.getenv(
    "BRAIN_OPS_URL",
    "https://web-production-f47d4.up.railway.app"
).rstrip("/")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SEC", "300"))

DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", "8"))
DAILY_SUMMARY_MINUTE = int(os.getenv("DAILY_SUMMARY_MINUTE", "0"))

STATE_FILE = Path(os.getenv("MONITOR_STATE_FILE", "monitor_state.json"))

TH_TZ = timezone(timedelta(hours=7))

if not BOT_TOKEN:
    raise RuntimeError("MONITOR_BOT_TOKEN missing")

if not ALLOWED_ID:
    raise RuntimeError("MONITOR_CHAT_ID missing")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"


# ─────────────────────────────────────────────────────────────
# State Manager
# ─────────────────────────────────────────────────────────────

def default_state():
    return {
        "previous": {
            "brain_ops": "unknown",
            "layer_a": "unknown",
            "db": "unknown",
        },
        "metrics": {
            "alerts_today": 0,
            "recoveries_today": 0,
            "last_reset_date": _today_key(),
            "latencies": [],
            "peak_latency_ms": 0,
            "checks_total": 0,
            "brain_ok_total": 0,
        },
        "timestamps": {
            "last_monitor_check": None,
            "last_brain_ok": None,
            "last_daily_summary_date": None,
            "first_started_at": _now_iso(),
            "last_startup_sent": None,
        },
        "events": {
            "brain_down_since": None,
            "layer_a_down_since": None,
            "recent_restarts": [],
        }
    }


class StateManager:
    def __init__(self, path: Path):
        self.path = path
        self.state = default_state()
        self.load()

    def load(self):
        try:
            if self.path.exists():
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                self.state = _deep_merge(default_state(), loaded)
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
        today = _today_key()
        if self.state["metrics"].get("last_reset_date") != today:
            self.state["metrics"]["alerts_today"] = 0
            self.state["metrics"]["recoveries_today"] = 0
            self.state["metrics"]["last_reset_date"] = today
            self.save()

    def record_latency(self, latency_ms: int):
        latencies = self.state["metrics"].setdefault("latencies", [])
        latencies.append(int(latency_ms))
        self.state["metrics"]["latencies"] = latencies[-60:]
        self.state["metrics"]["peak_latency_ms"] = max(
            int(self.state["metrics"].get("peak_latency_ms", 0)),
            int(latency_ms)
        )

    def latency_stats(self):
        latencies = self.state["metrics"].get("latencies", [])
        if not latencies:
            return {
                "current": 0,
                "avg": 0,
                "peak": int(self.state["metrics"].get("peak_latency_ms", 0)),
                "status": "unknown",
            }

        current = int(latencies[-1])
        avg = int(mean(latencies))
        peak = int(max(latencies))

        if avg <= 600 and peak <= 1500:
            status = "stable"
        elif avg <= 1200 and peak <= 3000:
            status = "elevated"
        else:
            status = "degraded"

        return {
            "current": current,
            "avg": avg,
            "peak": peak,
            "status": status,
        }


state_mgr = StateManager(STATE_FILE)


# ─────────────────────────────────────────────────────────────
# Telegram
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
        log.warning(f"telegram send failed: {e}")


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
        data = r.json()
        return data.get("result", [])
    except Exception as e:
        log.warning(f"getUpdates failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# Monitor Service
# ─────────────────────────────────────────────────────────────

async def fetch_monitor(client: httpx.AsyncClient) -> dict:
    """
    Read-only health fetcher.
    Supports current /health and /monitor/status schema.
    Also gracefully reads future optional fields if backend adds them.
    """

    result = {
        "brain_ok": False,
        "brain_lat": 0,
        "brain_version": "?",
        "brain_uptime": "?",
        "brain_uptime_seconds": None,
        "brain_signals": 0,

        "overall": "unknown",

        "layer_a_status": "no_data",
        "layer_a_last": None,
        "layer_a_open": 0,
        "signals_today": None,
        "signals_last_hour": None,
        "signal_inactivity": None,

        "layer_c_status": "observer_mode",
        "layer_c_last": None,
        "layer_c_version": None,

        "db_status": "unknown",

        "resources": {
            "cpu": None,
            "ram": None,
            "disk": None,
        },

        "api": {
            "last_brain_ok": state_mgr.state["timestamps"].get("last_brain_ok")
        },

        "errors": []
    }

    # /health
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

            result["brain_uptime_seconds"] = (
                h.get("uptime_seconds")
                or h.get("uptime_sec")
                or result["brain_uptime_seconds"]
            )

            db = h.get("db") or h.get("database") or {}
            if isinstance(db, dict):
                result["db_status"] = db.get("status") or db.get("health") or result["db_status"]
            elif isinstance(db, str):
                result["db_status"] = db

            resources = h.get("resources") or h.get("system") or {}
            _merge_resources(result, resources)

    except Exception as e:
        result["errors"].append(f"health: {type(e).__name__}")

    if not result["brain_ok"]:
        return result

    # /monitor/status
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
            db = m.get("db") or m.get("database") or {}

            result["layer_a_status"] = a.get("status", result["layer_a_status"])
            result["layer_a_last"] = a.get("last_signal_ts") or a.get("last_signal") or result["layer_a_last"]
            result["layer_a_open"] = a.get("open_trades_in_db", result["layer_a_open"])

            result["signals_today"] = a.get("signals_today") or a.get("today_signals")
            result["signals_last_hour"] = a.get("signals_last_hour") or a.get("last_hour_signals")
            result["signal_inactivity"] = a.get("signal_inactivity") or a.get("inactivity")

            result["brain_uptime"] = b.get("uptime_human", result["brain_uptime"])
            result["brain_uptime_seconds"] = b.get("uptime_seconds", result["brain_uptime_seconds"])
            result["brain_signals"] = b.get("total_signals_stored", result["brain_signals"])
            result["brain_version"] = b.get("version", result["brain_version"])

            result["layer_c_status"] = normalize_reflex_status(c.get("status", result["layer_c_status"]))
            result["layer_c_last"] = c.get("last_heartbeat") or c.get("last_sync") or c.get("updated_at")
            result["layer_c_version"] = c.get("version")

            if isinstance(db, dict):
                result["db_status"] = db.get("status") or db.get("health") or result["db_status"]
            elif isinstance(db, str):
                result["db_status"] = db

            resources = m.get("resources") or m.get("system") or {}
            _merge_resources(result, resources)

    except Exception as e:
        result["errors"].append(f"monitor/status: {type(e).__name__}")

    return result


def _merge_resources(result: dict, resources):
    if not isinstance(resources, dict):
        return

    result["resources"]["cpu"] = (
        resources.get("cpu")
        or resources.get("cpu_percent")
        or result["resources"]["cpu"]
    )
    result["resources"]["ram"] = (
        resources.get("ram")
        or resources.get("memory")
        or resources.get("memory_percent")
        or result["resources"]["ram"]
    )
    result["resources"]["disk"] = (
        resources.get("disk")
        or resources.get("disk_percent")
        or result["resources"]["disk"]
    )


def normalize_reflex_status(status: str) -> str:
    if not status:
        return "observer_mode"
    if str(status).lower() in ("unconfirmed", "observer", "observer_mode"):
        return "observer_mode"
    return str(status).lower()


# ─────────────────────────────────────────────────────────────
# Alert Manager
# ─────────────────────────────────────────────────────────────

async def process_alerts(client: httpx.AsyncClient, d: dict):
    state_mgr.reset_daily_if_needed()

    previous = state_mgr.state["previous"]
    events = state_mgr.state["events"]
    metrics = state_mgr.state["metrics"]

    # Brain Ops state
    brain_now = "ok" if d["brain_ok"] else "down"

    if brain_now == "down" and previous.get("brain_ops") != "down":
        events["brain_down_since"] = _now_iso()
        metrics["alerts_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "⚠️ <b>Brain Ops unavailable</b>\n"
            "Monitor could not reach Brain Ops API.\n\n"
            f"🕐 {_now_human()}"
        )

    elif brain_now == "ok" and previous.get("brain_ops") == "down":
        events["brain_down_since"] = None
        metrics["recoveries_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "✅ <b>Brain Ops recovered</b>\n"
            f"Latency: {d['brain_lat']}ms\n\n"
            f"🕐 {_now_human()}"
        )

    previous["brain_ops"] = brain_now

    # Layer A state
    a_now = d.get("layer_a_status", "no_data")

    if a_now == "likely_down" and previous.get("layer_a") != "likely_down":
        events["layer_a_down_since"] = _now_iso()
        metrics["alerts_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "⚠️ <b>Signal Bot inactivity detected</b>\n"
            f"Last signal: {_rel(d.get('layer_a_last'))}\n\n"
            f"🕐 {_now_human()}"
        )

    elif a_now in ("active", "idle") and previous.get("layer_a") == "likely_down":
        events["layer_a_down_since"] = None
        metrics["recoveries_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "✅ <b>Signal Bot activity recovered</b>\n"
            f"Layer A: {display_status(a_now)}\n\n"
            f"🕐 {_now_human()}"
        )

    previous["layer_a"] = a_now

    # DB state
    db_now = normalize_db(d.get("db_status", "unknown"))
    if db_now in ("degraded", "disconnected") and previous.get("db") not in ("degraded", "disconnected"):
        metrics["alerts_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            f"⚠️ <b>Database {db_now}</b>\n"
            "Brain API may be alive while data layer is unstable.\n\n"
            f"🕐 {_now_human()}"
        )

    elif db_now == "connected" and previous.get("db") in ("degraded", "disconnected"):
        metrics["recoveries_today"] += 1
        await send(
            client,
            "🛡️ <b>BTC System Monitor</b>\n\n"
            "✅ <b>Database recovered</b>\n\n"
            f"🕐 {_now_human()}"
        )

    previous["db"] = db_now

    state_mgr.save()


async def maybe_send_daily_summary(client: httpx.AsyncClient, latest: dict | None):
    now = datetime.now(TH_TZ)
    today = _today_key()

    if now.hour != DAILY_SUMMARY_HOUR or now.minute < DAILY_SUMMARY_MINUTE:
        return

    if state_mgr.state["timestamps"].get("last_daily_summary_date") == today:
        return

    state_mgr.state["timestamps"]["last_daily_summary_date"] = today
    state_mgr.save()

    await send(client, build_daily_summary(latest))


# ─────────────────────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────────────────────

def build_status_message(d: dict) -> str:
    latency = state_mgr.latency_stats()

    brain_icon = "🟢" if d["brain_ok"] else "🔴"
    a_icon = icon_for_layer_a(d.get("layer_a_status"))
    c_icon = icon_for_reflex(d.get("layer_c_status"))
    db_icon = icon_for_db(d.get("db_status"))
    monitor_icon = "🟢"

    restart_line = build_restart_line(d)
    latency_icon = icon_for_latency(latency["status"])

    signals_today = safe_value(d.get("signals_today"), fallback="not reported")
    signals_1h = safe_value(d.get("signals_last_hour"), fallback="not reported")
    inactivity = d.get("signal_inactivity") or _rel(d.get("layer_a_last"))

    resources = d.get("resources", {})
    cpu = percent_value(resources.get("cpu"))
    ram = percent_value(resources.get("ram"))
    disk = percent_value(resources.get("disk"))

    last_brain_ok = state_mgr.state["timestamps"].get("last_brain_ok")

    return (
        "🛡️ <b>BTC System Monitor</b>\n"
        "<i>Warm Watchtower · Read-only monitoring</i>\n\n"

        "<b>Nodes</b>\n"
        f"{brain_icon} Brain Ops — {display_brain(d)}\n"
        f"{a_icon} Signal Bot — {display_layer_a(d.get('layer_a_status'))}\n"
        f"{c_icon} Reflex Engine — {display_reflex(d.get('layer_c_status'))}\n"
        f"{monitor_icon} Monitor Node — online\n"
        f"{db_icon} Database — {display_db(d.get('db_status'))}\n"
        f"{restart_line}\n\n"

        "<b>Signals</b>\n"
        f"• today: {signals_today}\n"
        f"• last 1h: {signals_1h}\n"
        f"• inactivity: {inactivity}\n"
        f"• open trades: {d.get('layer_a_open', 0)}\n\n"

        "<b>Latency</b>\n"
        f"{latency_icon} {latency['status']}\n"
        f"• current: {latency['current']}ms\n"
        f"• avg: {latency['avg']}ms\n"
        f"• peak: {latency['peak']}ms\n\n"

        "<b>Resources</b>\n"
        f"• CPU: {cpu}\n"
        f"• RAM: {ram}\n"
        f"• Disk: {disk}\n\n"

        "<b>Heartbeat</b>\n"
        f"• Monitor check: {_rel(state_mgr.state['timestamps'].get('last_monitor_check'))}\n"
        f"• Layer A signal: {_rel(d.get('layer_a_last'))}\n"
        f"• Brain API: {_rel(last_brain_ok) if last_brain_ok else '—'}\n"
        f"• Reflex sync: {_rel(d.get('layer_c_last'))}\n\n"

        "<b>Versions</b>\n"
        f"• Brain Ops: {d.get('brain_version') or '?'}\n"
        f"• Monitor: {MONITOR_VERSION}\n"
        f"• Reflex: {d.get('layer_c_version') or display_reflex(d.get('layer_c_status'))}\n\n"

        f"🕐 {_now_human()}"
    )


def build_daily_summary(latest: dict | None) -> str:
    metrics = state_mgr.state["metrics"]
    latency = state_mgr.latency_stats()

    checks_total = max(int(metrics.get("checks_total", 0)), 1)
    brain_ok_total = int(metrics.get("brain_ok_total", 0))
    uptime_pct = round((brain_ok_total / checks_total) * 100, 2)

    signals_today = "not reported"
    if latest:
        signals_today = safe_value(latest.get("signals_today"), fallback="not reported")

    return (
        "🛡️ <b>Daily Infrastructure Report</b>\n\n"
        f"• Brain API uptime sample: {uptime_pct}%\n"
        f"• Signals today: {signals_today}\n"
        f"• Alerts today: {metrics.get('alerts_today', 0)}\n"
        f"• Recoveries today: {metrics.get('recoveries_today', 0)}\n"
        f"• Avg latency: {latency['avg']}ms\n"
        f"• Peak latency: {latency['peak']}ms\n"
        f"• Monitor version: {MONITOR_VERSION}\n\n"
        f"🕐 {_now_human()}"
    )


def build_help_message() -> str:
    return (
        "🛡️ <b>BTC System Monitor</b>\n\n"
        "<b>Commands</b>\n"
        "/status — full ecosystem status\n"
        "/summary — runtime summary\n"
        "/help — help\n\n"
        "<b>Role</b>\n"
        "Read-only monitoring layer for BTC ecosystem.\n"
        "It does not trade, execute, or modify core systems."
    )


def build_restart_line(d: dict) -> str:
    uptime_seconds = d.get("brain_uptime_seconds")
    uptime_human = d.get("brain_uptime")

    if uptime_seconds is not None:
        try:
            if int(uptime_seconds) < 600:
                return "⚠️ Brain Ops restarted recently"
        except Exception:
            pass

    if isinstance(uptime_human, str):
        lower = uptime_human.lower()
        if any(x in lower for x in ["1m", "2m", "3m", "4m", "5m", "minute"]):
            return "⚠️ Brain Ops restarted recently"

    return "🟢 Runtime appears stable"


def display_brain(d):
    if not d.get("brain_ok"):
        return "unavailable"
    return f"online · {d.get('brain_lat', 0)}ms · uptime {d.get('brain_uptime', '?')}"


def display_layer_a(status):
    if status == "active":
        return "Active"
    if status == "idle":
        return "Idle"
    if status == "likely_down":
        return "Inactivity detected"
    if status == "no_data":
        return "Waiting for first signal"
    return display_status(status)


def display_reflex(status):
    if normalize_reflex_status(status) == "observer_mode":
        return "Observer Mode"
    return display_status(status)


def display_db(status):
    return display_status(normalize_db(status))


def display_status(status):
    if not status:
        return "unknown"
    return str(status).replace("_", " ").title()


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


def icon_for_layer_a(status):
    if status == "active":
        return "🟢"
    if status == "idle":
        return "🟡"
    if status == "likely_down":
        return "🔴"
    return "⚪"


def icon_for_reflex(status):
    if normalize_reflex_status(status) == "active":
        return "🟢"
    return "🟡"


def icon_for_db(status):
    s = normalize_db(status)
    if s == "connected":
        return "🟢"
    if s == "disconnected":
        return "🔴"
    if s == "degraded":
        return "🟡"
    return "⚪"


def icon_for_latency(status):
    if status == "stable":
        return "🟢"
    if status == "elevated":
        return "🟡"
    if status == "degraded":
        return "🔴"
    return "⚪"


def percent_value(value):
    if value is None:
        return "not reported"
    try:
        if isinstance(value, str) and value.endswith("%"):
            return value
        return f"{float(value):.0f}%"
    except Exception:
        return str(value)


def safe_value(value, fallback="—"):
    if value is None:
        return fallback
    return value


# ─────────────────────────────────────────────────────────────
# Time Helpers
# ─────────────────────────────────────────────────────────────

def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _now_human():
    return datetime.now(TH_TZ).strftime("%d %b %Y %H:%M:%S")


def _today_key():
    return datetime.now(TH_TZ).strftime("%Y-%m-%d")


def _parse_iso(iso):
    if not iso:
        return None

    try:
        s = str(iso).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None


def _rel(iso):
    dt = _parse_iso(iso)
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


def _deep_merge(base, override):
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


# ─────────────────────────────────────────────────────────────
# Main Loop
# ─────────────────────────────────────────────────────────────

async def auto_check(client: httpx.AsyncClient) -> dict:
    log.info("auto-check running")

    d = await fetch_monitor(client)

    state_mgr.state["timestamps"]["last_monitor_check"] = _now_iso()
    state_mgr.state["metrics"]["checks_total"] = int(state_mgr.state["metrics"].get("checks_total", 0)) + 1

    if d["brain_ok"]:
        state_mgr.state["timestamps"]["last_brain_ok"] = _now_iso()
        state_mgr.state["metrics"]["brain_ok_total"] = int(state_mgr.state["metrics"].get("brain_ok_total", 0)) + 1
        state_mgr.record_latency(d.get("brain_lat", 0))

    state_mgr.save()

    await process_alerts(client, d)
    await maybe_send_daily_summary(client, d)

    return d


async def main():
    log.info(f"BTC System Monitor starting — interval={CHECK_INTERVAL}s")

    offset = 0
    last_check = 0.0
    latest_status = None

    async with httpx.AsyncClient() as client:
        # Gentle startup message, persisted to avoid spam during quick crash loops.
        last_startup = state_mgr.state["timestamps"].get("last_startup_sent")
        if _rel(last_startup) in ("—",) or _startup_message_allowed(last_startup):
            await send(
                client,
                "🛡️ <b>BTC System Monitor online</b>\n\n"
                f"Warm Watchtower started · {MONITOR_VERSION}\n"
                f"Auto-check every {CHECK_INTERVAL // 60} minute(s)."
            )
            state_mgr.state["timestamps"]["last_startup_sent"] = _now_iso()
            state_mgr.save()

        while True:
            try:
                now = asyncio.get_event_loop().time()

                if now - last_check >= CHECK_INTERVAL:
                    latest_status = await auto_check(client)
                    last_check = now

                updates = await get_updates(client, offset)

                for update in updates:
                    offset = int(update.get("update_id", offset)) + 1

                    msg = update.get("message", {})
                    chat_id = str(msg.get("chat", {}).get("id", ""))

                    if chat_id != ALLOWED_ID:
                        log.warning(f"unauthorized chat_id={chat_id}")
                        continue

                    text = msg.get("text", "").strip()

                    if text.startswith("/status"):
                        await send(client, "⏳ Checking ecosystem status...")
                        latest_status = await fetch_monitor(client)

                        if latest_status["brain_ok"]:
                            state_mgr.record_latency(latest_status.get("brain_lat", 0))
                            state_mgr.state["timestamps"]["last_brain_ok"] = _now_iso()

                        state_mgr.state["timestamps"]["last_monitor_check"] = _now_iso()
                        state_mgr.save()

                        await send(client, build_status_message(latest_status))

                    elif text.startswith("/summary"):
                        if not latest_status:
                            latest_status = await fetch_monitor(client)
                        await send(client, build_daily_summary(latest_status))

                    elif text.startswith(("/help", "/start")):
                        await send(client, build_help_message())

                await asyncio.sleep(1)

            except Exception as e:
                log.exception(f"main loop recovered from error: {e}")
                await asyncio.sleep(5)


def _startup_message_allowed(last_startup_iso):
    dt = _parse_iso(last_startup_iso)
    if not dt:
        return True

    diff = (datetime.now(timezone.utc) - dt).total_seconds()
    return diff > 3600


if __name__ == "__main__":
    asyncio.run(main())
