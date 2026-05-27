"""
btc_monitor_bot.py — BTC System Monitor Bot v2
Multi-node ecosystem observability layer

Monitors:
  - Brain Ops (btc-brain-ops)
  - Reflex Engine (btc-reflex-engine)
  - Signal Bot (inferred from Brain Ops DB)

Rules:
  - READ ONLY — never modifies any system
  - If monitor dies → Brain Ops and Reflex continue normally
  - Each node polled independently — one failure never affects others
  - Reflex integration is additive, isolated, optional

Commands:
  /status  — ecosystem health snapshot
  /help    — วิธีใช้

Auto-alert (every CHECK_INTERVAL_SEC):
  - Brain Ops down
  - Signal Bot likely_down
  - Recovery notifications

Environment Variables (Railway):
  MONITOR_BOT_TOKEN     — token จาก @BotFather
  MONITOR_CHAT_ID       — chat id ของคุณ
  BRAIN_OPS_URL         — https://web-production-f47d4.up.railway.app
  REFLEX_ENGINE_URL     — https://your-reflex.railway.app (optional)
  CHECK_INTERVAL_SEC    — default 300 (5 นาที)
"""

import os
import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.environ["MONITOR_BOT_TOKEN"]
ALLOWED_ID     = str(os.environ["MONITOR_CHAT_ID"])
BRAIN_URL      = os.environ.get("BRAIN_OPS_URL", "https://web-production-f47d4.up.railway.app").rstrip("/")
REFLEX_URL     = os.environ.get("REFLEX_ENGINE_URL", "").rstrip("/")   # optional
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL_SEC", "300"))
REQUEST_TIMEOUT = 5   # aggressive timeout per spec

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ── Runtime state ─────────────────────────────────────────────────────────────
_prev = {
    "brain_ops": "unknown",   # "ok" | "down"
    "layer_a":   "unknown",   # "active" | "idle" | "likely_down" | "no_data"
}

# Lightweight latency tracker (last 10 samples only)
_latency_samples: list[float] = []

_monitor_start = time.monotonic()
_last_brain_check: float = 0.0
_last_reflex_check: float = 0.0
# ─────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# NODE FETCHERS — each isolated, never raises, always returns safe structure
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_brain_status(client: httpx.AsyncClient) -> dict:
    """Poll Brain Ops — isolated, timeout-safe."""
    global _last_brain_check
    result = {
        "reachable": False,
        "lat_ms": 0,
        "version": "?",
        "uptime_human": "?",
        "total_signals": 0,
        "total_events": 0,
        "db_connected": False,
        "layer_a_status": "no_data",
        "layer_a_last": None,
        "layer_a_open": 0,
        "layer_c_status": "unconfirmed",
        "overall": "unknown",
    }
    try:
        t0 = time.monotonic()
        r = await client.get(f"{BRAIN_URL}/health", timeout=REQUEST_TIMEOUT)
        lat = int((time.monotonic() - t0) * 1000)
        if r.status_code == 200:
            result["reachable"] = True
            result["lat_ms"] = lat
            d = r.json()
            result["version"] = d.get("version", "?")
            result["db_connected"] = True
            _latency_samples.append(lat)
            if len(_latency_samples) > 10:
                _latency_samples.pop(0)
    except Exception as e:
        log.warning(f"Brain /health failed: {e}")
        return result

    _last_brain_check = time.monotonic()

    # /monitor/status — only if /health succeeded
    try:
        r2 = await client.get(f"{BRAIN_URL}/monitor/status", timeout=REQUEST_TIMEOUT)
        if r2.status_code == 200:
            m = r2.json()
            result["overall"] = m.get("overall", "unknown")
            a = m.get("layer_a", {})
            b = m.get("layer_b", {})
            c = m.get("layer_c", {})
            result["layer_a_status"] = a.get("status", "no_data")
            result["layer_a_last"]   = a.get("last_signal_ts")
            result["layer_a_open"]   = a.get("open_trades_in_db", 0)
            result["uptime_human"]   = b.get("uptime_human", "?")
            result["total_signals"]  = b.get("total_signals_stored", 0)
            result["total_events"]   = b.get("total_events_logged", 0)
            result["layer_c_status"] = c.get("status", "unconfirmed")
    except Exception as e:
        log.warning(f"Brain /monitor/status failed: {e}")

    return result


async def fetch_reflex_status(client: httpx.AsyncClient) -> dict:
    """
    Poll Reflex Engine — additive, isolated, optional.
    NEVER crashes monitor if Reflex is unreachable or REFLEX_ENGINE_URL not set.
    Returns stable fallback structure always.
    """
    fallback = {
        "reachable": False,
        "status": "observer_unreachable",
        "runtime_mode": "unknown",
        "adaptive_state": "unknown",
        "version": "?",
        "uptime_seconds": 0,
        "last_sync": None,
        "seconds_since_sync": None,
        "memory_nodes": 0,
        "active_reflections": 0,
        "brain_connected": False,
        "cycles_completed": 0,
        "cycles_failed": 0,
        "url_configured": bool(REFLEX_URL),
    }

    if not REFLEX_URL:
        fallback["status"] = "url_not_configured"
        return fallback

    global _last_reflex_check

    # Try /health first
    try:
        r = await client.get(f"{REFLEX_URL}/health", timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return fallback
        h = r.json()
        fallback["reachable"] = True
        fallback["version"] = h.get("version", "?")
    except Exception as e:
        log.warning(f"Reflex /health failed: {e}")
        return fallback

    # Try /status — enrich if available
    try:
        r2 = await client.get(f"{REFLEX_URL}/status", timeout=REQUEST_TIMEOUT)
        if r2.status_code == 200:
            d = r2.json()
            # Normalize — expose only operational metadata
            fallback["status"]            = _normalize_reflex_status(d.get("status", "observer_mode"))
            fallback["runtime_mode"]      = d.get("runtime_mode", "passive")
            fallback["adaptive_state"]    = d.get("adaptive_state", "stable")
            fallback["version"]           = d.get("version", fallback["version"])
            fallback["uptime_seconds"]    = d.get("uptime_seconds", 0)
            fallback["last_sync"]         = d.get("last_sync")
            fallback["seconds_since_sync"]= d.get("seconds_since_sync")
            fallback["memory_nodes"]      = d.get("memory_nodes", 0)
            fallback["active_reflections"]= d.get("active_reflections", 0)
            fallback["brain_connected"]   = d.get("brain_connected", False)
            fallback["cycles_completed"]  = d.get("cycles_completed", 0)
            fallback["cycles_failed"]     = d.get("cycles_failed", 0)
    except Exception as e:
        log.warning(f"Reflex /status failed: {e}")
        # Still reachable (health passed), just no detail

    _last_reflex_check = time.monotonic()
    return fallback


def _normalize_reflex_status(raw: str) -> str:
    """Normalize Reflex status — expose only safe operational labels."""
    safe = {"observer_mode", "passive", "adaptive", "stable", "synchronized"}
    return raw if raw in safe else "observer_mode"


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_ecosystem_message(brain: dict, reflex: dict) -> str:
    """Build full ecosystem status message — never raises."""
    try:
        now = time.monotonic()
        monitor_uptime = _fmt_duration(now - _monitor_start)
        since_brain = _fmt_duration(now - _last_brain_check) if _last_brain_check else "—"
        since_reflex = _fmt_duration(now - _last_reflex_check) if _last_reflex_check else "—"

        # ── Nodes section ─────────────────────────────────────────────────
        # Brain Ops
        b_icon = "🟢" if brain["reachable"] else "🔴"
        b_line = f"online · {brain['lat_ms']}ms" if brain["reachable"] else "offline"

        # Signal Bot (Layer A)
        a_status = brain["layer_a_status"]
        if a_status == "active":
            a_icon = "🟢"
            a_line = f"active · last {_rel(brain['layer_a_last'])} · {brain['layer_a_open']} open"
        elif a_status == "idle":
            a_icon = "🟡"
            a_line = f"idle · last {_rel(brain['layer_a_last'])}"
        elif a_status == "likely_down":
            a_icon = "🔴"
            a_line = "ไม่มี signal นานผิดปกติ"
        else:
            a_icon = "⚪"
            a_line = "waiting for first signal"

        # Reflex Engine
        if not reflex["url_configured"]:
            r_icon, r_line = "⚫", "URL not configured"
        elif not reflex["reachable"]:
            r_icon, r_line = "🔴", "unreachable"
        else:
            r_icon = "🟢"
            r_line = f"Observer Mode · {reflex['runtime_mode']}"

        # Monitor node
        db_icon = "🟢" if brain.get("db_connected") else "🔴"

        nodes = (
            f"<b>Nodes</b>\n"
            f"{b_icon} Brain Ops — {b_line}\n"
            f"{a_icon} Signal Bot — {a_line}\n"
            f"{r_icon} Reflex Engine — {r_line}\n"
            f"🟢 Monitor Node — online · {monitor_uptime}\n"
            f"{db_icon} Database — {'connected' if brain.get('db_connected') else 'unknown'}"
        )

        # ── Reflex Runtime section ────────────────────────────────────────
        if reflex["reachable"]:
            sync_ago = _fmt_duration(reflex["seconds_since_sync"]) if reflex["seconds_since_sync"] else "—"
            reflex_section = (
                f"\n\n<b>Reflex Runtime</b>\n"
                f"• runtime: {reflex['runtime_mode']}\n"
                f"• adaptive: {reflex['adaptive_state']}\n"
                f"• last sync: {sync_ago} ago\n"
                f"• memory nodes: {reflex['memory_nodes']}\n"
                f"• active reflections: {reflex['active_reflections']}\n"
                f"• cycles: {reflex['cycles_completed']} ok / {reflex['cycles_failed']} failed\n"
                f"• brain connected: {'true' if reflex['brain_connected'] else 'false'}"
            )
        else:
            reflex_section = ""

        # ── Latency section ───────────────────────────────────────────────
        if _latency_samples:
            cur = _latency_samples[-1]
            avg = int(sum(_latency_samples) / len(_latency_samples))
            peak = int(max(_latency_samples))
            lat_icon = "🟢" if cur < 500 else ("🟡" if cur < 1500 else "🔴")
            lat_section = (
                f"\n\n<b>Latency</b>\n"
                f"{lat_icon} {'stable' if cur < 500 else 'high'}\n"
                f"• current: {cur}ms · avg: {avg}ms · peak: {peak}ms"
            )
        else:
            lat_section = ""

        # ── Heartbeat section ─────────────────────────────────────────────
        hb_section = (
            f"\n\n<b>Heartbeat</b>\n"
            f"• monitor uptime: {monitor_uptime}\n"
            f"• Brain API: {since_brain} ago\n"
            f"• Reflex sync: {since_reflex} ago"
        )

        # ── Overall header ────────────────────────────────────────────────
        overall = brain.get("overall", "unknown")
        if overall == "healthy" and brain["reachable"]:
            header = "✅ <b>BTC System Monitor</b>"
        elif overall == "warning":
            header = "⚠️ <b>BTC System Monitor</b>"
        elif not brain["reachable"]:
            header = "🔴 <b>BTC System Monitor</b>"
        else:
            header = "🛡️ <b>BTC System Monitor</b>"

        return (
            f"{header}\n\n"
            f"{nodes}"
            f"{reflex_section}"
            f"{lat_section}"
            f"{hb_section}\n\n"
            f"🕐 {_now()}"
        )

    except Exception as e:
        log.error(f"build_ecosystem_message failed: {e}")
        return f"⚠️ Error building status message\n🕐 {_now()}"


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-ALERT
# ══════════════════════════════════════════════════════════════════════════════

async def auto_check(client: httpx.AsyncClient):
    """Poll all nodes independently — one failure never breaks others."""
    global _prev

    # Poll independently — failures isolated
    brain = await fetch_brain_status(client)
    # Reflex alert is informational only — no state tracking needed yet

    # ── Brain Ops alert ───────────────────────────────────────────────────
    brain_now = "ok" if brain["reachable"] else "down"
    if brain_now == "down" and _prev["brain_ops"] != "down":
        await send(client,
            "🚨 <b>ALERT — Brain Ops DOWN</b>\n\n"
            "❌ Brain Ops ไม่ตอบสนอง\n"
            "└ ตรวจสอบ Railway deployment\n\n"
            f"🕐 {_now()}"
        )
    elif brain_now == "ok" and _prev["brain_ops"] == "down":
        await send(client,
            "✅ <b>RECOVERY — Brain Ops กลับมาแล้ว</b>\n\n"
            f"🟢 Online · {brain['lat_ms']}ms\n\n"
            f"🕐 {_now()}"
        )
    _prev["brain_ops"] = brain_now

    # ── Layer A alert ─────────────────────────────────────────────────────
    a_now = brain["layer_a_status"]
    if a_now == "likely_down" and _prev["layer_a"] != "likely_down":
        await send(client,
            "🚨 <b>ALERT — Signal Bot หยุดทำงาน?</b>\n\n"
            "🔴 Layer A ไม่มี signal เกิน 6 ชั่วโมง\n"
            f"└ Last: {_rel(brain['layer_a_last'])}\n\n"
            "ตรวจสอบ Railway → BTC-ALERT-BOT\n\n"
            f"🕐 {_now()}"
        )
    elif a_now in ("active", "idle") and _prev["layer_a"] == "likely_down":
        await send(client,
            "✅ <b>RECOVERY — Signal Bot กลับมาแล้ว</b>\n\n"
            f"🟢 Layer A: {a_now}\n"
            f"└ Last: {_rel(brain['layer_a_last'])}\n\n"
            f"🕐 {_now()}"
        )
    _prev["layer_a"] = a_now

    log.info(f"Auto-check done — brain={brain_now} layer_a={a_now}")


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def send(client: httpx.AsyncClient, text: str):
    try:
        await client.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": ALLOWED_ID,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=10)
    except Exception as e:
        log.warning(f"send failed: {e}")


async def get_updates(client: httpx.AsyncClient, offset: int) -> list:
    try:
        r = await client.get(f"{TELEGRAM_API}/getUpdates", params={
            "offset": offset, "timeout": 5, "allowed_updates": ["message"],
        }, timeout=10)
        return r.json().get("result", [])
    except Exception as e:
        log.warning(f"getUpdates error: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _rel(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        diff = (datetime.now(timezone.utc) - datetime.fromisoformat(iso.replace("Z", "+00:00"))).total_seconds()
        return _fmt_duration(diff)
    except Exception:
        return "?"

def _fmt_duration(sec) -> str:
    if sec is None: return "—"
    sec = int(sec)
    if sec < 60:    return f"{sec}s"
    if sec < 3600:  return f"{sec//60}m {sec%60}s"
    if sec < 86400: return f"{sec//3600}h {(sec%3600)//60}m"
    return f"{sec//86400}d {(sec%86400)//3600}h"

def _now() -> str:
    return datetime.now().strftime("%d %b %Y  %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info(f"BTC Monitor Bot v2 starting — interval={CHECK_INTERVAL}s reflex={'configured' if REFLEX_URL else 'not configured'}")
    offset = 0
    last_check = 0.0

    async with httpx.AsyncClient() as client:
        await send(client,
            "🛡️ <b>BTC Monitor Bot v2 พร้อมแล้ว</b>\n\n"
            f"• /status — ecosystem snapshot\n"
            f"• Auto-alert ทุก {CHECK_INTERVAL//60} นาที\n"
            f"• Reflex: {'configured ✅' if REFLEX_URL else 'not configured ⚪'}\n\n"
            "Monitoring: Brain Ops · Signal Bot · Reflex Engine"
        )

        while True:
            now_mono = asyncio.get_event_loop().time()

            # ── Auto-check ────────────────────────────────────────────────
            if now_mono - last_check >= CHECK_INTERVAL:
                try:
                    await auto_check(client)
                except Exception as e:
                    log.error(f"auto_check error: {e}")
                last_check = now_mono

            # ── Commands ──────────────────────────────────────────────────
            updates = await get_updates(client, offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                chat_id = str(msg.get("chat", {}).get("id", ""))
                text = msg.get("text", "").strip()

                if chat_id != ALLOWED_ID:
                    continue

                if text.startswith("/status"):
                    await send(client, "⏳ กำลังตรวจสอบ...")
                    # Poll nodes independently
                    brain, reflex = await asyncio.gather(
                        fetch_brain_status(client),
                        fetch_reflex_status(client),
                        return_exceptions=False,
                    )
                    try:
                        msg_text = build_ecosystem_message(brain, reflex)
                    except Exception as e:
                        msg_text = f"⚠️ Error: {e}"
                    await send(client, msg_text)

                elif text.startswith(("/help", "/start")):
                    await send(client,
                        "🛡️ <b>BTC Monitor Bot v2</b>\n\n"
                        "<b>Commands:</b>\n"
                        "/status — ecosystem snapshot\n\n"
                        "<b>Auto-alert เมื่อ:</b>\n"
                        "• Brain Ops down\n"
                        "• Signal Bot หยุด (6h no signal)\n"
                        "• Recovery\n\n"
                        "<b>Nodes ที่ดูแล:</b>\n"
                        "• Layer A: Signal Bot (inferred)\n"
                        "• Layer B: Brain Ops\n"
                        "• Layer C: Reflex Engine\n"
                        "• Monitor Node: self"
                    )

            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
