"""
btc_monitor_bot.py — BTC Ecosystem Monitor v3 (W22)
Observability-only layer — read-only, no execution, no modification

Monitors:
  A) Brain Ops   — /health · /monitor/status · /weekly/W22-2026/status
  B) Reflex      — /health · /status
  C) Signal Bot  — inferred from Brain Ops DB (no public endpoint)

Rules:
  - READ ONLY — never writes, never commands, never modifies
  - Each node polled independently — one failure never affects others
  - Graceful degradation — show subsystem as degraded, continue others
  - If monitor dies → all 3 core systems continue normally

Commands:
  /status  — full W22 ecosystem snapshot
  /help    — วิธีใช้

Auto-alert (every CHECK_INTERVAL_SEC):
  - Brain Ops down / recovery
  - Signal Bot likely_down / recovery

Environment Variables (Railway):
  MONITOR_BOT_TOKEN     — token จาก @BotFather
  MONITOR_CHAT_ID       — chat id ของคุณ
  BRAIN_OPS_URL         — https://web-production-f47d4.up.railway.app
  REFLEX_ENGINE_URL     — https://your-reflex.railway.app  ← NEW (optional)
  CHECK_INTERVAL_SEC    — default 300
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
BOT_TOKEN       = os.environ["MONITOR_BOT_TOKEN"]
ALLOWED_ID      = str(os.environ["MONITOR_CHAT_ID"])
BRAIN_URL       = os.environ.get("BRAIN_OPS_URL", "https://web-production-f47d4.up.railway.app").rstrip("/")
REFLEX_URL      = os.environ.get("REFLEX_ENGINE_URL", "").rstrip("/")
CHECK_INTERVAL  = int(os.environ.get("CHECK_INTERVAL_SEC", "300"))
TIMEOUT         = 5
WEEK_ID         = "W22-2026"

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# ── Runtime state ─────────────────────────────────────────────────────────────
_prev = {"brain_ops": "unknown", "layer_a": "unknown"}
_latency_samples: list[float] = []
_monitor_start      = time.monotonic()
_last_brain_check   = 0.0
_last_reflex_check  = 0.0
# ─────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# A) BRAIN OPS FETCHER
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_brain(client: httpx.AsyncClient) -> dict:
    """Poll Brain Ops — isolated, timeout-safe, never raises."""
    global _last_brain_check
    r = {
        "reachable": False, "lat_ms": 0, "version": "?",
        "uptime_human": "?", "total_signals": 0, "total_events": 0,
        "db_connected": False,
        # layer_a (signal bot inferred)
        "layer_a_status": "no_data", "layer_a_last": None, "layer_a_open": 0,
        # layer_c (reflex inferred)
        "layer_c_status": "unconfirmed",
        "overall": "unknown",
        # W22 weekly export
        "weekly_reachable": False,
        "weekly_status": None,
        "weekly_export_complete": False,
        "weekly_signal_count": 0,
        "weekly_missed_count": 0,
        "weekly_event_count": 0,
        "weekly_error": None,
    }

    # /health
    try:
        t0 = time.monotonic()
        res = await client.get(f"{BRAIN_URL}/health", timeout=TIMEOUT)
        lat = int((time.monotonic() - t0) * 1000)
        if res.status_code == 200:
            r["reachable"] = True
            r["lat_ms"] = lat
            r["db_connected"] = True
            r["version"] = res.json().get("version", "?")
            _latency_samples.append(lat)
            if len(_latency_samples) > 10:
                _latency_samples.pop(0)
        _last_brain_check = time.monotonic()
    except Exception as e:
        log.warning(f"Brain /health: {e}")
        return r

    # /monitor/status
    try:
        res2 = await client.get(f"{BRAIN_URL}/monitor/status", timeout=TIMEOUT)
        if res2.status_code == 200:
            m = res2.json()
            r["overall"]         = m.get("overall", "unknown")
            a = m.get("layer_a", {})
            b = m.get("layer_b", {})
            c = m.get("layer_c", {})
            r["layer_a_status"]  = a.get("status", "no_data")
            r["layer_a_last"]    = a.get("last_signal_ts")
            r["layer_a_open"]    = a.get("open_trades_in_db", 0)
            r["uptime_human"]    = b.get("uptime_human", "?")
            r["total_signals"]   = b.get("total_signals_stored", 0)
            r["total_events"]    = b.get("total_events_logged", 0)
            r["layer_c_status"]  = c.get("status", "unconfirmed")
    except Exception as e:
        log.warning(f"Brain /monitor/status: {e}")

    # /weekly/W22-2026/status  ← NEW
    try:
        res3 = await client.get(f"{BRAIN_URL}/weekly/{WEEK_ID}/status", timeout=TIMEOUT)
        if res3.status_code == 200:
            w = res3.json()
            r["weekly_reachable"]       = True
            r["weekly_status"]          = w.get("status")
            r["weekly_export_complete"] = w.get("export_complete", False)
            r["weekly_signal_count"]    = w.get("signal_count", 0)
            r["weekly_missed_count"]    = w.get("missed_count", 0)
            r["weekly_event_count"]     = w.get("event_count", 0)
        elif res3.status_code == 404:
            r["weekly_error"] = "endpoint not found"
        else:
            r["weekly_error"] = f"HTTP {res3.status_code}"
    except Exception as e:
        r["weekly_error"] = "unreachable"
        log.warning(f"Brain /weekly/{WEEK_ID}/status: {e}")

    return r


# ══════════════════════════════════════════════════════════════════════════════
# B) REFLEX ENGINE FETCHER
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_reflex(client: httpx.AsyncClient) -> dict:
    """Poll Reflex Engine — additive, isolated, optional. Never raises."""
    global _last_reflex_check
    fb = {
        "reachable": False, "url_configured": bool(REFLEX_URL),
        "status": "url_not_configured" if not REFLEX_URL else "observer_unreachable",
        "runtime_mode": "unknown", "adaptive_state": "unknown",
        "version": "?", "uptime_seconds": 0,
        "last_structure": None, "last_phase": None,
        "last_location": None, "last_volatility": None,
        "last_weight": None,
        "last_sync": None, "seconds_since_sync": None,
        "memory_nodes": 0, "active_reflections": 0,
        "brain_connected": False,
        "cycles_completed": 0, "cycles_failed": 0,
    }

    if not REFLEX_URL:
        return fb

    # /health
    try:
        res = await client.get(f"{REFLEX_URL}/health", timeout=TIMEOUT)
        if res.status_code == 200:
            fb["reachable"] = True
            fb["version"] = res.json().get("version", "?")
        else:
            return fb
    except Exception as e:
        log.warning(f"Reflex /health: {e}")
        return fb

    # /status
    try:
        res2 = await client.get(f"{REFLEX_URL}/status", timeout=TIMEOUT)
        if res2.status_code == 200:
            d = res2.json()
            fb["status"]             = _norm_reflex(d.get("status", "observer_mode"))
            fb["runtime_mode"]       = d.get("runtime_mode", "passive")
            fb["adaptive_state"]     = d.get("adaptive_state", "stable")
            fb["version"]            = d.get("version", fb["version"])
            fb["uptime_seconds"]     = d.get("uptime_seconds", 0)
            fb["last_sync"]          = d.get("last_sync")
            fb["seconds_since_sync"] = d.get("seconds_since_sync")
            fb["memory_nodes"]       = d.get("memory_nodes", 0)
            fb["active_reflections"] = d.get("active_reflections", 0)
            fb["brain_connected"]    = False   # always false per spec
            fb["cycles_completed"]   = d.get("cycles_completed", 0)
            fb["cycles_failed"]      = d.get("cycles_failed", 0)
            # W22 new fields
            fb["last_structure"]     = d.get("last_structure")
            fb["last_phase"]         = d.get("last_phase")
            fb["last_location"]      = d.get("last_location")
            fb["last_volatility"]    = d.get("last_volatility")
            fb["last_weight"]        = d.get("last_weight")
        _last_reflex_check = time.monotonic()
    except Exception as e:
        log.warning(f"Reflex /status: {e}")

    return fb


def _norm_reflex(raw: str) -> str:
    safe = {"observer_mode", "passive", "adaptive", "stable", "synchronized"}
    return raw if raw in safe else "observer_mode"


# ══════════════════════════════════════════════════════════════════════════════
# MESSAGE BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_w22_message(brain: dict, reflex: dict) -> str:
    """Build W22 ecosystem status — never raises."""
    try:
        now = time.monotonic()
        monitor_up   = _fmt_dur(now - _monitor_start)
        since_brain  = _fmt_dur(now - _last_brain_check)  if _last_brain_check  else "—"
        since_reflex = _fmt_dur(now - _last_reflex_check) if _last_reflex_check else "—"

        lines = [f"🛡️ <b>BTC Ecosystem Monitor — {WEEK_ID}</b>\n"]

        # ── 1. Signal Bot ─────────────────────────────────────────────────
        a = brain["layer_a_status"]
        if a == "active":
            a_icon, a_st = "🟢", f"active · last {_rel(brain['layer_a_last'])} · {brain['layer_a_open']} open"
        elif a == "idle":
            a_icon, a_st = "🟡", f"idle · last {_rel(brain['layer_a_last'])}"
        elif a == "likely_down":
            a_icon, a_st = "🔴", "ไม่มี signal นานผิดปกติ"
        else:
            a_icon, a_st = "⚪", "running via Railway · waiting for signal"

        lines.append(
            f"<b>1. Signal Bot</b>\n"
            f"{a_icon} status: {a_st}\n"
            f"• W22 collection: running\n"
        )

        # ── 2. Brain Ops ──────────────────────────────────────────────────
        if brain["reachable"]:
            b_icon, b_st = "🟢", f"online · {brain['lat_ms']}ms · v{brain['version']}"
            db_icon = "🟢 connected"
        else:
            b_icon, b_st = "🔴", "offline"
            db_icon = "🔴 unknown"

        # W22 weekly export
        if brain["weekly_reachable"]:
            w_complete = brain["weekly_export_complete"]
            w_icon = "✅" if w_complete else "⏳"
            w_line = (
                f"{w_icon} W22 export: {'complete' if w_complete else 'in progress'}\n"
                f"• signals: {brain['weekly_signal_count']} · "
                f"missed: {brain['weekly_missed_count']} · "
                f"events: {brain['weekly_event_count']}"
            )
        elif brain["weekly_error"] == "endpoint not found":
            w_line = "⚪ /weekly endpoint pending deploy"
        else:
            w_line = f"⚠️ weekly status: {brain['weekly_error'] or 'unavailable'}"

        lines.append(
            f"<b>2. Brain Ops</b>\n"
            f"{b_icon} health: {b_st}\n"
            f"• DB: {db_icon}\n"
            f"• total signals: {brain['total_signals']} · events: {brain['total_events']}\n"
            f"• {w_line}\n"
        )

        # ── 3. Reflex Engine ──────────────────────────────────────────────
        if not reflex["url_configured"]:
            r_block = (
                f"<b>3. Reflex Engine</b>\n"
                f"⚫ URL not configured\n"
                f"• set REFLEX_ENGINE_URL to enable\n"
            )
        elif not reflex["reachable"]:
            r_block = (
                f"<b>3. Reflex Engine</b>\n"
                f"🔴 unreachable\n"
                f"• check Railway deployment\n"
            )
        else:
            sync_ago = _fmt_dur(reflex["seconds_since_sync"]) if reflex["seconds_since_sync"] else "—"
            r_block = (
                f"<b>3. Reflex Engine</b>\n"
                f"🟢 observer mode · {reflex['runtime_mode']}\n"
                f"• adaptive: {reflex['adaptive_state']}\n"
                f"• last structure: {reflex['last_structure'] or '—'}\n"
                f"• last phase: {reflex['last_phase'] or '—'}\n"
                f"• last location: {reflex['last_location'] or '—'}\n"
                f"• last volatility: {reflex['last_volatility'] or '—'}\n"
                f"• last weight: {reflex['last_weight'] or '—'}\n"
                f"• memory nodes: {reflex['memory_nodes']} · reflections: {reflex['active_reflections']}\n"
                f"• cycles: {reflex['cycles_completed']} ok / {reflex['cycles_failed']} failed\n"
                f"• last sync: {sync_ago} ago\n"
                f"• brain connected: false\n"
            )
        lines.append(r_block)

        # ── 4. Overall ────────────────────────────────────────────────────
        overall = brain.get("overall", "unknown")
        if overall == "healthy" and brain["reachable"]:
            ov_icon, ov_txt = "✅", "healthy"
        elif overall == "warning":
            ov_icon, ov_txt = "⚠️", "attention needed"
        elif not brain["reachable"]:
            ov_icon, ov_txt = "🔴", "degraded"
        else:
            ov_icon, ov_txt = "🟡", "checking"

        lat_txt = "—"
        if _latency_samples:
            cur  = _latency_samples[-1]
            avg  = int(sum(_latency_samples) / len(_latency_samples))
            peak = int(max(_latency_samples))
            lat_txt = f"{cur}ms · avg {avg}ms · peak {peak}ms"

        lines.append(
            f"<b>4. Overall</b>\n"
            f"{ov_icon} {ov_txt}\n"
            f"• latency: {lat_txt}\n"
            f"• monitor uptime: {monitor_up}\n"
            f"• Brain checked: {since_brain} ago\n"
            f"• Reflex checked: {since_reflex} ago\n"
        )

        lines.append(f"🕐 {_now()}")
        return "\n".join(lines)

    except Exception as e:
        log.error(f"build_w22_message error: {e}")
        return f"⚠️ Error building status\n🕐 {_now()}"


# ══════════════════════════════════════════════════════════════════════════════
# AUTO-ALERT
# ══════════════════════════════════════════════════════════════════════════════

async def auto_check(client: httpx.AsyncClient):
    """Poll all nodes independently — alerts on state change only."""
    global _prev
    brain = await fetch_brain(client)

    brain_now = "ok" if brain["reachable"] else "down"
    if brain_now == "down" and _prev["brain_ops"] != "down":
        await send(client,
            f"🚨 <b>ALERT — Brain Ops DOWN</b>\n\n"
            f"❌ ไม่ตอบสนอง\n└ ตรวจสอบ Railway\n\n🕐 {_now()}"
        )
    elif brain_now == "ok" and _prev["brain_ops"] == "down":
        await send(client,
            f"✅ <b>RECOVERY — Brain Ops กลับมาแล้ว</b>\n\n"
            f"🟢 {brain['lat_ms']}ms\n\n🕐 {_now()}"
        )
    _prev["brain_ops"] = brain_now

    a_now = brain["layer_a_status"]
    if a_now == "likely_down" and _prev["layer_a"] != "likely_down":
        await send(client,
            f"🚨 <b>ALERT — Signal Bot หยุด?</b>\n\n"
            f"🔴 ไม่มี signal เกิน 6h\n"
            f"└ last: {_rel(brain['layer_a_last'])}\n\n🕐 {_now()}"
        )
    elif a_now in ("active", "idle") and _prev["layer_a"] == "likely_down":
        await send(client,
            f"✅ <b>RECOVERY — Signal Bot กลับมา</b>\n\n"
            f"🟢 {a_now} · last {_rel(brain['layer_a_last'])}\n\n🕐 {_now()}"
        )
    _prev["layer_a"] = a_now
    log.info(f"auto_check — brain={brain_now} layer_a={a_now}")


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM HELPERS
# ══════════════════════════════════════════════════════════════════════════════

async def send(client: httpx.AsyncClient, text: str):
    try:
        await client.post(f"{TELEGRAM_API}/sendMessage", json={
            "chat_id": ALLOWED_ID, "text": text, "parse_mode": "HTML",
        }, timeout=10)
    except Exception as e:
        log.warning(f"send: {e}")


async def get_updates(client: httpx.AsyncClient, offset: int) -> list:
    try:
        r = await client.get(f"{TELEGRAM_API}/getUpdates", params={
            "offset": offset, "timeout": 5, "allowed_updates": ["message"],
        }, timeout=10)
        return r.json().get("result", [])
    except Exception as e:
        log.warning(f"getUpdates: {e}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _rel(iso: str | None) -> str:
    if not iso: return "—"
    try:
        diff = (datetime.now(timezone.utc) - datetime.fromisoformat(iso.replace("Z", "+00:00"))).total_seconds()
        return _fmt_dur(diff) + " ago"
    except Exception:
        return "?"

def _fmt_dur(sec) -> str:
    if sec is None: return "—"
    sec = int(sec)
    if sec < 60:    return f"{sec}s"
    if sec < 3600:  return f"{sec//60}m {sec%60}s"
    if sec < 86400: return f"{sec//3600}h {(sec%3600)//60}m"
    return f"{sec//86400}d {(sec%86400)//3600}h"

def _now() -> str:
    return datetime.now().strftime("%d %b %Y  %H:%M:%S")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info(f"BTC Ecosystem Monitor v3 ({WEEK_ID}) — interval={CHECK_INTERVAL}s reflex={'on' if REFLEX_URL else 'off'}")
    offset, last_check = 0, 0.0

    async with httpx.AsyncClient() as client:
        await send(client,
            f"🛡️ <b>BTC Ecosystem Monitor v3 ({WEEK_ID})</b>\n\n"
            f"• /status — W22 ecosystem snapshot\n"
            f"• auto-alert ทุก {CHECK_INTERVAL//60} นาที\n"
            f"• Reflex: {'configured ✅' if REFLEX_URL else 'set REFLEX_ENGINE_URL to enable'}\n"
            f"• Weekly: {WEEK_ID} tracking active"
        )

        while True:
            now = asyncio.get_event_loop().time()

            if now - last_check >= CHECK_INTERVAL:
                try:
                    await auto_check(client)
                except Exception as e:
                    log.error(f"auto_check: {e}")
                last_check = now

            updates = await get_updates(client, offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg    = update.get("message", {})
                cid    = str(msg.get("chat", {}).get("id", ""))
                text   = msg.get("text", "").strip()

                if cid != ALLOWED_ID:
                    continue

                if text.startswith("/status"):
                    await send(client, "⏳ กำลังตรวจสอบ W22 ecosystem...")
                    brain, reflex = await asyncio.gather(
                        fetch_brain(client),
                        fetch_reflex(client),
                    )
                    await send(client, build_w22_message(brain, reflex))

                elif text.startswith(("/help", "/start")):
                    await send(client,
                        f"🛡️ <b>BTC Ecosystem Monitor v3</b>\n\n"
                        f"<b>Commands:</b>\n"
                        f"/status — W22 ecosystem snapshot\n\n"
                        f"<b>Auto-alert:</b>\n"
                        f"• Brain Ops down / recovery\n"
                        f"• Signal Bot หยุด / recovery\n\n"
                        f"<b>Nodes:</b>\n"
                        f"• Signal Bot (inferred)\n"
                        f"• Brain Ops + W22 weekly\n"
                        f"• Reflex Engine (observer)\n"
                        f"• Monitor Node (self)"
                    )

            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
