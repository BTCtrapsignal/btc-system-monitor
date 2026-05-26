"""
BTC Monitor Bot
"""

import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("MONITOR_BOT_TOKEN")
ALLOWED_ID = os.getenv("MONITOR_CHAT_ID")

BRAIN_URL = os.getenv(
    "BRAIN_OPS_URL",
    "https://web-production-f47d4.up.railway.app"
)

CHECK_INTERVAL = int(
    os.getenv("CHECK_INTERVAL_SEC", "300")
)

if not BOT_TOKEN:
    raise RuntimeError("MONITOR_BOT_TOKEN missing")

if not ALLOWED_ID:
    raise RuntimeError("MONITOR_CHAT_ID missing")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

TH_TZ = timezone(timedelta(hours=7))

_prev_state = {
    "brain_ops": "unknown",
    "layer_a": "unknown",
}

async def send(client: httpx.AsyncClient, text: str):
    try:
        await client.post(
            f"{TELEGRAM_API}/sendMessage",
            json={
                "chat_id": ALLOWED_ID,
                "text": text,
                "parse_mode": "HTML",
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
                "allowed_updates": ["message"]
            },
            timeout=10
        )

        return r.json().get("result", [])

    except Exception as e:
        log.warning(f"getUpdates failed: {e}")
        return []


async def fetch_monitor(client: httpx.AsyncClient):

    result = {
        "brain_ok": False,
        "brain_lat": 0,
        "brain_version": "?",
        "brain_uptime": "?",
        "brain_signals": 0,
        "layer_a_status": "no_data",
        "layer_a_last": None,
        "layer_a_open": 0,
        "layer_c_status": "unconfirmed",
        "overall": "unknown",
    }

    try:
        r = await client.get(
            f"{BRAIN_URL}/health",
            timeout=8
        )

        if r.status_code == 200:
            result["brain_ok"] = True
            result["brain_lat"] = int(
                r.elapsed.total_seconds() * 1000
            )

            d = r.json()

            result["brain_version"] = d.get("version", "?")

    except Exception:
        pass

    if not result["brain_ok"]:
        return result

    try:
        r2 = await client.get(
            f"{BRAIN_URL}/monitor/status",
            timeout=8
        )

        if r2.status_code == 200:

            m = r2.json()

            result["overall"] = m.get("overall", "unknown")

            a = m.get("layer_a", {})
            b = m.get("layer_b", {})
            c = m.get("layer_c", {})

            result["layer_a_status"] = a.get(
                "status",
                "no_data"
            )

            result["layer_a_last"] = a.get(
                "last_signal_ts"
            )

            result["layer_a_open"] = a.get(
                "open_trades_in_db",
                0
            )

            result["brain_uptime"] = b.get(
                "uptime_human",
                "?"
            )

            result["brain_signals"] = b.get(
                "total_signals_stored",
                0
            )

            result["layer_c_status"] = c.get(
                "status",
                "unconfirmed"
            )

    except Exception:
        pass

    return result


def build_status_message(d):

    if not d["brain_ok"]:
        return (
            "🔴 <b>System Alert</b>\n\n"
            "❌ Brain Ops ไม่ตอบสนอง\n\n"
            f"🕐 {_now()}"
        )

    a_status = d["layer_a_status"]

    if a_status == "active":
        a_icon = "🟢"
        a_line = (
            f"Active · {_rel(d['layer_a_last'])}"
            f" · open {d['layer_a_open']} trades"
        )

    elif a_status == "idle":
        a_icon = "🟡"
        a_line = (
            f"Idle · {_rel(d['layer_a_last'])}"
        )

    elif a_status == "likely_down":
        a_icon = "🔴"
        a_line = "ไม่มี signal นานผิดปกติ"

    else:
        a_icon = "⚪"
        a_line = "ไม่มีข้อมูล"

    c_status = d["layer_c_status"]

    if c_status == "active":
        c_icon = "🟢"
        c_line = "Active"

    else:
        c_icon = "🟡"
        c_line = c_status

    overall = d["overall"]

    if overall == "healthy":
        header = "✅ <b>ระบบปกติ</b>"

    elif overall == "warning":
        header = "⚠️ <b>มีบางระบบ idle</b>"

    else:
        header = "🔴 <b>ระบบมีปัญหา</b>"

    return (
        f"{header}\n\n"

        f"{a_icon} <b>Layer A</b>\n"
        f"└ {a_line}\n\n"

        f"🟢 <b>Layer B</b>\n"
        f"└ {d['brain_lat']}ms"
        f" · uptime {d['brain_uptime']}"
        f" · signals {d['brain_signals']}\n\n"

        f"{c_icon} <b>Layer C</b>\n"
        f"└ {c_line}\n\n"

        f"🕐 {_now()}"
    )


async def auto_check(client):

    global _prev_state

    d = await fetch_monitor(client)

    brain_now = "ok" if d["brain_ok"] else "down"

    if (
        brain_now == "down"
        and _prev_state["brain_ops"] != "down"
    ):

        await send(
            client,
            (
                "🚨 <b>ALERT — Brain Ops DOWN</b>\n\n"
                "ตรวจสอบ Railway deployment"
            )
        )

    elif (
        brain_now == "ok"
        and _prev_state["brain_ops"] == "down"
    ):

        await send(
            client,
            (
                "✅ <b>RECOVERY — Brain Ops กลับมาแล้ว</b>\n\n"
                f"Latency: {d['brain_lat']}ms"
            )
        )

    _prev_state["brain_ops"] = brain_now

    a_now = d["layer_a_status"]

    if (
        a_now == "likely_down"
        and _prev_state["layer_a"] != "likely_down"
    ):

        await send(
            client,
            (
                "🚨 <b>ALERT — Signal Bot หยุด?</b>\n\n"
                f"Last signal: {_rel(d['layer_a_last'])}"
            )
        )

    elif (
        a_now in ("active", "idle")
        and _prev_state["layer_a"] == "likely_down"
    ):

        await send(
            client,
            (
                "✅ <b>RECOVERY — Signal Bot กลับมาแล้ว</b>"
            )
        )

    _prev_state["layer_a"] = a_now


def _rel(iso):

    if not iso:
        return "—"

    try:

        dt = datetime.fromisoformat(
            iso.replace("Z", "")
        )

        diff = (
            datetime.utcnow() - dt
        ).total_seconds()

        if diff < 60:
            return f"{int(diff)}s ago"

        if diff < 3600:
            return f"{int(diff // 60)}m ago"

        if diff < 86400:
            return f"{int(diff // 3600)}h ago"

        return f"{int(diff // 86400)}d ago"

    except Exception:
        return "?"


def _now():

    return datetime.now(
        TH_TZ
    ).strftime("%d %b %Y %H:%M:%S")


async def main():

    log.info(
        f"Monitor starting every {CHECK_INTERVAL}s"
    )

    offset = 0
    last_check = 0.0

    async with httpx.AsyncClient() as client:

        await send(
            client,
            (
                "🤖 <b>BTC Monitor Bot พร้อมแล้ว</b>\n\n"
                "• /status\n"
                "• /help\n"
                f"• Auto-check ทุก {CHECK_INTERVAL//60} นาที"
            )
        )

        while True:

            now = asyncio.get_event_loop().time()

            if now - last_check >= CHECK_INTERVAL:

                await auto_check(client)

                last_check = now

            updates = await get_updates(
                client,
                offset
            )

            for update in updates:

                offset = update["update_id"] + 1

                msg = update.get("message", {})

                chat_id = str(
                    msg.get("chat", {}).get("id", "")
                )

                text = msg.get("text", "").strip()

                if chat_id != ALLOWED_ID:
                    continue

                if text.startswith("/status"):

                    await send(
                        client,
                        "⏳ กำลังตรวจสอบ..."
                    )

                    d = await fetch_monitor(client)

                    await send(
                        client,
                        build_status_message(d)
                    )

                elif text.startswith(("/help", "/start")):

                    await send(
                        client,
                        (
                            "🤖 <b>BTC Monitor Bot</b>\n\n"
                            "/status\n"
                            "/help"
                        )
                    )

            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
