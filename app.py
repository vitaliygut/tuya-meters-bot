import asyncio
import json
import logging
import os
import sqlite3
import time

import tinytuya
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tuya-bot")

# --- настройки ---------------------------------------------------------
REGION = os.environ.get("TUYA_REGION", "eu")
API_ID = os.environ["TUYA_ACCESS_ID"]
API_SECRET = os.environ["TUYA_ACCESS_SECRET"]
DEVICE_ID = os.environ["TUYA_DEVICE_ID"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "300"))
REPORT_INTERVAL = int(os.environ.get("REPORT_INTERVAL", "0"))  # 0 = без автоотчётов
DB_PATH = os.environ.get("DB_PATH", "/data/readings.db")

# мониторинг и уведомления
NO_DATA_ALERT_HOURS = float(os.environ.get("NO_DATA_ALERT_HOURS", "48"))      # нет новых показаний
NO_CHANGE_ALERT_HOURS = float(os.environ.get("NO_CHANGE_ALERT_HOURS", "0"))   # показания не меняются (0 = выкл)
OFFLINE_ALERT_MINUTES = float(os.environ.get("OFFLINE_ALERT_MINUTES", "30"))  # устройство офлайн
API_FAIL_ALERT_MINUTES = float(os.environ.get("API_FAIL_ALERT_MINUTES", "30"))  # Tuya Cloud недоступен
ALERT_REPEAT_HOURS = float(os.environ.get("ALERT_REPEAT_HOURS", "24"))        # напоминание, пока проблема есть
RETRY_INTERVAL = int(os.environ.get("RETRY_INTERVAL", "60"))                  # повтор опроса после ошибки


def parse_sensors(raw):
    """code:Название:масштаб:единица,code2:..."""
    out = []
    for part in filter(None, (p.strip() for p in raw.split(","))):
        it = part.split(":")
        out.append(
            (
                it[0],
                it[1] if len(it) > 1 else it[0],
                float(it[2]) if len(it) > 2 else 1.0,
                it[3] if len(it) > 3 else "",
            )
        )
    return out


SENSORS = parse_sensors(os.environ.get("SENSORS", ""))

# --- кнопки ------------------------------------------------------------
BTN_NOW = "📊 Показания"
BTN_DAY = "📈 За 24 часа"
BTN_RAW = "🔧 Все DP"
BTN_HEALTH = "🩺 Состояние"

KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_NOW), KeyboardButton(text=BTN_DAY)],
        [KeyboardButton(text=BTN_HEALTH), KeyboardButton(text=BTN_RAW)],
    ],
    resize_keyboard=True,
)

# --- Tuya --------------------------------------------------------------
cloud = tinytuya.Cloud(apiRegion=REGION, apiKey=API_ID, apiSecret=API_SECRET, apiDeviceID=DEVICE_ID)
tuya_lock = asyncio.Lock()


def _get_status_sync():
    res = cloud.getstatus(DEVICE_ID)
    if not res or not res.get("success"):
        raise RuntimeError(f"Tuya API error: {res}")
    return {i["code"]: i["value"] for i in res["result"]}


async def get_status():
    async with tuya_lock:
        return await asyncio.to_thread(_get_status_sync)


def _check_online_sync():
    res = cloud.cloudrequest(f"/v1.0/devices/{DEVICE_ID}")
    if not res or not res.get("success"):
        raise RuntimeError(f"Tuya API error: {res}")
    return bool(res["result"].get("online", True))


async def check_online():
    async with tuya_lock:
        return await asyncio.to_thread(_check_online_sync)


# --- история (SQLite) --------------------------------------------------
def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS readings (ts INTEGER, code TEXT, value REAL)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_code_ts ON readings (code, ts)")
    return conn


def save_readings(status):
    now = int(time.time())
    rows = []
    for code, *_ in SENSORS:
        try:
            rows.append((now, code, float(status[code])))
        except (KeyError, TypeError, ValueError):
            pass
    if rows:
        with db() as conn:
            conn.executemany("INSERT INTO readings VALUES (?,?,?)", rows)


def delta_24h(code):
    since = int(time.time()) - 86400
    with db() as conn:
        first = conn.execute(
            "SELECT value FROM readings WHERE code=? AND ts>=? ORDER BY ts ASC LIMIT 1", (code, since)
        ).fetchone()
        last = conn.execute(
            "SELECT value FROM readings WHERE code=? ORDER BY ts DESC LIMIT 1", (code,)
        ).fetchone()
    if not first or not last:
        return None
    return last[0] - first[0]


def last_reading_ts():
    with db() as conn:
        row = conn.execute("SELECT MAX(ts) FROM readings").fetchone()
    return row[0] if row and row[0] else None


def last_change_ts():
    """Когда в последний раз менялось хоть одно из значений."""
    best = None
    with db() as conn:
        for code, *_ in SENSORS:
            last = conn.execute(
                "SELECT value FROM readings WHERE code=? ORDER BY ts DESC LIMIT 1", (code,)
            ).fetchone()
            if not last:
                continue
            row = conn.execute(
                "SELECT MAX(ts) FROM readings WHERE code=? AND value != ?", (code, last[0])
            ).fetchone()
            ts = row[0] if row and row[0] else conn.execute(
                "SELECT MIN(ts) FROM readings WHERE code=?", (code,)
            ).fetchone()[0]
            if ts and (best is None or ts > best):
                best = ts
    return best


# --- форматирование ----------------------------------------------------
def fmt_now(status):
    if not SENSORS:
        return "SENSORS не задан в .env. Нажмите «🔧 Все DP», чтобы увидеть коды."
    lines = ["📊 Показания счётчиков:"]
    for code, name, scale, unit in SENSORS:
        if code not in status:
            lines.append(f"{name}: нет данных ({code})")
            continue
        try:
            lines.append(f"{name}: {float(status[code]) * scale:.3f} {unit}".strip())
        except (TypeError, ValueError):
            lines.append(f"{name}: {status[code]}")
    return "\n".join(lines)


def fmt_day():
    if not SENSORS:
        return "SENSORS не задан в .env."
    lines = ["📈 Расход за 24 часа:"]
    for code, name, scale, unit in SENSORS:
        d = delta_24h(code)
        lines.append(f"{name}: нет данных" if d is None else f"{name}: {d * scale:.3f} {unit}".strip())
    lines.append("\n(считается по накопленной истории, сразу после запуска данных мало)")
    return "\n".join(lines)


def fmt_dur(seconds):
    seconds = int(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d} д {h} ч"
    if h:
        return f"{h} ч {m} мин"
    return f"{m} мин"


def fmt_ts(ts):
    return time.strftime("%d.%m.%Y %H:%M", time.localtime(ts)) if ts else "нет"


def fmt_raw(status):
    return "🔧 Все DP устройства:\n" + json.dumps(status, ensure_ascii=False, indent=2)


# --- бот ---------------------------------------------------------------
bot = Bot(TG_TOKEN)
dp = Dispatcher()
dp.message.filter(F.chat.id == CHAT_ID)  # отвечаем только владельцу


@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Выберите действие:", reply_markup=KEYBOARD)


@dp.message(F.text == BTN_NOW)
async def now(m: Message):
    try:
        status = await get_status()
        save_readings(status)
        await m.answer(fmt_now(status), reply_markup=KEYBOARD)
    except Exception as e:
        log.exception("now")
        await m.answer(f"Ошибка: {e}")


@dp.message(F.text == BTN_DAY)
async def day(m: Message):
    await m.answer(fmt_day(), reply_markup=KEYBOARD)


@dp.message(F.text == BTN_RAW)
async def raw(m: Message):
    try:
        await m.answer(fmt_raw(await get_status()), reply_markup=KEYBOARD)
    except Exception as e:
        log.exception("raw")
        await m.answer(f"Ошибка: {e}")


# --- мониторинг ---------------------------------------------------------
STARTED_AT = time.time()
health = {"online": None, "api_fail_since": None, "offline_since": None, "last_error": None}
sent_alerts = {}  # key -> время последнего уведомления


async def notify(text):
    try:
        await bot.send_message(CHAT_ID, text, reply_markup=KEYBOARD)
    except Exception:
        log.exception("не удалось отправить уведомление")


async def raise_alert(key, text):
    """Шлёт уведомление один раз и повторяет не чаще ALERT_REPEAT_HOURS."""
    now = time.time()
    last = sent_alerts.get(key)
    if last is None or now - last >= ALERT_REPEAT_HOURS * 3600:
        sent_alerts[key] = now
        await notify(text)


async def clear_alert(key, text):
    if key in sent_alerts:
        del sent_alerts[key]
        await notify(text)


def fmt_health(online):
    lines = ["🩺 Состояние"]
    if online is None:
        lines.append("Устройство: не удалось проверить")
    else:
        lines.append("Устройство: " + ("🟢 онлайн" if online else "🔴 офлайн"))
    lines.append(f"Последние данные: {fmt_ts(last_reading_ts())}")
    if SENSORS:
        lines.append(f"Последнее изменение показаний: {fmt_ts(last_change_ts())}")
    lines.append(f"Опрос каждые {fmt_dur(POLL_INTERVAL)}")
    if sent_alerts:
        lines.append("⚠️ Активные проблемы: " + ", ".join(sorted(sent_alerts)))
    else:
        lines.append("Проблем нет")
    return "\n".join(lines)


async def poll_once():
    """Один цикл опроса. Возвращает True, если всё прошло успешно."""
    now = time.time()

    # 1. доступность Tuya Cloud
    try:
        online = await check_online()
        status = await get_status() if online else None
    except Exception as e:
        log.warning("Tuya Cloud: %s", e)
        health["last_error"] = str(e)
        if health["api_fail_since"] is None:
            health["api_fail_since"] = now
        down = now - health["api_fail_since"]
        if down >= API_FAIL_ALERT_MINUTES * 60:
            await raise_alert(
                "api",
                f"⚠️ Нет связи с Tuya Cloud уже {fmt_dur(down)}.\nПоследняя ошибка: {health['last_error'][:300]}\n"
                "Проверьте интернет, Access ID/Secret и срок триала IoT Core на iot.tuya.com.",
            )
        return False

    if health["api_fail_since"] is not None:
        health["api_fail_since"] = None
        await clear_alert("api", "✅ Связь с Tuya Cloud восстановлена.")

    # 2. доступность устройства
    health["online"] = online
    if not online:
        if health["offline_since"] is None:
            health["offline_since"] = now
        off = now - health["offline_since"]
        if off >= OFFLINE_ALERT_MINUTES * 60:
            await raise_alert("offline", f"🔴 Устройство офлайн уже {fmt_dur(off)}. Проверьте питание и Wi-Fi модуля.")
    else:
        if health["offline_since"] is not None:
            health["offline_since"] = None
            await clear_alert("offline", "🟢 Устройство снова онлайн.")
        save_readings(status)

    # 3. свежесть данных
    if SENSORS:
        baseline = last_reading_ts() or STARTED_AT
        gap = now - baseline
        if gap >= NO_DATA_ALERT_HOURS * 3600:
            await raise_alert(
                "no_data",
                f"📭 Нет показаний уже {fmt_dur(gap)} (последние: {fmt_ts(last_reading_ts())}).\n"
                "Проверьте устройство и коды в SENSORS.",
            )
        else:
            await clear_alert("no_data", "✅ Показания снова поступают.")

        if NO_CHANGE_ALERT_HOURS > 0 and online:
            changed = last_change_ts()
            if changed and now - changed >= NO_CHANGE_ALERT_HOURS * 3600:
                await raise_alert(
                    "no_change",
                    f"💤 Показания не меняются уже {fmt_dur(now - changed)}. Возможно, нет расхода или счётчик завис.",
                )
            else:
                await clear_alert("no_change", "✅ Показания снова меняются.")

    return True


async def poll_loop():
    last_report = time.time()
    while True:
        ok = False
        try:
            ok = await poll_once()
            if ok and REPORT_INTERVAL and time.time() - last_report >= REPORT_INTERVAL:
                await notify(fmt_now(await get_status()))
                last_report = time.time()
        except Exception:
            log.exception("poll")
        await asyncio.sleep(POLL_INTERVAL if ok else min(POLL_INTERVAL, RETRY_INTERVAL))


@dp.message(F.text == BTN_HEALTH)
async def health_cmd(m: Message):
    try:
        online = await check_online()
    except Exception:
        online = None
    await m.answer(fmt_health(online), reply_markup=KEYBOARD)


async def main():
    asyncio.create_task(poll_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
