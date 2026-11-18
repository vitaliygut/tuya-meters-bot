"""
tuya-meters-bot

Облако Tuya (API): только показания счётчиков (по кнопке и ежемесячный отчёт).
Локальная сеть (tinytuya): алерты (протечка, краны, связь с устройством), без лимитов API.
"""
import asyncio
import calendar
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime

import tinytuya
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tuya-bot")

# --- настройки ---------------------------------------------------------
REGION = os.environ.get("TUYA_REGION", "eu")
API_ID = os.environ["TUYA_ACCESS_ID"]
API_SECRET = os.environ["TUYA_ACCESS_SECRET"]
DEVICE_ID = os.environ["TUYA_DEVICE_ID"]

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])  # чат, где работают кнопки
NOTIFY_CHAT_ID = int(os.environ.get("NOTIFY_CHAT_ID") or CHAT_ID)  # куда слать алерты и отчёты

DB_PATH = os.environ.get("DB_PATH", "/data/bot.db")
API_MONTHLY_LIMIT = int(os.environ.get("API_MONTHLY_LIMIT", "26000"))  # только для отображения

# локальный режим (алерты)
LOCAL_IP = os.environ.get("TUYA_LOCAL_IP", "").strip()
LOCAL_KEY_ENV = os.environ.get("TUYA_LOCAL_KEY", "").strip()  # пусто = взять из облака один раз
LOCAL_VERSION = os.environ.get("TUYA_LOCAL_VERSION", "auto").strip()  # auto, 3.3, 3.4, 3.5
LOCAL_POLL_INTERVAL = int(os.environ.get("LOCAL_POLL_INTERVAL", "15"))
OFFLINE_ALERT_MINUTES = float(os.environ.get("OFFLINE_ALERT_MINUTES", "10"))
ALERT_REPEAT_HOURS = float(os.environ.get("ALERT_REPEAT_HOURS", "24"))
LEAK_REPEAT_MINUTES = float(os.environ.get("LEAK_REPEAT_MINUTES", "30"))

# автоматическое снятие показаний раз в сутки (облако, 1 вызов API в день)
DAILY_READ_HOUR = int(os.environ.get("DAILY_READ_HOUR", "8"))  # час по TZ, -1 = выключено
DAILY_RETRY_MINUTES = int(os.environ.get("DAILY_RETRY_MINUTES", "60"))  # повтор после неудачи
DAILY_REPORT = os.environ.get("DAILY_REPORT", "false").lower() == "true"  # слать показания в чат каждый день
NO_DATA_ALERT_DAYS = float(os.environ.get("NO_DATA_ALERT_DAYS", "3"))  # нет показаний N дней -> тревога

# ежемесячный отчёт: число месяца (0 = выключен) и час по времени TZ
MONTHLY_REPORT_DAY = int(os.environ.get("MONTHLY_REPORT_DAY", "0"))
MONTHLY_REPORT_HOUR = int(os.environ.get("MONTHLY_REPORT_HOUR", "9"))


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


def parse_value(raw):
    """Строка из .env -> значение DP: true/false, null, число или текст."""
    low = raw.strip().lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none", ""):
        return None
    try:
        return int(low)
    except ValueError:
        pass
    try:
        return float(low)
    except ValueError:
        return raw.strip()


def parse_watch(raw):
    """code:Название:норма,..."""
    out = []
    for part in filter(None, (p.strip() for p in raw.split(","))):
        it = part.split(":")
        out.append((it[0], it[1] if len(it) > 1 else it[0], parse_value(it[2]) if len(it) > 2 else None))
    return out


SENSORS = parse_sensors(os.environ.get("SENSORS", ""))

DEFAULT_WATCH = (
    "status_wire_line:Проводные датчики:0,"
    "status_rf_sensor:Беспроводные датчики:null,"
    "notifications:Уведомления Neptun:0,"
    "status_crane_group_1:Кран группа 1:true,"
    "status_crane_group_2:Кран группа 2:true"
)
LEAK_WATCH = parse_watch(os.environ.get("LEAK_WATCH", DEFAULT_WATCH))

# --- кнопки ------------------------------------------------------------
BTN_NOW = "📊 Показания"
BTN_HEALTH = "🩺 Состояние"
BTN_RAW = "🔧 Все DP"

KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text=BTN_NOW)], [KeyboardButton(text=BTN_HEALTH), KeyboardButton(text=BTN_RAW)]],
    resize_keyboard=True,
)

# --- хранилище (SQLite) -------------------------------------------------
def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS readings (ts INTEGER, code TEXT, value REAL)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_readings ON readings (code, ts)")
    return conn


def meta_get(key):
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def meta_set(key, value):
    with db() as conn:
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))


def record_readings(status):
    """Сохраняет показания счётчиков из SENSORS. True, если получено хоть одно значение."""
    now_ts = int(time.time())
    rows = []
    for code, *_ in SENSORS:
        try:
            rows.append((now_ts, code, float(status[code])))
        except (KeyError, TypeError, ValueError):
            pass
    if not rows:
        return False
    with db() as conn:
        conn.executemany("INSERT INTO readings VALUES (?,?,?)", rows)
        conn.execute("DELETE FROM readings WHERE ts < ?", (now_ts - 400 * 86400,))
    meta_set("last_ok_ts", str(now_ts))
    return True


def reading_before(code, ts):
    """Последнее сохранённое значение не позже ts: (время, значение) или None."""
    with db() as conn:
        return conn.execute(
            "SELECT ts, value FROM readings WHERE code=? AND ts<=? ORDER BY ts DESC LIMIT 1", (code, int(ts))
        ).fetchone()


def count_api_call(n=1):
    try:
        key = "api_calls:" + datetime.now().strftime("%Y-%m")
        meta_set(key, str(int(meta_get(key) or 0) + n))
    except Exception:
        log.exception("count_api_call")


def api_usage():
    try:
        return int(meta_get("api_calls:" + datetime.now().strftime("%Y-%m")) or 0)
    except Exception:
        return 0


# --- облако Tuya: показания ----------------------------------------------
cloud = tinytuya.Cloud(apiRegion=REGION, apiKey=API_ID, apiSecret=API_SECRET, apiDeviceID=DEVICE_ID)
_cloud_lock = threading.Lock()


def cloud_get(url):
    """Любой запрос к облаку: под замком и со счётчиком вызовов."""
    with _cloud_lock:
        count_api_call()
        return cloud.cloudrequest(url)


def load_dp_map():
    """code -> числовой id DP (нужен, чтобы читать локальные данные по кодам)."""
    try:
        return json.loads(meta_get("dp_map") or "{}")
    except ValueError:
        return {}


def save_dp_map(new):
    current = load_dp_map()
    current.update(new)
    meta_set("dp_map", json.dumps(current))


SOURCES = ("status", "device", "shadow")
_working_source = None


def _try_source(name):
    urls = {
        "status": f"/v1.0/devices/{DEVICE_ID}/status",
        "device": f"/v1.0/devices/{DEVICE_ID}",
        "shadow": f"/v2.0/cloud/thing/{DEVICE_ID}/shadow/properties",
    }
    res = cloud_get(urls[name])
    if not (res and res.get("success")):
        return None, res
    result = res.get("result")
    if name == "status":
        return {i["code"]: i["value"] for i in (result or [])}, None
    if name == "device":
        return {i["code"]: i["value"] for i in ((result or {}).get("status") or [])}, None
    props = (result or {}).get("properties") or []
    ids = {p["code"]: str(p["dp_id"]) for p in props if p.get("code") and p.get("dp_id") is not None}
    if ids:
        save_dp_map(ids)
    return {p.get("code") or str(p.get("dp_id")): p.get("value") for p in props}, None


def _get_status_sync():
    """Показания из облака: первый рабочий источник запоминается, чтобы не тратить лимит."""
    global _working_source
    order = [_working_source] if _working_source else list(SOURCES)
    errors = []
    for name in order:
        status, err = _try_source(name)
        if status:
            if _working_source != name:
                log.info("Источник данных Tuya: %s", name)
            _working_source = name
            return status
        if err is not None:
            errors.append(f"{name}: {err}")
    _working_source = None
    if errors and len(errors) == len(order):
        raise RuntimeError("Tuya API error: " + " | ".join(errors))
    return {}


async def get_status():
    return await asyncio.to_thread(_get_status_sync)


def _fetch_dp_map_sync():
    """Один раз узнаём соответствие code -> dp_id (shadow, затем specifications)."""
    _try_source("shadow")
    if load_dp_map():
        return
    res = cloud_get(f"/v1.0/devices/{DEVICE_ID}/specifications")
    if res and res.get("success"):
        result = res.get("result") or {}
        ids = {}
        for item in (result.get("functions") or []) + (result.get("status") or []):
            if item.get("code") and item.get("dp_id") is not None:
                ids[item["code"]] = str(item["dp_id"])
        if ids:
            save_dp_map(ids)


SECRET_KEYS = {"local_key", "uid", "ip", "lat", "lon", "owner_id", "sn", "uuid", "mac", "time_zone"}


def _scrub(obj):
    if isinstance(obj, dict):
        return {k: _scrub(v) for k, v in obj.items() if k not in SECRET_KEYS}
    if isinstance(obj, list):
        return [_scrub(v) for v in obj]
    return obj


def _diag_sync():
    out = {}
    for name, url in [
        ("device", f"/v1.0/devices/{DEVICE_ID}"),
        ("status", f"/v1.0/devices/{DEVICE_ID}/status"),
        ("specifications", f"/v1.0/devices/{DEVICE_ID}/specifications"),
        ("shadow", f"/v2.0/cloud/thing/{DEVICE_ID}/shadow/properties"),
    ]:
        try:
            out[name] = _scrub(cloud_get(url))
        except Exception as e:
            out[name] = f"ошибка: {e}"
    return out


# --- локальный доступ (алерты) --------------------------------------------
_local = {"dev": None, "version": None, "key_fetched": 0.0}
local_state = {"ts": None, "dps": None, "fail_since": None, "fails": 0, "error": None}


def _refresh_local_key_sync():
    res = cloud_get(f"/v1.0/devices/{DEVICE_ID}")
    key = ((res or {}).get("result") or {}).get("local_key") if res and res.get("success") else None
    if not key:
        raise RuntimeError(f"не удалось получить local_key из облака: {res}")
    meta_set("local_key", key)
    _local["key_fetched"] = time.time()
    return key


def _get_local_key_sync():
    return LOCAL_KEY_ENV or meta_get("local_key") or _refresh_local_key_sync()


def _make_device(version, key):
    d = tinytuya.Device(DEVICE_ID, LOCAL_IP, key, version=float(version))
    for name, arg in (("set_socketTimeout", 5), ("set_socketRetryLimit", 1)):
        try:
            getattr(d, name)(arg)
        except Exception:
            pass
    return d


def _local_status_sync():
    key = _get_local_key_sync()
    if _local["dev"] is None:
        if _local["version"]:
            versions = [_local["version"]]
        elif LOCAL_VERSION != "auto":
            versions = [LOCAL_VERSION]
        else:
            versions = ["3.4", "3.3", "3.5", "3.2"]
        last = None
        for v in versions:
            d = _make_device(v, key)
            res = d.status()
            if res and "dps" in res:
                _local.update(dev=d, version=v)
                log.info("Локальное подключение: %s, протокол %s", LOCAL_IP, v)
                return res["dps"]
            last = res
        raise RuntimeError(f"локальный опрос не удался: {last}")
    res = _local["dev"].status()
    if not res or "dps" not in res:
        _local["dev"] = None
        raise RuntimeError(f"локальный опрос не удался: {res}")
    return res["dps"]


def to_codes(dps):
    """Локальные ключи ('1', '101') -> коды DP ('counter7'), если соответствие известно."""
    rev = {v: k for k, v in load_dp_map().items()}
    return {rev.get(str(k), str(k)): v for k, v in dps.items()}


# --- форматирование -----------------------------------------------------
def fmt_val(v):
    return json.dumps(v, ensure_ascii=False)


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


def fmt_daily(status):
    now_ts = time.time()
    lines = [f"📅 Показания на {datetime.now():%d.%m.%Y}:"]
    for code, name, scale, unit in SENSORS:
        try:
            value = float(status[code])
        except (KeyError, TypeError, ValueError):
            lines.append(f"{name}: нет данных ({code})")
            continue
        line = f"{name}: {value * scale:.3f} {unit}".strip()
        prev = reading_before(code, now_ts - 12 * 3600)
        if prev:
            line += f" (+{(value - prev[1]) * scale:.3f} {unit} с {time.strftime('%d.%m', time.localtime(prev[0]))})"
        lines.append(line)
    return "\n".join(lines)


def fmt_monthly(status, prev, prev_date, now):
    lines = [f"🗓 Показания на {now:%d.%m.%Y}:"]
    for code, name, scale, unit in SENSORS:
        if code not in status:
            lines.append(f"{name}: нет данных ({code})")
            continue
        try:
            value = float(status[code])
        except (TypeError, ValueError):
            lines.append(f"{name}: {status[code]}")
            continue
        line = f"{name}: {value * scale:.3f} {unit}".strip()
        if prev and code in prev:
            line += f" (+{(value - prev[code]) * scale:.3f} {unit} с {prev_date})".replace("  ", " ")
        lines.append(line)
    if not prev:
        lines.append("\nЭто первый ежемесячный отчёт, расход появится со следующего.")
    return "\n".join(lines)


def fmt_health():
    lines = ["🩺 Состояние"]
    if not LOCAL_IP:
        lines.append("Локальный режим выключен (не задан TUYA_LOCAL_IP), алертов нет")
    elif local_state["ts"] and not local_state["fail_since"]:
        lines.append(f"Связь по LAN: 🟢 ответ {fmt_dur(time.time() - local_state['ts'])} назад")
        lines.append(f"Локальный опрос: {LOCAL_IP}, протокол {_local['version']}, каждые {LOCAL_POLL_INTERVAL} с")
    elif local_state["fail_since"]:
        lines.append(f"Связь по LAN: 🔴 нет ответа уже {fmt_dur(time.time() - local_state['fail_since'])}")
        if local_state["error"]:
            lines.append(f"Ошибка: {str(local_state['error'])[:200]}")
    else:
        lines.append("Связь по LAN: ожидание первого ответа")
    if MONTHLY_REPORT_DAY:
        lines.append(f"Ежемесячный отчёт: {MONTHLY_REPORT_DAY} числа в {MONTHLY_REPORT_HOUR:02d}:00")
    last_ok = meta_get("last_ok_ts")
    lines.append(f"Последние показания: {fmt_ts(float(last_ok)) if last_ok else 'ещё не снимались'}")
    if SENSORS and DAILY_READ_HOUR >= 0:
        lines.append(f"Автосъём показаний: ежедневно в {DAILY_READ_HOUR:02d}:00, тревога через {NO_DATA_ALERT_DAYS:g} дн без данных")
    lines.append(f"Вызовов Tuya API в этом месяце: {api_usage()} из {API_MONTHLY_LIMIT}")
    lines.append("⚠️ Активные проблемы: " + ", ".join(sorted(sent_alerts)) if sent_alerts else "Проблем нет")
    return "\n".join(lines)


# --- бот ------------------------------------------------------------------
bot = Bot(TG_TOKEN)
dp = Dispatcher()
dp.message.filter(F.chat.id == CHAT_ID)  # отвечаем только владельцу

sent_alerts = {}  # key -> время последнего уведомления


async def notify(text):
    try:
        if NOTIFY_CHAT_ID == CHAT_ID:
            await bot.send_message(CHAT_ID, text, reply_markup=KEYBOARD)
        else:
            await bot.send_message(NOTIFY_CHAT_ID, text)
        return True
    except Exception:
        log.exception("не удалось отправить уведомление")
        return False


async def raise_alert(key, text, repeat=None):
    """Шлёт уведомление и повторяет его не чаще repeat секунд (по умолчанию ALERT_REPEAT_HOURS)."""
    now = time.time()
    last = sent_alerts.get(key)
    if repeat is None:
        repeat = ALERT_REPEAT_HOURS * 3600
    if last is None or now - last >= repeat:
        sent_alerts[key] = now
        await notify(text)


async def clear_alert(key, text):
    if key in sent_alerts:
        del sent_alerts[key]
        await notify(text)


async def answer_long(m: Message, text: str):
    for i in range(0, len(text), 3500):
        await m.answer(text[i : i + 3500], reply_markup=KEYBOARD)


@dp.message(CommandStart())
async def start(m: Message):
    await m.answer("Выберите действие:", reply_markup=KEYBOARD)


@dp.message(F.text == BTN_NOW)
async def now(m: Message):
    try:
        status = await get_status()  # облако: 1 вызов API
        record_readings(status)
        await m.answer(fmt_now(status), reply_markup=KEYBOARD)
    except Exception as e:
        log.exception("now")
        await m.answer(f"Ошибка: {e}")


@dp.message(F.text == BTN_HEALTH)
async def health_cmd(m: Message):
    await m.answer(fmt_health(), reply_markup=KEYBOARD)


@dp.message(F.text == BTN_RAW)
async def raw(m: Message):
    try:
        if local_state["dps"] is not None:
            data = to_codes(local_state["dps"])
            title = "🔧 Все DP (локально, без API):\n"
        else:
            data = await get_status()
            title = "🔧 Все DP (облако):\n"
        await answer_long(m, title + json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        log.exception("raw")
        await m.answer(f"Ошибка: {e}")


@dp.message(Command("diag"))
async def diag(m: Message):
    try:
        data = await asyncio.to_thread(_diag_sync)
        await answer_long(m, "🧪 Диагностика (облако, 4 вызова API):\n" + json.dumps(data, ensure_ascii=False, indent=1))
    except Exception as e:
        log.exception("diag")
        await m.answer(f"Ошибка: {e}")


# --- алерты по локальной сети -------------------------------------------------
async def check_leaks(status):
    for code, name, norm in LEAK_WATCH:
        if code not in status:
            continue
        value = status[code]
        key = f"leak:{code}"
        if value != norm:
            await raise_alert(
                key,
                f"🚨 {name}: {fmt_val(value)} (в норме {fmt_val(norm)}).\n"
                "Возможна протечка или сработала защита. Проверьте датчики и краны!",
                repeat=LEAK_REPEAT_MINUTES * 60,
            )
        else:
            await clear_alert(key, f"✅ {name}: снова в норме ({fmt_val(value)}).")


async def local_loop():
    if not LOCAL_IP:
        log.warning("TUYA_LOCAL_IP не задан: локальные алерты выключены")
        return
    last_map_try = 0.0
    while True:
        now_ts = time.time()
        try:
            if not load_dp_map() and now_ts - last_map_try > 600:
                last_map_try = now_ts
                try:
                    await asyncio.to_thread(_fetch_dp_map_sync)
                except Exception:
                    log.exception("не удалось получить соответствие DP")
            dps = await asyncio.to_thread(_local_status_sync)
            local_state.update(ts=now_ts, dps=dps, fails=0, error=None)
            if local_state["fail_since"] is not None:
                local_state["fail_since"] = None
                await clear_alert("offline", "🟢 Связь с устройством по локальной сети восстановлена.")
            await check_leaks(to_codes(dps))
        except Exception as e:
            log.warning("Локальный опрос: %s", e)
            local_state["fails"] += 1
            local_state["error"] = str(e)
            if local_state["fail_since"] is None:
                local_state["fail_since"] = now_ts
            down = now_ts - local_state["fail_since"]
            if down >= OFFLINE_ALERT_MINUTES * 60:
                await raise_alert(
                    "offline",
                    f"🔴 Нет связи с устройством по локальной сети уже {fmt_dur(down)}.\n"
                    f"Проверьте питание и Wi-Fi модуля, IP ({LOCAL_IP}), local key.",
                )
            # local_key мог смениться (устройство переподключали): раз в час берём новый из облака
            if not LOCAL_KEY_ENV and local_state["fails"] >= 5 and now_ts - _local["key_fetched"] > 3600:
                try:
                    await asyncio.to_thread(_refresh_local_key_sync)
                    _local["dev"] = None
                except Exception:
                    log.exception("обновление local_key")
        await asyncio.sleep(LOCAL_POLL_INTERVAL)


# --- ежемесячный отчёт (облако, 1 вызов в месяц) -----------------------------
def monthly_due(now):
    if not MONTHLY_REPORT_DAY or not SENSORS:
        return False
    day = min(MONTHLY_REPORT_DAY, calendar.monthrange(now.year, now.month)[1])
    if (now.day, now.hour) < (day, MONTHLY_REPORT_HOUR):
        return False
    return meta_get("monthly_last") != now.strftime("%Y-%m")


async def maybe_send_monthly():
    now_dt = datetime.now()
    if not monthly_due(now_dt):
        return
    status = await get_status()
    record_readings(status)
    prev = json.loads(meta_get("monthly_snapshot") or "null")
    prev_date = meta_get("monthly_snapshot_date") or ""
    if await notify(fmt_monthly(status, prev, prev_date, now_dt)):
        snap = {}
        for code, *_ in SENSORS:
            try:
                snap[code] = float(status[code])
            except (KeyError, TypeError, ValueError):
                pass
        meta_set("monthly_snapshot", json.dumps(snap))
        meta_set("monthly_snapshot_date", now_dt.strftime("%d.%m.%Y"))
        meta_set("monthly_last", now_dt.strftime("%Y-%m"))
        log.info("Ежемесячный отчёт отправлен")


async def monthly_loop():
    while True:
        try:
            await maybe_send_monthly()
        except Exception:
            log.exception("monthly")
        await asyncio.sleep(600)


# --- автосъём раз в сутки и тревога «нет данных» ------------------------------
async def check_no_data():
    if not SENSORS or DAILY_READ_HOUR < 0:
        return
    last_ok = float(meta_get("last_ok_ts") or meta_get("started_at") or time.time())
    gap = time.time() - last_ok
    if gap >= NO_DATA_ALERT_DAYS * 86400:
        await raise_alert(
            "no_data",
            f"📭 Нет показаний счётчиков уже {fmt_dur(gap)} (последние: {fmt_ts(last_ok)}).\n"
            "Проверьте устройство, связь с Tuya Cloud (ключи, триал IoT Core) и коды в SENSORS.",
        )
    else:
        await clear_alert("no_data", "✅ Показания счётчиков снова поступают.")


async def daily_loop():
    last_attempt = 0.0
    while True:
        try:
            now_dt = datetime.now()
            today = now_dt.strftime("%Y-%m-%d")
            if (
                SENSORS
                and DAILY_READ_HOUR >= 0
                and now_dt.hour >= DAILY_READ_HOUR
                and meta_get("daily_last") != today
                and time.time() - last_attempt >= DAILY_RETRY_MINUTES * 60
            ):
                last_attempt = time.time()
                try:
                    status = await get_status()
                    if record_readings(status):
                        meta_set("daily_last", today)
                        log.info("Суточное снятие показаний выполнено")
                        if DAILY_REPORT:
                            await notify(fmt_daily(status))
                    else:
                        log.warning("В ответе Tuya нет показаний из SENSORS")
                except Exception as e:
                    log.warning("Суточное снятие не удалось: %s", e)
            await check_no_data()
        except Exception:
            log.exception("daily_loop")
        await asyncio.sleep(300)


# --- служебное -------------------------------------------------------------
class UpdateLogger(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            inner = getattr(event, event.event_type, None)
            chat = getattr(inner, "chat", None)
            log.info(
                "Обновление: type=%s chat.id=%s chat.type=%s",
                event.event_type, getattr(chat, "id", None), getattr(chat, "type", None),
            )
        except Exception:
            log.exception("UpdateLogger")
        return await handler(event, data)


dp.update.outer_middleware(UpdateLogger())

fallback = Router()


@fallback.message()
async def unknown_chat(m: Message):
    log.warning(
        "Сообщение из чужого чата: chat.id=%s chat.type=%s user.id=%s (ожидался %s)",
        m.chat.id, m.chat.type, m.from_user.id if m.from_user else None, CHAT_ID,
    )
    await m.answer(f"Доступ закрыт. id этого чата: {m.chat.id}\nОжидается: {CHAT_ID}")


dp.include_router(fallback)


async def main():
    log.info("Старт. Владелец chat_id=%s, уведомления в %s", CHAT_ID, NOTIFY_CHAT_ID)
    if not meta_get("started_at"):
        meta_set("started_at", str(time.time()))
    asyncio.create_task(local_loop())
    asyncio.create_task(daily_loop())
    asyncio.create_task(monthly_loop())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
